from __future__ import annotations

import logging
import sqlite3
from collections.abc import Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import httpx

from parallax.adapters.base import (
    AdapterResolver,
    HistoricalAdapter,
    HistoryPlan,
)
from parallax.adapters.execution import (
    raise_for_status,
    run_adapter_refresh,
)
from parallax.adapters.results import NotModifiedRefresh, ResponseValidators
from parallax.config import IngestionConfig, Source
from parallax.domain import (
    HistoryOutcome,
    HistoryStatus,
    HistoryStopReason,
    IngestionBatchResult,
    IngestionFailure,
    IngestionSummary,
    ObservedBatch,
    ObservedCandidate,
    ParsedBatch,
    RunKind,
    StreamState,
    ValidatedBatch,
)
from parallax.identity import identity_keys
from parallax.transport import Transport
from parallax.validation import BatchValidator

if TYPE_CHECKING:
    from parallax.storage import Storage

LOGGER = logging.getLogger(__name__)
ERROR_MESSAGE_LIMIT = 2000


@dataclass(frozen=True, slots=True)
class _PreparedSuccess:
    batch: ValidatedBatch
    http_status: int | None
    observed_at: datetime
    conditional: ResponseValidators | None


@dataclass(frozen=True, slots=True)
class _PreparedHistory:
    batch: ObservedBatch
    outcome: HistoryOutcome


_PreparedOutcome = _PreparedSuccess | NotModifiedRefresh | _PreparedHistory


class IngestionService:
    def __init__(
        self,
        storage: Storage,
        transport: Transport,
        adapters: AdapterResolver,
        validator: BatchValidator,
        ingestion_config: IngestionConfig,
    ) -> None:
        self._storage = storage
        self._transport = transport
        self._adapters = adapters
        self._validator = validator
        self._ingestion_config = ingestion_config

    def fetch_source(
        self,
        source: Source,
        since: datetime | None = None,
    ) -> IngestionSummary:
        """Own one source's run from creation through success or final failure."""
        run_kind: RunKind = "history" if since is not None else "live"
        run_id = self._storage.start_fetch_run(source.id, run_kind)
        attempted_at = datetime.now(UTC)

        try:
            state = self._storage.get_stream_state(source.id)
            prepared = self._prepare(source, state, since)
            return self._commit_prepared(source, run_id, attempted_at, prepared)
        except BaseException as error:
            self._record_run_failure(source, run_id, run_kind, attempted_at, error)
            raise

    def fetch_sources(
        self,
        sources: Sequence[Source],
        since: datetime | None = None,
    ) -> IngestionBatchResult:
        """Fetch independent sources concurrently and commit outcomes serially."""
        if not sources:
            return IngestionBatchResult((), ())

        LOGGER.info(
            "operation=fetch_batch_start attempted=%s max_concurrent_sources=%s",
            len(sources),
            self._ingestion_config.max_concurrent_sources,
        )
        run_kind: RunKind = "history" if since is not None else "live"
        completed: dict[int, IngestionSummary] = {}
        failures: dict[int, IngestionFailure] = {}
        pending: dict[Future[_PreparedOutcome], tuple[int, Source, int, datetime]] = {}
        uncommitted: tuple[int, Source, int, datetime] | None = None
        starting: tuple[int, Source, int, datetime] | None = None
        source_iter = iter(enumerate(sources))

        with ThreadPoolExecutor(
            max_workers=self._ingestion_config.max_concurrent_sources,
            thread_name_prefix="parallax-fetch",
        ) as executor:

            def submit_next() -> bool:
                nonlocal starting
                try:
                    index, source = next(source_iter)
                except StopIteration:
                    return False
                run_id = self._storage.start_fetch_run(source.id, run_kind)
                starting = (index, source, run_id, datetime.now(UTC))
                state = self._storage.get_stream_state(source.id)
                future = executor.submit(self._prepare, source, state, since)
                pending[future] = starting
                starting = None
                return True

            try:
                for _ in range(
                    min(self._ingestion_config.max_concurrent_sources, len(sources))
                ):
                    submit_next()

                while pending:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        uncommitted = pending.pop(future)
                        index, source, run_id, attempted_at = uncommitted
                        try:
                            prepared = future.result()
                            summary = self._commit_prepared(
                                source, run_id, attempted_at, prepared
                            )
                        except sqlite3.Error:
                            # A storage failure aborts the batch; it is not an
                            # invalid source payload that another fetch can fix.
                            raise
                        except Exception as error:
                            recorded = self._record_run_failure(
                                source, run_id, run_kind, attempted_at, error
                            )
                            if not recorded:
                                raise
                            failures[index] = IngestionFailure(
                                source.id, type(error).__name__, _error_message(error)
                            )
                        else:
                            completed[index] = summary
                        uncommitted = None
                        submit_next()
            except BaseException as error:
                for future in pending:
                    future.cancel()
                executor.shutdown(wait=True, cancel_futures=True)
                unfinished = tuple(pending.values())
                if uncommitted is not None:
                    unfinished += (uncommitted,)
                if starting is not None:
                    unfinished += (starting,)
                for _, source, run_id, attempted_at in unfinished:
                    self._record_run_failure(
                        source, run_id, run_kind, attempted_at, error
                    )
                raise

        summaries = tuple(completed[index] for index in sorted(completed))
        ordered_failures = tuple(failures[index] for index in sorted(failures))
        LOGGER.info(
            "operation=fetch_batch_complete attempted=%s succeeded=%s failed=%s "
            "max_concurrent_sources=%s",
            len(sources),
            len(summaries),
            len(ordered_failures),
            self._ingestion_config.max_concurrent_sources,
        )
        return IngestionBatchResult(summaries, ordered_failures)

    def _prepare(
        self,
        source: Source,
        state: StreamState,
        since: datetime | None,
    ) -> _PreparedOutcome:
        adapter = self._adapters.resolve_source(source)
        if since is not None:
            if since.tzinfo is None:
                raise ValueError("history timestamps must be timezone-aware")
            until = datetime.now(UTC)
            if isinstance(adapter, HistoricalAdapter):
                plan = adapter.build_history_plan(source, since)
                return self._prepare_history(source, adapter, plan, since, until)
            return _PreparedHistory(
                ObservedBatch((), 0),
                HistoryOutcome(
                    since, until, None, None, 0, 0, "unsupported", "unsupported"
                ),
            )
        refresh = run_adapter_refresh(adapter, source, self._transport, state)
        if isinstance(refresh, NotModifiedRefresh):
            return refresh
        return _PreparedSuccess(
            self._validate(source, refresh.batch),
            refresh.http_status,
            refresh.observed_at,
            refresh.conditional,
        )

    def _prepare_history(
        self,
        source: Source,
        adapter: HistoricalAdapter,
        plan: HistoryPlan,
        since: datetime,
        until: datetime,
    ) -> _PreparedHistory:
        state = StreamState(source_id=source.id)
        budget = plan.max_items
        accepted: dict[str, ObservedCandidate] = {}
        warnings: list[str] = []
        rejected = 0
        pages = 0
        reason: HistoryStopReason = "page_budget"
        status: HistoryStatus = "truncated"
        for request in plan.requests:
            response = self._transport.request(request, source, state)
            pages += 1
            raise_for_status(response)
            page = adapter.parse_history_page(source, response, since)
            validated = self._validate_history(source, page.batch)
            rejected += validated.rejected_count
            warnings.extend(validated.warnings)
            for candidate in validated.candidates:
                if (
                    candidate.published_at is None
                    or not since <= candidate.published_at <= until
                ):
                    continue
                key, _ = identity_keys(candidate, provider_id=source.provider_id)
                accepted.setdefault(
                    key, ObservedCandidate(candidate, response.observed_at)
                )
                if len(accepted) == budget:
                    break
            if len(accepted) == budget:
                reason = "item_budget"
                break
            if page.exhausted:
                reason = "upstream_exhausted"
                status = "exhausted"
                break
        observations = tuple(accepted.values())
        published = [
            observation.candidate.published_at
            for observation in observations
            if observation.candidate.published_at is not None
        ]
        outcome = HistoryOutcome(
            requested_since=since,
            requested_until=until,
            observed_since=min(published, default=None),
            observed_until=max(published, default=None),
            pages_requested=pages,
            items_accepted=len(observations),
            status=status,
            stop_reason=reason,
        )
        LOGGER.info(
            "operation=history_batch source_id=%s pages=%s accepted=%s "
            "status=%s reason=%s",
            source.id,
            pages,
            len(observations),
            outcome.status,
            reason,
        )
        return _PreparedHistory(
            ObservedBatch(observations, rejected, tuple(warnings)), outcome
        )

    def _commit_prepared(
        self,
        source: Source,
        run_id: int,
        now: datetime,
        prepared: _PreparedOutcome,
    ) -> IngestionSummary:
        if isinstance(prepared, NotModifiedRefresh):
            return self._storage.record_not_modified(
                source.id,
                run_id,
                prepared.conditional.etag,
                prepared.conditional.last_modified,
                now + timedelta(seconds=source.interval_seconds),
                request_identity=prepared.conditional.request_identity,
            )
        if isinstance(prepared, _PreparedHistory):
            return self._storage.record_history(
                source, run_id, prepared.batch, prepared.outcome
            )
        conditional = prepared.conditional
        return self._storage.record_success(
            source=source,
            fetch_run_id=run_id,
            http_status=prepared.http_status,
            batch=prepared.batch,
            response_etag=conditional.etag if conditional else None,
            response_last_modified=conditional.last_modified if conditional else None,
            next_run_at=now + timedelta(seconds=source.interval_seconds),
            observed_at=prepared.observed_at,
            request_identity=conditional.request_identity if conditional else None,
        )

    def _validate(self, source: Source, parsed: ParsedBatch) -> ValidatedBatch:
        LOGGER.info(
            "operation=parse_batch source_id=%s candidates=%s warnings=%s",
            source.id,
            len(parsed.candidates),
            len(parsed.warnings),
        )
        return self._validator.validate(
            source.id, parsed, provider_id=source.provider_id
        )

    def _validate_history(self, source: Source, parsed: ParsedBatch) -> ValidatedBatch:
        if parsed.candidates:
            return self._validate(source, parsed)
        return ValidatedBatch((), 0, parsed.warnings)

    def _record_run_failure(
        self,
        source: Source,
        run_id: int,
        run_kind: RunKind,
        attempted_at: datetime,
        error: BaseException,
    ) -> bool:
        """Finalize one failed run without replacing the caller's error.

        Recording is best effort: when storage cannot take the failure (for
        example because the database is unavailable), the original error still
        propagates to the caller, including interruptions.
        """
        message = _error_message(error)
        next_run_at = None
        recorded = False
        try:
            if run_kind == "live":
                state = self._storage.get_stream_state(source.id)
                delay = min(
                    self._ingestion_config.retry_max_seconds,
                    self._ingestion_config.retry_base_seconds
                    * (2 ** min(20, state.consecutive_failures)),
                )
                next_run_at = attempted_at + timedelta(seconds=delay)
            status = (
                error.response.status_code
                if isinstance(error, httpx.HTTPStatusError)
                else None
            )
            self._storage.record_failure(
                source.id, run_id, error, next_run_at, status, message
            )
            recorded = True
        except BaseException as storage_error:
            LOGGER.error(
                "operation=fetch_failure_record_failed source_id=%s "
                "fetch_run_id=%s error_type=%s error=%s",
                source.id,
                run_id,
                type(storage_error).__name__,
                _error_message(storage_error),
            )
        self._log_failure(source.id, run_id, error, next_run_at, message)
        return recorded

    @staticmethod
    def _log_failure(
        source_id: str,
        run_id: int,
        error: BaseException,
        next_run_at: datetime | None,
        message: str | None = None,
    ) -> None:
        message = message or _error_message(error)
        context = (
            source_id,
            run_id,
            type(error).__name__,
            message,
            next_run_at.isoformat() if next_run_at is not None else None,
        )
        LOGGER.error(
            "operation=fetch_failure source_id=%s fetch_run_id=%s "
            "error_type=%s error=%s next_run_at=%s",
            *context,
        )
        LOGGER.debug(
            "operation=fetch_failure_trace source_id=%s fetch_run_id=%s "
            "error_type=%s error=%s next_run_at=%s",
            *context,
            exc_info=(type(error), error, error.__traceback__),
        )


def _error_message(error: BaseException) -> str:
    return " ".join(str(error).split())[:ERROR_MESSAGE_LIMIT] or type(error).__name__
