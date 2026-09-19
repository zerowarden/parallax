from __future__ import annotations

import logging
from collections.abc import Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

from parallax.adapters.base import (
    AdapterLookup,
    HistoricalAdapter,
    MultiRequestAdapter,
    SourceAdapter,
    SteppedAdapter,
)
from parallax.adapters.execution import (
    raise_for_status,
    run_adapter_refresh,
)
from parallax.config import IngestionConfig, Source
from parallax.domain import (
    HttpResponse,
    IngestionBatchResult,
    IngestionFailure,
    IngestionSummary,
    ParsedBatch,
    RequestSpec,
    StreamState,
    ValidatedBatch,
)
from parallax.storage import Storage
from parallax.transport import Transport
from parallax.validation import BatchValidator

LOGGER = logging.getLogger(__name__)
ERROR_MESSAGE_LIMIT = 2000


@dataclass(frozen=True, slots=True)
class _PreparedSuccess:
    batch: ValidatedBatch
    http_status: int | None
    etag: str | None
    last_modified: str | None
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class _PreparedNotModified:
    etag: str | None
    last_modified: str | None


@dataclass(frozen=True, slots=True)
class _PreparedHistory:
    batch: ValidatedBatch


_PreparedOutcome = _PreparedSuccess | _PreparedNotModified | _PreparedHistory


class IngestionService:
    def __init__(
        self,
        storage: Storage,
        transport: Transport,
        adapters: AdapterLookup,
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
        run_id = self._storage.start_fetch_run(source.id)
        state = self._storage.get_stream_state(source.id)
        now = datetime.now(UTC)

        try:
            prepared = self._prepare(source, state, since)
        except Exception as exc:
            self._record_preparation_failure(source, run_id, now, exc)
            raise
        return self._commit_prepared(source, run_id, now, prepared)

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
        completed: dict[int, IngestionSummary] = {}
        failures: dict[int, IngestionFailure] = {}
        pending: dict[Future[_PreparedOutcome], tuple[int, Source, int, datetime]] = {}
        uncommitted: tuple[int, Source, int, datetime] | None = None
        source_iter = iter(enumerate(sources))

        with ThreadPoolExecutor(
            max_workers=self._ingestion_config.max_concurrent_sources,
            thread_name_prefix="parallax-fetch",
        ) as executor:

            def submit_next() -> bool:
                try:
                    index, source = next(source_iter)
                except StopIteration:
                    return False
                run_id = self._storage.start_fetch_run(source.id)
                state = self._storage.get_stream_state(source.id)
                attempted_at = datetime.now(UTC)
                future = executor.submit(self._prepare, source, state, since)
                pending[future] = (index, source, run_id, attempted_at)
                return True

            for _ in range(
                min(self._ingestion_config.max_concurrent_sources, len(sources))
            ):
                submit_next()

            try:
                while pending:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        uncommitted = pending.pop(future)
                        index, source, run_id, attempted_at = uncommitted
                        try:
                            prepared = future.result()
                        except Exception as error:
                            self._record_preparation_failure(
                                source, run_id, attempted_at, error
                            )
                            failures[index] = IngestionFailure(
                                source.id, type(error).__name__, _error_message(error)
                            )
                        else:
                            completed[index] = self._commit_prepared(
                                source, run_id, attempted_at, prepared
                            )
                        uncommitted = None
                        submit_next()
            except BaseException:
                interruption = InterruptedError("Batch ingestion interrupted")
                for future in pending:
                    future.cancel()
                executor.shutdown(wait=True, cancel_futures=True)
                interrupted_runs = tuple(pending.values())
                if uncommitted is not None:
                    interrupted_runs += (uncommitted,)
                for _, source, run_id, attempted_at in interrupted_runs:
                    self._record_preparation_failure(
                        source, run_id, attempted_at, interruption
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
        adapter = self._adapters.get(source.endpoint.adapter)
        if since is not None and isinstance(adapter, HistoricalAdapter):
            requests = adapter.build_history_requests(source, since)
            if requests:
                return self._prepare_history(source, adapter, requests, since)
            LOGGER.info(
                "operation=history_empty source_id=%s adapter=%s",
                source.id,
                source.endpoint.adapter,
            )
        elif since is not None:
            LOGGER.info(
                "operation=history_unsupported source_id=%s adapter=%s",
                source.id,
                source.endpoint.adapter,
            )
        if isinstance(adapter, (MultiRequestAdapter, SteppedAdapter)):
            return self._prepare_combined(source, adapter)
        return self._prepare_single(source, adapter, state)

    def _prepare_single(
        self, source: Source, adapter: SourceAdapter, state: StreamState
    ) -> _PreparedSuccess | _PreparedNotModified:
        refresh = run_adapter_refresh(adapter, source, self._transport, state)
        response = refresh.responses[0]
        if refresh.not_modified:
            return _PreparedNotModified(
                response.headers.get("etag"), response.headers.get("last-modified")
            )
        assert refresh.batch is not None
        return _PreparedSuccess(
            self._validate(source, refresh.batch),
            response.status_code,
            response.headers.get("etag"),
            response.headers.get("last-modified"),
            refresh.observed_at,
        )

    def _prepare_combined(
        self,
        source: Source,
        adapter: MultiRequestAdapter | SteppedAdapter,
    ) -> _PreparedSuccess:
        refresh = run_adapter_refresh(
            adapter,
            source,
            self._transport,
            StreamState(source_id=source.id),
        )
        assert refresh.batch is not None
        return _PreparedSuccess(
            self._validate(source, refresh.batch),
            None,
            None,
            None,
            refresh.observed_at,
        )

    def _prepare_history(
        self,
        source: Source,
        adapter: HistoricalAdapter,
        requests: tuple[RequestSpec, ...],
        since: datetime,
    ) -> _PreparedHistory:
        state = StreamState(source_id=source.id)
        responses: list[HttpResponse] = []
        for request in requests:
            response = self._transport.request(request, source, state)
            raise_for_status(response)
            responses.append(response)

        parsed = adapter.parse_history_responses(source, tuple(responses), since)
        LOGGER.info(
            "operation=history_batch source_id=%s responses=%s candidates=%s",
            source.id,
            len(responses),
            len(parsed.candidates),
        )
        return _PreparedHistory(self._validate_history(source, parsed))

    def _commit_prepared(
        self,
        source: Source,
        run_id: int,
        now: datetime,
        prepared: _PreparedOutcome,
    ) -> IngestionSummary:
        if isinstance(prepared, _PreparedNotModified):
            return self._storage.record_not_modified(
                source.id,
                run_id,
                prepared.etag,
                prepared.last_modified,
                now + timedelta(seconds=source.interval_seconds),
            )
        if isinstance(prepared, _PreparedHistory):
            return self._storage.record_history(source, run_id, prepared.batch)
        return self._storage.record_success(
            source=source,
            fetch_run_id=run_id,
            http_status=prepared.http_status,
            batch=prepared.batch,
            response_etag=prepared.etag,
            response_last_modified=prepared.last_modified,
            next_run_at=now + timedelta(seconds=source.interval_seconds),
            observed_at=prepared.observed_at,
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

    def _record_preparation_failure(
        self,
        source: Source,
        run_id: int,
        attempted_at: datetime,
        error: Exception,
    ) -> None:
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
        message = _error_message(error)
        try:
            self._storage.record_failure(
                source.id, run_id, error, next_run_at, status, message
            )
        except Exception as storage_error:
            self._log_failure(source.id, run_id, storage_error, next_run_at)
            raise
        self._log_failure(source.id, run_id, error, next_run_at, message)

    @staticmethod
    def _log_failure(
        source_id: str,
        run_id: int,
        error: Exception,
        next_run_at: datetime,
        message: str | None = None,
    ) -> None:
        message = message or _error_message(error)
        context = (
            source_id,
            run_id,
            type(error).__name__,
            message,
            next_run_at.isoformat(),
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


def _error_message(error: Exception) -> str:
    return " ".join(str(error).split())[:ERROR_MESSAGE_LIMIT]
