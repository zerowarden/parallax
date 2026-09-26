"""Compatibility façade over the storage workflows, sharing one connection."""

from __future__ import annotations

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from parallax.config import Source
from parallax.domain import (
    AnalysisItem,
    BrowseView,
    ChangeEvent,
    HistoryOutcome,
    IngestionSummary,
    ObservedBatch,
    StreamState,
    ValidatedBatch,
)
from parallax.read_models import FetchRunRow, HeadlineRow
from parallax.storage import catalog, changes, foundation, queries, writer
from parallax.storage.foundation import SCHEMA_VERSION as SCHEMA_VERSION
from parallax.storage.foundation import (
    CollectorAlreadyRunningError as CollectorAlreadyRunningError,
)
from parallax.storage.foundation import collector_lock as collector_lock


class Storage:
    """Own one SQLite connection and delegate to cohesive storage operations.

    Collection callers must hold ``collector_lock`` for the entire workflow,
    including due-state reads and HTTP preparation. ``Runtime`` owns this lock.
    Read-only archives and independent consumer checkpoints need no collector lock.
    """

    def __init__(self, database_path: Path, *, read_only: bool = False) -> None:
        self.database_path = database_path
        self._connection = foundation.open_connection(
            database_path, read_only=read_only
        )

    def close(self) -> None:
        foundation.close_connection(self._connection, self.database_path)

    def initialize(self) -> None:
        return foundation.initialize(self._connection)

    def sync_sources(self, sources: Sequence[Source]) -> None:
        return catalog.sync_sources(self._connection, sources)

    def sources(self) -> tuple[Source, ...]:
        return catalog.sources(self._connection)

    def recover_abandoned_runs(self) -> None:
        return writer.recover_abandoned_runs(self._connection)

    def get_stream_state(self, source_id: str) -> StreamState:
        return queries.get_stream_state(self._connection, source_id)

    def last_content_change_at(self, source_id: str) -> datetime | None:
        return queries.last_content_change_at(self._connection, source_id)

    def start_fetch_run(self, source_id: str) -> int:
        return writer.start_fetch_run(self._connection, source_id)

    def record_success(
        self,
        source: Source,
        fetch_run_id: int,
        http_status: int | None,
        batch: ValidatedBatch,
        response_etag: str | None,
        response_last_modified: str | None,
        next_run_at: datetime,
        observed_at: datetime,
        request_identity: str | None = None,
    ) -> IngestionSummary:
        return writer.record_success(
            self._connection,
            source,
            fetch_run_id,
            http_status,
            batch,
            response_etag,
            response_last_modified,
            next_run_at,
            observed_at,
            request_identity,
        )

    def record_history(
        self,
        source: Source,
        fetch_run_id: int,
        batch: ObservedBatch,
        outcome: HistoryOutcome | None = None,
    ) -> IngestionSummary:
        return writer.record_history(
            self._connection, source, fetch_run_id, batch, outcome
        )

    def record_not_modified(
        self,
        source_id: str,
        fetch_run_id: int,
        response_etag: str | None,
        response_last_modified: str | None,
        next_run_at: datetime,
        request_identity: str | None = None,
    ) -> IngestionSummary:
        return writer.record_not_modified(
            self._connection,
            source_id,
            fetch_run_id,
            response_etag,
            response_last_modified,
            next_run_at,
            request_identity,
        )

    def record_failure(
        self,
        source_id: str,
        fetch_run_id: int,
        error: BaseException,
        next_run_at: datetime,
        http_status: int | None = None,
        error_message: str | None = None,
    ) -> None:
        return writer.record_failure(
            self._connection,
            source_id,
            fetch_run_id,
            error,
            next_run_at,
            http_status,
            error_message,
        )

    def latest_snapshot_headlines(
        self,
        limit_per_source: int | None = None,
        source_id: str | None = None,
        *,
        enabled_only: bool = True,
    ) -> list[HeadlineRow]:
        return queries.latest_snapshot_headlines(
            self._connection, limit_per_source, source_id, enabled_only=enabled_only
        )

    def browse_headlines(
        self,
        *,
        view: BrowseView,
        since: datetime,
        until: datetime,
        query: str | None = None,
        source_id: str | None = None,
        limit: int,
        offset: int,
    ) -> list[HeadlineRow]:
        return queries.browse_headlines(
            self._connection,
            view=view,
            since=since,
            until=until,
            query=query,
            source_id=source_id,
            limit=limit,
            offset=offset,
        )

    def count_browse_headlines(
        self,
        *,
        view: BrowseView,
        since: datetime,
        until: datetime,
        query: str | None = None,
        source_id: str | None = None,
    ) -> int:
        return queries.count_browse_headlines(
            self._connection,
            view=view,
            since=since,
            until=until,
            query=query,
            source_id=source_id,
        )

    def recent_fetch_runs(self, limit: int = 20) -> list[FetchRunRow]:
        return queries.recent_fetch_runs(self._connection, limit)

    def latest_fetch_runs(self, enabled_only: bool = True) -> list[FetchRunRow]:
        return queries.latest_fetch_runs(self._connection, enabled_only)

    def stream_states(self, enabled_only: bool = True) -> list[StreamState]:
        return queries.stream_states(self._connection, enabled_only)

    def changes_after(self, seq: int, limit: int = 100) -> list[ChangeEvent]:
        return changes.changes_after(self._connection, seq, limit)

    def get_consumer_checkpoint(self, consumer_name: str) -> int:
        return changes.get_consumer_checkpoint(self._connection, consumer_name)

    def set_consumer_checkpoint(self, consumer_name: str, last_seq: int) -> None:
        return changes.set_consumer_checkpoint(
            self._connection, consumer_name, last_seq
        )

    def hydrate_analysis_items(
        self,
        events: Sequence[ChangeEvent],
    ) -> list[AnalysisItem]:
        return changes.hydrate_analysis_items(self._connection, events)

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        with foundation.transaction(self._connection):
            yield
