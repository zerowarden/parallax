from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

from parallax.config import SchedulerConfig, SourceConfig
from parallax.domain import IngestionBatchResult, StreamState
from parallax.ingest import IngestionService
from parallax.registry import SourceRegistry
from parallax.scheduler import Scheduler
from parallax.storage import Storage


def _source(source_id: str) -> SourceConfig:
    return SourceConfig(
        id=source_id,
        name=source_id,
        region="US",
        language="en-US",
        adapter="fixture",
        url=f"https://example.test/{source_id}",
    )


class _Registry:
    def __init__(self, sources: list[SourceConfig]) -> None:
        self.sources = sources

    def enabled(self) -> list[SourceConfig]:
        return self.sources


class _Storage:
    def __init__(self, states: dict[str, StreamState]) -> None:
        self.states = states

    def get_stream_state(self, source_id: str) -> StreamState:
        return self.states[source_id]


class _Ingestion:
    def __init__(self) -> None:
        self.calls: list[list[SourceConfig]] = []

    def fetch_sources(self, sources: list[SourceConfig]) -> IngestionBatchResult:
        self.calls.append(sources)
        return IngestionBatchResult((), ())


def test_scheduler_dispatches_only_due_sources() -> None:
    due = _source("due")
    future = _source("future")
    ingestion = _Ingestion()
    scheduler = Scheduler(
        cast(SourceRegistry, _Registry([due, future])),
        cast(
            Storage,
            _Storage(
                {
                    due.id: StreamState(due.id),
                    future.id: StreamState(
                        future.id,
                        next_run_at=datetime.now(UTC) + timedelta(hours=1),
                    ),
                }
            ),
        ),
        cast(IngestionService, ingestion),
        SchedulerConfig(),
    )

    assert scheduler.run_due_once() == 1
    assert ingestion.calls == [[due]]


def test_scheduler_does_not_dispatch_when_nothing_is_due() -> None:
    source = _source("future")
    ingestion = _Ingestion()
    scheduler = Scheduler(
        cast(SourceRegistry, _Registry([source])),
        cast(
            Storage,
            _Storage(
                {
                    source.id: StreamState(
                        source.id, next_run_at=datetime.now(UTC) + timedelta(hours=1)
                    )
                }
            ),
        ),
        cast(IngestionService, ingestion),
        SchedulerConfig(),
    )

    assert scheduler.run_due_once() == 0
    assert ingestion.calls == []
