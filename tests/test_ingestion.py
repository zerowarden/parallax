from __future__ import annotations

import sqlite3
import threading
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from parallax.adapters.base import (
    Adapter,
    CompleteStep,
    ContinueStep,
    HistoryPlan,
)
from parallax.adapters.common.http import cookie_header
from parallax.adapters.execution import MAX_ADAPTER_STEPS
from parallax.adapters.registry import AdapterRegistry
from parallax.config import IngestionConfig, Source, ValidationConfig
from parallax.domain import (
    HeadlineCandidate,
    HistoryPage,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
    RunKind,
    StreamState,
    ValidatedBatch,
)
from parallax.ingest import IngestionService
from parallax.storage import Storage, foundation
from parallax.validation import BatchValidator
from source_factory import make_source

OBSERVED_AT = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)


class FakeTransport:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def request(
        self,
        spec: RequestSpec,
        source: Source,
        state: StreamState,
    ) -> HttpResponse:
        return HttpResponse(
            status_code=200,
            url=spec.url,
            headers={"etag": '"fixture-v1"'},
            content=self.content,
            observed_at=OBSERVED_AT,
        )


def test_http_response_rejects_naive_observation_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        HttpResponse(
            status_code=200,
            url="https://example.com/1",
            headers={},
            content=b"",
            observed_at=datetime(2026, 9, 18, 12, 0),
        )


def test_end_to_end_fetch_parse_validate_store(
    fixtures_dir: Path,
    tmp_path: Path,
):
    source = make_source(
        id="fixture-rss", adapter="rss", url="https://example.com/rss.xml"
    )
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage=storage,
        transport=FakeTransport((fixtures_dir / "rss" / "feed.xml").read_bytes()),
        adapters=AdapterRegistry(),
        validator=BatchValidator(ValidationConfig()),
        ingestion_config=IngestionConfig(),
    )

    summary = service.fetch_source(source)
    rows = storage.latest_snapshot_headlines(limit_per_source=10)
    state = storage.get_stream_state(source.id)

    assert summary.status == "success"
    assert summary.item_count == 2
    assert len(rows) == 2
    assert state.etag == '"fixture-v1"'
    assert state.last_success_at is not None
    assert state.last_success_at <= datetime.now(UTC)
    storage.close()


class MappingTransport:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads
        self.requested: list[str] = []

    def request(
        self,
        spec: RequestSpec,
        source: Source,
        state: StreamState,
    ) -> HttpResponse:
        self.requested.append(spec.url)
        return HttpResponse(
            status_code=200,
            url=spec.url,
            headers={},
            content=self.payloads[spec.url],
            observed_at=OBSERVED_AT,
        )


def test_multi_request_fetch_combines_responses_without_network(
    fixtures_dir: Path,
    tmp_path: Path,
):
    source = make_source(
        id="v2ex-share", adapter="v2ex_share", url="https://www.v2ex.com"
    )
    routes = ("create", "ideas", "programmer", "share")
    payloads = {
        f"https://www.v2ex.com/feed/{route}.json": (
            fixtures_dir / "v2ex" / f"{route}.json"
        ).read_bytes()
        for route in routes
    }
    transport = MappingTransport(payloads)
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage=storage,
        transport=transport,
        adapters=AdapterRegistry(),
        validator=BatchValidator(ValidationConfig()),
        ingestion_config=IngestionConfig(),
    )

    summary = service.fetch_source(source)
    rows = storage.latest_snapshot_headlines(source_id=source.id)
    state = storage.get_stream_state(source.id)
    storage.close()

    assert transport.requested == list(payloads)
    assert summary.status == "success"
    assert summary.item_count == 12
    assert len(rows) == 12
    assert rows[0].title == "把走路时想到的东西直接变成 Obsidian 里的 Markdown 笔记"
    assert state.etag is None


class FixedAdapterResolver:
    def __init__(self, adapters: dict[str, Adapter]) -> None:
        self._adapters = adapters

    def resolve_source(self, source: Source) -> Adapter:
        return self._adapters[source.endpoint.adapter]


class BatchAdapter:
    def build_request(self, source: Source) -> RequestSpec:
        return RequestSpec(method="GET", url=source.endpoint.url)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        return ParsedBatch(
            (HeadlineCandidate(f"title-{source.id}", source.endpoint.url, source.id),)
        )


class BatchTransport:
    def __init__(self, fail_source_id: str | None = None) -> None:
        self.fail_source_id = fail_source_id
        self.active = 0
        self.maximum_active = 0
        self._lock = threading.Lock()
        self._barrier = threading.Barrier(2)

    def request(
        self, spec: RequestSpec, source: Source, state: StreamState
    ) -> HttpResponse:
        if source.id == self.fail_source_id:
            raise RuntimeError("fixture failure")
        with self._lock:
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
        try:
            if self.fail_source_id is None and source.id in {"batch-one", "batch-two"}:
                self._barrier.wait(timeout=2)
            return HttpResponse(200, spec.url, {}, b"{}", OBSERVED_AT)
        finally:
            with self._lock:
                self.active -= 1


def _batch_sources() -> list[Source]:
    return [
        make_source(
            id=source_id, adapter="batch", url=f"https://example.test/{source_id}"
        )
        for source_id in ("batch-one", "batch-two", "batch-three")
    ]


def _batch_service(
    tmp_path: Path, sources: list[Source], transport: BatchTransport
) -> tuple[IngestionService, Storage]:
    storage = Storage(tmp_path / "batch.db")
    storage.initialize()
    storage.sync_sources(sources)
    return (
        IngestionService(
            storage,
            transport,
            FixedAdapterResolver({"batch": BatchAdapter()}),
            BatchValidator(ValidationConfig()),
            IngestionConfig(max_concurrent_sources=2),
        ),
        storage,
    )


def test_batch_fetches_concurrently_and_commits_on_caller_thread(
    tmp_path: Path,
) -> None:
    sources = _batch_sources()
    transport = BatchTransport()
    service, storage = _batch_service(tmp_path, sources, transport)
    commits: list[int] = []
    original_commit = storage.record_success

    def record_success(
        source: Source,
        fetch_run_id: int,
        http_status: int | None,
        batch: ValidatedBatch,
        response_etag: str | None,
        response_last_modified: str | None,
        next_run_at: datetime,
        observed_at: datetime,
        request_identity: str | None = None,
    ):
        commits.append(threading.get_ident())
        return original_commit(
            source,
            fetch_run_id,
            http_status,
            batch,
            response_etag,
            response_last_modified,
            next_run_at,
            observed_at,
        )

    storage.record_success = record_success  # type: ignore[method-assign]
    result = service.fetch_sources(sources)
    storage.close()

    assert [summary.source_id for summary in result.summaries] == [
        source.id for source in sources
    ]
    assert not result.failures
    assert transport.maximum_active == 2
    assert commits == [threading.get_ident()] * 3


def test_batch_returns_failure_without_preventing_other_commits(tmp_path: Path) -> None:
    sources = _batch_sources()
    service, storage = _batch_service(tmp_path, sources, BatchTransport("batch-two"))
    result = service.fetch_sources(sources)
    storage.close()

    assert [summary.source_id for summary in result.summaries] == [
        "batch-one",
        "batch-three",
    ]
    assert [(failure.source_id, failure.error_type) for failure in result.failures] == [
        ("batch-two", "RuntimeError")
    ]


def test_batch_records_uncommitted_runs_when_interrupted(tmp_path: Path) -> None:
    sources = _batch_sources()
    service, storage = _batch_service(tmp_path, sources, BatchTransport())

    def interrupted(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt

    storage.record_success = interrupted  # type: ignore[method-assign]

    with pytest.raises(KeyboardInterrupt):
        service.fetch_sources(sources)

    runs = storage.latest_fetch_runs(enabled_only=False)
    storage.close()

    assert len(runs) == 2
    assert {run.status for run in runs} == {"failed"}
    assert {run.error_type for run in runs} == {"KeyboardInterrupt"}


def test_batch_rolls_back_interrupted_transaction_before_cleanup(
    tmp_path: Path,
) -> None:
    sources = _batch_sources()
    service, storage = _batch_service(tmp_path, sources, BatchTransport())

    def interrupted(*args: object, **kwargs: object) -> None:
        with foundation.transaction(storage._connection):
            storage._connection.execute("""
                INSERT INTO consumer_checkpoints(consumer_name, last_seq, updated_at)
                VALUES ('interrupted', 1, '2026-01-01T00:00:00+00:00')
                """)
            raise KeyboardInterrupt

    storage.record_success = interrupted  # type: ignore[method-assign]

    with pytest.raises(KeyboardInterrupt):
        service.fetch_sources(sources)

    runs = storage.latest_fetch_runs(enabled_only=False)
    checkpoint = storage.get_consumer_checkpoint("interrupted")
    storage.close()

    assert checkpoint == 0
    assert len(runs) == 2
    assert {run.status for run in runs} == {"failed"}
    assert {run.error_type for run in runs} == {"KeyboardInterrupt"}


def test_fetch_source_finalizes_run_when_commit_fails(tmp_path: Path) -> None:
    source = make_source(
        id="batch-one", adapter="batch", url="https://example.test/batch-one"
    )
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage=storage,
        transport=MappingTransport({source.endpoint.url: b"{}"}),
        adapters=FixedAdapterResolver({"batch": BatchAdapter()}),
        validator=BatchValidator(ValidationConfig()),
        ingestion_config=IngestionConfig(),
    )

    def failing_commit(*args: object, **kwargs: object) -> None:
        raise sqlite3.OperationalError("disk I/O error")

    storage.record_success = failing_commit  # type: ignore[method-assign]

    with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
        service.fetch_source(source)

    runs = storage.latest_fetch_runs(enabled_only=False)
    storage.close()

    assert runs[0].status == "failed"
    assert runs[0].error_type == "OperationalError"
    assert runs[0].error_message == "disk I/O error"


def test_fetch_source_keeps_original_error_when_recording_fails(
    tmp_path: Path,
) -> None:
    source = make_source(
        id="failing-fixture", adapter="failing", url="https://example.com/failing"
    )
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage=storage,
        transport=MappingTransport({source.endpoint.url: b"{}"}),
        adapters=FixedAdapterResolver({"failing": FailingAdapter()}),
        validator=BatchValidator(ValidationConfig()),
        ingestion_config=IngestionConfig(),
    )

    def failing_record(*args: object, **kwargs: object) -> None:
        raise sqlite3.OperationalError("database is gone")

    storage.record_failure = failing_record  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="schema drift"):
        service.fetch_source(source)

    storage.close()


def test_batch_finalizes_started_runs_when_submission_fails(
    tmp_path: Path,
) -> None:
    sources = [
        make_source(
            id=f"submit-{index}",
            adapter="batch",
            url=f"https://example.test/submit-{index}",
        )
        for index in range(3)
    ]
    service, storage = _batch_service(tmp_path, sources, BatchTransport())
    original_start = storage.start_fetch_run
    submissions = 0

    def failing_start(source_id: str, run_kind: RunKind) -> int:
        nonlocal submissions
        submissions += 1
        if submissions == 2:
            raise sqlite3.OperationalError("database is locked")
        return original_start(source_id, run_kind)

    storage.start_fetch_run = failing_start  # type: ignore[method-assign]

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        service.fetch_sources(sources)

    runs = storage.latest_fetch_runs(enabled_only=False)
    storage.close()

    assert submissions == 2
    assert len(runs) == 1
    assert runs[0].status == "failed"
    assert runs[0].error_type == "OperationalError"


@pytest.mark.parametrize("failure", [sqlite3.OperationalError, sqlite3.IntegrityError])
def test_batch_records_commit_failure_without_relabelling(
    tmp_path: Path,
    failure: type[sqlite3.Error],
) -> None:
    sources = _batch_sources()
    service, storage = _batch_service(tmp_path, sources, BatchTransport())

    def failing_commit(*args: object, **kwargs: object) -> None:
        with foundation.transaction(storage._connection):
            storage._connection.execute("DELETE FROM source_surfaces")
            raise failure("database failure")

    storage.record_success = failing_commit  # type: ignore[method-assign]
    with pytest.raises(failure, match="database failure"):
        service.fetch_sources(sources)

    runs = storage.latest_fetch_runs(enabled_only=False)
    assert len(runs) == 2  # No replacement work is submitted after failure.
    assert {run.status for run in runs} == {"failed"}
    assert {run.error_type for run in runs} == {failure.__name__}
    assert not storage._connection.in_transaction
    assert storage._connection.execute(
        "SELECT COUNT(*) FROM source_surfaces"
    ).fetchone()[0] == len(sources)
    storage.close()


class NotModifiedTransport:
    def request(
        self,
        spec: RequestSpec,
        source: Source,
        state: StreamState,
    ) -> HttpResponse:
        return HttpResponse(
            status_code=304,
            url=spec.url,
            headers={"etag": '"fixture-v1"'},
            content=b"",
            observed_at=OBSERVED_AT,
        )


def test_not_modified_records_no_new_snapshot(
    fixtures_dir: Path, tmp_path: Path
) -> None:
    source = make_source(
        id="fixture-rss", adapter="rss", url="https://example.com/rss.xml"
    )
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage=storage,
        transport=FakeTransport((fixtures_dir / "rss" / "feed.xml").read_bytes()),
        adapters=AdapterRegistry(),
        validator=BatchValidator(ValidationConfig()),
        ingestion_config=IngestionConfig(),
    )
    service.fetch_source(source)

    not_modified_service = IngestionService(
        storage=storage,
        transport=NotModifiedTransport(),
        adapters=AdapterRegistry(),
        validator=BatchValidator(ValidationConfig()),
        ingestion_config=IngestionConfig(),
    )
    summary = not_modified_service.fetch_source(source)

    rows = storage.latest_snapshot_headlines()
    runs = storage.recent_fetch_runs()
    state = storage.get_stream_state(source.id)
    storage.close()

    assert summary.status == "not_modified"
    assert len(rows) == 2
    assert runs[0].status == "not_modified"
    assert state.last_success_at is not None


class FailingAdapter:
    def build_request(self, source: Source) -> RequestSpec:
        return RequestSpec(method="GET", url=source.endpoint.url)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        raise ValueError("upstream schema drift")


def test_parse_failure_creates_no_snapshot_or_success(tmp_path: Path) -> None:
    source = make_source(
        id="failing-fixture", adapter="failing", url="https://example.com/failing"
    )
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage=storage,
        transport=MappingTransport({source.endpoint.url: b"{}"}),
        adapters=FixedAdapterResolver({"failing": FailingAdapter()}),
        validator=BatchValidator(ValidationConfig()),
        ingestion_config=IngestionConfig(),
    )

    with pytest.raises(ValueError, match="schema drift"):
        service.fetch_source(source)

    rows = storage.latest_snapshot_headlines()
    runs = storage.recent_fetch_runs()
    state = storage.get_stream_state(source.id)
    changes = storage.changes_after(0)
    storage.close()

    assert rows == []
    assert runs[0].status == "failed"
    assert runs[0].error_type == "ValueError"
    assert state.last_success_at is None
    assert changes == []


class SteppedFixtureAdapter:
    def first_step(self, source: Source) -> ContinueStep:
        return ContinueStep(
            request=RequestSpec(method="GET", url="https://example.com/step-1")
        )

    def next_step(
        self,
        source: Source,
        response: HttpResponse,
        context: Mapping[str, object],
    ) -> ContinueStep | CompleteStep:
        if response.url.endswith("/step-1"):
            return ContinueStep(
                request=RequestSpec(
                    method="GET",
                    url="https://example.com/step-2",
                    headers={"Cookie": cookie_header(response.cookies)},
                ),
                context={"cookies": dict(response.cookies)},
            )
        return CompleteStep(
            batch=ParsedBatch(
                candidates=(
                    HeadlineCandidate(
                        title="Stepped headline",
                        url="https://example.com/article",
                        external_id="stepped-1",
                        position=1,
                    ),
                )
            )
        )


class UnboundedSteppedAdapter:
    def first_step(self, source: Source) -> ContinueStep:
        return ContinueStep(
            request=RequestSpec(method="GET", url="https://example.com/loop")
        )

    def next_step(
        self,
        source: Source,
        response: HttpResponse,
        context: Mapping[str, object],
    ) -> ContinueStep:
        return ContinueStep(
            request=RequestSpec(method="GET", url="https://example.com/loop")
        )


class QueuedTransport:
    def __init__(self, responses: list[HttpResponse]) -> None:
        self._responses = responses
        self.requests: list[RequestSpec] = []

    def request(
        self,
        spec: RequestSpec,
        source: Source,
        state: StreamState,
    ) -> HttpResponse:
        self.requests.append(spec)
        return self._responses.pop(0)


def _stepped_service(
    tmp_path: Path,
    source: Source,
    transport: QueuedTransport,
    adapter: Adapter,
) -> tuple[IngestionService, Storage]:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage=storage,
        transport=transport,
        adapters=FixedAdapterResolver({source.endpoint.adapter: adapter}),
        validator=BatchValidator(ValidationConfig()),
        ingestion_config=IngestionConfig(),
    )
    return service, storage


def test_stepped_fetch_drives_sequence_offline(tmp_path: Path):
    source = make_source(
        id="stepped-fixture", adapter="stepped", url="https://example.com/step-1"
    )
    transport = QueuedTransport(
        [
            HttpResponse(
                status_code=200,
                url="https://example.com/step-1",
                headers={},
                content=b"",
                observed_at=OBSERVED_AT,
                cookies={"session": "abc"},
            ),
            HttpResponse(
                status_code=200,
                url="https://example.com/step-2",
                headers={},
                content=b"{}",
                observed_at=OBSERVED_AT + timedelta(minutes=1),
            ),
        ]
    )
    service, storage = _stepped_service(
        tmp_path, source, transport, SteppedFixtureAdapter()
    )

    summary = service.fetch_source(source)
    rows = storage.latest_snapshot_headlines(source_id=source.id)
    state = storage.get_stream_state(source.id)
    storage.close()

    assert [request.url for request in transport.requests] == [
        "https://example.com/step-1",
        "https://example.com/step-2",
    ]
    assert transport.requests[1].headers["Cookie"] == "session=abc"
    assert summary.status == "success"
    assert summary.item_count == 1
    assert len(rows) == 1
    assert rows[0].title == "Stepped headline"
    assert rows[0].first_seen_at == (OBSERVED_AT + timedelta(minutes=1))
    assert state.etag is None


def test_stepped_fetch_rejects_unbounded_sequence(tmp_path: Path):
    source = make_source(
        id="loop-fixture", adapter="unbounded", url="https://example.com/loop"
    )
    transport = QueuedTransport(
        [
            HttpResponse(
                status_code=200,
                url="https://example.com/loop",
                headers={},
                content=b"",
                observed_at=OBSERVED_AT,
            )
            for _ in range(MAX_ADAPTER_STEPS)
        ]
    )
    service, storage = _stepped_service(
        tmp_path, source, transport, UnboundedSteppedAdapter()
    )

    with pytest.raises(RuntimeError, match="exceeded 5 steps"):
        service.fetch_source(source)

    runs = storage.latest_fetch_runs(enabled_only=False)
    storage.close()

    assert len(transport.requests) == MAX_ADAPTER_STEPS
    assert runs[0].status == "failed"


class HistoryFixtureAdapter:
    def build_request(self, source: Source) -> RequestSpec:
        return RequestSpec(method="GET", url="https://example.com/current")

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        return ParsedBatch(
            candidates=(
                HeadlineCandidate(
                    title="Current headline",
                    url="https://example.com/current",
                    external_id="current",
                ),
            )
        )

    def build_history_plan(
        self,
        source: Source,
        since: datetime,
    ) -> HistoryPlan:
        return HistoryPlan(
            (RequestSpec(method="GET", url="https://example.com/history"),), 1000
        )

    def parse_history_page(
        self,
        source: Source,
        response: HttpResponse,
        since: datetime,
    ) -> HistoryPage:
        return HistoryPage(
            ParsedBatch(
                candidates=(
                    HeadlineCandidate(
                        title="Backfilled headline",
                        url="https://example.com/old",
                        external_id="old",
                        published_at=since,
                    ),
                )
            ),
            exhausted=True,
        )


def test_history_fetch_records_without_snapshot(tmp_path: Path):
    source = make_source(
        id="history-fixture", adapter="history", url="https://example.com/current"
    )
    transport = MappingTransport(
        {
            "https://example.com/current": b"{}",
            "https://example.com/history": b"{}",
        }
    )
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage=storage,
        transport=transport,
        adapters=FixedAdapterResolver(
            {source.endpoint.adapter: HistoryFixtureAdapter()}
        ),
        validator=BatchValidator(ValidationConfig()),
        ingestion_config=IngestionConfig(),
    )

    summary = service.fetch_source(source, since=datetime(2026, 9, 1, tzinfo=UTC))

    runs = storage.recent_fetch_runs()
    rows = storage.latest_snapshot_headlines()
    state = storage.get_stream_state(source.id)
    changes = storage.changes_after(0)
    storage.close()

    assert summary.status == "history"
    assert summary.new_item_count == 1
    assert transport.requested == ["https://example.com/history"]
    assert runs[0].status == "history"
    assert rows == []
    assert state.last_success_at is None
    assert {event.event_type for event in changes} == {"item_created"}


def test_history_fetch_reports_unsupported_without_refresh(
    fixtures_dir: Path,
    tmp_path: Path,
):
    source = make_source(
        id="fixture-rss", adapter="rss", url="https://example.com/rss.xml"
    )
    transport = MappingTransport(
        {
            "https://example.com/rss.xml": (
                fixtures_dir / "rss" / "feed.xml"
            ).read_bytes()
        }
    )
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage=storage,
        transport=transport,
        adapters=AdapterRegistry(),
        validator=BatchValidator(ValidationConfig()),
        ingestion_config=IngestionConfig(),
    )

    summary = service.fetch_source(source, since=datetime(2026, 9, 1, tzinfo=UTC))

    rows = storage.latest_snapshot_headlines()
    state = storage.get_stream_state(source.id)
    storage.close()

    assert summary.status == "unsupported"
    assert summary.history is not None
    assert summary.history.status == "unsupported"
    assert summary.history.pages_requested == 0
    assert transport.requested == []
    assert rows == []
    assert state.last_success_at is None


@pytest.mark.parametrize(
    "budget,empty_page,expected_pages,expected_items,status,reason",
    [
        (3, 0, 2, 3, "truncated", "item_budget"),
        (20, 0, 3, 6, "truncated", "page_budget"),
        (20, 2, 2, 2, "exhausted", "upstream_exhausted"),
    ],
)
def test_now_history_is_incremental_and_separate_from_live_cap(
    tmp_path: Path,
    budget: int,
    empty_page: int,
    expected_pages: int,
    expected_items: int,
    status: str,
    reason: str,
) -> None:
    import json

    source = make_source(
        adapter="now_news",
        max_items=1,
        options={
            "history_max_pages": 3,
            "history_max_items": budget,
            "history_page_size": 2,
        },
    )
    requested: list[int] = []

    class PageTransport:
        def request(
            self, spec: RequestSpec, source: Source, state: StreamState
        ) -> HttpResponse:
            page = int(spec.params["pageNo"])
            requested.append(page)
            assert spec.params["pageSize"] == "2"
            entries = (
                []
                if page == empty_page
                else [
                    {
                        "newsId": f"{page}-{i}",
                        "title": f"Headline {page}-{i}",
                        "publishDate": int(OBSERVED_AT.timestamp() * 1000),
                    }
                    for i in range(2)
                ]
            )
            return HttpResponse(
                200, spec.url, {}, json.dumps(entries).encode(), OBSERVED_AT
            )

    storage = Storage(tmp_path / "history.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage,
        PageTransport(),
        AdapterRegistry(),
        BatchValidator(ValidationConfig()),
        IngestionConfig(),
    )
    since = OBSERVED_AT - timedelta(days=1)
    summary = service.fetch_source(source, since=since)
    assert requested == list(range(1, expected_pages + 1))
    assert summary.item_count == expected_items
    assert summary.history is not None
    assert summary.history.status == status
    assert summary.history.stop_reason == reason
    assert summary.history.requested_since == since
    assert summary.history.observed_since == OBSERVED_AT
    assert summary.history.observed_until == OBSERVED_AT
    assert summary.history.items_accepted == expected_items
    assert storage.latest_snapshot_headlines() == []
    assert storage.get_stream_state(source.id).last_success_at is None
    stored = storage.recent_fetch_runs()[0].history_json
    assert (
        stored is not None and json.loads(stored)["pages_requested"] == expected_pages
    )
    storage.close()


def test_history_old_ranked_page_does_not_imply_exhaustion(tmp_path: Path) -> None:
    import json

    source = make_source(
        adapter="now_news", max_items=1, options={"history_max_pages": 2}
    )
    pages: list[int] = []

    class RankedTransport:
        def request(
            self, spec: RequestSpec, source: Source, state: StreamState
        ) -> HttpResponse:
            page = int(spec.params["pageNo"])
            pages.append(page)
            published = OBSERVED_AT - timedelta(days=10) if page == 1 else OBSERVED_AT
            entries = [
                {
                    "newsId": str(page),
                    "title": "Headline",
                    "publishDate": int(published.timestamp() * 1000),
                }
            ]
            return HttpResponse(
                200, spec.url, {}, json.dumps(entries).encode(), OBSERVED_AT
            )

    storage = Storage(tmp_path / "history.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage,
        RankedTransport(),
        AdapterRegistry(),
        BatchValidator(ValidationConfig()),
        IngestionConfig(),
    )
    result = service.fetch_source(source, since=OBSERVED_AT - timedelta(days=1))
    assert pages == [1, 2]
    assert result.item_count == 1
    assert result.history is not None and result.history.status == "truncated"
    storage.close()


def test_batch_finalizes_run_when_initial_executor_submission_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sources = _batch_sources()
    service, storage = _batch_service(tmp_path, sources, BatchTransport())

    def cannot_submit(*args: object, **kwargs: object) -> None:
        raise RuntimeError("executor cannot submit")

    monkeypatch.setattr("parallax.ingest.ThreadPoolExecutor.submit", cannot_submit)
    with pytest.raises(RuntimeError, match="executor cannot submit"):
        service.fetch_sources(sources)
    runs = storage.recent_fetch_runs()
    assert len(runs) == 1
    assert runs[0].status == "failed"
    assert runs[0].error_type == "RuntimeError"
    storage.close()


def test_effective_request_identity_is_persisted_across_catalog_changes(
    tmp_path: Path,
) -> None:
    import httpx

    from parallax.config import HttpConfig
    from parallax.transport import HttpTransport

    source = make_source(adapter="batch")
    requested: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requested.append(request)
        if request.url.path == "/changed":
            return httpx.Response(200, content=b"{}")
        if "if-none-match" in request.headers:
            return httpx.Response(304)
        return httpx.Response(200, headers={"etag": '"original"'}, content=b"{}")

    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    storage.sync_sources([source])
    with HttpTransport(HttpConfig(), backend=httpx.MockTransport(respond)) as transport:
        service = IngestionService(
            storage,
            transport,
            FixedAdapterResolver({"batch": BatchAdapter()}),
            BatchValidator(ValidationConfig()),
            IngestionConfig(),
        )
        service.fetch_source(source)
        original_state = storage.get_stream_state(source.id)
        assert original_state.request_identity is not None
        assert service.fetch_source(source).status == "not_modified"
        assert storage.get_stream_state(source.id).etag == '"original"'
        changed = source.model_copy(
            update={
                "endpoint": source.endpoint.model_copy(
                    update={"url": "https://example.test/changed"}
                )
            }
        )
        storage.sync_sources([changed])
        assert service.fetch_source(changed).status == "success"
        state = storage.get_stream_state(source.id)
        assert state.etag is None and state.last_modified is None
        assert state.request_identity != original_state.request_identity
        assert "if-none-match" not in requested[-1].headers
    storage.close()


@pytest.mark.parametrize("batch", [False, True])
def test_failed_history_leaves_all_live_state_untouched(
    tmp_path: Path, batch: bool
) -> None:
    source = make_source(adapter="history", url="https://example.com/current")
    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    storage.sync_sources([source])
    service = IngestionService(
        storage,
        FakeTransport(b"{}"),
        FixedAdapterResolver({"history": HistoryFixtureAdapter()}),
        BatchValidator(ValidationConfig()),
        IngestionConfig(),
    )
    try:
        service.fetch_source(source)
        # Preserve a nontrivial retry state as well as successful validators.
        run = storage.start_fetch_run(source.id, "live")
        storage.record_failure(
            source.id, run, RuntimeError("live failure"), OBSERVED_AT
        )
        before = storage.get_stream_state(source.id)
        snapshots = storage.latest_snapshot_headlines()

        class BrokenHistory(HistoryFixtureAdapter):
            def parse_history_page(
                self,
                source: Source,
                response: HttpResponse,
                since: datetime,
            ) -> HistoryPage:
                raise ValueError("history schema changed")

        failing = IngestionService(
            storage,
            FakeTransport(b"{}"),
            FixedAdapterResolver({"history": BrokenHistory()}),
            BatchValidator(ValidationConfig()),
            IngestionConfig(),
        )
        if batch:
            result = failing.fetch_sources(
                [source], since=OBSERVED_AT - timedelta(days=1)
            )
            assert len(result.failures) == 1
        else:
            with pytest.raises(ValueError, match="history schema changed"):
                failing.fetch_source(source, since=OBSERVED_AT - timedelta(days=1))
        assert storage.get_stream_state(source.id) == before
        assert storage.latest_snapshot_headlines() == snapshots
        failed = storage.recent_fetch_runs()[0]
        assert failed.run_kind == "history"
        assert failed.status == "failed"
        assert failed.error_type == "ValueError"
    finally:
        storage.close()
