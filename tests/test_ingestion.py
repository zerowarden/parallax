from __future__ import annotations

import threading
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from parallax.adapters import AdapterRegistry
from parallax.adapters.base import (
    Adapter,
    CompleteStep,
    ContinueStep,
)
from parallax.adapters.common.http import cookie_header
from parallax.adapters.execution import MAX_ADAPTER_STEPS
from parallax.config import IngestionConfig, Source, ValidationConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
    StreamState,
    ValidatedBatch,
)
from parallax.ingest import IngestionService
from parallax.storage import Storage
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


class FixedAdapterLookup:
    def __init__(self, adapters: dict[str, Adapter]) -> None:
        self._adapters = adapters

    def get(self, name: str) -> Adapter:
        return self._adapters[name]


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
            FixedAdapterLookup({"batch": BatchAdapter()}),
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
    assert {run.error_type for run in runs} == {"InterruptedError"}


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
        adapters=FixedAdapterLookup({"failing": FailingAdapter()}),
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
        adapters=FixedAdapterLookup({source.endpoint.adapter: adapter}),
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
    assert rows[0].first_seen_at == (OBSERVED_AT + timedelta(minutes=1)).isoformat()
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

    def build_history_requests(
        self,
        source: Source,
        since: datetime,
    ) -> tuple[RequestSpec, ...]:
        return (RequestSpec(method="GET", url="https://example.com/history"),)

    def parse_history_responses(
        self,
        source: Source,
        responses: tuple[HttpResponse, ...],
        since: datetime,
    ) -> ParsedBatch:
        return ParsedBatch(
            candidates=(
                HeadlineCandidate(
                    title="Backfilled headline",
                    url="https://example.com/old",
                    external_id="old",
                    published_at=since,
                ),
            )
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
        adapters=FixedAdapterLookup({source.endpoint.adapter: HistoryFixtureAdapter()}),
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


def test_history_fetch_falls_back_when_unsupported(
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

    assert summary.status == "success"
    assert transport.requested == ["https://example.com/rss.xml"]
    assert rows
    assert state.last_success_at is not None
