from __future__ import annotations

import json
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

import httpx
import pytest

from parallax.adapters.cn.douban import DoubanHotMoviesAdapter
from parallax.adapters.cn.weibo import WeiboHotAdapter
from parallax.adapters.common.http import html_request, json_post, json_request
from parallax.adapters.common.parsing import scalar_text
from parallax.adapters.execution import run_adapter_refresh
from parallax.adapters.hk.hk01 import Hk01LatestAdapter
from parallax.adapters.registry import AdapterRegistry
from parallax.adapters.results import FullRefresh
from parallax.config import (
    AuthConfig,
    Endpoint,
    HttpConfig,
    IngestionConfig,
    Source,
    ValidationConfig,
)
from parallax.domain import (
    HeadlineCandidate,
    HistoryOutcome,
    HttpResponse,
    IngestionSummary,
    ObservedBatch,
    ObservedCandidate,
    ParsedBatch,
    RequestSpec,
    StreamState,
)
from parallax.ingest import IngestionService
from parallax.numbers import optional_finite_number, optional_integer
from parallax.parsing import parse_timestamp
from parallax.storage import Storage
from parallax.transport import HttpTransport
from parallax.validation import BatchValidationError, BatchValidator
from parallax.web import is_usable_external_url
from source_factory import make_source

OBSERVED_AT = datetime(2026, 9, 18, 12, tzinfo=UTC)


class PayloadTransport:
    def __init__(self, content: bytes, observed_at: datetime = OBSERVED_AT) -> None:
        self.content = content
        self.observed_at = observed_at

    def request(
        self, spec: RequestSpec, source: Source, state: StreamState
    ) -> HttpResponse:
        return HttpResponse(200, spec.url, {}, self.content, self.observed_at)


def test_history_preserves_observation_order_for_wording_and_urls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources = [
        make_source(id=name, channel_id=name, adapter="hackernews_hot")
        for name in ("older", "newer")
    ]
    newer_committed = Event()
    older_observed = Event()
    commits: list[str] = []

    class DelayedHistory:
        def request(
            self, spec: RequestSpec, source: Source, state: StreamState
        ) -> HttpResponse:
            newer = source.id == "newer"
            title = "Newer" if newer else "Older"
            response = HttpResponse(
                200,
                spec.url,
                {},
                json.dumps(
                    {
                        "hits": [
                            {
                                "objectID": "1",
                                "title": title,
                                "url": f"https://example.test/{title}",
                                "created_at_i": int(OBSERVED_AT.timestamp()),
                            }
                        ],
                        "nbPages": 1,
                    }
                ).encode(),
                OBSERVED_AT + timedelta(microseconds=800 if newer else 100),
            )
            if newer:
                assert older_observed.wait(timeout=3)
            else:
                older_observed.set()
                assert newer_committed.wait(timeout=3)
            return response

    with closing(Storage(tmp_path / "history.db")) as storage:
        storage.initialize()
        storage.sync_sources(sources)
        original_commit = storage.record_history

        def record_history(
            source: Source,
            fetch_run_id: int,
            batch: ObservedBatch,
            outcome: HistoryOutcome | None = None,
        ) -> IngestionSummary:
            result = original_commit(source, fetch_run_id, batch, outcome)
            commits.append(source.id)
            if source.id == "newer":
                newer_committed.set()
            return result

        monkeypatch.setattr(storage, "record_history", record_history)
        service = IngestionService(
            storage,
            DelayedHistory(),
            AdapterRegistry(),
            BatchValidator(ValidationConfig()),
            IngestionConfig(max_concurrent_sources=2),
        )
        result = service.fetch_sources(sources, since=OBSERVED_AT - timedelta(days=1))
        assert not result.failures
        assert commits == ["newer", "older"]
        rows = storage.browse_headlines(
            view="all", since=OBSERVED_AT, until=datetime.now(UTC), limit=10, offset=0
        )
        assert len(rows) == 1
        assert rows[0].title == "Newer"
        assert rows[0].url == "https://example.test/Newer"
        assert rows[0].first_seen_at == OBSERVED_AT + timedelta(microseconds=100)
        assert storage.latest_snapshot_headlines() == []
        assert all(
            storage.get_stream_state(source.id).last_success_at is None
            for source in sources
        )
        assert sum(run.new_version_count for run in storage.recent_fetch_runs()) == 2


def test_observed_candidate_requires_aware_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        ObservedCandidate(
            HeadlineCandidate("Title", "https://example.test/a"), datetime(2026, 9, 18)
        )


def test_history_keeps_each_pages_observation_time_before_empty_page(
    tmp_path: Path,
) -> None:
    source = make_source(adapter="now_news", options={"history_max_pages": 4})

    class Pages:
        def request(
            self, spec: RequestSpec, source: Source, state: StreamState
        ) -> HttpResponse:
            page = int(spec.params["pageNo"])
            entries = (
                []
                if page == 4
                else [
                    {
                        "newsId": 1 if page == 3 else page,
                        "title": f"Page {page}",
                        "publishDate": int(OBSERVED_AT.timestamp() * 1000),
                    }
                ]
            )
            return HttpResponse(
                200,
                spec.url,
                {},
                json.dumps(entries).encode(),
                OBSERVED_AT + timedelta(microseconds=page),
            )

    storage = Storage(tmp_path / "pages.db")
    storage.initialize()
    storage.sync_sources((source,))
    try:
        service = IngestionService(
            storage,
            Pages(),
            AdapterRegistry(),
            BatchValidator(ValidationConfig()),
            IngestionConfig(),
        )
        result = service.fetch_source(source, since=OBSERVED_AT - timedelta(days=1))
        rows = storage.browse_headlines(
            view="all", since=OBSERVED_AT, until=datetime.now(UTC), limit=10, offset=0
        )
        assert {row.title: row.first_seen_at for row in rows} == {
            "Page 1": OBSERVED_AT + timedelta(microseconds=1),
            "Page 2": OBSERVED_AT + timedelta(microseconds=2),
        }
        assert result.history is not None and result.history.pages_requested == 4
    finally:
        storage.close()


@pytest.mark.parametrize("header", ["Accept", "Cookie", "Authorization"])
@pytest.mark.parametrize("override", ["override", ""])
def test_source_header_overrides_are_case_insensitive(
    header: str, override: str
) -> None:
    source = make_source(headers={header.lower(): override})
    captured: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, content=b"{}")

    with HttpTransport(HttpConfig(), backend=httpx.MockTransport(respond)) as transport:
        transport.request(
            RequestSpec("GET", source.endpoint.url, {header: "original"}),
            source,
            StreamState(source.id),
        )
    expected = [] if header == "Cookie" and not override else [override]
    assert captured[0].headers.get_list(header) == expected


def test_resolved_auth_overrides_declared_header_regardless_of_case(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARALLAX_TEST_TOKEN", "resolved")
    source = make_source(
        headers={"authorization": "old"},
        auth=AuthConfig(kind="bearer", env_var="PARALLAX_TEST_TOKEN"),
    )
    captured: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200)

    with HttpTransport(HttpConfig(), backend=httpx.MockTransport(respond)) as transport:
        transport.request(
            RequestSpec("GET", source.endpoint.url), source, StreamState(source.id)
        )
    assert captured[0].headers.get_list("authorization") == ["Bearer resolved"]


@pytest.mark.parametrize("allow_empty", [False, True])
@pytest.mark.parametrize(
    "payload_kind", ["empty", "invalid", "mixed", "malformed", "missing-container"]
)
def test_empty_policy_preserves_snapshot_when_payload_is_invalid(
    tmp_path: Path, allow_empty: bool, payload_kind: str
) -> None:
    source = make_source()
    valid = "<item><title>Valid</title><link>https://example.test/a</link></item>"
    invalid = "<item><title>Broken</title></item>"

    def rss(items: str) -> bytes:
        return f"<rss><channel>{items}</channel></rss>".encode()

    storage = Storage(tmp_path / "empty.db")
    storage.initialize()
    storage.sync_sources((source,))
    transport = PayloadTransport(rss(valid))
    service = IngestionService(
        storage,
        transport,
        AdapterRegistry(),
        BatchValidator(ValidationConfig(allow_empty_batches=allow_empty)),
        IngestionConfig(),
    )
    try:
        service.fetch_source(source)
        before = storage.get_stream_state(source.id).last_success_at
        transport.content = {
            "empty": rss(""),
            "invalid": rss(invalid),
            "mixed": rss(invalid + valid),
            "malformed": b"<rss>",
            "missing-container": b"<rss/>",
        }[payload_kind]
        fails = (
            payload_kind in {"invalid", "malformed", "missing-container"}
            or payload_kind == "empty"
            and not allow_empty
        )
        if fails:
            with pytest.raises((ValueError, RuntimeError)):
                service.fetch_source(source)
            assert storage.recent_fetch_runs()[0].status == "failed"
            assert storage.get_stream_state(source.id).last_success_at == before
        else:
            result = service.fetch_source(source)
            assert result.status == "success"
            assert result.rejected_count == (1 if payload_kind == "mixed" else 0)
        rows = storage.latest_snapshot_headlines()
        assert len(rows) == (0 if payload_kind == "empty" and allow_empty else 1)
    finally:
        storage.close()


def test_boolean_timestamp_and_rank_do_not_become_numeric_evidence() -> None:
    source = make_source(adapter="hk01_latest")
    payload = json.dumps(
        {"items": [{"data": {"articleId": 42, "title": "Title", "publishTime": True}}]}
    ).encode()
    batch = Hk01LatestAdapter().parse(
        source, HttpResponse(200, source.endpoint.url, {}, payload, OBSERVED_AT)
    )
    assert batch.candidates[0].published_at is None
    source = make_source(adapter="weibo_hot")
    payload = json.dumps(
        {
            "ok": 1,
            "data": {
                "realtime": [
                    {"word": "Invalid", "realpos": True},
                    {"word": "Valid", "realpos": 2, "num": True},
                ]
            },
        }
    ).encode()
    batch = WeiboHotAdapter().parse(
        source, HttpResponse(200, source.endpoint.url, {}, payload, OBSERVED_AT)
    )
    assert [item.title for item in batch.candidates] == ["Valid"]
    assert batch.candidates[0].metrics == {}


@pytest.mark.parametrize(
    "url,valid",
    [
        ("https://example.test/a", True),
        ("http://[::1]:0/a", True),
        ("https://[2001:db8::1]:443/a", True),
        ("https://example.test/中文?q=a%20b", True),
        ("https://example.test/story\nnext", False),
        ("https://example.test/ space", False),
        (" https://example.test/a", False),
        ("https://example.test/\x00", False),
        ("https://example.test/<a>", False),
        ("https://example.test:99999/a", False),
        ("https://[::1/a", False),
        ("javascript:alert(1)", False),
        ("https://", False),
    ],
)
def test_url_acceptance_agrees_at_all_boundaries(url: str, valid: bool) -> None:
    if valid:
        assert Endpoint(adapter="rss", url=url).url == url
    else:
        with pytest.raises(ValueError):
            Endpoint(adapter="rss", url=url)
    assert is_usable_external_url(url) is valid
    candidate = HeadlineCandidate("Test", url, external_id="test")
    control = HeadlineCandidate(
        "Control", "https://control.test/a", external_id="control"
    )
    result = BatchValidator(ValidationConfig()).validate(
        "fixture", ParsedBatch((candidate, control)), provider_id="fixture"
    )
    assert (candidate in result.candidates) is valid


@pytest.mark.parametrize(
    "value,integer,number",
    [
        (None, None, None),
        (False, None, None),
        (True, None, None),
        (0, 0, 0),
        (-2, -2, -2),
        (3, 3, 3),
        (0.0, None, 0.0),
        (1.5, None, 1.5),
        ("3", None, None),
        (float("nan"), None, None),
        (float("inf"), None, None),
        (float("-inf"), None, None),
        ([], None, None),
        ({}, None, None),
    ],
)
def test_numeric_boundary_contract(
    value: object, integer: int | None, number: int | float | None
) -> None:
    assert optional_integer(value) == integer
    assert optional_finite_number(value) == number
    if value is None or isinstance(value, str) or number is not None:
        assert scalar_text(value) == ("" if value is None else str(value))
    else:
        with pytest.raises(ValueError, match="string or number"):
            scalar_text(value)
    if number is not None or isinstance(value, str):
        assert parse_timestamp(value) is not None
    else:
        assert parse_timestamp(value) is None


def test_timestamp_does_not_coerce_arbitrary_objects() -> None:
    class LooksLikeTimestamp:
        def __str__(self) -> str:
            return "2026-09-18T12:00:00Z"

    assert parse_timestamp(LooksLikeTimestamp()) is None
    assert parse_timestamp(10**1000) is None


@pytest.mark.parametrize(
    "value", [True, False, float("nan"), float("inf"), "5", {}, 0, 2, 1.5]
)
def test_adapter_metrics_use_integer_or_finite_number_contract(value: object) -> None:
    source = make_source()
    weibo = json.dumps(
        {"ok": 1, "data": {"realtime": [{"word": "Title", "realpos": 1, "num": value}]}}
    ).encode()
    douban = json.dumps(
        {"items": [{"id": 1, "title": "Title", "rating": {"value": value}}]}
    ).encode()
    heat = (
        WeiboHotAdapter()
        .parse(source, HttpResponse(200, source.endpoint.url, {}, weibo, OBSERVED_AT))
        .candidates[0]
        .metrics
    )
    rating = (
        DoubanHotMoviesAdapter()
        .parse(source, HttpResponse(200, source.endpoint.url, {}, douban, OBSERVED_AT))
        .candidates[0]
        .metrics
    )
    assert heat == ({"heat": value} if type(value) is int else {})
    expected_rating = {"rating": value} if type(value) is int or value == 1.5 else {}
    assert rating == expected_rating
    # Serialization must not need the encoder's non-standard NaN/Infinity support.
    json.dumps(dict(heat), allow_nan=False)
    json.dumps(dict(rating), allow_nan=False)


@pytest.mark.parametrize("position", [True, False, 0, -1])
def test_generic_validation_rejects_invalid_rank(position: int) -> None:
    candidate = HeadlineCandidate("Title", "https://example.test/a", position=position)
    with pytest.raises(BatchValidationError):
        BatchValidator(ValidationConfig(allow_empty_batches=True)).validate(
            "fixture", ParsedBatch((candidate,)), provider_id="fixture"
        )


@pytest.mark.parametrize(
    "adapter_name,template",
    [
        ("hk01_latest", '{"items": ENTRIES}'),
        ("weibo_hot", '{"ok": 1, "data": {"realtime": ENTRIES}}'),
        ("sspai_hot", '{"error": 0, "data": ENTRIES}'),
        ("juejin_hot", '{"err_no": 0, "data": ENTRIES}'),
        ("dongqiudi_news", '{"articles": ENTRIES}'),
        ("douban_hot_movies", '{"items": ENTRIES}'),
        ("bilibili_hot_search", '{"code": 0, "list": ENTRIES}'),
        ("wallstreetcn_quick", '{"data": {"items": ENTRIES}}'),
    ],
)
@pytest.mark.parametrize("allow_empty", [False, True])
@pytest.mark.parametrize("entries", ["[]", '["malformed row"]'])
def test_json_extraction_distinguishes_empty_from_unusable(
    tmp_path: Path, adapter_name: str, template: str, allow_empty: bool, entries: str
) -> None:
    source = make_source(adapter=adapter_name)
    with closing(Storage(tmp_path / "extraction.db")) as storage:
        storage.initialize()
        storage.sync_sources((source,))
        service = IngestionService(
            storage,
            PayloadTransport(template.replace("ENTRIES", entries).encode()),
            AdapterRegistry(),
            BatchValidator(ValidationConfig(allow_empty_batches=allow_empty)),
            IngestionConfig(),
        )
        if entries == "[]" and allow_empty:
            assert service.fetch_source(source).item_count == 0
            assert storage.recent_fetch_runs()[0].status == "success"
        else:
            with pytest.raises((BatchValidationError, ValueError)):
                service.fetch_source(source)
            assert storage.recent_fetch_runs()[0].status == "failed"


@pytest.mark.parametrize(
    "adapter_name,payload",
    [
        ("hk01_latest", {"items": [{"type": 2}]}),
        ("weibo_hot", {"ok": 1, "data": {"realtime": [{"is_ad": 1, "word": "Ad"}]}}),
        ("nowcoder_hot", {"data": {"result": [{"type": 99}]}}),
        ("wallstreetcn_news", {"data": {"items": [{"resource_type": "ad"}]}}),
    ],
)
def test_intentional_non_content_exclusions_can_produce_empty_batch(
    adapter_name: str, payload: object
) -> None:
    source = make_source(adapter=adapter_name)
    adapter = AdapterRegistry().resolve_source(source)
    refresh = run_adapter_refresh(
        adapter,
        source,
        PayloadTransport(json.dumps(payload).encode()),
        StreamState(source.id),
    )
    assert isinstance(refresh, FullRefresh)
    batch = refresh.batch
    assert (
        BatchValidator(ValidationConfig(allow_empty_batches=True))
        .validate(source.id, batch, provider_id=source.provider_id)
        .candidates
        == ()
    )


def test_conditional_identity_uses_effective_case_insensitive_headers() -> None:
    source = make_source(headers={"aCcEpT": "effective"})
    observed: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        observed.append(request)
        return httpx.Response(200)

    with HttpTransport(HttpConfig(), backend=httpx.MockTransport(respond)) as transport:
        first = transport.request(
            RequestSpec("GET", source.endpoint.url, {"Accept": "unused"}),
            source,
            StreamState(source.id),
        )
        second = transport.request(
            RequestSpec("GET", source.endpoint.url, {"accept": "different unused"}),
            source,
            StreamState(
                source.id, etag='"etag"', request_identity=first.request_identity
            ),
        )
    assert first.request_identity == second.request_identity
    assert observed[1].headers.get_list("accept") == ["effective"]
    assert observed[1].headers["if-none-match"] == '"etag"'


@pytest.mark.parametrize("request_kind", ["json", "html", "json_post"])
def test_adapter_header_overrides_fold_before_defaults_are_combined(
    request_kind: str,
) -> None:
    source = make_source()
    # Replacing an existing default key in a dict retains its old position;
    # folding only after that dict merge would incorrectly select "earlier".
    headers = {
        "accept": "earlier",
        "Accept": "latest",
        "content-type": "earlier",
        "Content-Type": "latest",
    }
    if request_kind == "json_post":
        spec = json_post(source, {}, headers=headers)
    elif request_kind == "html":
        spec = html_request(source, headers=headers)
    else:
        spec = json_request(source, headers=headers)
    captured: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200)

    with HttpTransport(HttpConfig(), backend=httpx.MockTransport(respond)) as transport:
        transport.request(spec, source, StreamState(source.id))
    assert captured[0].headers.get_list("accept") == ["latest"]
    assert captured[0].headers.get_list("content-type") == ["latest"]
