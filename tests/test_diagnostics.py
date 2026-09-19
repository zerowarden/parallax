from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from rich.console import Console

from parallax.adapters import AdapterRegistry
from parallax.adapters.base import AdapterLookup
from parallax.config import IngestionConfig, Source, ValidationConfig
from parallax.diagnostics import (
    ACCESS_BLOCKED,
    CONFIGURATION_BROKEN,
    HEALTHY,
    NETWORK_FAILING,
    QUIET,
    RATE_LIMITED,
    SCHEMA_BROKEN,
    UPSTREAM_ERROR,
    DiagnosticService,
    SourceDiagnostic,
)
from parallax.domain import HttpResponse, RequestSpec, StreamState
from parallax.ingest import IngestionService
from parallax.presentation import Presenter
from parallax.storage import Storage
from parallax.validation import BatchValidator
from source_factory import make_source

OBSERVED_AT = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)


class ScriptedTransport:
    def __init__(self, responses: Sequence[HttpResponse | Exception]) -> None:
        self._responses: list[HttpResponse | Exception] = list(responses)
        self.requests: list[RequestSpec] = []

    def request(
        self,
        spec: RequestSpec,
        source: Source,
        state: StreamState,
    ) -> HttpResponse:
        self.requests.append(spec)
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _source(adapter: str, url: str, **options: object) -> Source:
    raw_max_items = options.pop("max_items", 30)
    max_items = raw_max_items if isinstance(raw_max_items, int) else 30
    return make_source(
        adapter=adapter,
        url=url,
        max_items=max_items,
        options=dict(options),
        language="zh-CN",
        market="CN",
    )


def _response(
    source: Source,
    payload: bytes,
    *,
    status_code: int = 200,
    headers: Mapping[str, str] | None = None,
) -> HttpResponse:
    return HttpResponse(
        status_code=status_code,
        url=source.endpoint.url,
        headers=dict(headers or {}),
        content=payload,
        observed_at=OBSERVED_AT,
    )


def _service(
    tmp_path: Path,
    transport: ScriptedTransport,
    adapters: AdapterLookup | None = None,
) -> tuple[DiagnosticService, Storage]:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    service = DiagnosticService(
        storage=storage,
        transport=transport,
        adapters=adapters or AdapterRegistry(),
        validator=BatchValidator(ValidationConfig()),
    )
    return service, storage


def test_diagnose_healthy_source_reports_accepted_items(
    fixtures_dir: Path,
    tmp_path: Path,
):
    source = _source("rss", "https://example.com/rss.xml")
    payload = (fixtures_dir / "rss" / "feed.xml").read_bytes()
    transport = ScriptedTransport(
        [
            _response(
                source,
                payload,
                headers={
                    "content-type": "application/rss+xml",
                    "etag": '"v1"',
                },
            )
        ]
    )
    service, storage = _service(tmp_path, transport)

    diagnostic = service.diagnose(source)
    runs = storage.recent_fetch_runs()
    headlines = storage.latest_snapshot_headlines()
    storage.close()

    assert diagnostic.classification == HEALTHY
    assert diagnostic.healthy is True
    assert diagnostic.has_problem is False
    assert diagnostic.accepted_count == 2
    assert diagnostic.rejected_count == 0
    assert diagnostic.upstream_host == "example.com"
    assert diagnostic.responses[0].status_code == 200
    assert diagnostic.responses[0].content_type == "application/rss+xml"
    assert diagnostic.responses[0].byte_count == len(payload)
    assert runs == []
    assert headlines == []


def test_diagnose_quiet_when_upstream_not_modified(tmp_path: Path):
    source = _source("rss", "https://example.com/rss.xml")
    transport = ScriptedTransport(
        [
            HttpResponse(
                status_code=304,
                url=source.endpoint.url,
                headers={"etag": '"v1"'},
                content=b"",
                observed_at=OBSERVED_AT,
            )
        ]
    )
    service, storage = _service(tmp_path, transport)

    diagnostic = service.diagnose(source)
    storage.close()

    assert diagnostic.classification == QUIET
    assert diagnostic.has_problem is False
    assert diagnostic.accepted_count is None
    assert "not modified" in diagnostic.detail


def test_diagnose_rate_limited_keeps_retry_after(tmp_path: Path):
    source = _source("rss", "https://example.com/rss.xml")
    transport = ScriptedTransport(
        [
            _response(
                source,
                b"slow down",
                status_code=429,
                headers={"retry-after": "120", "set-cookie": "session=secret"},
            )
        ]
    )
    service, storage = _service(tmp_path, transport)

    diagnostic = service.diagnose(source)
    storage.close()

    assert diagnostic.classification == RATE_LIMITED
    assert diagnostic.has_problem is True
    assert diagnostic.responses[0].status_code == 429
    assert diagnostic.responses[0].headers["retry-after"] == "120"
    assert "set-cookie" not in diagnostic.responses[0].headers
    assert diagnostic.error_type == "HTTPStatusError"


def test_diagnose_access_blocked_and_upstream_error(tmp_path: Path):
    forbidden = _source("rss", "https://example.com/rss.xml")
    unavailable = _source("rss", "https://example.com/rss.xml")
    service, storage = _service(
        tmp_path,
        ScriptedTransport([_response(forbidden, b"", status_code=403)]),
    )
    blocked = service.diagnose(forbidden)
    storage.close()

    other_storage = Storage(tmp_path / "other.db")
    other_storage.initialize()
    other = DiagnosticService(
        storage=other_storage,
        transport=ScriptedTransport([_response(unavailable, b"", status_code=503)]),
        adapters=AdapterRegistry(),
        validator=BatchValidator(ValidationConfig()),
    )
    failed = other.diagnose(unavailable)
    other_storage.close()

    assert blocked.classification == ACCESS_BLOCKED
    assert failed.classification == UPSTREAM_ERROR


def test_diagnose_network_failure(tmp_path: Path):
    source = _source("rss", "https://example.com/rss.xml")
    transport = ScriptedTransport([httpx.ConnectError("dns lookup failed")])
    service, storage = _service(tmp_path, transport)

    diagnostic = service.diagnose(source)
    storage.close()

    assert diagnostic.classification == NETWORK_FAILING
    assert diagnostic.error_type == "ConnectError"
    assert diagnostic.upstream_host == "example.com"


def test_diagnose_schema_broken_on_incompatible_payload(tmp_path: Path):
    source = _source("zhihu_hot", "https://www.zhihu.com/hot")
    transport = ScriptedTransport([_response(source, b"<html>login</html>")])
    service, storage = _service(tmp_path, transport)

    diagnostic = service.diagnose(source)
    storage.close()

    assert diagnostic.classification == SCHEMA_BROKEN
    assert diagnostic.error_type == "ValueError"
    assert diagnostic.responses[0].status_code == 200


def test_diagnose_empty_default_batch_as_schema_broken(tmp_path: Path) -> None:
    source = _source("rss", "https://example.com/rss.xml")
    transport = ScriptedTransport(
        [_response(source, b"<rss><channel></channel></rss>")]
    )
    service, storage = _service(tmp_path, transport)

    diagnostic = service.diagnose(source)
    storage.close()

    assert diagnostic.classification == SCHEMA_BROKEN
    assert diagnostic.has_problem is True
    assert diagnostic.accepted_count == 0
    assert "empty batch rejected" in diagnostic.detail


def test_diagnose_configuration_broken_for_unknown_adapter(tmp_path: Path):
    source = _source("does_not_exist", "https://example.com/")
    transport = ScriptedTransport([])
    service, storage = _service(tmp_path, transport)

    diagnostic = service.diagnose(source)
    storage.close()

    assert diagnostic.classification == CONFIGURATION_BROKEN
    assert "Unknown adapter" in diagnostic.detail
    assert transport.requests == []


def test_diagnose_multi_request_adapter(fixtures_dir: Path, tmp_path: Path):
    source = _source("v2ex_share", "https://www.v2ex.com")
    routes = ("create", "ideas", "programmer", "share")
    responses = [
        _response(
            source,
            (fixtures_dir / "v2ex" / f"{route}.json").read_bytes(),
            headers={"content-type": "application/json"},
        )
        for route in routes
    ]
    transport = ScriptedTransport(responses)
    service, storage = _service(tmp_path, transport)

    diagnostic = service.diagnose(source)
    storage.close()

    assert diagnostic.classification == HEALTHY
    assert diagnostic.accepted_count == 12
    assert len(diagnostic.responses) == 4
    assert len(transport.requests) == 4


def test_diagnose_reports_last_change_after_ingestion(
    fixtures_dir: Path,
    tmp_path: Path,
):
    source = make_source(
        id="fixture-rss",
        adapter="rss",
        url="https://example.com/rss.xml",
    )
    payload = (fixtures_dir / "rss" / "feed.xml").read_bytes()
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    storage.sync_sources([source])
    ingestion = IngestionService(
        storage=storage,
        transport=ScriptedTransport([_response(source, payload)]),
        adapters=AdapterRegistry(),
        validator=BatchValidator(ValidationConfig()),
        ingestion_config=IngestionConfig(),
    )
    ingestion.fetch_source(source)

    diagnostic = DiagnosticService(
        storage=storage,
        transport=ScriptedTransport([_response(source, payload)]),
        adapters=AdapterRegistry(),
        validator=BatchValidator(ValidationConfig()),
    ).diagnose(source)
    storage.close()

    assert diagnostic.last_success_at is not None
    assert diagnostic.last_attempt_at is not None
    assert diagnostic.last_change_at is not None
    assert diagnostic.consecutive_failures == 0


def test_diagnose_does_not_advance_stream_state(tmp_path: Path):
    source = _source("rss", "https://example.com/rss.xml")
    transport = ScriptedTransport([_response(source, b"<rss></rss>", status_code=500)])
    service, storage = _service(tmp_path, transport)

    service.diagnose(source)
    state = storage.get_stream_state(source.id)
    storage.close()

    assert state.last_attempt_at is None
    assert state.consecutive_failures == 0


def test_presenter_renders_no_cookies_or_credentials(tmp_path: Path):
    diagnostic = SourceDiagnostic(
        source_id="fixture",
        name="Fixture",
        adapter="rss",
        upstream_host="example.com",
        last_attempt_at=None,
        last_success_at=None,
        last_change_at=None,
        consecutive_failures=0,
        classification=ACCESS_BLOCKED,
        detail="upstream returned HTTP 403",
        responses=(),
    )
    console = Console(record=True, width=120)
    Presenter(console).diagnostics([diagnostic])
    output = console.export_text()

    assert "fixture" in output
    assert "access-blocked" in output
    assert "cookie" not in output.lower()
    assert "authorization" not in output.lower()


def test_response_diagnostics_redact_sensitive_headers(tmp_path: Path):
    source = _source("rss", "https://example.com/rss.xml")
    transport = ScriptedTransport(
        [
            _response(
                source,
                b"{}",
                headers={
                    "content-type": "application/json",
                    "authorization": "Bearer secret",
                    "set-cookie": "session=secret",
                    "x-request-id": "abc",
                },
            )
        ]
    )
    service, storage = _service(tmp_path, transport)

    diagnostic = service.diagnose(source)
    storage.close()

    assert diagnostic.responses[0].headers == {"content-type": "application/json"}
    assert "secret" not in json.dumps(
        {
            "detail": diagnostic.detail,
            "error": diagnostic.error_message,
        }
    )


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"retry-after": "30"}, RATE_LIMITED),
        ({"www-authenticate": "Bearer"}, ACCESS_BLOCKED),
    ],
)
def test_status_classification(
    tmp_path: Path,
    headers: Mapping[str, str],
    expected: str,
):
    status = 429 if expected == RATE_LIMITED else 401
    source = _source("rss", "https://example.com/rss.xml")
    transport = ScriptedTransport(
        [_response(source, b"", status_code=status, headers=headers)]
    )
    service, storage = _service(tmp_path, transport)

    diagnostic = service.diagnose(source)
    storage.close()

    assert diagnostic.classification == expected
