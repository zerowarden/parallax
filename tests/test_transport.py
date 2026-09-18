from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from parallax.config import (
    AuthConfig,
    HttpConfig,
    RedirectPolicyConfig,
    SourceConfig,
)
from parallax.domain import RequestSpec, StreamState
from parallax.transport import HttpTransport, UnsafeRedirectError


class _RecordingHandler(BaseHTTPRequestHandler):
    observed_paths: list[str] = []
    observed_bodies: list[tuple[str, str]] = []

    def do_GET(self) -> None:
        type(self).observed_paths.append(self.path)
        body = json.dumps({"ok": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Set-Cookie", "fixture=1; Path=/")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        payload = self.rfile.read(length).decode("utf-8")
        content_type = self.headers.get("Content-Type", "")
        type(self).observed_bodies.append((content_type, payload))
        body = json.dumps({"ok": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def recording_server() -> Iterator[tuple[str, list[str]]]:
    _RecordingHandler.observed_paths = []
    _RecordingHandler.observed_bodies = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RecordingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}", _RecordingHandler.observed_paths
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _source(url: str) -> SourceConfig:
    return SourceConfig(
        id="fixture",
        name="Fixture",
        region="CN",
        language="zh-CN",
        adapter="fixture",
        url=url,
    )


def test_transport_preserves_configured_url_query(
    recording_server: tuple[str, list[str]],
):
    base_url, observed_paths = recording_server
    source = _source(f"{base_url}/hot?tag=abc%3D")

    with HttpTransport(HttpConfig()) as transport:
        transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )

    assert len(observed_paths) == 1
    assert parse_qs(urlsplit(observed_paths[0]).query) == {"tag": ["abc="]}


def test_transport_merges_request_params_into_url_query(
    recording_server: tuple[str, list[str]],
):
    base_url, observed_paths = recording_server
    source = _source(f"{base_url}/hot?tag=abc%3D")

    with HttpTransport(HttpConfig()) as transport:
        transport.request(
            RequestSpec(method="GET", url=source.url, params={"id": "zhihu"}),
            source,
            StreamState(source_id=source.id),
        )

    assert len(observed_paths) == 1
    assert parse_qs(urlsplit(observed_paths[0]).query) == {
        "tag": ["abc="],
        "id": ["zhihu"],
    }


def test_transport_captures_response_cookies(
    recording_server: tuple[str, list[str]],
):
    base_url, _ = recording_server
    source = _source(f"{base_url}/cookies")

    with HttpTransport(HttpConfig()) as transport:
        response = transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )

    assert response.cookies == {"fixture": "1"}


def test_transport_forwards_post_body(
    recording_server: tuple[str, list[str]],
):
    base_url, _ = recording_server
    source = _source(f"{base_url}/rank")
    payload = json.dumps({"page_params": {"rank_name": "热搜榜"}}).encode("utf-8")

    with HttpTransport(HttpConfig()) as transport:
        response = transport.request(
            RequestSpec(
                method="POST",
                url=source.url,
                headers={"Content-Type": "application/json"},
                content=payload,
            ),
            source,
            StreamState(source_id=source.id),
        )

    assert response.status_code == 200
    assert _RecordingHandler.observed_bodies == [
        ("application/json", payload.decode("utf-8"))
    ]


def test_transport_follows_same_authority_redirect_with_headers() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/start":
            return httpx.Response(302, headers={"Location": "/final"})
        return httpx.Response(200, content=b"ok")

    transport = HttpTransport(HttpConfig())
    transport._client.close()
    transport._client = httpx.Client(transport=httpx.MockTransport(handler))
    source = _source("https://example.test/start")
    source.headers["X-Api-Key"] = "secret"
    try:
        response = transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert response.status_code == 200
    assert [request.url.path for request in requests] == ["/start", "/final"]
    assert [request.headers["X-Api-Key"] for request in requests] == [
        "secret",
        "secret",
    ]


def test_transport_rejects_cross_authority_redirect_before_leaking_headers() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            302,
            headers={"Location": "https://attacker.test/collect"},
        )

    transport = HttpTransport(HttpConfig())
    transport._client.close()
    transport._client = httpx.Client(transport=httpx.MockTransport(handler))
    source = _source("https://example.test/start")
    source.auth = AuthConfig(
        kind="header",
        env_var="PARALLAX_TEST_SECRET",
        name="X-Api-Key",
    )
    try:
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setenv("PARALLAX_TEST_SECRET", "secret")
            with pytest.raises(UnsafeRedirectError):
                transport.request(
                    RequestSpec(method="GET", url=source.url),
                    source,
                    StreamState(source_id=source.id),
                )
    finally:
        transport.close()

    assert len(requests) == 1
    assert requests[0].url.host == "example.test"
    assert requests[0].headers["X-Api-Key"] == "secret"


def _mock_transport(
    handler: Callable[[httpx.Request], httpx.Response],
    config: HttpConfig | None = None,
) -> HttpTransport:
    transport = HttpTransport(config or HttpConfig())
    transport._client.close()
    transport._client = httpx.Client(transport=httpx.MockTransport(handler))
    return transport


def test_transport_follows_allowlisted_cross_authority_redirect() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "example.test":
            return httpx.Response(
                302,
                headers={"Location": "https://publisher.test/final"},
            )
        return httpx.Response(200, content=b"ok")

    transport = _mock_transport(handler)
    source = _source("https://example.test/start")
    source.redirect = RedirectPolicyConfig(allowed_hosts=("publisher.test",))
    try:
        response = transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert response.status_code == 200
    assert [request.url.host for request in requests] == [
        "example.test",
        "publisher.test",
    ]


def test_transport_follows_allowlisted_downgrade_with_upstream_cookie() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "example.test":
            return httpx.Response(
                301,
                headers={
                    "Location": "http://publisher.test/final",
                    "Set-Cookie": "affinity=upstream; Path=/",
                },
            )
        return httpx.Response(200, content=b"ok")

    transport = _mock_transport(handler)
    source = _source("https://example.test/start")
    source.redirect = RedirectPolicyConfig(
        allowed_hosts=("publisher.test",),
        allow_https_downgrade=True,
    )
    try:
        response = transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert response.status_code == 200
    assert [request.url.host for request in requests] == [
        "example.test",
        "publisher.test",
    ]
    assert "affinity=upstream" not in requests[1].headers.get("cookie", "")


def test_transport_does_not_reuse_upstream_cookies_across_requests() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=b"ok",
            headers={"Set-Cookie": "session=1; Path=/"},
        )

    transport = _mock_transport(handler)
    source = _source("https://example.test/feed")
    try:
        for _ in range(2):
            transport.request(
                RequestSpec(method="GET", url=source.url),
                source,
                StreamState(source_id=source.id),
            )
    finally:
        transport.close()

    assert len(requests) == 2
    assert "session=1" not in requests[1].headers.get("cookie", "")


def test_transport_sends_cookie_declared_in_request_spec() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"ok")

    transport = _mock_transport(handler)
    source = _source("https://example.test/feed")
    try:
        transport.request(
            RequestSpec(
                method="GET",
                url=source.url,
                headers={"Cookie": "ttwid=abc; sessionid=def"},
            ),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert requests[0].headers["Cookie"] == "ttwid=abc; sessionid=def"


def test_transport_declared_cookie_overrides_upstream_jar_cookie() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=b"ok",
            headers={"Set-Cookie": "session=1; Path=/"},
        )

    transport = _mock_transport(handler)
    source = _source("https://example.test/feed")
    try:
        transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )
        transport.request(
            RequestSpec(
                method="GET",
                url=source.url,
                headers={"Cookie": "declared=1"},
            ),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert requests[1].headers["Cookie"] == "declared=1"


def test_transport_reapplies_declared_cookie_across_same_authority_redirect() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/start":
            return httpx.Response(
                302,
                headers={
                    "Location": "/final",
                    "Set-Cookie": "affinity=upstream; Path=/",
                },
            )
        return httpx.Response(200, content=b"ok")

    transport = _mock_transport(handler)
    source = _source("https://example.test/start")
    try:
        transport.request(
            RequestSpec(
                method="GET",
                url=source.url,
                headers={"Cookie": "declared=1"},
            ),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert [request.headers["Cookie"] for request in requests] == [
        "declared=1",
        "declared=1",
    ]


def test_transport_strips_upstream_cookie_across_same_authority_redirect() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/start":
            return httpx.Response(
                302,
                headers={
                    "Location": "/final",
                    "Set-Cookie": "affinity=upstream; Path=/",
                },
            )
        return httpx.Response(200, content=b"ok")

    transport = _mock_transport(handler)
    source = _source("https://example.test/start")
    try:
        transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert "cookie" not in requests[1].headers


def test_transport_omits_empty_declared_cookie() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"ok")

    transport = _mock_transport(handler)
    source = _source("https://example.test/feed")
    try:
        transport.request(
            RequestSpec(method="GET", url=source.url, headers={"Cookie": ""}),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert "cookie" not in requests[0].headers


def test_transport_rejects_allowlisted_redirect_with_sensitive_header() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            302,
            headers={"Location": "https://publisher.test/final"},
        )

    transport = _mock_transport(handler)
    source = _source("https://example.test/start")
    source.redirect = RedirectPolicyConfig(allowed_hosts=("publisher.test",))
    source.headers["Authorization"] = "Bearer secret"
    try:
        with pytest.raises(UnsafeRedirectError):
            transport.request(
                RequestSpec(method="GET", url=source.url),
                source,
                StreamState(source_id=source.id),
            )
    finally:
        transport.close()

    assert len(requests) == 1


def test_transport_allows_explicit_same_host_https_downgrade() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/start":
            return httpx.Response(
                302,
                headers={"Location": "http://example.test/final"},
            )
        return httpx.Response(200, content=b"ok")

    transport = _mock_transport(handler)
    source = _source("https://example.test/start")
    source.redirect = RedirectPolicyConfig(allow_https_downgrade=True)
    try:
        response = transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert response.status_code == 200
    assert [str(request.url) for request in requests] == [
        "https://example.test/start",
        "http://example.test/final",
    ]


def test_transport_rejects_unapproved_https_downgrade() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            302,
            headers={"Location": "http://example.test/final"},
        )

    transport = _mock_transport(handler)
    source = _source("https://example.test/start")
    try:
        with pytest.raises(UnsafeRedirectError):
            transport.request(
                RequestSpec(method="GET", url=source.url),
                source,
                StreamState(source_id=source.id),
            )
    finally:
        transport.close()

    assert len(requests) == 1


def test_transport_retries_one_idempotent_connect_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("temporary", request=request)
        return httpx.Response(200, content=b"ok")

    monkeypatch.setattr("parallax.transport.time.sleep", delays.append)
    transport = _mock_transport(
        handler,
        HttpConfig(max_attempts=2, retry_backoff_seconds=0.25),
    )
    source = _source("https://example.test/start")
    try:
        response = transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert response.status_code == 200
    assert attempts == 2
    assert delays == [0.25]


class _ReadTimeoutStream(httpx.SyncByteStream):
    def __iter__(self) -> Iterator[bytes]:
        yield b"partial"
        raise httpx.ReadTimeout("read timed out")

    def close(self) -> None:
        pass


def test_transport_retries_one_idempotent_read_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ReadTimeout("temporary", request=request)
        return httpx.Response(200, content=b"ok")

    monkeypatch.setattr("parallax.transport.time.sleep", delays.append)
    transport = _mock_transport(
        handler,
        HttpConfig(max_attempts=2, retry_backoff_seconds=0.25),
    )
    source = _source("https://example.test/start")
    try:
        response = transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert response.status_code == 200
    assert attempts == 2
    assert delays == [0.25]


def test_transport_retries_one_idempotent_body_read_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(200, stream=_ReadTimeoutStream())
        return httpx.Response(200, content=b"ok")

    monkeypatch.setattr("parallax.transport.time.sleep", delays.append)
    transport = _mock_transport(
        handler,
        HttpConfig(max_attempts=2, retry_backoff_seconds=0.25),
    )
    source = _source("https://example.test/start")
    try:
        response = transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert response.content == b"ok"
    assert attempts == 2
    assert delays == [0.25]


@pytest.mark.parametrize("method", ["POST", "PUT"])
def test_transport_does_not_retry_non_idempotent_read_timeout(method: str) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ReadTimeout("temporary", request=request)

    transport = _mock_transport(handler)
    source = _source("https://example.test/start")
    try:
        with pytest.raises(httpx.ReadTimeout):
            transport.request(
                RequestSpec(method=method, url=source.url),
                source,
                StreamState(source_id=source.id),
            )
    finally:
        transport.close()

    assert attempts == 1


@pytest.mark.parametrize("method", ["POST", "PUT"])
def test_transport_does_not_retry_non_idempotent_connect_failure(method: str) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("temporary", request=request)

    transport = _mock_transport(handler)
    source = _source("https://example.test/start")
    try:
        with pytest.raises(httpx.ConnectError):
            transport.request(
                RequestSpec(method=method, url=source.url),
                source,
                StreamState(source_id=source.id),
            )
    finally:
        transport.close()

    assert attempts == 1


def test_transport_does_not_retry_http_response() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(405, content=b"method not allowed")

    transport = _mock_transport(handler)
    source = _source("https://example.test/start")
    try:
        response = transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )
    finally:
        transport.close()

    assert response.status_code == 405
    assert attempts == 1


def test_transport_limits_same_host_requests() -> None:
    active = 0
    maximum = 0
    lock = threading.Lock()
    two_active = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
            if active == 2:
                two_active.set()
        release.wait(timeout=2)
        with lock:
            active -= 1
        return httpx.Response(200, content=b"ok")

    transport = _mock_transport(handler, HttpConfig(max_connections_per_host=2))
    source = _source("https://example.test/start")

    def fetch() -> None:
        try:
            transport.request(
                RequestSpec(method="GET", url=source.url),
                source,
                StreamState(source_id=source.id),
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=fetch) for _ in range(3)]
    for thread in threads:
        thread.start()
    assert two_active.wait(timeout=1)
    assert not release.is_set()
    release.set()
    for thread in threads:
        thread.join(timeout=2)
    transport.close()

    assert not errors
    assert maximum == 2
    assert not any(thread.is_alive() for thread in threads)


def test_transport_allows_different_hosts_to_overlap() -> None:
    active_hosts: set[str] = set()
    lock = threading.Lock()
    different_hosts_active = threading.Event()
    release = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host is not None
        with lock:
            active_hosts.add(request.url.host)
            if len(active_hosts) == 2:
                different_hosts_active.set()
        release.wait(timeout=2)
        with lock:
            active_hosts.remove(request.url.host)
        return httpx.Response(200, content=b"ok")

    transport = _mock_transport(handler, HttpConfig(max_connections_per_host=1))
    first = _source("https://first.test/start")
    second = _source("https://second.test/start")

    def fetch(source: SourceConfig) -> None:
        transport.request(
            RequestSpec(method="GET", url=source.url),
            source,
            StreamState(source_id=source.id),
        )

    threads = [
        threading.Thread(target=fetch, args=(first,)),
        threading.Thread(target=fetch, args=(second,)),
    ]
    for thread in threads:
        thread.start()
    assert different_hosts_active.wait(timeout=1)
    release.set()
    for thread in threads:
        thread.join(timeout=2)
    transport.close()

    assert not any(thread.is_alive() for thread in threads)
