from __future__ import annotations

import logging
import threading
import time
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Protocol
from urllib.parse import urlsplit

import httpx

from parallax.config import AuthConfig, HttpConfig, SourceConfig
from parallax.domain import HttpResponse, RequestSpec, StreamState

LOGGER = logging.getLogger(__name__)
MAX_REDIRECTS = 20
_TRANSIENT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout)


class ResponseTooLargeError(RuntimeError):
    pass


class UnsafeRedirectError(httpx.TransportError):
    """A redirect violates the source's configured safety policy."""


class Transport(Protocol):
    def request(
        self,
        spec: RequestSpec,
        source: SourceConfig,
        state: StreamState,
    ) -> HttpResponse:
        """Perform the configured request under shared HTTP policy."""
        ...


@dataclass(slots=True)
class HttpTransport(AbstractContextManager["HttpTransport"]):
    config: HttpConfig
    _client: httpx.Client = field(init=False, repr=False)
    _host_semaphores: dict[str, threading.Semaphore] = field(
        init=False,
        default_factory=dict,
        repr=False,
    )
    _host_semaphores_lock: threading.Lock = field(
        init=False,
        default_factory=threading.Lock,
        repr=False,
    )

    def __post_init__(self) -> None:
        timeout = httpx.Timeout(
            connect=self.config.connect_timeout_seconds,
            read=self.config.read_timeout_seconds,
            write=self.config.write_timeout_seconds,
            pool=self.config.pool_timeout_seconds,
        )
        limits = httpx.Limits(
            max_connections=self.config.max_connections,
            max_keepalive_connections=self.config.max_keepalive_connections,
        )
        self._client = httpx.Client(
            timeout=timeout,
            limits=limits,
            follow_redirects=False,
            headers={"User-Agent": self.config.user_agent},
        )

    def request(
        self,
        spec: RequestSpec,
        source: SourceConfig,
        state: StreamState,
    ) -> HttpResponse:
        """Perform the configured request under shared HTTP policy."""
        headers = dict(spec.headers)
        headers.update(source.headers)
        params = dict(spec.params)
        self._apply_conditional_headers(headers, state)
        self._apply_auth(headers, params, source.auth)

        url = httpx.URL(spec.url)
        if params:
            url = url.copy_merge_params(params)

        LOGGER.info(
            "operation=http_request source_id=%s method=%s url=%s",
            source.id,
            spec.method,
            _safe_url(spec.url),
        )

        request = self._client.build_request(
            spec.method,
            url,
            headers=headers,
            content=spec.content,
        )
        # The transport never sends browser cookies; drop any cookie the shared
        # client jar merged in so request policy sees a cookie-free request and
        # later fetches cannot inherit stale upstream cookies.
        request.headers.pop("cookie", None)
        redirect_count = 0
        attempt = 0
        while True:
            attempt += 1
            try:
                response, host_semaphore = self._send_once(request)
            except _TRANSIENT_ERRORS as exc:
                if not self._retry_transient(request, source, attempt, exc):
                    raise
                continue
            try:
                next_request = response.next_request
                if self.config.follow_redirects and next_request is not None:
                    if redirect_count >= MAX_REDIRECTS:
                        raise httpx.TooManyRedirects(
                            f"Exceeded {MAX_REDIRECTS} redirects",
                            request=request,
                        )
                    if not _same_safe_authority(request.url, next_request.url):
                        # Upstream cookies (for example load-balancer affinity)
                        # are not credentials of ours and must not follow a
                        # redirect to a different authority.
                        _discard_upstream_cookies(next_request)
                    if not self._is_safe_redirect(request, next_request, source):
                        raise UnsafeRedirectError(
                            "Refusing unsafe redirect: "
                            f"{_safe_url(str(request.url))} -> "
                            f"{_safe_url(str(next_request.url))}",
                            request=request,
                        )
                    LOGGER.info(
                        "operation=http_redirect source_id=%s from_url=%s to_url=%s",
                        source.id,
                        _safe_url(str(request.url)),
                        _safe_url(str(next_request.url)),
                    )
                    request = next_request
                    redirect_count += 1
                    attempt = 0
                    continue

                try:
                    content = self._read_bounded(response)
                except httpx.ReadTimeout as exc:
                    if not self._retry_transient(request, source, attempt, exc):
                        raise
                    continue
                final_url = str(response.url)
                LOGGER.info(
                    "operation=http_response source_id=%s status=%s bytes=%s url=%s",
                    source.id,
                    response.status_code,
                    len(content),
                    _safe_url(final_url),
                )
                return HttpResponse(
                    status_code=response.status_code,
                    url=final_url,
                    headers=dict(response.headers),
                    content=content,
                    cookies=dict(response.cookies),
                )
            finally:
                try:
                    response.close()
                finally:
                    host_semaphore.release()

    def close(self) -> None:
        LOGGER.info("operation=http_client_close")
        self._client.close()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _read_bounded(self, response: httpx.Response) -> bytes:
        declared = response.headers.get("content-length")
        declared_size = int(declared) if declared and declared.isdigit() else 0
        if declared_size > self.config.max_response_bytes:
            raise ResponseTooLargeError(
                f"Response declares {declared} bytes; limit is "
                f"{self.config.max_response_bytes}"
            )

        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_bytes():
            total += len(chunk)
            if total > self.config.max_response_bytes:
                raise ResponseTooLargeError(
                    f"Response exceeded {self.config.max_response_bytes} bytes"
                )
            chunks.append(chunk)
        return b"".join(chunks)

    def _send_once(
        self,
        request: httpx.Request,
    ) -> tuple[httpx.Response, threading.Semaphore]:
        semaphore = self._host_semaphore(request.url)
        semaphore.acquire()
        try:
            response = self._client.send(request, stream=True, follow_redirects=False)
        except BaseException:
            semaphore.release()
            raise
        return response, semaphore

    def _host_semaphore(self, url: httpx.URL) -> threading.Semaphore:
        host = url.host
        if host is None:
            raise ValueError("HTTP request URL must include a hostname")
        with self._host_semaphores_lock:
            return self._host_semaphores.setdefault(
                host.lower(),
                threading.Semaphore(self.config.max_connections_per_host),
            )

    def _retry_transient(
        self,
        request: httpx.Request,
        source: SourceConfig,
        attempt: int,
        error: httpx.TransportError,
    ) -> bool:
        if not self._can_retry(request, attempt):
            return False
        delay = self.config.retry_backoff_seconds
        LOGGER.warning(
            "operation=http_retry source_id=%s url=%s error_type=%s "
            "attempt=%s max_attempts=%s delay=%s",
            source.id,
            _safe_url(str(request.url)),
            type(error).__name__,
            attempt,
            self.config.max_attempts,
            delay,
        )
        time.sleep(delay)
        return True

    def _can_retry(self, request: httpx.Request, attempt: int) -> bool:
        return (
            request.method.upper() in {"GET", "HEAD"}
            and attempt < self.config.max_attempts
        )

    @staticmethod
    def _is_safe_redirect(
        current: httpx.Request,
        target: httpx.Request,
        source: SourceConfig,
    ) -> bool:
        is_downgrade = current.url.scheme == "https" and target.url.scheme == "http"
        if is_downgrade and not source.redirect.allow_https_downgrade:
            return False
        if _same_safe_authority(current.url, target.url):
            return True
        target_host = target.url.host
        return (
            target_host is not None
            and target_host.lower() in source.redirect.allowed_hosts
            and source.auth.kind == "none"
            and not _has_sensitive_request_headers(current.headers)
            and not _has_sensitive_request_headers(target.headers)
        )

    @staticmethod
    def _apply_conditional_headers(
        headers: dict[str, str],
        state: StreamState,
    ) -> None:
        if state.etag:
            headers.setdefault("If-None-Match", state.etag)
        if state.last_modified:
            headers.setdefault("If-Modified-Since", state.last_modified)

    @staticmethod
    def _apply_auth(
        headers: dict[str, str],
        params: dict[str, str],
        auth: AuthConfig,
    ) -> None:
        secret = auth.resolve_secret()
        if secret is None:
            return
        if auth.kind == "header":
            assert auth.name is not None
            headers[auth.name] = secret
        elif auth.kind == "bearer":
            headers["Authorization"] = f"{auth.prefix} {secret}"
        elif auth.kind == "query":
            assert auth.name is not None
            params[auth.name] = secret


def _safe_url(url: str) -> str:
    split = urlsplit(url)
    hostname = split.hostname or ""
    if ":" in hostname:
        hostname = f"[{hostname}]"
    netloc = hostname
    if split.port is not None:
        netloc = f"{netloc}:{split.port}"
    return split._replace(netloc=netloc, query="", fragment="").geturl()


def _same_safe_authority(current: httpx.URL, target: httpx.URL) -> bool:
    if current.host != target.host:
        return False
    if current.scheme == target.scheme:
        return _effective_port(current) == _effective_port(target)
    return {current.scheme, target.scheme} == {"http", "https"} and {
        _effective_port(current),
        _effective_port(target),
    } == {80, 443}


def _effective_port(url: httpx.URL) -> int | None:
    if url.port is not None:
        return url.port
    return {"http": 80, "https": 443}.get(url.scheme)


def _discard_upstream_cookies(request: httpx.Request) -> None:
    request.headers.pop("cookie", None)


def _has_sensitive_request_headers(headers: httpx.Headers) -> bool:
    sensitive_names = {
        "authorization",
        "proxy-authorization",
        "cookie",
        "x-api-key",
        "x-auth-token",
        "x-access-token",
    }
    return any(name.lower() in sensitive_names for name in headers)
