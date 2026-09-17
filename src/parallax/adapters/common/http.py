from __future__ import annotations

import json
from collections.abc import Mapping

from parallax.config import SourceConfig
from parallax.domain import RequestSpec

JSON_ACCEPT = "application/json, text/plain;q=0.9, */*;q=0.1"
HTML_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"


def cookie_header(cookies: Mapping[str, str]) -> str:
    """Render response cookies as a Cookie request header."""
    return "; ".join(f"{name}={value}" for name, value in cookies.items())


def json_request(
    source: SourceConfig,
    *,
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, str] | None = None,
) -> RequestSpec:
    """Build a GET request for the configured JSON endpoint."""
    return _request(source, JSON_ACCEPT, headers, params)


def json_post(
    source: SourceConfig,
    body: Mapping[str, object],
    *,
    headers: Mapping[str, str] | None = None,
) -> RequestSpec:
    """Build a POST request carrying a JSON body for the configured endpoint."""
    return RequestSpec(
        method="POST",
        url=source.url,
        headers={
            "Accept": JSON_ACCEPT,
            "Content-Type": "application/json",
            **(headers or {}),
        },
        content=json.dumps(body, ensure_ascii=False).encode("utf-8"),
    )


def html_request(
    source: SourceConfig,
    *,
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, str] | None = None,
) -> RequestSpec:
    """Build a GET request for a configured HTML page."""
    return _request(source, HTML_ACCEPT, headers, params)


def _request(
    source: SourceConfig,
    accept: str,
    headers: Mapping[str, str] | None,
    params: Mapping[str, str] | None,
) -> RequestSpec:
    return RequestSpec(
        method="GET",
        url=source.url,
        headers={"Accept": accept, **(headers or {})},
        params=dict(params or {}),
    )
