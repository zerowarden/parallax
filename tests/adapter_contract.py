from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlsplit

import pytest

from parallax.adapters.base import SourceAdapter
from parallax.config import SourceConfig
from parallax.domain import HeadlineCandidate, HttpResponse, ParsedBatch


def response_for(
    source: SourceConfig,
    payload: bytes,
    *,
    status_code: int = 200,
    headers: Mapping[str, str] | None = None,
    cookies: Mapping[str, str] | None = None,
) -> HttpResponse:
    """Build an in-memory upstream response for parser tests."""
    return HttpResponse(
        status_code=status_code,
        url=source.url,
        headers=dict(headers or {}),
        content=payload,
        cookies=dict(cookies or {}),
    )


def assert_candidate_contract(candidate: HeadlineCandidate) -> None:
    """Assert source-independent invariants every parsed candidate must meet."""
    assert candidate.title, "candidate title must not be blank"
    assert candidate.title == candidate.title.strip(), "title must be trimmed"

    parsed_url = urlsplit(candidate.url)
    assert parsed_url.scheme in {"http", "https"}, "URL must use HTTP(S)"
    assert parsed_url.hostname, f"URL must include a host: {candidate.url!r}"

    if candidate.position is not None:
        assert candidate.position >= 1, "position must be positive when present"

    if candidate.published_at is not None:
        published_at = candidate.published_at
        assert published_at.tzinfo is not None, "published_at must be tz-aware"

    if candidate.external_id is not None:
        external_id = candidate.external_id
        assert external_id, "external_id must not be blank when present"
        assert external_id == external_id.strip(), "external_id must be trimmed"


def assert_batch_contract(batch: ParsedBatch, *, min_items: int = 1) -> None:
    assert len(batch.candidates) >= min_items, "batch contains too few candidates"
    for candidate in batch.candidates:
        assert_candidate_contract(candidate)


def assert_parse_rejects(
    adapter: SourceAdapter,
    source: SourceConfig,
    payload: bytes,
    *,
    match: str | None = None,
) -> None:
    """Assert an incompatible payload fails clearly instead of parsing as empty."""
    with pytest.raises(ValueError, match=match):
        adapter.parse(source, response_for(source, payload))
