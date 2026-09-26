from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import (
    decode_json_object,
    extracted_batch,
    ranked_candidates,
    require_list,
    require_mapping,
    scalar_text,
    text,
)
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    JsonValue,
    ParsedBatch,
    RequestSpec,
)
from parallax.numbers import optional_integer
from parallax.parsing import parse_timestamp

LIVE_CHANNEL = "global-channel"
NEWS_EXCLUDED_RESOURCE_TYPES = frozenset({"ad", "theme"})


@dataclass(frozen=True, slots=True)
class _Fields:
    title: str
    url: str
    external_id: str | None
    raw_published_at: str
    metrics: Mapping[str, JsonValue]


_Extractor = Callable[[dict[str, Any]], _Fields | None]


class WallstreetcnQuickAdapter:
    """Native adapter for the WallstreetCN live/快讯 flow.

    Entries without a ``title`` fall back to ``content_text``; both are upstream
    values, not derived summaries. ``display_time`` is Unix seconds and becomes
    ``published_at``, preserving the raw value separately.
    """

    def build_request(self, source: Source) -> RequestSpec:
        limit = source.max_items
        return json_request(
            source,
            params={"channel": LIVE_CHANNEL, "limit": str(limit)},
        )

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        entries = _entries(payload=response.content, key="items", label="live")
        return _batch(entries, source, _live_fields)


class WallstreetcnNewsAdapter:
    """Native adapter for the WallstreetCN article information flow.

    Advertisement/theme containers and live entries are skipped, matching the
    reference implementation. A missing ``title`` falls back to
    ``content_short``; entries without a resource URL are skipped.
    """

    def build_request(self, source: Source) -> RequestSpec:
        limit = source.max_items
        return json_request(
            source,
            params={
                "channel": LIVE_CHANNEL,
                "accept": "article",
                "limit": str(limit),
            },
        )

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        entries = _entries(payload=response.content, key="items", label="news")
        return _batch(entries, source, _news_fields)


class WallstreetcnHotAdapter:
    """Native adapter for the WallstreetCN daily hot-article list.

    The payload also carries week items; only ``day_items`` (the surface the
    reference implementation collects) is parsed. Pageviews are kept as bounded
    metrics.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source, params={"period": "all"})

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        entries = _entries(payload=response.content, key="day_items", label="hot")
        return _batch(entries, source, _hot_fields)


def _entries(*, payload: bytes, key: str, label: str) -> list[Any]:
    decoded = decode_json_object(payload, label=f"WallstreetCN {label}")
    data = require_mapping(
        decoded.get("data"),
        f"WallstreetCN {label} response does not contain a data object",
    )
    return require_list(
        data.get(key),
        f"WallstreetCN {label} response does not contain a {key} list",
    )


def _batch(
    entries: list[Any],
    source: Source,
    extract: _Extractor,
) -> ParsedBatch:
    excluded = 0

    def build(entry: dict[str, Any], position: int) -> HeadlineCandidate | None:
        nonlocal excluded
        fields = extract(entry)
        if fields is None:
            excluded += 1
            return None
        if not fields.title or not fields.url:
            return None
        return HeadlineCandidate(
            title=fields.title,
            url=fields.url,
            external_id=fields.external_id,
            published_at=parse_timestamp(fields.raw_published_at),
            raw_published_at=fields.raw_published_at or None,
            position=position,
            metrics=fields.metrics,
        )

    candidates = ranked_candidates(entries, max_items=source.max_items, build=build)
    return extracted_batch(
        candidates, entries=entries, label="WallstreetCN", excluded_count=excluded
    )


def _live_fields(entry: dict[str, Any]) -> _Fields:
    return _Fields(
        title=text(entry.get("title")) or text(entry.get("content_text")),
        url=text(entry.get("uri")),
        external_id=scalar_text(entry.get("id")) or None,
        raw_published_at=scalar_text(entry.get("display_time")),
        metrics={},
    )


def _news_fields(entry: dict[str, Any]) -> _Fields | None:
    if text(entry.get("resource_type")) in NEWS_EXCLUDED_RESOURCE_TYPES:
        return None
    resource = entry.get("resource")
    if not isinstance(resource, dict):
        raise ValueError("WallstreetCN news entry does not contain a resource object")
    if text(resource.get("type")) == "live":
        return None
    return _Fields(
        title=text(resource.get("title")) or text(resource.get("content_short")),
        url=text(resource.get("uri")),
        external_id=scalar_text(resource.get("id")) or None,
        raw_published_at=scalar_text(resource.get("display_time")),
        metrics={},
    )


def _hot_fields(entry: dict[str, Any]) -> _Fields | None:
    metrics: dict[str, JsonValue] = {}
    pageviews = optional_integer(entry.get("pageviews"))
    if pageviews is not None:
        metrics["pageviews"] = pageviews
    return _Fields(
        title=text(entry.get("title")),
        url=text(entry.get("uri")),
        external_id=scalar_text(entry.get("id")) or None,
        raw_published_at=scalar_text(entry.get("display_time")),
        metrics=metrics,
    )
