from __future__ import annotations

from typing import Any
from urllib.parse import quote

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import (
    decode_json_object,
    require_list,
    require_mapping,
    text,
)
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)
from parallax.parsing import parse_timestamp

SEARCH_URL_TEMPLATE = "https://search.bilibili.com/all?keyword={keyword}"
VIDEO_URL_TEMPLATE = "https://www.bilibili.com/video/{bvid}"
REFERER = "https://www.bilibili.com/"


class BilibiliHotSearchAdapter:
    """Metadata-only adapter for the Bilibili search hot-word list.

    The top-level ``timestamp`` is the response time, not a publication time.
    ``keyword`` is used as the identity because it also defines the canonical
    search URL.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Bilibili")
        code = payload.get("code")
        if code != 0:
            raise ValueError(f"Unexpected Bilibili code: {code!r}")
        items = require_list(
            payload.get("list"),
            "Bilibili response does not contain a list",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for position, item in enumerate(items[:max_items], start=1):
            if not isinstance(item, dict):
                continue
            keyword = text(item.get("keyword"))
            metrics: dict[str, Any] = {}
            heat_score = item.get("heat_score")
            if isinstance(heat_score, (int, float)):
                metrics["heat_score"] = heat_score
            candidates.append(
                HeadlineCandidate(
                    title=text(item.get("show_name")),
                    url=(
                        SEARCH_URL_TEMPLATE.format(keyword=quote(keyword, safe=""))
                        if keyword
                        else ""
                    ),
                    external_id=keyword or None,
                    position=position,
                    metrics=metrics,
                )
            )
        return ParsedBatch(candidates=tuple(candidates))


class BilibiliHotVideoAdapter:
    """Native adapter for the Bilibili popular-video board.

    ``ps`` follows ``max_items`` (30 is accepted upstream; the default page is
    20). ``pubdate`` is the video publication time (epoch seconds) and becomes
    ``published_at`` with the raw value preserved. Author, view, and like counts
    are kept as bounded metrics.
    """

    def build_request(self, source: Source) -> RequestSpec:
        limit = source.max_items
        return json_request(
            source,
            headers={"Referer": REFERER},
            params={"ps": str(limit), "pn": "1"},
        )

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Bilibili")
        _require_ok(payload)
        data = require_mapping(
            payload.get("data"),
            "Bilibili response does not contain a data object",
        )
        items = require_list(
            data.get("list"),
            "Bilibili response does not contain a list",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            bvid = text(item.get("bvid"))
            title = text(item.get("title"))
            if not bvid or not title:
                continue
            metrics: dict[str, Any] = {}
            owner = item.get("owner")
            if isinstance(owner, dict):
                author = text(owner.get("name"))
                if author:
                    metrics["author"] = author
            stat = item.get("stat")
            if isinstance(stat, dict):
                for key in ("view", "like"):
                    value = stat.get(key)
                    if isinstance(value, int):
                        metrics[key] = value
            raw_published = text(item.get("pubdate"))
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=VIDEO_URL_TEMPLATE.format(bvid=bvid),
                    external_id=bvid,
                    published_at=parse_timestamp(raw_published or None),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Bilibili popular response contains no videos")
        return ParsedBatch(candidates=tuple(candidates))


class BilibiliRankingAdapter:
    """Native adapter for the Bilibili all-site ranking.

    The public v1 ranking JSON is used because the v2 endpoint answers anonymous
    clients with the risk-control code ``-352``. The v1 surface exposes no
    publication time, so ``published_at`` stays ``None``. Author, play count,
    and ranking points are kept as bounded metrics.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(
            source,
            headers={"Referer": REFERER},
            params={"rid": "0", "type": "all"},
        )

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Bilibili")
        _require_ok(payload)
        data = require_mapping(
            payload.get("data"),
            "Bilibili response does not contain a data object",
        )
        items = require_list(
            data.get("list"),
            "Bilibili response does not contain a list",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            bvid = text(item.get("bvid"))
            title = text(item.get("title"))
            if not bvid or not title:
                continue
            metrics: dict[str, Any] = {}
            author = text(item.get("author"))
            if author:
                metrics["author"] = author
            for key in ("play", "pts"):
                value = item.get(key)
                if isinstance(value, int):
                    metrics[key] = value
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=VIDEO_URL_TEMPLATE.format(bvid=bvid),
                    external_id=bvid,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Bilibili ranking response contains no videos")
        return ParsedBatch(candidates=tuple(candidates))


def _require_ok(payload: dict[str, Any]) -> None:
    code = payload.get("code")
    if code != 0:
        raise ValueError(f"Unexpected Bilibili code: {code!r}")
