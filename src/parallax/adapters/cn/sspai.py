from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import (
    decode_json_object,
    require_list,
    text,
)
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)
from parallax.parsing import parse_timestamp

TAG = "热门文章"
DETAIL_URL_TEMPLATE = "https://sspai.com/post/{article_id}"


class SspaiHotAdapter:
    """Native adapter for the SSPAI hot-article tag list.

    The public tag API serves anonymous JSON. The reference client appends a
    ``created_at`` cache-buster; it is omitted here because the endpoint works
    without it and deterministic requests are easier to reason about. The list
    exposes ``released_time`` (publication time) and ``created_time`` (draft
    time); only the former is treated as ``published_at``.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        limit = option_int(source, "max_items", 30)
        return json_request(
            source,
            params={
                "limit": str(limit),
                "offset": "0",
                "tag": TAG,
                "released": "false",
            },
        )

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="SSPAI")
        error = payload.get("error")
        if error != 0:
            raise ValueError(f"SSPAI API error {error!r}: {text(payload.get('msg'))}")
        entries = require_list(
            payload.get("data"),
            "SSPAI response does not contain a data list",
        )

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            article_id = text(entry.get("id"))
            title = text(entry.get("title"))
            if not article_id or not title:
                continue
            metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
            for key in ("like_count", "comment_count"):
                value = entry.get(key)
                if isinstance(value, int):
                    metrics[key] = value
            raw_published = text(entry.get("released_time"))
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=DETAIL_URL_TEMPLATE.format(article_id=article_id),
                    external_id=article_id,
                    published_at=parse_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        return ParsedBatch(candidates=tuple(candidates))
