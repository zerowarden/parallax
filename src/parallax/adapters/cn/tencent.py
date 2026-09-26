from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import (
    decode_json_object,
    extracted_batch,
    parse_china_timestamp,
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
    ParsedBatch,
    RequestSpec,
)

REFERER = "https://news.qq.com/"


class TencentHotAdapter:
    """Metadata-only adapter for Tencent News' morning-brief article list.

    The upstream ``publish_time`` value is Beijing wall time without an offset.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source, headers={"Referer": REFERER})

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Tencent")
        data = require_mapping(
            payload.get("data"),
            "Tencent response does not contain a data object",
        )
        tabs = require_list(
            data.get("tabs"),
            "Tencent response does not contain tabs",
        )
        if not tabs:
            raise ValueError("Tencent response contains no tabs")
        tab = require_mapping(tabs[0], "Tencent response has an invalid first tab")
        articles = require_list(
            tab.get("articleList"),
            "Tencent response does not contain an articleList",
        )

        candidates = ranked_candidates(
            articles,
            max_items=source.max_items,
            build=_hot_candidate,
        )
        return extracted_batch(candidates, entries=articles, label="tencent")


def _hot_candidate(entry: dict[str, Any], position: int) -> HeadlineCandidate:
    link_info = entry.get("link_info")
    link_url = text(link_info.get("url")) if isinstance(link_info, dict) else ""
    raw_published = scalar_text(entry.get("publish_time"))
    return HeadlineCandidate(
        title=text(entry.get("title")),
        url=link_url,
        external_id=scalar_text(entry.get("id")) or None,
        published_at=parse_china_timestamp(raw_published),
        raw_published_at=raw_published or None,
        position=position,
    )
