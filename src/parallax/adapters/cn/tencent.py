from __future__ import annotations

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import (
    decode_json_object,
    parse_china_timestamp,
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

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for position, item in enumerate(articles[:max_items], start=1):
            if not isinstance(item, dict):
                continue
            link_info = item.get("link_info")
            link_url = text(link_info.get("url")) if isinstance(link_info, dict) else ""
            raw_published = text(item.get("publish_time"))
            candidates.append(
                HeadlineCandidate(
                    title=text(item.get("title")),
                    url=link_url,
                    external_id=text(item.get("id")) or None,
                    published_at=parse_china_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=position,
                )
            )
        return ParsedBatch(candidates=tuple(candidates))
