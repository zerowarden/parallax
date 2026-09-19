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

SEARCH_URL_TEMPLATE = "https://s.weibo.com/weibo?q={query}"
REFERER = "https://weibo.com/"


class WeiboHotAdapter:
    """Native adapter for the Weibo realtime hot-search surface.

    The public sidebar JSON endpoint is used instead of the ``s.weibo.com``
    summary page, which redirects anonymous clients to a passport flow and
    requires a session cookie. The JSON surface is served anonymously with a
    plain public-interface header profile. Promoted entries carry an ad marker
    and no ``realpos``; they are skipped so promotion stays out of the organic
    rank, matching the reference implementation's row filtering.

    The surface exposes no publication time, and no stable per-item ID is
    published, so the search word is the identity (as in the reference).
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source, headers={"Referer": REFERER})

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Weibo")
        if payload.get("ok") != 1:
            raise ValueError(
                f"Weibo hot-search response is not ok: {payload.get('ok')!r}"
            )
        data = require_mapping(
            payload.get("data"),
            "Weibo response does not contain a data object",
        )
        entries = require_list(
            data.get("realtime"),
            "Weibo response does not contain a realtime list",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            position = entry.get("realpos")
            if not isinstance(position, int) or position < 1:
                continue
            title = text(entry.get("word"))
            if not title:
                continue
            metrics: dict[str, Any] = {}
            heat = entry.get("num")
            if isinstance(heat, int):
                metrics["heat"] = heat
            label = text(entry.get("label_name"))
            if label:
                metrics["label"] = label
            query = text(entry.get("word_scheme")) or title
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=SEARCH_URL_TEMPLATE.format(query=quote(query, safe="")),
                    external_id=title,
                    position=position,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        return ParsedBatch(candidates=tuple(candidates))
