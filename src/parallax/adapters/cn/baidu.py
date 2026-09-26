from __future__ import annotations

import json
import re
from typing import Any

from parallax.adapters.common.http import html_request
from parallax.adapters.common.parsing import (
    decode_html,
    extracted_batch,
    ranked_candidates,
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

EMBEDDED_DATA = re.compile(r"<!--s-data:(.*?)-->", re.S)


class BaiduHotSearchAdapter:
    """Native adapter for the Baidu realtime hot search board.

    The board page embeds its JSON payload in an ``s-data`` HTML comment;
    pinned ``isTop`` entries are excluded so promotion stays out of organic
    rank. The upstream ``rawUrl`` is stored unchanged because it is the
    canonical search surface for the hot word.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return html_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        html = decode_html(response.content, label="Baidu")
        match = EMBEDDED_DATA.search(html)
        if match is None:
            raise ValueError("Baidu page does not contain an s-data payload")
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid Baidu s-data JSON: {exc}") from exc
        data = require_mapping(
            payload.get("data"),
            "Baidu s-data does not contain a data object",
        )
        cards = require_list(
            data.get("cards"),
            "Baidu s-data does not contain a cards list",
        )
        if not cards:
            raise ValueError("Baidu s-data contains no cards")
        card = require_mapping(cards[0], "Baidu s-data contains an invalid card")
        entries = require_list(
            card.get("content"),
            "Baidu card does not contain a content list",
        )

        excluded = 0

        def build(entry: dict[str, Any], position: int) -> HeadlineCandidate | None:
            nonlocal excluded
            if entry.get("isTop"):
                excluded += 1
                return None
            return HeadlineCandidate(
                title=text(entry.get("word")),
                url=text(entry.get("rawUrl")),
                external_id=None,
                position=position,
            )

        candidates = ranked_candidates(entries, max_items=source.max_items, build=build)
        return extracted_batch(
            candidates, entries=entries, label="baidu", excluded_count=excluded
        )
