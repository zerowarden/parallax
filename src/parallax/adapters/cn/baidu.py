from __future__ import annotations

import json
import re

from parallax.adapters.common.http import html_request
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import (
    decode_html,
    require_list,
    require_mapping,
    text,
)
from parallax.config import SourceConfig
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

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return html_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
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

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for entry in entries:
            if not isinstance(entry, dict) or entry.get("isTop"):
                continue
            url = text(entry.get("rawUrl"))
            candidates.append(
                HeadlineCandidate(
                    title=text(entry.get("word")),
                    url=url,
                    external_id=None,
                    position=len(candidates) + 1,
                    metrics={"stream_kind": source.stream_kind},
                )
            )
            if len(candidates) >= max_items:
                break
        return ParsedBatch(candidates=tuple(candidates))
