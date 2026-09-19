from __future__ import annotations

import json
import re
from urllib.parse import quote

from parallax.adapters.common.http import html_request
from parallax.adapters.common.parsing import (
    decode_html,
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

BASE_URL = "https://www.kuaishou.com"
APOLLO_STATE = re.compile(r"window\.__APOLLO_STATE__\s*=\s*(\{.+?\});")
HOT_RANK_KEY = 'visionHotRank({"page":"home"})'
ITEM_PREFIX = "VisionHotRankItem:"
PINNED_TAG = "置顶"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)


class KuaishouHotAdapter:
    """Native adapter for the Kuaishou home hot-rank list.

    The homepage embeds its data in ``window.__APOLLO_STATE__``; a static
    desktop Chrome user agent is required (the generic one is blocked). Pinned
    (``置顶``) entries are excluded and the hot-search word is the identity.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return html_request(
            source,
            headers={"User-Agent": BROWSER_USER_AGENT},
        )

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        html = decode_html(response.content, label="Kuaishou")
        match = APOLLO_STATE.search(html)
        if match is None:
            raise ValueError("Kuaishou page does not contain window.__APOLLO_STATE__")
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid Kuaishou APOLLO JSON: {exc}") from exc

        client = require_mapping(
            payload.get("defaultClient"),
            "Kuaishou APOLLO state does not contain a client",
        )
        root_query = require_mapping(
            client.get("ROOT_QUERY"),
            "Kuaishou APOLLO state does not contain a ROOT_QUERY",
        )
        rank_ref = require_mapping(
            root_query.get(HOT_RANK_KEY),
            "Kuaishou APOLLO state does not contain the hot rank",
        )
        rank = require_mapping(
            client.get(text(rank_ref.get("id"))),
            "Kuaishou hot-rank node is missing",
        )
        items = require_list(
            rank.get("items"),
            "Kuaishou hot-rank node does not contain items",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            item_id = text(item.get("id"))
            node = client.get(item_id)
            if not isinstance(node, dict) or node.get("tagType") == PINNED_TAG:
                continue
            word = item_id.removeprefix(ITEM_PREFIX)
            title = text(node.get("name"))
            if not word or not title:
                continue
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=f"{BASE_URL}/search/video?searchKey={quote(title, safe='')}",
                    external_id=word,
                    position=len(candidates) + 1,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Kuaishou hot rank does not contain any items")
        return ParsedBatch(candidates=tuple(candidates))
