from __future__ import annotations

import json
import re
from urllib.parse import urlsplit

from parallax.adapters.common.http import html_request
from parallax.adapters.common.parsing import (
    decode_html,
    parse_china_timestamp,
    require_list,
    text,
)
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

EMBEDDED_DATA = re.compile(r"var\s+allData\s*=\s*(\{[\s\S]*?\});")
ARTICLE_PATH = re.compile(r"/c/([^/]+)$")


class IfengHotAdapter:
    """Native adapter for the Ifeng homepage hot-news strip.

    The homepage embeds its data in a ``var allData = {...};`` script
    assignment; only the ``hotNews1`` list is used. ``newsTime`` may be absent
    and is parsed as China wall time when present.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return html_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        html = decode_html(response.content, label="Ifeng")
        match = EMBEDDED_DATA.search(html)
        if match is None:
            raise ValueError("Ifeng page does not contain an allData payload")
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid Ifeng allData JSON: {exc}") from exc
        entries = require_list(
            payload.get("hotNews1"),
            "Ifeng allData does not contain a hotNews1 list",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            url = text(entry.get("url"))
            path_match = ARTICLE_PATH.search(urlsplit(url).path)
            raw_published = text(entry.get("newsTime"))
            candidates.append(
                HeadlineCandidate(
                    title=text(entry.get("title")),
                    url=url,
                    external_id=path_match.group(1) if path_match else None,
                    published_at=parse_china_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                )
            )
            if len(candidates) >= max_items:
                break
        return ParsedBatch(candidates=tuple(candidates))
