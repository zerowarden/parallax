from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from parallax.adapters.common.http import html_request
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import (
    decode_html,
    parse_relative_time,
    text,
)
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

BASE_URL = "https://www.tkww.hk"
AGE_LABEL = re.compile(r"\d+\s*(?:分鐘|小時|天|日|週|月|年)前")


class TkwwNewsAdapter:
    """Native adapter for the Ta Kung Wen Wei homepage story list.

    Cards carry a stable ``data-storyid``, a title anchor, and a Chinese
    relative age label, which is approximated to a UTC publication time with
    the raw label preserved.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return html_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="Takungpao"))

        max_items = option_int(source, "max_items", 50)
        candidates: list[HeadlineCandidate] = []
        seen: set[str] = set()
        for card in tree.css("div.story-list-unit-inner"):
            link = card.css_first("a[title]")
            if link is None:
                link = card.css_first(".story-list-unit-title-box a")
            if link is None:
                link = card.css_first("a[href*='/a/']")
            if link is None:
                continue
            href = text(link.attributes.get("href"))
            title = text(link.attributes.get("title")) or link.text(strip=True)
            if not href or not title or href in seen:
                continue
            seen.add(href)

            story = card.css_first("[data-storyid]")
            external_id = text(story.attributes.get("data-storyid")) if story else ""
            age = AGE_LABEL.search(card.text(separator=" ", strip=True))
            raw_published = age.group(0) if age is not None else ""
            metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=urljoin(BASE_URL, href),
                    external_id=external_id or None,
                    published_at=parse_relative_time(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Takungpao page does not contain any news items")
        return ParsedBatch(candidates=tuple(candidates))
