from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlsplit

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

BASE_URL = "https://www.wenweipo.com"
ARTICLE_PATH = re.compile(r"/(AP[0-9A-Za-z]+)\.html$")


class WenweipoNewsAdapter:
    """Native adapter for the Wen Wei Po homepage news list.

    Cards expose the article URL (which is the stable identity) and a Chinese
    relative age label, which is approximated to a UTC publication time with
    the raw label preserved.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return html_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="Wen Wei Po"))

        max_items = option_int(source, "max_items", 50)
        candidates: list[HeadlineCandidate] = []
        seen: set[str] = set()
        for card in tree.css("div.describe"):
            link = card.css_first("h3 a")
            if link is None:
                continue
            href = text(link.attributes.get("href"))
            title = link.text(strip=True) or text(link.attributes.get("title"))
            if not href or not title or href in seen:
                continue
            seen.add(href)
            article_match = ARTICLE_PATH.search(urlsplit(href).path)

            metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
            age_node = card.css_first("time.time")
            raw_published = age_node.text(strip=True) if age_node is not None else ""
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=urljoin(BASE_URL, href),
                    external_id=article_match.group(1) if article_match else None,
                    published_at=parse_relative_time(
                        raw_published, now=response.observed_at
                    ),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Wen Wei Po page does not contain any news items")
        return ParsedBatch(candidates=tuple(candidates))
