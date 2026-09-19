from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from parallax.adapters.common.http import html_request
from parallax.adapters.common.parsing import decode_html, text
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

BASE_URL = "https://www.thestandard.com.hk"
ARTICLE_ID = re.compile(r"/article/(\d+)/")


class TheStandardNewsAdapter:
    """Native adapter for The Standard news listing.

    Cards expose a stable article id in the URL and an English relative age
    label ("19 mins ago"). The label is kept as the bounded ``age`` metric but
    is not converted into a publication time, because the upstream surface
    offers no absolute clock.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return html_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="The Standard"))

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        seen: set[str] = set()
        for card in tree.css("div.list-item__container"):
            link = card.css_first("a")
            if link is None:
                continue
            href = text(link.attributes.get("href"))
            title_node = card.css_first(".list-item__title")
            title = (
                title_node.text(strip=True)
                if title_node is not None
                else link.text(strip=True)
            )
            if not href or not title or href in seen:
                continue
            seen.add(href)

            metrics: dict[str, Any] = {}
            age_node = card.css_first(".list-item__date-time")
            age = age_node.text(strip=True) if age_node is not None else ""
            if age:
                metrics["age"] = age
            article = ARTICLE_ID.search(href)
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=urljoin(BASE_URL, href),
                    external_id=article.group(1) if article else None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("The Standard page does not contain any news items")
        return ParsedBatch(candidates=tuple(candidates))
