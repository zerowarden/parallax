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

BASE_URL = "https://www.am730.com.hk"
ARTICLE_ID = re.compile(r"/(\d+)/")


class Am730NewsAdapter:
    """Native adapter for the am730 news list.

    Cards expose a stable article id in the URL, a section label, and a Chinese
    relative age label ("2分鐘前"), which is approximated to a UTC publication
    time with the raw label preserved.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return html_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="am730"))

        max_items = option_int(source, "max_items", 50)
        candidates: list[HeadlineCandidate] = []
        seen: set[str] = set()
        for card in tree.css("li.newslist-item"):
            link = card.css_first(".newsitem-title a")
            if link is None:
                continue
            href = text(link.attributes.get("href"))
            title = link.text(strip=True)
            if not href or not title or href in seen:
                continue
            seen.add(href)

            metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
            section = card.css_first(".newsitem-unit a")
            section_name = section.text(strip=True) if section is not None else ""
            if section_name:
                metrics["section"] = section_name
            age_node = card.css_first(".newsitem-time")
            raw_published = age_node.text(strip=True) if age_node is not None else ""
            article = ARTICLE_ID.search(href)
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=urljoin(BASE_URL, href),
                    external_id=article.group(1) if article else None,
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
            raise ValueError("am730 page does not contain any news items")
        return ParsedBatch(candidates=tuple(candidates))
