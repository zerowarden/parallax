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

BASE_URL = "https://www.gelonghui.com"
ARTICLE_PATH = re.compile(r"/news/(\d+)$")


class GelonghuiNewsAdapter:
    """Native adapter for the Gelonghui news listing.

    Items carry a category plus a relative age label ("刚刚", "2分钟前"), which
    is approximated to a UTC publication time while the raw label is kept.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return html_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="Gelonghui"))

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for row in tree.css(".article-content"):
            link = row.css_first(".detail-right > a")
            if link is None:
                continue
            title_node = link.css_first("h2")
            href = text(link.attributes.get("href"))
            title = title_node.text(strip=True) if title_node else ""
            if not href or not title:
                continue
            spans = row.css(".time > span")
            category = spans[0].text(strip=True) if spans else ""
            raw_published = spans[2].text(strip=True) if len(spans) > 2 else ""
            metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
            if category:
                metrics["category"] = category
            url = urljoin(BASE_URL, href)
            article_match = ARTICLE_PATH.search(urlsplit(href).path)
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=url,
                    external_id=article_match.group(1) if article_match else None,
                    published_at=parse_relative_time(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Gelonghui page does not contain any news items")
        return ParsedBatch(candidates=tuple(candidates))
