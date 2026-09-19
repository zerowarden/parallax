from __future__ import annotations

import re
from urllib.parse import urljoin, urlsplit

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
from parallax.parsing import parse_timestamp

BASE_URL = "https://sputniknews.cn"
ARTICLE_PATH = re.compile(r"/(\d+)\.html$")


class SputnikNewsAdapter:
    """Native adapter for the Sputnik China news lenta widget.

    Each item carries a `data-unixtime` attribute with the publication time in
    epoch seconds; the article URL is used as the stable identity.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return html_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="Sputnik"))

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for row in tree.css(".lenta__item"):
            link = row.css_first("a")
            title_node = row.css_first(".lenta__item-text")
            date_node = row.css_first(".lenta__item-date")
            if link is None or title_node is None:
                continue
            href = (link.attributes.get("href") or "").strip()
            title = title_node.text(strip=True)
            if not href or not title:
                continue
            url = urljoin(BASE_URL, href)
            article_match = ARTICLE_PATH.search(urlsplit(href).path)
            raw_published = text(
                date_node.attributes.get("data-unixtime") if date_node else ""
            )
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=url,
                    external_id=article_match.group(1) if article_match else None,
                    published_at=parse_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Sputnik page does not contain any news items")
        return ParsedBatch(candidates=tuple(candidates))
