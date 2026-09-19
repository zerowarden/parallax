from __future__ import annotations

import re
from urllib.parse import urljoin, urlsplit

from selectolax.parser import HTMLParser

from parallax.adapters.common.http import html_request
from parallax.adapters.common.parsing import decode_html, parse_china_timestamp, text
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

BASE_URL = "https://www.zaochenbao.com"
ARTICLE_PATH = re.compile(r"/(\d+)\.html$")


class ZaobaoRealtimeAdapter:
    """Native adapter for the Zaochenbao realtime listing.

    The page is GB2312-encoded (decoded as GB18030). Its clock label renders as
    ``2026-09-17- 14:15:46``, so the stray ``- `` before the time is
    normalized before parsing.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return html_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(
            decode_html(response.content, label="Zaobao", encoding="gb18030")
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for row in tree.css("div.list-block > a.item"):
            href = text(row.attributes.get("href"))
            title_node = row.css_first(".eps")
            date_node = row.css_first(".pdt10")
            title = title_node.text(strip=True) if title_node else ""
            if not href or not title:
                continue
            raw_published = (
                text(date_node.text(strip=True)).replace("- ", " ") if date_node else ""
            )
            url = urljoin(BASE_URL, href)
            article_match = ARTICLE_PATH.search(urlsplit(href).path)
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=url,
                    external_id=article_match.group(1) if article_match else None,
                    published_at=parse_china_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Zaobao page does not contain any news items")
        return ParsedBatch(candidates=tuple(candidates))
