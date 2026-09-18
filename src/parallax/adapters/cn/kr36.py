from __future__ import annotations

import re
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

BASE_URL = "https://www.36kr.com"
NEWSFLASH_PATH = re.compile(r"/newsflashes/(\d+)$")


class Kr36QuickAdapter:
    """Native adapter for the 36Kr newsflash listing.

    The listing shows a relative age label ("5分钟前"), which is approximated to
    a UTC publication time while the raw label is kept. The dated
    ``/hot-list/renqi/<date>/1`` page is client-rendered and is not implemented.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return html_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="36Kr"))

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for row in tree.css(".newsflash-item"):
            link = row.css_first("a.item-title")
            time_node = row.css_first(".time")
            if link is None:
                continue
            href = text(link.attributes.get("href"))
            title = link.text(strip=True)
            if not href or not title:
                continue
            raw_published = time_node.text(strip=True) if time_node else ""
            url = urljoin(BASE_URL, href)
            newsflash_match = NEWSFLASH_PATH.search(urlsplit(href).path)
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=url,
                    external_id=(newsflash_match.group(1) if newsflash_match else None),
                    published_at=parse_relative_time(
                        raw_published, now=response.observed_at
                    ),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics={"stream_kind": source.stream_kind},
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("36Kr page does not contain any newsflashes")
        return ParsedBatch(candidates=tuple(candidates))
