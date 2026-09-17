from __future__ import annotations

import re
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from parallax.adapters.common.http import html_request
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import decode_html, text
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

BASE_URL = "https://www.hkej.com"
DETAIL_ID = re.compile(r"/article/(\d+)/")


class HkejInstantAdapter:
    """Native adapter for the HKEJ instant-news listing.

    Titled article links live in ``h3``/``h4`` headings across several listing
    blocks, so the heading anchor is the unit rather than one card class. Rows
    expose the article id in the detail URL but no per-row publication time, so
    ``published_at`` stays ``None``.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return html_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="HKEJ"))

        max_items = option_int(source, "max_items", 50)
        candidates: list[HeadlineCandidate] = []
        seen: set[str] = set()
        for link in tree.css("h3 a[href*='/article/'], h4 a[href*='/article/']"):
            href = text(link.attributes.get("href"))
            title = link.text(strip=True)
            if not href or not title or href in seen:
                continue
            seen.add(href)

            detail = DETAIL_ID.search(href)
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=urljoin(BASE_URL, href),
                    external_id=detail.group(1) if detail else None,
                    position=len(candidates) + 1,
                    metrics={"stream_kind": source.stream_kind},
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("HKEJ page does not contain any instant-news items")
        return ParsedBatch(candidates=tuple(candidates))
