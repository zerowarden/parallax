from __future__ import annotations

import re
from urllib.parse import urljoin, urlsplit

from selectolax.parser import HTMLParser

from parallax.adapters.common.http import html_request
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import decode_html
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

BASE_URL = "https://bbs.hupu.com"
THREAD_PATH = re.compile(r"/(\d+)\.html$")


class HupuHotAdapter:
    """Native adapter for the Hupu daily hot-thread listing.

    Threads carry no publication timestamp on this surface; the thread URL is
    used as the stable identity.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return html_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="Hupu"))

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for row in tree.css("li.bbs-sl-web-post-body"):
            link = row.css_first("a.p-title")
            if link is None:
                continue
            href = (link.attributes.get("href") or "").strip()
            title = link.text(strip=True)
            if not href or not title:
                continue
            url = urljoin(BASE_URL, href)
            thread_match = THREAD_PATH.search(urlsplit(href).path)
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=url,
                    external_id=thread_match.group(1) if thread_match else None,
                    position=len(candidates) + 1,
                    metrics={"stream_kind": source.stream_kind},
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Hupu page does not contain any hot threads")
        return ParsedBatch(candidates=tuple(candidates))
