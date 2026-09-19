from __future__ import annotations

from typing import Any

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

BASE_URL = "https://github.com"


class GithubTrendingAdapter:
    """Native adapter for the GitHub trending repository listing.

    Titles keep GitHub's "org /repo" formatting; only embedded newlines are
    removed. The star count is kept as a bounded metric. This surface exposes no
    publication timestamp.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return html_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="GitHub"))

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for row in tree.css("main .Box div[data-hpc] article"):
            link = row.css_first("h2 a")
            if link is None:
                continue
            href = text(link.attributes.get("href"))
            title = link.text(strip=True).replace("\n", "")
            if not href or not title:
                continue
            star_node = row.css_first("[href$='stargazers']")
            stars = text(star_node.text(strip=True)) if star_node else ""
            metrics: dict[str, Any] = {}
            if stars:
                metrics["stars"] = stars
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=f"{BASE_URL}{href}",
                    external_id=None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("GitHub page does not contain any trending repositories")
        return ParsedBatch(candidates=tuple(candidates))
