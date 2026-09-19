from __future__ import annotations

from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from parallax.adapters.common.http import html_request
from parallax.adapters.common.parsing import decode_html
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

BASE_URL = "https://kaopu.news"
DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


class KaopuNewsAdapter:
    """Native adapter for the Kaopu aggregated story listing.

    Kaopu's Cloudflare edge answers the generic client and a Windows Chrome
    profile with a 403 challenge; a static macOS Chrome user agent is served the
    page (the reference implementation documents the same workaround). The
    listing exposes a first-published date and an update-recency label but no
    publication clock, so ``published_at`` stays ``None`` and the raw label is
    preserved as the ``recency`` metric. The publisher list is kept as bounded
    ``provenance`` metadata; story pages are not fetched.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return html_request(source, headers={"User-Agent": DESKTOP_USER_AGENT})

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="Kaopu"))

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        seen: set[str] = set()
        for article in tree.css("article"):
            link = article.css_first('a[href^="/story/"]')
            title_node = article.css_first("h2")
            if link is None or title_node is None:
                continue
            href = (link.attributes.get("href") or "").strip()
            title = title_node.text(strip=True)
            if not href or not title or href in seen:
                continue
            seen.add(href)

            metrics: dict[str, object] = {}
            meta = article.css_first(".story-meta span")
            recency = meta.text(strip=True) if meta is not None else ""
            if recency:
                metrics["recency"] = recency
            provenance = article.css_first(".story-provenance")
            publishers = provenance.text(strip=True) if provenance is not None else ""
            if publishers:
                metrics["provenance"] = publishers

            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=urljoin(BASE_URL, href),
                    external_id=None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Kaopu page does not contain any stories")
        return ParsedBatch(candidates=tuple(candidates))
