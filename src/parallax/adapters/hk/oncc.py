from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from parallax.adapters.common.http import html_request
from parallax.adapters.common.parsing import (
    CHINA_STANDARD_TIME,
    decode_html,
    text,
)
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

BASE_URL = "https://hk.on.cc"
PUBDATE_FORMAT = "%Y%m%d%H%M%S"


class OnccNewsAdapter:
    """Native adapter for the On.cc news index.

    Each card carries a compact ``pubdate`` attribute
    (``YYYYMMDDHHMMSS`` in Hong Kong wall time) and a stable ``rel`` story key;
    the anchor href supplies the canonical article URL.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return html_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="On.cc"))

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        seen: set[str] = set()
        for card in tree.css("div.focusItem[pubdate]"):
            link = card.css_first("a[href^='/hk/bkn/cnt/']")
            title_node = card.css_first(".focusTitle")
            title = title_node.text(strip=True) if title_node is not None else ""
            href = text(link.attributes.get("href")) if link is not None else ""
            if not href or not title or href in seen:
                continue
            seen.add(href)

            raw_published = text(card.attributes.get("pubdate"))
            metrics: dict[str, Any] = {}
            district = text(card.attributes.get("district"))
            if district:
                metrics["district"] = district
            external_id = text(card.attributes.get("rel"))
            if not external_id:
                external_id = href.rsplit("/", 1)[-1].removesuffix(".html")
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=urljoin(BASE_URL, href),
                    external_id=external_id or None,
                    published_at=_parse_pubdate(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("On.cc page does not contain any news items")
        return ParsedBatch(candidates=tuple(candidates))


def _parse_pubdate(value: str) -> datetime | None:
    if len(value) != 14 or not value.isdigit():
        return None
    try:
        local = datetime.strptime(value, PUBDATE_FORMAT)
    except ValueError:
        return None
    return local.replace(tzinfo=CHINA_STANDARD_TIME).astimezone(UTC)
