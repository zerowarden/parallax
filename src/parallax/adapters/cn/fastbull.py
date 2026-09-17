from __future__ import annotations

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
from parallax.parsing import parse_timestamp

BASE_URL = "https://www.fastbull.com"


class FastbullExpressAdapter:
    """Native adapter for the Fastbull express-news listing.

    Rows carry `data-href`/`data-id` identifiers and a millisecond epoch
    `data-date`; the constructed article URL is used as stable identity.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return html_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="Fastbull"))

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for row in tree.css(".content-list.news-list"):
            title_node = row.css_first(".title_name")
            external_id = text(row.attributes.get("data-id"))
            href = text(row.attributes.get("data-href")) or external_id
            raw_published = text(row.attributes.get("data-date"))
            title = title_node.text(strip=True) if title_node else ""
            if not href or not title:
                continue
            path = href if href.startswith("/") else f"/cn/fastshort/{href}"
            url = f"{BASE_URL}{path}"
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=url,
                    external_id=external_id or None,
                    published_at=parse_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics={"stream_kind": source.stream_kind},
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Fastbull page does not contain any express news")
        return ParsedBatch(candidates=tuple(candidates))


class FastbullNewsAdapter:
    """Native adapter for the Fastbull news listing.

    Only the 头条新闻 headline block is observed; the ``.report_list`` research
    and analysis stream (``/cn/newsdetail/``) is a different surface and is
    explicitly excluded. Rows are anchors carrying ``data-date`` (millisecond
    epoch) descendants; the article URL is used as stable identity.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return html_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="Fastbull"))

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for row in tree.css(".news-top .trending_type"):
            title_node = row.css_first(".title")
            date_node = row.css_first("[data-date]")
            href = text(row.attributes.get("href"))
            external_id = text(row.attributes.get("data-id"))
            title = title_node.text(strip=True) if title_node else ""
            if not href or not title:
                continue
            raw_published = text(
                date_node.attributes.get("data-date") if date_node else ""
            )
            url = href if href.startswith("http") else f"{BASE_URL}{href}"
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=url,
                    external_id=external_id or None,
                    published_at=parse_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics={"stream_kind": source.stream_kind},
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Fastbull page does not contain any news items")
        return ParsedBatch(candidates=tuple(candidates))
