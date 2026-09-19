from __future__ import annotations

import re
from datetime import UTC, datetime

from selectolax.parser import HTMLParser, Node

from parallax.adapters.common.http import JSON_ACCEPT, html_request
from parallax.adapters.common.parsing import (
    decode_html,
    decode_json_object,
    require_list,
    text,
)
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)
from parallax.parsing import parse_timestamp

ITEM_URL_TEMPLATE = "https://news.ycombinator.com/item?id={item_id}"
SEARCH_URL = "https://hn.algolia.com/api/v1/search_by_date"
MAX_SEARCH_HITS = 100
_POINTS = re.compile(r"^(\d+)")


class HackerNewsHotAdapter:
    """Native adapter for the Hacker News front page.

    Rows are ``tr.athing`` entries; the score sits in the following subtext row
    (``#score_<id>``) and the submission time in its ``span.age[title]``
    attribute, which the reference implementation ignores. The stable identity
    is the HN item id and the stored URL is the canonical item permalink. Items
    without a score yet keep no ``points`` metric. HN exposes submission times
    as naive UTC timestamps.

    History uses HN's public search API, which accepts a Unix-time lower bound;
    the window is therefore exact rather than paginated.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return html_request(source)

    def build_history_requests(
        self,
        source: Source,
        since: datetime,
    ) -> tuple[RequestSpec, ...]:
        hits = min(source.max_items, MAX_SEARCH_HITS)
        return (
            RequestSpec(
                method="GET",
                url=SEARCH_URL,
                headers={"Accept": JSON_ACCEPT},
                params={
                    "tags": "story",
                    "numericFilters": f"created_at_i>{int(since.timestamp())}",
                    "hitsPerPage": str(hits),
                },
            ),
        )

    def parse_history_responses(
        self,
        source: Source,
        responses: tuple[HttpResponse, ...],
        since: datetime,
    ) -> ParsedBatch:
        payload = decode_json_object(
            responses[0].content,
            label="Hacker News search",
        )
        hits = require_list(
            payload.get("hits"),
            "Hacker News search response does not contain hits",
        )
        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for hit in hits:
            if not isinstance(hit, dict):
                continue
            item_id = text(hit.get("objectID"))
            title = text(hit.get("title"))
            if not item_id or not title:
                continue
            metrics: dict[str, object] = {}
            points = hit.get("points")
            if isinstance(points, int):
                metrics["points"] = points
            raw_published = text(hit.get("created_at_i"))
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=(
                        text(hit.get("url"))
                        or ITEM_URL_TEMPLATE.format(item_id=item_id)
                    ),
                    external_id=item_id,
                    published_at=_parse_hn_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        return ParsedBatch(candidates=tuple(candidates))

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="Hacker News"))

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for row in tree.css("tr.athing"):
            item_id = text(row.attributes.get("id"))
            link = row.css_first(".titleline a")
            if not item_id or link is None:
                continue
            title = link.text(strip=True)
            if not title:
                continue

            metrics: dict[str, object] = {}
            raw_published = ""
            subtext = _subtext_row(row)
            if subtext is not None:
                score = subtext.css_first(f"#score_{item_id}")
                points = _points(score.text(strip=True)) if score is not None else None
                if points is not None:
                    metrics["points"] = points
                age = subtext.css_first("span.age")
                if age is not None:
                    raw_published = text(age.attributes.get("title"))

            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=ITEM_URL_TEMPLATE.format(item_id=item_id),
                    external_id=item_id,
                    published_at=_parse_hn_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Hacker News page does not contain any stories")
        return ParsedBatch(candidates=tuple(candidates))


def _subtext_row(row: Node) -> Node | None:
    sibling = row.next
    while sibling is not None and sibling.tag != "tr":
        sibling = sibling.next
    return sibling


def _points(value: str) -> int | None:
    match = _POINTS.match(value)
    return int(match.group(1)) if match else None


def _parse_hn_timestamp(value: str) -> datetime | None:
    parsed = parse_timestamp(value or None)
    if parsed is not None or not value:
        return parsed
    try:
        naive = datetime.fromisoformat(value)
    except ValueError:
        return None
    if naive.tzinfo is not None:
        return naive.astimezone(UTC)
    return naive.replace(tzinfo=UTC)
