from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from parallax.adapters.common.rss import parse_feed_candidates
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

FEED_URL_TEMPLATE = "https://www.hket.com/rss/{section}"
SECTION_FEEDS = (
    "hongkong",
    "finance",
    "china",
    "world",
    "lifestyle",
    "technology",
    "entertainment",
)
DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
_EPOCH = datetime.min.replace(tzinfo=UTC)


class HketRssAdapter:
    """Multi-feed adapter for the Hket section RSS feeds.

    The feed host answers the generic client with HTTP 403, so each request
    carries a static desktop browser user agent; the HTML pages need no such
    header. Section feeds are combined, deduplicated by external ID, and
    ordered newest first, with positions reassigned to the merged order.
    """

    def build_requests(self, source: Source) -> tuple[RequestSpec, ...]:
        return tuple(
            RequestSpec(
                method="GET",
                url=FEED_URL_TEMPLATE.format(section=section),
                headers={
                    "Accept": (
                        "application/rss+xml, application/atom+xml, "
                        "application/xml, text/xml;q=0.9, */*;q=0.1"
                    ),
                    "User-Agent": DESKTOP_USER_AGENT,
                },
            )
            for section in SECTION_FEEDS
        )

    def parse_responses(
        self,
        source: Source,
        responses: tuple[HttpResponse, ...],
    ) -> ParsedBatch:
        merged: list[HeadlineCandidate] = []
        seen: set[str] = set()
        for response in responses:
            for candidate in parse_feed_candidates(
                response.content,
                source,
                label="Hket feed",
            ):
                key = candidate.external_id or candidate.url
                if not key or key in seen:
                    continue
                seen.add(key)
                merged.append(candidate)

        merged.sort(key=_recency, reverse=True)
        max_items = source.max_items
        candidates = tuple(
            replace(candidate, position=position)
            for position, candidate in enumerate(merged[:max_items], start=1)
        )
        if not candidates:
            raise ValueError("Hket feeds contain no items")
        return ParsedBatch(candidates=candidates)


def _recency(candidate: HeadlineCandidate) -> datetime:
    return candidate.published_at or _EPOCH
