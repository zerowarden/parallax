from __future__ import annotations

from dataclasses import replace

from parallax.adapters.common.options import option_int
from parallax.adapters.common.rss import RssAdapter, parse_feed_candidates
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
)


class MingpaoRssAdapter(RssAdapter):
    """RSS adapter for the Ming Pao realtime feed.

    The publisher's ``ins/all.xml`` feed writes a stray
    ``" target="blank`` attribute fragment into both ``<link>`` and ``<guid>``
    for some items. A double quote cannot appear unencoded in a URL, so each
    value is cut at the first quote; the shared validator then rejects any
    candidate that is still not a real HTTP(S) URL.
    """

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        candidates = parse_feed_candidates(
            response.content,
            source,
            label="Ming Pao feed",
        )
        max_items = option_int(source, "max_items", 100)
        return ParsedBatch(
            candidates=tuple(
                _strip_attribute_fragment(candidate)
                for candidate in candidates[:max_items]
            )
        )


def _strip_attribute_fragment(candidate: HeadlineCandidate) -> HeadlineCandidate:
    external_id = _cut_at_quote(candidate.external_id)
    return replace(
        candidate,
        url=_cut_at_quote(candidate.url),
        external_id=external_id or None,
    )


def _cut_at_quote(value: str | None) -> str:
    if value is None:
        return ""
    return value.split('"', 1)[0].strip()
