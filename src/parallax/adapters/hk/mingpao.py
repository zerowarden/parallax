from __future__ import annotations

from dataclasses import replace

from parallax.adapters.common.rss import RssAdapter, parse_feed_batch
from parallax.config import Source
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

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        return parse_feed_batch(
            response.content,
            source,
            label="Ming Pao feed",
            transform=_strip_attribute_fragment,
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
