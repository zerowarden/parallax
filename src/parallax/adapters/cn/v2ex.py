from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from parallax.adapters.common.http import JSON_ACCEPT
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import decode_json_object, require_list, text
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)
from parallax.parsing import parse_timestamp

FEED_URL_TEMPLATE = "https://www.v2ex.com/feed/{route}.json"
FEED_ROUTES = ("create", "ideas", "programmer", "share")


class V2exShareAdapter:
    """Multi-request adapter for the public V2EX JSON feeds.

    Four JSON Feed documents (create/ideas/programmer/share) are combined and
    ordered by modification or publication time, newest first.
    """

    def build_requests(self, source: SourceConfig) -> tuple[RequestSpec, ...]:
        return tuple(
            RequestSpec(
                method="GET",
                url=FEED_URL_TEMPLATE.format(route=route),
                headers={"Accept": JSON_ACCEPT},
            )
            for route in FEED_ROUTES
        )

    def parse_responses(
        self,
        source: SourceConfig,
        responses: tuple[HttpResponse, ...],
    ) -> ParsedBatch:
        entries: list[dict[str, Any]] = []
        for response in responses:
            payload = decode_json_object(response.content, label="V2EX")
            items = require_list(
                payload.get("items"),
                "V2EX feed does not contain an items list",
            )
            entries.extend(item for item in items if isinstance(item, dict))
        entries.sort(key=_sort_key, reverse=True)

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for position, entry in enumerate(entries[:max_items], start=1):
            raw_published = text(entry.get("date_published"))
            candidates.append(
                HeadlineCandidate(
                    title=text(entry.get("title")),
                    url=text(entry.get("url")),
                    external_id=text(entry.get("id")) or None,
                    published_at=parse_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=position,
                    metrics={"stream_kind": source.stream_kind},
                )
            )
        return ParsedBatch(candidates=tuple(candidates))


def _sort_key(entry: dict[str, Any]) -> datetime:
    raw = text(entry.get("date_modified")) or text(entry.get("date_published"))
    return parse_timestamp(raw) or datetime.min.replace(tzinfo=UTC)
