from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from parallax.adapters.common.http import JSON_ACCEPT
from parallax.adapters.common.parsing import (
    decode_json_object,
    extracted_batch,
    parse_china_timestamp,
    ranked_candidates,
    require_list,
    scalar_text,
    text,
)
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

CHANNELS = ("zhongguo", "guandian", "gj")
FEED_URL_TEMPLATE = "http://china.cankaoxiaoxi.com/json/channel/{channel}/list.json"
_EPOCH = datetime.min.replace(tzinfo=UTC)


class CankaoxiaoxiNewsAdapter:
    """Multi-request adapter for the Cankaoxiaoxi channel listings.

    Three public channel documents are combined and ordered by upstream publish
    time, newest first. ``publishTime`` is Beijing wall time without an offset,
    so it is anchored to UTC+8 instead of being guessed as UTC.
    """

    def build_requests(self, source: Source) -> tuple[RequestSpec, ...]:
        return tuple(
            RequestSpec(
                method="GET",
                url=FEED_URL_TEMPLATE.format(channel=channel),
                headers={"Accept": JSON_ACCEPT},
            )
            for channel in CHANNELS
        )

    def parse_responses(
        self,
        source: Source,
        responses: tuple[HttpResponse, ...],
    ) -> ParsedBatch:
        raw_entries: list[object] = []
        entries: list[dict[str, Any]] = []
        for response in responses:
            payload = decode_json_object(response.content, label="Cankaoxiaoxi")
            items = require_list(
                payload.get("list"),
                "Cankaoxiaoxi response does not contain a list",
            )
            raw_entries.extend(items)
            entries.extend(item for item in items if isinstance(item, dict))
        entries.sort(key=_sort_key, reverse=True)
        candidates = ranked_candidates(
            entries,
            max_items=source.max_items,
            build=_news_candidate,
        )
        return extracted_batch(candidates, entries=raw_entries, label="cankaoxiaoxi")


def _news_candidate(entry: dict[str, Any], position: int) -> HeadlineCandidate | None:
    data = entry.get("data")
    if not isinstance(data, dict):
        return None
    title = text(data.get("title"))
    url = text(data.get("url"))
    if not title or not url:
        return None
    raw_published = text(data.get("publishTime"))
    return HeadlineCandidate(
        title=title,
        url=url,
        external_id=scalar_text(data.get("id")) or None,
        published_at=parse_china_timestamp(raw_published),
        raw_published_at=raw_published or None,
        position=position,
    )


def _sort_key(entry: dict[str, Any]) -> datetime:
    data = entry.get("data")
    if not isinstance(data, dict):
        return _EPOCH
    return parse_china_timestamp(text(data.get("publishTime"))) or _EPOCH
