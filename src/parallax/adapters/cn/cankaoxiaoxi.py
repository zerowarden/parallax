from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from parallax.adapters.common.http import JSON_ACCEPT
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import (
    decode_json_object,
    parse_china_timestamp,
    require_list,
    text,
)
from parallax.config import SourceConfig
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

    def build_requests(self, source: SourceConfig) -> tuple[RequestSpec, ...]:
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
        source: SourceConfig,
        responses: tuple[HttpResponse, ...],
    ) -> ParsedBatch:
        entries: list[dict[str, Any]] = []
        for response in responses:
            payload = decode_json_object(response.content, label="Cankaoxiaoxi")
            items = require_list(
                payload.get("list"),
                "Cankaoxiaoxi response does not contain a list",
            )
            entries.extend(item for item in items if isinstance(item, dict))
        entries.sort(key=_sort_key, reverse=True)

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for entry in entries:
            data = entry.get("data")
            if not isinstance(data, dict):
                continue
            title = text(data.get("title"))
            url = text(data.get("url"))
            if not title or not url:
                continue
            raw_published = text(data.get("publishTime"))
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=url,
                    external_id=text(data.get("id")) or None,
                    published_at=parse_china_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics={"stream_kind": source.stream_kind},
                )
            )
            if len(candidates) >= max_items:
                break
        return ParsedBatch(candidates=tuple(candidates))


def _sort_key(entry: dict[str, Any]) -> datetime:
    data = entry.get("data")
    if not isinstance(data, dict):
        return _EPOCH
    return parse_china_timestamp(text(data.get("publishTime"))) or _EPOCH
