from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import decode_json_object, require_list, text
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

FEED_DETAIL_TYPE = 74
DISCUSS_TYPE = 0
FEED_URL_TEMPLATE = "https://www.nowcoder.com/feed/main/detail/{uuid}"
DISCUSS_URL_TEMPLATE = "https://www.nowcoder.com/discuss/{entry_id}"


class NowcoderHotAdapter:
    """Native adapter for the Nowcoder hot-search list.

    The ``size`` query parameter controls the returned list length and defaults
    to 10 upstream, so it is derived from ``max_items``. Only feed entries
    (type 74) and discuss entries (type 0) have canonical detail URLs; other
    entry types are skipped rather than given a guessed URL.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        size = option_int(source, "max_items", 30)
        return json_request(source, params={"size": str(size)})

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Nowcoder")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError("Nowcoder response does not contain a data object")
        entries = require_list(
            data.get("result"),
            "Nowcoder response does not contain a result list",
        )

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_type = entry.get("type")
            entry_id = text(entry.get("id"))
            uuid = text(entry.get("uuid"))
            if entry_type == FEED_DETAIL_TYPE and uuid:
                external_id = uuid
                url = FEED_URL_TEMPLATE.format(uuid=uuid)
            elif entry_type == DISCUSS_TYPE and entry_id:
                external_id = entry_id
                url = DISCUSS_URL_TEMPLATE.format(entry_id=entry_id)
            else:
                continue
            metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
            hot_value = entry.get("hotValueFromDolphin")
            if isinstance(hot_value, (int, float)):
                metrics["hot_value"] = hot_value
            candidates.append(
                HeadlineCandidate(
                    title=text(entry.get("title")),
                    url=url,
                    external_id=external_id,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        return ParsedBatch(candidates=tuple(candidates))
