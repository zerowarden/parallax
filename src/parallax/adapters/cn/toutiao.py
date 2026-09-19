from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import decode_json_object, require_list, text
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

TRENDING_URL_TEMPLATE = "https://www.toutiao.com/trending/{cluster_id}/"


class ToutiaoHotAdapter:
    """Metadata-only adapter for the Toutiao hot board.

    The ``fixed_top_data`` slot holds promoted pinned entries and is excluded;
    only the ranked ``data`` list is collected.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Toutiao")
        items = require_list(
            payload.get("data"),
            "Toutiao response does not contain a data list",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for position, item in enumerate(items[:max_items], start=1):
            if not isinstance(item, dict):
                continue
            cluster_id = text(item.get("ClusterIdStr"))
            metrics: dict[str, Any] = {}
            hot_value = text(item.get("HotValue"))
            if hot_value:
                metrics["hot_value"] = hot_value
            candidates.append(
                HeadlineCandidate(
                    title=text(item.get("Title")),
                    url=(
                        TRENDING_URL_TEMPLATE.format(cluster_id=cluster_id)
                        if cluster_id
                        else ""
                    ),
                    external_id=cluster_id or None,
                    position=position,
                    metrics=metrics,
                )
            )
        return ParsedBatch(candidates=tuple(candidates))
