from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import (
    decode_json_object,
    extracted_batch,
    ranked_candidates,
    require_list,
    scalar_text,
    text,
)
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    JsonValue,
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

        candidates = ranked_candidates(
            items,
            max_items=source.max_items,
            build=_hot_candidate,
        )
        return extracted_batch(candidates, entries=items, label="toutiao")


def _hot_candidate(entry: dict[str, Any], position: int) -> HeadlineCandidate:
    cluster_id = scalar_text(entry.get("ClusterIdStr"))
    metrics: dict[str, JsonValue] = {}
    hot_value = scalar_text(entry.get("HotValue"))
    if hot_value:
        metrics["hot_value"] = hot_value
    return HeadlineCandidate(
        title=text(entry.get("Title")),
        url=TRENDING_URL_TEMPLATE.format(cluster_id=cluster_id) if cluster_id else "",
        external_id=cluster_id or None,
        position=position,
        metrics=metrics,
    )
