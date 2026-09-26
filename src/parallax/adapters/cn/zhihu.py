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


class ZhihuHotAdapter:
    """Metadata-only adapter for the public Zhihu web hot list.

    This surface exposes no per-item publication time. The ``card_id`` value
    carries the question ID used in the canonical URL behind a type prefix
    (``Q_``), which is stripped so identity matches the question itself.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Zhihu")
        entries = require_list(
            payload.get("data"),
            "Zhihu response does not contain a data list",
        )

        candidates = ranked_candidates(
            entries,
            max_items=source.max_items,
            build=_hot_candidate,
        )
        return extracted_batch(candidates, entries=entries, label="zhihu")


def _hot_candidate(entry: dict[str, Any], position: int) -> HeadlineCandidate | None:
    target = entry.get("target")
    if not isinstance(target, dict):
        return None
    metrics: dict[str, JsonValue] = {}
    heat = _area_text(target, "metrics_area", "text")
    if heat:
        metrics["heat"] = heat
    external_id = scalar_text(entry.get("card_id")).removeprefix("Q_")
    return HeadlineCandidate(
        title=_area_text(target, "title_area", "text"),
        url=_area_text(target, "link", "url"),
        external_id=external_id or None,
        position=position,
        metrics=metrics,
    )


def _area_text(target: dict[str, Any], area: str, key: str) -> str:
    section = target.get(area)
    if not isinstance(section, dict):
        return ""
    return text(section.get(key))
