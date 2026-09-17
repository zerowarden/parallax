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


class ZhihuHotAdapter:
    """Metadata-only adapter for the public Zhihu web hot list.

    This surface exposes no per-item publication time. The ``card_id`` value
    carries the question ID used in the canonical URL behind a type prefix
    (``Q_``), which is stripped so identity matches the question itself.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return json_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Zhihu")
        entries = require_list(
            payload.get("data"),
            "Zhihu response does not contain a data list",
        )

        max_items = option_int(source, "max_items", 20)
        candidates: list[HeadlineCandidate] = []
        for position, entry in enumerate(entries[:max_items], start=1):
            if not isinstance(entry, dict):
                continue
            target = entry.get("target")
            if not isinstance(target, dict):
                continue
            metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
            heat = _area_text(target, "metrics_area", "text")
            if heat:
                metrics["heat"] = heat
            external_id = text(entry.get("card_id")).removeprefix("Q_")
            candidates.append(
                HeadlineCandidate(
                    title=_area_text(target, "title_area", "text"),
                    url=_area_text(target, "link", "url"),
                    external_id=external_id or None,
                    position=position,
                    metrics=metrics,
                )
            )
        return ParsedBatch(candidates=tuple(candidates))


def _area_text(target: dict[str, Any], area: str, key: str) -> str:
    section = target.get(area)
    if not isinstance(section, dict):
        return ""
    return text(section.get(key))
