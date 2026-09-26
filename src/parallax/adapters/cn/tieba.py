from __future__ import annotations

import html
from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import (
    decode_json_object,
    extracted_batch,
    ranked_candidates,
    require_list,
    require_mapping,
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
from parallax.numbers import optional_integer
from parallax.parsing import parse_timestamp


class TiebaHotAdapter:
    """Native adapter for the Tieba hot-topic list.

    ``topic_url`` is HTML-escaped upstream (``&amp;``), so it is unescaped to
    produce a usable URL; ``topic_id`` is the stable identity. ``create_time``
    is Unix seconds and is preserved as publication time. The list order is the
    upstream rank.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Tieba")
        data = require_mapping(
            payload.get("data"),
            "Tieba response does not contain a data object",
        )
        bang_topic = require_mapping(
            data.get("bang_topic"),
            "Tieba response does not contain bang_topic",
        )
        entries = require_list(
            bang_topic.get("topic_list"),
            "Tieba response does not contain a topic_list",
        )

        candidates = ranked_candidates(
            entries,
            max_items=source.max_items,
            build=_hot_candidate,
        )
        return extracted_batch(candidates, entries=entries, label="tieba")


def _hot_candidate(entry: dict[str, Any], position: int) -> HeadlineCandidate | None:
    external_id = scalar_text(entry.get("topic_id"))
    title = text(entry.get("topic_name"))
    url = html.unescape(text(entry.get("topic_url")))
    if not external_id or not title or not url:
        return None
    metrics: dict[str, JsonValue] = {}
    discuss_count = optional_integer(entry.get("discuss_num"))
    if discuss_count is not None:
        metrics["discuss_count"] = discuss_count
    raw_published = scalar_text(entry.get("create_time"))
    return HeadlineCandidate(
        title=title,
        url=url,
        external_id=external_id,
        published_at=parse_timestamp(raw_published),
        raw_published_at=raw_published or None,
        position=position,
        metrics=metrics,
    )
