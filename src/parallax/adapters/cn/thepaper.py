from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import (
    decode_json_object,
    extracted_batch,
    ranked_candidates,
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


class ThePaperHotAdapter:
    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="The Paper")

        if payload.get("resultCode") != 1:
            raise ValueError(
                f"Unexpected The Paper resultCode: {payload.get('resultCode')!r}"
            )

        hot_news = payload.get("data", {}).get("hotNews")
        if not isinstance(hot_news, list):
            raise ValueError("The Paper response does not contain data.hotNews")

        candidates = ranked_candidates(
            hot_news,
            max_items=source.max_items,
            build=_hot_candidate,
        )
        return extracted_batch(candidates, entries=hot_news, label="thepaper")


def _hot_candidate(entry: dict[str, Any], position: int) -> HeadlineCandidate:
    cont_id = scalar_text(entry.get("contId"))
    metrics: dict[str, JsonValue] = {}
    praise_times = optional_integer(entry.get("praiseTimes"))
    if praise_times is not None:
        metrics["praise_times"] = praise_times
    raw_published = entry.get("pubTimeLong")
    return HeadlineCandidate(
        title=text(entry.get("name")),
        url=f"https://www.thepaper.cn/newsDetail_forward_{cont_id}" if cont_id else "",
        external_id=cont_id or None,
        published_at=parse_timestamp(raw_published),
        raw_published_at=None if raw_published is None else str(raw_published),
        position=position,
        metrics=metrics,
    )
