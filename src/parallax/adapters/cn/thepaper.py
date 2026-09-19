from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import decode_json_object, text
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)
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

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for position, item in enumerate(hot_news[:max_items], start=1):
            if not isinstance(item, dict):
                continue
            cont_id = text(item.get("contId"))
            title = text(item.get("name"))
            raw_published = item.get("pubTimeLong")
            metrics: dict[str, Any] = {}
            if item.get("praiseTimes") is not None:
                metrics["praise_times"] = item.get("praiseTimes")

            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=(
                        f"https://www.thepaper.cn/newsDetail_forward_{cont_id}"
                        if cont_id
                        else ""
                    ),
                    external_id=cont_id or None,
                    published_at=parse_timestamp(raw_published),
                    raw_published_at=(
                        None if raw_published is None else str(raw_published)
                    ),
                    position=position,
                    metrics=metrics,
                )
            )

        return ParsedBatch(candidates=tuple(candidates))
