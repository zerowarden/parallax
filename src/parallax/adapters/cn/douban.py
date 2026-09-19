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

SUBJECT_URL_TEMPLATE = "https://movie.douban.com/subject/{movie_id}"


class DoubanHotMoviesAdapter:
    """Metadata-only adapter for the Douban recent-hot movie list."""

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(
            source,
            headers={"Referer": "https://movie.douban.com/"},
        )

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Douban")
        items = require_list(
            payload.get("items"),
            "Douban response does not contain an items list",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for position, item in enumerate(items[:max_items], start=1):
            if not isinstance(item, dict):
                continue
            movie_id = text(item.get("id"))
            metrics: dict[str, Any] = {}
            rating = item.get("rating")
            if isinstance(rating, dict):
                value = rating.get("value")
                if isinstance(value, (int, float)):
                    metrics["rating"] = value
            candidates.append(
                HeadlineCandidate(
                    title=text(item.get("title")),
                    url=(
                        SUBJECT_URL_TEMPLATE.format(movie_id=movie_id)
                        if movie_id
                        else ""
                    ),
                    external_id=movie_id or None,
                    position=position,
                    metrics=metrics,
                )
            )
        return ParsedBatch(candidates=tuple(candidates))
