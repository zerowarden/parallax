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
from parallax.numbers import optional_finite_number

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

        candidates = ranked_candidates(
            items,
            max_items=source.max_items,
            build=_hot_candidate,
        )
        return extracted_batch(candidates, entries=items, label="douban")


def _hot_candidate(entry: dict[str, Any], position: int) -> HeadlineCandidate:
    movie_id = scalar_text(entry.get("id"))
    metrics: dict[str, JsonValue] = {}
    rating = entry.get("rating")
    if isinstance(rating, dict):
        value = optional_finite_number(rating.get("value"))
        if value is not None:
            metrics["rating"] = value
    return HeadlineCandidate(
        title=text(entry.get("title")),
        url=SUBJECT_URL_TEMPLATE.format(movie_id=movie_id) if movie_id else "",
        external_id=movie_id or None,
        position=position,
        metrics=metrics,
    )
