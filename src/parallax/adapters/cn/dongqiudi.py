from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import (
    decode_json_object,
    extracted_batch,
    parse_china_timestamp,
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

ARTICLE_URL_TEMPLATE = "https://www.dongqiudi.com/article/{article_id}"


class DongqiudiNewsAdapter:
    """Metadata-only adapter for the Dongqiudi web tab feed.

    The upstream article order is preserved as ``position``; it is not
    re-sorted even though ``created_at`` values are not strictly chronological.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Dongqiudi")
        articles = require_list(
            payload.get("articles"),
            "Dongqiudi response does not contain an articles list",
        )

        candidates = ranked_candidates(
            articles,
            max_items=source.max_items,
            build=_news_candidate,
        )
        return extracted_batch(candidates, entries=articles, label="dongqiudi")


def _news_candidate(entry: dict[str, Any], position: int) -> HeadlineCandidate:
    article_id = scalar_text(entry.get("id"))
    url = text(entry.get("share")) or text(entry.get("url"))
    if not url and article_id:
        url = ARTICLE_URL_TEMPLATE.format(article_id=article_id)
    raw_published = text(entry.get("created_at"))
    metrics: dict[str, JsonValue] = {}
    category = text(entry.get("category"))
    if category:
        metrics["category"] = category
    return HeadlineCandidate(
        title=text(entry.get("title")),
        url=url,
        external_id=article_id or None,
        published_at=parse_china_timestamp(raw_published),
        raw_published_at=raw_published or None,
        position=position,
        metrics=metrics,
    )
