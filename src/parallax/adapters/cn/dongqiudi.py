from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import (
    decode_json_object,
    parse_china_timestamp,
    require_list,
    text,
)
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

ARTICLE_URL_TEMPLATE = "https://www.dongqiudi.com/article/{article_id}"


class DongqiudiNewsAdapter:
    """Metadata-only adapter for the Dongqiudi web tab feed.

    The upstream article order is preserved as ``position``; it is not
    re-sorted even though ``created_at`` values are not strictly chronological.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return json_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Dongqiudi")
        articles = require_list(
            payload.get("articles"),
            "Dongqiudi response does not contain an articles list",
        )

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for position, article in enumerate(articles[:max_items], start=1):
            if not isinstance(article, dict):
                continue
            article_id = text(article.get("id"))
            url = text(article.get("share")) or text(article.get("url"))
            if not url and article_id:
                url = ARTICLE_URL_TEMPLATE.format(article_id=article_id)
            raw_published = text(article.get("created_at"))
            metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
            category = text(article.get("category"))
            if category:
                metrics["category"] = category
            candidates.append(
                HeadlineCandidate(
                    title=text(article.get("title")),
                    url=url,
                    external_id=article_id or None,
                    published_at=parse_china_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=position,
                    metrics=metrics,
                )
            )
        return ParsedBatch(candidates=tuple(candidates))
