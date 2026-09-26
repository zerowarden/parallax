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
from parallax.parsing import parse_timestamp


class Hk01LatestAdapter:
    """Metadata-only adapter for HK01's public latest-page JSON response."""

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="HK01")

        items = payload.get("items")
        if not isinstance(items, list):
            raise ValueError("HK01 response does not contain an items list")

        excluded = 0

        def build(item: dict[str, Any], position: int) -> HeadlineCandidate | None:
            nonlocal excluded
            if item.get("type") == 2:
                excluded += 1
                return None
            data = item.get("data")
            if not isinstance(data, dict):
                return None
            article_id = scalar_text(data.get("articleId"))
            raw_published = data.get("publishTime")
            tags = data.get("tags")
            authors = data.get("authors")
            metrics: dict[str, JsonValue] = {}
            if isinstance(tags, list):
                metrics["tags"] = [
                    text(tag.get("tagName"))
                    for tag in tags
                    if isinstance(tag, dict) and tag.get("tagName")
                ]
            if isinstance(authors, list):
                metrics["authors"] = [
                    text(author.get("publishName"))
                    for author in authors
                    if isinstance(author, dict) and author.get("publishName")
                ]
            return HeadlineCandidate(
                title=text(data.get("title")),
                url=(
                    f"https://hk01.com/sns/article/{article_id}" if article_id else ""
                ),
                external_id=article_id or None,
                published_at=parse_timestamp(raw_published),
                raw_published_at=(
                    None if raw_published is None else str(raw_published)
                ),
                position=position,
                metrics=metrics,
            )

        candidates = ranked_candidates(items, max_items=source.max_items, build=build)
        return extracted_batch(
            candidates, entries=items, label="hk01", excluded_count=excluded
        )
