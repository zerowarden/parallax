from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import decode_json_object, text
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)
from parallax.parsing import parse_timestamp


class Hk01LatestAdapter:
    """Metadata-only adapter for HK01's public latest-page JSON response."""

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return json_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="HK01")

        items = payload.get("items")
        if not isinstance(items, list):
            raise ValueError("HK01 response does not contain an items list")

        max_items = option_int(source, "max_items", 50)
        candidates: list[HeadlineCandidate] = []
        for item in items:
            if not isinstance(item, dict) or item.get("type") == 2:
                continue
            data = item.get("data")
            if not isinstance(data, dict):
                continue

            article_id = text(data.get("articleId"))
            raw_published = data.get("publishTime")
            tags = data.get("tags")
            authors = data.get("authors")
            metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
            if isinstance(tags, list):
                metrics["tags"] = [
                    tag.get("tagName")
                    for tag in tags
                    if isinstance(tag, dict) and tag.get("tagName")
                ]
            if isinstance(authors, list):
                metrics["authors"] = [
                    author.get("publishName")
                    for author in authors
                    if isinstance(author, dict) and author.get("publishName")
                ]

            candidates.append(
                HeadlineCandidate(
                    title=text(data.get("title")),
                    url=(
                        f"https://hk01.com/sns/article/{article_id}"
                        if article_id
                        else ""
                    ),
                    external_id=article_id or None,
                    published_at=parse_timestamp(raw_published),
                    raw_published_at=(
                        None if raw_published is None else str(raw_published)
                    ),
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break

        return ParsedBatch(candidates=tuple(candidates))
