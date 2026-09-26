from __future__ import annotations

from datetime import datetime
from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.options import (
    DEFAULT_HISTORY_MAX_PAGES,
    DEFAULT_HISTORY_PAGE_SIZE,
    option_int,
)
from parallax.adapters.common.parsing import decode_json_list, text
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HistoryPage,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)
from parallax.parsing import parse_timestamp

PLAYER_URL_TEMPLATE = "https://news.now.com/home/local/player?newsId={news_id}"
AD_STORY_PREFIX = "NM-JOBAD"


class NowNewsAdapter:
    """Native adapter for the Now News ranked news JSON API.

    The public API returns a paginated ranked list. Entries whose
    ``storyTitle`` carries the observed recruitment-ad marker are skipped.
    ``publishDate`` is a millisecond epoch. History requests walk the pages
    within a bounded page count and keep only items inside the window.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return _page_request(source, page=1)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        entries = decode_json_list(response.content, label="Now News")
        batch = _parse_entries(source, entries, since=None)
        if not batch.candidates:
            raise ValueError("Now News response contains no articles")
        return batch

    def build_history_requests(
        self,
        source: Source,
        since: datetime,
    ) -> tuple[RequestSpec, ...]:
        pages = option_int(source, "history_max_pages", DEFAULT_HISTORY_MAX_PAGES)
        size = option_int(source, "history_page_size", DEFAULT_HISTORY_PAGE_SIZE)
        return tuple(
            _page_request(source, page=page, size=size) for page in range(1, pages + 1)
        )

    def parse_history_page(
        self,
        source: Source,
        response: HttpResponse,
        since: datetime,
    ) -> HistoryPage:
        entries = decode_json_list(response.content, label="Now News")
        return HistoryPage(
            _parse_entries(source, entries, since=since), exhausted=not entries
        )


def _page_request(source: Source, *, page: int, size: int | None = None) -> RequestSpec:
    size = source.max_items if size is None else size
    return json_request(
        source,
        params={"pageSize": str(size), "pageNo": str(page)},
    )


def _parse_entries(
    source: Source,
    entries: list[object],
    since: datetime | None,
) -> ParsedBatch:
    max_items = source.max_items
    candidates: list[HeadlineCandidate] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        news_id = text(entry.get("newsId"))
        title = text(entry.get("title"))
        if not news_id or not title or news_id in seen:
            continue
        if text(entry.get("storyTitle")).startswith(AD_STORY_PREFIX):
            continue
        raw_published = text(entry.get("publishDate"))
        published = parse_timestamp(raw_published)
        if since is not None and (published is None or published < since):
            continue
        seen.add(news_id)
        metrics: dict[str, Any] = {}
        publisher = text(entry.get("newsSource"))
        if publisher:
            metrics["source"] = publisher
        views = text(entry.get("viewCount"))
        if views.isdigit():
            metrics["view_count"] = int(views)
        candidates.append(
            HeadlineCandidate(
                title=title,
                url=PLAYER_URL_TEMPLATE.format(news_id=news_id),
                external_id=news_id,
                published_at=published,
                raw_published_at=raw_published or None,
                position=len(candidates) + 1,
                metrics=metrics,
            )
        )
        if since is None and len(candidates) >= max_items:
            return ParsedBatch(candidates=tuple(candidates))
    return ParsedBatch(candidates=tuple(candidates))
