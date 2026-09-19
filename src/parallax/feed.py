from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol

from parallax.domain import BrowseView
from parallax.normalization import normalize_title_for_version
from parallax.storage import HeadlineRow

FeedOrder = Literal["observed", "published"]
PAGE_SIZE = 100


@dataclass(frozen=True, slots=True)
class HeadlineGroup:
    """One exact story group with every observed appearance."""

    representative: HeadlineRow
    appearances: tuple[HeadlineRow, ...]


@dataclass(frozen=True, slots=True)
class HeadlineFeedRow:
    title: str
    url: str
    published_at: str | None
    first_seen_at: str
    source_name: str
    duplicate_count: int


@dataclass(frozen=True, slots=True)
class BrowsePage:
    """One bounded page of source-local headlines for a browse view."""

    items: tuple[HeadlineFeedRow, ...]
    page: int
    total_pages: int


class BrowseHeadlineReader(Protocol):
    def count_browse_headlines(
        self,
        *,
        view: BrowseView,
        since: datetime,
        until: datetime,
        query: str | None = None,
        source_id: str | None = None,
    ) -> int: ...

    def browse_headlines(
        self,
        *,
        view: BrowseView,
        since: datetime,
        until: datetime,
        query: str | None = None,
        source_id: str | None = None,
        limit: int,
        offset: int,
    ) -> list[HeadlineRow]: ...


def browse_items(
    storage: BrowseHeadlineReader,
    *,
    view: BrowseView,
    since: datetime,
    until: datetime,
    query: str | None = None,
    source_id: str | None = None,
    page: int = 1,
    page_size: int = PAGE_SIZE,
) -> BrowsePage:
    """Read one bounded page of historical items for a top-level browse view.

    Browser pagination intentionally keeps source-local rows. The terminal's
    transitive cross-source grouping requires the complete result set, which
    would defeat bounded SQL pagination here.
    """
    if page_size < 1:
        raise ValueError("page_size must be positive")
    total_items = storage.count_browse_headlines(
        view=view,
        since=since,
        until=until,
        query=query,
        source_id=source_id,
    )
    total_pages = max(1, (total_items + page_size - 1) // page_size)
    current_page = min(max(page, 1), total_pages)
    rows = storage.browse_headlines(
        view=view,
        since=since,
        until=until,
        query=query,
        source_id=source_id,
        limit=page_size,
        offset=(current_page - 1) * page_size,
    )
    return BrowsePage(
        items=tuple(_headline_feed_row(row, duplicate_count=0) for row in rows),
        page=current_page,
        total_pages=total_pages,
    )


def build_headline_groups(
    rows: Iterable[HeadlineRow],
    *,
    order: FeedOrder,
) -> tuple[HeadlineGroup, ...]:
    """Collapse per-source headlines into exact story groups.

    Two observations describe the same resource when their stored canonical
    URLs match, or when their normalized headlines and item kinds match;
    equality is transitive, so the union of both relations forms the groups.
    ``observed`` groups include every row and rank by the latest
    ``first_seen_at``; ``published`` groups require a known ``published_at``
    and rank strictly by it. This is a read model: nothing is persisted and
    per-source identity and provenance remain untouched in storage.
    """
    ordered = _ordered_rows(rows, order)
    keys = [_identity_keys(row, index) for index, row in enumerate(ordered)]
    parent: dict[str, str] = {}
    for url_key, title_key in keys:
        if url_key and title_key:
            _union(parent, url_key, title_key)

    grouped: dict[str, list[HeadlineRow]] = {}
    for (url_key, title_key), row in zip(keys, ordered, strict=True):
        root = _find(parent, url_key or title_key)
        grouped.setdefault(root, []).append(row)

    groups: list[HeadlineGroup] = []
    for members in grouped.values():
        representative = _representative(members, order)
        if representative is None:
            continue
        groups.append(
            HeadlineGroup(
                representative=representative,
                appearances=tuple(members),
            )
        )
    groups.sort(
        key=lambda group: _chronology_key(group.representative, order),
        reverse=True,
    )
    return tuple(groups)


def build_headline_feed(
    groups: Iterable[HeadlineGroup],
) -> tuple[HeadlineFeedRow, ...]:
    """Flatten exact groups into the display feed."""
    return tuple(_feed_row(group) for group in groups)


def _ordered_rows(rows: Iterable[HeadlineRow], order: FeedOrder) -> list[HeadlineRow]:
    """Sort rows deterministically, newest chronology first."""
    ordered = sorted(
        rows,
        key=lambda row: (
            row.source_name,
            row.position if row.position is not None else 0,
            row.item_id,
        ),
    )
    ordered.sort(key=lambda row: _chronology_key(row, order), reverse=True)
    return ordered


def _chronology_key(row: HeadlineRow, order: FeedOrder) -> str:
    if order == "observed":
        return row.first_seen_at
    return row.published_at or ""


def _representative(
    members: list[HeadlineRow],
    order: FeedOrder,
) -> HeadlineRow | None:
    if order == "observed":
        return max(members, key=lambda row: row.first_seen_at)
    dated = [row for row in members if row.published_at is not None]
    if not dated:
        return None
    return max(dated, key=lambda row: row.published_at or "")


def _feed_row(group: HeadlineGroup) -> HeadlineFeedRow:
    return _headline_feed_row(
        group.representative,
        duplicate_count=len(group.appearances) - 1,
    )


def _headline_feed_row(
    representative: HeadlineRow, *, duplicate_count: int
) -> HeadlineFeedRow:
    return HeadlineFeedRow(
        title=representative.title,
        url=representative.url,
        published_at=representative.published_at,
        first_seen_at=representative.first_seen_at,
        source_name=representative.source_name,
        duplicate_count=duplicate_count,
    )


def _identity_keys(row: HeadlineRow, index: int) -> tuple[str, str]:
    """Return the URL and title keys that decide exact group membership."""
    url_key = f"url:{row.canonical_url}" if row.canonical_url else ""
    title_key = (
        f"title:{row.item_kind}:{normalize_title_for_version(row.title).casefold()}"
        if row.title.strip()
        else ""
    )
    if not url_key and not title_key:
        return f"row:{index}", ""
    return url_key, title_key


def _find(parent: dict[str, str], key: str) -> str:
    parent.setdefault(key, key)
    root = key
    while parent[root] != root:
        root = parent[root]
    while parent[key] != root:
        parent[key], key = root, parent[key]
    return root


def _union(parent: dict[str, str], left: str, right: str) -> None:
    left_root = _find(parent, left)
    right_root = _find(parent, right)
    if left_root != right_root:
        parent[right_root] = left_root
