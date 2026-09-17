from __future__ import annotations

import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass, replace

from parallax.identity import canonicalize_url
from parallax.storage import HeadlineRow


@dataclass(frozen=True, slots=True)
class HeadlineFeedRow:
    title: str
    url: str
    published_at: str | None
    first_seen_at: str
    source_name: str
    duplicate_count: int


def build_headline_feed(rows: Iterable[HeadlineRow]) -> tuple[HeadlineFeedRow, ...]:
    """Collapse per-source headlines into one recency-ordered display view.

    Two observations describe the same story when their canonical URLs match
    or their normalized headlines match. The first (most recent) observation
    represents the story and later matches only increment ``duplicate_count``.
    This is a read model: nothing is persisted and per-source identity and
    provenance remain untouched in storage.
    """
    ordered = sorted(
        rows,
        key=lambda row: (
            row.source_name,
            row.position if row.position is not None else 0,
        ),
    )
    ordered.sort(key=_recency_key, reverse=True)

    merged: list[HeadlineFeedRow] = []
    by_url: dict[str, int] = {}
    by_title: dict[str, int] = {}
    for row in ordered:
        url_key = canonicalize_url(row.url)
        title_key = _normalize_title(row.title)
        index = by_url.get(url_key) if url_key else None
        if index is None and title_key:
            index = by_title.get(title_key)

        if index is not None:
            merged[index] = replace(
                merged[index],
                duplicate_count=merged[index].duplicate_count + 1,
            )
            continue

        merged.append(
            HeadlineFeedRow(
                title=row.title,
                url=row.url,
                published_at=row.published_at,
                first_seen_at=row.first_seen_at,
                source_name=row.source_name,
                duplicate_count=0,
            )
        )
        index = len(merged) - 1
        if url_key:
            by_url[url_key] = index
        if title_key:
            by_title[title_key] = index

    return tuple(merged)


def _recency_key(row: HeadlineRow) -> str:
    return row.published_at or row.first_seen_at


def _normalize_title(title: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", title).split()).casefold()
