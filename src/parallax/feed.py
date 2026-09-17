from __future__ import annotations

import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass

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

    Two observations describe the same story when their stored canonical URLs
    match or their normalized headlines match; equality is transitive, so the
    union of both relations forms the story groups. The first (most recent)
    observation represents the story and later members only increment
    ``duplicate_count``. This is a read model: nothing is persisted and
    per-source identity and provenance remain untouched in storage.
    """
    ordered = sorted(
        rows,
        key=lambda row: (
            row.source_name,
            row.position if row.position is not None else 0,
        ),
    )
    ordered.sort(key=_recency_key, reverse=True)

    keys = [_identity_keys(row, index) for index, row in enumerate(ordered)]
    parent: dict[str, str] = {}
    for url_key, title_key in keys:
        if url_key and title_key:
            _union(parent, url_key, title_key)

    groups: dict[str, list[HeadlineRow]] = {}
    for (url_key, title_key), row in zip(keys, ordered, strict=True):
        root = _find(parent, url_key or title_key)
        groups.setdefault(root, []).append(row)

    return tuple(
        HeadlineFeedRow(
            title=members[0].title,
            url=members[0].url,
            published_at=members[0].published_at,
            first_seen_at=members[0].first_seen_at,
            source_name=members[0].source_name,
            duplicate_count=len(members) - 1,
        )
        for members in groups.values()
    )


def _identity_keys(row: HeadlineRow, index: int) -> tuple[str, str]:
    """Return the URL and title keys that decide story membership."""
    url_key = f"url:{row.canonical_url}" if row.canonical_url else ""
    title_key = f"title:{_normalize_title(row.title)}" if row.title.strip() else ""
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


def _recency_key(row: HeadlineRow) -> str:
    return row.published_at or row.first_seen_at


def _normalize_title(title: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", title).split()).casefold()
