from __future__ import annotations

from collections.abc import Iterable

from parallax.feed import (
    FeedOrder,
    HeadlineGroup,
    build_headline_feed,
    build_headline_groups,
)
from parallax.storage import HeadlineRow


def _row(
    source_name: str,
    title: str,
    url: str,
    *,
    canonical_url: str | None = None,
    published_at: str | None = "2026-09-17T10:00:00+00:00",
    first_seen_at: str = "2026-09-17T10:05:00+00:00",
    position: int | None = 1,
    item_id: int = 1,
    item_kind: str = "article",
) -> HeadlineRow:
    return HeadlineRow(
        source_id=source_name.casefold(),
        source_name=source_name,
        item_id=item_id,
        stream_kind="latest",
        item_kind=item_kind,
        position=position,
        title=title,
        url=url,
        canonical_url=canonical_url if canonical_url is not None else url,
        published_at=published_at,
        first_seen_at=first_seen_at,
    )


def _groups(
    rows: Iterable[HeadlineRow], order: FeedOrder = "observed"
) -> tuple[HeadlineGroup, ...]:
    return build_headline_groups(rows, order=order)


def test_groups_collapse_same_canonical_url_across_sources() -> None:
    rows = [
        _row(
            "Alpha",
            "Story one",
            "https://example.com/story#comments",
            canonical_url="https://example.com/story",
            first_seen_at="2026-09-17T11:05:00+00:00",
        ),
        _row(
            "Beta",
            "Story one (updated)",
            "https://EXAMPLE.com/story",
            canonical_url="https://example.com/story",
            first_seen_at="2026-09-17T09:05:00+00:00",
        ),
    ]

    groups = _groups(rows)
    feed = build_headline_feed(groups)

    assert len(groups) == 1
    assert len(groups[0].appearances) == 2
    assert len(feed) == 1
    assert feed[0].source_name == "Alpha"
    assert feed[0].title == "Story one"
    assert feed[0].duplicate_count == 1


def test_groups_deduplicate_on_stored_canonical_url() -> None:
    rows = [
        _row(
            "Alpha",
            "First wording",
            "https://example.com/story?utm_source=alpha",
            canonical_url="https://example.com/story",
        ),
        _row(
            "Beta",
            "Second wording",
            "https://example.com/story?utm_source=beta",
            canonical_url="https://example.com/story",
        ),
    ]

    groups = _groups(rows)

    assert len(groups) == 1
    assert len(groups[0].appearances) == 2


def test_groups_collapse_normalized_titles_across_sources() -> None:
    rows = [
        _row(
            "Alpha",
            "Breaking  News",
            "https://alpha.example/story",
            first_seen_at="2026-09-17T11:05:00+00:00",
        ),
        _row(
            "Beta",
            "ｂｒｅａｋｉｎｇ news",
            "https://beta.example/story",
            first_seen_at="2026-09-17T10:05:00+00:00",
        ),
    ]

    groups = _groups(rows)

    assert len(groups) == 1
    assert groups[0].representative.source_name == "Alpha"


def test_title_only_matches_respect_item_kind() -> None:
    rows = [
        _row(
            "Alpha",
            "Shared title",
            "https://alpha.example/article",
            item_kind="article",
        ),
        _row(
            "Beta",
            "Shared title",
            "https://beta.example/ranking",
            item_kind="ranking",
        ),
    ]

    groups = _groups(rows)

    assert len(groups) == 2
    assert {group.representative.item_kind for group in groups} == {
        "article",
        "ranking",
    }


def test_groups_keep_distinct_stories_separate() -> None:
    rows = [
        _row("Alpha", "Story one", "https://example.com/one"),
        _row("Beta", "Story two", "https://example.com/two"),
    ]

    feed = build_headline_feed(_groups(rows))

    assert len(feed) == 2
    assert {row.title for row in feed} == {"Story one", "Story two"}
    assert all(row.duplicate_count == 0 for row in feed)


def test_groups_collapse_transitive_title_and_url_chain() -> None:
    rows = [
        _row(
            "Alpha",
            "Shared title",
            "https://alpha.example/a",
            first_seen_at="2026-09-17T11:05:00+00:00",
        ),
        _row(
            "Beta",
            "Shared title",
            "https://beta.example/b",
            first_seen_at="2026-09-17T10:05:00+00:00",
        ),
        _row(
            "Gamma",
            "Edited title",
            "https://beta.example/b",
            first_seen_at="2026-09-17T09:05:00+00:00",
        ),
    ]

    groups = _groups(rows)

    assert len(groups) == 1
    assert len(groups[0].appearances) == 3
    assert groups[0].representative.source_name == "Alpha"


def test_groups_keep_disjoint_merge_groups_separate() -> None:
    rows = [
        _row("Alpha", "Shared", "https://alpha.example/a"),
        _row("Beta", "Shared", "https://beta.example/b"),
        _row("Gamma", "Other", "https://gamma.example/c"),
        _row("Delta", "Other", "https://delta.example/d"),
    ]

    feed = build_headline_feed(_groups(rows))

    assert len(feed) == 2
    assert {row.title for row in feed} == {"Shared", "Other"}
    assert all(row.duplicate_count == 1 for row in feed)


def test_observed_order_uses_first_seen_not_publication() -> None:
    rows = [
        _row(
            "Alpha",
            "Older publication, seen recently",
            "https://example.com/recent",
            published_at="2026-09-16T10:00:00+00:00",
            first_seen_at="2026-09-17T12:00:00+00:00",
        ),
        _row(
            "Beta",
            "Newest publication, seen earlier",
            "https://example.com/newest",
            published_at="2026-09-17T11:00:00+00:00",
            first_seen_at="2026-09-17T09:00:00+00:00",
        ),
    ]

    feed = build_headline_feed(_groups(rows, order="observed"))

    assert [row.title for row in feed] == [
        "Older publication, seen recently",
        "Newest publication, seen earlier",
    ]


def test_published_order_uses_publication_only() -> None:
    rows = [
        _row(
            "Alpha",
            "Older publication, seen recently",
            "https://example.com/recent",
            published_at="2026-09-16T10:00:00+00:00",
            first_seen_at="2026-09-17T12:00:00+00:00",
        ),
        _row(
            "Beta",
            "Newest publication, seen earlier",
            "https://example.com/newest",
            published_at="2026-09-17T11:00:00+00:00",
            first_seen_at="2026-09-17T09:00:00+00:00",
        ),
    ]

    feed = build_headline_feed(_groups(rows, order="published"))

    assert [row.title for row in feed] == [
        "Newest publication, seen earlier",
        "Older publication, seen recently",
    ]


def test_published_order_excludes_groups_without_publication_time() -> None:
    rows = [
        _row(
            "Alpha",
            "Undated trend",
            "https://example.com/undated",
            published_at=None,
            first_seen_at="2026-09-17T12:00:00+00:00",
        ),
        _row("Beta", "Dated article", "https://example.com/dated"),
    ]

    assert len(_groups(rows, order="observed")) == 2
    published = _groups(rows, order="published")
    assert len(published) == 1
    assert published[0].representative.title == "Dated article"


def test_published_representative_uses_dated_appearance() -> None:
    rows = [
        _row(
            "Alpha",
            "Undated appearance",
            "https://example.com/story",
            canonical_url="https://example.com/story",
            published_at=None,
            first_seen_at="2026-09-17T12:00:00+00:00",
        ),
        _row(
            "Beta",
            "Dated appearance",
            "https://example.com/story?utm_source=beta",
            canonical_url="https://example.com/story",
            published_at="2026-09-17T09:00:00+00:00",
            first_seen_at="2026-09-17T09:05:00+00:00",
        ),
    ]

    observed = _groups(rows, order="observed")
    published = _groups(rows, order="published")

    assert observed[0].representative.source_name == "Alpha"
    assert published[0].representative.source_name == "Beta"
    assert len(published[0].appearances) == 2


def test_group_ordering_and_representatives_are_deterministic_for_ties() -> None:
    rows = [
        _row(
            "Beta",
            "Second story",
            "https://example.com/second",
            first_seen_at="2026-09-17T10:05:00+00:00",
        ),
        _row(
            "Alpha",
            "First story",
            "https://example.com/first",
            first_seen_at="2026-09-17T10:05:00+00:00",
        ),
    ]

    groups = _groups(rows)
    repeated = _groups(rows)

    assert groups == repeated
    assert [group.representative.source_name for group in groups] == ["Alpha", "Beta"]


def test_groups_increment_duplicate_count_per_extra_observation() -> None:
    rows = [
        _row("Alpha", "Same story", "https://example.com/story"),
        _row("Beta", "Same story", "https://example.com/story"),
        _row("Gamma", "Same story", "https://example.com/story"),
    ]

    feed = build_headline_feed(_groups(rows))

    assert len(feed) == 1
    assert feed[0].duplicate_count == 2


def test_groups_return_empty_tuple_for_no_rows() -> None:
    assert _groups([]) == ()
    assert build_headline_feed(()) == ()
