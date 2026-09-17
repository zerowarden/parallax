from __future__ import annotations

from parallax.feed import build_headline_feed
from parallax.storage import HeadlineRow


def _row(
    source_name: str,
    title: str,
    url: str,
    *,
    published_at: str | None = "2026-09-17T10:00:00+00:00",
    first_seen_at: str = "2026-09-17T10:05:00+00:00",
    position: int | None = 1,
) -> HeadlineRow:
    return HeadlineRow(
        source_id=source_name.casefold(),
        source_name=source_name,
        position=position,
        title=title,
        url=url,
        published_at=published_at,
        first_seen_at=first_seen_at,
    )


def test_feed_collapses_same_canonical_url_across_sources() -> None:
    rows = [
        _row(
            "Alpha",
            "Story one",
            "https://example.com/story#comments",
            published_at="2026-09-17T11:00:00+00:00",
        ),
        _row(
            "Beta",
            "Story one (updated)",
            "https://EXAMPLE.com/story",
            published_at="2026-09-17T09:00:00+00:00",
        ),
    ]

    feed = build_headline_feed(rows)

    assert len(feed) == 1
    assert feed[0].source_name == "Alpha"
    assert feed[0].title == "Story one"
    assert feed[0].duplicate_count == 1


def test_feed_collapses_normalized_titles_across_sources() -> None:
    rows = [
        _row(
            "Alpha",
            "Breaking  News",
            "https://alpha.example/story",
            published_at="2026-09-17T11:00:00+00:00",
        ),
        _row(
            "Beta",
            "ｂｒｅａｋｉｎｇ news",
            "https://beta.example/story",
            published_at="2026-09-17T10:00:00+00:00",
        ),
    ]

    feed = build_headline_feed(rows)

    assert len(feed) == 1
    assert feed[0].source_name == "Alpha"
    assert feed[0].duplicate_count == 1


def test_feed_keeps_distinct_stories_separate() -> None:
    rows = [
        _row("Alpha", "Story one", "https://example.com/one"),
        _row("Beta", "Story two", "https://example.com/two"),
    ]

    feed = build_headline_feed(rows)

    assert len(feed) == 2
    assert {row.title for row in feed} == {"Story one", "Story two"}
    assert all(row.duplicate_count == 0 for row in feed)


def test_feed_orders_by_publication_then_first_seen() -> None:
    rows = [
        _row(
            "Alpha",
            "Older",
            "https://example.com/older",
            published_at="2026-09-16T10:00:00+00:00",
        ),
        _row(
            "Beta",
            "Undated recent",
            "https://example.com/undated",
            published_at=None,
            first_seen_at="2026-09-17T09:30:00+00:00",
        ),
        _row(
            "Gamma",
            "Newest",
            "https://example.com/newest",
            published_at="2026-09-17T11:00:00+00:00",
        ),
    ]

    feed = build_headline_feed(rows)

    assert [row.title for row in feed] == ["Newest", "Undated recent", "Older"]


def test_feed_increments_duplicate_count_per_extra_observation() -> None:
    rows = [
        _row("Alpha", "Same story", "https://example.com/story"),
        _row("Beta", "Same story", "https://example.com/story"),
        _row("Gamma", "Same story", "https://example.com/story"),
    ]

    feed = build_headline_feed(rows)

    assert len(feed) == 1
    assert feed[0].duplicate_count == 2


def test_feed_returns_empty_tuple_for_no_rows() -> None:
    assert build_headline_feed([]) == ()
