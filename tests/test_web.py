from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from flask.testing import FlaskClient

from parallax.config import Source
from parallax.domain import (
    BrowseSurface,
    BrowseView,
    HeadlineCandidate,
    ItemKind,
    StreamKind,
    ValidatedBatch,
)
from parallax.feed import PAGE_SIZE, browse_items
from parallax.storage import HeadlineRow, Storage
from parallax.web import (
    create_app,
    format_relative,
    is_usable_external_url,
    window_start,
)
from source_factory import make_source

NOW = datetime(2026, 9, 18, 4, 0, tzinfo=UTC)
SURFACE_CASES: tuple[tuple[str, tuple[BrowseSurface, ...]], ...] = (
    ("news-source", ("news",)),
    ("discover-source", ("discover",)),
    ("both-source", ("news", "discover")),
)


def _source(
    source_id: str,
    name: str,
    *,
    stream_kind: StreamKind = "latest",
    item_kind: ItemKind = "article",
    surfaces: tuple[BrowseSurface, ...] = ("news",),
) -> Source:
    return make_source(
        id=source_id,
        channel_label=name,
        url="https://example.test/feed.xml",
        stream_kind=stream_kind,
        item_kind=item_kind,
        surfaces=surfaces,
    )


def _candidate(
    title: str,
    url: str,
    *,
    published_at: datetime | None = None,
) -> HeadlineCandidate:
    return HeadlineCandidate(title=title, url=url, published_at=published_at)


def _record(
    storage: Storage,
    source: Source,
    candidates: Sequence[HeadlineCandidate],
    observed_at: datetime,
) -> None:
    run_id = storage.start_fetch_run(source.id)
    storage.record_success(
        source,
        run_id,
        200,
        ValidatedBatch(candidates=tuple(candidates), rejected_count=0),
        None,
        None,
        observed_at + timedelta(hours=1),
        observed_at,
    )


@pytest.fixture
def storage(tmp_path: Path) -> Iterator[Storage]:
    store = Storage(tmp_path / "parallax.db")
    store.initialize()
    yield store
    store.close()


def _client(
    storage: Storage,
    sources: Sequence[Source] = (),
    *,
    now: datetime = NOW,
) -> FlaskClient:
    app = create_app(storage, sources, now=lambda: now)
    app.config.update(TESTING=True)
    return app.test_client()


def _html(client: FlaskClient, url: str = "/") -> str:
    return client.get(url).get_data(as_text=True)


def _titles(html: str) -> list[str]:
    blocks = html.split('<div class="headline">')[1:]
    return [
        re.sub(r"<[^>]+>", "", block.split("</div>", 1)[0]).strip() for block in blocks
    ]


class _RecordingBrowseReader:
    def __init__(self, total: int) -> None:
        self.total = total
        self.count_calls: list[dict[str, object]] = []
        self.browse_calls: list[dict[str, object]] = []

    def count_browse_headlines(
        self,
        *,
        view: BrowseView,
        since: datetime,
        until: datetime,
        query: str | None = None,
        source_id: str | None = None,
    ) -> int:
        self.count_calls.append(
            {
                "view": view,
                "since": since,
                "until": until,
                "query": query,
                "source_id": source_id,
            }
        )
        return self.total

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
    ) -> list[HeadlineRow]:
        self.browse_calls.append(
            {
                "view": view,
                "since": since,
                "until": until,
                "query": query,
                "source_id": source_id,
                "limit": limit,
                "offset": offset,
            }
        )
        return []


def test_today_window_starts_at_hong_kong_midnight() -> None:
    assert window_start(1, NOW) == datetime(2026, 9, 17, 16, 0, tzinfo=UTC)
    assert window_start(3, NOW) == datetime(2026, 9, 15, 16, 0, tzinfo=UTC)
    assert window_start(7, NOW) == datetime(2026, 9, 11, 16, 0, tzinfo=UTC)
    assert window_start(30, NOW) == datetime(2026, 8, 19, 16, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-09-18T03:59:30+00:00", "just now"),
        ("2026-09-18T03:00:30+00:00", "59m ago"),
        ("2026-09-18T01:00:00+00:00", "3h ago"),
        ("2026-09-17T03:00:00+00:00", "yesterday"),
        ("2026-09-15T04:00:00+00:00", "Sep 15"),
    ],
)
def test_format_relative(value: str, expected: str) -> None:
    assert format_relative(value, NOW) == expected


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://example.test/story", True),
        ("http://example.test/story", True),
        ("", False),
        ("javascript:alert(1)", False),
        ("https://", False),
        ("https://example.test:invalid/story", False),
    ],
)
def test_external_url_requires_a_usable_http_destination(
    url: str, expected: bool
) -> None:
    assert is_usable_external_url(url) is expected


def test_browse_items_requests_only_the_selected_page() -> None:
    reader = _RecordingBrowseReader(total=250)

    result = browse_items(
        reader,
        view="all",
        since=NOW - timedelta(days=30),
        until=NOW,
        query="OpenAI",
        source_id="alpha",
        page=2,
    )

    assert result.page == 2
    assert result.total_pages == 3
    assert result.items == ()
    assert reader.count_calls == [
        {
            "view": "all",
            "since": NOW - timedelta(days=30),
            "until": NOW,
            "query": "OpenAI",
            "source_id": "alpha",
        }
    ]
    assert reader.browse_calls == [
        {
            "view": "all",
            "since": NOW - timedelta(days=30),
            "until": NOW,
            "query": "OpenAI",
            "source_id": "alpha",
            "limit": PAGE_SIZE,
            "offset": PAGE_SIZE,
        }
    ]


def test_browse_items_rejects_non_positive_page_size() -> None:
    reader = _RecordingBrowseReader(total=0)

    with pytest.raises(ValueError, match="page_size must be positive"):
        browse_items(reader, view="all", since=NOW, until=NOW, page_size=0)

    assert reader.count_calls == []
    assert reader.browse_calls == []


def test_storage_browse_rejects_invalid_limit_and_offset(storage: Storage) -> None:
    kwargs = {"view": "all", "since": NOW, "until": NOW}

    with pytest.raises(ValueError, match="limit must be positive"):
        storage.browse_headlines(**kwargs, limit=0, offset=0)
    with pytest.raises(ValueError, match="offset must not be negative"):
        storage.browse_headlines(**kwargs, limit=1, offset=-1)


def test_storage_browse_rejects_invalid_time_ranges(storage: Storage) -> None:
    naive = datetime(2026, 9, 18, 4, 0)

    with pytest.raises(ValueError, match="timezone-aware"):
        storage.count_browse_headlines(view="all", since=naive, until=NOW)
    with pytest.raises(ValueError, match="start must not be after"):
        storage.count_browse_headlines(
            view="all", since=NOW, until=NOW - timedelta(seconds=1)
        )


def test_storage_browse_rejects_unknown_view(storage: Storage) -> None:
    with pytest.raises(ValueError, match="unknown browse view"):
        storage.count_browse_headlines(
            view=cast(BrowseView, "bogus"), since=NOW, until=NOW
        )


def test_browse_views_follow_explicit_surfaces(storage: Storage) -> None:
    sources = [
        _source(f"source-{index}", f"Source {index}", surfaces=surfaces)
        for index, (_, surfaces) in enumerate(SURFACE_CASES)
    ]
    storage.sync_sources(sources)
    for index, source in enumerate(sources):
        _record(
            storage,
            source,
            [_candidate(f"Title {index}", f"https://example.test/{index}")],
            NOW - timedelta(minutes=5),
        )

    client = _client(storage, sources)
    all_titles = set(_titles(_html(client, "/?view=all")))
    news_titles = set(_titles(_html(client, "/?view=news")))
    discover_titles = set(_titles(_html(client, "/?view=discover")))

    assert all_titles == {"Title 0", "Title 1", "Title 2"}
    assert news_titles == {"Title 0", "Title 2"}
    assert discover_titles == {"Title 1", "Title 2"}


def test_day_filter_uses_first_seen_and_keeps_undated_items(storage: Storage) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [_candidate("Seen before midnight", "https://example.test/old")],
        NOW - timedelta(hours=20),
    )
    _record(
        storage,
        source,
        [_candidate("Seen after midnight", "https://example.test/new")],
        NOW - timedelta(hours=2),
    )

    client = _client(storage, [source])
    today_html = _html(client, "/?days=1")
    three_day_html = _html(client, "/?days=3")

    assert "Seen after midnight" in today_html
    assert "Seen before midnight" not in today_html
    assert "Seen before midnight" in three_day_html


@pytest.mark.parametrize("days", [3, 7, 30])
def test_multi_day_windows_include_the_cutoff_but_not_earlier_items(
    storage: Storage, days: int
) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    cutoff = window_start(days, NOW)
    _record(
        storage,
        source,
        [_candidate("Exactly at cutoff", "https://example.test/cutoff")],
        cutoff,
    )
    _record(
        storage,
        source,
        [_candidate("Before cutoff", "https://example.test/before")],
        cutoff - timedelta(seconds=1),
    )

    html = _html(_client(storage, [source]), f"/?days={days}")

    assert "Exactly at cutoff" in html
    assert "Before cutoff" not in html


def test_date_window_excludes_items_observed_after_request_time(
    storage: Storage,
) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [_candidate("Future observation", "https://example.test/future")],
        NOW + timedelta(seconds=1),
    )

    html = _html(_client(storage, [source]), "/?days=1")

    assert "Future observation" not in html


def test_pagination_splits_without_overlap(storage: Storage) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [
            _candidate(f"Headline {index:03d}", f"https://example.test/{index}")
            for index in range(150)
        ],
        NOW - timedelta(hours=1),
    )

    client = _client(storage, [source])
    page_one = _titles(_html(client, "/?days=1&page=1"))
    page_two = _titles(_html(client, "/?days=1&page=2"))

    assert len(page_one) == PAGE_SIZE
    assert len(page_two) == 50
    assert set(page_one).isdisjoint(page_two)
    assert len(set(page_one) | set(page_two)) == 150


def test_pagination_preserves_filters(storage: Storage) -> None:
    source = _source("alpha", "Alpha")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [
            _candidate(f"Headline {index:03d}", f"https://example.test/{index}")
            for index in range(101)
        ],
        NOW - timedelta(hours=1),
    )

    client = _client(storage, [source])
    html = _html(client, "/?view=news&days=7&q=Headline&source=alpha")

    assert "Page 1 / 2" in html
    assert "days=7" in html
    assert "q=Headline" in html
    assert "source=alpha" in html


def test_all_view_navigation_preserves_filters_and_resets_page(
    storage: Storage,
) -> None:
    source = _source("alpha", "Alpha")
    storage.sync_sources([source])

    html = _html(
        _client(storage, [source]),
        "/?view=news&days=7&q=OpenAI&source=alpha&page=2",
    )

    assert 'href="/?view=all&amp;days=7&amp;q=OpenAI&amp;source=alpha">All</a>' in html


def test_all_view_is_rendered_as_the_active_navigation_option(
    storage: Storage,
) -> None:
    html = _html(_client(storage), "/?view=all")

    assert 'class="active" href="/?view=all&amp;days=1">All</a>' in html


def test_search_matches_titles_and_composes_with_filters(storage: Storage) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [
            _candidate("OpenAI ships a model", "https://example.test/openai"),
            _candidate("Other story", "https://example.test/other"),
        ],
        NOW - timedelta(minutes=1),
    )

    client = _client(storage, [source])
    html = _html(client, "/?view=news&days=1&q=openai")

    assert "OpenAI ships a model" in html
    assert "Other story" not in html


@pytest.mark.parametrize(
    ("query", "matching_title", "decoy_title"),
    [
        ("100%", "100% sure", "100 percent sure"),
        ("under_score", "under_score", "underXscore"),
        (r"path\name", r"path\name", "pathname"),
    ],
)
def test_search_treats_like_metacharacters_literally(
    storage: Storage,
    query: str,
    matching_title: str,
    decoy_title: str,
) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [
            _candidate(matching_title, "https://example.test/match"),
            _candidate(decoy_title, "https://example.test/decoy"),
        ],
        NOW - timedelta(minutes=1),
    )

    client = _client(storage, [source])
    html = client.get("/", query_string={"q": query}).get_data(as_text=True)

    assert matching_title in html
    assert decoy_title not in html


def test_source_filter_restricts_results(storage: Storage) -> None:
    alpha = _source("alpha", "Alpha")
    beta = _source("beta", "Beta")
    storage.sync_sources([alpha, beta])
    _record(
        storage,
        alpha,
        [_candidate("Alpha story", "https://alpha.test/story")],
        NOW - timedelta(minutes=2),
    )
    _record(
        storage,
        beta,
        [_candidate("Beta story", "https://beta.test/story")],
        NOW - timedelta(minutes=1),
    )

    client = _client(storage, [alpha, beta])
    html = _html(client, "/?source=alpha")

    assert "Alpha story" in html
    assert "Beta story" not in html


def test_disabled_sources_are_not_browsable(storage: Storage) -> None:
    alpha = _source("alpha", "Alpha")
    beta = _source("beta", "Beta")
    storage.sync_sources([alpha, beta])
    _record(
        storage,
        beta,
        [_candidate("Beta story", "https://beta.test/story")],
        NOW - timedelta(minutes=1),
    )
    storage.sync_sources([alpha])

    client = _client(storage, [alpha])
    html = _html(client, "/")

    assert "Beta story" not in html


def test_latest_stored_title_version_is_displayed(storage: Storage) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [_candidate("Original title", "https://example.test/story")],
        NOW - timedelta(hours=2),
    )
    _record(
        storage,
        source,
        [_candidate("Updated title", "https://example.test/story")],
        NOW - timedelta(hours=1),
    )

    client = _client(storage, [source])
    html = _html(client, "/?days=1")

    assert "Updated title" in html
    assert "Original title" not in html


def test_published_metadata_is_present_only_when_known(storage: Storage) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [
            _candidate(
                "Dated headline",
                "https://example.test/dated",
                published_at=NOW - timedelta(hours=2),
            ),
            _candidate("Undated headline", "https://example.test/undated"),
        ],
        NOW - timedelta(minutes=1),
    )

    html = _html(_client(storage, [source]), "/")

    assert "Dated headline" in html
    assert "Undated headline" in html
    assert html.count('class="published"') == 1
    assert "Published unknown" not in html


def test_headline_link_uses_original_destination_safely(storage: Storage) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [_candidate("Linked headline", "https://example.test/original")],
        NOW - timedelta(minutes=1),
    )

    html = _html(_client(storage, [source]), "/")

    assert (
        '<a href="https://example.test/original" target="_blank" '
        'rel="noopener noreferrer">Linked headline</a>' in html
    )


def test_long_mixed_language_headlines_render_as_source_text(storage: Storage) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    chinese = "香港與國際市場焦點" * 12
    english = " ".join(["A detailed international market headline"] * 12)
    _record(
        storage,
        source,
        [
            _candidate(chinese, "https://example.test/chinese"),
            _candidate(english, "https://example.test/english"),
        ],
        NOW - timedelta(minutes=1),
    )

    html = _html(_client(storage, [source]), "/")

    assert chinese in html
    assert english in html


def test_browser_deduplicates_items_at_item_level(storage: Storage) -> None:
    alpha = _source("alpha", "Alpha")
    beta = _source("beta", "Beta")
    storage.sync_sources([alpha, beta])
    _record(
        storage,
        alpha,
        [_candidate("Shared story", "https://example.test/story")],
        NOW - timedelta(minutes=2),
    )
    _record(
        storage,
        beta,
        [_candidate("Shared story", "https://example.test/story")],
        NOW - timedelta(minutes=1),
    )

    client = _client(storage, [alpha, beta])
    html = _html(client, "/?days=1")

    assert html.count('class="headline"') == 1
    assert "Beta" in html
    assert "(+1)" not in html


def test_script_title_is_escaped(storage: Storage) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [_candidate('<script>alert("x")</script>', "https://example.test/x")],
        NOW - timedelta(minutes=1),
    )

    client = _client(storage, [source])
    html = _html(client, "/")

    assert "<script>alert" not in html
    assert "&lt;script&gt;" in html


@pytest.mark.parametrize("url", ["", "javascript:alert(1)"])
def test_item_without_usable_url_renders_plain_headline(
    storage: Storage, url: str
) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [_candidate("No link headline", url)],
        NOW - timedelta(minutes=1),
    )

    client = _client(storage, [source])
    html = _html(client, "/")

    assert "No link headline" in html
    assert f'href="{url}"' not in html


def test_invalid_query_parameters_fall_back_to_defaults(storage: Storage) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [_candidate("Article headline", "https://example.test/article")],
        NOW - timedelta(minutes=1),
    )

    client = _client(storage, [source])
    html = _html(client, "/?view=bogus&days=99&page=abc&q=%20%20")

    assert "Article headline" in html
    assert "Page 1 / 1" in html


def test_page_beyond_range_clamps_to_last_page(storage: Storage) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])
    _record(
        storage,
        source,
        [_candidate("Article headline", "https://example.test/article")],
        NOW - timedelta(minutes=1),
    )

    client = _client(storage, [source])
    html = _html(client, "/?page=9")

    assert "Article headline" in html
    assert "Page 1 / 1" in html


def test_empty_result_offers_default_link(storage: Storage) -> None:
    source = _source("news", "News Feed")
    storage.sync_sources([source])

    client = _client(storage, [source])
    html = _html(client, "/?q=missing")

    assert "No items match the current filters." in html
    assert "News &middot; Today &middot; All sources" in html


def test_source_dropdown_lists_configured_sources(storage: Storage) -> None:
    alpha = _source("alpha", "Alpha")
    beta = _source("beta", "Beta")
    storage.sync_sources([alpha, beta])

    client = _client(storage, [alpha, beta])
    html = _html(client, "/")

    assert '<option value="alpha"' in html
    assert '<option value="beta"' in html
    assert "All sources" in html


def test_source_dropdown_labels_options_by_provider(storage: Storage) -> None:
    alpha = _source("alpha", "Alpha")
    beta = _source("beta", "Beta", surfaces=("discover",))
    storage.sync_sources([alpha, beta])

    client = _client(storage, [alpha, beta])
    html = _html(client, "/")

    assert ">Fixture Provider — Alpha</option>" in html
    assert ">Fixture Provider — Beta</option>" in html
