from __future__ import annotations

from rich.console import Console

from parallax.feed import HeadlineFeedRow
from parallax.presentation import Presenter
from parallax.storage import HeadlineRow


def test_headline_content_is_rendered_as_literal_text() -> None:
    console = Console(record=True, width=160)
    row = HeadlineRow(
        source_id="fixture",
        source_name="Source [bold]literal[/bold]",
        position=1,
        title="Malformed [/bold] and [red]literal[/red]",
        url="https://example.test/[story]",
        canonical_url="https://example.test/[story]",
        published_at=None,
        first_seen_at="2026-09-17T00:00:00+00:00",
    )

    Presenter(console).headlines([row])
    output = console.export_text()

    assert "[bold]literal[/bold]" in output
    assert "Malformed [/bold] and [red]literal[/red]" in output
    assert "https://example.test/[story]" in output


def test_feed_renders_deduplicated_source_and_literal_content() -> None:
    console = Console(record=True, width=160)
    rows = (
        HeadlineFeedRow(
            title="Title [bold]literal[/bold]",
            url="https://example.test/[story]",
            published_at="2026-09-17T10:00:00+00:00",
            first_seen_at="2026-09-17T10:05:00+00:00",
            source_name="Alpha [red]literal[/red]",
            duplicate_count=2,
        ),
    )

    Presenter(console).feed(rows)
    output = console.export_text()

    assert "Source" in output
    assert "Alpha [red]literal[/red] (+2)" in output
    assert "Title [bold]literal[/bold]" in output
    assert "https://example.test/[story]" in output


def test_feed_renders_unknown_publication_time() -> None:
    console = Console(record=True, width=160)
    rows = (
        HeadlineFeedRow(
            title="Undated",
            url="https://example.test/undated",
            published_at=None,
            first_seen_at="2026-09-17T10:05:00+00:00",
            source_name="Alpha",
            duplicate_count=0,
        ),
    )

    Presenter(console).feed(rows)

    assert "unknown" in console.export_text()


def test_feed_reports_empty_state() -> None:
    console = Console(record=True)

    Presenter(console).feed(())

    assert "No stored headlines yet." in console.export_text()
