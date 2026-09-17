from __future__ import annotations

from rich.console import Console

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
        published_at=None,
        first_seen_at="2026-09-17T00:00:00+00:00",
    )

    Presenter(console).headlines([row])
    output = console.export_text()

    assert "[bold]literal[/bold]" in output
    assert "Malformed [/bold] and [red]literal[/red]" in output
    assert "https://example.test/[story]" in output
