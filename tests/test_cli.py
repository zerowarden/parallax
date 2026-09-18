from __future__ import annotations

from datetime import datetime

import pytest
from typer.testing import CliRunner

from parallax.cli import app
from parallax.config import SourceConfig
from parallax.domain import IngestionBatchResult, IngestionFailure
from parallax.storage import HeadlineRow


def _source() -> SourceConfig:
    return SourceConfig(
        id="fixture",
        name="Fixture",
        region="US",
        language="en-US",
        adapter="fixture",
        url="https://example.test/fixture",
    )


def _headline_row(
    source_name: str,
    title: str,
    url: str,
    *,
    position: int | None = 1,
    published_at: str | None = "2026-09-17T10:00:00+00:00",
    first_seen_at: str = "2026-09-17T10:05:00+00:00",
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
        canonical_url=url,
        published_at=published_at,
        first_seen_at=first_seen_at,
    )


class _Registry:
    def enabled(self) -> list[SourceConfig]:
        return [_source()]

    def get(self, source_id: str) -> SourceConfig:
        return _source()


class _Storage:
    def __init__(self, rows: list[HeadlineRow]) -> None:
        self.rows = rows
        self.calls: list[tuple[int | None, str | None]] = []

    def latest_snapshot_headlines(
        self,
        limit_per_source: int | None = None,
        source_id: str | None = None,
    ) -> list[HeadlineRow]:
        self.calls.append((limit_per_source, source_id))
        return list(self.rows)


class _ShowRuntime:
    def __init__(self, rows: list[HeadlineRow]) -> None:
        self.registry = _Registry()
        self.storage = _Storage(rows)

    def __enter__(self) -> _ShowRuntime:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Ingestion:
    def __init__(self, result: IngestionBatchResult) -> None:
        self.result = result
        self.since = None

    def fetch_sources(
        self, sources: list[SourceConfig], since: object = None
    ) -> IngestionBatchResult:
        self.since = since
        return self.result


class _Runtime:
    def __init__(self, result: IngestionBatchResult) -> None:
        self.registry = _Registry()
        self.ingestion = _Ingestion(result)

    def __enter__(self) -> _Runtime:
        return self

    def __exit__(self, *args: object) -> None:
        return None


def test_fetch_all_exits_nonzero_when_the_batch_has_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _Runtime(
        IngestionBatchResult(
            (), (IngestionFailure("fixture", "RuntimeError", "failed"),)
        )
    )
    monkeypatch.setattr("parallax.cli._runtime", lambda config: runtime)
    monkeypatch.setenv("COLUMNS", "200")

    result = CliRunner().invoke(app, ["fetch-all"])

    assert result.exit_code == 1
    assert "failed" in result.output
    assert "RuntimeError: failed" in result.output


def test_fetch_all_forwards_the_since_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _Runtime(IngestionBatchResult((), ()))
    monkeypatch.setattr("parallax.cli._runtime", lambda config: runtime)

    result = CliRunner().invoke(app, ["fetch-all", "--since", "1d"])

    assert result.exit_code == 0
    assert isinstance(runtime.ingestion.since, datetime)


def test_show_defaults_to_observed_order(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        _headline_row(
            "Alpha",
            "Seen recently",
            "https://example.test/recent",
            published_at="2026-09-16T10:00:00+00:00",
            first_seen_at="2026-09-17T12:00:00+00:00",
        ),
        _headline_row(
            "Beta",
            "Published earlier",
            "https://example.test/published",
            published_at="2026-09-17T11:00:00+00:00",
            first_seen_at="2026-09-17T09:00:00+00:00",
            item_id=2,
        ),
    ]
    runtime = _ShowRuntime(rows)
    monkeypatch.setattr("parallax.cli._runtime", lambda config: runtime)
    monkeypatch.setenv("COLUMNS", "200")

    result = CliRunner().invoke(app, ["show"])

    assert result.exit_code == 0
    assert runtime.storage.calls == [(None, None)]
    assert "Latest observed headlines" in result.output
    assert result.output.index("Seen recently") < result.output.index(
        "Published earlier"
    )


def test_show_published_order_excludes_undated_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        _headline_row(
            "Alpha",
            "Dated article",
            "https://example.test/dated",
            published_at="2026-09-17T10:00:00+00:00",
        ),
        _headline_row(
            "Beta",
            "Undated trend",
            "https://example.test/undated",
            published_at=None,
            first_seen_at="2026-09-17T12:00:00+00:00",
            item_id=2,
        ),
    ]
    runtime = _ShowRuntime(rows)
    monkeypatch.setattr("parallax.cli._runtime", lambda config: runtime)
    monkeypatch.setenv("COLUMNS", "200")

    result = CliRunner().invoke(app, ["show", "--order", "published"])

    assert result.exit_code == 0
    assert "Latest published headlines" in result.output
    assert "Dated article" in result.output
    assert "Undated trend" not in result.output


def test_show_with_source_uses_position_rendering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        _headline_row(
            "Fixture", "First position", "https://example.test/1", position=1
        ),
        _headline_row(
            "Fixture",
            "Second position",
            "https://example.test/2",
            position=2,
            item_id=2,
        ),
    ]
    runtime = _ShowRuntime(rows)
    monkeypatch.setattr("parallax.cli._runtime", lambda config: runtime)
    monkeypatch.setenv("COLUMNS", "200")

    result = CliRunner().invoke(app, ["show", "--source", "fixture"])

    assert result.exit_code == 0
    assert runtime.storage.calls == [(None, "fixture")]
    assert "First position" in result.output
    assert "Second position" in result.output
    assert "Latest observed headlines" not in result.output


def test_show_rejects_non_default_order_with_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _ShowRuntime([])
    monkeypatch.setattr("parallax.cli._runtime", lambda config: runtime)
    monkeypatch.setenv("COLUMNS", "200")

    result = CliRunner().invoke(
        app,
        ["show", "--source", "fixture", "--order", "published"],
    )

    assert result.exit_code == 2
    assert "only valid for the global feed" in result.output
    assert runtime.storage.calls == []


def test_show_rejects_unknown_order(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = _ShowRuntime([])
    monkeypatch.setattr("parallax.cli._runtime", lambda config: runtime)
    monkeypatch.setenv("COLUMNS", "200")

    result = CliRunner().invoke(app, ["show", "--order", "recent"])

    assert result.exit_code == 2
    assert runtime.storage.calls == []
