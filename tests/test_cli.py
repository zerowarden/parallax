from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from parallax.cli import app
from parallax.config import Source
from parallax.domain import IngestionBatchResult, IngestionFailure
from parallax.storage import HeadlineRow
from source_factory import make_source


def _source() -> Source:
    return make_source(adapter="fixture", url="https://example.test/fixture")


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
        entity_kind=None,
        item_variant=None,
        position=position,
        title=title,
        url=url,
        canonical_url=url,
        published_at=published_at,
        first_seen_at=first_seen_at,
    )


class _Registry:
    def enabled(self) -> list[Source]:
        return [_source()]

    def get(self, source_id: str) -> Source:
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
        self, sources: list[Source], since: object = None
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


class _WebApp:
    def __init__(self) -> None:
        self.run_calls: list[dict[str, object]] = []

    def run(self, **kwargs: object) -> None:
        self.run_calls.append(kwargs)


class _WebRuntime:
    def __init__(self, storage: object) -> None:
        self.registry = _Registry()
        self.storage = storage

    def __enter__(self) -> _WebRuntime:
        return self

    def __exit__(self, *args: object) -> None:
        return None


def test_web_serves_local_reader_with_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    application = _WebApp()
    runtime = _WebRuntime(storage=object())
    captured: dict[str, object] = {}

    def fake_create_app(storage: object, sources: object) -> _WebApp:
        captured["storage"] = storage
        captured["sources"] = sources
        return application

    monkeypatch.setattr("parallax.cli._runtime", lambda config: runtime)
    monkeypatch.setattr("parallax.cli.create_app", fake_create_app)

    result = CliRunner().invoke(app, ["web"])

    assert result.exit_code == 0
    assert "Parallax web reader: http://127.0.0.1:8765" in result.output
    assert application.run_calls == [
        {"host": "127.0.0.1", "port": 8765, "threaded": False}
    ]
    assert captured["storage"] is runtime.storage


def test_web_forwards_host_and_port(monkeypatch: pytest.MonkeyPatch) -> None:
    application = _WebApp()
    runtime = _WebRuntime(storage=object())

    monkeypatch.setattr("parallax.cli._runtime", lambda config: runtime)
    monkeypatch.setattr(
        "parallax.cli.create_app",
        lambda storage, sources: application,
    )

    result = CliRunner().invoke(app, ["web", "--host", "127.0.0.1", "--port", "9000"])

    assert result.exit_code == 0
    assert "Parallax web reader: http://127.0.0.1:9000" in result.output
    assert application.run_calls[0]["port"] == 9000


def test_web_quiets_werkzeug_access_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    application = _WebApp()
    runtime = _WebRuntime(storage=object())
    werkzeug_logger = logging.getLogger("werkzeug")
    monkeypatch.setattr(werkzeug_logger, "level", logging.INFO)
    monkeypatch.setattr("parallax.cli._runtime", lambda config: runtime)
    monkeypatch.setattr(
        "parallax.cli.create_app",
        lambda storage, sources: application,
    )

    result = CliRunner().invoke(app, ["web"])

    assert result.exit_code == 0
    assert werkzeug_logger.level == logging.WARNING


def _write_catalog(base: Path, *, adapter: str = "rss", options: str = "") -> Path:
    (base / "sources").mkdir(parents=True, exist_ok=True)
    (base / "config.toml").write_text("schema_version = 0\n")
    (base / "sources" / "cn.toml").write_text(f"""
[[sources]]
id = "fixture"
provider_id = "fixture"
provider_name = "Fixture Provider"
provider_kind = "publisher"
channel_id = "main"
channel_label = "Main"
channel_role = "aggregate"
stream_kind = "latest"
item_kind = "article"
topics = ["business"]
surfaces = ["news"]
language = "en-GB"
market = "GB"
interval_seconds = 1800
max_items = 100
[sources.endpoint]
adapter = "{adapter}"
url = "https://example.test/feed.xml"
{options}
""")
    return base / "config.toml"


def test_config_lint_accepts_a_valid_catalog(tmp_path: Path) -> None:
    config = _write_catalog(tmp_path)

    result = CliRunner().invoke(app, ["config", "lint", "--config", str(config)])

    assert result.exit_code == 0
    assert "Catalog OK" in result.output


def test_config_lint_rejects_unknown_adapters(tmp_path: Path) -> None:
    config = _write_catalog(tmp_path, adapter="missing")

    result = CliRunner().invoke(app, ["config", "lint", "--config", str(config)])

    assert result.exit_code == 1
    assert "Unknown adapter" in result.output


def test_config_lint_rejects_invalid_adapter_options(tmp_path: Path) -> None:
    config = _write_catalog(
        tmp_path,
        options="[sources.endpoint.options]\nmax_item = 10",
    )

    result = CliRunner().invoke(app, ["config", "lint", "--config", str(config)])

    assert result.exit_code == 1
    assert "Unknown options" in result.output


def test_config_lint_rejects_malformed_catalogs(tmp_path: Path) -> None:
    config = _write_catalog(tmp_path)
    config.write_text(
        config.read_text().replace("schema_version = 0", "schema_version = 1")
    )

    result = CliRunner().invoke(app, ["config", "lint", "--config", str(config)])

    assert result.exit_code == 1
    assert "schema_version" in result.output


def test_config_resolve_outputs_complete_json(tmp_path: Path) -> None:
    config = _write_catalog(tmp_path)

    result = CliRunner().invoke(app, ["config", "resolve", "--config", str(config)])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["sources"][0]["id"] == "fixture"
    assert payload["sources"][0]["provider_name"] == "Fixture Provider"
    assert payload["sources"][0]["interval_seconds"] == 1800
    assert payload["sources"][0]["max_items"] == 100
    assert payload["app"]["database_path"] == str(
        (tmp_path / "data/parallax.db").resolve()
    )
    assert "defined_in" not in payload["sources"][0]


def test_config_source_shows_resolved_definition(tmp_path: Path) -> None:
    config = _write_catalog(tmp_path)

    result = CliRunner().invoke(
        app, ["config", "source", "fixture", "--config", str(config)]
    )

    assert result.exit_code == 0
    assert "Fixture Provider" in result.output
    assert "en-GB" in result.output
    assert "GB" in result.output
    assert "cn.toml" in result.output
    assert "1800" in result.output
    assert "100" in result.output


def test_config_source_rejects_unknown_ids(tmp_path: Path) -> None:
    config = _write_catalog(tmp_path)

    result = CliRunner().invoke(
        app, ["config", "source", "missing", "--config", str(config)]
    )

    assert result.exit_code == 1
    assert "Unknown source" in result.output
