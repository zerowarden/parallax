from __future__ import annotations

from datetime import datetime

import pytest
from typer.testing import CliRunner

from parallax.cli import app
from parallax.config import SourceConfig
from parallax.domain import IngestionBatchResult, IngestionFailure


def _source() -> SourceConfig:
    return SourceConfig(
        id="fixture",
        name="Fixture",
        region="US",
        language="en-US",
        adapter="fixture",
        url="https://example.test/fixture",
    )


class _Registry:
    def enabled(self) -> list[SourceConfig]:
        return [_source()]


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
