from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path
from select import select

import pytest
from typer.testing import CliRunner

from parallax.cli import app
from parallax.runtime import ArchiveRuntime, DiagnosticRuntime, Runtime
from parallax.storage import Storage
from source_factory import make_source


def _config(tmp_path: Path) -> Path:
    config = tmp_path / "config.toml"
    config.write_text(
        "schema_version = 0\n[app]\n"
        'database_path = "archive.db"\nlog_path = "archive.log"\n'
    )
    (tmp_path / "sources").mkdir()
    return config


def _archive(tmp_path: Path) -> int:
    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    storage.sync_sources([make_source()])
    run = storage.start_fetch_run("fixture", "live")
    storage.close()
    return run


@pytest.mark.parametrize("command", ["show", "status", "runs"])
def test_archive_commands_ignore_broken_catalog_and_do_not_reconcile(
    tmp_path: Path, command: str
) -> None:
    config = _config(tmp_path)
    run = _archive(tmp_path)
    (tmp_path / "sources" / "invalid.toml").write_text("not valid TOML")
    result = CliRunner().invoke(app, [command, "--config", str(config)])
    assert result.exit_code == 0, result.output
    with ArchiveRuntime.build(config) as reader:
        assert [source.id for source in reader.catalog.enabled_sources()] == ["fixture"]
        assert reader.storage.recent_fetch_runs()[0].id == run
        assert reader.storage.recent_fetch_runs()[0].status == "running"
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            reader.storage.sync_sources([])


def test_catalog_listing_does_not_create_archive(tmp_path: Path) -> None:
    config = _config(tmp_path)
    result = CliRunner().invoke(app, ["sources", "--config", str(config)])
    assert result.exit_code == 0, result.output
    assert not (tmp_path / "archive.db").exists()


def test_reading_missing_archive_does_not_create_it(tmp_path: Path) -> None:
    config = _config(tmp_path)
    with pytest.raises(sqlite3.OperationalError):
        ArchiveRuntime.build(config)
    assert not (tmp_path / "archive.db").exists()


def test_collector_owns_recovery_and_empty_catalog_reconciliation(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    run = _archive(tmp_path)
    with Runtime.build(config) as collector:
        recovered = collector.storage.recent_fetch_runs()[0]
        assert recovered.id == run
        assert recovered.status == "failed"
        assert recovered.error_type == "AbandonedRun"
        assert collector.storage.get_stream_state("fixture").consecutive_failures == 1
        assert collector.storage.sources() == ()
        with pytest.raises(RuntimeError, match="Another collector"):
            Runtime.build(config)
        with ArchiveRuntime.build(config) as reader:
            assert reader.storage.recent_fetch_runs()[0] == recovered
    with Runtime.build(config) as next_collector:
        assert (
            next_collector.storage.get_stream_state("fixture").consecutive_failures == 1
        )


@pytest.mark.parametrize("runtime_type", [Runtime, DiagnosticRuntime])
def test_transport_startup_failure_closes_storage_and_releases_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runtime_type: type[Runtime] | type[DiagnosticRuntime],
) -> None:
    config = _config(tmp_path)
    _archive(tmp_path)
    opened: list[Storage] = []

    class TrackedStorage(Storage):
        def __init__(self, database_path: Path, *, read_only: bool = False) -> None:
            super().__init__(database_path, read_only=read_only)
            opened.append(self)

    def fail_transport(*args: object, **kwargs: object) -> None:
        raise RuntimeError("transport startup failure")

    with monkeypatch.context() as patch:
        patch.setattr("parallax.storage.Storage", TrackedStorage)
        patch.setattr("parallax.transport.HttpTransport", fail_transport)
        with pytest.raises(RuntimeError, match="transport startup failure"):
            runtime_type.build(config)
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0].recent_fetch_runs()
    with Runtime.build(config):
        pass


def test_diagnostics_do_not_reconcile_or_recover_runs(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _archive(tmp_path)
    with DiagnosticRuntime.build(config):
        pass
    with ArchiveRuntime.build(config) as reader:
        assert reader.catalog.source("fixture").enabled
        assert reader.storage.recent_fetch_runs()[0].status == "running"


@pytest.mark.parametrize(
    "command", [["run", "--once"], ["fetch", "fixture"], ["fetch-all"]]
)
def test_collector_process_rejects_manual_and_scheduled_collection(
    tmp_path: Path, command: list[str]
) -> None:
    config = _config(tmp_path)
    _archive(tmp_path)
    with Runtime.build(config) as owner:
        run = owner.storage.start_fetch_run("fixture", "live")
        result = subprocess.run(
            [sys.executable, "-m", "parallax", *command, "--config", str(config)],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert result.returncode != 0
        assert "Another collector" in result.stderr
        assert "stop it before starting another collection command" in result.stderr
        assert "Traceback" not in result.stderr
        # A rejected collector must not recover the owner's active run.
        with ArchiveRuntime.build(config) as reader:
            assert reader.storage.recent_fetch_runs()[0].id == run
            assert reader.storage.recent_fetch_runs()[0].status == "running"
        result = subprocess.run(
            [sys.executable, "-m", "parallax", "runs", "--config", str(config)],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert result.returncode == 0, result.stderr


def test_collector_process_crash_releases_ownership(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _archive(tmp_path)
    script = """
import logging
import sys
from pathlib import Path
from parallax.runtime import Runtime
logging.disable(logging.CRITICAL)
with Runtime.build(Path(sys.argv[1])) as runtime:
    runtime.storage.start_fetch_run("fixture", "live")
    print("ready", flush=True)
    sys.stdin.readline()
"""
    with subprocess.Popen(
        [sys.executable, "-c", script, str(config)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            assert process.stdout is not None
            assert select([process.stdout], [], [], 15)[0], "collector did not start"
            assert process.stdout.readline() == "ready\n"
            with pytest.raises(RuntimeError, match="Another collector"):
                Runtime.build(config)
        finally:
            process.kill()
            process.communicate(timeout=15)
    with Runtime.build(config) as successor:
        recovered = successor.storage.recent_fetch_runs()[0]
        assert recovered.status == "failed"
        assert recovered.error_type == "AbandonedRun"


@pytest.mark.parametrize("command", ["show", "status", "runs", "web"])
@pytest.mark.parametrize(
    "invalid",
    [
        "[http]\nmax_connections = -1\n",
        "[scheduler]\nloop_sleep_seconds = -1\n",
        "[ingestion]\nmax_concurrent_sources = -1\n",
        "[validation]\nmax_title_length = -1\n",
    ],
)
def test_archive_commands_ignore_collector_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    invalid: str,
) -> None:
    from parallax.config import CatalogError, load_catalog

    config = _config(tmp_path)
    _archive(tmp_path)
    config.write_text(config.read_text() + invalid)
    served: list[int] = []

    def serve(application: object, **kwargs: object) -> None:
        from flask import Flask

        assert isinstance(application, Flask)
        served.append(application.test_client().get("/").status_code)

    monkeypatch.setattr("flask.Flask.run", serve)
    result = CliRunner().invoke(app, [command, "--config", str(config)])
    assert result.exit_code == 0, result.output
    if command == "web":
        assert served == [200]
    with pytest.raises(CatalogError):
        load_catalog(config)


def test_runtime_does_not_configure_global_logging(tmp_path: Path) -> None:
    import logging

    config = _config(tmp_path)
    root = logging.getLogger()
    handlers = root.handlers[:]
    level = root.level
    for _ in range(2):
        with Runtime.build(config):
            assert root.handlers == handlers
            assert root.level == level
    assert not (tmp_path / "archive.log").exists()
