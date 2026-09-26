from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "module,command",
    [
        ("parallax.adapters.base", []),
        ("parallax.adapters.results", []),
        ("parallax.archive", []),
        ("parallax.diagnostic_models", []),
        ("parallax.feed", []),
        ("parallax.presentation", []),
        ("parallax.cli", []),
        ("parallax.cli", ["sources"]),
        ("parallax.cli", ["config", "resolve"]),
        ("parallax.cli", ["config", "source", "thepaper-hot"]),
    ],
)
def test_reading_contracts_does_not_load_collection_infrastructure(
    module: str,
    command: list[str],
    project_root: Path,
) -> None:
    script = """
import importlib
import sys
importlib.import_module(sys.argv[1])
if sys.argv[2:]:
    from typer.testing import CliRunner
    from parallax.cli import app
    result = CliRunner().invoke(app, sys.argv[2:])
    assert result.exit_code == 0, result.output
forbidden = (
    "parallax.adapters.cn.", "parallax.adapters.hk.",
    "httpx", "sqlite3", "parallax.storage", "selectolax",
)
# Typer itself imports fcntl for terminal support; pure contracts must not.
if sys.argv[1] != "parallax.cli":
    forbidden += ("fcntl",)
loaded = sorted(
    name for name in sys.modules
    if any(name == prefix or name.startswith(prefix) for prefix in forbidden)
)
assert not loaded, loaded
"""
    arguments = (
        [*command, "--config", str(project_root / "config/config.toml")]
        if command
        else []
    )
    result = subprocess.run(
        [sys.executable, "-c", script, module, *arguments],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_archive_storage_import_works_without_posix_locking() -> None:
    script = """
import sys
sys.modules['fcntl'] = None
from parallax.storage import Storage
from parallax.runtime import ArchiveRuntime
assert 'parallax.storage.locking' not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
