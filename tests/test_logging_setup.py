from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

from parallax.logging_setup import configure_logging


@pytest.mark.parametrize("fail", [False, True])
def test_logging_bootstrap_closes_its_handlers_and_restores_host(
    tmp_path: Path,
    fail: bool,
) -> None:
    root = logging.getLogger()
    original = root.handlers[:]
    level = root.level
    for _ in range(3):
        owned: list[logging.Handler] = []
        try:
            with configure_logging("DEBUG", tmp_path / "app.log"):
                owned = [
                    handler for handler in root.handlers if handler not in original
                ]
                assert len(owned) == 2
                root.info("bootstrap test")
                if fail:
                    raise RuntimeError("bootstrap failed")
        except RuntimeError:
            assert fail
        assert root.handlers == original
        assert root.level == level
        assert all(handler not in root.handlers for handler in owned)
        files = [
            handler for handler in owned if isinstance(handler, RotatingFileHandler)
        ]
        assert len(files) == 1
        assert files[0].stream is None
