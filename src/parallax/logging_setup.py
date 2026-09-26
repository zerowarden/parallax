from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path

from rich.logging import RichHandler

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


@contextmanager
def configure_logging(level: str, log_path: Path) -> Generator[None]:
    """Own application handlers for one CLI invocation, preserving host handlers."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with ExitStack() as resources:
        resources.callback(root.setLevel, root.level)
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        resources.callback(file_handler.close)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))

        console_handler = RichHandler(
            level=numeric_level,
            show_time=True,
            show_level=True,
            show_path=False,
            rich_tracebacks=False,
            log_time_format="[%Y-%m-%d %H:%M:%S]",
        )
        resources.callback(console_handler.close)
        console_handler.setFormatter(logging.Formatter("%(name)s %(message)s"))
        root.setLevel(numeric_level)
        for handler in (file_handler, console_handler):
            root.addHandler(handler)
            resources.callback(root.removeHandler, handler)
        yield
