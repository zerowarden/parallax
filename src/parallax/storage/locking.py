"""POSIX collector process ownership, imported only by collection workflows."""

from __future__ import annotations

import fcntl
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path


class CollectorAlreadyRunningError(RuntimeError):
    """Another collection workflow already owns this archive."""


@contextmanager
def collector_lock(database_path: Path) -> Generator[None, None, None]:
    """One collector per archive; the OS releases ownership after a crash."""
    path = database_path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_name(path.name + ".collector.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CollectorAlreadyRunningError(
                f"Another collector owns {path}; stop it before starting "
                "another collection command."
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
