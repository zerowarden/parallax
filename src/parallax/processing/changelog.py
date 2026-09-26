from __future__ import annotations

from collections.abc import Iterable

from parallax.domain import ChangeEvent
from parallax.processing import CheckpointedReader


class ChangeLogReader(CheckpointedReader[ChangeEvent]):
    """Independent consumer boundary for raw change-log events.

    Consumers own a named checkpoint. Collection commits events first; a
    consumer can process them later without participating in ingestion.
    """

    def _read_after(self, checkpoint: int, limit: int) -> Iterable[ChangeEvent]:
        return self._storage.changes_after(checkpoint, limit=limit)
