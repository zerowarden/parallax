from __future__ import annotations

from collections.abc import Iterator

from parallax.domain import ChangeEvent
from parallax.storage import Storage


class ChangeLogReader:
    """Independent consumer boundary for future categorisation jobs.

    Consumers own a named checkpoint. Collection commits events first; a
    consumer can process them later without participating in ingestion.
    """

    def __init__(self, storage: Storage, consumer_name: str) -> None:
        self._storage = storage
        self._consumer_name = consumer_name

    def pending(self, limit: int = 100) -> Iterator[ChangeEvent]:
        checkpoint = self._storage.get_consumer_checkpoint(self._consumer_name)
        yield from self._storage.changes_after(checkpoint, limit=limit)

    def checkpoint(self, seq: int) -> None:
        self._storage.set_consumer_checkpoint(self._consumer_name, seq)
