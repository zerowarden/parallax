from __future__ import annotations

from collections.abc import Iterator

from parallax.domain import AnalysisItem
from parallax.processing.changelog import ChangeLogReader
from parallax.storage import Storage


class AnalysisInputReader:
    """Checkpointed, typed projection of committed change events.

    Wraps the shared change-log checkpoint so a processor receives exact
    committed item versions, including disabled sources, in sequence order.
    Consumers advance the named checkpoint only after successful processing.
    """

    def __init__(self, storage: Storage, consumer_name: str) -> None:
        self._storage = storage
        self._change_log = ChangeLogReader(storage, consumer_name)

    def pending(self, limit: int = 100) -> Iterator[AnalysisItem]:
        events = list(self._change_log.pending(limit))
        yield from self._storage.hydrate_analysis_items(events)

    def checkpoint(self, seq: int) -> None:
        self._change_log.checkpoint(seq)
