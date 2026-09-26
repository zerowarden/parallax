from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from parallax.domain import AnalysisItem

if TYPE_CHECKING:
    from parallax.storage import Storage


class AnalysisInputReader:
    """Checkpointed, typed projection of committed change events.

    Wraps the shared change-log checkpoint so a processor receives exact
    committed item versions, including disabled sources, in sequence order.
    Consumers advance the named checkpoint only after successful processing.
    """

    def __init__(self, storage: Storage, consumer_name: str) -> None:
        self._storage = storage
        self._consumer_name = consumer_name

    def pending(self, limit: int = 100) -> Iterator[AnalysisItem]:
        checkpoint = self._storage.get_consumer_checkpoint(self._consumer_name)
        yield from self._storage.analysis_items_after(checkpoint, limit)

    def checkpoint(self, seq: int) -> None:
        self._storage.advance_consumer_checkpoint(self._consumer_name, seq)
