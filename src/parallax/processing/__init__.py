"""Interfaces for independent downstream processing consumers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parallax.storage import Storage


class CheckpointedReader[T](ABC):
    """A consumer that reads committed entries after its own named checkpoint.

    Collection commits events first; a consumer can process them later without
    participating in ingestion. The checkpoint advances only when the consumer
    calls :meth:`checkpoint`, so a failed processor does not lose entries.
    """

    def __init__(self, storage: Storage, consumer_name: str) -> None:
        self._storage = storage
        self._consumer_name = consumer_name

    def pending(self, limit: int = 100) -> Iterator[T]:
        checkpoint = self._storage.get_consumer_checkpoint(self._consumer_name)
        yield from self._read_after(checkpoint, limit)

    def checkpoint(self, seq: int) -> None:
        self._storage.advance_consumer_checkpoint(self._consumer_name, seq)

    @abstractmethod
    def _read_after(self, checkpoint: int, limit: int) -> Iterable[T]:
        """Read committed entries after ``checkpoint`` in sequence order."""
        raise NotImplementedError
