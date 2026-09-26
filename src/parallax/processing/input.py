from __future__ import annotations

from collections.abc import Iterable

from parallax.domain import AnalysisItem
from parallax.processing import CheckpointedReader


class AnalysisInputReader(CheckpointedReader[AnalysisItem]):
    """Checkpointed, typed projection of committed change events.

    Wraps the shared change-log checkpoint so a processor receives exact
    committed item versions, including disabled sources, in sequence order.
    Consumers advance the named checkpoint only after successful processing.
    """

    def _read_after(self, checkpoint: int, limit: int) -> Iterable[AnalysisItem]:
        return self._storage.analysis_items_after(checkpoint, limit)
