from __future__ import annotations

import logging
import time
from datetime import UTC, datetime

from parallax.config import SchedulerConfig
from parallax.ingest import IngestionService
from parallax.registry import SourceRegistry
from parallax.storage import Storage

LOGGER = logging.getLogger(__name__)


class Scheduler:
    def __init__(
        self,
        registry: SourceRegistry,
        storage: Storage,
        ingestion: IngestionService,
        config: SchedulerConfig,
    ) -> None:
        self._registry = registry
        self._storage = storage
        self._ingestion = ingestion
        self._config = config

    def run_due_once(self) -> int:
        now = datetime.now(UTC)
        due = []
        for source in self._registry.enabled():
            state = self._storage.get_stream_state(source.id)
            if state.next_run_at is not None and state.next_run_at > now:
                LOGGER.debug(
                    "operation=scheduler_skip source_id=%s next_run_at=%s",
                    source.id,
                    state.next_run_at.isoformat(),
                )
                continue
            due.append(source)
            LOGGER.info(
                "operation=scheduler_dispatch source_id=%s",
                source.id,
            )
        if due:
            result = self._ingestion.fetch_sources(due)
            succeeded = len(result.summaries)
            failed = len(result.failures)
        else:
            succeeded = 0
            failed = 0
        LOGGER.info(
            "operation=scheduler_cycle attempted=%s succeeded=%s failed=%s",
            len(due),
            succeeded,
            failed,
        )
        return len(due)

    def run_forever(self) -> None:
        LOGGER.info(
            "operation=scheduler_start loop_sleep_seconds=%s",
            self._config.loop_sleep_seconds,
        )
        while True:
            self.run_due_once()
            time.sleep(self._config.loop_sleep_seconds)
