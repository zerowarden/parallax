from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Self

from parallax.adapters import AdapterRegistry
from parallax.config import ResolvedConfig, SourceCatalog, load_catalog
from parallax.config.loader import load_archive_config
from parallax.diagnostics import DiagnosticService
from parallax.ingest import IngestionService
from parallax.logging_setup import configure_logging
from parallax.scheduler import Scheduler
from parallax.storage import Storage, collector_lock
from parallax.transport import HttpTransport
from parallax.validation import BatchValidator


@dataclass(slots=True)
class _OwnedResources:
    _resources: ExitStack

    def close(self) -> None:
        self._resources.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


@dataclass(slots=True)
class ArchiveRuntime(_OwnedResources):
    catalog: SourceCatalog
    storage: Storage

    @classmethod
    def build(cls, config_path: Path) -> ArchiveRuntime:
        config = load_archive_config(config_path)
        with ExitStack() as resources:
            storage = Storage(config.database_path, read_only=True)
            resources.callback(storage.close)
            catalog = SourceCatalog(sources=storage.sources())
            return cls(resources.pop_all(), catalog, storage)


@dataclass(slots=True)
class DiagnosticRuntime(_OwnedResources):
    catalog: SourceCatalog
    diagnostics: DiagnosticService

    @classmethod
    def build(cls, config_path: Path) -> DiagnosticRuntime:
        config = load_catalog(config_path)
        with ExitStack() as resources:
            storage = Storage(config.app.database_path, read_only=True)
            resources.callback(storage.close)
            transport = resources.enter_context(HttpTransport(config.http))
            diagnostics = DiagnosticService(
                storage, transport, AdapterRegistry(), BatchValidator(config.validation)
            )
            return cls(resources.pop_all(), config, diagnostics)


@dataclass(slots=True)
class Runtime(_OwnedResources):
    catalog: ResolvedConfig
    storage: Storage
    transport: HttpTransport
    ingestion: IngestionService
    diagnostics: DiagnosticService
    scheduler: Scheduler

    @classmethod
    def build(cls, config_path: Path) -> Runtime:
        config = load_catalog(config_path)
        adapters = AdapterRegistry()
        for source in config.sources:
            adapters.validate_source(source)
        configure_logging(config.app.log_level, config.app.log_path)
        with ExitStack() as resources:
            resources.enter_context(collector_lock(config.app.database_path))
            storage = Storage(config.app.database_path)
            resources.callback(storage.close)
            storage.initialize()
            transport = resources.enter_context(HttpTransport(config.http))
            validator = BatchValidator(config.validation)
            ingestion = IngestionService(
                storage=storage,
                transport=transport,
                adapters=adapters,
                validator=validator,
                ingestion_config=config.ingestion,
            )
            scheduler = Scheduler(
                catalog=config,
                storage=storage,
                ingestion=ingestion,
                config=config.scheduler,
            )
            diagnostics = DiagnosticService(storage, transport, adapters, validator)
            storage.recover_abandoned_runs()
            storage.sync_sources(config.sources)
            return cls(
                resources.pop_all(),
                config,
                storage,
                transport,
                ingestion,
                diagnostics,
                scheduler,
            )
