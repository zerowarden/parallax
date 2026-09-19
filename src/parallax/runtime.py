from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from parallax.adapters import AdapterRegistry
from parallax.config import ResolvedConfig, load_catalog
from parallax.diagnostics import DiagnosticService
from parallax.ingest import IngestionService
from parallax.logging_setup import configure_logging
from parallax.registry import SourceRegistry
from parallax.scheduler import Scheduler
from parallax.storage import Storage
from parallax.transport import HttpTransport
from parallax.validation import BatchValidator


@dataclass(slots=True)
class Runtime:
    config: ResolvedConfig
    registry: SourceRegistry
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
        registry = SourceRegistry.from_sources(config.sources)
        storage = Storage(config.app.database_path)
        storage.initialize()
        storage.sync_sources(config.sources)
        transport = HttpTransport(config.http)
        validator = BatchValidator(config.validation)
        ingestion = IngestionService(
            storage=storage,
            transport=transport,
            adapters=adapters,
            validator=validator,
            ingestion_config=config.ingestion,
        )
        scheduler = Scheduler(
            registry=registry,
            storage=storage,
            ingestion=ingestion,
            config=config.scheduler,
        )
        diagnostics = DiagnosticService(
            storage=storage,
            transport=transport,
            adapters=adapters,
            validator=validator,
        )
        return cls(
            config=config,
            registry=registry,
            storage=storage,
            transport=transport,
            ingestion=ingestion,
            diagnostics=diagnostics,
            scheduler=scheduler,
        )

    def close(self) -> None:
        self.transport.close()
        self.storage.close()

    def __enter__(self) -> Runtime:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
