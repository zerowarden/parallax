from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from parallax.adapters import AdapterRegistry
from parallax.config import Settings, load_settings
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
    settings: Settings
    registry: SourceRegistry
    storage: Storage
    transport: HttpTransport
    ingestion: IngestionService
    diagnostics: DiagnosticService
    scheduler: Scheduler

    @classmethod
    def build(cls, config_path: Path) -> Runtime:
        settings = load_settings(config_path)
        adapters = AdapterRegistry()
        for source in settings.sources:
            adapters.validate_source(source)
        configure_logging(settings.app.log_level, settings.app.log_path)
        registry = SourceRegistry.from_sources(settings.sources)
        storage = Storage(settings.app.database_path)
        storage.initialize()
        storage.sync_sources(settings.sources)
        transport = HttpTransport(settings.http)
        validator = BatchValidator(settings.validation)
        ingestion = IngestionService(
            storage=storage,
            transport=transport,
            adapters=adapters,
            validator=validator,
            ingestion_config=settings.ingestion,
        )
        scheduler = Scheduler(
            registry=registry,
            storage=storage,
            ingestion=ingestion,
            config=settings.scheduler,
        )
        diagnostics = DiagnosticService(
            storage=storage,
            transport=transport,
            adapters=adapters,
            validator=validator,
        )
        return cls(
            settings=settings,
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
