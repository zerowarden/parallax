from __future__ import annotations

import json

from parallax.config.models import (
    AppConfig,
    ConfigModel,
    HttpConfig,
    IngestionConfig,
    SchedulerConfig,
    Source,
    ValidationConfig,
)


class ResolvedConfig(ConfigModel):
    """Frozen, deterministic catalog after sources resolve."""

    app: AppConfig
    http: HttpConfig
    ingestion: IngestionConfig
    scheduler: SchedulerConfig
    validation: ValidationConfig

    sources: tuple[Source, ...]

    def source(self, source_id: str) -> Source:
        for source in self.sources:
            if source.id == source_id:
                return source
        raise KeyError(f"Unknown source: {source_id}")

    def enabled_sources(self) -> tuple[Source, ...]:
        return tuple(source for source in self.sources if source.enabled)

    def resolved_json(self) -> str:
        """Return the complete resolved configuration, including app paths."""
        return json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            ensure_ascii=False,
            indent=2,
        )
