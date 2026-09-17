from __future__ import annotations

from dataclasses import dataclass

from parallax.config import SourceConfig


@dataclass(frozen=True, slots=True)
class SourceRegistry:
    _sources: dict[str, SourceConfig]

    @classmethod
    def from_sources(cls, sources: list[SourceConfig]) -> SourceRegistry:
        return cls({source.id: source for source in sources})

    def get(self, source_id: str) -> SourceConfig:
        try:
            return self._sources[source_id]
        except KeyError as exc:
            raise KeyError(f"Unknown source: {source_id}") from exc

    def all(self) -> tuple[SourceConfig, ...]:
        return tuple(self._sources.values())

    def enabled(self) -> tuple[SourceConfig, ...]:
        return tuple(source for source in self._sources.values() if source.enabled)
