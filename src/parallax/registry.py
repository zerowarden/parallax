from __future__ import annotations

from dataclasses import dataclass

from parallax.config import Source


@dataclass(frozen=True, slots=True)
class SourceRegistry:
    _sources: dict[str, Source]

    @classmethod
    def from_sources(cls, sources: tuple[Source, ...]) -> SourceRegistry:
        return cls({source.id: source for source in sources})

    def get(self, source_id: str) -> Source:
        try:
            return self._sources[source_id]
        except KeyError as exc:
            raise KeyError(f"Unknown source: {source_id}") from exc

    def all(self) -> tuple[Source, ...]:
        return tuple(self._sources.values())

    def enabled(self) -> tuple[Source, ...]:
        return tuple(source for source in self._sources.values() if source.enabled)
