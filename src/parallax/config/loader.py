from __future__ import annotations

import tomllib
from pathlib import Path

from pydantic import ValidationError

from parallax.config.catalog import ResolvedConfig
from parallax.config.models import (
    AppConfig,
    CatalogRoot,
    Source,
    SourceDefinition,
    SourceFile,
)

SCHEMA_VERSION = 0

DEFAULT_SOURCES_DIRNAME = "sources"


class CatalogError(RuntimeError):
    """The root configuration or a source definition violates the schema rules."""


def load_catalog(
    root_path: Path,
) -> ResolvedConfig:
    """Load, resolve, and freeze the root configuration and its sources.

    Source definitions are read recursively from the ``sources`` directory
    next to the root file. File order and location carry no semantic meaning.
    """
    root_path = root_path.expanduser().resolve()
    raw = _read_toml(root_path)
    version = raw.get("schema_version")
    if version != SCHEMA_VERSION:
        raise CatalogError(
            f"Unsupported schema_version {version!r} in {root_path}; "
            f"expected {SCHEMA_VERSION}"
        )
    try:
        root = CatalogRoot.model_validate(raw)
    except ValidationError as exc:
        raise CatalogError(f"Invalid catalog root {root_path}:\n{exc}") from exc

    directory = root_path.parent / DEFAULT_SOURCES_DIRNAME
    resolved_sources = _resolve_sources(_load_sources(directory))
    _validate_provider_metadata(resolved_sources)

    return ResolvedConfig(
        app=_resolve_app_paths(root.app, root_path.parent),
        http=root.http,
        ingestion=root.ingestion,
        scheduler=root.scheduler,
        validation=root.validation,
        sources=resolved_sources,
    )


def _load_sources(directory: Path) -> tuple[tuple[str, SourceDefinition], ...]:
    if not directory.is_dir():
        raise CatalogError(f"sources directory does not exist: {directory}")
    loaded: list[tuple[str, SourceDefinition]] = []
    paths = sorted(
        directory.rglob("*.toml"),
        key=lambda item: item.relative_to(directory).as_posix(),
    )
    for path in paths:
        if not path.is_file():
            continue
        origin = path.relative_to(directory).as_posix()
        loaded.extend(
            (origin, definition) for definition in _parse_sources(path).sources
        )
    return tuple(loaded)


def _parse_sources(path: Path) -> SourceFile:
    raw = _read_toml(path)
    try:
        return SourceFile.model_validate(raw)
    except ValidationError as exc:
        raise CatalogError(f"Invalid source file {path}:\n{exc}") from exc


def _resolve_sources(
    definitions: tuple[tuple[str, SourceDefinition], ...],
) -> tuple[Source, ...]:
    resolved: dict[str, Source] = {}
    for origin, definition in definitions:
        if definition.id in resolved:
            raise CatalogError(f"duplicate source id: {definition.id}")
        resolved[definition.id] = Source.from_definition(
            definition,
            defined_in=origin,
        )
    return tuple(resolved[source_id] for source_id in sorted(resolved))


def _validate_provider_metadata(sources: tuple[Source, ...]) -> None:
    declared: dict[str, tuple[str, str]] = {}
    for source in sources:
        meta = (source.provider_name, source.provider_kind)
        existing = declared.setdefault(source.provider_id, meta)
        if existing != meta:
            raise CatalogError(
                f"provider {source.provider_id!r} is declared inconsistently: "
                f"{existing[0]!r}/{existing[1]!r} vs {meta[0]!r}/{meta[1]!r}"
            )


def _resolve_app_paths(app: AppConfig, base_dir: Path) -> AppConfig:
    return AppConfig(
        database_path=_resolve_path(base_dir, app.database_path),
        log_path=_resolve_path(base_dir, app.log_path),
        log_level=app.log_level,
    )


def _resolve_path(base_dir: Path, path: Path) -> Path:
    path = path.expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _read_toml(path: Path) -> dict[str, object]:
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except FileNotFoundError as exc:
        raise CatalogError(f"catalog file does not exist: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise CatalogError(f"invalid TOML in {path}: {exc}") from exc
