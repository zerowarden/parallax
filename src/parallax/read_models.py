"""Immutable archive projections, independent of storage and collection services."""

from __future__ import annotations

from dataclasses import dataclass

from parallax.domain import (
    EntityKind,
    ItemKind,
    ItemVariant,
    StreamKind,
)


@dataclass(frozen=True, slots=True)
class HeadlineRow:
    source_id: str
    source_name: str
    item_id: int
    stream_kind: StreamKind
    item_kind: ItemKind
    entity_kind: EntityKind | None
    item_variant: ItemVariant | None
    position: int | None
    title: str
    url: str
    canonical_url: str
    published_at: str | None
    first_seen_at: str


@dataclass(frozen=True, slots=True)
class FetchRunRow:
    id: int
    source_id: str
    status: str
    started_at: str
    finished_at: str | None
    item_count: int
    new_item_count: int
    new_version_count: int
    rejected_count: int
    http_status: int | None
    error_type: str | None
    error_message: str | None
    history_json: str | None = None
