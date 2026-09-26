"""Archive read contracts, independent of SQLite and collection services."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from parallax.domain import (
    BrowseView,
    EntityKind,
    ItemKind,
    ItemVariant,
    RunKind,
    StreamKind,
    StreamState,
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
    published_at: datetime | None
    first_seen_at: datetime


@dataclass(frozen=True, slots=True)
class FetchRunRow:
    id: int
    source_id: str
    status: str
    run_kind: RunKind
    started_at: datetime
    finished_at: datetime | None
    item_count: int
    new_item_count: int
    new_version_count: int
    rejected_count: int
    http_status: int | None
    error_type: str | None
    error_message: str | None
    history_json: str | None = None


class BrowseHeadlineReader(Protocol):
    def count_browse_headlines(
        self,
        *,
        view: BrowseView,
        since: datetime,
        until: datetime,
        query: str | None = None,
        source_id: str | None = None,
    ) -> int: ...

    def browse_headlines(
        self,
        *,
        view: BrowseView,
        since: datetime,
        until: datetime,
        query: str | None = None,
        source_id: str | None = None,
        limit: int,
        offset: int,
    ) -> list[HeadlineRow]: ...


class SourceHealthReader(Protocol):
    def get_stream_state(self, source_id: str) -> StreamState: ...
    def last_content_change_at(self, source_id: str) -> datetime | None: ...
