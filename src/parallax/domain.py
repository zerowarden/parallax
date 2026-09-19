from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, TypeGuard, get_args

ItemKind = Literal[
    "article",
    "post",
    "video",
    "entity",
]
EntityKind = Literal[
    "game",
    "movie",
    "product",
    "repository",
    "topic",
    "security",
]
ItemVariant = Literal["flash"]
StreamKind = Literal[
    "latest",
    "hot",
    "ranking",
    "curated",
]
BrowseSurface = Literal["news", "discover"]
BrowseView = Literal["all", "news", "discover"]
ProviderKind = Literal[
    "publisher",
    "platform",
    "community",
    "government",
    "aggregator",
]
ChannelRole = Literal["aggregate", "section", "view"]

ITEM_KINDS: frozenset[str] = frozenset(get_args(ItemKind))
ENTITY_KINDS: frozenset[str] = frozenset(get_args(EntityKind))
ITEM_VARIANTS: frozenset[str] = frozenset(get_args(ItemVariant))
STREAM_KINDS: frozenset[str] = frozenset(get_args(StreamKind))
BROWSE_VIEWS: frozenset[str] = frozenset(get_args(BrowseView))


def is_stream_kind(value: str) -> TypeGuard[StreamKind]:
    return value in STREAM_KINDS


def is_item_kind(value: str) -> TypeGuard[ItemKind]:
    return value in ITEM_KINDS


def is_entity_kind(value: str) -> TypeGuard[EntityKind]:
    return value in ENTITY_KINDS


def is_item_variant(value: str) -> TypeGuard[ItemVariant]:
    return value in ITEM_VARIANTS


def is_browse_view(value: str) -> TypeGuard[BrowseView]:
    return value in BROWSE_VIEWS


def validate_item_classification(
    *,
    item_kind: str,
    entity_kind: str | None,
    item_variant: str | None,
) -> None:
    """Enforce the ontology's conditional rules for one item declaration."""
    if item_kind == "entity":
        if entity_kind is None:
            raise ValueError("entity_kind is required when item_kind is 'entity'")
        if not is_entity_kind(entity_kind):
            raise ValueError(f"unknown entity_kind: {entity_kind!r}")
    elif entity_kind is not None:
        raise ValueError(
            f"entity_kind is only valid when item_kind is 'entity', not {item_kind!r}"
        )
    if item_variant is None:
        return
    if not is_item_variant(item_variant):
        raise ValueError(f"unknown item_variant: {item_variant!r}")
    if item_variant == "flash" and item_kind != "article":
        raise ValueError("item_variant 'flash' requires item_kind 'article'")


@dataclass(frozen=True, slots=True)
class RequestSpec:
    method: str
    url: str
    headers: Mapping[str, str] = field(default_factory=dict)
    params: Mapping[str, str] = field(default_factory=dict)
    content: bytes | None = None


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status_code: int
    url: str
    headers: Mapping[str, str]
    content: bytes
    observed_at: datetime
    cookies: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class HeadlineCandidate:
    title: str
    url: str
    external_id: str | None = None
    published_at: datetime | None = None
    raw_published_at: str | None = None
    position: int | None = None
    metrics: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ParsedBatch:
    candidates: tuple[HeadlineCandidate, ...]
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ValidatedBatch:
    candidates: tuple[HeadlineCandidate, ...]
    rejected_count: int
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class StreamState:
    source_id: str
    next_run_at: datetime | None = None
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    consecutive_failures: int = 0
    etag: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True, slots=True)
class IngestionSummary:
    source_id: str
    fetch_run_id: int
    status: str
    item_count: int = 0
    new_item_count: int = 0
    new_version_count: int = 0
    rejected_count: int = 0


@dataclass(frozen=True, slots=True)
class IngestionFailure:
    source_id: str
    error_type: str
    error_message: str


@dataclass(frozen=True, slots=True)
class IngestionBatchResult:
    summaries: tuple[IngestionSummary, ...]
    failures: tuple[IngestionFailure, ...]


@dataclass(frozen=True, slots=True)
class ChangeEvent:
    seq: int
    event_type: str
    source_id: str
    item_id: int
    item_version_id: int | None
    created_at: str


@dataclass(frozen=True, slots=True)
class AnalysisItem:
    """One committed item version projected for downstream consumers.

    ``source_language`` is the configured publisher hint, not detected
    headline language. Datetimes are timezone-aware.
    """

    change_seq: int
    event_type: str
    item_id: int
    item_version_id: int
    title: str
    source_id: str
    source_language: str
    source_enabled: bool
    stream_kind: StreamKind
    item_kind: ItemKind
    entity_kind: EntityKind | None
    item_variant: ItemVariant | None
    original_url: str
    canonical_url: str
    published_at: datetime | None
    first_seen_at: datetime
