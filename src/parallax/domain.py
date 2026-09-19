from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, TypeGuard, get_args

StreamKind = Literal["latest", "hot"]
ItemKind = Literal[
    "article",
    "flash",
    "game",
    "movie",
    "post",
    "product",
    "ranking",
    "repository",
    "trend",
    "video",
]
BrowseView = Literal["all", "news", "discover"]
BrowseCategory = Literal["news", "discover"]
STREAM_KINDS: frozenset[str] = frozenset(get_args(StreamKind))
ITEM_KINDS: frozenset[str] = frozenset(get_args(ItemKind))
BROWSE_VIEWS: frozenset[str] = frozenset(get_args(BrowseView))
DISCOVER_STREAM_KINDS: frozenset[str] = frozenset({"hot"})
DISCOVER_ITEM_KINDS: frozenset[str] = frozenset(
    {"game", "movie", "product", "ranking", "repository", "trend", "video"}
)


def is_stream_kind(value: str) -> TypeGuard[StreamKind]:
    return value in STREAM_KINDS


def is_item_kind(value: str) -> TypeGuard[ItemKind]:
    return value in ITEM_KINDS


def is_browse_view(value: str) -> TypeGuard[BrowseView]:
    return value in BROWSE_VIEWS


def classify_browse_view(
    *, stream_kind: StreamKind, item_kind: ItemKind
) -> BrowseCategory:
    """Map stored stream/item metadata to the reader's top-level views.

    Discovery covers ranking/trending/list surfaces and object catalogues;
    everything else is treated as news/editorial reporting.
    """
    if stream_kind in DISCOVER_STREAM_KINDS or item_kind in DISCOVER_ITEM_KINDS:
        return "discover"
    return "news"


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
    original_url: str
    canonical_url: str
    published_at: datetime | None
    first_seen_at: datetime
