from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


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
    cookies: Mapping[str, str] = field(default_factory=dict)


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
