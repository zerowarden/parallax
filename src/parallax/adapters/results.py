"""Execution outcomes with explicit representation and validator ownership."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from parallax.domain import HttpResponse, ParsedBatch


@dataclass(frozen=True, slots=True)
class ResponseValidators:
    etag: str | None
    last_modified: str | None
    request_identity: str | None

    @classmethod
    def from_response(cls, response: HttpResponse) -> ResponseValidators:
        return cls(
            response.headers.get("etag"),
            response.headers.get("last-modified"),
            response.request_identity,
        )


@dataclass(frozen=True, slots=True)
class FullRefresh:
    batch: ParsedBatch
    observed_at: datetime
    http_status: int | None
    conditional: ResponseValidators | None

    def __post_init__(self) -> None:
        _require_observation(self.observed_at)
        if self.http_status is not None and not 200 <= self.http_status < 300:
            raise ValueError("A full refresh requires a successful HTTP status")
        if (self.http_status is None) != (self.conditional is None):
            raise ValueError("Only a single-response refresh has stream validators")

    @classmethod
    def combined(
        cls, batch: ParsedBatch, responses: tuple[HttpResponse, ...]
    ) -> FullRefresh:
        if not responses:
            raise ValueError("A full refresh requires an HTTP observation")
        if any(not 200 <= response.status_code < 300 for response in responses):
            raise ValueError("A combined refresh requires successful HTTP responses")
        return cls(
            batch, max(response.observed_at for response in responses), None, None
        )


@dataclass(frozen=True, slots=True)
class NotModifiedRefresh:
    observed_at: datetime
    conditional: ResponseValidators

    def __post_init__(self) -> None:
        _require_observation(self.observed_at)


type RefreshOutcome = FullRefresh | NotModifiedRefresh


def _require_observation(observed_at: datetime) -> None:
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
