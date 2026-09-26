"""Source-health result values without transport or storage dependencies."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Final, Literal

type DiagnosticClassification = Literal[
    "healthy",
    "quiet",
    "rate-limited",
    "access-blocked",
    "schema-broken",
    "network-failing",
    "configuration-broken",
    "upstream-error",
]

HEALTHY: Final = "healthy"
QUIET: Final = "quiet"
RATE_LIMITED: Final = "rate-limited"
ACCESS_BLOCKED: Final = "access-blocked"
SCHEMA_BROKEN: Final = "schema-broken"
NETWORK_FAILING: Final = "network-failing"
CONFIGURATION_BROKEN: Final = "configuration-broken"
UPSTREAM_ERROR: Final = "upstream-error"

_PROBLEM_CLASSIFICATIONS = frozenset(
    {
        RATE_LIMITED,
        ACCESS_BLOCKED,
        SCHEMA_BROKEN,
        NETWORK_FAILING,
        CONFIGURATION_BROKEN,
        UPSTREAM_ERROR,
    }
)


@dataclass(frozen=True, slots=True)
class ResponseDiagnostic:
    """Bounded, secret-free view of one upstream response."""

    status_code: int
    content_type: str | None
    byte_count: int
    headers: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class SourceDiagnostic:
    source_id: str
    name: str
    adapter: str
    upstream_host: str
    last_attempt_at: datetime | None
    last_success_at: datetime | None
    last_change_at: datetime | None
    consecutive_failures: int
    classification: DiagnosticClassification
    detail: str
    responses: tuple[ResponseDiagnostic, ...] = ()
    accepted_count: int | None = None
    rejected_count: int | None = None
    warnings: tuple[str, ...] = ()
    error_type: str | None = None
    error_message: str | None = None

    @property
    def healthy(self) -> bool:
        return self.classification == HEALTHY

    @property
    def has_problem(self) -> bool:
        return self.classification in _PROBLEM_CLASSIFICATIONS
