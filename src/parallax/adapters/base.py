from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol, runtime_checkable

from parallax.config import SourceConfig
from parallax.domain import HttpResponse, ParsedBatch, RequestSpec


class SourceAdapter(Protocol):
    def build_request(self, source: SourceConfig) -> RequestSpec:
        """Build the upstream request without performing I/O."""
        ...

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        """Parse an upstream response without persistence or display concerns."""
        ...


@runtime_checkable
class MultiRequestAdapter(Protocol):
    """Adapter that needs several independent upstream requests per refresh.

    The adapter owns request construction and response combination only; the
    orchestration layer performs every request through the shared transport.
    """

    def build_requests(self, source: SourceConfig) -> tuple[RequestSpec, ...]:
        """Build the ordered upstream requests without performing I/O."""
        ...

    def parse_responses(
        self,
        source: SourceConfig,
        responses: tuple[HttpResponse, ...],
    ) -> ParsedBatch:
        """Parse the ordered upstream responses without persistence concerns."""
        ...


@dataclass(frozen=True, slots=True)
class ContinueStep:
    """A request to perform next, with the adapter's own carried context."""

    request: RequestSpec
    context: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CompleteStep:
    """The parsed batch that ends a stepped sequence."""

    batch: ParsedBatch


AdapterStep = ContinueStep | CompleteStep


@runtime_checkable
class SteppedAdapter(Protocol):
    """Adapter whose requests depend on earlier responses.

    Used for session/bootstrap flows where a later request needs cookies or
    values from an earlier response. The adapter owns the sequence and its
    immutable context; the orchestration layer performs every request through
    the shared transport under a bounded step count.
    """

    def first_step(self, source: SourceConfig) -> AdapterStep:
        """Build the first request or complete immediately."""
        ...

    def next_step(
        self,
        source: SourceConfig,
        response: HttpResponse,
        context: Mapping[str, object],
    ) -> AdapterStep:
        """Advance the sequence using the previous response and context."""
        ...


Adapter = SourceAdapter | MultiRequestAdapter | SteppedAdapter


@runtime_checkable
class HistoricalAdapter(Protocol):
    """Adapter that can widen a refresh to a publication-time window.

    A history fetch is best effort: the adapter translates the requested
    ``since`` window into whatever the upstream surface supports (date filters,
    bounded pagination) and keeps only in-window items. An empty window is a
    valid result; incompatible payloads must still fail clearly.
    """

    def build_history_requests(
        self,
        source: SourceConfig,
        since: datetime,
    ) -> tuple[RequestSpec, ...]:
        """Build the requests covering items published since ``since``."""
        ...

    def parse_history_responses(
        self,
        source: SourceConfig,
        responses: tuple[HttpResponse, ...],
        since: datetime,
    ) -> ParsedBatch:
        """Parse the history responses, keeping only in-window items."""
        ...


class AdapterLookup(Protocol):
    def get(self, name: str) -> Adapter:
        """Return the registered adapter for a configured name."""
        ...
