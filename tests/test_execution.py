from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pytest

from parallax.adapters.base import CompleteStep, ContinueStep
from parallax.adapters.execution import MAX_ADAPTER_STEPS, run_adapter_refresh
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
    StreamState,
)
from source_factory import make_source

OBSERVED_AT = datetime(2026, 9, 18, tzinfo=UTC)
BATCH = ParsedBatch((HeadlineCandidate("Title", "https://example.test/article"),))


class RecordingTransport:
    def __init__(self) -> None:
        self.requests: list[RequestSpec] = []

    def request(
        self, spec: RequestSpec, source: Source, state: StreamState
    ) -> HttpResponse:
        self.requests.append(spec)
        return HttpResponse(
            200, spec.url, {}, b"", OBSERVED_AT + timedelta(seconds=len(self.requests))
        )


class SteppedFixture:
    def __init__(self, requests_to_complete: int) -> None:
        self.requests_to_complete = requests_to_complete

    def first_step(self, source: Source) -> ContinueStep | CompleteStep:
        if self.requests_to_complete == 0:
            return CompleteStep(BATCH)
        return ContinueStep(RequestSpec("GET", source.endpoint.url), {"count": 0})

    def next_step(
        self, source: Source, response: HttpResponse, context: Mapping[str, object]
    ) -> ContinueStep | CompleteStep:
        count = context["count"]
        assert isinstance(count, int)
        count += 1
        if count == self.requests_to_complete:
            return CompleteStep(BATCH)
        return ContinueStep(RequestSpec("GET", source.endpoint.url), {"count": count})


@pytest.mark.parametrize("count", [1, MAX_ADAPTER_STEPS])
def test_stepped_refresh_accepts_completion_through_request_limit(count: int) -> None:
    source = make_source()
    transport = RecordingTransport()
    result = run_adapter_refresh(
        SteppedFixture(count), source, transport, StreamState(source.id)
    )
    assert result.batch == BATCH
    assert len(transport.requests) == count
    assert result.observed_at == OBSERVED_AT + timedelta(seconds=count)


def test_stepped_refresh_rejects_completion_without_observation() -> None:
    source = make_source()
    transport = RecordingTransport()
    with pytest.raises(ValueError, match="HTTP observation"):
        run_adapter_refresh(
            SteppedFixture(0), source, transport, StreamState(source.id)
        )
    assert transport.requests == []


def test_stepped_refresh_never_sends_request_beyond_limit() -> None:
    source = make_source()
    transport = RecordingTransport()
    with pytest.raises(RuntimeError, match="exceeded"):
        run_adapter_refresh(
            SteppedFixture(MAX_ADAPTER_STEPS + 1),
            source,
            transport,
            StreamState(source.id),
        )
    assert len(transport.requests) == MAX_ADAPTER_STEPS
