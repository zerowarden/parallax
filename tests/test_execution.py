from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from parallax.adapters.base import CompleteStep, ContinueStep
from parallax.adapters.execution import MAX_ADAPTER_STEPS, run_adapter_refresh
from parallax.adapters.results import (
    FullRefresh,
    NotModifiedRefresh,
    ResponseValidators,
)
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
    def __init__(self, responses: Sequence[HttpResponse] = ()) -> None:
        self.requests: list[RequestSpec] = []
        self.states: list[StreamState] = []
        self.responses = list(responses)

    def request(
        self, spec: RequestSpec, source: Source, state: StreamState
    ) -> HttpResponse:
        self.requests.append(spec)
        self.states.append(state)
        if self.responses:
            return self.responses.pop(0)
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
    assert isinstance(result, FullRefresh)
    assert result.conditional is None
    assert result.http_status is None
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


class SingleFixture:
    def __init__(self) -> None:
        self.parsed = 0

    def build_request(self, source: Source) -> RequestSpec:
        return RequestSpec("GET", source.endpoint.url)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        self.parsed += 1
        return BATCH


class MultiFixture:
    def __init__(self, count: int) -> None:
        self.count = count
        self.responses: tuple[HttpResponse, ...] = ()

    def build_requests(self, source: Source) -> tuple[RequestSpec, ...]:
        return tuple(
            RequestSpec("GET", f"{source.endpoint.url}/{index}")
            for index in range(self.count)
        )

    def parse_responses(
        self, source: Source, responses: tuple[HttpResponse, ...]
    ) -> ParsedBatch:
        self.responses = responses
        return BATCH


@pytest.mark.parametrize("status", [200, 201, 304])
def test_single_execution_owns_status_validators_and_observation(status: int) -> None:
    source = make_source()
    response = HttpResponse(
        status,
        source.endpoint.url,
        {"etag": "new", "last-modified": "today"},
        b"",
        OBSERVED_AT,
        request_identity="representation",
    )
    transport = RecordingTransport([response])
    state = StreamState(source.id, etag="old", request_identity="representation")
    adapter = SingleFixture()
    result = run_adapter_refresh(adapter, source, transport, state)
    assert transport.states == [state]
    assert result.observed_at == OBSERVED_AT
    assert result.conditional == ResponseValidators("new", "today", "representation")
    if status == 304:
        assert isinstance(result, NotModifiedRefresh)
        assert adapter.parsed == 0
    else:
        assert isinstance(result, FullRefresh)
        assert adapter.parsed == 1
        assert result.batch == BATCH
        assert result.http_status == status


def test_combined_execution_owns_observation_without_stream_validators() -> None:
    source = make_source()
    responses = [
        HttpResponse(
            200,
            source.endpoint.url,
            {"etag": str(index)},
            b"",
            OBSERVED_AT + timedelta(microseconds=index),
            request_identity=str(index),
        )
        for index in (800, 100)
    ]
    transport = RecordingTransport(responses)
    adapter = MultiFixture(2)
    result = run_adapter_refresh(
        adapter,
        source,
        transport,
        StreamState(source.id, etag="stale", request_identity="stale"),
    )
    assert isinstance(result, FullRefresh)
    assert result.observed_at == OBSERVED_AT + timedelta(microseconds=800)
    assert result.conditional is None and result.http_status is None
    assert adapter.responses == tuple(responses)
    assert all(state == StreamState(source.id) for state in transport.states)


def test_empty_multi_request_plan_fails_before_io() -> None:
    source = make_source()
    transport = RecordingTransport()
    with pytest.raises(ValueError, match="no requests"):
        run_adapter_refresh(MultiFixture(0), source, transport, StreamState(source.id))
    assert transport.requests == []


@pytest.mark.parametrize("adapter", [MultiFixture(1), SteppedFixture(1)])
def test_combined_representations_cannot_treat_a_partial_304_as_success(
    adapter: MultiFixture | SteppedFixture,
) -> None:
    source = make_source()
    transport = RecordingTransport(
        [HttpResponse(304, source.endpoint.url, {}, b"", OBSERVED_AT)]
    )
    with pytest.raises(httpx.HTTPStatusError):
        run_adapter_refresh(adapter, source, transport, StreamState(source.id))


@pytest.mark.parametrize(
    "status,conditional",
    [
        (304, ResponseValidators(None, None, None)),
        (200, None),
        (None, ResponseValidators(None, None, None)),
    ],
)
def test_full_outcome_rejects_contradictory_metadata(
    status: int | None, conditional: ResponseValidators | None
) -> None:
    with pytest.raises(ValueError):
        FullRefresh(BATCH, OBSERVED_AT, status, conditional)


def test_refresh_outcomes_require_an_aware_observation() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        FullRefresh(BATCH, datetime(2026, 9, 18), None, None)
    with pytest.raises(ValueError, match="timezone-aware"):
        NotModifiedRefresh(datetime(2026, 9, 18), ResponseValidators(None, None, None))
    with pytest.raises(ValueError, match="HTTP observation"):
        FullRefresh.combined(BATCH, ())
    with pytest.raises(ValueError, match="successful HTTP responses"):
        FullRefresh.combined(
            BATCH, (HttpResponse(304, "https://example.test", {}, b"", OBSERVED_AT),)
        )
