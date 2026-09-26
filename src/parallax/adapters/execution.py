from __future__ import annotations

import httpx

from parallax.adapters.base import (
    Adapter,
    CompleteStep,
    MultiRequestAdapter,
    SourceAdapter,
    SteppedAdapter,
)
from parallax.adapters.results import (
    FullRefresh,
    NotModifiedRefresh,
    RefreshOutcome,
    ResponseValidators,
)
from parallax.config import Source
from parallax.domain import HttpResponse, StreamState
from parallax.transport import Transport

MAX_ADAPTER_STEPS = 5


def run_adapter_refresh(
    adapter: Adapter,
    source: Source,
    transport: Transport,
    state: StreamState,
) -> RefreshOutcome:
    """Execute an adapter plan without persistence or scheduling decisions."""
    if isinstance(adapter, MultiRequestAdapter):
        return _run_multi_request(adapter, source, transport)
    if isinstance(adapter, SteppedAdapter):
        return _run_stepped(adapter, source, transport)
    return _run_single_request(adapter, source, transport, state)


def _run_single_request(
    adapter: SourceAdapter,
    source: Source,
    transport: Transport,
    state: StreamState,
) -> RefreshOutcome:
    response = transport.request(adapter.build_request(source), source, state)
    if response.status_code == 304:
        return NotModifiedRefresh(
            response.observed_at, ResponseValidators.from_response(response)
        )
    raise_for_status(response)
    return FullRefresh(
        adapter.parse(source, response),
        response.observed_at,
        response.status_code,
        ResponseValidators.from_response(response),
    )


def _run_multi_request(
    adapter: MultiRequestAdapter,
    source: Source,
    transport: Transport,
) -> FullRefresh:
    requests = adapter.build_requests(source)
    if not requests:
        raise ValueError(f"Adapter for {source.id!r} produced no requests")

    state = StreamState(source_id=source.id)
    responses: list[HttpResponse] = []
    for request in requests:
        response = transport.request(request, source, state)
        raise_for_status(response)
        responses.append(response)
    return FullRefresh.combined(
        adapter.parse_responses(source, tuple(responses)), tuple(responses)
    )


def _run_stepped(
    adapter: SteppedAdapter,
    source: Source,
    transport: Transport,
) -> FullRefresh:
    state = StreamState(source_id=source.id)
    step = adapter.first_step(source)
    if isinstance(step, CompleteStep):
        raise ValueError(f"Adapter for {source.id!r} must make an HTTP observation")
    responses: list[HttpResponse] = []
    for _ in range(MAX_ADAPTER_STEPS):
        response = transport.request(step.request, source, state)
        raise_for_status(response)
        responses.append(response)
        step = adapter.next_step(source, response, step.context)
        if isinstance(step, CompleteStep):
            return FullRefresh.combined(step.batch, tuple(responses))
    raise RuntimeError(f"Adapter for {source.id!r} exceeded {MAX_ADAPTER_STEPS} steps")


def raise_for_status(response: HttpResponse) -> None:
    if 200 <= response.status_code < 300:
        return
    request = httpx.Request("GET", "https://upstream.invalid/")
    raw = httpx.Response(
        status_code=response.status_code,
        request=request,
        headers=response.headers,
        content=response.content,
    )
    raise httpx.HTTPStatusError(
        f"Unexpected upstream status {response.status_code}",
        request=request,
        response=raw,
    )
