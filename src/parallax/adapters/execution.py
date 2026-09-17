from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import httpx

from parallax.adapters.base import (
    Adapter,
    CompleteStep,
    MultiRequestAdapter,
    SourceAdapter,
    SteppedAdapter,
)
from parallax.config import SourceConfig
from parallax.domain import HttpResponse, ParsedBatch, StreamState
from parallax.transport import Transport

MAX_ADAPTER_STEPS = 5


@dataclass(frozen=True, slots=True)
class AdapterRefresh:
    """One adapter plan executed through the shared transport."""

    responses: tuple[HttpResponse, ...]
    batch: ParsedBatch | None = None
    not_modified: bool = False


def run_adapter_refresh(
    adapter: Adapter,
    source: SourceConfig,
    transport: Transport,
    state: StreamState,
) -> AdapterRefresh:
    """Execute an adapter plan without persistence or scheduling decisions."""
    if isinstance(adapter, MultiRequestAdapter):
        return _run_multi_request(adapter, source, transport)
    if isinstance(adapter, SteppedAdapter):
        return _run_stepped(adapter, source, transport)
    return _run_single_request(adapter, source, transport, state)


def _run_single_request(
    adapter: SourceAdapter,
    source: SourceConfig,
    transport: Transport,
    state: StreamState,
) -> AdapterRefresh:
    response = transport.request(adapter.build_request(source), source, state)
    if response.status_code == 304:
        return AdapterRefresh(responses=(response,), not_modified=True)
    raise_for_status(response)
    return AdapterRefresh(
        responses=(response,),
        batch=adapter.parse(source, response),
    )


def _run_multi_request(
    adapter: MultiRequestAdapter,
    source: SourceConfig,
    transport: Transport,
) -> AdapterRefresh:
    requests = adapter.build_requests(source)
    if not requests:
        raise ValueError(f"Adapter for {source.id!r} produced no requests")

    state = StreamState(source_id=source.id)
    responses: list[HttpResponse] = []
    for request in requests:
        response = transport.request(request, source, state)
        raise_for_status(response)
        responses.append(response)
    return AdapterRefresh(
        responses=tuple(responses),
        batch=adapter.parse_responses(source, tuple(responses)),
    )


def _run_stepped(
    adapter: SteppedAdapter,
    source: SourceConfig,
    transport: Transport,
) -> AdapterRefresh:
    state = StreamState(source_id=source.id)
    context: Mapping[str, object] = {}
    step = adapter.first_step(source)
    responses: list[HttpResponse] = []
    for _ in range(MAX_ADAPTER_STEPS):
        if isinstance(step, CompleteStep):
            return AdapterRefresh(responses=tuple(responses), batch=step.batch)
        context = step.context
        response = transport.request(step.request, source, state)
        raise_for_status(response)
        responses.append(response)
        step = adapter.next_step(source, response, context)
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
