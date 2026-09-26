from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import datetime
from urllib.parse import urlsplit

import httpx

from parallax.adapters.base import AdapterResolver
from parallax.adapters.execution import run_adapter_refresh
from parallax.adapters.results import NotModifiedRefresh, RefreshOutcome
from parallax.archive import SourceHealthReader
from parallax.config import Source
from parallax.diagnostic_models import (
    ACCESS_BLOCKED,
    CONFIGURATION_BROKEN,
    HEALTHY,
    NETWORK_FAILING,
    QUIET,
    RATE_LIMITED,
    SCHEMA_BROKEN,
    UPSTREAM_ERROR,
    DiagnosticClassification,
    ResponseDiagnostic,
    SourceDiagnostic,
)
from parallax.domain import HttpResponse, RequestSpec, StreamState
from parallax.transport import ResponseTooLargeError, Transport
from parallax.validation import BatchValidationError, BatchValidator

LOGGER = logging.getLogger(__name__)

SAFE_RESPONSE_HEADERS = frozenset(
    {
        "content-type",
        "content-length",
        "date",
        "etag",
        "last-modified",
        "cache-control",
        "retry-after",
    }
)

_ERROR_MESSAGE_LIMIT = 500


class DiagnosticService:
    """Read-only source-health diagnostics through the shared transport.

    A diagnosis performs the adapter's normal request plan but never records
    fetch runs, snapshots, or stream state, so it cannot perturb scheduling.
    Responses and errors are reduced to bounded, secret-free evidence.
    """

    def __init__(
        self,
        storage: SourceHealthReader,
        transport: Transport,
        adapters: AdapterResolver,
        validator: BatchValidator,
    ) -> None:
        self._storage = storage
        self._transport = transport
        self._adapters = adapters
        self._validator = validator

    def diagnose(self, source: Source) -> SourceDiagnostic:
        state = self._storage.get_stream_state(source.id)
        last_change = self._storage.last_content_change_at(source.id)
        try:
            adapter = self._adapters.resolve_source(source)
        except (KeyError, ValueError) as exc:
            return self._finish(
                source,
                state,
                last_change,
                classification=CONFIGURATION_BROKEN,
                detail=str(exc),
                upstream_host=_host(source.endpoint.url),
            )

        recorder = _RecordingTransport(self._transport)
        try:
            refresh = run_adapter_refresh(adapter, source, recorder, state)
        except httpx.HTTPStatusError as exc:
            return self._status_failure(source, state, last_change, recorder, exc)
        except httpx.TransportError as exc:
            return self._exception_result(
                source,
                state,
                last_change,
                recorder,
                exc,
                classification=NETWORK_FAILING,
                detail=f"transport failure after {len(recorder.responses)} response(s)",
            )
        except ResponseTooLargeError as exc:
            return self._exception_result(
                source,
                state,
                last_change,
                recorder,
                exc,
                classification=UPSTREAM_ERROR,
                detail="response exceeded the configured size limit",
            )
        except (ValueError, RuntimeError) as exc:
            classification: DiagnosticClassification = (
                SCHEMA_BROKEN if recorder.responses else CONFIGURATION_BROKEN
            )
            detail = (
                "response did not match the expected schema"
                if classification == SCHEMA_BROKEN
                else "adapter configuration or request construction failed"
            )
            return self._exception_result(
                source,
                state,
                last_change,
                recorder,
                exc,
                classification=classification,
                detail=detail,
            )

        return self._completed(source, state, last_change, recorder, refresh)

    def _completed(
        self,
        source: Source,
        state: StreamState,
        last_change: datetime | None,
        recorder: _RecordingTransport,
        refresh: RefreshOutcome,
    ) -> SourceDiagnostic:
        host = _upstream_host(recorder.responses, source)
        if isinstance(refresh, NotModifiedRefresh):
            return self._finish(
                source,
                state,
                last_change,
                classification=QUIET,
                detail="upstream reported not modified (HTTP 304)",
                upstream_host=host,
                responses=_response_diagnostics(recorder.responses),
            )

        parsed_count = len(refresh.batch.candidates)
        try:
            validated = self._validator.validate(
                source.id, refresh.batch, provider_id=source.provider_id
            )
        except BatchValidationError as exc:
            return self._finish(
                source,
                state,
                last_change,
                classification=SCHEMA_BROKEN,
                detail=(
                    "empty batch rejected by validation policy"
                    if parsed_count == 0
                    else f"all {parsed_count} candidate(s) failed validation"
                ),
                upstream_host=host,
                responses=_response_diagnostics(recorder.responses),
                accepted_count=0,
                rejected_count=parsed_count,
                error_type=type(exc).__name__,
                error_message=str(exc)[:_ERROR_MESSAGE_LIMIT],
            )

        classification: DiagnosticClassification
        if validated.candidates:
            classification = HEALTHY
            detail = f"accepted {len(validated.candidates)} headline item(s)"
        else:
            classification = QUIET
            detail = "upstream returned no headline items"
        return self._finish(
            source,
            state,
            last_change,
            classification=classification,
            detail=detail,
            upstream_host=host,
            responses=_response_diagnostics(recorder.responses),
            accepted_count=len(validated.candidates),
            rejected_count=validated.rejected_count,
            warnings=validated.warnings,
        )

    def _status_failure(
        self,
        source: Source,
        state: StreamState,
        last_change: datetime | None,
        recorder: _RecordingTransport,
        exc: httpx.HTTPStatusError,
    ) -> SourceDiagnostic:
        status = exc.response.status_code
        return self._finish(
            source,
            state,
            last_change,
            classification=_classify_status(status),
            detail=f"upstream returned HTTP {status}",
            upstream_host=_upstream_host(recorder.responses, source),
            responses=_response_diagnostics(recorder.responses),
            error_type=type(exc).__name__,
            error_message=f"HTTP {status}",
        )

    def _exception_result(
        self,
        source: Source,
        state: StreamState,
        last_change: datetime | None,
        recorder: _RecordingTransport,
        exc: Exception,
        *,
        classification: DiagnosticClassification,
        detail: str,
    ) -> SourceDiagnostic:
        return self._finish(
            source,
            state,
            last_change,
            classification=classification,
            detail=detail,
            upstream_host=_upstream_host(recorder.responses, source),
            responses=_response_diagnostics(recorder.responses),
            error_type=type(exc).__name__,
            error_message=str(exc)[:_ERROR_MESSAGE_LIMIT],
        )

    def _finish(
        self,
        source: Source,
        state: StreamState,
        last_change: datetime | None,
        *,
        classification: DiagnosticClassification,
        detail: str,
        upstream_host: str,
        responses: tuple[ResponseDiagnostic, ...] = (),
        accepted_count: int | None = None,
        rejected_count: int | None = None,
        warnings: tuple[str, ...] = (),
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> SourceDiagnostic:
        diagnostic = SourceDiagnostic(
            source_id=source.id,
            name=source.channel_label,
            adapter=source.endpoint.adapter,
            upstream_host=upstream_host,
            last_attempt_at=state.last_attempt_at,
            last_success_at=state.last_success_at,
            last_change_at=last_change,
            consecutive_failures=state.consecutive_failures,
            classification=classification,
            detail=detail,
            responses=responses,
            accepted_count=accepted_count,
            rejected_count=rejected_count,
            warnings=warnings,
            error_type=error_type,
            error_message=error_message,
        )
        LOGGER.info(
            "operation=diagnose source_id=%s adapter=%s classification=%s "
            "responses=%s accepted=%s rejected=%s",
            diagnostic.source_id,
            diagnostic.adapter,
            diagnostic.classification,
            len(diagnostic.responses),
            diagnostic.accepted_count,
            diagnostic.rejected_count,
        )
        return diagnostic


class _RecordingTransport:
    """Transport decorator that retains responses as diagnostic evidence."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport
        self.responses: list[HttpResponse] = []

    def request(
        self,
        spec: RequestSpec,
        source: Source,
        state: StreamState,
    ) -> HttpResponse:
        response = self._transport.request(spec, source, state)
        self.responses.append(response)
        return response


def _classify_status(status: int) -> DiagnosticClassification:
    if status == 429:
        return RATE_LIMITED
    if status in {401, 403}:
        return ACCESS_BLOCKED
    return UPSTREAM_ERROR


def _response_diagnostics(
    responses: Sequence[HttpResponse],
) -> tuple[ResponseDiagnostic, ...]:
    return tuple(
        ResponseDiagnostic(
            status_code=response.status_code,
            content_type=response.headers.get("content-type"),
            byte_count=len(response.content),
            headers=_safe_headers(response.headers),
        )
        for response in responses
    )


def _safe_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {
        name: value
        for name, value in headers.items()
        if name.lower() in SAFE_RESPONSE_HEADERS
    }


def _upstream_host(responses: Sequence[HttpResponse], source: Source) -> str:
    urls = [response.url for response in responses if response.url]
    try:
        return _host(urls[-1] if urls else source.endpoint.url)
    except ValueError:
        return ""


def _host(url: str) -> str:
    return urlsplit(url).hostname or ""
