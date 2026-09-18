from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from rich.console import Console
from rich.table import Table
from rich.text import Text

from parallax.config import SourceConfig
from parallax.diagnostics import SourceDiagnostic
from parallax.domain import (
    IngestionFailure,
    IngestionSummary,
    StreamState,
)
from parallax.feed import HeadlineFeedRow
from parallax.storage import FetchRunRow, HeadlineRow

FAILED_STATUS = "failed"


@dataclass(frozen=True, slots=True)
class FetchResultRow:
    """Presentation row unifying one fetch summary or failure."""

    source_id: str
    status: str
    item_count: int | None
    new_item_count: int | None
    new_version_count: int | None
    rejected_count: int | None
    error: str | None = None

    @property
    def failed(self) -> bool:
        return self.status == FAILED_STATUS


def build_fetch_result_rows(
    summaries: Iterable[IngestionSummary],
    failures: Iterable[IngestionFailure],
) -> tuple[FetchResultRow, ...]:
    return tuple(_summary_row(summary) for summary in summaries) + tuple(
        _failure_row(failure) for failure in failures
    )


def _summary_row(summary: IngestionSummary) -> FetchResultRow:
    return FetchResultRow(
        source_id=summary.source_id,
        status=summary.status,
        item_count=summary.item_count,
        new_item_count=summary.new_item_count,
        new_version_count=summary.new_version_count,
        rejected_count=summary.rejected_count,
    )


def _failure_row(failure: IngestionFailure) -> FetchResultRow:
    return FetchResultRow(
        source_id=failure.source_id,
        status=FAILED_STATUS,
        item_count=None,
        new_item_count=None,
        new_version_count=None,
        rejected_count=None,
        error=_error_text(failure.error_type, failure.error_message),
    )


class Presenter:
    def __init__(self, console: Console | None = None) -> None:
        self.console = console or Console()

    def sources(self, sources: Iterable[SourceConfig]) -> None:
        table = Table(title="Configured sources")
        table.add_column("ID")
        table.add_column("Name")
        table.add_column("Region")
        table.add_column("Adapter")
        table.add_column("Method")
        table.add_column("Schedule")
        table.add_column("Enabled")
        for source in sources:
            table.add_row(
                _literal(source.id),
                _literal(source.name),
                _literal(source.region),
                _literal(source.adapter),
                _literal(source.retrieval_method),
                f"{source.schedule_seconds}s",
                "yes" if source.enabled else "no",
            )
        self.console.print(table)

    def fetch_results(
        self,
        summaries: Iterable[IngestionSummary],
        failures: Iterable[IngestionFailure] = (),
    ) -> None:
        table = Table(title="Fetch results")
        table.add_column("Source")
        table.add_column("Status")
        table.add_column("Items", justify="right")
        table.add_column("New items", justify="right")
        table.add_column("New versions", justify="right")
        table.add_column("Rejected", justify="right")
        table.add_column("Error")
        for row in build_fetch_result_rows(summaries, failures):
            status = (
                Text(row.status, style="red") if row.failed else _literal(row.status)
            )
            table.add_row(
                _literal(row.source_id),
                status,
                _count(row.item_count),
                _count(row.new_item_count),
                _count(row.new_version_count),
                _count(row.rejected_count),
                _literal(row.error or ""),
            )
        self.console.print(table)

    def headlines(self, rows: Iterable[HeadlineRow]) -> None:
        grouped: dict[str, list[HeadlineRow]] = defaultdict(list)
        for row in rows:
            grouped[row.source_name].append(row)

        if not grouped:
            self.console.print("No stored headlines yet.")
            return

        for source_name, source_rows in grouped.items():
            table = self._headline_table(source_name, with_source=False)
            for row in source_rows:
                table.add_row(
                    str(row.position or "-"),
                    _literal(row.title),
                    _literal(row.published_at or "unknown"),
                    _literal(row.url),
                )
            self.console.print(table)

    def feed(self, rows: Iterable[HeadlineFeedRow]) -> None:
        table = self._headline_table("Latest headlines", with_source=True)
        count = 0
        for count, row in enumerate(rows, start=1):
            source = (
                row.source_name
                if row.duplicate_count == 0
                else f"{row.source_name} (+{row.duplicate_count})"
            )
            table.add_row(
                str(count),
                _literal(row.title),
                _literal(row.published_at or "unknown"),
                _literal(source),
                _literal(row.url),
            )
        if count == 0:
            self.console.print("No stored headlines yet.")
            return
        self.console.print(table)

    def _headline_table(self, title: str, *, with_source: bool) -> Table:
        table = Table(title=_literal(title), show_header=True)
        table.add_column("#", width=4, justify="right")
        table.add_column("Headline", overflow="fold")
        table.add_column("Published", width=25)
        if with_source:
            table.add_column("Source", overflow="fold")
        table.add_column("URL", overflow="fold")
        return table

    def status(
        self,
        states: Iterable[StreamState],
        runs: Iterable[FetchRunRow],
    ) -> None:
        state_table = Table(title="Enabled stream state")
        state_table.add_column("Source")
        state_table.add_column("Last attempt")
        state_table.add_column("Last success")
        state_table.add_column("Next run")
        state_table.add_column("Failures", justify="right")
        for state in states:
            state_table.add_row(
                _literal(state.source_id),
                _dt(state.last_attempt_at),
                _dt(state.last_success_at),
                _dt(state.next_run_at),
                str(state.consecutive_failures),
            )
        self.console.print(state_table)
        self.fetch_runs(runs, title="Latest fetch run per enabled source")

    def diagnostics(self, diagnostics: Iterable[SourceDiagnostic]) -> None:
        for diagnostic in diagnostics:
            table = Table(
                title=_literal(f"{diagnostic.source_id} — {diagnostic.classification}")
            )
            table.add_column("Field", style="bold", no_wrap=True)
            table.add_column("Value", overflow="fold")
            table.add_row("Name", _literal(diagnostic.name))
            table.add_row("Adapter", _literal(diagnostic.adapter))
            table.add_row(
                "Upstream host", _literal(diagnostic.upstream_host or "unknown")
            )
            table.add_row("Last attempt", _dt(diagnostic.last_attempt_at))
            table.add_row("Last success", _dt(diagnostic.last_success_at))
            table.add_row("Last content change", _dt(diagnostic.last_change_at))
            table.add_row("Consecutive failures", str(diagnostic.consecutive_failures))
            table.add_row("Classification", _literal(diagnostic.classification))
            table.add_row("Detail", _literal(diagnostic.detail))
            if diagnostic.accepted_count is not None:
                table.add_row("Accepted", str(diagnostic.accepted_count))
            if diagnostic.rejected_count is not None:
                table.add_row("Rejected", str(diagnostic.rejected_count))
            if diagnostic.error_type:
                table.add_row(
                    "Error",
                    _literal(
                        f"{diagnostic.error_type}: {diagnostic.error_message or ''}"
                    ),
                )
            if diagnostic.warnings:
                table.add_row("Warnings", _literal("; ".join(diagnostic.warnings)))
            for index, response in enumerate(diagnostic.responses, start=1):
                table.add_row(f"Response {index} status", str(response.status_code))
                table.add_row(
                    f"Response {index} content type",
                    response.content_type or "unknown",
                )
                table.add_row(f"Response {index} bytes", str(response.byte_count))
                for name, value in sorted(response.headers.items()):
                    table.add_row(f"Response {index} {name}", _literal(value))
            self.console.print(table)

    def fetch_runs(
        self,
        runs: Iterable[FetchRunRow],
        title: str = "Fetch runs",
    ) -> None:
        run_table = Table(title=_literal(title))
        run_table.add_column("Run")
        run_table.add_column("Source")
        run_table.add_column("Status")
        run_table.add_column("HTTP")
        run_table.add_column("Items", justify="right")
        run_table.add_column("New", justify="right")
        run_table.add_column("Versions", justify="right")
        run_table.add_column("Error")
        for run in runs:
            error = (
                _error_text(run.error_type, run.error_message) if run.error_type else ""
            )
            run_table.add_row(
                str(run.id),
                _literal(run.source_id),
                _literal(run.status),
                str(run.http_status or "-"),
                str(run.item_count),
                str(run.new_item_count),
                str(run.new_version_count),
                _literal(error),
            )
        self.console.print(run_table)


def _dt(value: datetime | None) -> str:
    return "-" if value is None else value.isoformat(timespec="seconds")


def _count(value: int | None) -> str:
    return "-" if value is None else str(value)


def _error_text(error_type: str, error_message: str | None) -> str:
    if not error_message:
        return error_type
    return f"{error_type}: {error_message}"


def _literal(value: object) -> Text:
    return Text(str(value))
