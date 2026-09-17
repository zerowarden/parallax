from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime

from rich.console import Console
from rich.table import Table
from rich.text import Text

from parallax.config import SourceConfig
from parallax.diagnostics import SourceDiagnostic
from parallax.domain import IngestionSummary, StreamState
from parallax.storage import FetchRunRow, HeadlineRow


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

    def summaries(self, summaries: Iterable[IngestionSummary]) -> None:
        table = Table(title="Fetch results")
        table.add_column("Source")
        table.add_column("Status")
        table.add_column("Items", justify="right")
        table.add_column("New items", justify="right")
        table.add_column("New versions", justify="right")
        table.add_column("Rejected", justify="right")
        for summary in summaries:
            table.add_row(
                _literal(summary.source_id),
                _literal(summary.status),
                str(summary.item_count),
                str(summary.new_item_count),
                str(summary.new_version_count),
                str(summary.rejected_count),
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
            table = Table(title=_literal(source_name), show_header=True)
            table.add_column("#", width=4, justify="right")
            table.add_column("Headline", overflow="fold")
            table.add_column("Published", width=25)
            table.add_column("URL", overflow="fold")
            for row in source_rows:
                table.add_row(
                    str(row.position or "-"),
                    _literal(row.title),
                    _literal(row.published_at or "unknown"),
                    _literal(row.url),
                )
            self.console.print(table)

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
            error = ""
            if run.error_type:
                error = f"{run.error_type}: {run.error_message or ''}"
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


def _literal(value: object) -> Text:
    return Text(str(value))
