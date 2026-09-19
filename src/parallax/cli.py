from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from parallax.adapters import AdapterRegistry
from parallax.config import CatalogError, ResolvedConfig, load_catalog
from parallax.feed import FeedOrder, build_headline_feed, build_headline_groups
from parallax.parsing import parse_since
from parallax.presentation import Presenter
from parallax.runtime import Runtime
from parallax.web import create_app

app = typer.Typer(
    no_args_is_help=True,
    help="Collect and inspect public headline feeds.",
)
config_app = typer.Typer(
    no_args_is_help=True,
    help="Validate and inspect the configured catalog.",
)
app.add_typer(config_app, name="config")
CONFIG_OPTION = Annotated[
    Path,
    typer.Option(
        "--config",
        "-c",
        exists=True,
        dir_okay=False,
        readable=True,
        help="Path to the TOML configuration file.",
    ),
]
SINCE_OPTION = Annotated[
    str | None,
    typer.Option(
        "--since",
        help="Backfill items published within a window (7d, 2w, 3m, 1y).",
    ),
]
ORDER_OPTION = Annotated[
    FeedOrder,
    typer.Option(
        "--order",
        help="Global feed order: observed (latest seen) or published.",
    ),
]


def _runtime(config: Path) -> Runtime:
    return Runtime.build(config)


def _load_catalog_or_exit(config: Path) -> ResolvedConfig:
    try:
        return load_catalog(config)
    except CatalogError as exc:
        typer.echo(f"Catalog invalid: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def _since_window(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return parse_since(value)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


@app.command("init")
def initialize(
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Initialize the database and synchronize configured sources."""
    with _runtime(config) as runtime:
        logging.getLogger(__name__).info(
            "operation=cli_init database=%s",
            runtime.config.app.database_path,
        )
        typer.echo(f"Initialized {runtime.config.app.database_path}")


@app.command("sources")
def list_sources(
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Display configured source streams."""
    with _runtime(config) as runtime:
        logging.getLogger(__name__).info("operation=cli_sources")
        Presenter().sources(runtime.registry.all())


@app.command("fetch")
def fetch_source(
    source_id: Annotated[str, typer.Argument(help="Configured source id")],
    since: SINCE_OPTION = None,
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Fetch one source immediately, independent of its schedule."""
    window = _since_window(since)
    with _runtime(config) as runtime:
        source = runtime.registry.get(source_id)
        logging.getLogger(__name__).info(
            "operation=cli_fetch source_id=%s since=%s",
            source.id,
            since or "-",
        )
        try:
            summary = runtime.ingestion.fetch_source(source, since=window)
        except Exception as exc:
            typer.echo(
                f"Fetch failed for {source.id}: {type(exc).__name__}: {exc}",
                err=True,
            )
            raise typer.Exit(code=1) from exc
        Presenter().fetch_results([summary])


@app.command("fetch-all")
def fetch_all(
    since: SINCE_OPTION = None,
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Fetch every enabled source immediately."""
    window = _since_window(since)
    with _runtime(config) as runtime:
        logger = logging.getLogger(__name__)
        logger.info(
            "operation=cli_fetch_all count=%s since=%s",
            len(runtime.registry.enabled()),
            since or "-",
        )
        result = runtime.ingestion.fetch_sources(
            runtime.registry.enabled(),
            since=window,
        )
        Presenter().fetch_results(result.summaries, result.failures)
    if result.failures:
        raise typer.Exit(code=1)


@app.command("run")
def run_scheduler(
    once: Annotated[
        bool,
        typer.Option("--once", help="Run one due-source scheduler cycle and exit."),
    ] = False,
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Run the persistent scheduler."""
    with _runtime(config) as runtime:
        logging.getLogger(__name__).info(
            "operation=cli_run_scheduler once=%s",
            once,
        )
        if once:
            runtime.scheduler.run_due_once()
            return
        try:
            runtime.scheduler.run_forever()
        except KeyboardInterrupt:
            logging.getLogger(__name__).info(
                "operation=scheduler_stop reason=keyboard_interrupt"
            )


@app.command("show")
def show_headlines(
    source_id: Annotated[
        str | None,
        typer.Option("--source", help="Only show one configured source id."),
    ] = None,
    limit: Annotated[
        int | None,
        typer.Option(
            "--limit",
            min=1,
            help=(
                "Optional headlines per source. "
                "Default: all stored in latest snapshot."
            ),
        ),
    ] = None,
    order: ORDER_OPTION = "observed",
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Display the latest stored snapshot for each source."""
    if source_id is not None and order != "observed":
        raise typer.BadParameter("--order is only valid for the global feed")
    with _runtime(config) as runtime:
        if source_id is not None:
            runtime.registry.get(source_id)
        logging.getLogger(__name__).info(
            "operation=cli_show source_id=%s limit=%s order=%s",
            source_id or "all",
            limit,
            order,
        )
        rows = runtime.storage.latest_snapshot_headlines(
            limit_per_source=limit,
            source_id=source_id,
        )
        if source_id is None:
            groups = build_headline_groups(rows, order=order)
            Presenter().feed(build_headline_feed(groups), order=order)
        else:
            Presenter().headlines(rows)


@app.command("web")
def serve_web(
    host: Annotated[
        str,
        typer.Option("--host", help="Address the local reader binds to."),
    ] = "127.0.0.1",
    port: Annotated[
        int,
        typer.Option("--port", min=1, max=65535, help="Port for the local reader."),
    ] = 8765,
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Serve the browser reader over the stored headline archive."""
    with _runtime(config) as runtime:
        logging.getLogger("werkzeug").setLevel(logging.WARNING)
        application = create_app(runtime.storage, runtime.registry.enabled())
        typer.echo(f"Parallax web reader: http://{host}:{port}")
        application.run(host=host, port=port, threaded=False)


@app.command("doctor")
def diagnose_sources(
    source_id: Annotated[
        str | None,
        typer.Argument(
            help="Configured source id; omit to diagnose all enabled sources."
        ),
    ] = None,
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Diagnose source health without recording fetch results."""
    with _runtime(config) as runtime:
        if source_id is not None:
            sources = [runtime.registry.get(source_id)]
        else:
            sources = list(runtime.registry.enabled())
        logging.getLogger(__name__).info(
            "operation=cli_doctor source_id=%s count=%s",
            source_id or "all",
            len(sources),
        )
        diagnostics = [runtime.diagnostics.diagnose(source) for source in sources]
        Presenter().diagnostics(diagnostics)
        if any(diagnostic.has_problem for diagnostic in diagnostics):
            raise typer.Exit(code=1)


@app.command("status")
def show_status(
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Display scheduler state and recent collection outcomes."""
    with _runtime(config) as runtime:
        logging.getLogger(__name__).info("operation=cli_status")
        Presenter().status(
            runtime.storage.stream_states(enabled_only=True),
            runtime.storage.latest_fetch_runs(enabled_only=True),
        )


@app.command("runs")
def show_fetch_runs(
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, max=1000, help="Historical fetch runs."),
    ] = 50,
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Display fetch history. Status intentionally shows only the latest run."""
    with _runtime(config) as runtime:
        logging.getLogger(__name__).info(
            "operation=cli_runs limit=%s",
            limit,
        )
        Presenter().fetch_runs(
            runtime.storage.recent_fetch_runs(limit=limit),
            title="Fetch run history",
        )


@config_app.command("lint")
def config_lint(
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Validate the entire catalog, including adapter references and options."""
    resolved = _load_catalog_or_exit(config)
    registry = AdapterRegistry()
    problems: list[str] = []
    for source in resolved.sources:
        try:
            registry.validate_source(source)
        except (KeyError, ValueError) as exc:
            problems.append(f"{source.id}: {exc}")
    if problems:
        for problem in problems:
            typer.echo(problem, err=True)
        typer.echo(f"Catalog invalid: {len(problems)} source problem(s)", err=True)
        raise typer.Exit(code=1)
    typer.echo(
        f"Catalog OK: {len(resolved.sources)} sources "
        f"({len(resolved.enabled_sources())} enabled)"
    )


@config_app.command("resolve")
def config_resolve(
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Print the fully resolved configuration as deterministic JSON."""
    resolved = _load_catalog_or_exit(config)
    typer.echo(resolved.resolved_json())


@config_app.command("source")
def config_source(
    source_id: Annotated[str, typer.Argument(help="Configured source id")],
    config: CONFIG_OPTION = Path("config/config.toml"),
) -> None:
    """Show one fully resolved source definition."""
    resolved = _load_catalog_or_exit(config)
    try:
        source = resolved.source(source_id)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    Presenter().source_detail(source)
