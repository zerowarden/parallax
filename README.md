# Parallax

Parallax collects headlines from configured public sources and stores them in a
durable local SQLite archive. Source adapters, HTTP transport, ingestion,
persistence, and CLI presentation remain separate.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Outbound HTTPS access to enabled sources

## Run

```bash
uv sync
uv run parallax init
uv run parallax fetch-all
uv run parallax show
```

The global feed defaults to latest observed order. Publication order is explicit,
and source-specific views retain the source's latest snapshot order:

```bash
uv run parallax show --order published
uv run parallax show --source SOURCE_ID
```

Run one due-source scheduler cycle with `uv run parallax run --once`, or keep
the scheduler running with `uv run parallax run`.

## Web reader

Serve the stored archive as a local browser reader:

```bash
uv run parallax web
```

Then open <http://127.0.0.1:8765>. The reader supports All / News / Discover
views, Today / 3 / 7 / 30 day filters, source filtering, headline search, and
pagination. Date filtering is based on when Parallax first observed an item,
not the publisher's publication time.

To keep each request bounded, the browser shows source-local appearances;
cross-source grouping remains available in the terminal feed.

## Development

```bash
uv sync --dev
make format
make check
```

Live-source checks are opt-in:

```bash
uv run pytest -m live
```

## License
MIT
