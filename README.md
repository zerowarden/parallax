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

Run one due-source scheduler cycle with `uv run parallax run --once`, or keep
the scheduler running with `uv run parallax run`.

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
