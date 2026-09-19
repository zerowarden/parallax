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

All / News / Discover placement is explicit source configuration, not inferred
from item or stream kinds. Items observed through several channels are
deduplicated at item level before rendering.

## Configuration

`config/config.toml` contains only Parallax package settings: `[app]`,
`[http]`, `[ingestion]`, `[scheduler]`, and `[validation]`.

Sources are discovered recursively from the `sources/` directory next to the
root file. Subdirectories and file names are organizational only; every TOML
file is validated identically. Each `[[sources]]` entry is self-contained:

```toml
[[sources]]
id = "thepaper-hot"
provider_id = "thepaper"
channel_id = "hot"
channel_label = "热榜"
channel_role = "view"
stream_kind = "hot"
item_kind = "article"
topics = []
surfaces = ["discover"]
language = "zh-CN"
market = "CN"
interval_seconds = 1800
max_items = 30
enabled = true
provider_name = "澎湃新闻"
provider_kind = "publisher"
[sources.endpoint]
adapter = "thepaper_hot"
url = "https://cache.thepaper.cn/contentapi/wwwIndex/rightSidebar"
```

File names and locations carry no semantic meaning. Inspect the catalog with:

```bash
uv run parallax config lint
uv run parallax config resolve
uv run parallax config source thepaper-hot
```

The database is a local archive and schema version 0 requires a recreated
database; run `uv run parallax init` against a fresh `data/parallax.db` after
changing the schema.

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
