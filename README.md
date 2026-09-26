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

Browsing shows the most recently observed title across channels. “Appeared in”
identifies a source that observed the item; it does not attribute that wording to
the source. Source filters select appearances. `show --source` uses the specific
version preserved in that source's latest snapshot.

`sources` loads only the catalog. `show`, `status`, `runs`, and `web` open an
existing archive read-only, using its stored catalog without reconciliation.
`doctor` probes the configured sources but also opens the archive read-only.
Initialize an archive before using these commands. Only collector commands
(`init`, `fetch`, `fetch-all`, `run`) reconcile the catalog. An existing empty
source directory is valid and disables every stored source on reconciliation;
history remains stored.

## Historical collection

`fetch SOURCE_ID --since 7d` and `fetch-all --since 7d` request best-effort
publication history. Unsupported adapters return `unsupported` without a live
refresh. Each result reports the requested window, earliest/latest accepted
publication timestamps, pages fetched, accepted items, and a stopping reason.
This range is observed evidence, not a guarantee of complete window coverage.
The outcome is also retained in `fetch_runs.history_json`.

Now News (`now_news`, configured stream `now-news`) uses the public, undocumented
`https://newsapi1.now.com/pccw-news-api/api/getRankNewsList` JSON listing without
authentication. `publishDate` supplies publication time; listing order supplies
rank. History pages are fetched and parsed individually. The positive integer
endpoint options `history_max_pages` (default 5), `history_page_size` (100), and
`history_max_items` (1000) are independent of the live `max_items` limit and are
checked by catalog linting. Collection stops at an item/page budget or an empty
upstream page. Ranked pages are not assumed to be chronological, so old items
do not prove that the requested window has been covered.

Hacker News history retains its one-page Algolia search budget (at most 100
hits), with a separate `history_max_items` option (default 1000). It reports
truncation unless the response establishes exhaustion. No live re-verification
was performed for these history changes; parser contracts use checked-in
fixtures. A successful backfill preserves the live snapshot and freshness state.

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

The database is a local archive with schema version 1 and no upgrade migrations.
This format stores fixed-width UTC microseconds, conditional-request identities,
and history outcomes. Version 0 archives are rejected without modification:
preserve the old archive, configure a fresh database path, and run
`uv run parallax init`. Previously discarded timestamp precision cannot be
recovered. Equal observation timestamps break title ties by version ID and
snapshot ties by snapshot ID.

Collectors hold an exclusive POSIX lock beside the database for their lifetime.
After acquiring ownership, startup finalizes leftover `running` records as
`AbandonedRun` and makes those sources due again. Source payload failures may be
isolated after rollback; SQLite failures abort a batch with their original
category. Failure recording is best effort if SQLite is unavailable. Readers
neither take collector ownership nor recover runs. Direct storage callers must
follow the same ownership policy before recovering abandoned work.

Conditional validators are bound to a hash of the effective request, including
method, URL, parameters, headers/auth, body, and representation-selection
settings. A changed request sends no previous validators. Full responses replace
validator state (including absent headers); matching 304 responses preserve
omitted validators. Only the hash is persisted, not resolved credentials.

RSS/Atom parsing uses `defusedxml` with DTD, entity, and external-reference
prohibitions enabled; transport response-size limits remain independent.

## Development

```bash
uv sync --dev
make format
make check
```

Ruff handles linting and formatting. Type checks use mypy for application code
and basedpyright for application code and tests. The local pre-commit hooks run
the same lint, format, and type checks as `make check`.

Live-source checks are opt-in:

```bash
uv run pytest -m live
```

## License
MIT
