"""SQLite connection, schema compatibility, and transactions."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = 1


def open_connection(
    database_path: Path, *, read_only: bool = False
) -> sqlite3.Connection:
    if not read_only:
        database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(
        (f"{database_path.resolve().as_uri()}?mode=ro" if read_only else database_path),
        timeout=10.0,
        isolation_level=None,
        uri=read_only,
    )
    try:
        connection.row_factory = sqlite3.Row
        check_schema(connection, require_existing=read_only)
        connection.execute("PRAGMA foreign_keys = ON")
        if not read_only:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 10000")
    except BaseException:
        connection.close()
        raise
    LOGGER.info(
        "operation=database_open path=%s sqlite_version=%s",
        database_path,
        sqlite3.sqlite_version,
    )

    return connection


def close_connection(connection: sqlite3.Connection, database_path: Path) -> None:
    LOGGER.info("operation=database_close path=%s", database_path)
    connection.close()


def initialize(connection: sqlite3.Connection) -> None:
    LOGGER.info("operation=schema_initialize version=%s", SCHEMA_VERSION)
    connection.executescript("""
            CREATE TABLE IF NOT EXISTS schema_meta (
                version INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sources (
                source_id TEXT PRIMARY KEY,
                provider_id TEXT NOT NULL,
                provider_name TEXT NOT NULL,
                provider_kind TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                channel_label TEXT NOT NULL,
                channel_role TEXT NOT NULL,
                stream_kind TEXT NOT NULL,
                item_kind TEXT NOT NULL,
                entity_kind TEXT,
                item_variant TEXT,
                topics_json TEXT NOT NULL,
                language TEXT NOT NULL,
                market TEXT NOT NULL,
                adapter TEXT NOT NULL,
                url TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                interval_seconds INTEGER NOT NULL,
                config_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS source_surfaces (
                source_id TEXT NOT NULL REFERENCES sources(source_id),
                surface TEXT NOT NULL,
                PRIMARY KEY(source_id, surface)
            );

            CREATE INDEX IF NOT EXISTS idx_source_surfaces_surface
                ON source_surfaces(surface, source_id);

            CREATE TABLE IF NOT EXISTS stream_state (
                source_id TEXT PRIMARY KEY REFERENCES sources(source_id),
                next_run_at TEXT,
                last_attempt_at TEXT,
                last_success_at TEXT,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                etag TEXT,
                last_modified TEXT,
                request_identity TEXT
            );

            CREATE TABLE IF NOT EXISTS fetch_runs (
                run_kind TEXT NOT NULL CHECK (run_kind IN ('live', 'history')),
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL REFERENCES sources(source_id),
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                http_status INTEGER,
                item_count INTEGER NOT NULL DEFAULT 0,
                new_item_count INTEGER NOT NULL DEFAULT 0,
                new_version_count INTEGER NOT NULL DEFAULT 0,
                rejected_count INTEGER NOT NULL DEFAULT 0,
                warning_count INTEGER NOT NULL DEFAULT 0,
                error_type TEXT,
                error_message TEXT,
                response_etag TEXT,
                response_last_modified TEXT,
                history_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_fetch_runs_source_started
                ON fetch_runs(source_id, started_at DESC);

            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                identity_key TEXT NOT NULL UNIQUE,
                external_id TEXT,
                original_url TEXT NOT NULL,
                canonical_url TEXT NOT NULL,
                item_kind TEXT NOT NULL,
                entity_kind TEXT,
                item_variant TEXT,
                published_at TEXT,
                raw_published_at TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_items_first_seen
                ON items(first_seen_at);

            CREATE INDEX IF NOT EXISTS idx_items_published
                ON items(published_at DESC);

            CREATE TABLE IF NOT EXISTS item_url_identities (
                item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                identity_key TEXT NOT NULL,
                PRIMARY KEY(item_id, identity_key)
            );

            CREATE INDEX IF NOT EXISTS idx_item_url_identities_key
                ON item_url_identities(identity_key, item_id);

            CREATE TABLE IF NOT EXISTS observations (
                source_id TEXT NOT NULL REFERENCES sources(source_id),
                item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                upstream_id TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                position INTEGER,
                PRIMARY KEY(source_id, item_id)
            );

            CREATE INDEX IF NOT EXISTS idx_observations_item
                ON observations(item_id);

            CREATE INDEX IF NOT EXISTS idx_observations_source_last_seen
                ON observations(source_id, last_seen_at DESC);

            CREATE TABLE IF NOT EXISTS item_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                title_hash TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                UNIQUE(item_id, title_hash)
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fetch_run_id INTEGER NOT NULL UNIQUE
                    REFERENCES fetch_runs(id) ON DELETE CASCADE,
                source_id TEXT NOT NULL REFERENCES sources(source_id),
                observed_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_snapshots_source_observed
                ON snapshots(source_id, observed_at DESC);

            CREATE TABLE IF NOT EXISTS snapshot_entries (
                snapshot_id INTEGER NOT NULL
                    REFERENCES snapshots(id) ON DELETE CASCADE,
                position INTEGER,
                item_id INTEGER NOT NULL REFERENCES items(id),
                item_version_id INTEGER NOT NULL REFERENCES item_versions(id),
                original_url TEXT NOT NULL,
                canonical_url TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                PRIMARY KEY(snapshot_id, item_id)
            );

            CREATE INDEX IF NOT EXISTS idx_snapshot_entries_position
                ON snapshot_entries(snapshot_id, position);

            CREATE TABLE IF NOT EXISTS change_log (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                source_id TEXT NOT NULL REFERENCES sources(source_id),
                item_id INTEGER NOT NULL REFERENCES items(id),
                item_version_id INTEGER REFERENCES item_versions(id),
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_change_log_source_created
                ON change_log(source_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS consumer_checkpoints (
                consumer_name TEXT PRIMARY KEY,
                last_seq INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            """)
    row = connection.execute("SELECT version FROM schema_meta LIMIT 1").fetchone()
    if row is None:
        with transaction(connection):
            connection.execute(
                "INSERT INTO schema_meta(version) VALUES (?)",
                (SCHEMA_VERSION,),
            )
    elif row["version"] != SCHEMA_VERSION:
        raise RuntimeError(
            f"Unsupported schema version {row['version']}; "
            f"expected {SCHEMA_VERSION}. Recreate the database to continue."
        )


def check_schema(connection: sqlite3.Connection, *, require_existing: bool) -> None:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_meta'"
    ).fetchone()
    if exists is None:
        if require_existing:
            raise RuntimeError("Archive is not initialized; run parallax init")
        return
    row = connection.execute("SELECT version FROM schema_meta LIMIT 1").fetchone()
    if row is None or row["version"] != SCHEMA_VERSION:
        raise RuntimeError(
            "Unsupported schema version; preserve the old archive and "
            "recreate the database to continue."
        )
    for table, required in (
        ("fetch_runs", {"run_kind"}),
        ("snapshot_entries", {"original_url", "canonical_url"}),
    ):
        columns = {
            column["name"]
            for column in connection.execute(f"PRAGMA table_info({table})")
        }
        if not required <= columns:
            raise RuntimeError(
                "Archive uses an older schema layout; preserve the old archive "
                "and recreate the database to continue."
            )


@contextmanager
def transaction(connection: sqlite3.Connection) -> Generator[None, None, None]:
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
        connection.execute("COMMIT")
    except BaseException:
        _rollback_active_transaction(connection)
        raise


def _rollback_active_transaction(connection: sqlite3.Connection) -> None:
    """Roll back a still-open transaction without masking the original error."""
    try:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
    except BaseException:
        LOGGER.exception("operation=transaction_rollback_failed")
