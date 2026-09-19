from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from parallax.config import Source
from parallax.domain import (
    AnalysisItem,
    BrowseView,
    ChangeEvent,
    EntityKind,
    HeadlineCandidate,
    IngestionSummary,
    ItemVariant,
    StreamState,
    ValidatedBatch,
    is_browse_view,
    is_entity_kind,
    is_item_kind,
    is_item_variant,
    is_stream_kind,
)
from parallax.identity import identity_keys
from parallax.normalization import canonicalize_url, normalize_title_for_version

LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = 0

_LATEST_ITEM_VERSION_JOIN_SQL = """
    JOIN item_versions iv ON iv.id = (
        SELECT iv2.id
        FROM item_versions iv2
        WHERE iv2.item_id = i.id
        ORDER BY iv2.last_seen_at DESC, iv2.id DESC
        LIMIT 1
    )
"""


@dataclass(frozen=True, slots=True)
class HeadlineRow:
    source_id: str
    source_name: str
    item_id: int
    stream_kind: str
    item_kind: str
    entity_kind: str | None
    item_variant: str | None
    position: int | None
    title: str
    url: str
    canonical_url: str
    published_at: str | None
    first_seen_at: str


@dataclass(frozen=True, slots=True)
class FetchRunRow:
    id: int
    source_id: str
    status: str
    started_at: str
    finished_at: str | None
    item_count: int
    new_item_count: int
    new_version_count: int
    rejected_count: int
    http_status: int | None
    error_type: str | None
    error_message: str | None


class Storage:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self.database_path,
            timeout=10.0,
            isolation_level=None,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._connection.execute("PRAGMA busy_timeout = 10000")
        LOGGER.info(
            "operation=database_open path=%s sqlite_version=%s",
            self.database_path,
            sqlite3.sqlite_version,
        )

    def close(self) -> None:
        LOGGER.info("operation=database_close path=%s", self.database_path)
        self._connection.close()

    def initialize(self) -> None:
        LOGGER.info("operation=schema_initialize version=%s", SCHEMA_VERSION)
        self._connection.executescript("""
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
                    last_modified TEXT
                );

                CREATE TABLE IF NOT EXISTS fetch_runs (
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
                    response_last_modified TEXT
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
        row = self._connection.execute(
            "SELECT version FROM schema_meta LIMIT 1"
        ).fetchone()
        if row is None:
            with self.transaction():
                self._connection.execute(
                    "INSERT INTO schema_meta(version) VALUES (?)",
                    (SCHEMA_VERSION,),
                )
        elif row["version"] != SCHEMA_VERSION:
            raise RuntimeError(
                f"Unsupported schema version {row['version']}; "
                f"expected {SCHEMA_VERSION}. Recreate the database to continue."
            )

    def sync_sources(self, sources: Sequence[Source]) -> None:
        now = _iso_now()
        LOGGER.info("operation=source_sync count=%s", len(sources))
        with self.transaction():
            for source in sources:
                self._connection.execute(
                    """
                    INSERT INTO sources(
                        source_id, provider_id, provider_name, provider_kind,
                        channel_id, channel_label, channel_role, stream_kind,
                        item_kind, entity_kind, item_variant, topics_json,
                        language, market, adapter, url, enabled,
                        interval_seconds, config_json, updated_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    ON CONFLICT(source_id) DO UPDATE SET
                        provider_id=excluded.provider_id,
                        provider_name=excluded.provider_name,
                        provider_kind=excluded.provider_kind,
                        channel_id=excluded.channel_id,
                        channel_label=excluded.channel_label,
                        channel_role=excluded.channel_role,
                        stream_kind=excluded.stream_kind,
                        item_kind=excluded.item_kind,
                        entity_kind=excluded.entity_kind,
                        item_variant=excluded.item_variant,
                        topics_json=excluded.topics_json,
                        language=excluded.language,
                        market=excluded.market,
                        adapter=excluded.adapter,
                        url=excluded.url,
                        enabled=excluded.enabled,
                        interval_seconds=excluded.interval_seconds,
                        config_json=excluded.config_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        source.id,
                        source.provider_id,
                        source.provider_name,
                        source.provider_kind,
                        source.channel_id,
                        source.channel_label,
                        source.channel_role,
                        source.stream_kind,
                        source.item_kind,
                        source.entity_kind,
                        source.item_variant,
                        json.dumps(list(source.topics), ensure_ascii=False),
                        source.language,
                        source.market,
                        source.endpoint.adapter,
                        source.endpoint.url,
                        int(source.enabled),
                        source.interval_seconds,
                        json.dumps(source.model_dump(mode="json"), ensure_ascii=False),
                        now,
                    ),
                )
                self._connection.execute(
                    "DELETE FROM source_surfaces WHERE source_id = ?",
                    (source.id,),
                )
                for surface in sorted(source.surfaces):
                    self._connection.execute(
                        """
                        INSERT INTO source_surfaces(source_id, surface)
                        VALUES (?, ?)
                        """,
                        (source.id, surface),
                    )
                self._connection.execute(
                    """
                    INSERT INTO stream_state(source_id, consecutive_failures)
                    VALUES (?, 0)
                    ON CONFLICT(source_id) DO NOTHING
                    """,
                    (source.id,),
                )
            if sources:
                placeholders = ", ".join("?" for _ in sources)
                cursor = self._connection.execute(
                    f"""
                    UPDATE sources SET enabled = 0, updated_at = ?
                    WHERE enabled = 1
                      AND source_id NOT IN ({placeholders})
                    """,
                    (now, *(source.id for source in sources)),
                )
                if cursor.rowcount > 0:
                    LOGGER.info("operation=source_disable count=%s", cursor.rowcount)

    def get_stream_state(self, source_id: str) -> StreamState:
        row = self._connection.execute(
            "SELECT * FROM stream_state WHERE source_id = ?",
            (source_id,),
        ).fetchone()
        if row is None:
            return StreamState(source_id=source_id)
        return _stream_state_row(row)

    def last_content_change_at(self, source_id: str) -> datetime | None:
        row = self._connection.execute(
            """
            SELECT MAX(created_at) AS changed_at
            FROM change_log
            WHERE source_id = ?
            """,
            (source_id,),
        ).fetchone()
        return _parse_dt(row["changed_at"])

    def start_fetch_run(self, source_id: str) -> int:
        started_at = _iso_now()
        with self.transaction():
            cursor = self._connection.execute(
                """
                INSERT INTO fetch_runs(source_id, started_at, status)
                VALUES (?, ?, 'running')
                """,
                (source_id, started_at),
            )
            self._connection.execute(
                """
                UPDATE stream_state
                SET last_attempt_at = ?
                WHERE source_id = ?
                """,
                (started_at, source_id),
            )
        run_id = _lastrowid(cursor)
        LOGGER.info(
            "operation=fetch_run_start source_id=%s fetch_run_id=%s",
            source_id,
            run_id,
        )
        return run_id

    def record_success(
        self,
        source: Source,
        fetch_run_id: int,
        http_status: int | None,
        batch: ValidatedBatch,
        response_etag: str | None,
        response_last_modified: str | None,
        next_run_at: datetime,
        observed_at: datetime,
    ) -> IngestionSummary:
        now = _iso_now()
        observed = _iso(observed_at)
        with self.transaction():
            snapshot_cursor = self._connection.execute(
                """
                INSERT INTO snapshots(fetch_run_id, source_id, observed_at)
                VALUES (?, ?, ?)
                """,
                (fetch_run_id, source.id, observed),
            )
            snapshot_id = _lastrowid(snapshot_cursor)

            new_items, new_versions, stored = self._store_candidates(
                source,
                batch,
                seen_at=observed,
                committed_at=now,
            )
            for candidate, (item_id, version_id) in zip(
                batch.candidates,
                stored,
                strict=True,
            ):
                self._connection.execute(
                    """
                    INSERT INTO snapshot_entries(
                        snapshot_id, position, item_id, item_version_id,
                        metrics_json
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        candidate.position,
                        item_id,
                        version_id,
                        json.dumps(candidate.metrics, ensure_ascii=False),
                    ),
                )

            self._connection.execute(
                """
                UPDATE fetch_runs SET
                    finished_at = ?,
                    status = 'success',
                    http_status = ?,
                    item_count = ?,
                    new_item_count = ?,
                    new_version_count = ?,
                    rejected_count = ?,
                    warning_count = ?,
                    response_etag = ?,
                    response_last_modified = ?
                WHERE id = ?
                """,
                (
                    now,
                    http_status,
                    len(batch.candidates),
                    new_items,
                    new_versions,
                    batch.rejected_count,
                    len(batch.warnings),
                    response_etag,
                    response_last_modified,
                    fetch_run_id,
                ),
            )
            self._record_stream_success(
                source.id,
                next_run_at,
                now,
                response_etag,
                response_last_modified,
            )

        LOGGER.info(
            "operation=fetch_commit source_id=%s fetch_run_id=%s items=%s "
            "new_items=%s new_versions=%s rejected=%s",
            source.id,
            fetch_run_id,
            len(batch.candidates),
            new_items,
            new_versions,
            batch.rejected_count,
        )
        return IngestionSummary(
            source_id=source.id,
            fetch_run_id=fetch_run_id,
            status="success",
            item_count=len(batch.candidates),
            new_item_count=new_items,
            new_version_count=new_versions,
            rejected_count=batch.rejected_count,
        )

    def record_history(
        self,
        source: Source,
        fetch_run_id: int,
        batch: ValidatedBatch,
    ) -> IngestionSummary:
        """Store backfilled items without touching snapshots or scheduling.

        A history fetch observes older publications; it must not replace the
        latest snapshot or advance the source's freshness state.
        """
        now = _iso_now()
        with self.transaction():
            new_items, new_versions, _ = self._store_candidates(
                source,
                batch,
                seen_at=now,
                committed_at=now,
            )
            self._connection.execute(
                """
                UPDATE fetch_runs SET
                    finished_at = ?,
                    status = 'history',
                    item_count = ?,
                    new_item_count = ?,
                    new_version_count = ?,
                    rejected_count = ?,
                    warning_count = ?
                WHERE id = ?
                """,
                (
                    now,
                    len(batch.candidates),
                    new_items,
                    new_versions,
                    batch.rejected_count,
                    len(batch.warnings),
                    fetch_run_id,
                ),
            )

        LOGGER.info(
            "operation=fetch_history source_id=%s fetch_run_id=%s items=%s "
            "new_items=%s new_versions=%s rejected=%s",
            source.id,
            fetch_run_id,
            len(batch.candidates),
            new_items,
            new_versions,
            batch.rejected_count,
        )
        return IngestionSummary(
            source_id=source.id,
            fetch_run_id=fetch_run_id,
            status="history",
            item_count=len(batch.candidates),
            new_item_count=new_items,
            new_version_count=new_versions,
            rejected_count=batch.rejected_count,
        )

    def record_not_modified(
        self,
        source_id: str,
        fetch_run_id: int,
        response_etag: str | None,
        response_last_modified: str | None,
        next_run_at: datetime,
    ) -> IngestionSummary:
        now = _iso_now()
        with self.transaction():
            self._connection.execute(
                """
                UPDATE fetch_runs SET
                    finished_at = ?,
                    status = 'not_modified',
                    http_status = 304,
                    response_etag = ?,
                    response_last_modified = ?
                WHERE id = ?
                """,
                (now, response_etag, response_last_modified, fetch_run_id),
            )
            self._record_stream_success(
                source_id,
                next_run_at,
                now,
                response_etag,
                response_last_modified,
            )
        LOGGER.info(
            "operation=fetch_not_modified source_id=%s fetch_run_id=%s",
            source_id,
            fetch_run_id,
        )
        return IngestionSummary(
            source_id=source_id,
            fetch_run_id=fetch_run_id,
            status="not_modified",
        )

    def record_failure(
        self,
        source_id: str,
        fetch_run_id: int,
        error: Exception,
        next_run_at: datetime,
        http_status: int | None = None,
        error_message: str | None = None,
    ) -> None:
        now = _iso_now()
        error_type = type(error).__name__
        message = error_message if error_message is not None else str(error)[:2000]
        with self.transaction():
            self._connection.execute(
                """
                UPDATE fetch_runs SET
                    finished_at = ?,
                    status = 'failed',
                    http_status = ?,
                    error_type = ?,
                    error_message = ?
                WHERE id = ?
                """,
                (now, http_status, error_type, message, fetch_run_id),
            )
            self._connection.execute(
                """
                UPDATE stream_state SET
                    next_run_at = ?,
                    consecutive_failures = consecutive_failures + 1
                WHERE source_id = ?
                """,
                (_iso(next_run_at), source_id),
            )
        LOGGER.info(
            "operation=fetch_failure_recorded source_id=%s fetch_run_id=%s "
            "error_type=%s next_run_at=%s",
            source_id,
            fetch_run_id,
            error_type,
            _iso(next_run_at),
        )

    def latest_snapshot_headlines(
        self,
        limit_per_source: int | None = None,
        source_id: str | None = None,
        *,
        enabled_only: bool = True,
    ) -> list[HeadlineRow]:
        params: list[object] = []
        source_filter = ""
        if source_id:
            source_filter = "AND s.source_id = ?"
            params.append(source_id)

        enabled_filter = "AND src.enabled = 1" if enabled_only else ""

        limit_filter = ""
        if limit_per_source is not None:
            if limit_per_source < 1:
                raise ValueError("limit_per_source must be positive")
            limit_filter = "WHERE row_number <= ?"
            params.append(limit_per_source)

        rows = self._connection.execute(
            f"""
            WITH latest_snapshots AS (
                SELECT source_id, snapshot_id
                FROM (
                    SELECT
                        source_id,
                        id AS snapshot_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY source_id
                            ORDER BY observed_at DESC, id DESC
                        ) AS snapshot_rank
                    FROM snapshots
                )
                WHERE snapshot_rank = 1
            ),
            ranked AS (
                SELECT
                    s.source_id,
                    src.provider_name || ' — ' || src.channel_label
                        AS source_name,
                    src.stream_kind,
                    i.item_kind,
                    i.entity_kind,
                    i.item_variant,
                    se.position,
                    iv.title,
                    i.id AS item_id,
                    i.original_url AS url,
                    i.canonical_url,
                    i.published_at,
                    i.first_seen_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.source_id
                        ORDER BY
                            CASE WHEN se.position IS NULL THEN 1 ELSE 0 END,
                            se.position,
                            iv.id
                    ) AS row_number
                FROM snapshots s
                JOIN latest_snapshots ls
                  ON ls.source_id = s.source_id
                 AND ls.snapshot_id = s.id
                JOIN snapshot_entries se ON se.snapshot_id = s.id
                JOIN items i ON i.id = se.item_id
                JOIN item_versions iv ON iv.id = se.item_version_id
                JOIN sources src ON src.source_id = s.source_id
                WHERE 1 = 1 {source_filter} {enabled_filter}
            )
            SELECT * FROM ranked
            {limit_filter}
            ORDER BY source_name, row_number
            """,
            params,
        ).fetchall()
        return [_headline_row(row) for row in rows]

    def browse_headlines(
        self,
        *,
        view: BrowseView,
        since: datetime,
        until: datetime,
        query: str | None = None,
        source_id: str | None = None,
        limit: int,
        offset: int,
    ) -> list[HeadlineRow]:
        """Read one bounded page of durable item history.

        Items are deduplicated at item level; each item is projected with a
        representative observation (latest observation from a source exposing
        the requested surface) and its most recently observed stored title
        version. Ordered newest first.
        """
        if limit < 1:
            raise ValueError("limit must be positive")
        if offset < 0:
            raise ValueError("offset must not be negative")
        _validate_browse_bounds(view, since, until)
        filters, params = _browse_filters(
            view=view,
            since=since,
            until=until,
            query=query,
        )
        representative, representative_params = _representative_filter(
            view=view,
            source_id=source_id,
        )
        params.extend(representative_params)
        params.extend((limit, offset))
        rows = self._connection.execute(
            f"""
            SELECT
                o.source_id,
                src.provider_name || ' — ' || src.channel_label
                    AS source_name,
                i.id AS item_id,
                src.stream_kind,
                i.item_kind,
                i.entity_kind,
                i.item_variant,
                o.position,
                iv.title,
                i.original_url AS url,
                i.canonical_url,
                i.published_at,
                i.first_seen_at
            FROM items i
            {_LATEST_ITEM_VERSION_JOIN_SQL}
            JOIN observations o ON o.item_id = i.id
            JOIN sources src ON src.source_id = o.source_id
            WHERE {" AND ".join(filters)}
              AND {representative}
            ORDER BY i.first_seen_at DESC, i.id DESC
            LIMIT ? OFFSET ?
            """,
            params,
        ).fetchall()
        return [_headline_row(row) for row in rows]

    def count_browse_headlines(
        self,
        *,
        view: BrowseView,
        since: datetime,
        until: datetime,
        query: str | None = None,
        source_id: str | None = None,
    ) -> int:
        """Count durable items matching the same predicates as browsing."""
        _validate_browse_bounds(view, since, until)
        filters, params = _browse_filters(
            view=view,
            since=since,
            until=until,
            query=query,
        )
        membership, membership_params = _membership_filter(
            view=view,
            source_id=source_id,
        )
        filters.append(membership)
        params.extend(membership_params)
        version_join = _LATEST_ITEM_VERSION_JOIN_SQL if query else ""
        row = self._connection.execute(
            f"""
            SELECT COUNT(*)
            FROM items i
            {version_join}
            WHERE {" AND ".join(filters)}
            """,
            params,
        ).fetchone()
        if row is None:
            raise RuntimeError("browse count query returned no row")
        return int(row[0])

    def recent_fetch_runs(self, limit: int = 20) -> list[FetchRunRow]:
        rows = self._connection.execute(
            """
            SELECT * FROM fetch_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [self._fetch_run_row(row) for row in rows]

    def latest_fetch_runs(self, enabled_only: bool = True) -> list[FetchRunRow]:
        enabled_filter = "AND src.enabled = 1" if enabled_only else ""
        rows = self._connection.execute(f"""
            WITH latest AS (
                SELECT source_id, MAX(id) AS run_id
                FROM fetch_runs
                GROUP BY source_id
            )
            SELECT fr.*
            FROM fetch_runs fr
            JOIN latest l ON l.run_id = fr.id
            JOIN sources src ON src.source_id = fr.source_id
            WHERE 1 = 1 {enabled_filter}
            ORDER BY fr.source_id
            """).fetchall()
        return [self._fetch_run_row(row) for row in rows]

    def stream_states(self, enabled_only: bool = True) -> list[StreamState]:
        enabled_filter = "WHERE src.enabled = 1" if enabled_only else ""
        rows = self._connection.execute(f"""
            SELECT st.*
            FROM stream_state st
            JOIN sources src ON src.source_id = st.source_id
            {enabled_filter}
            ORDER BY st.source_id
            """).fetchall()
        return [_stream_state_row(row) for row in rows]

    @staticmethod
    def _fetch_run_row(row: sqlite3.Row) -> FetchRunRow:
        return FetchRunRow(
            id=row["id"],
            source_id=row["source_id"],
            status=row["status"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            item_count=row["item_count"],
            new_item_count=row["new_item_count"],
            new_version_count=row["new_version_count"],
            rejected_count=row["rejected_count"],
            http_status=row["http_status"],
            error_type=row["error_type"],
            error_message=row["error_message"],
        )

    def changes_after(self, seq: int, limit: int = 100) -> list[ChangeEvent]:
        rows = self._connection.execute(
            """
            SELECT * FROM change_log
            WHERE seq > ?
            ORDER BY seq
            LIMIT ?
            """,
            (seq, limit),
        ).fetchall()
        return [
            ChangeEvent(
                seq=row["seq"],
                event_type=row["event_type"],
                source_id=row["source_id"],
                item_id=row["item_id"],
                item_version_id=row["item_version_id"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def get_consumer_checkpoint(self, consumer_name: str) -> int:
        row = self._connection.execute(
            """
            SELECT last_seq FROM consumer_checkpoints
            WHERE consumer_name = ?
            """,
            (consumer_name,),
        ).fetchone()
        return 0 if row is None else int(row["last_seq"])

    def set_consumer_checkpoint(self, consumer_name: str, last_seq: int) -> None:
        now = _iso_now()
        with self.transaction():
            self._connection.execute(
                """
                INSERT INTO consumer_checkpoints(consumer_name, last_seq, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(consumer_name) DO UPDATE SET
                    last_seq=excluded.last_seq,
                    updated_at=excluded.updated_at
                """,
                (consumer_name, last_seq, now),
            )

    def hydrate_analysis_items(
        self,
        events: Sequence[ChangeEvent],
    ) -> list[AnalysisItem]:
        """Hydrate committed change events into typed analysis inputs.

        Events are returned in the supplied order. A missing item, missing or
        mismatched item version, missing observation, unknown classification,
        or unreadable timestamp fails clearly so a consumer never silently
        skips committed evidence.
        """
        if not events:
            return []
        placeholders = ", ".join("?" for _ in events)
        rows = self._connection.execute(
            f"""
            SELECT
                cl.seq,
                cl.event_type,
                cl.source_id AS event_source_id,
                cl.item_id,
                cl.item_version_id,
                iv.title,
                i.original_url,
                i.canonical_url,
                i.published_at,
                i.first_seen_at,
                i.item_kind,
                i.entity_kind,
                i.item_variant,
                src.language AS source_language,
                src.enabled AS source_enabled,
                src.stream_kind,
                o.source_id AS observation_source_id
            FROM change_log cl
            LEFT JOIN items i ON i.id = cl.item_id
            LEFT JOIN item_versions iv
              ON iv.id = cl.item_version_id AND iv.item_id = cl.item_id
            LEFT JOIN observations o
              ON o.item_id = cl.item_id AND o.source_id = cl.source_id
            LEFT JOIN sources src ON src.source_id = cl.source_id
            WHERE cl.seq IN ({placeholders})
            ORDER BY cl.seq
            """,
            tuple(event.seq for event in events),
        ).fetchall()
        by_seq = {int(row["seq"]): row for row in rows}
        return [_analysis_item(event, by_seq.get(event.seq)) for event in events]

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self._connection.execute("ROLLBACK")
            raise
        else:
            self._connection.execute("COMMIT")

    def _record_stream_success(
        self,
        source_id: str,
        next_run_at: datetime,
        now: str,
        response_etag: str | None,
        response_last_modified: str | None,
    ) -> None:
        self._connection.execute(
            """
            UPDATE stream_state SET
                next_run_at = ?,
                last_success_at = ?,
                consecutive_failures = 0,
                etag = COALESCE(?, etag),
                last_modified = COALESCE(?, last_modified)
            WHERE source_id = ?
            """,
            (
                _iso(next_run_at),
                now,
                response_etag,
                response_last_modified,
                source_id,
            ),
        )

    def _store_candidates(
        self,
        source: Source,
        batch: ValidatedBatch,
        *,
        seen_at: str,
        committed_at: str,
    ) -> tuple[int, int, tuple[tuple[int, int], ...]]:
        new_items = 0
        new_versions = 0
        stored: list[tuple[int, int]] = []
        for candidate in batch.candidates:
            item_id, item_is_new = self._upsert_item(source, candidate, seen_at)
            observation_is_new = self._upsert_observation(
                source, item_id, candidate, seen_at
            )
            version_id, version_is_new = self._upsert_version(
                item_id,
                candidate.title.strip(),
                seen_at,
            )
            if item_is_new:
                new_items += 1
                self._append_change(
                    "item_created",
                    source.id,
                    item_id,
                    version_id,
                    committed_at,
                )
            else:
                if observation_is_new:
                    self._append_change(
                        "item_observed",
                        source.id,
                        item_id,
                        version_id,
                        committed_at,
                    )
                if version_is_new:
                    self._append_change(
                        "headline_version_created",
                        source.id,
                        item_id,
                        version_id,
                        committed_at,
                    )
            if version_is_new:
                new_versions += 1
            stored.append((item_id, version_id))
        return new_items, new_versions, tuple(stored)

    def _upsert_item(
        self,
        source: Source,
        candidate: HeadlineCandidate,
        seen_at: str,
    ) -> tuple[int, bool]:
        primary_key, url_key = identity_keys(
            candidate,
            provider_id=source.provider_id,
        )
        external_id = candidate.external_id.strip() if candidate.external_id else None
        if not external_id:
            external_id = None
        canonical_url = canonicalize_url(candidate.url)
        existing = self._connection.execute(
            "SELECT id FROM items WHERE identity_key = ?",
            (primary_key,),
        ).fetchone()
        if existing is None and external_id is not None:
            existing = self._connection.execute(
                """
                SELECT id FROM items
                WHERE identity_key = ? AND external_id IS NULL
                """,
                (url_key,),
            ).fetchone()
            if existing is not None:
                self._connection.execute(
                    """
                    UPDATE items SET identity_key = ?, external_id = ?
                    WHERE id = ?
                    """,
                    (primary_key, external_id, existing["id"]),
                )
        elif existing is None:
            matches = self._connection.execute(
                """
                SELECT item_id FROM item_url_identities
                WHERE identity_key = ?
                ORDER BY item_id
                LIMIT 2
                """,
                (url_key,),
            ).fetchall()
            if len(matches) == 1:
                existing = self._connection.execute(
                    "SELECT id FROM items WHERE id = ?",
                    (matches[0]["item_id"],),
                ).fetchone()
        published = _iso(candidate.published_at) if candidate.published_at else None

        if existing is None:
            cursor = self._connection.execute(
                """
                INSERT INTO items(
                    identity_key, external_id, original_url, canonical_url,
                    item_kind, entity_kind, item_variant, published_at,
                    raw_published_at, first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    primary_key,
                    external_id,
                    candidate.url.strip(),
                    canonical_url,
                    source.item_kind,
                    source.entity_kind,
                    source.item_variant,
                    published,
                    candidate.raw_published_at,
                    seen_at,
                    seen_at,
                ),
            )
            item_id = _lastrowid(cursor)
            self._record_url_identity(item_id, url_key)
            return item_id, True

        self._connection.execute(
            """
            UPDATE items SET
                external_id = COALESCE(external_id, ?),
                original_url = CASE
                    WHEN last_seen_at <= ? THEN ?
                    ELSE original_url
                END,
                canonical_url = CASE
                    WHEN last_seen_at <= ? THEN ?
                    ELSE canonical_url
                END,
                published_at = COALESCE(published_at, ?),
                raw_published_at = COALESCE(raw_published_at, ?),
                first_seen_at = MIN(first_seen_at, ?),
                last_seen_at = MAX(last_seen_at, ?)
            WHERE id = ?
            """,
            (
                external_id,
                seen_at,
                candidate.url.strip(),
                seen_at,
                canonical_url,
                published,
                candidate.raw_published_at,
                seen_at,
                seen_at,
                existing["id"],
            ),
        )
        item_id = int(existing["id"])
        self._record_url_identity(item_id, url_key)
        return item_id, False

    def _record_url_identity(self, item_id: int, identity_key: str) -> None:
        self._connection.execute(
            """
            INSERT INTO item_url_identities(item_id, identity_key)
            VALUES (?, ?)
            ON CONFLICT(item_id, identity_key) DO NOTHING
            """,
            (item_id, identity_key),
        )

    def _upsert_observation(
        self,
        source: Source,
        item_id: int,
        candidate: HeadlineCandidate,
        seen_at: str,
    ) -> bool:
        existing = self._connection.execute(
            """
            SELECT 1 FROM observations
            WHERE source_id = ? AND item_id = ?
            """,
            (source.id, item_id),
        ).fetchone()
        if existing is None:
            self._connection.execute(
                """
                INSERT INTO observations(
                    source_id, item_id, upstream_id, first_seen_at,
                    last_seen_at, position
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source.id,
                    item_id,
                    candidate.external_id,
                    seen_at,
                    seen_at,
                    candidate.position,
                ),
            )
            return True
        self._connection.execute(
            """
            UPDATE observations SET
                upstream_id = COALESCE(upstream_id, ?),
                first_seen_at = MIN(first_seen_at, ?),
                position = CASE
                    WHEN last_seen_at <= ? THEN ?
                    ELSE position
                END,
                last_seen_at = MAX(last_seen_at, ?)
            WHERE source_id = ? AND item_id = ?
            """,
            (
                candidate.external_id,
                seen_at,
                seen_at,
                candidate.position,
                seen_at,
                source.id,
                item_id,
            ),
        )
        return False

    def _upsert_version(
        self,
        item_id: int,
        title: str,
        seen_at: str,
    ) -> tuple[int, bool]:
        title_hash = hashlib.sha256(
            normalize_title_for_version(title).encode("utf-8")
        ).hexdigest()
        existing = self._connection.execute(
            """
            SELECT id FROM item_versions
            WHERE item_id = ? AND title_hash = ?
            """,
            (item_id, title_hash),
        ).fetchone()
        version_id = int(existing["id"]) if existing is not None else None
        if version_id is not None:
            self._connection.execute(
                """
                UPDATE item_versions SET
                    first_seen_at = MIN(first_seen_at, ?),
                    last_seen_at = MAX(last_seen_at, ?)
                WHERE id = ?
                """,
                (seen_at, seen_at, version_id),
            )
            return version_id, False

        cursor = self._connection.execute(
            """
            INSERT INTO item_versions(
                item_id, title, title_hash, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (item_id, title, title_hash, seen_at, seen_at),
        )
        return _lastrowid(cursor), True

    def _append_change(
        self,
        event_type: str,
        source_id: str,
        item_id: int,
        version_id: int,
        now: str,
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO change_log(
                event_type, source_id, item_id, item_version_id, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (event_type, source_id, item_id, version_id, now),
        )


def _headline_row(row: sqlite3.Row) -> HeadlineRow:
    return HeadlineRow(
        source_id=row["source_id"],
        source_name=row["source_name"],
        item_id=row["item_id"],
        stream_kind=row["stream_kind"],
        item_kind=row["item_kind"],
        entity_kind=row["entity_kind"],
        item_variant=row["item_variant"],
        position=row["position"],
        title=row["title"],
        url=row["url"],
        canonical_url=row["canonical_url"],
        published_at=row["published_at"],
        first_seen_at=row["first_seen_at"],
    )


def _iso_now() -> str:
    return _iso(datetime.now(UTC))


def _stream_state_row(row: sqlite3.Row) -> StreamState:
    return StreamState(
        source_id=row["source_id"],
        next_run_at=_parse_dt(row["next_run_at"]),
        last_attempt_at=_parse_dt(row["last_attempt_at"]),
        last_success_at=_parse_dt(row["last_success_at"]),
        consecutive_failures=row["consecutive_failures"],
        etag=row["etag"],
        last_modified=row["last_modified"],
    )


def _analysis_item(
    event: ChangeEvent,
    row: sqlite3.Row | None,
) -> AnalysisItem:
    if row is None:
        raise ValueError(f"change event {event.seq} is missing from the change log")
    if row["observation_source_id"] is None:
        raise ValueError(
            f"change event {event.seq} references missing observation "
            f"{event.source_id!r} of item {event.item_id}"
        )
    version_id = row["item_version_id"]
    if version_id is None or row["title"] is None:
        raise ValueError(
            f"change event {event.seq} references missing item version "
            f"{event.item_version_id}"
        )
    source_language = row["source_language"]
    if source_language is None:
        raise ValueError(
            f"change event {event.seq} references missing source "
            f"{row['event_source_id']!r}"
        )
    stream_kind = str(row["stream_kind"])
    if not is_stream_kind(stream_kind):
        raise ValueError(
            f"item {event.item_id} has unsupported stream kind {stream_kind!r}"
        )
    item_kind = str(row["item_kind"])
    if not is_item_kind(item_kind):
        raise ValueError(
            f"item {event.item_id} has unsupported item kind {item_kind!r}"
        )
    entity_kind: EntityKind | None = None
    if row["entity_kind"] is not None:
        raw_entity_kind = str(row["entity_kind"])
        if not is_entity_kind(raw_entity_kind):
            raise ValueError(
                f"item {event.item_id} has unsupported entity kind "
                f"{raw_entity_kind!r}"
            )
        entity_kind = raw_entity_kind
    item_variant: ItemVariant | None = None
    if row["item_variant"] is not None:
        raw_item_variant = str(row["item_variant"])
        if not is_item_variant(raw_item_variant):
            raise ValueError(
                f"item {event.item_id} has unsupported item variant "
                f"{raw_item_variant!r}"
            )
        item_variant = raw_item_variant
    first_seen_at = _parse_dt(row["first_seen_at"])
    if first_seen_at is None:
        raise ValueError(f"item {event.item_id} has no first_seen_at")
    return AnalysisItem(
        change_seq=event.seq,
        event_type=event.event_type,
        item_id=int(row["item_id"]),
        item_version_id=int(version_id),
        title=str(row["title"]),
        source_id=str(row["event_source_id"]),
        source_language=str(source_language),
        source_enabled=bool(row["source_enabled"]),
        stream_kind=stream_kind,
        item_kind=item_kind,
        entity_kind=entity_kind,
        item_variant=item_variant,
        original_url=str(row["original_url"]),
        canonical_url=str(row["canonical_url"]),
        published_at=_parse_dt(row["published_at"]),
        first_seen_at=first_seen_at,
    )


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds")


def _validate_browse_bounds(
    view: BrowseView,
    since: datetime,
    until: datetime,
) -> None:
    if not is_browse_view(view):
        raise ValueError(f"unknown browse view: {view}")
    if since.tzinfo is None or until.tzinfo is None:
        raise ValueError("browse timestamps must be timezone-aware")
    if since > until:
        raise ValueError("browse start must not be after its end")


def _browse_filters(
    *,
    view: BrowseView,
    since: datetime,
    until: datetime,
    query: str | None,
) -> tuple[list[str], list[object]]:
    filters = [
        "i.first_seen_at >= ?",
        "i.first_seen_at <= ?",
    ]
    params: list[object] = [_iso(since), _iso(until)]
    if query:
        filters.append("iv.title LIKE ? ESCAPE '\\'")
        params.append(f"%{_escape_like(query)}%")
    return filters, params


def _membership_filter(
    *,
    view: BrowseView,
    source_id: str | None,
) -> tuple[str, list[object]]:
    """Item-level membership in one browse view, independent of enumeration."""
    surface_join = ""
    surface_filter = ""
    params: list[object] = []
    if view != "all":
        surface_join = "JOIN source_surfaces ss ON ss.source_id = o.source_id"
        surface_filter = "AND ss.surface = ?"
        params.append(view)
    source_filter = ""
    if source_id:
        source_filter = "AND o.source_id = ?"
        params.append(source_id)
    return (
        "EXISTS ("
        "SELECT 1 FROM observations o "
        "JOIN sources s ON s.source_id = o.source_id "
        f"{surface_join} "
        "WHERE o.item_id = i.id AND s.enabled = 1 "
        f"{surface_filter} "
        f"{source_filter}"
        ")",
        params,
    )


def _representative_filter(
    *,
    view: BrowseView,
    source_id: str | None,
) -> tuple[str, list[object]]:
    """Choose one deterministic observation row per item for display."""
    surface_join = ""
    surface_filter = ""
    params: list[object] = []
    if view != "all":
        surface_join = "JOIN source_surfaces ss3 ON ss3.source_id = o3.source_id"
        surface_filter = "AND ss3.surface = ?"
        params.append(view)
    source_filter = ""
    if source_id:
        source_filter = "AND o3.source_id = ?"
        params.append(source_id)
    return (
        "o.source_id = ("
        "SELECT o3.source_id FROM observations o3 "
        "JOIN sources s3 ON s3.source_id = o3.source_id "
        f"{surface_join} "
        "WHERE o3.item_id = i.id AND s3.enabled = 1 "
        f"{surface_filter} "
        f"{source_filter} "
        "ORDER BY o3.last_seen_at DESC, o3.source_id LIMIT 1"
        ")",
        params,
    )


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _lastrowid(cursor: sqlite3.Cursor) -> int:
    rowid = cursor.lastrowid
    if rowid is None:
        raise RuntimeError("INSERT did not produce a rowid")
    return rowid
