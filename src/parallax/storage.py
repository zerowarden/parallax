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

from parallax.config import SourceConfig
from parallax.domain import (
    ChangeEvent,
    HeadlineCandidate,
    IngestionSummary,
    StreamState,
    ValidatedBatch,
)
from parallax.identity import canonicalize_url, identity_key

LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class HeadlineRow:
    source_id: str
    source_name: str
    position: int | None
    title: str
    url: str
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
                    name TEXT NOT NULL,
                    region TEXT NOT NULL,
                    language TEXT NOT NULL,
                    adapter TEXT NOT NULL,
                    url TEXT NOT NULL,
                    stream_kind TEXT NOT NULL,
                    item_kind TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    schedule_seconds INTEGER NOT NULL,
                    config_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

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
                    source_id TEXT NOT NULL REFERENCES sources(source_id),
                    identity_key TEXT NOT NULL,
                    external_id TEXT,
                    original_url TEXT NOT NULL,
                    canonical_url TEXT NOT NULL,
                    item_kind TEXT NOT NULL,
                    published_at TEXT,
                    raw_published_at TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    UNIQUE(source_id, identity_key)
                );

                CREATE INDEX IF NOT EXISTS idx_items_source_published
                    ON items(source_id, published_at DESC);

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
                f"expected {SCHEMA_VERSION}"
            )

    def sync_sources(self, sources: Sequence[SourceConfig]) -> None:
        now = _iso_now()
        LOGGER.info("operation=source_sync count=%s", len(sources))
        with self.transaction():
            for source in sources:
                self._connection.execute(
                    """
                    INSERT INTO sources(
                        source_id, name, region, language, adapter, url,
                        stream_kind, item_kind, enabled, schedule_seconds,
                        config_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_id) DO UPDATE SET
                        name=excluded.name,
                        region=excluded.region,
                        language=excluded.language,
                        adapter=excluded.adapter,
                        url=excluded.url,
                        stream_kind=excluded.stream_kind,
                        item_kind=excluded.item_kind,
                        enabled=excluded.enabled,
                        schedule_seconds=excluded.schedule_seconds,
                        config_json=excluded.config_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        source.id,
                        source.name,
                        source.region,
                        source.language,
                        source.adapter,
                        source.url,
                        source.stream_kind,
                        source.item_kind,
                        int(source.enabled),
                        source.schedule_seconds,
                        json.dumps(source.model_dump(mode="json"), ensure_ascii=False),
                        now,
                    ),
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
        source: SourceConfig,
        fetch_run_id: int,
        http_status: int | None,
        batch: ValidatedBatch,
        response_etag: str | None,
        response_last_modified: str | None,
        next_run_at: datetime,
    ) -> IngestionSummary:
        now = _iso_now()
        with self.transaction():
            snapshot_cursor = self._connection.execute(
                """
                INSERT INTO snapshots(fetch_run_id, source_id, observed_at)
                VALUES (?, ?, ?)
                """,
                (fetch_run_id, source.id, now),
            )
            snapshot_id = _lastrowid(snapshot_cursor)

            new_items, new_versions, stored = self._store_candidates(source, batch, now)
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
        source: SourceConfig,
        fetch_run_id: int,
        batch: ValidatedBatch,
    ) -> IngestionSummary:
        """Store backfilled items without touching snapshots or scheduling.

        A history fetch observes older publications; it must not replace the
        latest snapshot or advance the source's freshness state.
        """
        now = _iso_now()
        with self.transaction():
            new_items, new_versions, _ = self._store_candidates(source, batch, now)
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

    def latest_headlines(
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
                SELECT source_id, MAX(id) AS snapshot_id
                FROM snapshots
                GROUP BY source_id
            ),
            ranked AS (
                SELECT
                    s.source_id,
                    src.name AS source_name,
                    se.position,
                    iv.title,
                    i.original_url AS url,
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
        return [
            HeadlineRow(
                source_id=row["source_id"],
                source_name=row["source_name"],
                position=row["position"],
                title=row["title"],
                url=row["url"],
                published_at=row["published_at"],
                first_seen_at=row["first_seen_at"],
            )
            for row in rows
        ]

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
        source: SourceConfig,
        batch: ValidatedBatch,
        now: str,
    ) -> tuple[int, int, tuple[tuple[int, int], ...]]:
        new_items = 0
        new_versions = 0
        stored: list[tuple[int, int]] = []
        for candidate in batch.candidates:
            item_id, item_is_new = self._upsert_item(source, candidate, now)
            version_id, version_is_new = self._upsert_version(
                item_id,
                candidate.title.strip(),
                now,
            )
            if item_is_new:
                new_items += 1
                self._append_change(
                    "item_created",
                    source.id,
                    item_id,
                    version_id,
                    now,
                )
            if version_is_new:
                new_versions += 1
                if not item_is_new:
                    self._append_change(
                        "headline_version_created",
                        source.id,
                        item_id,
                        version_id,
                        now,
                    )
            stored.append((item_id, version_id))
        return new_items, new_versions, tuple(stored)

    def _upsert_item(
        self,
        source: SourceConfig,
        candidate: HeadlineCandidate,
        now: str,
    ) -> tuple[int, bool]:
        key = identity_key(candidate)
        canonical_url = canonicalize_url(candidate.url)
        existing = self._connection.execute(
            """
            SELECT id, published_at, raw_published_at
            FROM items
            WHERE source_id = ? AND identity_key = ?
            """,
            (source.id, key),
        ).fetchone()
        published = _iso(candidate.published_at) if candidate.published_at else None

        if existing is None:
            cursor = self._connection.execute(
                """
                INSERT INTO items(
                    source_id, identity_key, external_id, original_url,
                    canonical_url, item_kind, published_at, raw_published_at,
                    first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source.id,
                    key,
                    candidate.external_id,
                    candidate.url.strip(),
                    canonical_url,
                    source.item_kind,
                    published,
                    candidate.raw_published_at,
                    now,
                    now,
                ),
            )
            return _lastrowid(cursor), True

        self._connection.execute(
            """
            UPDATE items SET
                original_url = ?,
                canonical_url = ?,
                published_at = COALESCE(published_at, ?),
                raw_published_at = COALESCE(raw_published_at, ?),
                last_seen_at = ?
            WHERE id = ?
            """,
            (
                candidate.url.strip(),
                canonical_url,
                published,
                candidate.raw_published_at,
                now,
                existing["id"],
            ),
        )
        return int(existing["id"]), False

    def _upsert_version(
        self,
        item_id: int,
        title: str,
        now: str,
    ) -> tuple[int, bool]:
        title_hash = hashlib.sha256(title.encode("utf-8")).hexdigest()
        existing = self._connection.execute(
            """
            SELECT id FROM item_versions
            WHERE item_id = ? AND title_hash = ?
            """,
            (item_id, title_hash),
        ).fetchone()
        if existing is not None:
            self._connection.execute(
                "UPDATE item_versions SET last_seen_at = ? WHERE id = ?",
                (now, existing["id"]),
            )
            return int(existing["id"]), False

        cursor = self._connection.execute(
            """
            INSERT INTO item_versions(
                item_id, title, title_hash, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (item_id, title, title_hash, now, now),
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


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds")


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
