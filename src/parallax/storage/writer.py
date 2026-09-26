"""Atomic ingestion writes.

Each public write owns one transaction on the supplied connection. Identity,
items, observations, versions, snapshots, events and successful run/source state
commit together. Internal helpers never open a connection or commit independently.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime

from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HistoryOutcome,
    IngestionSummary,
    ObservedBatch,
    RunKind,
    ValidatedBatch,
)
from parallax.normalization import canonicalize_url, normalize_title_for_version
from parallax.storage import foundation, identity, queries
from parallax.storage.encoding import (
    encode_datetime,
    encode_metrics,
    inserted_row_id,
    timestamp_now,
)

LOGGER = logging.getLogger(__name__)

# Upper bound on one persisted ``fetch_runs.error_message`` value. Storage owns
# the column bound so logging and persistence do not drift apart.
ERROR_MESSAGE_LIMIT = 2000


@dataclass(frozen=True, slots=True)
class _ResolvedObservation:
    """One accepted candidate after persistent identity resolution.

    Several accepted candidates can resolve to the same persistent item, for
    example when one carries an authoritative external ID and another arrives
    through a previously recorded URL alias. The first candidate in batch order
    is the representative: it supplies the snapshot position, item version, and
    metrics. Later candidates are collapsed during storage and contribute no
    versions, observations, or snapshot entries.
    """

    candidate: HeadlineCandidate
    item_id: int
    version_id: int
    metrics_json: str


def recover_abandoned_runs(connection: sqlite3.Connection) -> None:
    """Finalize abandoned runs; caller must hold the exclusive collector lock."""
    now = timestamp_now()
    with foundation.transaction(connection):
        connection.execute(
            """
            UPDATE stream_state SET next_run_at = ?,
                consecutive_failures = consecutive_failures + 1
            WHERE source_id IN (
                SELECT source_id FROM fetch_runs
                WHERE status = 'running' AND run_kind = 'live'
            )
            """,
            (now,),
        )
        cursor = connection.execute(
            """
            UPDATE fetch_runs SET status = 'failed', finished_at = ?,
                error_type = 'AbandonedRun',
                error_message = 'Previous collector exited before run completion'
            WHERE status = 'running'
            """,
            (now,),
        )
    if cursor.rowcount:
        LOGGER.warning("operation=fetch_runs_recovered count=%s", cursor.rowcount)


def start_fetch_run(
    connection: sqlite3.Connection, source_id: str, run_kind: RunKind
) -> int:
    started_at = timestamp_now()
    with foundation.transaction(connection):
        cursor = connection.execute(
            """
            INSERT INTO fetch_runs(source_id, run_kind, started_at, status)
            VALUES (?, ?, ?, 'running')
            """,
            (source_id, run_kind, started_at),
        )
        if run_kind == "live":
            connection.execute(
                """
                UPDATE stream_state
                SET last_attempt_at = ?
                WHERE source_id = ?
                """,
                (started_at, source_id),
            )
    run_id = inserted_row_id(cursor)
    LOGGER.info(
        "operation=fetch_run_start source_id=%s fetch_run_id=%s run_kind=%s",
        source_id,
        run_id,
        run_kind,
    )
    return run_id


def record_success(
    connection: sqlite3.Connection,
    source: Source,
    fetch_run_id: int,
    http_status: int | None,
    batch: ValidatedBatch,
    response_etag: str | None,
    response_last_modified: str | None,
    next_run_at: datetime,
    observed_at: datetime,
    request_identity: str | None = None,
) -> IngestionSummary:
    now = timestamp_now()
    observed = encode_datetime(observed_at)
    with foundation.transaction(connection):
        _require_running_run(connection, source.id, fetch_run_id, "live")
        snapshot_cursor = connection.execute(
            """
            INSERT INTO snapshots(fetch_run_id, source_id, observed_at)
            VALUES (?, ?, ?)
            """,
            (fetch_run_id, source.id, observed),
        )
        snapshot_id = inserted_row_id(snapshot_cursor)

        new_items, new_versions, resolved = _store_candidates(
            connection,
            source,
            ObservedBatch.from_validated(batch, observed_at),
            committed_at=now,
        )
        for observation in resolved:
            connection.execute(
                """
                INSERT INTO snapshot_entries(
                    snapshot_id, position, item_id, item_version_id,
                    original_url, canonical_url, metrics_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    observation.candidate.position,
                    observation.item_id,
                    observation.version_id,
                    observation.candidate.url,
                    canonicalize_url(observation.candidate.url),
                    observation.metrics_json,
                ),
            )

        connection.execute(
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
                len(resolved),
                new_items,
                new_versions,
                batch.rejected_count,
                len(batch.warnings),
                response_etag,
                response_last_modified,
                fetch_run_id,
            ),
        )
        _record_stream_success(
            connection,
            source.id,
            next_run_at,
            now,
            response_etag,
            response_last_modified,
            request_identity,
        )

    LOGGER.info(
        "operation=fetch_commit source_id=%s fetch_run_id=%s items=%s "
        "new_items=%s new_versions=%s rejected=%s",
        source.id,
        fetch_run_id,
        len(resolved),
        new_items,
        new_versions,
        batch.rejected_count,
    )
    return IngestionSummary(
        source_id=source.id,
        fetch_run_id=fetch_run_id,
        status="success",
        item_count=len(resolved),
        new_item_count=new_items,
        new_version_count=new_versions,
        rejected_count=batch.rejected_count,
    )


def record_history(
    connection: sqlite3.Connection,
    source: Source,
    fetch_run_id: int,
    batch: ObservedBatch,
    outcome: HistoryOutcome | None = None,
) -> IngestionSummary:
    """Store backfilled items without touching snapshots or scheduling.

    A history fetch observes older publications; it must not replace the
    latest snapshot or advance the source's freshness state.
    """
    now = timestamp_now()
    status = (
        "unsupported"
        if outcome is not None and outcome.status == "unsupported"
        else "history"
    )
    history_json = (
        json.dumps(asdict(outcome), default=encode_datetime)
        if outcome is not None
        else None
    )
    with foundation.transaction(connection):
        _require_running_run(connection, source.id, fetch_run_id, "history")
        new_items, new_versions, resolved = _store_candidates(
            connection,
            source,
            batch,
            committed_at=now,
        )
        connection.execute(
            """
            UPDATE fetch_runs SET
                finished_at = ?,
                status = ?,
                item_count = ?,
                new_item_count = ?,
                new_version_count = ?,
                rejected_count = ?,
                warning_count = ?,
                history_json = ?
            WHERE id = ?
            """,
            (
                now,
                status,
                len(resolved),
                new_items,
                new_versions,
                batch.rejected_count,
                len(batch.warnings),
                history_json,
                fetch_run_id,
            ),
        )

    LOGGER.info(
        "operation=fetch_history source_id=%s fetch_run_id=%s items=%s "
        "new_items=%s new_versions=%s rejected=%s",
        source.id,
        fetch_run_id,
        len(resolved),
        new_items,
        new_versions,
        batch.rejected_count,
    )
    return IngestionSummary(
        source_id=source.id,
        fetch_run_id=fetch_run_id,
        status=status,
        item_count=len(resolved),
        new_item_count=new_items,
        new_version_count=new_versions,
        rejected_count=batch.rejected_count,
        history=outcome,
    )


def record_not_modified(
    connection: sqlite3.Connection,
    source_id: str,
    fetch_run_id: int,
    response_etag: str | None,
    response_last_modified: str | None,
    next_run_at: datetime,
    request_identity: str | None = None,
) -> IngestionSummary:
    now = timestamp_now()
    with foundation.transaction(connection):
        _require_running_run(connection, source_id, fetch_run_id, "live")
        state = queries.get_stream_state(connection, source_id)
        if state.request_identity != request_identity:
            raise ValueError("304 response does not match the stored request identity")
        connection.execute(
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
        _record_stream_success(
            connection,
            source_id,
            next_run_at,
            now,
            response_etag if response_etag is not None else state.etag,
            (
                response_last_modified
                if response_last_modified is not None
                else state.last_modified
            ),
            request_identity,
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
    connection: sqlite3.Connection,
    source_id: str,
    fetch_run_id: int,
    error: BaseException,
    next_run_at: datetime | None,
    http_status: int | None = None,
    error_message: str | None = None,
) -> None:
    now = timestamp_now()
    error_type = type(error).__name__
    message = (error_message if error_message is not None else str(error))[
        :ERROR_MESSAGE_LIMIT
    ]
    with foundation.transaction(connection):
        cursor = connection.execute(
            """
            UPDATE fetch_runs SET
                finished_at = ?,
                status = 'failed',
                http_status = ?,
                error_type = ?,
                error_message = ?
            WHERE id = ? AND source_id = ? AND status = 'running'
            RETURNING run_kind
            """,
            (now, http_status, error_type, message, fetch_run_id, source_id),
        )
        run = cursor.fetchone()
        cursor.close()
        if run is None:
            return
        if run["run_kind"] == "live":
            if next_run_at is None:
                raise ValueError("live failure requires a retry time")
            connection.execute(
                """
                UPDATE stream_state SET
                    next_run_at = ?,
                    consecutive_failures = consecutive_failures + 1
                WHERE source_id = ?
                """,
                (encode_datetime(next_run_at), source_id),
            )
    LOGGER.info(
        "operation=fetch_failure_recorded source_id=%s fetch_run_id=%s "
        "error_type=%s next_run_at=%s",
        source_id,
        fetch_run_id,
        error_type,
        encode_datetime(next_run_at) if next_run_at is not None else None,
    )


def _require_running_run(
    connection: sqlite3.Connection,
    source_id: str,
    fetch_run_id: int,
    run_kind: RunKind,
) -> None:
    run = connection.execute(
        """
        SELECT 1 FROM fetch_runs
        WHERE id = ? AND source_id = ? AND run_kind = ? AND status = 'running'
        """,
        (fetch_run_id, source_id, run_kind),
    ).fetchone()
    if run is None:
        raise ValueError(f"expected a running {run_kind} run for source {source_id}")


def _record_stream_success(
    connection: sqlite3.Connection,
    source_id: str,
    next_run_at: datetime,
    now: str,
    response_etag: str | None,
    response_last_modified: str | None,
    request_identity: str | None,
) -> None:
    connection.execute(
        """
        UPDATE stream_state SET
            next_run_at = ?,
            last_success_at = ?,
            consecutive_failures = 0,
            etag = ?,
            last_modified = ?,
            request_identity = ?
        WHERE source_id = ?
        """,
        (
            encode_datetime(next_run_at),
            now,
            response_etag,
            response_last_modified,
            request_identity,
            source_id,
        ),
    )


def _store_candidates(
    connection: sqlite3.Connection,
    source: Source,
    batch: ObservedBatch,
    *,
    committed_at: str,
) -> tuple[int, int, tuple[_ResolvedObservation, ...]]:
    """Persist one validated batch as unique resolved observations.

    Identity resolution runs for every accepted candidate, so URL aliases
    keep being learned even for candidates that collapse. The first
    candidate that resolves to an item is its representative and the only
    one that writes item fields, an observation, a version, and change
    events; later candidates resolving to the same item are collapsed.
    Counts derived here describe the resolved set, not the accepted list.
    """
    new_items = 0
    new_versions = 0
    resolved: dict[int, _ResolvedObservation] = {}
    for observation in batch.observations:
        candidate = observation.candidate
        metrics_json = encode_metrics(candidate.metrics)
        seen_at = encode_datetime(observation.observed_at)
        item_id, item_is_new = identity.resolve_item(
            connection, source, candidate, seen_at
        )
        if item_id in resolved:
            LOGGER.warning(
                "operation=candidate_collapsed source_id=%s item_id=%s title=%r",
                source.id,
                item_id,
                candidate.title[:120],
            )
            continue
        if not item_is_new:
            identity.refresh_item(connection, candidate, item_id, seen_at)
        observation_is_new = _upsert_observation(
            connection, source, item_id, candidate, seen_at
        )
        version_id, version_is_new = _upsert_version(
            connection,
            item_id,
            candidate.title.strip(),
            seen_at,
        )
        if item_is_new:
            new_items += 1
            _append_change(
                connection,
                "item_created",
                source.id,
                item_id,
                version_id,
                committed_at,
            )
        else:
            if observation_is_new:
                _append_change(
                    connection,
                    "item_observed",
                    source.id,
                    item_id,
                    version_id,
                    committed_at,
                )
            if version_is_new:
                _append_change(
                    connection,
                    "headline_version_created",
                    source.id,
                    item_id,
                    version_id,
                    committed_at,
                )
        if version_is_new:
            new_versions += 1
        resolved[item_id] = _ResolvedObservation(
            candidate=candidate,
            item_id=item_id,
            version_id=version_id,
            metrics_json=metrics_json,
        )
    return new_items, new_versions, tuple(resolved.values())


def _upsert_observation(
    connection: sqlite3.Connection,
    source: Source,
    item_id: int,
    candidate: HeadlineCandidate,
    seen_at: str,
) -> bool:
    existing = connection.execute(
        """
        SELECT 1 FROM observations
        WHERE source_id = ? AND item_id = ?
        """,
        (source.id, item_id),
    ).fetchone()
    if existing is None:
        connection.execute(
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
    connection.execute(
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
    connection: sqlite3.Connection,
    item_id: int,
    title: str,
    seen_at: str,
) -> tuple[int, bool]:
    title_hash = hashlib.sha256(
        normalize_title_for_version(title).encode("utf-8")
    ).hexdigest()
    existing = connection.execute(
        """
        SELECT id FROM item_versions
        WHERE item_id = ? AND title_hash = ?
        """,
        (item_id, title_hash),
    ).fetchone()
    version_id = int(existing["id"]) if existing is not None else None
    if version_id is not None:
        connection.execute(
            """
            UPDATE item_versions SET
                first_seen_at = MIN(first_seen_at, ?),
                last_seen_at = MAX(last_seen_at, ?)
            WHERE id = ?
            """,
            (seen_at, seen_at, version_id),
        )
        return version_id, False

    cursor = connection.execute(
        """
        INSERT INTO item_versions(
            item_id, title, title_hash, first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (item_id, title, title_hash, seen_at, seen_at),
    )
    return inserted_row_id(cursor), True


def _append_change(
    connection: sqlite3.Connection,
    event_type: str,
    source_id: str,
    item_id: int,
    version_id: int,
    now: str,
) -> None:
    connection.execute(
        """
        INSERT INTO change_log(
            event_type, source_id, item_id, item_version_id, created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (event_type, source_id, item_id, version_id, now),
    )
