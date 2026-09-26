"""Archive browse and source-health queries; no collection side effects."""

from __future__ import annotations

import sqlite3
from datetime import datetime

from parallax.archive import FetchRunRow, HeadlineRow
from parallax.domain import (
    BrowseView,
    StreamState,
    is_browse_view,
)
from parallax.storage.encoding import (
    decode_datetime,
    decode_required_datetime,
    encode_datetime,
)

_LATEST_ITEM_VERSION_JOIN_SQL = """
    JOIN item_versions iv ON iv.id = (
        SELECT iv2.id
        FROM item_versions iv2
        WHERE iv2.item_id = i.id
        ORDER BY iv2.last_seen_at DESC, iv2.id DESC
        LIMIT 1
    )
"""


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
        published_at=decode_datetime(row["published_at"]),
        first_seen_at=decode_required_datetime(row["first_seen_at"]),
    )


def _stream_state_row(row: sqlite3.Row) -> StreamState:
    return StreamState(
        source_id=row["source_id"],
        next_run_at=decode_datetime(row["next_run_at"]),
        last_attempt_at=decode_datetime(row["last_attempt_at"]),
        last_success_at=decode_datetime(row["last_success_at"]),
        consecutive_failures=row["consecutive_failures"],
        etag=row["etag"],
        last_modified=row["last_modified"],
        request_identity=row["request_identity"],
    )


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
    since: datetime,
    until: datetime,
    query: str | None,
) -> tuple[list[str], list[object]]:
    filters = [
        "i.first_seen_at >= ?",
        "i.first_seen_at <= ?",
    ]
    params: list[object] = [encode_datetime(since), encode_datetime(until)]
    if query:
        filters.append("iv.title LIKE ? ESCAPE '\\'")
        params.append(f"%{_escape_like(query)}%")
    return filters, params


def _eligible_observations(
    *,
    view: BrowseView,
    source_id: str | None,
) -> tuple[str, list[object]]:
    """Shared eligibility for membership and representative-source selection."""
    joins = ""
    filters = ["eligible.item_id = i.id", "src.enabled = 1"]
    params: list[object] = []
    if view != "all":
        joins = "JOIN source_surfaces ss ON ss.source_id = eligible.source_id"
        filters.append("ss.surface = ?")
        params.append(view)
    if source_id:
        filters.append("eligible.source_id = ?")
        params.append(source_id)
    return (
        "FROM observations eligible "
        "JOIN sources src ON src.source_id = eligible.source_id "
        f"{joins} WHERE {' AND '.join(filters)}",
        params,
    )


def _membership_filter(
    *,
    view: BrowseView,
    source_id: str | None,
) -> tuple[str, list[object]]:
    """Count membership without ordering or selecting a representative."""
    eligible, params = _eligible_observations(view=view, source_id=source_id)
    return f"EXISTS (SELECT 1 {eligible})", params


def _representative_filter(
    *,
    view: BrowseView,
    source_id: str | None,
) -> tuple[str, list[object]]:
    """Choose one deterministic eligible observation per item for display."""
    eligible, params = _eligible_observations(view=view, source_id=source_id)
    return (
        f"o.source_id = (SELECT eligible.source_id {eligible} "
        "ORDER BY eligible.last_seen_at DESC, eligible.source_id LIMIT 1)",
        params,
    )


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def get_stream_state(connection: sqlite3.Connection, source_id: str) -> StreamState:
    row = connection.execute(
        "SELECT * FROM stream_state WHERE source_id = ?",
        (source_id,),
    ).fetchone()
    if row is None:
        return StreamState(source_id=source_id)
    return _stream_state_row(row)


def last_content_change_at(
    connection: sqlite3.Connection, source_id: str
) -> datetime | None:
    row = connection.execute(
        """
        SELECT MAX(created_at) AS changed_at
        FROM change_log
        WHERE source_id = ?
        """,
        (source_id,),
    ).fetchone()
    return decode_datetime(row["changed_at"])


def latest_snapshot_headlines(
    connection: sqlite3.Connection,
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

    rows = connection.execute(
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
                se.original_url AS url,
                se.canonical_url,
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
    connection: sqlite3.Connection,
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
    version across all channels, which may differ from the wording that
    the representative source displayed. Equal observation timestamps
    choose the greater version ID. Ordered newest first.
    """
    if limit < 1:
        raise ValueError("limit must be positive")
    if offset < 0:
        raise ValueError("offset must not be negative")
    _validate_browse_bounds(view, since, until)
    filters, params = _browse_filters(
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
    rows = connection.execute(
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
    connection: sqlite3.Connection,
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
    row = connection.execute(
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


def recent_fetch_runs(
    connection: sqlite3.Connection, limit: int = 20
) -> list[FetchRunRow]:
    rows = connection.execute(
        """
        SELECT * FROM fetch_runs
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [_fetch_run_row(row) for row in rows]


def latest_fetch_runs(
    connection: sqlite3.Connection, enabled_only: bool = True
) -> list[FetchRunRow]:
    enabled_filter = "AND src.enabled = 1" if enabled_only else ""
    rows = connection.execute(f"""
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
    return [_fetch_run_row(row) for row in rows]


def stream_states(
    connection: sqlite3.Connection, enabled_only: bool = True
) -> list[StreamState]:
    enabled_filter = "WHERE src.enabled = 1" if enabled_only else ""
    rows = connection.execute(f"""
        SELECT st.*
        FROM stream_state st
        JOIN sources src ON src.source_id = st.source_id
        {enabled_filter}
        ORDER BY st.source_id
        """).fetchall()
    return [_stream_state_row(row) for row in rows]


def _fetch_run_row(row: sqlite3.Row) -> FetchRunRow:
    return FetchRunRow(
        id=row["id"],
        source_id=row["source_id"],
        status=row["status"],
        run_kind=row["run_kind"],
        started_at=decode_required_datetime(row["started_at"]),
        finished_at=decode_datetime(row["finished_at"]),
        item_count=row["item_count"],
        new_item_count=row["new_item_count"],
        new_version_count=row["new_version_count"],
        rejected_count=row["rejected_count"],
        http_status=row["http_status"],
        error_type=row["error_type"],
        error_message=row["error_message"],
        history_json=row["history_json"],
    )
