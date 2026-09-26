"""Read committed change events, hydrate analysis inputs and persist checkpoints."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Sequence
from typing import TypeGuard

from parallax.domain import (
    AnalysisItem,
    ChangeEvent,
    is_entity_kind,
    is_item_kind,
    is_item_variant,
    is_stream_kind,
)
from parallax.storage import foundation
from parallax.storage.encoding import decode_datetime, timestamp_now


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
    stream_kind = _classification_value(
        row["stream_kind"], is_stream_kind, item_id=event.item_id, label="stream kind"
    )
    item_kind = _classification_value(
        row["item_kind"], is_item_kind, item_id=event.item_id, label="item kind"
    )
    entity_kind = _optional_classification_value(
        row["entity_kind"], is_entity_kind, item_id=event.item_id, label="entity kind"
    )
    item_variant = _optional_classification_value(
        row["item_variant"],
        is_item_variant,
        item_id=event.item_id,
        label="item variant",
    )
    first_seen_at = decode_datetime(row["first_seen_at"])
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
        published_at=decode_datetime(row["published_at"]),
        first_seen_at=first_seen_at,
    )


def _classification_value[T: str](
    value: object,
    is_valid: Callable[[str], TypeGuard[T]],
    *,
    item_id: int,
    label: str,
) -> T:
    """Decode a stored classification using its authoritative domain predicate."""
    text = str(value)
    if not is_valid(text):
        raise ValueError(f"item {item_id} has unsupported {label} {text!r}")
    return text


def _optional_classification_value[T: str](
    value: object,
    is_valid: Callable[[str], TypeGuard[T]],
    *,
    item_id: int,
    label: str,
) -> T | None:
    if value is None:
        return None
    return _classification_value(value, is_valid, item_id=item_id, label=label)


def changes_after(
    connection: sqlite3.Connection, seq: int, limit: int = 100
) -> list[ChangeEvent]:
    rows = connection.execute(
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


def get_consumer_checkpoint(connection: sqlite3.Connection, consumer_name: str) -> int:
    row = connection.execute(
        """
        SELECT last_seq FROM consumer_checkpoints
        WHERE consumer_name = ?
        """,
        (consumer_name,),
    ).fetchone()
    return 0 if row is None else int(row["last_seq"])


def set_consumer_checkpoint(
    connection: sqlite3.Connection, consumer_name: str, last_seq: int
) -> None:
    now = timestamp_now()
    with foundation.transaction(connection):
        connection.execute(
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
    connection: sqlite3.Connection,
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
    rows = connection.execute(
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
