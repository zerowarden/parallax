"""Conversions at the SQLite persistence boundary."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime


def timestamp_now() -> str:
    return encode_datetime(datetime.now(UTC))


def encode_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("stored timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def decode_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def inserted_row_id(cursor: sqlite3.Cursor) -> int:
    rowid = cursor.lastrowid
    if rowid is None:
        raise RuntimeError("INSERT did not produce a rowid")
    return rowid
