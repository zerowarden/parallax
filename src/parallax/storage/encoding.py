"""Conversions at the SQLite persistence boundary."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime

from parallax.domain import JsonValue


def encode_metrics(metrics: Mapping[str, JsonValue]) -> str:
    try:
        return json.dumps(dict(metrics), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError(
            "candidate metrics must contain only finite JSON values"
        ) from exc


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


def decode_required_datetime(value: str | None) -> datetime:
    decoded = decode_datetime(value)
    if decoded is None:
        raise ValueError("required stored timestamp is missing")
    return decoded


def inserted_row_id(cursor: sqlite3.Cursor) -> int:
    rowid = cursor.lastrowid
    if rowid is None:
        raise RuntimeError("INSERT did not produce a rowid")
    return rowid
