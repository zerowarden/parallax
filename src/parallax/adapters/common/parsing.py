from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

CHINA_STANDARD_TIME = timezone(timedelta(hours=8))

_RELATIVE_PATTERN = re.compile(r"^(\d+)\s*(秒|秒鐘|分钟|分鐘|小时|小時|天|日)前$")
_RELATIVE_UNITS = {
    "秒": timedelta(seconds=1),
    "秒鐘": timedelta(seconds=1),
    "分钟": timedelta(minutes=1),
    "分鐘": timedelta(minutes=1),
    "小时": timedelta(hours=1),
    "小時": timedelta(hours=1),
    "天": timedelta(days=1),
    "日": timedelta(days=1),
}
_YESTERDAY_PATTERN = re.compile(r"^昨天\s*(\d{1,2}):(\d{2})$")


def decode_json_object(content: bytes, *, label: str) -> dict[str, Any]:
    """Decode an upstream JSON object, failing clearly on incompatible payloads."""
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid {label} JSON response: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} response must be a JSON object")
    return payload


def decode_json_list(content: bytes, *, label: str) -> list[Any]:
    """Decode an upstream JSON list, failing clearly on incompatible payloads."""
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid {label} JSON response: {exc}") from exc
    if not isinstance(payload, list):
        raise ValueError(f"{label} response must be a JSON list")
    return payload


def decode_html(
    content: bytes,
    *,
    label: str,
    encoding: str = "utf-8",
) -> str:
    """Decode an upstream HTML document, failing clearly on bad bytes."""
    try:
        return content.decode(encoding)
    except (UnicodeDecodeError, LookupError) as exc:
        raise ValueError(f"Invalid {label} HTML encoding: {exc}") from exc


def text(value: object) -> str:
    """Normalize an upstream scalar to trimmed text."""
    return "" if value is None else str(value).strip()


def require_mapping(value: object, message: str) -> dict[str, Any]:
    """Narrow an upstream value to a JSON object, failing clearly otherwise."""
    if not isinstance(value, dict):
        raise ValueError(message)
    return value


def require_list(value: object, message: str) -> list[Any]:
    """Narrow an upstream value to a JSON list, failing clearly otherwise."""
    if not isinstance(value, list):
        raise ValueError(message)
    return value


def parse_china_timestamp(value: str) -> datetime | None:
    """Parse a naive China-wall-time timestamp into UTC.

    Chinese publishers commonly emit local wall time without an offset;
    already-offset values are respected.
    """
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=CHINA_STANDARD_TIME)
    return parsed.astimezone(UTC)


def parse_relative_time(value: str, *, now: datetime | None = None) -> datetime | None:
    """Approximate a Chinese relative-time label as a UTC publication time.

    Labels such as "刚刚", "3分钟前", or "昨天 12:05" are coarse upstream
    evidence, so the result is anchored at ``now`` and callers keep the raw
    label as well. Unsupported labels return ``None``.
    """
    label = value.strip()
    if not label:
        return None
    reference = (now or datetime.now(CHINA_STANDARD_TIME)).astimezone(UTC)
    if label in {"刚刚", "刚才"}:
        return reference
    match = _RELATIVE_PATTERN.fullmatch(label)
    if match:
        amount = int(match.group(1))
        return reference - _RELATIVE_UNITS[match.group(2)] * amount
    match = _YESTERDAY_PATTERN.fullmatch(label)
    if match:
        local = reference.astimezone(CHINA_STANDARD_TIME) - timedelta(days=1)
        return local.replace(
            hour=int(match.group(1)),
            minute=int(match.group(2)),
            second=0,
            microsecond=0,
        ).astimezone(UTC)
    return None
