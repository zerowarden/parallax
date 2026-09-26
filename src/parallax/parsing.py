from __future__ import annotations

import email.utils
import re
from datetime import UTC, datetime, timedelta, tzinfo

from parallax.numbers import optional_finite_number

_SINCE_PATTERN = re.compile(r"^(\d+)\s*([dwmy])?$", re.IGNORECASE)
_SINCE_DAYS = {"d": 1, "w": 7, "m": 30, "y": 365}


def parse_since(value: str, *, now: datetime | None = None) -> datetime:
    """Resolve a relative history window into a UTC cutoff.

    Accepts ``7d`` (days), ``2w`` (weeks), ``3m`` (months, approximated as 30
    days), and ``1y`` (years, approximated as 365 days); a bare number means
    days. The approximation matches the coarse intent of manual backfills.
    """
    match = _SINCE_PATTERN.match(value.strip())
    if match is None:
        raise ValueError(
            f"Invalid since window {value!r}; use a number with d/w/m/y, e.g. 7d"
        )
    amount = int(match.group(1))
    unit = (match.group(2) or "d").lower()
    reference = now or datetime.now(UTC)
    return reference - timedelta(days=amount * _SINCE_DAYS[unit])


def parse_timestamp(
    value: object, *, naive_tz: tzinfo | None = None
) -> datetime | None:
    """Parse common upstream timestamp representations into UTC.

    Numeric values are interpreted as Unix seconds, or milliseconds when the
    magnitude clearly exceeds the seconds range. Strings may be numeric,
    ISO-8601, or RFC-822/2822 timestamps. A naive datetime is rejected unless
    ``naive_tz`` names the wall-clock zone the upstream source implies.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return _as_utc(value, naive_tz=naive_tz)
    numeric = optional_finite_number(value)
    if numeric is not None:
        return _from_unix(numeric)
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    try:
        numeric = float(text)
    except ValueError:
        numeric = None
    if numeric is not None:
        return _from_unix(numeric)

    parsed_iso = parse_iso_timestamp(text, naive_tz=naive_tz)
    if parsed_iso is not None:
        return parsed_iso

    try:
        parsed = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError):
        return None
    return _as_utc(parsed, naive_tz=naive_tz)


def parse_iso_timestamp(
    value: str, *, naive_tz: tzinfo | None = None
) -> datetime | None:
    """Parse an ISO-8601 string into UTC, anchoring a naive value at naive_tz."""
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed, naive_tz=naive_tz)


def _from_unix(value: int | float) -> datetime | None:
    try:
        if abs(value) >= 10_000_000_000:
            value /= 1000.0
        return datetime.fromtimestamp(value, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _as_utc(value: datetime, *, naive_tz: tzinfo | None = None) -> datetime | None:
    if value.tzinfo is None:
        if naive_tz is None:
            return None
        value = value.replace(tzinfo=naive_tz)
    return value.astimezone(UTC)
