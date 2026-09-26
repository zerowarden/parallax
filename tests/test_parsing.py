from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from parallax.adapters.common.parsing import (
    parse_china_timestamp,
    parse_relative_time,
    scalar_text,
    text,
)
from parallax.parsing import parse_since, parse_timestamp

NOW = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize("value, expected", [(None, ""), ("  標題  ", "標題")])
def test_optional_upstream_text(value: object, expected: str) -> None:
    assert text(value) == expected
    assert scalar_text(value) == expected


@pytest.mark.parametrize("value", [{"title": "Headline"}, ["Headline"], True, 42, 1.5])
def test_upstream_text_rejects_non_text_values(value: object) -> None:
    with pytest.raises(ValueError, match="Expected upstream text"):
        text(value)


@pytest.mark.parametrize("value, expected", [(0, "0"), (42, "42"), (1.5, "1.5")])
def test_explicit_numeric_scalar_conversion(value: object, expected: str) -> None:
    assert scalar_text(value) == expected


@pytest.mark.parametrize("value", [{"id": 42}, [42], True, float("nan"), float("inf")])
def test_scalar_conversion_rejects_structures_booleans_and_nonfinite_numbers(
    value: object,
) -> None:
    with pytest.raises(ValueError, match="Expected upstream string or number"):
        scalar_text(value)


def test_parse_relative_time_handles_common_labels():
    assert parse_relative_time("刚刚", now=NOW) == NOW
    assert parse_relative_time("3分钟前", now=NOW) == NOW - timedelta(minutes=3)
    assert parse_relative_time("2小时前", now=NOW) == NOW - timedelta(hours=2)
    assert parse_relative_time("1天前", now=NOW) == NOW - timedelta(days=1)
    assert parse_relative_time("昨天 08:30", now=NOW) == datetime(
        2026, 9, 16, 0, 30, tzinfo=UTC
    )


def test_parse_relative_time_handles_traditional_labels():
    assert parse_relative_time("5分鐘前", now=NOW) == NOW - timedelta(minutes=5)
    assert parse_relative_time("3小時前", now=NOW) == NOW - timedelta(hours=3)
    assert parse_relative_time("2日前", now=NOW) == NOW - timedelta(days=2)


def test_parse_since_resolves_relative_windows():
    assert parse_since("7d", now=NOW) == NOW - timedelta(days=7)
    assert parse_since("2w", now=NOW) == NOW - timedelta(days=14)
    assert parse_since("3m", now=NOW) == NOW - timedelta(days=90)
    assert parse_since("1y", now=NOW) == NOW - timedelta(days=365)
    assert parse_since("5", now=NOW) == NOW - timedelta(days=5)
    assert parse_since(" 2D ", now=NOW) == NOW - timedelta(days=2)


def test_parse_since_rejects_unknown_windows():
    with pytest.raises(ValueError, match="Invalid since window"):
        parse_since("soon")


def test_parse_relative_time_returns_none_for_unknown_labels():
    assert parse_relative_time("上周", now=NOW) is None
    assert parse_relative_time("", now=NOW) is None


def test_parse_china_timestamp_treats_naive_values_as_beijing_time():
    assert parse_china_timestamp("2026-09-17 14:15:46") == datetime(
        2026, 9, 17, 6, 15, 46, tzinfo=UTC
    )
    assert parse_china_timestamp("") is None
    assert parse_china_timestamp("not-a-timestamp") is None


def test_generic_timestamp_parser_rejects_timezone_ambiguous_values() -> None:
    assert parse_timestamp("2026-09-17T14:15:46") is None
    assert parse_timestamp(datetime(2026, 9, 17, 14, 15, 46)) is None
    assert parse_timestamp("2026-09-17T14:15:46+08:00") == datetime(
        2026, 9, 17, 6, 15, 46, tzinfo=UTC
    )
