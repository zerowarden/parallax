from __future__ import annotations

from parallax.config import Source

DEFAULT_HISTORY_MAX_ITEMS = 1000
DEFAULT_HISTORY_MAX_PAGES = 5
DEFAULT_HISTORY_PAGE_SIZE = 100


def option_int(source: Source, key: str, default: int) -> int:
    """Read a positive integer source option, failing clearly when invalid."""
    value = source.endpoint.options.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"source.endpoint.options.{key} must be an integer")
    if value < 1:
        raise ValueError(f"source.endpoint.options.{key} must be positive")
    return int(value)
