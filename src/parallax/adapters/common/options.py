from __future__ import annotations

from parallax.config import SourceConfig


def option_int(source: SourceConfig, key: str, default: int) -> int:
    """Read a positive integer source option, failing clearly when invalid."""
    value = source.options.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"source.options.{key} must be an integer")
    if value < 1:
        raise ValueError(f"source.options.{key} must be positive")
    return int(value)
