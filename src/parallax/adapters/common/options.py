from __future__ import annotations

from collections.abc import Mapping

from parallax.config import Source

DEFAULT_HISTORY_MAX_ITEMS = 1000


def positive_integer_options(
    source: Source, defaults: Mapping[str, int]
) -> dict[str, int]:
    """Validate one adapter's declared option names and positive integer values."""
    unknown = sorted(set(source.endpoint.options) - defaults.keys())
    if unknown:
        raise ValueError(
            f"Unknown options for adapter {source.endpoint.adapter!r}: "
            f"{', '.join(unknown)}"
        )
    return {key: option_int(source, key, default) for key, default in defaults.items()}


def option_int(source: Source, key: str, default: int) -> int:
    """Read a positive integer source option, failing clearly when invalid."""
    value = source.endpoint.options.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"source.endpoint.options.{key} must be an integer")
    if value < 1:
        raise ValueError(f"source.endpoint.options.{key} must be positive")
    return int(value)
