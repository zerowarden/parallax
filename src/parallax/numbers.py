"""Numeric refiners for untrusted values, without coercion or unit policy."""

import math


def optional_integer(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def optional_finite_number(value: object) -> int | float | None:
    integer = optional_integer(value)
    if integer is not None:
        return integer
    return value if isinstance(value, float) and math.isfinite(value) else None
