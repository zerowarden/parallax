"""Case-insensitive HTTP header composition, without request execution."""

from collections.abc import Mapping

import httpx


def compose_headers(*layers: Mapping[str, str]) -> httpx.Headers:
    """Compose declarations in precedence order, folding names by assignment."""
    headers = httpx.Headers()
    for layer in layers:
        for name, value in layer.items():
            headers[name] = value
    return headers
