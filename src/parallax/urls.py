"""Shared lexical validation; URL identity and redirect policy stay separate."""

from urllib.parse import SplitResult, urlsplit

_ILLEGAL_CHARACTERS = frozenset(' "<>\\^`{|}')


def parse_http_url(value: str) -> SplitResult:
    """Parse an absolute HTTP(S) URL without silently removing invalid input."""
    if any(
        character.isspace()
        or character in _ILLEGAL_CHARACTERS
        or ord(character) < 0x20
        or ord(character) == 0x7F
        for character in value
    ):
        raise ValueError("URL contains illegal characters")
    try:
        split = urlsplit(value)
        _ = split.port
    except ValueError as exc:
        raise ValueError("URL contains an invalid authority or port") from exc
    if split.scheme not in {"http", "https"} or not split.hostname:
        raise ValueError("URL must be an absolute HTTP(S) URL")
    return split
