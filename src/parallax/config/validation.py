from __future__ import annotations

import re

LANGUAGE_TAG_PATTERN = re.compile(r"^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$")
MARKET_PATTERN = re.compile(r"^[A-Z]{2,8}$")
TOPIC_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")


def validate_language_tag(value: str) -> str:
    """Validate the BCP-47 shape Parallax accepts for source language tags."""
    if not LANGUAGE_TAG_PATTERN.match(value):
        raise ValueError(f"language {value!r} is not a valid BCP-47 tag")
    return value


def validate_market(value: str) -> str:
    """Validate an uppercase ISO-3166 style market code or GLOBAL."""
    if not MARKET_PATTERN.match(value):
        raise ValueError(
            f"market {value!r} must be an uppercase code such as 'GB' or 'GLOBAL'"
        )
    return value


def validate_topic_id(value: str) -> str:
    """Validate a lowercase topic slug declared inside a source entry."""
    if not TOPIC_ID_PATTERN.match(value):
        raise ValueError(f"topic {value!r} must be a lowercase slug")
    return value
