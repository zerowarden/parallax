from __future__ import annotations

import unicodedata
from urllib.parse import unquote, urlsplit, urlunsplit

TRACKING_QUERY_KEYS = frozenset(
    {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "fbclid",
        "gclid",
    }
)


def normalize_url_for_identity(url: str) -> str:
    """Conservatively normalize a URL without removing query semantics."""
    split = urlsplit(url.strip())
    scheme = split.scheme.lower()
    hostname = (split.hostname or "").lower()
    port = split.port

    if port and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        netloc = f"{hostname}:{port}"
    else:
        netloc = hostname

    return urlunsplit((scheme, netloc, split.path, split.query, ""))


def canonicalize_url(url: str) -> str:
    """Normalize a URL for cross-source resource grouping.

    Tracking-only query components are removed; every other component keeps
    its order, multiplicity, and encoding.
    """
    split = urlsplit(normalize_url_for_identity(url))
    components = split.query.split("&") if split.query else []
    retained = [
        component
        for component in components
        if unquote(component.partition("=")[0]).casefold() not in TRACKING_QUERY_KEYS
    ]
    query = "&".join(retained)
    return urlunsplit((split.scheme, split.netloc, split.path, query, ""))


def normalize_title_for_version(title: str) -> str:
    """Derive the comparison key used to decide whether a title changed.

    Applies compatibility normalization and collapses whitespace runs; it does
    not alter case, punctuation, digits, wording, or script.
    """
    return " ".join(unicodedata.normalize("NFKC", title).split())
