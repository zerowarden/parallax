from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

from parallax.domain import HeadlineCandidate


def canonicalize_url(url: str) -> str:
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


def identity_key(candidate: HeadlineCandidate) -> str:
    if candidate.external_id:
        return f"external:{candidate.external_id.strip()}"
    return f"url:{canonicalize_url(candidate.url)}"
