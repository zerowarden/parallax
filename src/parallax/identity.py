from __future__ import annotations

from parallax.domain import HeadlineCandidate
from parallax.normalization import normalize_url_for_identity


def identity_key(candidate: HeadlineCandidate) -> str:
    if candidate.external_id:
        return f"external:{candidate.external_id.strip()}"
    return f"url:{normalize_url_for_identity(candidate.url)}"
