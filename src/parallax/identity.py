from __future__ import annotations

from parallax.domain import HeadlineCandidate
from parallax.normalization import normalize_url_for_identity


def identity_keys(
    candidate: HeadlineCandidate,
    *,
    provider_id: str,
) -> tuple[str, str]:
    """Return the preferred identity and its stable URL fallback.

    Distinct upstream IDs remain distinct even when they currently share a
    URL.  Keeping the URL key alongside the preferred key lets persistence
    reconcile an observation when an upstream ID appears or disappears.
    """
    url_key = f"url:{normalize_url_for_identity(candidate.url)}"
    external_id = candidate.external_id.strip() if candidate.external_id else ""
    if external_id:
        return f"external:{provider_id}:{external_id}", url_key
    return url_key, url_key


def identity_key(candidate: HeadlineCandidate, *, provider_id: str) -> str:
    """Return the provider-scoped canonical identity of one observation.

    An upstream stable ID is authoritative within its provider, so the same
    article exposed by several provider channels collapses to one item. URLs
    remain global because they identify the resource itself.
    """
    preferred, _ = identity_keys(candidate, provider_id=provider_id)
    return preferred
