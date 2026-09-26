"""Persistent identity resolution on the ingestion transaction’s connection.

These functions never begin or commit transactions; the writer owns atomicity.
"""

from __future__ import annotations

import sqlite3

from parallax.config import Source
from parallax.domain import HeadlineCandidate
from parallax.identity import identity_keys
from parallax.normalization import canonicalize_url
from parallax.storage.encoding import encode_datetime, inserted_row_id


def _external_id(candidate: HeadlineCandidate) -> str | None:
    external_id = candidate.external_id.strip() if candidate.external_id else None
    return external_id or None


def resolve_item(
    connection: sqlite3.Connection,
    source: Source,
    candidate: HeadlineCandidate,
    seen_at: str,
) -> tuple[int, bool]:
    """Resolve one candidate to a persistent item, creating it when new.

    Resolution may upgrade a URL-only item to an authoritative external
    identity in place and records the candidate URL as an alias of the
    resolved item. Observation metadata is applied separately by
    ``refresh_item`` so only the representative candidate updates a
    pre-existing item.
    """
    primary_key, url_key = identity_keys(
        candidate,
        provider_id=source.provider_id,
    )
    external_id = _external_id(candidate)
    existing = connection.execute(
        "SELECT id FROM items WHERE identity_key = ?",
        (primary_key,),
    ).fetchone()
    if existing is None and external_id is not None:
        existing = connection.execute(
            """
            SELECT id FROM items
            WHERE identity_key = ? AND external_id IS NULL
            """,
            (url_key,),
        ).fetchone()
        if existing is not None:
            connection.execute(
                """
                UPDATE items SET identity_key = ?, external_id = ?
                WHERE id = ?
                """,
                (primary_key, external_id, existing["id"]),
            )
    elif existing is None:
        matches = connection.execute(
            """
            SELECT item_id FROM item_url_identities
            WHERE identity_key = ?
            ORDER BY item_id
            LIMIT 2
            """,
            (url_key,),
        ).fetchall()
        if len(matches) == 1:
            existing = connection.execute(
                "SELECT id FROM items WHERE id = ?",
                (matches[0]["item_id"],),
            ).fetchone()

    if existing is not None:
        item_id = int(existing["id"])
        _record_url_identity(connection, item_id, url_key)
        return item_id, False

    canonical_url = canonicalize_url(candidate.url)
    published = (
        encode_datetime(candidate.published_at) if candidate.published_at else None
    )
    cursor = connection.execute(
        """
        INSERT INTO items(
            identity_key, external_id, original_url, canonical_url,
            item_kind, entity_kind, item_variant, published_at,
            raw_published_at, first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            primary_key,
            external_id,
            candidate.url.strip(),
            canonical_url,
            source.item_kind,
            source.entity_kind,
            source.item_variant,
            published,
            candidate.raw_published_at,
            seen_at,
            seen_at,
        ),
    )
    item_id = inserted_row_id(cursor)
    _record_url_identity(connection, item_id, url_key)
    return item_id, True


def refresh_item(
    connection: sqlite3.Connection,
    candidate: HeadlineCandidate,
    item_id: int,
    seen_at: str,
) -> None:
    """Apply one representative observation to an existing item."""
    external_id = _external_id(candidate)
    canonical_url = canonicalize_url(candidate.url)
    published = (
        encode_datetime(candidate.published_at) if candidate.published_at else None
    )
    connection.execute(
        """
        UPDATE items SET
            external_id = COALESCE(external_id, ?),
            original_url = CASE
                WHEN last_seen_at <= ? THEN ?
                ELSE original_url
            END,
            canonical_url = CASE
                WHEN last_seen_at <= ? THEN ?
                ELSE canonical_url
            END,
            published_at = COALESCE(published_at, ?),
            raw_published_at = COALESCE(raw_published_at, ?),
            first_seen_at = MIN(first_seen_at, ?),
            last_seen_at = MAX(last_seen_at, ?)
        WHERE id = ?
        """,
        (
            external_id,
            seen_at,
            candidate.url.strip(),
            seen_at,
            canonical_url,
            published,
            candidate.raw_published_at,
            seen_at,
            seen_at,
            item_id,
        ),
    )


def _record_url_identity(
    connection: sqlite3.Connection, item_id: int, identity_key: str
) -> None:
    connection.execute(
        """
        INSERT INTO item_url_identities(item_id, identity_key)
        VALUES (?, ?)
        ON CONFLICT(item_id, identity_key) DO NOTHING
        """,
        (item_id, identity_key),
    )
