"""Reconcile and read the persisted source catalog without deleting history."""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Sequence

from parallax.config import Source
from parallax.storage import foundation
from parallax.storage.encoding import timestamp_now

LOGGER = logging.getLogger(__name__)


def sync_sources(connection: sqlite3.Connection, sources: Sequence[Source]) -> None:
    now = timestamp_now()
    LOGGER.info("operation=source_sync count=%s", len(sources))
    with foundation.transaction(connection):
        connection.execute(
            "UPDATE sources SET enabled = 0, updated_at = ? WHERE enabled = 1",
            (now,),
        )
        for source in sources:
            connection.execute(
                """
                INSERT INTO sources(
                    source_id, provider_id, provider_name, provider_kind,
                    channel_id, channel_label, channel_role, stream_kind,
                    item_kind, entity_kind, item_variant, topics_json,
                    language, market, adapter, url, enabled,
                    interval_seconds, config_json, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(source_id) DO UPDATE SET
                    provider_id=excluded.provider_id,
                    provider_name=excluded.provider_name,
                    provider_kind=excluded.provider_kind,
                    channel_id=excluded.channel_id,
                    channel_label=excluded.channel_label,
                    channel_role=excluded.channel_role,
                    stream_kind=excluded.stream_kind,
                    item_kind=excluded.item_kind,
                    entity_kind=excluded.entity_kind,
                    item_variant=excluded.item_variant,
                    topics_json=excluded.topics_json,
                    language=excluded.language,
                    market=excluded.market,
                    adapter=excluded.adapter,
                    url=excluded.url,
                    enabled=excluded.enabled,
                    interval_seconds=excluded.interval_seconds,
                    config_json=excluded.config_json,
                    updated_at=excluded.updated_at
                """,
                (
                    source.id,
                    source.provider_id,
                    source.provider_name,
                    source.provider_kind,
                    source.channel_id,
                    source.channel_label,
                    source.channel_role,
                    source.stream_kind,
                    source.item_kind,
                    source.entity_kind,
                    source.item_variant,
                    json.dumps(list(source.topics), ensure_ascii=False),
                    source.language,
                    source.market,
                    source.endpoint.adapter,
                    source.endpoint.url,
                    int(source.enabled),
                    source.interval_seconds,
                    json.dumps(source.model_dump(mode="json"), ensure_ascii=False),
                    now,
                ),
            )
            connection.execute(
                "DELETE FROM source_surfaces WHERE source_id = ?",
                (source.id,),
            )
            for surface in sorted(source.surfaces):
                connection.execute(
                    """
                    INSERT INTO source_surfaces(source_id, surface)
                    VALUES (?, ?)
                    """,
                    (source.id, surface),
                )
            connection.execute(
                """
                INSERT INTO stream_state(source_id, consecutive_failures)
                VALUES (?, 0)
                ON CONFLICT(source_id) DO NOTHING
                """,
                (source.id,),
            )


def sources(connection: sqlite3.Connection) -> tuple[Source, ...]:
    """Return the enabled catalog stored by the last collector reconciliation."""
    rows = connection.execute(
        "SELECT config_json FROM sources WHERE enabled = 1 ORDER BY source_id"
    ).fetchall()
    return tuple(Source.model_validate_json(row["config_json"]) for row in rows)
