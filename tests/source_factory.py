from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from parallax.config import AuthConfig, Endpoint, RedirectPolicyConfig, Source
from parallax.domain import (
    BrowseSurface,
    ChannelRole,
    EntityKind,
    ItemKind,
    ItemVariant,
    ProviderKind,
    StreamKind,
)


def make_source(
    *,
    id: str = "fixture",
    provider_id: str = "fixture",
    provider_name: str = "Fixture Provider",
    provider_kind: ProviderKind = "publisher",
    adapter: str = "rss",
    url: str = "https://example.test/feed.xml",
    options: Mapping[str, Any] | None = None,
    channel_id: str = "main",
    channel_label: str = "Fixture",
    channel_role: ChannelRole = "aggregate",
    stream_kind: StreamKind = "latest",
    item_kind: ItemKind = "article",
    entity_kind: EntityKind | None = None,
    item_variant: ItemVariant | None = None,
    topics: Iterable[str] = (),
    surfaces: Iterable[BrowseSurface] = ("news",),
    language: str = "en-US",
    market: str = "US",
    interval_seconds: int = 900,
    max_items: int = 30,
    enabled: bool = True,
    headers: Mapping[str, str] | None = None,
    auth: AuthConfig | None = None,
    redirect: RedirectPolicyConfig | None = None,
) -> Source:
    """Build a resolved source for tests with explicit v2 declarations."""
    return Source(
        id=id,
        provider_id=provider_id,
        provider_name=provider_name,
        provider_kind=provider_kind,
        channel_id=channel_id,
        channel_label=channel_label,
        channel_role=channel_role,
        stream_kind=stream_kind,
        item_kind=item_kind,
        entity_kind=entity_kind,
        item_variant=item_variant,
        topics=tuple(topics),
        surfaces=frozenset(surfaces),
        language=language,
        market=market,
        interval_seconds=interval_seconds,
        max_items=max_items,
        enabled=enabled,
        endpoint=Endpoint(adapter=adapter, url=url, options=dict(options or {})),
        headers=dict(headers or {}),
        auth=auth or AuthConfig(),
        redirect=redirect or RedirectPolicyConfig(),
    )
