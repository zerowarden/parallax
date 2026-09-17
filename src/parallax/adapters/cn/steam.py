from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlsplit

from selectolax.parser import HTMLParser

from parallax.adapters.common.http import html_request
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import decode_html, text
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

BASE_URL = "https://store.steampowered.com"
APP_PATH = re.compile(r"/app/(\d+)(?:/|$)")


class SteamPlayersAdapter:
    """Native adapter for the Steam concurrent-player ranking.

    The stats page exposes no publication time, so ``published_at`` stays
    ``None``; the reference client substitutes the fetch time, which Parallax
    deliberately does not do. The current player count is kept as a metric.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return html_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        tree = HTMLParser(decode_html(response.content, label="Steam"))

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for row in tree.css("#detailStats tr.player_count_row"):
            link = row.css_first("a.gameLink")
            players_node = row.css_first(".currentServers")
            if link is None:
                continue
            href = text(link.attributes.get("href"))
            title = link.text(strip=True)
            if not href or not title:
                continue
            players = text(players_node.text(strip=True)) if players_node else ""
            metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
            if players:
                metrics["current_players"] = players
            url = href if href.startswith("http") else urljoin(BASE_URL, href)
            app_match = APP_PATH.search(urlsplit(href).path)
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=url,
                    external_id=app_match.group(1) if app_match else None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Steam page does not contain any player rows")
        return ParsedBatch(candidates=tuple(candidates))
