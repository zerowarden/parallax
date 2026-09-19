from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from flask import Flask, render_template, request, url_for

from parallax.config import Source
from parallax.domain import BrowseView, is_browse_view
from parallax.feed import browse_items
from parallax.storage import Storage

LOGGER = logging.getLogger(__name__)

APPLICATION_TIMEZONE = ZoneInfo("Asia/Hong_Kong")
DAY_WINDOWS: tuple[tuple[int, str], ...] = (
    (1, "Today"),
    (3, "3 days"),
    (7, "7 days"),
    (30, "30 days"),
)
VALID_DAYS: frozenset[int] = frozenset(days for days, _ in DAY_WINDOWS)


@dataclass(frozen=True, slots=True)
class SourceFilterOption:
    """One source-filter entry labelled by its provider and channel."""

    id: str
    provider_name: str
    channel_label: str

    @property
    def label(self) -> str:
        return f"{self.provider_name} — {self.channel_label}"


def build_source_filter_options(
    sources: Sequence[Source],
) -> tuple[SourceFilterOption, ...]:
    options = [
        SourceFilterOption(
            id=source.id,
            provider_name=source.provider_name,
            channel_label=source.channel_label,
        )
        for source in sources
    ]
    return tuple(
        sorted(options, key=lambda option: (option.provider_name, option.channel_label))
    )


def create_app(
    storage: Storage,
    sources: Sequence[Source],
    *,
    now: Callable[[], datetime] | None = None,
) -> Flask:
    """Build the read-only Flask application over the stored archive."""
    app = Flask(__name__)
    clock = now or _utc_now
    source_options = build_source_filter_options(sources)

    app.jinja_env.filters["relative"] = format_relative
    app.jinja_env.tests["usable_url"] = is_usable_external_url

    @app.template_global()
    def browse_url(
        view: BrowseView,
        days: int,
        q: str = "",
        source: str = "",
        page: int = 1,
    ) -> str:
        params: dict[str, Any] = {"view": view, "days": days}
        if q:
            params["q"] = q
        if source:
            params["source"] = source
        if page > 1:
            params["page"] = page
        return url_for("index", **params)

    @app.get("/")
    def index() -> str | tuple[str, int]:
        current_time = clock()
        view = _view(request.args.get("view"))
        days = _days(request.args.get("days"))
        query = (request.args.get("q") or "").strip()
        source_id = (request.args.get("source") or "").strip()
        page = _page(request.args.get("page"))
        try:
            browse = browse_items(
                storage,
                view=view,
                since=window_start(days, current_time),
                until=current_time,
                query=query or None,
                source_id=source_id or None,
                page=page,
            )
            return render_template(
                "index.html",
                view=view,
                days=days,
                day_windows=DAY_WINDOWS,
                query=query,
                source_id=source_id,
                sources=source_options,
                browse=browse,
                now=current_time,
            )
        except (sqlite3.Error, ValueError):
            LOGGER.exception(
                "operation=web_browse_failed view=%s days=%s page=%s",
                view,
                days,
                page,
            )
            return render_template("error.html"), 500

    return app


def window_start(days: int, now: datetime) -> datetime:
    """UTC instant where the local calendar window begins.

    ``days=1`` is local midnight today; larger windows count calendar days
    including today.
    """
    local_now = now.astimezone(APPLICATION_TIMEZONE)
    local_midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    return (local_midnight - timedelta(days=days - 1)).astimezone(UTC)


def format_relative(value: str, now: datetime) -> str:
    """Render a stored timestamp relative to the request time."""
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    local_moment = moment.astimezone(APPLICATION_TIMEZONE)
    local_now = now.astimezone(APPLICATION_TIMEZONE)
    elapsed_seconds = (local_now - local_moment).total_seconds()
    if elapsed_seconds < 60:
        return "just now"
    if elapsed_seconds < 3600:
        return f"{int(elapsed_seconds // 60)}m ago"
    if elapsed_seconds < 86400:
        return f"{int(elapsed_seconds // 3600)}h ago"
    if local_moment.date() == (local_now - timedelta(days=1)).date():
        return "yesterday"
    return f"{local_moment.strftime('%b')} {local_moment.day}"


def is_usable_external_url(value: str) -> bool:
    """Return whether a stored URL is safe to expose as a browser link."""
    try:
        parsed = urlsplit(value.strip())
        _ = parsed.port
    except ValueError:
        return False
    return parsed.scheme in {"http", "https"} and parsed.hostname is not None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _view(value: str | None) -> BrowseView:
    return value if value is not None and is_browse_view(value) else "news"


def _days(value: str | None) -> int:
    try:
        days = int(value) if value is not None else 1
    except ValueError:
        return 1
    return days if days in VALID_DAYS else 1


def _page(value: str | None) -> int:
    try:
        page = int(value) if value is not None else 1
    except ValueError:
        return 1
    return max(page, 1)
