"""Archive read contracts, independent of SQLite and collection services."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from parallax.domain import BrowseView, StreamState
from parallax.read_models import FetchRunRow as FetchRunRow
from parallax.read_models import HeadlineRow as HeadlineRow


class BrowseHeadlineReader(Protocol):
    def count_browse_headlines(
        self,
        *,
        view: BrowseView,
        since: datetime,
        until: datetime,
        query: str | None = None,
        source_id: str | None = None,
    ) -> int: ...

    def browse_headlines(
        self,
        *,
        view: BrowseView,
        since: datetime,
        until: datetime,
        query: str | None = None,
        source_id: str | None = None,
        limit: int,
        offset: int,
    ) -> list[HeadlineRow]: ...


class SourceHealthReader(Protocol):
    def get_stream_state(self, source_id: str) -> StreamState: ...
    def last_content_change_at(self, source_id: str) -> datetime | None: ...
