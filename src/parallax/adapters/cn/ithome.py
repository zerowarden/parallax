from __future__ import annotations

from parallax.adapters.common.http import json_request
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import (
    decode_json_object,
    parse_china_timestamp,
    require_list,
    text,
)
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

ITHOME_BASE_URL = "https://www.ithome.com"
AD_URL_MARKER = "lapin"


class IthomeNewsAdapter:
    """Metadata-only adapter for ITHome's public news JSON list.

    The reference implementation scrapes the HTML list and drops advertising
    entries; this adapter uses the site's JSON list with the same lapin-URL
    filter. ``postdate`` is Beijing wall time without an offset.
    """

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return json_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="ITHome")
        entries = require_list(
            payload.get("newslist"),
            "ITHome response does not contain a newslist",
        )

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            path = text(entry.get("url"))
            if AD_URL_MARKER in path:
                continue
            news_id = text(entry.get("newsid"))
            raw_published = text(entry.get("postdate"))
            candidates.append(
                HeadlineCandidate(
                    title=text(entry.get("title")),
                    url=_absolute_url(path),
                    external_id=news_id or None,
                    published_at=parse_china_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics={"stream_kind": source.stream_kind},
                )
            )
            if len(candidates) >= max_items:
                break
        return ParsedBatch(candidates=tuple(candidates))


def _absolute_url(path: str) -> str:
    if path.startswith("/"):
        return f"{ITHOME_BASE_URL}{path}"
    return path
