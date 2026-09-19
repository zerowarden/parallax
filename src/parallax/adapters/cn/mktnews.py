from __future__ import annotations

import re
from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import (
    decode_json_object,
    require_list,
    text,
)
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)
from parallax.parsing import parse_timestamp

DETAIL_URL_TEMPLATE = "https://mktnews.net/flashDetail.html?id={flash_id}"
HEADLINE_PREFIX = re.compile(r"^【([^】]*)】")


class MktNewsFlashAdapter:
    """Metadata-only adapter for the MKTNews flash stream.

    Flash entries often carry no title; the ``【headline】`` prefix of the
    content, or the content itself, is used as the title like in the reference
    client. Content-derived titles can exceed the validator's title limit and
    are rejected rather than rewritten.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(
            source,
            headers={
                "Origin": "https://mktnews.net",
                "Referer": "https://mktnews.net/",
            },
        )

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="MKTNews")
        status = payload.get("status")
        if status != 200:
            raise ValueError(f"Unexpected MKTNews status: {status!r}")
        entries = require_list(
            payload.get("data"),
            "MKTNews response does not contain a data list",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for position, entry in enumerate(entries[:max_items], start=1):
            if not isinstance(entry, dict):
                continue
            data = entry.get("data")
            if not isinstance(data, dict):
                continue
            flash_id = text(entry.get("id"))
            raw_published = text(entry.get("time"))
            metrics: dict[str, Any] = {}
            if entry.get("important") == 1:
                metrics["important"] = True
            candidates.append(
                HeadlineCandidate(
                    title=_flash_title(data),
                    url=(
                        DETAIL_URL_TEMPLATE.format(flash_id=flash_id)
                        if flash_id
                        else ""
                    ),
                    external_id=flash_id or None,
                    published_at=parse_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=position,
                    metrics=metrics,
                )
            )
        return ParsedBatch(candidates=tuple(candidates))


def _flash_title(data: dict[str, Any]) -> str:
    title = text(data.get("title"))
    if title:
        return title
    content = text(data.get("content"))
    match = HEADLINE_PREFIX.match(content)
    if match:
        return match.group(1).strip()
    return content
