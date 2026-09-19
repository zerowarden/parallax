from __future__ import annotations

import json
import re
from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import parse_china_timestamp, require_list, text
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

JS_PREFIX = "var newest = "
AD_CHANNEL = 5
BOLD_TAG = re.compile(r"</?b>")
HEADLINE_PREFIX = re.compile(r"^【([^】]*)】")
DETAIL_URL_TEMPLATE = "https://flash.jin10.com/detail/{flash_id}"


class Jin10FlashAdapter:
    """Native adapter for the Jin10 flash feed.

    The endpoint serves a JavaScript assignment (``var newest = [...]``) rather
    than raw JSON, so the wrapper is stripped before parsing. Entries that only
    belong to the advertising channel are skipped, matching the reference
    implementation. The upstream cache-buster query parameter is optional and is
    intentionally not sent, keeping requests deterministic.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        entries = _decode_flash_array(response.content)

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if AD_CHANNEL in (entry.get("channel") or []):
                continue
            data = entry.get("data")
            if not isinstance(data, dict):
                continue
            flash_text = BOLD_TAG.sub(
                "",
                text(data.get("title")) or text(data.get("content")),
            )
            if not flash_text:
                continue
            headline = HEADLINE_PREFIX.match(flash_text)
            title = headline.group(1).strip() if headline else flash_text
            flash_id = text(entry.get("id"))
            raw_published = text(entry.get("time"))
            metrics: dict[str, Any] = {}
            if entry.get("important") == 1:
                metrics["important"] = True
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=(
                        DETAIL_URL_TEMPLATE.format(flash_id=flash_id)
                        if flash_id
                        else ""
                    ),
                    external_id=flash_id or None,
                    published_at=parse_china_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        return ParsedBatch(candidates=tuple(candidates))


def _decode_flash_array(content: bytes) -> list[Any]:
    try:
        raw = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"Invalid Jin10 payload encoding: {exc}") from exc
    text_value = raw.strip()
    if not text_value.startswith(JS_PREFIX):
        raise ValueError("Jin10 response is not a 'var newest' JavaScript payload")
    text_value = text_value[len(JS_PREFIX) :].strip().rstrip(";").strip()
    try:
        payload = json.loads(text_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid Jin10 JSON payload: {exc}") from exc
    return require_list(payload, "Jin10 payload must be a list")
