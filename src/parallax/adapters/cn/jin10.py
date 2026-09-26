from __future__ import annotations

import json
import re
from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import (
    extracted_batch,
    parse_china_timestamp,
    ranked_candidates,
    require_list,
    scalar_text,
    text,
)
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    JsonValue,
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

        excluded = 0

        def build(entry: dict[str, Any], position: int) -> HeadlineCandidate | None:
            nonlocal excluded
            if AD_CHANNEL in (entry.get("channel") or []):
                excluded += 1
                return None
            data = entry.get("data")
            if not isinstance(data, dict):
                return None
            flash_text = BOLD_TAG.sub(
                "",
                text(data.get("title")) or text(data.get("content")),
            )
            if not flash_text:
                return None
            headline = HEADLINE_PREFIX.match(flash_text)
            title = headline.group(1).strip() if headline else flash_text
            flash_id = scalar_text(entry.get("id"))
            raw_published = text(entry.get("time"))
            metrics: dict[str, JsonValue] = {}
            if entry.get("important") == 1:
                metrics["important"] = True
            return HeadlineCandidate(
                title=title,
                url=DETAIL_URL_TEMPLATE.format(flash_id=flash_id) if flash_id else "",
                external_id=flash_id or None,
                published_at=parse_china_timestamp(raw_published),
                raw_published_at=raw_published or None,
                position=position,
                metrics=metrics,
            )

        candidates = ranked_candidates(entries, max_items=source.max_items, build=build)
        return extracted_batch(
            candidates, entries=entries, label="jin10", excluded_count=excluded
        )


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
