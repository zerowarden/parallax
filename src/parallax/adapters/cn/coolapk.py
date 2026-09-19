from __future__ import annotations

import base64
import hashlib
import random
import string
import time
from typing import Any

from selectolax.parser import HTMLParser

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import decode_json_object, require_list, text
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)
from parallax.parsing import parse_timestamp

APP_ID = "com.coolapk.market"
# Public app constant the web client signs requests with; not a credential.
APP_TOKEN_SALT = "c67ef5943784d09750dcfbb31020f0ab"
ARTICLE_URL_PREFIX = "https://www.coolapk.com"
DEVICE_ID_LENGTHS = (10, 6, 6, 6, 14)
STATIC_HEADERS = {
    "X-Requested-With": "XMLHttpRequest",
    "X-App-Id": APP_ID,
    "X-Sdk-Int": "29",
    "X-Sdk-Locale": "zh-CN",
    "X-App-Version": "11.0",
    "X-Api-Version": "11",
    "X-App-Code": "2101202",
    "User-Agent": (
        "Dalvik/2.1.0 (Linux; U; Android 10; Redmi K30 5G MIUI/V12.0.3.0.QGICMXM) "
        "(#Build; Redmi; Redmi K30 5G; QKQ1.191222.002 test-keys; 10) "
        "+CoolMarket/11.0-2101202"
    ),
}


def new_device_id() -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "-".join(
        "".join(random.choices(alphabet, k=length)) for length in DEVICE_ID_LENGTHS
    )


def app_token(*, now: int, device: str) -> str:
    """Build the public X-App-Token header value the app client requires."""
    md5_now = hashlib.md5(str(now).encode()).hexdigest()
    raw = f"token://{APP_ID}/{APP_TOKEN_SALT}?{md5_now}${device}&{APP_ID}"
    md5_s = hashlib.md5(base64.b64encode(raw.encode())).hexdigest()
    return md5_s + device + f"0x{now:x}"


class CoolapkHotAdapter:
    """Native adapter for the Coolapk daily hot feed.

    Requests require a computed ``X-App-Token`` header, so they cannot be a
    plain configured URL. Titles fall back to the first message line when no
    editor title is set, and the row subtitle is kept as the heat metric.
    """

    def build_request(self, source: Source) -> RequestSpec:
        now = int(time.time())
        headers = dict(STATIC_HEADERS)
        headers["X-App-Token"] = app_token(now=now, device=new_device_id())
        return json_request(source, headers=headers)

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Coolapk")
        entries = require_list(
            payload.get("data"),
            "Coolapk response does not contain a data list",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_id = text(entry.get("id"))
            if not entry_id:
                continue
            title = text(entry.get("editor_title")) or _message_title(
                entry.get("message")
            )
            if not title:
                continue
            metrics: dict[str, Any] = {}
            target_row = entry.get("targetRow")
            if isinstance(target_row, dict):
                heat = text(target_row.get("subTitle"))
                if heat:
                    metrics["heat"] = heat
            raw_published = text(entry.get("dateline"))
            article_path = text(entry.get("url"))
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=(
                        f"{ARTICLE_URL_PREFIX}{article_path}"
                        if article_path.startswith("/")
                        else article_path
                    ),
                    external_id=entry_id,
                    published_at=parse_timestamp(raw_published),
                    raw_published_at=raw_published or None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Coolapk response contains no feed items")
        return ParsedBatch(candidates=tuple(candidates))


def _message_title(value: object) -> str:
    message = text(value)
    if not message:
        return ""
    plain = HTMLParser(message).text(separator="\n", strip=True)
    return plain.split("\n")[0].strip()
