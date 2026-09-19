from __future__ import annotations

import hashlib
import time
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import (
    decode_json_object,
    require_list,
    require_mapping,
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

DETAIL_URL_TEMPLATE = "https://www.cls.cn/detail/{article_id}"
TELEGRAPH_REFERER = "https://www.cls.cn/telegraph"
SIGN_PARAMS = {"appName": "CailianpressWeb", "os": "web", "sv": "7.7.5"}


def signed_params(
    extra: Mapping[str, str] | None = None,
    *,
    last_time: int | None = None,
) -> dict[str, str]:
    """Build the sorted, signed query parameters the CLS API requires.

    The sign is md5(sha1(sorted query string)); the algorithm comes from the
    public web client and is a protocol constant, not a credential.
    """
    params = dict(SIGN_PARAMS)
    if extra:
        params.update(extra)
    if last_time is not None:
        params["last_time"] = str(last_time)
    ordered = {key: params[key] for key in sorted(params)}
    query = urlencode(list(ordered.items()))
    sign = hashlib.md5(hashlib.sha1(query.encode()).hexdigest().encode()).hexdigest()
    return {**ordered, "sign": sign}


class ClsTelegraphAdapter:
    """Native adapter for the CLS telegraph roll list.

    Requests carry a computed sign and a current cursor, so they cannot be a
    plain configured URL. Ad-only entries are skipped.
    """

    def build_request(self, source: Source) -> RequestSpec:
        rn = source.max_items
        return json_request(
            source,
            headers={"Referer": TELEGRAPH_REFERER},
            params=signed_params(
                {"refresh_type": "1", "rn": str(rn)},
                last_time=int(time.time()),
            ),
        )

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="CLS")
        data = require_mapping(
            payload.get("data"),
            "CLS response does not contain a data object",
        )
        entries = require_list(
            data.get("roll_data"),
            "CLS response does not contain roll_data",
        )
        candidates = _candidates(entries, source, skip_ads=True)
        if not candidates:
            raise ValueError("CLS telegraph response contains no articles")
        return ParsedBatch(candidates=tuple(candidates))


class ClsDepthAdapter:
    """Native adapter for the CLS depth article list."""

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source, params=signed_params())

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="CLS")
        data = require_mapping(
            payload.get("data"),
            "CLS response does not contain a data object",
        )
        entries = require_list(
            data.get("depth_list"),
            "CLS depth response does not contain depth_list",
        )
        candidates = _candidates(entries, source)
        if not candidates:
            raise ValueError("CLS depth response contains no articles")
        return ParsedBatch(candidates=tuple(candidates))


class ClsHotAdapter:
    """Native adapter for the CLS hot article list."""

    def build_request(self, source: Source) -> RequestSpec:
        return json_request(source, params=signed_params())

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="CLS")
        entries = require_list(
            payload.get("data"),
            "CLS hot response does not contain a data list",
        )
        candidates = _candidates(entries, source)
        if not candidates:
            raise ValueError("CLS hot response contains no articles")
        return ParsedBatch(candidates=tuple(candidates))


def _candidates(
    entries: list[Any],
    source: Source,
    *,
    skip_ads: bool = False,
) -> list[HeadlineCandidate]:
    max_items = source.max_items
    candidates: list[HeadlineCandidate] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if skip_ads and entry.get("is_ad"):
            continue
        article_id = text(entry.get("id"))
        title = text(entry.get("title")) or text(entry.get("brief"))
        if not article_id or not title:
            continue
        raw_published = text(entry.get("ctime"))
        candidates.append(
            HeadlineCandidate(
                title=title,
                url=DETAIL_URL_TEMPLATE.format(article_id=article_id),
                external_id=article_id,
                published_at=_published_at(entry.get("ctime")),
                raw_published_at=raw_published or None,
                position=len(candidates) + 1,
            )
        )
        if len(candidates) >= max_items:
            break
    return candidates


def _published_at(value: object) -> datetime | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return parse_timestamp(value * 1000)
