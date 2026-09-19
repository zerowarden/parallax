from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.parsing import decode_json_object, require_list, text
from parallax.config import Source
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

DEVICE_ID = "14a4b5ba98e790dce6dc07482447cf48"


class IqiyiHotRanklistAdapter:
    """Native adapter for the iQiyi hot rank list.

    The endpoint requires a static device token in the query string and a
    Referer header; neither is an authentication credential. ``showDate`` is a
    date-only value, so it is kept as the bounded ``show_date`` metric rather
    than a fabricated midnight publication timestamp.
    """

    def build_request(self, source: Source) -> RequestSpec:
        count = source.max_items
        return json_request(
            source,
            headers={"Referer": "https://www.iqiyi.com"},
            params={
                "channelName": "recommend",
                "data_source": "v7_rec_sec_hot_rank_list",
                "tempId": "85",
                "count": str(count),
                "block_id": "hot_ranklist",
                "device": DEVICE_ID,
                "from": "webapp",
            },
        )

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="iQiyi")
        code = payload.get("code")
        if code != 0:
            raise ValueError(f"Unexpected iQiyi code: {code!r}")
        items = require_list(
            payload.get("items"),
            "iQiyi response does not contain an items list",
        )
        if not items:
            raise ValueError("iQiyi response contains no items")
        item = items[0]
        if not isinstance(item, dict):
            raise ValueError("iQiyi response contains an invalid item")
        videos = require_list(
            item.get("video"),
            "iQiyi item does not contain a video block list",
        )
        if not videos:
            raise ValueError("iQiyi item contains no video blocks")
        video = videos[0]
        if not isinstance(video, dict):
            raise ValueError("iQiyi response contains an invalid video block")
        entries = require_list(
            video.get("data"),
            "iQiyi video block does not contain a data list",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for entry in entries[:max_items]:
            if not isinstance(entry, dict):
                continue
            metrics: dict[str, Any] = {}
            show_date = text(entry.get("showDate"))
            if show_date:
                metrics["show_date"] = show_date
            tag = text(entry.get("tag"))
            if tag:
                metrics["tag"] = tag
            candidates.append(
                HeadlineCandidate(
                    title=text(entry.get("title")),
                    url=text(entry.get("page_url")),
                    external_id=text(entry.get("entity_id")) or None,
                    position=len(candidates) + 1,
                    metrics=metrics,
                )
            )
        return ParsedBatch(candidates=tuple(candidates))
