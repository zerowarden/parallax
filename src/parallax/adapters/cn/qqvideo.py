from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_post
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

COVER_URL_TEMPLATE = "https://v.qq.com/x/cover/{cover_id}.html"
REFERER = "https://v.qq.com/"

PAGE_BODY: dict[str, Any] = {
    "page_params": {
        "rank_channel_id": "100113",
        "rank_name": "HotSearch",
        "rank_page_size": "30",
        "tab_mvl_sub_mod_id": "792ac_19e77Sub_1b2",
        "tab_name": "热搜榜",
        "tab_type": "hot_rank",
        "tab_vl_data_src": "f5200deb4596bbf3",
        "page_id": "scms_shake",
        "page_type": "scms_shake",
        "source_key": "",
        "tag_id": "",
        "tag_type": "",
        "new_mark_label_enabled": "1",
    },
    "page_context": {
        "page_index": "1",
    },
    "flip_info": {
        "page_strategy_id": "",
        "page_module_id": "792ac_19e77",
        "module_strategy_id": {},
        "sub_module_id": "20251106065177",
        "flip_params": {
            "folding_screen_show_num": "",
            "is_mvl": "1",
            "mvl_strategy_info": (
                '{"default_strategy_id":"06755800b45b49238582a6fa1ad0f5c5",'
                '"default_version":"3836",'
                '"hit_page_uuid":"b5080d97dc694a5fb50eb9e7c99326ac",'
                '"hit_tab_info":null,"gray_status_info":null,'
                '"bypass_to_un_exp_id":""}'
            ),
            "mvl_sub_mod_id": "20251106065177",
            "pad_post_show_num": "",
            "pad_pro_post_show_num": "",
            "pad_pro_small_hor_pic_display_num": "",
            "pad_small_hor_pic_display_num": "",
            "page_id": "scms_shake",
            "page_num": "0",
            "page_type": "scms_shake",
            "post_show_num": "",
            "shake_size": "",
            "small_hor_pic_display_num": "",
            "source_key": "100113",
            "un_policy_id": "06755800b45b49238582a6fa1ad0f5c5",
            "un_strategy_id": "06755800b45b49238582a6fa1ad0f5c5",
        },
        "relace_children_key": [],
    },
}


class QqvideoHotSearchAdapter:
    """Native adapter for the Tencent Video TV hot-search rank.

    The public page endpoint requires a POST with the page-context body the web
    client sends; the body is a protocol constant (strategy/module identifiers
    included) rather than a credential. Cards expose no reliable publication
    time, so ``published_at`` stays ``None`` instead of substituting the fetch
    date. The cover id is the stable identity and defines the canonical URL.
    """

    def build_request(self, source: Source) -> RequestSpec:
        return json_post(source, PAGE_BODY, headers={"Referer": REFERER})

    def parse(self, source: Source, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Tencent Video")
        ret = payload.get("ret")
        if ret != 0:
            raise ValueError(f"Unexpected Tencent Video ret: {ret!r}")
        data = require_mapping(
            payload.get("data"),
            "Tencent Video response does not contain a data object",
        )
        card = require_mapping(
            data.get("card"),
            "Tencent Video response does not contain a card object",
        )
        children = require_mapping(
            card.get("children_list"),
            "Tencent Video response does not contain children_list",
        )
        container = require_mapping(
            children.get("list"),
            "Tencent Video response does not contain a card list",
        )
        cards = require_list(
            container.get("cards"),
            "Tencent Video response does not contain any cards",
        )

        max_items = source.max_items
        candidates: list[HeadlineCandidate] = []
        for item in cards:
            if not isinstance(item, dict):
                continue
            cover_id = text(item.get("id"))
            params = item.get("params")
            if not cover_id or not isinstance(params, dict):
                continue
            title = text(params.get("title"))
            if not title:
                continue
            metrics: dict[str, Any] = {}
            subtitle = text(params.get("sub_title"))
            if subtitle:
                metrics["subtitle"] = subtitle
            raw_published = text(params.get("publish_date"))
            rank = text(params.get("rank_num"))
            candidates.append(
                HeadlineCandidate(
                    title=title,
                    url=COVER_URL_TEMPLATE.format(cover_id=cover_id),
                    external_id=cover_id,
                    published_at=parse_timestamp(raw_published or None),
                    raw_published_at=raw_published or None,
                    position=int(rank) if rank.isdigit() else len(candidates) + 1,
                    metrics=metrics,
                )
            )
            if len(candidates) >= max_items:
                break
        if not candidates:
            raise ValueError("Tencent Video response contains no ranked cards")
        return ParsedBatch(candidates=tuple(candidates))
