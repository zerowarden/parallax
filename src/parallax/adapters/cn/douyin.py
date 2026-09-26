from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from parallax.adapters.base import AdapterStep, CompleteStep, ContinueStep
from parallax.adapters.common.http import cookie_header
from parallax.adapters.common.options import fixed_endpoint_options
from parallax.adapters.common.parsing import (
    decode_json_object,
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
from parallax.numbers import optional_finite_number

LOGIN_URL = "https://login.douyin.com/"
HOT_URL = (
    "https://www.douyin.com/aweme/v1/web/hot/search/list/"
    "?device_platform=webapp&aid=6383&channel=channel_pc_web&detail_list=1"
)
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)
DETAIL_URL_TEMPLATE = "https://www.douyin.com/hot/{sentence_id}"


def validate_config(source: Source) -> None:
    fixed_endpoint_options(source, HOT_URL)


class DouyinHotAdapter:
    """Two-step adapter for the Douyin hot-search list.

    The API only answers when cookies issued by the login landing page are
    sent; step 1 collects them, step 2 sends them explicitly. ``event_time`` is
    the trend's event time rather than a publication time, so it is kept as a
    bounded metric and ``published_at`` stays ``None``.
    """

    def first_step(self, source: Source) -> AdapterStep:
        return ContinueStep(
            request=RequestSpec(
                method="GET",
                url=LOGIN_URL,
                headers={"User-Agent": BROWSER_USER_AGENT},
            )
        )

    def next_step(
        self,
        source: Source,
        response: HttpResponse,
        context: Mapping[str, object],
    ) -> AdapterStep:
        if not context:
            return ContinueStep(
                request=RequestSpec(
                    method="GET",
                    url=source.endpoint.url,
                    headers={
                        "User-Agent": BROWSER_USER_AGENT,
                        "Cookie": cookie_header(response.cookies),
                    },
                ),
                context={"stage": "hot"},
            )
        return CompleteStep(batch=_parse_hot(source, response))


def _parse_hot(source: Source, response: HttpResponse) -> ParsedBatch:
    payload = decode_json_object(response.content, label="Douyin")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("Douyin response does not contain a data object")
    entries = require_list(
        data.get("word_list"),
        "Douyin response does not contain a word_list",
    )

    candidates = ranked_candidates(
        entries,
        max_items=source.max_items,
        build=_hot_candidate,
    )
    if not candidates:
        raise ValueError("Douyin hot list contains no entries")
    return ParsedBatch(candidates=tuple(candidates))


def _hot_candidate(entry: dict[str, Any], position: int) -> HeadlineCandidate | None:
    sentence_id = scalar_text(entry.get("sentence_id"))
    title = text(entry.get("word"))
    if not sentence_id or not title:
        return None
    metrics: dict[str, JsonValue] = {}
    hot_value = optional_finite_number(entry.get("hot_value"))
    if hot_value is not None:
        metrics["hot_value"] = hot_value
    event_time = optional_finite_number(entry.get("event_time"))
    if event_time is not None:
        metrics["event_time"] = event_time
    return HeadlineCandidate(
        title=title,
        url=DETAIL_URL_TEMPLATE.format(sentence_id=sentence_id),
        external_id=sentence_id,
        position=position,
        metrics=metrics,
    )
