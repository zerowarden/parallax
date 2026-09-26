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
    require_mapping,
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

BOOTSTRAP_URL = "https://xueqiu.com/hq"
HOT_URL = "https://stock.xueqiu.com/v5/stock/hot_stock/list.json"
HOT_PARAMS = {"_type": "10", "type": "10"}
STOCK_URL_TEMPLATE = "https://xueqiu.com/s/{code}"


def validate_config(source: Source) -> None:
    fixed_endpoint_options(source, HOT_URL)


class XueqiuHotstockAdapter:
    """Two-step adapter for the Xueqiu hot-stock list.

    The API requires cookies issued by the xueqiu.com landing page; step 1
    collects them and step 2 sends them explicitly. Ad entries are skipped.
    """

    def first_step(self, source: Source) -> AdapterStep:
        return ContinueStep(
            request=RequestSpec(method="GET", url=BOOTSTRAP_URL),
        )

    def next_step(
        self,
        source: Source,
        response: HttpResponse,
        context: Mapping[str, object],
    ) -> AdapterStep:
        if not context:
            size = source.max_items
            return ContinueStep(
                request=RequestSpec(
                    method="GET",
                    url=source.endpoint.url,
                    params={**HOT_PARAMS, "size": str(size)},
                    headers={"Cookie": cookie_header(response.cookies)},
                ),
                context={"stage": "hot"},
            )
        return CompleteStep(batch=_parse_hot(source, response))


def _parse_hot(source: Source, response: HttpResponse) -> ParsedBatch:
    payload = decode_json_object(response.content, label="Xueqiu")
    data = require_mapping(
        payload.get("data"),
        "Xueqiu response does not contain a data object",
    )
    entries = require_list(
        data.get("items"),
        "Xueqiu response does not contain an items list",
    )

    candidates = ranked_candidates(
        entries,
        max_items=source.max_items,
        build=_hot_candidate,
    )
    if not candidates:
        raise ValueError("Xueqiu hot-stock list contains no entries")
    return ParsedBatch(candidates=tuple(candidates))


def _hot_candidate(entry: dict[str, Any], position: int) -> HeadlineCandidate | None:
    if entry.get("ad"):
        return None
    code = scalar_text(entry.get("code"))
    title = text(entry.get("name"))
    if not code or not title:
        return None
    metrics: dict[str, JsonValue] = {}
    percent = optional_finite_number(entry.get("percent"))
    if percent is not None:
        metrics["percent"] = percent
    exchange = text(entry.get("exchange"))
    if exchange:
        metrics["exchange"] = exchange
    return HeadlineCandidate(
        title=title,
        url=STOCK_URL_TEMPLATE.format(code=code),
        external_id=code,
        position=position,
        metrics=metrics,
    )
