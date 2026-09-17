from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from parallax.adapters.base import AdapterStep, CompleteStep, ContinueStep
from parallax.adapters.common.http import cookie_header
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import (
    decode_json_object,
    require_list,
    require_mapping,
    text,
)
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

BOOTSTRAP_URL = "https://xueqiu.com/hq"
HOT_URL = "https://stock.xueqiu.com/v5/stock/hot_stock/list.json"
HOT_PARAMS = {"_type": "10", "type": "10"}
STOCK_URL_TEMPLATE = "https://xueqiu.com/s/{code}"


class XueqiuHotstockAdapter:
    """Two-step adapter for the Xueqiu hot-stock list.

    The API requires cookies issued by the xueqiu.com landing page; step 1
    collects them and step 2 sends them explicitly. Ad entries are skipped.
    """

    def first_step(self, source: SourceConfig) -> AdapterStep:
        return ContinueStep(
            request=RequestSpec(method="GET", url=BOOTSTRAP_URL),
        )

    def next_step(
        self,
        source: SourceConfig,
        response: HttpResponse,
        context: Mapping[str, object],
    ) -> AdapterStep:
        if not context:
            size = option_int(source, "max_items", 30)
            return ContinueStep(
                request=RequestSpec(
                    method="GET",
                    url=HOT_URL,
                    params={**HOT_PARAMS, "size": str(size)},
                    headers={"Cookie": cookie_header(response.cookies)},
                ),
                context={"stage": "hot"},
            )
        return CompleteStep(batch=_parse_hot(source, response))


def _parse_hot(source: SourceConfig, response: HttpResponse) -> ParsedBatch:
    payload = decode_json_object(response.content, label="Xueqiu")
    data = require_mapping(
        payload.get("data"),
        "Xueqiu response does not contain a data object",
    )
    entries = require_list(
        data.get("items"),
        "Xueqiu response does not contain an items list",
    )

    max_items = option_int(source, "max_items", 30)
    candidates: list[HeadlineCandidate] = []
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("ad"):
            continue
        code = text(entry.get("code"))
        title = text(entry.get("name"))
        if not code or not title:
            continue
        metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
        percent = entry.get("percent")
        if isinstance(percent, (int, float)) and not isinstance(percent, bool):
            metrics["percent"] = percent
        exchange = text(entry.get("exchange"))
        if exchange:
            metrics["exchange"] = exchange
        candidates.append(
            HeadlineCandidate(
                title=title,
                url=STOCK_URL_TEMPLATE.format(code=code),
                external_id=code,
                position=len(candidates) + 1,
                metrics=metrics,
            )
        )
        if len(candidates) >= max_items:
            break
    if not candidates:
        raise ValueError("Xueqiu hot-stock list contains no entries")
    return ParsedBatch(candidates=tuple(candidates))
