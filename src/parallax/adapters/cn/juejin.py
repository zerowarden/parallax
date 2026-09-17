from __future__ import annotations

from typing import Any

from parallax.adapters.common.http import json_request
from parallax.adapters.common.options import option_int
from parallax.adapters.common.parsing import decode_json_object, require_list, text
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)

POST_URL_TEMPLATE = "https://juejin.cn/post/{content_id}"


class JuejinHotAdapter:
    """Metadata-only adapter for the Juejin hot article rank."""

    def build_request(self, source: SourceConfig) -> RequestSpec:
        return json_request(source)

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        payload = decode_json_object(response.content, label="Juejin")
        err_no = payload.get("err_no")
        if err_no != 0:
            raise ValueError(f"Unexpected Juejin err_no: {err_no!r}")
        entries = require_list(
            payload.get("data"),
            "Juejin response does not contain a data list",
        )

        max_items = option_int(source, "max_items", 30)
        candidates: list[HeadlineCandidate] = []
        for position, entry in enumerate(entries[:max_items], start=1):
            if not isinstance(entry, dict):
                continue
            content = entry.get("content")
            if not isinstance(content, dict):
                continue
            content_id = text(content.get("content_id"))
            metrics: dict[str, Any] = {"stream_kind": source.stream_kind}
            candidates.append(
                HeadlineCandidate(
                    title=text(content.get("title")),
                    url=(
                        POST_URL_TEMPLATE.format(content_id=content_id)
                        if content_id
                        else ""
                    ),
                    external_id=content_id or None,
                    position=position,
                    metrics=metrics,
                )
            )
        return ParsedBatch(candidates=tuple(candidates))
