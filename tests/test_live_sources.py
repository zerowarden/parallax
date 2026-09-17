from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from adapter_contract import assert_batch_contract
from parallax.adapters import AdapterRegistry
from parallax.adapters.base import ContinueStep, MultiRequestAdapter, SteppedAdapter
from parallax.config import SourceConfig, load_settings
from parallax.domain import HttpResponse, StreamState
from parallax.transport import HttpTransport
from parallax.validation import BatchValidator

pytestmark = pytest.mark.live

NATIVE_SOURCE_IDS = (
    "thepaper-hot",
    "hk01-latest",
    "tencent-hot",
    "zhihu",
    "toutiao",
    "bilibili-hot-search",
    "solidot",
    "juejin",
    "dongqiudi",
    "producthunt",
    "chongbuluo-latest",
    "mktnews-flash",
    "douban",
    "ithome",
    "jin10",
    "nowcoder",
    "iqiyi-hot-ranklist",
    "baidu",
    "ifeng",
    "v2ex-share",
    "chongbuluo-hot",
    "hupu",
    "sputnik-news",
    "fastbull-express",
    "fastbull-news",
    "zaobao",
    "gelonghui",
    "github-trending-today",
    "steam",
    "cls-telegraph",
    "cls-depth",
    "cls-hot",
    "coolapk",
    "douyin",
    "xueqiu-hotstock",
    "wallstreetcn-quick",
    "wallstreetcn-news",
    "wallstreetcn-hot",
    "cankaoxiaoxi",
    "tieba",
    "weibo",
    "sspai",
    "hackernews",
    "hn-new",
    "kaopu",
    "qqvideo-tv-hotsearch",
    "bilibili-hot-video",
    "bilibili-ranking",
    "hket-directory",
    "hkej",
    "mingpao-realtime",
    "am730",
    "oncc",
    "wenweipo",
    "tkww",
    "now-news",
    "scmp-directory",
    "hkfp-latest",
    "thestandard-latest",
)

REDIRECT_SOURCE_IDS = (
    "bloomberg-markets",
    "rthk-local-zh",
    "rthk-greaterchina",
    "rthk-cinternational",
    "rthk-cfinance",
    "rthk-csport",
    "xueqiu-hotstock",
    "scmp-directory",
)


@pytest.mark.parametrize("source_id", NATIVE_SOURCE_IDS + REDIRECT_SOURCE_IDS)
def test_live_native_source_smoke(source_id: str, project_root: Path) -> None:
    """Fetch a configured native source directly from its publisher surface."""
    settings = load_settings(project_root / "config.toml")
    source = _configured_source(settings.sources, source_id)
    adapter = AdapterRegistry().get(source.adapter)

    with HttpTransport(settings.http) as transport:
        if isinstance(adapter, MultiRequestAdapter):
            responses = tuple(
                transport.request(spec, source, StreamState(source_id=source.id))
                for spec in adapter.build_requests(source)
            )
            for response in responses:
                _assert_http_ok(response)
            batch = adapter.parse_responses(source, responses)
        elif isinstance(adapter, SteppedAdapter):
            step = adapter.first_step(source)
            while isinstance(step, ContinueStep):
                response = transport.request(
                    step.request,
                    source,
                    StreamState(source_id=source.id),
                )
                _assert_http_ok(response)
                step = adapter.next_step(source, response, step.context)
            batch = step.batch
        else:
            request = adapter.build_request(source)
            response = transport.request(
                request,
                source,
                StreamState(source_id=source.id),
            )
            _assert_http_ok(response)
            batch = adapter.parse(source, response)

    assert_batch_contract(batch)
    validated = BatchValidator(settings.validation).validate(source.id, batch)
    assert validated.candidates


def _configured_source(
    sources: Sequence[SourceConfig],
    source_id: str,
) -> SourceConfig:
    for source in sources:
        if source.id == source_id:
            return source
    raise AssertionError(f"source {source_id!r} is not configured")


def _assert_http_ok(response: HttpResponse) -> None:
    if response.status_code == 200:
        return
    content_type = response.headers.get("content-type", "unknown")
    raise AssertionError(
        "live request failed: "
        f"status={response.status_code} content_type={content_type} url={response.url}"
    )
