from __future__ import annotations

from pathlib import Path

import pytest

from adapter_contract import assert_batch_contract
from parallax.adapters import AdapterRegistry
from parallax.adapters.execution import run_adapter_refresh
from parallax.config import load_catalog
from parallax.domain import StreamState
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
    "pbs-headlines",
    "bbc-top",
    "ft-home",
    "dailymail-news",
    "nyt-homepage",
    "npr-top",
    "aljazeera-all",
    "dw-all",
    "sky-home",
    "independent-news",
    "economist-finance",
    "wsj-world",
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
    config = load_catalog(project_root / "config/config.toml")
    source = config.source(source_id)
    adapter = AdapterRegistry().get(source.endpoint.adapter)

    with HttpTransport(config.http) as transport:
        refresh = run_adapter_refresh(
            adapter, source, transport, StreamState(source.id)
        )

    batch = refresh.batch
    assert batch is not None, (
        "An unconditional smoke fetch must return a representation"
    )
    assert refresh.responses
    assert refresh.observed_at.tzinfo is not None
    assert_batch_contract(batch)
    validated = BatchValidator(config.validation).validate(
        source.id, batch, provider_id=source.provider_id
    )
    assert validated.candidates
