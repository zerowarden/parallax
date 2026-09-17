from __future__ import annotations

import tomllib
from importlib.metadata import version
from pathlib import Path

import pytest
from pydantic import ValidationError

from parallax import __version__
from parallax.adapters import AdapterRegistry
from parallax.adapters.common.options import option_int
from parallax.config import (
    AppConfig,
    HttpConfig,
    IngestionConfig,
    RedirectPolicyConfig,
    SourceConfig,
    load_settings,
)

NATIVE_SOURCES = {
    "tencent-hot": "tencent_hot",
    "zhihu": "zhihu_hot",
    "toutiao": "toutiao_hot",
    "bilibili-hot-search": "bilibili_hot_search",
    "solidot": "rss",
    "juejin": "juejin_hot",
    "dongqiudi": "dongqiudi_news",
    "producthunt": "rss",
    "chongbuluo-latest": "rss",
    "mktnews-flash": "mktnews_flash",
    "douban": "douban_hot_movies",
    "ithome": "ithome_news",
    "jin10": "jin10_flash",
    "nowcoder": "nowcoder_hot",
    "iqiyi-hot-ranklist": "iqiyi_hot_ranklist",
    "baidu": "baidu_hot_search",
    "ifeng": "ifeng_hot",
    "v2ex-share": "v2ex_share",
    "chongbuluo-hot": "chongbuluo_hot",
    "hupu": "hupu_hot",
    "sputnik-news": "sputnik_news",
    "fastbull-express": "fastbull_express",
    "fastbull-news": "fastbull_news",
    "zaobao": "zaobao_realtime",
    "gelonghui": "gelonghui_news",
    "36kr-quick": "kr36_quick",
    "github-trending-today": "github_trending",
    "steam": "steam_players",
    "kuaishou": "kuaishou_hot",
    "cls-telegraph": "cls_telegraph",
    "cls-depth": "cls_depth",
    "cls-hot": "cls_hot",
    "coolapk": "coolapk_hot",
    "douyin": "douyin_hot",
    "xueqiu-hotstock": "xueqiu_hotstock",
    "wallstreetcn-quick": "wallstreetcn_quick",
    "wallstreetcn-news": "wallstreetcn_news",
    "wallstreetcn-hot": "wallstreetcn_hot",
    "cankaoxiaoxi": "cankaoxiaoxi_news",
    "tieba": "tieba_hot",
    "weibo": "weibo_hot",
    "sspai": "sspai_hot",
    "hackernews": "hackernews_hot",
    "hn-new": "hackernews_hot",
    "mingpao-realtime": "mingpao_rss",
    "kaopu": "kaopu_news",
    "qqvideo-tv-hotsearch": "qqvideo_hot_search",
    "bilibili-hot-video": "bilibili_hot_video",
    "bilibili-ranking": "bilibili_ranking",
    "hket-directory": "hket_rss",
    "hkej": "hkej_instant",
    "am730": "am730_news",
    "oncc": "oncc_news",
    "wenweipo": "wenweipo_news",
    "tkww": "tkww_news",
    "now-news": "now_news",
    "scmp-directory": "rss",
    "hkfp-latest": "rss",
    "thestandard-latest": "thestandard_news",
}
EXCLUDED_SOURCE_IDS = frozenset(
    {"36kr-renqi", "inmedia", "tvb-news", "thewitness-latest"}
)

REQUIRED_DOCUMENTS = ("README.md",)


def test_required_documents_exist(project_root: Path):
    missing = [
        name for name in REQUIRED_DOCUMENTS if not (project_root / name).is_file()
    ]

    assert not missing, f"required repository documents missing: {missing}"


def test_release_registry_contains_only_resolvable_streams():
    root = Path(__file__).resolve().parents[1]
    settings = load_settings(root / "config.toml")

    assert len(settings.sources) == 79
    assert len({source.id for source in settings.sources}) == len(settings.sources)
    assert not ({source.id for source in settings.sources} & EXCLUDED_SOURCE_IDS)
    assert all(source.adapter != "unsupported" for source in settings.sources)
    assert {source.id for source in settings.sources if not source.enabled} == {
        "36kr-quick",
        "kuaishou",
    }

    registry = AdapterRegistry()
    for adapter in {source.adapter for source in settings.sources}:
        registry.get(adapter)


def test_registry_contains_hong_kong_and_catalog_sources():
    root = Path(__file__).resolve().parents[1]
    settings = load_settings(root / "config.toml")
    ids = {source.id for source in settings.sources}

    assert "mingpao-realtime" in ids
    assert "hk01-latest" in ids
    assert "hkej" in ids
    assert "zhihu" in ids
    assert "weibo" in ids


@pytest.mark.parametrize(("source_id", "adapter"), sorted(NATIVE_SOURCES.items()))
def test_native_sources_use_configured_adapters(
    source_id: str,
    adapter: str,
):
    root = Path(__file__).resolve().parents[1]
    settings = load_settings(root / "config.toml")
    by_id = {source.id: source for source in settings.sources}

    source = by_id[source_id]
    assert source.adapter == adapter


@pytest.mark.parametrize(
    ("source_id", "url", "stream_kind"),
    [
        ("hackernews", "https://news.ycombinator.com/", "hot"),
        ("hn-new", "https://news.ycombinator.com/newest", "latest"),
    ],
)
def test_hacker_news_streams_use_public_html_listings(
    source_id: str, url: str, stream_kind: str
) -> None:
    root = Path(__file__).resolve().parents[1]
    sources = {
        source.id: source for source in load_settings(root / "config.toml").sources
    }

    source = sources[source_id]

    assert source.adapter == "hackernews_hot"
    assert source.url == url
    assert source.stream_kind == stream_kind
    assert source.retrieval_method == "unofficial_web_html"
    assert source.enabled


def test_release_metadata_matches_installed_package(project_root: Path) -> None:
    with (project_root / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)

    expected = pyproject["project"]["version"]

    assert isinstance(expected, str)
    assert __version__ == expected
    assert version("parallax") == expected


def test_unknown_configuration_fields_are_rejected() -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        AppConfig.model_validate({"log_levle": "INFO"})


def test_invalid_log_level_is_rejected() -> None:
    with pytest.raises(ValidationError, match="app.log_level"):
        AppConfig(log_level="verbose")


def test_ingestion_and_transport_defaults() -> None:
    ingestion = IngestionConfig()
    transport = HttpConfig()

    assert ingestion.max_concurrent_sources == 8
    assert ingestion.retry_base_seconds == 30
    assert ingestion.retry_max_seconds == 1800
    assert transport.max_connections_per_host == 2
    assert transport.max_attempts == 2
    assert transport.retry_backoff_seconds == 0.5


@pytest.mark.parametrize(
    "config",
    [
        {"max_concurrent_sources": 0},
        {"retry_base_seconds": 31, "retry_max_seconds": 30},
    ],
)
def test_ingestion_rejects_invalid_bounds(config: dict[str, int]) -> None:
    with pytest.raises(ValidationError):
        IngestionConfig.model_validate(config)


@pytest.mark.parametrize(
    "host",
    [
        "*.example.test",
        "https://example.test",
        "example.test/path",
        "user@example.test",
        "example.test:443",
    ],
)
def test_redirect_policy_rejects_non_hostname_hosts(host: str) -> None:
    with pytest.raises(ValidationError, match="exact hostnames"):
        RedirectPolicyConfig(allowed_hosts=(host,))


def test_redirect_policy_normalizes_exact_hosts() -> None:
    policy = RedirectPolicyConfig(
        allowed_hosts=("WWW.Example.TEST", "publisher.example.test"),
        allow_https_downgrade=True,
    )

    assert policy.allowed_hosts == ("www.example.test", "publisher.example.test")
    assert policy.allow_https_downgrade is True


def test_source_redirect_defaults_to_no_cross_authority_or_downgrade() -> None:
    source = SourceConfig(
        id="fixture",
        name="Fixture",
        region="CN",
        language="zh-CN",
        adapter="rss",
        url="https://example.test/feed.xml",
    )

    assert source.redirect.allowed_hosts == ()
    assert source.redirect.allow_https_downgrade is False


@pytest.mark.parametrize(
    ("source_id", "allowed_hosts", "allow_https_downgrade"),
    [
        ("bloomberg-markets", ("www.bloomberg.com",), False),
        ("xueqiu-hotstock", ("www.xueqiu.com",), False),
        ("rthk-local-zh", ("rthk9.rthk.hk",), True),
        ("rthk-greaterchina", ("rthk9.rthk.hk",), True),
        ("rthk-cinternational", ("rthk9.rthk.hk",), True),
        ("rthk-cfinance", ("rthk9.rthk.hk",), True),
        ("rthk-csport", ("rthk9.rthk.hk",), True),
        ("scmp-directory", (), True),
    ],
)
def test_redirect_sources_have_exact_approved_policy(
    source_id: str,
    allowed_hosts: tuple[str, ...],
    allow_https_downgrade: bool,
) -> None:
    root = Path(__file__).resolve().parents[1]
    sources = {
        source.id: source for source in load_settings(root / "config.toml").sources
    }

    assert sources[source_id].redirect.allowed_hosts == allowed_hosts
    assert sources[source_id].redirect.allow_https_downgrade is allow_https_downgrade


def test_registry_rejects_unknown_adapter_and_options() -> None:
    registry = AdapterRegistry()
    source = SourceConfig(
        id="fixture",
        name="Fixture",
        region="CN",
        language="zh-CN",
        adapter="rss",
        url="https://example.test/feed.xml",
        options={"max_item": 10},
    )

    with pytest.raises(ValueError, match="Unknown options.*max_item"):
        registry.validate_source(source)

    source.adapter = "missing"
    with pytest.raises(KeyError, match="Unknown adapter"):
        registry.validate_source(source)


@pytest.mark.parametrize("value", [True, 1.5, "5", 0, -1])
def test_integer_options_reject_invalid_or_coerced_values(value: object) -> None:
    source = SourceConfig(
        id="fixture",
        name="Fixture",
        region="CN",
        language="zh-CN",
        adapter="rss",
        url="https://example.test/feed.xml",
        options={"max_items": value},
    )
    with pytest.raises(ValueError, match="must be"):
        option_int(source, "max_items", default=10)
