from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from adapter_contract import (
    assert_batch_contract,
    assert_parse_rejects,
    response_for,
)
from parallax.adapters.base import CompleteStep, ContinueStep
from parallax.adapters.cn.baidu import BaiduHotSearchAdapter
from parallax.adapters.cn.bilibili import (
    BilibiliHotSearchAdapter,
    BilibiliHotVideoAdapter,
    BilibiliRankingAdapter,
)
from parallax.adapters.cn.cankaoxiaoxi import CankaoxiaoxiNewsAdapter
from parallax.adapters.cn.chongbuluo import ChongbuluoHotAdapter
from parallax.adapters.cn.cls import (
    ClsDepthAdapter,
    ClsHotAdapter,
    ClsTelegraphAdapter,
    signed_params,
)
from parallax.adapters.cn.coolapk import CoolapkHotAdapter, app_token
from parallax.adapters.cn.dongqiudi import DongqiudiNewsAdapter
from parallax.adapters.cn.douban import DoubanHotMoviesAdapter
from parallax.adapters.cn.douyin import DouyinHotAdapter
from parallax.adapters.cn.fastbull import FastbullExpressAdapter, FastbullNewsAdapter
from parallax.adapters.cn.gelonghui import GelonghuiNewsAdapter
from parallax.adapters.cn.github import GithubTrendingAdapter
from parallax.adapters.cn.hackernews import HackerNewsHotAdapter
from parallax.adapters.cn.hupu import HupuHotAdapter
from parallax.adapters.cn.ifeng import IfengHotAdapter
from parallax.adapters.cn.iqiyi import IqiyiHotRanklistAdapter
from parallax.adapters.cn.ithome import IthomeNewsAdapter
from parallax.adapters.cn.jin10 import Jin10FlashAdapter
from parallax.adapters.cn.juejin import JuejinHotAdapter
from parallax.adapters.cn.kaopu import KaopuNewsAdapter
from parallax.adapters.cn.kr36 import Kr36QuickAdapter
from parallax.adapters.cn.kuaishou import KuaishouHotAdapter
from parallax.adapters.cn.mktnews import MktNewsFlashAdapter
from parallax.adapters.cn.nowcoder import NowcoderHotAdapter
from parallax.adapters.cn.qqvideo import QqvideoHotSearchAdapter
from parallax.adapters.cn.sputnik import SputnikNewsAdapter
from parallax.adapters.cn.sspai import SspaiHotAdapter
from parallax.adapters.cn.steam import SteamPlayersAdapter
from parallax.adapters.cn.tencent import TencentHotAdapter
from parallax.adapters.cn.thepaper import ThePaperHotAdapter
from parallax.adapters.cn.tieba import TiebaHotAdapter
from parallax.adapters.cn.toutiao import ToutiaoHotAdapter
from parallax.adapters.cn.v2ex import V2exShareAdapter
from parallax.adapters.cn.wallstreetcn import (
    WallstreetcnHotAdapter,
    WallstreetcnNewsAdapter,
    WallstreetcnQuickAdapter,
)
from parallax.adapters.cn.weibo import WeiboHotAdapter
from parallax.adapters.cn.xueqiu import XueqiuHotstockAdapter
from parallax.adapters.cn.zaobao import ZaobaoRealtimeAdapter
from parallax.adapters.cn.zhihu import ZhihuHotAdapter
from parallax.adapters.common.http import json_post
from parallax.adapters.common.rss import RssAdapter
from parallax.adapters.hk.am730 import Am730NewsAdapter
from parallax.adapters.hk.hk01 import Hk01LatestAdapter
from parallax.adapters.hk.hkej import HkejInstantAdapter
from parallax.adapters.hk.hket import HketRssAdapter
from parallax.adapters.hk.mingpao import MingpaoRssAdapter
from parallax.adapters.hk.now_news import NowNewsAdapter
from parallax.adapters.hk.oncc import OnccNewsAdapter
from parallax.adapters.hk.thestandard import TheStandardNewsAdapter
from parallax.adapters.hk.tkww import TkwwNewsAdapter
from parallax.adapters.hk.wenweipo import WenweipoNewsAdapter
from parallax.config import SourceConfig, ValidationConfig
from parallax.validation import BatchValidator


def _source(
    adapter: str,
    url: str,
    **options: object,
) -> SourceConfig:
    return SourceConfig(
        id="fixture",
        name="Fixture",
        region="CN",
        language="zh-CN",
        adapter=adapter,
        url=url,
        options=options,
    )


def test_rss_adapter_builds_request():
    source = _source("rss", "https://example.com/feed.xml")

    request = RssAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == "https://example.com/feed.xml"
    assert request.params == {}
    assert "xml" in request.headers["Accept"]


def test_rss_adapter_parses_items(fixtures_dir: Path):
    source = _source("rss", "https://example.com/feed.xml", max_items=20)
    payload = (fixtures_dir / "rss" / "feed.xml").read_bytes()

    batch = RssAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert [candidate.title for candidate in batch.candidates] == [
        "First headline",
        "Second headline",
    ]
    assert [candidate.external_id for candidate in batch.candidates] == [
        "story-1",
        "story-2",
    ]
    assert [candidate.position for candidate in batch.candidates] == [1, 2]
    assert batch.candidates[0].published_at is not None


def test_rss_adapter_rejects_dtd():
    source = _source("rss", "https://example.com/feed.xml")
    payload = (
        b'<?xml version="1.0"?>'
        b'<!DOCTYPE rss [<!ENTITY x "unsafe">]>'
        b"<rss><channel></channel></rss>"
    )

    assert_parse_rejects(RssAdapter(), source, payload, match="DTD or entity")


def test_rss_adapter_rejects_malformed_xml():
    source = _source("rss", "https://example.com/feed.xml")

    assert_parse_rejects(RssAdapter(), source, b"<rss><channel>", match="Invalid XML")


def test_rss_adapter_rejects_feed_without_channel():
    source = _source("rss", "https://example.com/feed.xml")

    assert_parse_rejects(
        RssAdapter(),
        source,
        b'<rss version="2.0"></rss>',
        match="does not contain a channel",
    )


def test_thepaper_adapter_builds_request():
    source = _source(
        "thepaper_hot",
        "https://cache.thepaper.cn/contentapi/wwwIndex/rightSidebar",
    )

    request = ThePaperHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("application/json")


def test_thepaper_adapter_parses_publication_time(fixtures_dir: Path):
    source = _source(
        "thepaper_hot",
        "https://cache.thepaper.cn/contentapi/wwwIndex/rightSidebar",
    )
    payload = (fixtures_dir / "thepaper" / "hot.json").read_bytes()

    batch = ThePaperHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    first = batch.candidates[0]
    assert first.title == "澎湃测试标题一"
    assert first.external_id == "1001"
    assert first.url == "https://www.thepaper.cn/newsDetail_forward_1001"
    assert first.published_at is not None
    assert first.raw_published_at == "1789540210000"
    assert first.metrics["praise_times"] == 123
    assert [candidate.position for candidate in batch.candidates] == [1, 2]


def test_thepaper_adapter_rejects_invalid_json():
    source = _source("thepaper_hot", "https://example.com/api")

    assert_parse_rejects(
        ThePaperHotAdapter(),
        source,
        b"<html>",
        match="Invalid The Paper JSON",
    )


def test_thepaper_adapter_rejects_changed_schema():
    source = _source("thepaper_hot", "https://example.com/api")

    assert_parse_rejects(
        ThePaperHotAdapter(),
        source,
        b'{"resultCode": 1, "data": {}}',
        match="hotNews",
    )


def test_hk01_adapter_parses_listing_metadata(fixtures_dir: Path):
    source = _source("hk01_latest", "https://web-data.api.hk01.com/v2/page/latest")
    payload = (fixtures_dir / "hk01" / "latest.json").read_bytes()

    batch = Hk01LatestAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 1
    first = batch.candidates[0]
    assert first.title == "香港01測試標題"
    assert first.external_id == "123456"
    assert first.position == 1
    assert first.published_at is not None
    assert first.metrics["tags"] == ["香港"]


def test_hk01_adapter_rejects_missing_items():
    source = _source("hk01_latest", "https://example.com/latest")

    assert_parse_rejects(
        Hk01LatestAdapter(),
        source,
        b'{"status": "ok"}',
        match="items list",
    )


def test_tencent_adapter_builds_request():
    source = _source(
        "tencent_hot",
        "https://i.news.qq.com/web_backend/v2/getTagInfo?tagId=aEWqxLtdgmQ%3D",
    )

    request = TencentHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {}
    assert request.headers["Referer"] == "https://news.qq.com/"
    assert request.headers["Accept"].startswith("application/json")


def test_tencent_adapter_parses_articles(fixtures_dir: Path):
    source = _source("tencent_hot", "https://example.com/getTagInfo")
    payload = (fixtures_dir / "tencent" / "hot.json").read_bytes()

    batch = TencentHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert [candidate.external_id for candidate in batch.candidates] == [
        "20260916A09GK500",
        "20260916A0CSZE00",
        "20260916A0CL3400",
    ]
    first = batch.candidates[0]
    assert first.title == "美众议院通过决议限制特朗普战争权力，7名共和党议员跳反"
    assert first.url == "https://view.inews.qq.com/a/20260916A09GK500"
    assert first.published_at == datetime(2026, 9, 16, 13, 35, 26, tzinfo=UTC)
    assert first.raw_published_at == "2026-09-16 21:35:26"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_tencent_adapter_ignores_other_tabs():
    source = _source("tencent_hot", "https://example.com/getTagInfo")
    payload = json.dumps(
        {
            "data": {
                "tabs": [
                    {
                        "articleList": [
                            {
                                "id": "kept",
                                "title": "保留条目",
                                "publish_time": "2026-09-16 21:35:26",
                                "link_info": {"url": "https://example.com/kept"},
                            }
                        ]
                    },
                    {
                        "articleList": [
                            {
                                "id": "ignored",
                                "title": "忽略条目",
                                "link_info": {"url": "https://example.com/ignored"},
                            }
                        ]
                    },
                ]
            }
        }
    ).encode()

    batch = TencentHotAdapter().parse(source, response_for(source, payload))

    assert [candidate.external_id for candidate in batch.candidates] == ["kept"]


def test_tencent_adapter_keeps_unknown_publish_time():
    source = _source("tencent_hot", "https://example.com/getTagInfo")
    payload = json.dumps(
        {
            "data": {
                "tabs": [
                    {
                        "articleList": [
                            {
                                "id": "missing-time",
                                "title": "无时间",
                                "link_info": {"url": "https://example.com/a"},
                            },
                            {
                                "id": "invalid-time",
                                "title": "坏时间",
                                "publish_time": "not-a-timestamp",
                                "link_info": {"url": "https://example.com/b"},
                            },
                        ]
                    }
                ]
            }
        }
    ).encode()

    batch = TencentHotAdapter().parse(source, response_for(source, payload))

    assert [candidate.published_at for candidate in batch.candidates] == [None, None]
    assert batch.candidates[1].raw_published_at == "not-a-timestamp"


def test_tencent_adapter_rejects_missing_tabs():
    source = _source("tencent_hot", "https://example.com/getTagInfo")

    assert_parse_rejects(
        TencentHotAdapter(),
        source,
        b'{"ret": 0, "data": {}}',
        match="does not contain tabs",
    )


def test_tencent_adapter_rejects_invalid_json():
    source = _source("tencent_hot", "https://example.com/getTagInfo")

    assert_parse_rejects(
        TencentHotAdapter(),
        source,
        b"<html>",
        match="Invalid Tencent JSON",
    )


def test_zhihu_adapter_builds_request():
    source = _source(
        "zhihu_hot",
        "https://www.zhihu.com/api/v3/feed/topstory/hot-list-web?limit=20&desktop=true",
    )

    request = ZhihuHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {}


def test_zhihu_adapter_parses_hot_list(fixtures_dir: Path):
    source = _source("zhihu_hot", "https://example.com/hot-list-web")
    payload = (fixtures_dir / "zhihu" / "hot.json").read_bytes()

    batch = ZhihuHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert [candidate.external_id for candidate in batch.candidates] == [
        "2082841016533632412",
        "2083156467859956956",
        "2083299849705714867",
    ]
    first = batch.candidates[0]
    assert first.title.startswith("平陆运河正式通航")
    assert first.url == "https://www.zhihu.com/question/2082841016533632412"
    assert first.published_at is None
    assert first.metrics["heat"] == "2915 万热度"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_zhihu_adapter_skips_entries_without_target():
    source = _source("zhihu_hot", "https://example.com/hot-list-web")
    payload = json.dumps(
        {
            "data": [
                {
                    "card_id": "Q_1",
                    "target": {
                        "title_area": {"text": "有效条目"},
                        "link": {"url": "https://www.zhihu.com/question/1"},
                    },
                },
                {"card_id": "Q_2"},
            ]
        }
    ).encode()

    batch = ZhihuHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 1
    assert batch.candidates[0].external_id == "1"


def test_zhihu_adapter_rejects_missing_data_list():
    source = _source("zhihu_hot", "https://example.com/hot-list-web")

    assert_parse_rejects(
        ZhihuHotAdapter(),
        source,
        b'{"error": {}}',
        match="does not contain a data list",
    )


def test_toutiao_adapter_builds_request():
    source = _source(
        "toutiao_hot",
        "https://www.toutiao.com/hot-event/hot-board/?origin=toutiao_pc",
    )

    request = ToutiaoHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {}


def test_toutiao_adapter_parses_ranked_items(fixtures_dir: Path):
    source = _source("toutiao_hot", "https://example.com/hot-board/")
    payload = (fixtures_dir / "toutiao" / "hot.json").read_bytes()

    batch = ToutiaoHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "近百国代表齐聚北京香山论坛"
    assert first.external_id == "7686104082540432942"
    assert first.url == "https://www.toutiao.com/trending/7686104082540432942/"
    assert first.published_at is None
    assert first.metrics["hot_value"] == "15235790"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]
    assert all(
        candidate.title != "习近平提笔写下“人民的保护神”"
        for candidate in batch.candidates
    )


def test_toutiao_adapter_rejects_missing_data_list():
    source = _source("toutiao_hot", "https://example.com/hot-board/")

    assert_parse_rejects(
        ToutiaoHotAdapter(),
        source,
        b'{"status": "success"}',
        match="does not contain a data list",
    )


def test_bilibili_adapter_builds_request():
    source = _source(
        "bilibili_hot_search",
        "https://s.search.bilibili.com/main/hotword?limit=30",
    )

    request = BilibiliHotSearchAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {}


def test_bilibili_adapter_parses_hot_search(fixtures_dir: Path):
    source = _source("bilibili_hot_search", "https://example.com/hotword")
    payload = (fixtures_dir / "bilibili" / "hot_search.json").read_bytes()

    batch = BilibiliHotSearchAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 5
    first = batch.candidates[0]
    assert first.title == "钟文泽评测iPhone 18 Pro"
    assert first.external_id == "钟文泽评测iPhone 18 Pro"
    parsed_url = urlsplit(first.url)
    assert parsed_url.scheme == "https"
    assert parsed_url.netloc == "search.bilibili.com"
    assert parse_qs(parsed_url.query) == {"keyword": ["钟文泽评测iPhone 18 Pro"]}
    assert first.published_at is None
    assert first.metrics["heat_score"] == 4028290
    mismatched = batch.candidates[4]
    assert mismatched.title == "U23国足逆转朝鲜迎亚运开门红"
    assert mismatched.external_id == "U23国足逆转朝鲜"
    mismatched_url = urlsplit(mismatched.url)
    assert parse_qs(mismatched_url.query) == {"keyword": ["U23国足逆转朝鲜"]}
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3, 4, 5]


def test_bilibili_adapter_rejects_error_code():
    source = _source("bilibili_hot_search", "https://example.com/hotword")

    assert_parse_rejects(
        BilibiliHotSearchAdapter(),
        source,
        b'{"code": -400, "list": []}',
        match="Unexpected Bilibili code",
    )


def test_bilibili_adapter_rejects_missing_list():
    source = _source("bilibili_hot_search", "https://example.com/hotword")

    assert_parse_rejects(
        BilibiliHotSearchAdapter(),
        source,
        b'{"code": 0}',
        match="does not contain a list",
    )


def test_adapter_invalid_items_are_left_for_validation():
    source = _source("toutiao_hot", "https://example.com/hot-board/")
    payload = json.dumps(
        {
            "data": [
                {"ClusterIdStr": "1", "Title": ""},
                {"ClusterIdStr": "2", "Title": "有效标题"},
            ]
        }
    ).encode()

    batch = ToutiaoHotAdapter().parse(source, response_for(source, payload))
    validated = BatchValidator(ValidationConfig()).validate(source.id, batch)

    assert len(validated.candidates) == 1
    assert validated.rejected_count == 1
    assert validated.candidates[0].title == "有效标题"


@pytest.mark.parametrize(
    ("fixture", "feed_url", "title", "link"),
    [
        (
            "solidot/feed.xml",
            "https://www.solidot.org/index.rss",
            "FAST 发现极短周期、最轻双中子星系统",
            "https://www.solidot.org/story?sid=85396",
        ),
        (
            "chongbuluo/latest.xml",
            "https://www.chongbuluo.com/forum.php?mod=rss&view=newthread",
            "财务不相干了，想转行你们觉得靠谱吗？",
            "https://www.chongbuluo.com/thread-25180-1-1.html",
        ),
    ],
)
def test_rss_adapter_parses_guidless_feed(
    fixtures_dir: Path,
    fixture: str,
    feed_url: str,
    title: str,
    link: str,
):
    source = _source("rss", feed_url)
    payload = (fixtures_dir / fixture).read_bytes()

    batch = RssAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == title
    assert first.url == link
    assert first.external_id is None
    assert first.published_at is not None
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_rss_adapter_parses_atom_feed(fixtures_dir: Path):
    source = _source("rss", "https://www.producthunt.com/feed")
    payload = (fixtures_dir / "producthunt" / "feed.xml").read_bytes()

    batch = RssAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "Twigg"
    assert first.external_id == "tag:www.producthunt.com,2005:Post/1250484"
    assert first.url == "https://www.producthunt.com/products/twigg"
    assert first.published_at == datetime(2026, 9, 14, 16, 29, 15, tzinfo=UTC)
    assert first.metrics["updated_at"] == "2026-09-17T06:53:59-07:00"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


_DAILYMAIL_NEWS_LINK = (
    "https://www.dailymail.com/news/article-16136593/Headteacher-20-000-year-"
    "London-private-school-pleads-guilty-injuring-injuring-cyclist.html?"
    "ns_mchannel=rss&ns_campaign=1490&ito=1490"
)


@pytest.mark.parametrize(
    (
        "fixture",
        "feed_url",
        "title",
        "link",
        "external_id",
        "published_at",
    ),
    [
        (
            "pbs-headlines/feed.xml",
            "https://www.pbs.org/newshour/feeds/rss/headlines",
            "WATCH LIVE: Senate expected to hold vote on College Sports Act",
            "https://www.pbs.org/newshour/politics/watch-live-senate-expected-to-hold-vote-on-college-sports-act",
            "https://www.pbs.org/newshour/politics/watch-live-senate-expected-to-hold-vote-on-college-sports-act",
            "2026-09-17T14:23:49+00:00",
        ),
        (
            "bbc-top/feed.xml",
            "https://feeds.bbci.co.uk/news/rss.xml",
            "Remains found after wildfire identified as mother-of-three "
            "missing since 2019",
            "https://www.bbc.co.uk/news/articles/c9e8e21x3vp4o?at_medium=RSS&at_campaign=rss",
            "https://www.bbc.co.uk/news/articles/c9e8e21x3vp4o#1",
            "2026-09-17T14:36:51+00:00",
        ),
        (
            "ft-home/feed.xml",
            "https://www.ft.com/rss/home/international",
            "Trump fails to bend the Fed to his will",
            "https://www.ft.com/content/fb8e1037-8c48-49d2-809e-950472bcbae5?syn-25a6b1a6=1",
            "fb8e1037-8c48-49d2-809e-950472bcbae5",
            "2026-09-17T04:00:31+00:00",
        ),
        (
            "dailymail-news/feed.xml",
            "https://www.dailymail.co.uk/news/index.rss",
            "Headteacher at £20,000-a-year top London private school "
            "pleads guilty to injuring cyclist while driving",
            _DAILYMAIL_NEWS_LINK,
            _DAILYMAIL_NEWS_LINK,
            "2026-09-17T14:50:15+00:00",
        ),
        (
            "nyt-homepage/feed.xml",
            "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
            "Who Will Win the Midterms? Republicans Are Reeling.",
            "https://www.nytimes.com/2026/09/17/us/politics/midterms-map-republicans-democrats.html",
            "https://www.nytimes.com/2026/09/17/us/politics/midterms-map-republicans-democrats.html",
            "2026-09-17T12:23:24+00:00",
        ),
        (
            "npr-top/feed.xml",
            "https://feeds.npr.org/1001/rss.xml",
            "Greetings from Beijing, where baozi are worth the wait in line",
            "https://www.npr.org/2026/09/17/g-s1-142416/china-beijing-buns-baozi",
            "https://www.npr.org/2026/09/17/g-s1-142416/china-beijing-buns-baozi",
            "2026-09-17T13:59:17+00:00",
        ),
        (
            "aljazeera-all/feed.xml",
            "https://www.aljazeera.com/xml/rss/all.xml",
            "Ghalibaf’s maths missile at Trump decoded: Is Iran fixing "
            "US interest rates?",
            "https://www.aljazeera.com/news/2026/9/17/ghalibafs-maths-missile-at-trump-decoded-is-iran-fixing-us-interest-rates?traffic_source=rss",
            "https://www.aljazeera.com/?t=1789633752",
            "2026-09-17T14:10:41+00:00",
        ),
        (
            "dw-all/feed.xml",
            "https://rss.dw.com/xml/rss-en-all",
            "Germany cuts development aid despite growing crises",
            "https://www.dw.com/en/germany-cuts-development-aid-despite-growing-crises/a-79257532?maca=en-rss-en-all-1573-xml-mrss",
            "79257532",
            "2026-09-17T14:36:00+00:00",
        ),
        (
            "sky-home/feed.xml",
            "https://feeds.skynews.com/feeds/rss/home.xml",
            "Father of Noah Woods, 3, pays tribute to son after body "
            "found following huge search",
            "https://news.sky.com/story/noah-woods-fundraiser-launched-after-body-found-in-search-for-missing-boy-raises-more-than-29000-13588898",
            "https://news.sky.com/story/noah-woods-fundraiser-launched-after-body-found-in-search-for-missing-boy-raises-more-than-29000-13588898",
            "2026-09-17T09:53:00+00:00",
        ),
        (
            "independent-news/feed.xml",
            "https://www.independent.co.uk/news/rss",
            "Carney accuses Trump of weaponising trade after president "
            "condemns ‘laughable’ EU-Canada membership",
            "https://www.independent.co.uk/news/world/americas/canada-eu-trump-mark-carney-b3051798.html",
            "b3051798",
            "2026-09-17T10:33:46+00:00",
        ),
        (
            "economist-finance/feed.xml",
            "https://www.economist.com/finance-and-economics/rss.xml",
            "GDP per person no longer grows like it used to",
            "https://www.economist.com/finance-and-economics/2026/09/17/gdp-per-person-no-longer-grows-like-it-used-to",
            "dc617d38-4e77-43d9-a4c5-4a6ebea55daf",
            "2026-09-17T10:30:49+00:00",
        ),
        (
            "wsj-world/feed.xml",
            "https://feeds.content.dowjones.io/public/rss/RSSWorldNews",
            "Carney Puts His Pivot Away From the U.S. to the Test",
            "https://www.wsj.com/world/europe/canada-mark-carney-european-union-a515b2a4?mod=rss_worldnews",
            "WP-WSJ-0003901378",
            "2026-09-17T14:02:00+00:00",
        ),
    ],
)
def test_rss_adapter_parses_publisher_feed(
    fixtures_dir: Path,
    fixture: str,
    feed_url: str,
    title: str,
    link: str,
    external_id: str,
    published_at: str,
):
    source = _source("rss", feed_url)
    payload = (fixtures_dir / fixture).read_bytes()

    batch = RssAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == title
    assert first.url == link
    assert first.external_id == external_id
    assert first.published_at == datetime.fromisoformat(published_at)
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]
    assert all(candidate.published_at is not None for candidate in batch.candidates)


def test_juejin_adapter_builds_request():
    source = _source(
        "juejin_hot",
        "https://api.juejin.cn/content_api/v1/content/article_rank?category_id=1&type=hot&spider=0",
    )

    request = JuejinHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {}


def test_juejin_adapter_parses_hot_rank(fixtures_dir: Path):
    source = _source("juejin_hot", "https://example.com/article_rank")
    payload = (fixtures_dir / "juejin" / "hot.json").read_bytes()

    batch = JuejinHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "ValidX时间段验证详解：ISO 8601标准与简化格式"
    assert first.external_id == "7684649314159312930"
    assert first.url == "https://juejin.cn/post/7684649314159312930"
    assert first.published_at is None
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_juejin_adapter_rejects_error_envelope():
    source = _source("juejin_hot", "https://example.com/article_rank")

    assert_parse_rejects(
        JuejinHotAdapter(),
        source,
        b'{"err_no": 1, "err_msg": "failed"}',
        match="Unexpected Juejin err_no",
    )


def test_juejin_adapter_rejects_missing_data_list():
    source = _source("juejin_hot", "https://example.com/article_rank")

    assert_parse_rejects(
        JuejinHotAdapter(),
        source,
        b'{"err_no": 0, "err_msg": "success"}',
        match="does not contain a data list",
    )


def test_dongqiudi_adapter_builds_request():
    source = _source("dongqiudi_news", "https://api.dongqiudi.com/app/tabs/web/1.json")

    request = DongqiudiNewsAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {}


def test_dongqiudi_adapter_parses_articles(fixtures_dir: Path):
    source = _source("dongqiudi_news", "https://example.com/web/1.json")
    payload = (fixtures_dir / "dongqiudi" / "news.json").read_bytes()

    batch = DongqiudiNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "国足亚运队2-1逆转朝鲜，张玉宁、吴曦互相传射，李昊神扑"
    assert first.external_id == "6355444"
    assert first.url == "https://www.dongqiudi.com/article/6355444"
    assert first.published_at == datetime(2026, 9, 16, 11, 51, 59, tzinfo=UTC)
    assert first.raw_published_at == "2026-09-16 19:51:59"
    assert first.metrics["category"] == "足球"
    second = batch.candidates[1]
    assert second.url == "https://n.dongqiudi.com/webapp/tops.html?id=6355486"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_dongqiudi_adapter_keeps_unknown_created_at():
    source = _source("dongqiudi_news", "https://example.com/web/1.json")
    payload = json.dumps(
        {
            "articles": [
                {
                    "id": 1,
                    "title": "无时间",
                    "share": "https://example.com/article/1",
                }
            ]
        }
    ).encode()

    batch = DongqiudiNewsAdapter().parse(source, response_for(source, payload))

    assert batch.candidates[0].published_at is None
    assert batch.candidates[0].raw_published_at is None


def test_dongqiudi_adapter_rejects_missing_articles_list():
    source = _source("dongqiudi_news", "https://example.com/web/1.json")

    assert_parse_rejects(
        DongqiudiNewsAdapter(),
        source,
        b'{"id": 1}',
        match="does not contain an articles list",
    )


def test_mktnews_adapter_builds_request():
    source = _source(
        "mktnews_flash",
        "https://api.mktnews.net/api/flash?type=0&limit=50",
    )

    request = MktNewsFlashAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {}
    assert request.headers["Origin"] == "https://mktnews.net"
    assert request.headers["Referer"] == "https://mktnews.net/"


def test_mktnews_adapter_parses_flash_stream(fixtures_dir: Path):
    source = _source("mktnews_flash", "https://example.com/api/flash")
    payload = (fixtures_dir / "mktnews" / "flash.json").read_bytes()

    batch = MktNewsFlashAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "01a0ae06-1b5b-7118-85d3-84c7787342ca"
    assert first.title.startswith("Australia's S&P/ASX 200 index")
    assert first.url == (
        "https://mktnews.net/flashDetail.html?id=01a0ae06-1b5b-7118-85d3-84c7787342ca"
    )
    assert first.published_at == datetime(2026, 9, 17, 6, 20, 30, tzinfo=UTC)
    assert "important" not in first.metrics
    assert batch.candidates[1].metrics["important"] is True
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_mktnews_adapter_extracts_bracketed_headline():
    source = _source("mktnews_flash", "https://example.com/api/flash")
    payload = json.dumps(
        {
            "status": 200,
            "data": [
                {
                    "id": "flash-1",
                    "time": "2026-09-17T06:20:30.000Z",
                    "important": 0,
                    "data": {
                        "title": None,
                        "content": "【重要数据】中国9月CPI同比上涨0.5%",
                    },
                }
            ],
        }
    ).encode()

    batch = MktNewsFlashAdapter().parse(source, response_for(source, payload))

    assert batch.candidates[0].title == "重要数据"


def test_mktnews_adapter_rejects_unexpected_status():
    source = _source("mktnews_flash", "https://example.com/api/flash")

    assert_parse_rejects(
        MktNewsFlashAdapter(),
        source,
        b'{"status": 500, "message": "failed"}',
        match="Unexpected MKTNews status",
    )


def test_mktnews_adapter_rejects_missing_data_list():
    source = _source("mktnews_flash", "https://example.com/api/flash")

    assert_parse_rejects(
        MktNewsFlashAdapter(),
        source,
        b'{"status": 200}',
        match="does not contain a data list",
    )


def test_douban_adapter_builds_request():
    source = _source(
        "douban_hot_movies",
        "https://m.douban.com/rexxar/api/v2/subject/recent_hot/movie",
    )

    request = DoubanHotMoviesAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {}
    assert request.headers["Referer"] == "https://movie.douban.com/"


def test_douban_adapter_parses_hot_movies(fixtures_dir: Path):
    source = _source("douban_hot_movies", "https://example.com/recent_hot/movie")
    payload = (fixtures_dir / "douban" / "hot_movies.json").read_bytes()

    batch = DoubanHotMoviesAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "求救信号"
    assert first.external_id == "36439868"
    assert first.url == "https://movie.douban.com/subject/36439868"
    assert first.published_at is None
    assert first.metrics["rating"] == 7.1
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_douban_adapter_rejects_missing_items():
    source = _source("douban_hot_movies", "https://example.com/recent_hot/movie")

    assert_parse_rejects(
        DoubanHotMoviesAdapter(),
        source,
        b'{"total": 0}',
        match="does not contain an items list",
    )


def test_ithome_adapter_builds_request():
    source = _source("ithome_news", "https://api.ithome.com/json/newslist/news")

    request = IthomeNewsAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {}


def test_ithome_adapter_parses_news_and_skips_ads(fixtures_dir: Path):
    source = _source("ithome_news", "https://example.com/newslist/news")
    payload = (fixtures_dir / "ithome" / "news.json").read_bytes()

    batch = IthomeNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 4
    first = batch.candidates[0]
    assert first.title.startswith("莱迪思推出 FPGA 开发 AI 辅助工具")
    assert first.external_id == "1003574"
    assert first.url == "https://www.ithome.com/1/003/574.htm"
    assert first.published_at == datetime(2026, 9, 17, 6, 12, 16, 567000, tzinfo=UTC)
    assert first.raw_published_at == "2026-09-17T14:12:16.567"
    assert all("lapin" not in candidate.url for candidate in batch.candidates)
    assert all("鸿蒙版" not in candidate.title for candidate in batch.candidates)
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3, 4]


def test_ithome_adapter_keeps_unknown_postdate():
    source = _source("ithome_news", "https://example.com/newslist/news")
    payload = json.dumps(
        {"newslist": [{"newsid": 1, "title": "无时间", "url": "/1/000/001.htm"}]}
    ).encode()

    batch = IthomeNewsAdapter().parse(source, response_for(source, payload))

    assert batch.candidates[0].published_at is None
    assert batch.candidates[0].raw_published_at is None


def test_ithome_adapter_rejects_missing_newslist():
    source = _source("ithome_news", "https://example.com/newslist/news")

    assert_parse_rejects(
        IthomeNewsAdapter(),
        source,
        b'{"toplist": []}',
        match="does not contain a newslist",
    )


def test_jin10_adapter_builds_request():
    source = _source("jin10_flash", "https://www.jin10.com/flash_newest.js")

    request = Jin10FlashAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {}


def test_jin10_adapter_parses_flash_stream(fixtures_dir: Path):
    source = _source("jin10_flash", "https://example.com/flash_newest.js")
    payload = (fixtures_dir / "jin10" / "flash.js").read_bytes()

    batch = Jin10FlashAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "印度7月美债购买量创纪录 净买入跃升至152亿美元"
    assert first.external_id == "20260917143656863800"
    assert first.url == "https://flash.jin10.com/detail/20260917143656863800"
    assert first.published_at == datetime(2026, 9, 17, 6, 36, 56, tzinfo=UTC)
    assert first.raw_published_at == "2026-09-17 14:36:56"
    assert "important" not in first.metrics
    important = batch.candidates[1]
    assert important.metrics["important"] is True
    assert important.title == "集运指数（欧线）主力合约日内涨超6.00%，现报2173.0点。"
    assert all(
        candidate.external_id != "20260917143714849800"
        for candidate in batch.candidates
    )
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_jin10_adapter_rejects_non_javascript_payload():
    source = _source("jin10_flash", "https://example.com/flash_newest.js")

    assert_parse_rejects(
        Jin10FlashAdapter(),
        source,
        b'{"id": "1"}',
        match="var newest",
    )


def test_jin10_adapter_rejects_broken_payload():
    source = _source("jin10_flash", "https://example.com/flash_newest.js")

    assert_parse_rejects(
        Jin10FlashAdapter(),
        source,
        b"var newest = {broken",
        match="Invalid Jin10 JSON",
    )


def test_nowcoder_adapter_builds_request():
    source = _source(
        "nowcoder_hot",
        "https://gw-c.nowcoder.com/api/sparta/hot-search/top-hot-pc",
    )

    request = NowcoderHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {"size": "30"}


def test_nowcoder_adapter_parses_hot_list(fixtures_dir: Path):
    source = _source("nowcoder_hot", "https://example.com/top-hot-pc")
    payload = (fixtures_dir / "nowcoder" / "hot.json").read_bytes()

    batch = NowcoderHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 4
    first = batch.candidates[0]
    assert first.title == "一个应届生对大厂招聘者的忠告"
    assert first.external_id == "230b23931569461e9775faeb97aef65b"
    assert first.url == (
        "https://www.nowcoder.com/feed/main/detail/230b23931569461e9775faeb97aef65b"
    )
    assert first.published_at is None
    assert first.metrics["hot_value"] == 19893
    discuss = batch.candidates[3]
    assert discuss.external_id == "929731481189044224"
    assert discuss.url == "https://www.nowcoder.com/discuss/929731481189044224"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3, 4]


def test_nowcoder_adapter_rejects_missing_result_list():
    source = _source("nowcoder_hot", "https://example.com/top-hot-pc")

    assert_parse_rejects(
        NowcoderHotAdapter(),
        source,
        b'{"success": true}',
        match="data object",
    )


def test_iqiyi_adapter_builds_request():
    source = _source(
        "iqiyi_hot_ranklist",
        "https://mesh.if.iqiyi.com/portal/lw/v7/channel/card/videoTab",
    )

    request = IqiyiHotRanklistAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Referer"] == "https://www.iqiyi.com"
    assert request.params["block_id"] == "hot_ranklist"
    assert request.params["count"] == "30"


def test_iqiyi_adapter_parses_hot_ranklist(fixtures_dir: Path):
    source = _source("iqiyi_hot_ranklist", "https://example.com/videoTab")
    payload = (fixtures_dir / "iqiyi" / "hot_ranklist.json").read_bytes()

    batch = IqiyiHotRanklistAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "生逢其时"
    assert first.external_id == "2510478291221401"
    assert first.url == "https://www.iqiyi.com/v_2bkbhy2gi2w.html"
    assert first.published_at is None
    assert first.metrics["show_date"] == "2026-9-3"
    assert first.metrics["tag"] == "家庭;年代;生活;当代;内地"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_iqiyi_adapter_rejects_error_code():
    source = _source("iqiyi_hot_ranklist", "https://example.com/videoTab")

    assert_parse_rejects(
        IqiyiHotRanklistAdapter(),
        source,
        b'{"code": -1}',
        match="Unexpected iQiyi code",
    )


def test_iqiyi_adapter_rejects_missing_video_blocks():
    source = _source("iqiyi_hot_ranklist", "https://example.com/videoTab")

    assert_parse_rejects(
        IqiyiHotRanklistAdapter(),
        source,
        b'{"code": 0, "items": [{"video": []}]}',
        match="no video blocks",
    )


def test_baidu_adapter_builds_request():
    source = _source("baidu_hot_search", "https://top.baidu.com/board?tab=realtime")

    request = BaiduHotSearchAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_baidu_adapter_parses_ranked_hot_search(fixtures_dir: Path):
    source = _source("baidu_hot_search", "https://example.com/board")
    payload = (fixtures_dir / "baidu" / "hot.html").read_bytes()

    batch = BaiduHotSearchAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "机顶盒将成为历史"
    assert first.external_id is None
    assert first.url.startswith("https://www.baidu.com/s?wd=")
    assert first.published_at is None
    assert all(
        candidate.title != "服务国家战略 造就栋梁之材" for candidate in batch.candidates
    )
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_baidu_adapter_rejects_page_without_payload():
    source = _source("baidu_hot_search", "https://example.com/board")

    assert_parse_rejects(
        BaiduHotSearchAdapter(),
        source,
        b"<html><body>no data</body></html>",
        match="s-data payload",
    )


def test_baidu_adapter_rejects_missing_cards():
    source = _source("baidu_hot_search", "https://example.com/board")

    assert_parse_rejects(
        BaiduHotSearchAdapter(),
        source,
        b'<!--s-data:{"data": {"cards": []}}-->',
        match="no cards",
    )


def test_ifeng_adapter_builds_request():
    source = _source("ifeng_hot", "https://www.ifeng.com/")

    request = IfengHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_ifeng_adapter_parses_hot_news(fixtures_dir: Path):
    source = _source("ifeng_hot", "https://example.com/")
    payload = (fixtures_dir / "ifeng" / "home.html").read_bytes()

    batch = IfengHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 4
    first = batch.candidates[0]
    assert first.title == "印巴军舰海上相撞，有三个疑点"
    assert first.external_id == "8wU9ULYnClY"
    assert first.published_at == datetime(2026, 9, 16, 23, 25, 23, tzinfo=UTC)
    assert first.raw_published_at == "2026-09-17 07:25:23"
    assert batch.candidates[1].published_at is None
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3, 4]


def test_ifeng_adapter_rejects_page_without_payload():
    source = _source("ifeng_hot", "https://example.com/")

    assert_parse_rejects(
        IfengHotAdapter(),
        source,
        b"<html><body>no data</body></html>",
        match="allData payload",
    )


def test_ifeng_adapter_rejects_missing_hot_news_list():
    source = _source("ifeng_hot", "https://example.com/")

    assert_parse_rejects(
        IfengHotAdapter(),
        source,
        b'<script>var allData = {"other": []};</script>',
        match="hotNews1 list",
    )


def test_v2ex_adapter_builds_ordered_requests():
    source = _source("v2ex_share", "https://www.v2ex.com")

    requests = V2exShareAdapter().build_requests(source)

    assert [request.url for request in requests] == [
        "https://www.v2ex.com/feed/create.json",
        "https://www.v2ex.com/feed/ideas.json",
        "https://www.v2ex.com/feed/programmer.json",
        "https://www.v2ex.com/feed/share.json",
    ]
    assert all(request.method == "GET" for request in requests)


def test_v2ex_adapter_combines_feeds_newest_first(fixtures_dir: Path):
    source = _source("v2ex_share", "https://www.v2ex.com")
    adapter = V2exShareAdapter()
    responses = tuple(
        response_for(source, (fixtures_dir / "v2ex" / f"{route}.json").read_bytes())
        for route in ("create", "ideas", "programmer", "share")
    )

    batch = adapter.parse_responses(source, responses)

    assert_batch_contract(batch)
    assert len(batch.candidates) == 12
    first = batch.candidates[0]
    assert first.external_id == "https://www.v2ex.com/t/1242709"
    assert first.title == "把走路时想到的东西直接变成 Obsidian 里的 Markdown 笔记"
    assert first.published_at == datetime(2026, 9, 17, 6, 47, 41, tzinfo=UTC)
    assert batch.candidates[1].external_id == "https://www.v2ex.com/t/1242707"
    assert [candidate.position for candidate in batch.candidates] == list(range(1, 13))


def test_v2ex_adapter_rejects_feed_without_items():
    source = _source("v2ex_share", "https://www.v2ex.com")

    with pytest.raises(ValueError, match="items list"):
        V2exShareAdapter().parse_responses(
            source,
            (response_for(source, b'{"version": "https://jsonfeed.org/version/1"}'),),
        )


def test_v2ex_adapter_rejects_invalid_json_feed():
    source = _source("v2ex_share", "https://www.v2ex.com")

    with pytest.raises(ValueError, match="Invalid V2EX JSON"):
        V2exShareAdapter().parse_responses(
            source,
            (response_for(source, b"<html>"),),
        )


def test_chongbuluo_hot_adapter_builds_request():
    source = _source(
        "chongbuluo_hot",
        "https://www.chongbuluo.com/forum.php?mod=guide&view=hot",
    )

    request = ChongbuluoHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_chongbuluo_hot_adapter_parses_listing(fixtures_dir: Path):
    source = _source("chongbuluo_hot", "https://example.com/guide")
    payload = (fixtures_dir / "chongbuluo-hot" / "listing.html").read_bytes()

    batch = ChongbuluoHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "能不能把你们在社会上悟到最贵的一句话送给我"
    assert first.external_id == "24840"
    assert first.published_at is None
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_chongbuluo_hot_adapter_rejects_page_without_threads():
    source = _source("chongbuluo_hot", "https://example.com/guide")

    assert_parse_rejects(
        ChongbuluoHotAdapter(),
        source,
        b"<html><body></body></html>",
        match="hot threads",
    )


def test_hupu_adapter_builds_request():
    source = _source("hupu_hot", "https://bbs.hupu.com/topic-daily-hot")

    request = HupuHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_hupu_adapter_parses_listing(fixtures_dir: Path):
    source = _source("hupu_hot", "https://example.com/topic-daily-hot")
    payload = (fixtures_dir / "hupu" / "listing.html").read_bytes()

    batch = HupuHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title.startswith("男朋友买了苹果18，我想借他的旧手机用一段时间")
    assert first.external_id == "642445808"
    assert first.published_at is None
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_hupu_adapter_rejects_page_without_threads():
    source = _source("hupu_hot", "https://example.com/topic-daily-hot")

    assert_parse_rejects(
        HupuHotAdapter(),
        source,
        b"<html><body></body></html>",
        match="hot threads",
    )


def test_sputnik_news_adapter_builds_request():
    source = _source("sputnik_news", "https://sputniknews.cn/services/widget/lenta/")

    request = SputnikNewsAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_sputnik_news_adapter_parses_listing(fixtures_dir: Path):
    source = _source("sputnik_news", "https://example.com/lenta/")
    payload = (fixtures_dir / "sputnik-news" / "listing.html").read_bytes()

    batch = SputnikNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "NASA发现百年一遇月球新陨石坑"
    assert first.external_id == "1073292072"
    assert first.published_at == datetime(2026, 9, 17, 6, 50, 9, tzinfo=UTC)
    assert first.raw_published_at is not None
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_sputnik_news_adapter_rejects_page_without_items():
    source = _source("sputnik_news", "https://example.com/lenta/")

    assert_parse_rejects(
        SputnikNewsAdapter(),
        source,
        b"<html><body></body></html>",
        match="news items",
    )


def test_fastbull_express_adapter_builds_request():
    source = _source(
        "fastbull_express",
        "https://www.fastbull.com/cn/express-news",
    )

    request = FastbullExpressAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_fastbull_express_adapter_parses_listing(fixtures_dir: Path):
    source = _source("fastbull_express", "https://example.com/cn/express-news")
    payload = (fixtures_dir / "fastbull-express" / "listing.html").read_bytes()

    batch = FastbullExpressAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "纯苯2610盘中触及跌停，跌幅扩大至5.99%，价格报8601元/吨。"
    assert first.external_id == "4282077_1_1"
    assert first.published_at == datetime(2026, 9, 17, 6, 58, 9, 351000, tzinfo=UTC)
    assert first.raw_published_at is not None
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_fastbull_express_adapter_rejects_page_without_items():
    source = _source("fastbull_express", "https://example.com/cn/express-news")

    assert_parse_rejects(
        FastbullExpressAdapter(),
        source,
        b"<html><body></body></html>",
        match="express news",
    )


def test_fastbull_news_adapter_builds_request():
    source = _source("fastbull_news", "https://www.fastbull.com/cn/news")

    request = FastbullNewsAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_fastbull_news_adapter_parses_listing(fixtures_dir: Path):
    source = _source("fastbull_news", "https://example.com/cn/news")
    payload = (fixtures_dir / "fastbull-news" / "listing.html").read_bytes()

    batch = FastbullNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "美联储重启加息，美伊局势与通胀压力交织"
    assert first.external_id == "4386613_1"
    assert first.published_at == datetime(2026, 9, 17, 1, 44, 48, 912000, tzinfo=UTC)
    assert first.raw_published_at is not None
    assert [candidate.external_id for candidate in batch.candidates] == [
        "4386613_1",
        "4386586_1",
        "4386572_1",
    ]
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]
    assert all("/cn/newsdetail/" not in candidate.url for candidate in batch.candidates)
    assert all(
        candidate.title != "盈亏比高为什么仍会亏损？胜率、期望值与仓位计算"
        for candidate in batch.candidates
    )


def test_fastbull_news_adapter_rejects_page_without_items():
    source = _source("fastbull_news", "https://example.com/cn/news")

    assert_parse_rejects(
        FastbullNewsAdapter(),
        source,
        b"<html><body></body></html>",
        match="news items",
    )


def test_zaobao_adapter_builds_request():
    source = _source("zaobao_realtime", "https://www.zaochenbao.com/realtime/")

    request = ZaobaoRealtimeAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_zaobao_adapter_parses_gb2312_listing(fixtures_dir: Path):
    source = _source("zaobao_realtime", "https://example.com/realtime/")
    payload = (fixtures_dir / "zaobao" / "listing.html").read_bytes()

    batch = ZaobaoRealtimeAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "辽宁绥中多名学生呕吐 初步判断食堂烹饪不当"
    assert first.external_id == "1781279"
    assert first.published_at == datetime(2026, 9, 17, 6, 15, 46, tzinfo=UTC)
    assert first.raw_published_at == "2026-09-17 14:15:46"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_zaobao_adapter_rejects_page_without_items():
    source = _source("zaobao_realtime", "https://example.com/realtime/")

    assert_parse_rejects(
        ZaobaoRealtimeAdapter(),
        source,
        b"<html><body></body></html>",
        match="news items",
    )


def test_gelonghui_adapter_builds_request():
    source = _source("gelonghui_news", "https://www.gelonghui.com/news/")

    request = GelonghuiNewsAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_gelonghui_adapter_parses_listing(fixtures_dir: Path):
    source = _source("gelonghui_news", "https://example.com/news/")
    payload = (fixtures_dir / "gelonghui" / "listing.html").read_bytes()

    batch = GelonghuiNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "ETF洞察|纳指科技ETF溢价26.08%，多只纳指ETF超溢价10%"
    assert first.external_id == "5313638"
    assert first.url == "https://www.gelonghui.com/news/5313638"
    assert first.published_at is not None
    assert first.raw_published_at == "2分钟前"
    assert first.metrics["category"] == "A股异动"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_gelonghui_adapter_rejects_page_without_items():
    source = _source("gelonghui_news", "https://example.com/news/")

    assert_parse_rejects(
        GelonghuiNewsAdapter(),
        source,
        b"<html><body></body></html>",
        match="news items",
    )


def test_kr36_adapter_builds_request():
    source = _source("kr36_quick", "https://www.36kr.com/newsflashes")

    request = Kr36QuickAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_kr36_adapter_parses_listing(fixtures_dir: Path):
    source = _source("kr36_quick", "https://example.com/newsflashes")
    payload = (fixtures_dir / "36kr-quick" / "listing.html").read_bytes()

    batch = Kr36QuickAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "谷歌与瑞典钢铁企业Stegra合作，推动近零排放钢厂投产"
    assert first.external_id == "3987159841258243"
    assert first.url == "https://www.36kr.com/newsflashes/3987159841258243"
    assert first.published_at is not None
    assert first.raw_published_at == "28秒前"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_kr36_adapter_rejects_page_without_flashes():
    source = _source("kr36_quick", "https://example.com/newsflashes")

    assert_parse_rejects(
        Kr36QuickAdapter(),
        source,
        b"<html><body></body></html>",
        match="newsflashes",
    )


def test_github_trending_adapter_builds_request():
    source = _source(
        "github_trending",
        "https://github.com/trending?spoken_language_code=",
    )

    request = GithubTrendingAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_github_trending_adapter_parses_listing(fixtures_dir: Path):
    source = _source("github_trending", "https://example.com/trending")
    payload = (fixtures_dir / "github" / "listing.html").read_bytes()

    batch = GithubTrendingAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "alibaba /open-code-review"
    assert first.external_id is None
    assert first.url == "https://github.com/alibaba/open-code-review"
    assert first.published_at is None
    assert first.metrics["stars"] == "32,855"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_github_trending_adapter_rejects_page_without_repositories():
    source = _source("github_trending", "https://example.com/trending")

    assert_parse_rejects(
        GithubTrendingAdapter(),
        source,
        b"<html><body></body></html>",
        match="trending repositories",
    )


def test_steam_adapter_builds_request():
    source = _source("steam_players", "https://store.steampowered.com/stats/stats/")

    request = SteamPlayersAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_steam_adapter_parses_player_ranking(fixtures_dir: Path):
    source = _source("steam_players", "https://example.com/stats/stats/")
    payload = (fixtures_dir / "steam" / "listing.html").read_bytes()

    batch = SteamPlayersAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "Counter-Strike 2"
    assert first.external_id == "730"
    assert first.url == "https://store.steampowered.com/app/730/CounterStrike_2/"
    assert first.published_at is None
    assert first.metrics["current_players"] == "528,852"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_steam_adapter_rejects_page_without_rows():
    source = _source("steam_players", "https://example.com/stats/stats/")

    assert_parse_rejects(
        SteamPlayersAdapter(),
        source,
        b"<html><body></body></html>",
        match="player rows",
    )


def test_kuaishou_adapter_builds_request():
    source = _source("kuaishou_hot", "https://www.kuaishou.com/?isHome=1")

    request = KuaishouHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["User-Agent"].startswith("Mozilla/5.0")
    assert request.headers["Accept"].startswith("text/html")


def test_kuaishou_adapter_parses_hot_rank(fixtures_dir: Path):
    source = _source("kuaishou_hot", "https://example.com/?isHome=1")
    payload = (fixtures_dir / "kuaishou" / "home.html").read_bytes()

    batch = KuaishouHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "美联储宣布加息25个基点"
    assert first.external_id == "美联储宣布加息25个基点"
    assert first.url == (
        "https://www.kuaishou.com/search/video"
        "?searchKey=%E7%BE%8E%E8%81%94%E5%82%A8%E5%AE%A3%E5%B8%83%E5%8A%A0%E6%81%AF25%E4%B8%AA%E5%9F%BA%E7%82%B9"
    )
    assert first.published_at is None
    assert all(
        candidate.title != "现代化产业体系骨干是先进制造业"
        for candidate in batch.candidates
    )
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_kuaishou_adapter_rejects_page_without_state():
    source = _source("kuaishou_hot", "https://example.com/?isHome=1")

    assert_parse_rejects(
        KuaishouHotAdapter(),
        source,
        b"<html><body></body></html>",
        match="APOLLO_STATE",
    )


def test_cls_signed_params_are_stable():
    assert signed_params() == {
        "appName": "CailianpressWeb",
        "os": "web",
        "sv": "7.7.5",
        "sign": "9c11221af4f6b47b253098a8b9957b8f",
    }


def test_cls_telegraph_adapter_builds_signed_request():
    source = _source("cls_telegraph", "https://www.cls.cn/v1/roll/get_roll_list")

    request = ClsTelegraphAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Referer"] == "https://www.cls.cn/telegraph"
    assert request.params["refresh_type"] == "1"
    assert request.params["rn"] == "30"
    assert request.params["last_time"].isdigit()
    assert len(request.params["sign"]) == 32


def test_cls_telegraph_adapter_parses_roll_list(fixtures_dir: Path):
    source = _source("cls_telegraph", "https://example.com/get_roll_list")
    payload = (fixtures_dir / "cls" / "telegraph.json").read_bytes()

    batch = ClsTelegraphAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "内蒙古满洲里公路口岸进出口贸易值创新高"
    assert first.external_id == "2486084"
    assert first.url == "https://www.cls.cn/detail/2486084"
    assert first.published_at == datetime.fromtimestamp(1789629612, tz=UTC)
    assert first.raw_published_at == "1789629612"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_cls_telegraph_adapter_skips_ads():
    source = _source("cls_telegraph", "https://example.com/get_roll_list")
    payload = json.dumps(
        {
            "data": {
                "roll_data": [
                    {
                        "id": 1,
                        "title": "广告",
                        "brief": "广告",
                        "ctime": 1789629000,
                        "is_ad": 1,
                    },
                    {
                        "id": 2,
                        "title": "正文",
                        "brief": "",
                        "ctime": 1789628000,
                        "is_ad": 0,
                    },
                ]
            }
        }
    ).encode()

    batch = ClsTelegraphAdapter().parse(source, response_for(source, payload))

    assert [candidate.external_id for candidate in batch.candidates] == ["2"]
    assert batch.candidates[0].position == 1


def test_cls_depth_adapter_parses_depth_list(fixtures_dir: Path):
    source = _source("cls_depth", "https://example.com/depth")
    payload = (fixtures_dir / "cls" / "depth.json").read_bytes()

    batch = ClsDepthAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "2486007"
    assert first.title.startswith("无人矿卡告别")
    assert first.url == "https://www.cls.cn/detail/2486007"
    assert first.published_at is not None
    assert first.raw_published_at == "1789626989"


def test_cls_hot_adapter_parses_hot_list(fixtures_dir: Path):
    source = _source("cls_hot", "https://example.com/hot/list")
    payload = (fixtures_dir / "cls" / "hot.json").read_bytes()

    batch = ClsHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "2485626"
    assert first.title.startswith("【早报】时隔3年多，美联储重启加息")
    assert first.published_at == datetime.fromtimestamp(1789599600, tz=UTC)


def test_cls_telegraph_adapter_rejects_missing_data():
    source = _source("cls_telegraph", "https://example.com/get_roll_list")

    assert_parse_rejects(
        ClsTelegraphAdapter(),
        source,
        b'{"error": 0}',
        match="data object",
    )


def test_cls_hot_adapter_rejects_missing_data_list():
    source = _source("cls_hot", "https://example.com/hot/list")

    assert_parse_rejects(
        ClsHotAdapter(),
        source,
        b'{"data": {}}',
        match="data list",
    )


def test_coolapk_token_vector():
    assert app_token(now=1789629000, device="abc123") == (
        "38ed6fd2b3893a8a79200a35fa27803cabc1230x6aab9248"
    )


def test_coolapk_adapter_builds_signed_request():
    source = _source("coolapk_hot", "https://api.coolapk.com/v6/page/dataList")

    request = CoolapkHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["X-App-Id"] == "com.coolapk.market"
    assert request.headers["X-App-Version"] == "11.0"
    assert request.headers["User-Agent"].startswith("Dalvik/")
    assert len(request.headers["X-App-Token"]) > 32


def test_coolapk_adapter_parses_hot_feed(fixtures_dir: Path):
    source = _source("coolapk_hot", "https://example.com/dataList")
    payload = (fixtures_dir / "coolapk" / "hot.json").read_bytes()

    batch = CoolapkHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "73792743"
    assert first.title == (
        "都别争了，这下真大结局了，极客湾好久没用这么炸裂的标题形容一款处理器了[疑问][疑问]"
    )
    assert first.url == "https://www.coolapk.com/feed/73792743"
    assert first.published_at == datetime.fromtimestamp(1789566319, tz=UTC)
    assert first.raw_published_at == "1789566319"
    assert first.metrics["heat"] == "255.7万热度 3103讨论"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_coolapk_adapter_skips_entries_without_id():
    source = _source("coolapk_hot", "https://example.com/dataList")
    payload = json.dumps(
        {
            "data": [
                {"id": "", "editor_title": "跳过", "url": "/feed/0"},
                {"id": "1", "editor_title": "保留", "url": "/feed/1"},
            ]
        }
    ).encode()

    batch = CoolapkHotAdapter().parse(source, response_for(source, payload))

    assert [candidate.external_id for candidate in batch.candidates] == ["1"]


def test_coolapk_adapter_rejects_missing_data_list():
    source = _source("coolapk_hot", "https://example.com/dataList")

    assert_parse_rejects(
        CoolapkHotAdapter(),
        source,
        b'{"data": {}}',
        match="data list",
    )


def test_douyin_adapter_steps_through_cookie_bootstrap(fixtures_dir: Path):
    source = _source("douyin_hot", "https://www.douyin.com/")
    payload = (fixtures_dir / "douyin" / "hot.json").read_bytes()
    adapter = DouyinHotAdapter()

    first = adapter.first_step(source)
    assert isinstance(first, ContinueStep)
    assert first.request.url == "https://login.douyin.com/"
    assert first.request.headers["User-Agent"].startswith("Mozilla/5.0")

    landing = response_for(
        source,
        b"",
        cookies={"passport_csrf_token": "test-token"},
    )
    second = adapter.next_step(source, landing, first.context)
    assert isinstance(second, ContinueStep)
    assert second.request.url.startswith(
        "https://www.douyin.com/aweme/v1/web/hot/search/list/"
    )
    assert second.request.headers["Cookie"] == "passport_csrf_token=test-token"
    assert second.request.headers["User-Agent"].startswith("Mozilla/5.0")

    final = adapter.next_step(source, response_for(source, payload), second.context)
    assert isinstance(final, CompleteStep)
    batch = final.batch
    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first_candidate = batch.candidates[0]
    assert first_candidate.title == "美联储宣布加息25个基点"
    assert first_candidate.external_id == "2653116"
    assert first_candidate.url == "https://www.douyin.com/hot/2653116"
    assert first_candidate.published_at is None
    assert first_candidate.metrics["hot_value"] == 11442242
    assert first_candidate.metrics["event_time"] == 1789599892
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_douyin_adapter_rejects_missing_word_list():
    source = _source("douyin_hot", "https://www.douyin.com/")

    with pytest.raises(ValueError, match="word_list"):
        DouyinHotAdapter().next_step(
            source,
            response_for(source, b'{"data": {}}'),
            {"stage": "hot"},
        )


def test_xueqiu_adapter_steps_through_cookie_bootstrap(fixtures_dir: Path):
    source = _source("xueqiu_hotstock", "https://xueqiu.com/")
    payload = (fixtures_dir / "xueqiu" / "hot.json").read_bytes()
    adapter = XueqiuHotstockAdapter()

    first = adapter.first_step(source)
    assert isinstance(first, ContinueStep)
    assert first.request.url == "https://xueqiu.com/hq"

    landing = response_for(source, b"", cookies={"xq_a_token": "test-token"})
    second = adapter.next_step(source, landing, first.context)
    assert isinstance(second, ContinueStep)
    assert second.request.url == (
        "https://stock.xueqiu.com/v5/stock/hot_stock/list.json"
    )
    assert second.request.params == {"_type": "10", "type": "10", "size": "30"}
    assert second.request.headers["Cookie"] == "xq_a_token=test-token"

    final = adapter.next_step(source, response_for(source, payload), second.context)
    assert isinstance(final, CompleteStep)
    batch = final.batch
    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first_candidate = batch.candidates[0]
    assert first_candidate.external_id == "SH601872"
    assert first_candidate.title == "招商轮船"
    assert first_candidate.url == "https://xueqiu.com/s/SH601872"
    assert first_candidate.published_at is None
    assert first_candidate.metrics["percent"] == 10.02
    assert first_candidate.metrics["exchange"] == "SH"


def test_xueqiu_adapter_skips_ad_entries():
    source = _source("xueqiu_hotstock", "https://xueqiu.com/")
    payload = json.dumps(
        {
            "data": {
                "items": [
                    {"code": "SZ000001", "name": "广告", "ad": 1},
                    {"code": "SZ000002", "name": "正文", "ad": 0},
                ]
            }
        }
    ).encode()

    final = XueqiuHotstockAdapter().next_step(
        source,
        response_for(source, payload),
        {"stage": "hot"},
    )

    assert isinstance(final, CompleteStep)
    assert [candidate.external_id for candidate in final.batch.candidates] == [
        "SZ000002"
    ]


def test_wallstreetcn_quick_adapter_builds_request():
    source = _source(
        "wallstreetcn_quick", "https://api-one.wallstcn.com/apiv1/content/lives"
    )

    request = WallstreetcnQuickAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {"channel": "global-channel", "limit": "30"}
    assert request.headers["Accept"].startswith("application/json")


def test_wallstreetcn_quick_adapter_parses_live_fixture(fixtures_dir: Path):
    source = _source(
        "wallstreetcn_quick", "https://api-one.wallstcn.com/apiv1/content/lives"
    )
    payload = (fixtures_dir / "wallstreetcn" / "live.json").read_bytes()

    batch = WallstreetcnQuickAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "3166704"
    assert first.title == "外交部：“一国两制”下香港国际金融、航运、贸易中心地位持续巩固"
    assert first.url == "https://wallstreetcn.com/livenews/3166704"
    assert first.published_at == datetime.fromtimestamp(1789630364, tz=UTC)
    assert first.raw_published_at == "1789630364"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_wallstreetcn_quick_adapter_falls_back_to_content_text():
    source = _source(
        "wallstreetcn_quick", "https://api-one.wallstcn.com/apiv1/content/lives"
    )
    payload = json.dumps(
        {
            "data": {
                "items": [
                    {
                        "id": 1,
                        "title": "",
                        "content_text": "快讯正文",
                        "display_time": 1789630364,
                        "uri": "https://wallstreetcn.com/livenews/1",
                        "type": "live",
                    }
                ]
            }
        },
        ensure_ascii=False,
    ).encode()

    batch = WallstreetcnQuickAdapter().parse(source, response_for(source, payload))

    assert [candidate.title for candidate in batch.candidates] == ["快讯正文"]


def test_wallstreetcn_quick_adapter_rejects_missing_items_list():
    source = _source(
        "wallstreetcn_quick", "https://api-one.wallstcn.com/apiv1/content/lives"
    )

    assert_parse_rejects(
        WallstreetcnQuickAdapter(),
        source,
        b'{"data": {}}',
        match="items list",
    )


def test_wallstreetcn_quick_adapter_rejects_invalid_json():
    source = _source(
        "wallstreetcn_quick", "https://api-one.wallstcn.com/apiv1/content/lives"
    )

    assert_parse_rejects(
        WallstreetcnQuickAdapter(),
        source,
        b"<html>",
        match="Invalid WallstreetCN live JSON",
    )


def test_wallstreetcn_news_adapter_builds_request():
    source = _source(
        "wallstreetcn_news",
        "https://api-one.wallstcn.com/apiv1/content/information-flow",
    )

    request = WallstreetcnNewsAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {
        "channel": "global-channel",
        "accept": "article",
        "limit": "30",
    }


def test_wallstreetcn_news_adapter_parses_information_flow(fixtures_dir: Path):
    source = _source(
        "wallstreetcn_news",
        "https://api-one.wallstcn.com/apiv1/content/information-flow",
    )
    payload = (fixtures_dir / "wallstreetcn" / "news.json").read_bytes()

    batch = WallstreetcnNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert [candidate.external_id for candidate in batch.candidates] == [
        "3781977",
        "3781978",
        "41959865",
    ]
    first = batch.candidates[0]
    assert first.published_at == datetime.fromtimestamp(1789630016, tz=UTC)
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_wallstreetcn_news_adapter_filters_containers_and_falls_back():
    source = _source(
        "wallstreetcn_news",
        "https://api-one.wallstcn.com/apiv1/content/information-flow",
    )
    payload = json.dumps(
        {
            "data": {
                "items": [
                    {
                        "resource_type": "theme",
                        "resource": {
                            "id": 1,
                            "title": "专题",
                            "content_short": "专题",
                            "display_time": 1789630000,
                            "uri": "https://wallstreetcn.com/themes/1",
                            "type": "theme",
                        },
                    },
                    {
                        "resource_type": "article",
                        "resource": {
                            "id": 2,
                            "title": "",
                            "content_short": "摘要标题",
                            "display_time": 1789630016,
                            "uri": "https://wallstreetcn.com/articles/2",
                            "type": None,
                        },
                    },
                ]
            }
        },
        ensure_ascii=False,
    ).encode()

    batch = WallstreetcnNewsAdapter().parse(source, response_for(source, payload))

    assert [candidate.external_id for candidate in batch.candidates] == ["2"]
    assert batch.candidates[0].title == "摘要标题"
    assert batch.candidates[0].position == 1


def test_wallstreetcn_news_adapter_rejects_missing_items_list():
    source = _source(
        "wallstreetcn_news",
        "https://api-one.wallstcn.com/apiv1/content/information-flow",
    )

    assert_parse_rejects(
        WallstreetcnNewsAdapter(),
        source,
        b'{"data": {}}',
        match="items list",
    )


def test_wallstreetcn_hot_adapter_builds_request():
    source = _source(
        "wallstreetcn_hot",
        "https://api-one.wallstcn.com/apiv1/content/articles/hot",
    )

    request = WallstreetcnHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {"period": "all"}


def test_wallstreetcn_hot_adapter_parses_day_items(fixtures_dir: Path):
    source = _source(
        "wallstreetcn_hot",
        "https://api-one.wallstcn.com/apiv1/content/articles/hot",
    )
    payload = (fixtures_dir / "wallstreetcn" / "hot.json").read_bytes()

    batch = WallstreetcnHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert [candidate.external_id for candidate in batch.candidates] == [
        "3781921",
        "3781919",
        "3781871",
    ]
    first = batch.candidates[0]
    assert first.title.startswith("沃什：加息展现FOMC内部坚定一致")
    assert first.published_at == datetime.fromtimestamp(1789595070, tz=UTC)
    assert first.raw_published_at == "1789595070"
    assert first.metrics["pageviews"] == 369990
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_wallstreetcn_hot_adapter_ignores_week_items():
    source = _source(
        "wallstreetcn_hot",
        "https://api-one.wallstcn.com/apiv1/content/articles/hot",
    )
    payload = json.dumps(
        {
            "data": {
                "day_items": [
                    {
                        "id": 1,
                        "title": "日榜",
                        "display_time": 1789595070,
                        "uri": "https://wallstreetcn.com/articles/1",
                        "pageviews": 10,
                    }
                ],
                "week_items": [
                    {
                        "id": 2,
                        "title": "周榜",
                        "display_time": 1789500000,
                        "uri": "https://wallstreetcn.com/articles/2",
                        "pageviews": 99,
                    }
                ],
            }
        },
        ensure_ascii=False,
    ).encode()

    batch = WallstreetcnHotAdapter().parse(source, response_for(source, payload))

    assert [candidate.external_id for candidate in batch.candidates] == ["1"]


def test_wallstreetcn_hot_adapter_rejects_missing_day_items():
    source = _source(
        "wallstreetcn_hot",
        "https://api-one.wallstcn.com/apiv1/content/articles/hot",
    )

    assert_parse_rejects(
        WallstreetcnHotAdapter(),
        source,
        b'{"data": {}}',
        match="day_items list",
    )


def test_cankaoxiaoxi_adapter_builds_three_channel_requests():
    source = _source("cankaoxiaoxi_news", "http://china.cankaoxiaoxi.com")

    requests = CankaoxiaoxiNewsAdapter().build_requests(source)

    assert [request.url for request in requests] == [
        "http://china.cankaoxiaoxi.com/json/channel/zhongguo/list.json",
        "http://china.cankaoxiaoxi.com/json/channel/guandian/list.json",
        "http://china.cankaoxiaoxi.com/json/channel/gj/list.json",
    ]
    assert all(request.method == "GET" for request in requests)


def test_cankaoxiaoxi_adapter_combines_channels_newest_first(fixtures_dir: Path):
    source = _source("cankaoxiaoxi_news", "http://china.cankaoxiaoxi.com")
    responses = tuple(
        response_for(
            source,
            (fixtures_dir / "cankaoxiaoxi" / f"{channel}.json").read_bytes(),
        )
        for channel in ("zhongguo", "guandian", "gj")
    )

    batch = CankaoxiaoxiNewsAdapter().parse_responses(source, responses)

    assert_batch_contract(batch)
    assert len(batch.candidates) == 9
    first = batch.candidates[0]
    assert first.external_id == "c401b683234b436688cd4ee679df13fd"
    assert first.title == "法媒分析：“再移民”成欧洲极右翼核心主张"
    assert first.published_at == datetime(2026, 9, 17, 7, 4, 29, tzinfo=UTC)
    assert first.raw_published_at == "2026-09-17 15:04:29"
    assert [candidate.position for candidate in batch.candidates] == list(range(1, 10))


def test_cankaoxiaoxi_adapter_respects_max_items(fixtures_dir: Path):
    source = _source(
        "cankaoxiaoxi_news",
        "http://china.cankaoxiaoxi.com",
        max_items=2,
    )
    responses = tuple(
        response_for(
            source,
            (fixtures_dir / "cankaoxiaoxi" / f"{channel}.json").read_bytes(),
        )
        for channel in ("zhongguo", "guandian", "gj")
    )

    batch = CankaoxiaoxiNewsAdapter().parse_responses(source, responses)

    assert len(batch.candidates) == 2
    assert [candidate.position for candidate in batch.candidates] == [1, 2]


def test_cankaoxiaoxi_adapter_rejects_missing_list():
    source = _source("cankaoxiaoxi_news", "http://china.cankaoxiaoxi.com")

    with pytest.raises(ValueError, match="does not contain a list"):
        CankaoxiaoxiNewsAdapter().parse_responses(
            source,
            (response_for(source, b'{"data": []}'),),
        )


def test_cankaoxiaoxi_adapter_rejects_invalid_json():
    source = _source("cankaoxiaoxi_news", "http://china.cankaoxiaoxi.com")

    with pytest.raises(ValueError, match="Invalid Cankaoxiaoxi JSON"):
        CankaoxiaoxiNewsAdapter().parse_responses(
            source,
            (response_for(source, b"<html>"),),
        )


def test_tieba_adapter_builds_request():
    source = _source("tieba_hot", "https://tieba.baidu.com/hottopic/browse/topicList")

    request = TiebaHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("application/json")


def test_tieba_adapter_parses_topic_list(fixtures_dir: Path):
    source = _source("tieba_hot", "https://tieba.baidu.com/hottopic/browse/topicList")
    payload = (fixtures_dir / "tieba" / "topics.json").read_bytes()

    batch = TiebaHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "28364580"
    assert first.title == "草台班子!亚运代表队被困机场"
    assert first.url.startswith(
        "https://tieba.baidu.com/hottopic/browse/hottopic?"
        "topic_id=28364580&topic_name="
    )
    assert "&amp;" not in first.url
    assert first.published_at == datetime.fromtimestamp(1789610507, tz=UTC)
    assert first.raw_published_at == "1789610507"
    assert first.metrics["discuss_count"] == 2970420
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_tieba_adapter_rejects_missing_topic_list():
    source = _source("tieba_hot", "https://tieba.baidu.com/hottopic/browse/topicList")

    assert_parse_rejects(
        TiebaHotAdapter(),
        source,
        b'{"data": {"bang_topic": {}}}',
        match="topic_list",
    )


def test_tieba_adapter_rejects_missing_data():
    source = _source("tieba_hot", "https://tieba.baidu.com/hottopic/browse/topicList")

    assert_parse_rejects(
        TiebaHotAdapter(),
        source,
        b'{"errno": 0}',
        match="data object",
    )


def test_weibo_adapter_builds_request():
    source = _source("weibo_hot", "https://weibo.com/ajax/side/hotSearch")

    request = WeiboHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Referer"] == "https://weibo.com/"
    assert request.headers["Accept"].startswith("application/json")


def test_weibo_adapter_parses_hot_search(fixtures_dir: Path):
    source = _source("weibo_hot", "https://weibo.com/ajax/side/hotSearch")
    payload = (fixtures_dir / "weibo" / "hot_search.json").read_bytes()

    batch = WeiboHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 4
    first = batch.candidates[0]
    assert first.external_id == "如果你出生于1992年至2003年之间"
    assert first.title == "如果你出生于1992年至2003年之间"
    assert first.url.startswith("https://s.weibo.com/weibo?q=%23")
    assert first.url.endswith("%23")
    assert "#" not in first.url
    assert first.published_at is None
    assert first.metrics["heat"] == 1644492
    assert first.metrics["label"] == "新"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3, 4]
    assert all(
        candidate.external_id != "还是哥伦比亚会玩" for candidate in batch.candidates
    )


def test_weibo_adapter_encodes_non_topic_words():
    source = _source("weibo_hot", "https://weibo.com/ajax/side/hotSearch")
    payload = json.dumps(
        {
            "ok": 1,
            "data": {
                "realtime": [
                    {
                        "word": "亚运会 离谱",
                        "word_scheme": "亚运会 离谱",
                        "num": 1066050,
                        "realpos": 2,
                        "label_name": None,
                    }
                ]
            },
        },
        ensure_ascii=False,
    ).encode()

    batch = WeiboHotAdapter().parse(source, response_for(source, payload))

    assert batch.candidates[0].url == (
        "https://s.weibo.com/weibo?q=%E4%BA%9A%E8%BF%90%E4%BC%9A%20%E7%A6%BB%E8%B0%B1"
    )
    assert "label" not in batch.candidates[0].metrics


def test_weibo_adapter_rejects_failed_status():
    source = _source("weibo_hot", "https://weibo.com/ajax/side/hotSearch")

    assert_parse_rejects(
        WeiboHotAdapter(),
        source,
        b'{"ok": 0, "data": {}}',
        match="not ok",
    )


def test_weibo_adapter_rejects_missing_realtime_list():
    source = _source("weibo_hot", "https://weibo.com/ajax/side/hotSearch")

    assert_parse_rejects(
        WeiboHotAdapter(),
        source,
        b'{"ok": 1, "data": {}}',
        match="realtime list",
    )


def test_weibo_adapter_rejects_invalid_json():
    source = _source("weibo_hot", "https://weibo.com/ajax/side/hotSearch")

    assert_parse_rejects(
        WeiboHotAdapter(),
        source,
        b"<html>passport</html>",
        match="Invalid Weibo JSON",
    )


def test_sspai_adapter_builds_request():
    source = _source("sspai_hot", "https://sspai.com/api/v1/article/tag/page/get")

    request = SspaiHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {
        "limit": "30",
        "offset": "0",
        "tag": "热门文章",
        "released": "false",
    }
    assert "created_at" not in request.params


def test_sspai_adapter_parses_hot_articles(fixtures_dir: Path):
    source = _source("sspai_hot", "https://sspai.com/api/v1/article/tag/page/get")
    payload = (fixtures_dir / "sspai" / "hot.json").read_bytes()

    batch = SspaiHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "114461"
    assert first.title.startswith("与 AI 搏斗失败后重新开始找工作")
    assert first.url == "https://sspai.com/post/114461"
    assert first.published_at == datetime.fromtimestamp(1789196400, tz=UTC)
    assert first.raw_published_at == "1789196400"
    assert first.metrics["like_count"] == 114
    assert first.metrics["comment_count"] == 20
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_sspai_adapter_rejects_api_error():
    source = _source("sspai_hot", "https://sspai.com/api/v1/article/tag/page/get")

    assert_parse_rejects(
        SspaiHotAdapter(),
        source,
        b'{"error": 1001, "msg": "bad tag"}',
        match="SSPAI API error",
    )


def test_sspai_adapter_rejects_missing_data_list():
    source = _source("sspai_hot", "https://sspai.com/api/v1/article/tag/page/get")

    assert_parse_rejects(
        SspaiHotAdapter(),
        source,
        b'{"error": 0}',
        match="data list",
    )


def test_sspai_adapter_rejects_invalid_json():
    source = _source("sspai_hot", "https://sspai.com/api/v1/article/tag/page/get")

    assert_parse_rejects(
        SspaiHotAdapter(),
        source,
        b"<html>",
        match="Invalid SSPAI JSON",
    )


def test_hackernews_adapter_builds_request():
    source = _source("hackernews_hot", "https://news.ycombinator.com/")

    request = HackerNewsHotAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_hackernews_adapter_parses_front_page(fixtures_dir: Path):
    source = _source("hackernews_hot", "https://news.ycombinator.com/")
    payload = (fixtures_dir / "hackernews" / "frontpage.html").read_bytes()

    batch = HackerNewsHotAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "49724881"
    assert first.title == "Nvidia announces native GPU programming in Rust"
    assert first.url == "https://news.ycombinator.com/item?id=49724881"
    assert first.published_at == datetime(2026, 9, 16, 11, 15, 53, tzinfo=UTC)
    assert first.raw_published_at == "2026-09-16T11:15:53"
    assert first.metrics["points"] == 615
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_hackernews_adapter_keeps_items_without_score():
    source = _source("hackernews_hot", "https://news.ycombinator.com/")
    payload = (
        b"<html><body><table>"
        b'<tr class="athing" id="1"><td class="title">'
        b'<span class="titleline"><a href="https://example.com/">Fresh item</a>'
        b"</span></td></tr>"
        b'<tr><td class="subtext"><span class="age" '
        b'title="2026-09-17T04:00:00">1 hour ago</span></td></tr>'
        b"</table></body></html>"
    )

    batch = HackerNewsHotAdapter().parse(source, response_for(source, payload))

    assert len(batch.candidates) == 1
    candidate = batch.candidates[0]
    assert candidate.external_id == "1"
    assert "points" not in candidate.metrics
    assert candidate.published_at == datetime(2026, 9, 17, 4, 0, tzinfo=UTC)


def test_hackernews_adapter_rejects_page_without_stories():
    source = _source("hackernews_hot", "https://news.ycombinator.com/")

    assert_parse_rejects(
        HackerNewsHotAdapter(),
        source,
        b"<html><body>Service unavailable</body></html>",
        match="does not contain any stories",
    )


def test_kaopu_adapter_builds_request_with_mac_user_agent():
    source = _source("kaopu_news", "https://kaopu.news/")

    request = KaopuNewsAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")
    assert request.headers["User-Agent"].startswith(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    )


def test_kaopu_adapter_parses_story_listing(fixtures_dir: Path):
    source = _source("kaopu_news", "https://kaopu.news/")
    payload = (fixtures_dir / "kaopu" / "stories.html").read_bytes()

    batch = KaopuNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 4
    first = batch.candidates[0]
    assert first.external_id is None
    assert first.title == "美联储三年来首次加息25个基点并暗示年内或再加息"
    assert first.url == (
        "https://kaopu.news/story/2026-09-11/"
        "cpi-data-looms-as-fed-rate-hike-odds-rise-7fe0ae"
    )
    assert first.published_at is None
    assert first.metrics["recency"] == "9月11日首发 · 52分钟前更新"
    assert first.metrics["provenance"] == "第一财经 · 中央通訊社 · 澎湃新闻 等"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3, 4]


def test_kaopu_adapter_deduplicates_repeated_stories():
    source = _source("kaopu_news", "https://kaopu.news/")
    payload = (
        b"<html><body><main>"
        b'<article><a href="/story/2026-09-17/one-abc"><h2>Title</h2></a></article>'
        b'<article><a href="/story/2026-09-17/one-abc"><h2>Title</h2></a></article>'
        b"</main></body></html>"
    )

    batch = KaopuNewsAdapter().parse(source, response_for(source, payload))

    assert [candidate.url for candidate in batch.candidates] == [
        "https://kaopu.news/story/2026-09-17/one-abc"
    ]


def test_kaopu_adapter_rejects_challenge_page():
    source = _source("kaopu_news", "https://kaopu.news/")

    assert_parse_rejects(
        KaopuNewsAdapter(),
        source,
        b"<html><body>Just a moment...</body></html>",
        match="does not contain any stories",
    )


def test_json_post_builds_json_body_request():
    source = _source("qqvideo_hot_search", "https://example.com/rank")

    request = json_post(source, {"page_params": {"tab_name": "热搜榜"}})

    assert request.method == "POST"
    assert request.url == source.url
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["Accept"].startswith("application/json")
    assert json.loads(request.content or b"{}") == {
        "page_params": {"tab_name": "热搜榜"}
    }


def test_qqvideo_adapter_builds_post_request():
    source = _source(
        "qqvideo_hot_search",
        "https://pbaccess.video.qq.com/trpc.vector_layout.page_view.PageService/getCard"
        "?video_appid=3000010&vversion_platform=2",
    )

    request = QqvideoHotSearchAdapter().build_request(source)

    assert request.method == "POST"
    assert request.url == source.url
    assert request.headers["Referer"] == "https://v.qq.com/"
    assert request.headers["Content-Type"] == "application/json"
    body = json.loads(request.content or b"{}")
    assert body["page_params"]["rank_name"] == "HotSearch"
    assert body["page_params"]["rank_page_size"] == "30"
    assert body["page_context"] == {"page_index": "1"}


def test_qqvideo_adapter_parses_hot_search(fixtures_dir: Path):
    source = _source("qqvideo_hot_search", "https://example.com/getCard")
    payload = (fixtures_dir / "qqvideo" / "hot_search.json").read_bytes()

    batch = QqvideoHotSearchAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "mzc00200803dr6b"
    assert first.title == "兰香如故"
    assert first.url == "https://v.qq.com/x/cover/mzc00200803dr6b.html"
    assert first.published_at is None
    assert first.raw_published_at is None
    assert first.metrics["subtitle"] == "高门跌落！婢女深宅挣生机"
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_qqvideo_adapter_rejects_failed_response():
    source = _source("qqvideo_hot_search", "https://example.com/getCard")

    assert_parse_rejects(
        QqvideoHotSearchAdapter(),
        source,
        b'{"ret": 100, "msg": "denied", "data": {}}',
        match="Unexpected Tencent Video ret",
    )


def test_qqvideo_adapter_rejects_empty_cards():
    source = _source("qqvideo_hot_search", "https://example.com/getCard")
    payload = json.dumps(
        {
            "ret": 0,
            "msg": "",
            "data": {"card": {"children_list": {"list": {"cards": []}}}},
        }
    ).encode()

    with pytest.raises(ValueError, match="no ranked cards"):
        QqvideoHotSearchAdapter().parse(source, response_for(source, payload))


def test_qqvideo_adapter_rejects_invalid_json():
    source = _source("qqvideo_hot_search", "https://example.com/getCard")

    assert_parse_rejects(
        QqvideoHotSearchAdapter(),
        source,
        b"<html>",
        match="Invalid Tencent Video JSON",
    )


def test_bilibili_hot_video_adapter_builds_request():
    source = _source(
        "bilibili_hot_video",
        "https://api.bilibili.com/x/web-interface/popular",
    )

    request = BilibiliHotVideoAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {"ps": "30", "pn": "1"}
    assert request.headers["Referer"] == "https://www.bilibili.com/"


def test_bilibili_hot_video_adapter_parses_popular(fixtures_dir: Path):
    source = _source(
        "bilibili_hot_video",
        "https://api.bilibili.com/x/web-interface/popular",
    )
    payload = (fixtures_dir / "bilibili" / "hot_video.json").read_bytes()

    batch = BilibiliHotVideoAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "BV1cSec6tEux"
    assert first.title == "折叠还是直板？iPhone 18 Pro&Duo深度视频"
    assert first.url == "https://www.bilibili.com/video/BV1cSec6tEux"
    assert first.published_at == datetime.fromtimestamp(1789560000, tz=UTC)
    assert first.raw_published_at == "1789560000"
    assert first.metrics["author"] == "影视飓风"
    assert first.metrics["view"] == 3267498
    assert first.metrics["like"] == 309809
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_bilibili_hot_video_adapter_rejects_error_code():
    source = _source(
        "bilibili_hot_video",
        "https://api.bilibili.com/x/web-interface/popular",
    )

    assert_parse_rejects(
        BilibiliHotVideoAdapter(),
        source,
        b'{"code": -352, "message": "-352"}',
        match="Unexpected Bilibili code",
    )


def test_bilibili_hot_video_adapter_rejects_empty_list():
    source = _source(
        "bilibili_hot_video",
        "https://api.bilibili.com/x/web-interface/popular",
    )

    with pytest.raises(ValueError, match="no videos"):
        BilibiliHotVideoAdapter().parse(
            source,
            response_for(source, b'{"code": 0, "data": {"list": []}}'),
        )


def test_bilibili_ranking_adapter_builds_request():
    source = _source(
        "bilibili_ranking",
        "https://api.bilibili.com/x/web-interface/ranking",
    )

    request = BilibiliRankingAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {"rid": "0", "type": "all"}
    assert request.headers["Referer"] == "https://www.bilibili.com/"


def test_bilibili_ranking_adapter_parses_ranking(fixtures_dir: Path):
    source = _source(
        "bilibili_ranking",
        "https://api.bilibili.com/x/web-interface/ranking",
    )
    payload = (fixtures_dir / "bilibili" / "ranking.json").read_bytes()

    batch = BilibiliRankingAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "BV1cSec6tEux"
    assert first.title == "折叠还是直板？iPhone 18 Pro&Duo深度视频"
    assert first.url == "https://www.bilibili.com/video/BV1cSec6tEux"
    assert first.published_at is None
    assert first.metrics["author"] == "影视飓风"
    assert first.metrics["play"] == 3017156
    assert first.metrics["pts"] == 0
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]


def test_bilibili_ranking_adapter_rejects_missing_data():
    source = _source(
        "bilibili_ranking",
        "https://api.bilibili.com/x/web-interface/ranking",
    )

    assert_parse_rejects(
        BilibiliRankingAdapter(),
        source,
        b'{"code": 0}',
        match="data object",
    )


def test_hket_rss_adapter_builds_section_requests():
    source = _source("hket_rss", "https://www.hket.com/rss")

    requests = HketRssAdapter().build_requests(source)

    assert [request.url for request in requests] == [
        "https://www.hket.com/rss/hongkong",
        "https://www.hket.com/rss/finance",
        "https://www.hket.com/rss/china",
        "https://www.hket.com/rss/world",
        "https://www.hket.com/rss/lifestyle",
        "https://www.hket.com/rss/technology",
        "https://www.hket.com/rss/entertainment",
    ]
    assert all(
        request.headers["User-Agent"].startswith("Mozilla/5.0") for request in requests
    )


def test_hket_rss_adapter_merges_and_deduplicates(fixtures_dir: Path):
    source = _source("hket_rss", "https://www.hket.com/rss")
    payload = (fixtures_dir / "hket" / "feed.xml").read_bytes()
    responses = tuple(response_for(source, payload) for _ in range(7))

    batch = HketRssAdapter().parse_responses(source, responses)

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]
    first = batch.candidates[0]
    assert first.external_id == "https://topick.hket.com/article/4195359"
    assert first.published_at is not None


def test_hket_rss_adapter_rejects_empty_feeds():
    source = _source("hket_rss", "https://www.hket.com/rss")
    payload = b'<rss version="2.0"><channel><title>empty</title></channel></rss>'

    with pytest.raises(ValueError, match="no items"):
        HketRssAdapter().parse_responses(
            source,
            (response_for(source, payload),),
        )


def test_mingpao_rss_adapter_builds_request():
    source = _source("mingpao_rss", "https://news.mingpao.com/rss/ins/all.xml")

    request = MingpaoRssAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert "rss+xml" in request.headers["Accept"]


def test_mingpao_rss_adapter_strips_attribute_fragments(fixtures_dir: Path):
    source = _source("mingpao_rss", "https://news.mingpao.com/rss/ins/all.xml")
    payload = (fixtures_dir / "mingpao" / "feed.xml").read_bytes()

    batch = MingpaoRssAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    assert [candidate.position for candidate in batch.candidates] == [1, 2, 3]
    first = batch.candidates[0]
    assert first.title == "死有對証｜「御用奸人」陳少邦10年冇演過好人 親揭入行遺憾"
    assert first.url == (
        "https://ol.mingpao.com/ldy/showbiz/latest/20260917/1789651170428/"
        "%e6%ad%bb%e6%9c%89%e5%b0%8d%e8%a8%bc-%e3%80%8c%e5%be%a1%e7%94%a8"
        "%e5%a5%b8%e4%ba%ba%e3%80%8d%e9%99%b3%e5%b0%91%e9%82%a610%e5%b9%b4"
        "%e5%86%87%e6%bc%94%e9%81%8e%e5%a5%bd%e4%ba%ba-%e8%a6%aa%e6%8f%ad"
        "%e5%85%a5%e8%a1%8c%e9%81%ba%e6%86%be"
    )
    assert first.external_id == first.url
    assert first.published_at == datetime(2026, 9, 17, 13, 19, 25, tzinfo=UTC)
    assert all('"' not in candidate.url for candidate in batch.candidates)
    assert all(
        candidate.external_id is None or '"' not in candidate.external_id
        for candidate in batch.candidates
    )


def test_mingpao_rss_adapter_accepts_empty_item_list():
    source = _source("mingpao_rss", "https://news.mingpao.com/rss/ins/all.xml")
    payload = b'<rss version="2.0"><channel><title>empty</title></channel></rss>'

    batch = MingpaoRssAdapter().parse(source, response_for(source, payload))

    assert batch.candidates == ()


def test_mingpao_rss_adapter_rejects_malformed_feed():
    source = _source("mingpao_rss", "https://news.mingpao.com/rss/ins/all.xml")

    with pytest.raises(ValueError, match="Invalid Ming Pao feed"):
        MingpaoRssAdapter().parse(source, response_for(source, b"<rss><channel>"))


def test_now_news_adapter_builds_request():
    source = _source(
        "now_news",
        "https://newsapi1.now.com/pccw-news-api/api/getRankNewsList",
    )

    request = NowNewsAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.params == {"pageSize": "30", "pageNo": "1"}


def test_now_news_adapter_parses_and_skips_ads(fixtures_dir: Path):
    source = _source(
        "now_news",
        "https://newsapi1.now.com/pccw-news-api/api/getRankNewsList",
    )
    payload = (fixtures_dir / "now-news" / "hot.json").read_bytes()

    batch = NowNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 2
    first = batch.candidates[0]
    assert first.external_id == "662600"
    assert first.title.startswith("Hyrox｜北京站")
    assert first.url == "https://news.now.com/home/local/player?newsId=662600"
    assert first.published_at == datetime.fromtimestamp(1789554752, tz=UTC)
    assert first.raw_published_at == "1789554752000"
    assert first.metrics["source"] == "nownews"
    assert first.metrics["view_count"] == 168
    assert all(candidate.external_id != "446579" for candidate in batch.candidates)


def test_now_news_adapter_rejects_non_list_payload():
    source = _source(
        "now_news",
        "https://newsapi1.now.com/pccw-news-api/api/getRankNewsList",
    )

    assert_parse_rejects(
        NowNewsAdapter(),
        source,
        b'{"error": "unexpected"}',
        match="JSON list",
    )


def test_now_news_adapter_history_pages_and_filters(fixtures_dir: Path):
    source = _source(
        "now_news",
        "https://newsapi1.now.com/pccw-news-api/api/getRankNewsList",
        max_items=30,
        history_max_pages=2,
    )
    payload = (fixtures_dir / "now-news" / "hot.json").read_bytes()
    since = datetime.fromtimestamp(1789560000, tz=UTC)

    requests = NowNewsAdapter().build_history_requests(source, since)
    batch = NowNewsAdapter().parse_history_responses(
        source,
        tuple(response_for(source, payload) for _ in requests),
        since,
    )

    assert [request.params["pageNo"] for request in requests] == ["1", "2"]
    assert [candidate.external_id for candidate in batch.candidates] == ["662618"]


def test_hkej_adapter_builds_request():
    source = _source("hkej_instant", "https://www.hkej.com/instantnews")

    request = HkejInstantAdapter().build_request(source)

    assert request.method == "GET"
    assert request.url == source.url
    assert request.headers["Accept"].startswith("text/html")


def test_hkej_adapter_parses_instant_listing(fixtures_dir: Path):
    source = _source("hkej_instant", "https://www.hkej.com/instantnews")
    payload = (fixtures_dir / "hkej" / "instant.html").read_bytes()

    batch = HkejInstantAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "4516211"
    assert first.title == "北都丨陳國基:今將推3幅洪水橋大學城用地 年底截止申請"
    assert first.url.startswith(
        "https://www.hkej.com/instantnews/current/article/4516211/"
    )
    assert first.published_at is None


def test_hkej_adapter_rejects_empty_page():
    source = _source("hkej_instant", "https://www.hkej.com/instantnews")

    assert_parse_rejects(
        HkejInstantAdapter(),
        source,
        b"<html><body>maintenance</body></html>",
        match="instant-news items",
    )


def test_am730_adapter_parses_news_list(fixtures_dir: Path):
    source = _source("am730_news", "https://www.am730.com.hk/")
    payload = (fixtures_dir / "am730" / "home.html").read_bytes()

    batch = Am730NewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 2
    first = batch.candidates[0]
    assert first.external_id == "1053706"
    assert first.title.startswith("61歲婦人涉向女童落安眠藥")
    assert first.url.startswith("https://www.am730.com.hk/本地/1053706/")
    assert first.published_at is not None
    assert first.raw_published_at == "2分鐘前"
    assert first.metrics["section"] == "本地"


def test_am730_adapter_rejects_empty_page():
    source = _source("am730_news", "https://www.am730.com.hk/")

    assert_parse_rejects(
        Am730NewsAdapter(),
        source,
        b"<html><body></body></html>",
        match="news items",
    )


def test_oncc_adapter_parses_focus_items(fixtures_dir: Path):
    source = _source("oncc_news", "https://hk.on.cc/hk/news/index.html")
    payload = (fixtures_dir / "oncc" / "home.html").read_bytes()

    batch = OnccNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "bkn-20260917170747836-0917_00822_001"
    assert first.title == "六旬婦涉向女童下安眠藥　准保釋11‧12再訊"
    assert first.url == (
        "https://hk.on.cc/hk/bkn/cnt/news/20260917/"
        "bkn-20260917170747836-0917_00822_001.html"
    )
    assert first.published_at == datetime(2026, 9, 17, 9, 7, 47, tzinfo=UTC)
    assert first.raw_published_at == "20260917170747"
    assert first.metrics["district"] == "香港"


def test_oncc_adapter_rejects_empty_page():
    source = _source("oncc_news", "https://hk.on.cc/hk/news/index.html")

    assert_parse_rejects(
        OnccNewsAdapter(),
        source,
        b"<html><body></body></html>",
        match="news items",
    )


def test_wenweipo_adapter_parses_news_list(fixtures_dir: Path):
    source = _source("wenweipo_news", "https://www.wenweipo.com/")
    payload = (fixtures_dir / "wenweipo" / "home.html").read_bytes()

    batch = WenweipoNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.title == "疑因投資欠債逾百萬起爭執　杏花邨男子涉持刀殺妻後自首"
    assert first.external_id == "AP6aab6f06e4b01d54a28395bc"
    assert first.published_at is not None
    assert first.raw_published_at == "4小時前"


def test_wenweipo_adapter_rejects_empty_page():
    source = _source("wenweipo_news", "https://www.wenweipo.com/")

    assert_parse_rejects(
        WenweipoNewsAdapter(),
        source,
        b"<html><body></body></html>",
        match="news items",
    )


def test_tkww_adapter_parses_story_list(fixtures_dir: Path):
    source = _source("tkww_news", "https://www.tkww.hk/")
    payload = (fixtures_dir / "tkww" / "home.html").read_bytes()

    batch = TkwwNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "AP6aab9f7ae4b05bea53178d85"
    assert (
        first.title
        == "李家超：五年規劃及施政報告凝聚集體智慧　希望立法會充分討論並支持"
    )
    assert first.url == (
        "https://www.tkww.hk/a/202609/17/AP6aab9f7ae4b05bea53178d85.html"
    )
    assert first.published_at is not None
    assert first.raw_published_at == "1小時前"


def test_tkww_adapter_rejects_empty_page():
    source = _source("tkww_news", "https://www.tkww.hk/")

    assert_parse_rejects(
        TkwwNewsAdapter(),
        source,
        b"<html><body></body></html>",
        match="news items",
    )


def test_thestandard_adapter_parses_listing(fixtures_dir: Path):
    source = _source("thestandard_news", "https://www.thestandard.com.hk/news")
    payload = (fixtures_dir / "thestandard" / "news.html").read_bytes()

    batch = TheStandardNewsAdapter().parse(source, response_for(source, payload))

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "343096"
    assert first.title.startswith("Cathay welcomes new Five-Year Plan")
    assert first.url == (
        "https://www.thestandard.com.hk/news/article/343096/"
        "Cathay-welcomes-new-Five-Year-Plan-to-strengthen-international-aviation-hub-status"
    )
    assert first.published_at is None
    assert first.metrics["age"] == "27 mins ago"


def test_thestandard_adapter_rejects_empty_page():
    source = _source("thestandard_news", "https://www.thestandard.com.hk/news")

    assert_parse_rejects(
        TheStandardNewsAdapter(),
        source,
        b"<html><body></body></html>",
        match="news items",
    )


def test_hackernews_adapter_builds_history_request():
    source = _source("hackernews_hot", "https://news.ycombinator.com/")
    since = datetime(2026, 9, 17, 0, 0, tzinfo=UTC)

    requests = HackerNewsHotAdapter().build_history_requests(source, since)

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "GET"
    assert request.url == "https://hn.algolia.com/api/v1/search_by_date"
    assert request.params["tags"] == "story"
    assert request.params["numericFilters"] == (
        f"created_at_i>{int(since.timestamp())}"
    )
    assert request.params["hitsPerPage"] == "30"


def test_hackernews_adapter_parses_history(fixtures_dir: Path):
    source = _source("hackernews_hot", "https://news.ycombinator.com/")
    payload = (fixtures_dir / "hackernews" / "algolia.json").read_bytes()
    since = datetime(2026, 9, 17, 8, 0, tzinfo=UTC)

    batch = HackerNewsHotAdapter().parse_history_responses(
        source,
        (response_for(source, payload),),
        since,
    )

    assert_batch_contract(batch)
    assert len(batch.candidates) == 3
    first = batch.candidates[0]
    assert first.external_id == "49738210"
    assert first.title == (
        "Nolan Bushnell (Atari, Co-Founder) Career Interview (2006) [video]"
    )
    assert first.url == "https://www.youtube.com/watch?v=wpR9JziuCsQ"
    assert first.published_at == datetime(2026, 9, 17, 9, 10, 32, tzinfo=UTC)
    assert first.raw_published_at == "1789636232"
    assert first.position is None


def test_hackernews_adapter_rejects_incompatible_history_payload():
    source = _source("hackernews_hot", "https://news.ycombinator.com/")
    since = datetime(2026, 9, 17, 8, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="hits"):
        HackerNewsHotAdapter().parse_history_responses(
            source,
            (response_for(source, b'{"items": []}'),),
            since,
        )
