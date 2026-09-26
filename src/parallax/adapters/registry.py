from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from parallax.adapters.base import Adapter
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
)
from parallax.adapters.cn.coolapk import CoolapkHotAdapter
from parallax.adapters.cn.dongqiudi import DongqiudiNewsAdapter
from parallax.adapters.cn.douban import DoubanHotMoviesAdapter
from parallax.adapters.cn.douyin import DouyinHotAdapter
from parallax.adapters.cn.douyin import validate_config as validate_douyin_config
from parallax.adapters.cn.fastbull import FastbullExpressAdapter, FastbullNewsAdapter
from parallax.adapters.cn.gelonghui import GelonghuiNewsAdapter
from parallax.adapters.cn.github import GithubTrendingAdapter
from parallax.adapters.cn.hackernews import HackerNewsHotAdapter
from parallax.adapters.cn.hackernews import (
    validate_options as validate_hackernews_options,
)
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
from parallax.adapters.cn.xueqiu import validate_config as validate_xueqiu_config
from parallax.adapters.cn.zaobao import ZaobaoRealtimeAdapter
from parallax.adapters.cn.zhihu import ZhihuHotAdapter
from parallax.adapters.common.options import positive_integer_options
from parallax.adapters.common.rss import RssAdapter
from parallax.adapters.hk.am730 import Am730NewsAdapter
from parallax.adapters.hk.hk01 import Hk01LatestAdapter
from parallax.adapters.hk.hkej import HkejInstantAdapter
from parallax.adapters.hk.hket import HketRssAdapter
from parallax.adapters.hk.mingpao import MingpaoRssAdapter
from parallax.adapters.hk.now_news import NowNewsAdapter
from parallax.adapters.hk.now_news import validate_options as validate_now_news_options
from parallax.adapters.hk.oncc import OnccNewsAdapter
from parallax.adapters.hk.thestandard import TheStandardNewsAdapter
from parallax.adapters.hk.tkww import TkwwNewsAdapter
from parallax.adapters.hk.wenweipo import WenweipoNewsAdapter
from parallax.config import Source


def _no_options(source: Source) -> None:
    positive_integer_options(source, {})


@dataclass(frozen=True, slots=True)
class AdapterRegistration:
    adapter: Adapter
    preflight: Callable[[Source], None] = _no_options


class AdapterRegistry:
    def __init__(self) -> None:
        self._registrations: dict[str, AdapterRegistration] = {
            "am730_news": AdapterRegistration(Am730NewsAdapter()),
            "baidu_hot_search": AdapterRegistration(BaiduHotSearchAdapter()),
            "bilibili_hot_search": AdapterRegistration(BilibiliHotSearchAdapter()),
            "bilibili_hot_video": AdapterRegistration(BilibiliHotVideoAdapter()),
            "bilibili_ranking": AdapterRegistration(BilibiliRankingAdapter()),
            "cankaoxiaoxi_news": AdapterRegistration(CankaoxiaoxiNewsAdapter()),
            "chongbuluo_hot": AdapterRegistration(ChongbuluoHotAdapter()),
            "cls_depth": AdapterRegistration(ClsDepthAdapter()),
            "cls_hot": AdapterRegistration(ClsHotAdapter()),
            "cls_telegraph": AdapterRegistration(ClsTelegraphAdapter()),
            "coolapk_hot": AdapterRegistration(CoolapkHotAdapter()),
            "dongqiudi_news": AdapterRegistration(DongqiudiNewsAdapter()),
            "douban_hot_movies": AdapterRegistration(DoubanHotMoviesAdapter()),
            "douyin_hot": AdapterRegistration(
                DouyinHotAdapter(), validate_douyin_config
            ),
            "fastbull_express": AdapterRegistration(FastbullExpressAdapter()),
            "fastbull_news": AdapterRegistration(FastbullNewsAdapter()),
            "gelonghui_news": AdapterRegistration(GelonghuiNewsAdapter()),
            "github_trending": AdapterRegistration(GithubTrendingAdapter()),
            "hackernews_hot": AdapterRegistration(
                HackerNewsHotAdapter(), validate_hackernews_options
            ),
            "hk01_latest": AdapterRegistration(Hk01LatestAdapter()),
            "hkej_instant": AdapterRegistration(HkejInstantAdapter()),
            "hket_rss": AdapterRegistration(HketRssAdapter()),
            "hupu_hot": AdapterRegistration(HupuHotAdapter()),
            "ifeng_hot": AdapterRegistration(IfengHotAdapter()),
            "iqiyi_hot_ranklist": AdapterRegistration(IqiyiHotRanklistAdapter()),
            "ithome_news": AdapterRegistration(IthomeNewsAdapter()),
            "jin10_flash": AdapterRegistration(Jin10FlashAdapter()),
            "juejin_hot": AdapterRegistration(JuejinHotAdapter()),
            "kaopu_news": AdapterRegistration(KaopuNewsAdapter()),
            "kr36_quick": AdapterRegistration(Kr36QuickAdapter()),
            "kuaishou_hot": AdapterRegistration(KuaishouHotAdapter()),
            "mingpao_rss": AdapterRegistration(MingpaoRssAdapter()),
            "mktnews_flash": AdapterRegistration(MktNewsFlashAdapter()),
            "now_news": AdapterRegistration(
                NowNewsAdapter(), validate_now_news_options
            ),
            "nowcoder_hot": AdapterRegistration(NowcoderHotAdapter()),
            "oncc_news": AdapterRegistration(OnccNewsAdapter()),
            "qqvideo_hot_search": AdapterRegistration(QqvideoHotSearchAdapter()),
            "rss": AdapterRegistration(RssAdapter()),
            "sputnik_news": AdapterRegistration(SputnikNewsAdapter()),
            "sspai_hot": AdapterRegistration(SspaiHotAdapter()),
            "steam_players": AdapterRegistration(SteamPlayersAdapter()),
            "tencent_hot": AdapterRegistration(TencentHotAdapter()),
            "thepaper_hot": AdapterRegistration(ThePaperHotAdapter()),
            "thestandard_news": AdapterRegistration(TheStandardNewsAdapter()),
            "tieba_hot": AdapterRegistration(TiebaHotAdapter()),
            "tkww_news": AdapterRegistration(TkwwNewsAdapter()),
            "toutiao_hot": AdapterRegistration(ToutiaoHotAdapter()),
            "v2ex_share": AdapterRegistration(V2exShareAdapter()),
            "wallstreetcn_hot": AdapterRegistration(WallstreetcnHotAdapter()),
            "wallstreetcn_news": AdapterRegistration(WallstreetcnNewsAdapter()),
            "wallstreetcn_quick": AdapterRegistration(WallstreetcnQuickAdapter()),
            "weibo_hot": AdapterRegistration(WeiboHotAdapter()),
            "wenweipo_news": AdapterRegistration(WenweipoNewsAdapter()),
            "xueqiu_hotstock": AdapterRegistration(
                XueqiuHotstockAdapter(), validate_xueqiu_config
            ),
            "zaobao_realtime": AdapterRegistration(ZaobaoRealtimeAdapter()),
            "zhihu_hot": AdapterRegistration(ZhihuHotAdapter()),
        }

    def resolve_source(self, source: Source) -> Adapter:
        """Resolve and validate a source before any request construction or I/O."""
        name = source.endpoint.adapter
        try:
            registration = self._registrations[name]
        except KeyError as exc:
            available = ", ".join(self.names())
            raise KeyError(f"Unknown adapter {name!r}; available: {available}") from exc
        registration.preflight(source)
        return registration.adapter

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._registrations))
