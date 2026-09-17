from __future__ import annotations

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
from parallax.config import SourceConfig

COMMON_OPTIONS = frozenset({"max_items"})
ADAPTER_OPTIONS = {
    "now_news": frozenset({"history_max_pages"}),
}


class AdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, Adapter] = {
            "am730_news": Am730NewsAdapter(),
            "baidu_hot_search": BaiduHotSearchAdapter(),
            "bilibili_hot_search": BilibiliHotSearchAdapter(),
            "bilibili_hot_video": BilibiliHotVideoAdapter(),
            "bilibili_ranking": BilibiliRankingAdapter(),
            "cankaoxiaoxi_news": CankaoxiaoxiNewsAdapter(),
            "chongbuluo_hot": ChongbuluoHotAdapter(),
            "cls_depth": ClsDepthAdapter(),
            "cls_hot": ClsHotAdapter(),
            "cls_telegraph": ClsTelegraphAdapter(),
            "coolapk_hot": CoolapkHotAdapter(),
            "dongqiudi_news": DongqiudiNewsAdapter(),
            "douban_hot_movies": DoubanHotMoviesAdapter(),
            "douyin_hot": DouyinHotAdapter(),
            "fastbull_express": FastbullExpressAdapter(),
            "fastbull_news": FastbullNewsAdapter(),
            "gelonghui_news": GelonghuiNewsAdapter(),
            "github_trending": GithubTrendingAdapter(),
            "hackernews_hot": HackerNewsHotAdapter(),
            "hk01_latest": Hk01LatestAdapter(),
            "hkej_instant": HkejInstantAdapter(),
            "hket_rss": HketRssAdapter(),
            "hupu_hot": HupuHotAdapter(),
            "ifeng_hot": IfengHotAdapter(),
            "iqiyi_hot_ranklist": IqiyiHotRanklistAdapter(),
            "ithome_news": IthomeNewsAdapter(),
            "jin10_flash": Jin10FlashAdapter(),
            "juejin_hot": JuejinHotAdapter(),
            "kaopu_news": KaopuNewsAdapter(),
            "kr36_quick": Kr36QuickAdapter(),
            "kuaishou_hot": KuaishouHotAdapter(),
            "mingpao_rss": MingpaoRssAdapter(),
            "mktnews_flash": MktNewsFlashAdapter(),
            "now_news": NowNewsAdapter(),
            "nowcoder_hot": NowcoderHotAdapter(),
            "oncc_news": OnccNewsAdapter(),
            "qqvideo_hot_search": QqvideoHotSearchAdapter(),
            "rss": RssAdapter(),
            "sputnik_news": SputnikNewsAdapter(),
            "sspai_hot": SspaiHotAdapter(),
            "steam_players": SteamPlayersAdapter(),
            "tencent_hot": TencentHotAdapter(),
            "thepaper_hot": ThePaperHotAdapter(),
            "thestandard_news": TheStandardNewsAdapter(),
            "tieba_hot": TiebaHotAdapter(),
            "tkww_news": TkwwNewsAdapter(),
            "toutiao_hot": ToutiaoHotAdapter(),
            "v2ex_share": V2exShareAdapter(),
            "wallstreetcn_hot": WallstreetcnHotAdapter(),
            "wallstreetcn_news": WallstreetcnNewsAdapter(),
            "wallstreetcn_quick": WallstreetcnQuickAdapter(),
            "weibo_hot": WeiboHotAdapter(),
            "wenweipo_news": WenweipoNewsAdapter(),
            "xueqiu_hotstock": XueqiuHotstockAdapter(),
            "zaobao_realtime": ZaobaoRealtimeAdapter(),
            "zhihu_hot": ZhihuHotAdapter(),
        }

    def get(self, name: str) -> Adapter:
        try:
            return self._adapters[name]
        except KeyError as exc:
            available = ", ".join(sorted(self._adapters))
            raise KeyError(f"Unknown adapter {name!r}; available: {available}") from exc

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._adapters))

    def validate_source(self, source: SourceConfig) -> None:
        self.get(source.adapter)
        allowed = COMMON_OPTIONS | ADAPTER_OPTIONS.get(source.adapter, frozenset())
        unknown = sorted(set(source.options) - allowed)
        if unknown:
            joined = ", ".join(unknown)
            raise ValueError(
                f"Unknown options for adapter {source.adapter!r}: {joined}"
            )
