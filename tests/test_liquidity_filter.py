from __future__ import annotations

from pathlib import Path

import pandas as pd

from zoo_index.config import Rules, load_rules
from zoo_index.index import _filter_min_listing_age
from zoo_index.runner import _get_constituents_for_rebalance


def _rules_with(min_listing_days: int = 0, min_daily_amount: float = 0.0) -> Rules:
    base = load_rules(Path(__file__).resolve().parent.parent / "rules.yml")
    return Rules(
        strict_keywords=base.strict_keywords,
        extended_keywords=base.extended_keywords,
        exclude_patterns=base.exclude_patterns,
        force_include=base.force_include,
        force_exclude=base.force_exclude,
        exclude_st=base.exclude_st,
        allow_beijing=base.allow_beijing,
        min_listing_days=min_listing_days,
        min_daily_amount=min_daily_amount,
        max_suspension_days=base.max_suspension_days,
    )


def test_filter_min_listing_age_excludes_recent_listing() -> None:
    stock_basic = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "name": "金龙鱼",
                "exchange": "SZSE",
                "list_date": 20240101,
                "delist_date": 99999999,
            },
            {
                "ts_code": "600000.SH",
                "name": "熊猫",
                "exchange": "SSE",
                "list_date": 20200101,
                "delist_date": 99999999,
            },
        ]
    )
    # 截至 20240201，金龙鱼上市 31 天 < 60 → 剔除；熊猫 > 60 → 保留。
    filtered = _filter_min_listing_age(stock_basic, "20240201", 60)
    assert set(filtered["ts_code"]) == {"600000.SH"}
    # 门槛为 0 时不过滤。
    assert set(_filter_min_listing_age(stock_basic, "20240201", 0)["ts_code"]) == {
        "000001.SZ",
        "600000.SH",
    }


def test_rebalance_excludes_low_amount() -> None:
    class LiquidityClient:
        def get_daily(self, trade_date: str) -> pd.DataFrame:
            # 金龙鱼成交额偏低，熊猫达标。
            return pd.DataFrame(
                [
                    {"ts_code": "000001.SZ", "close": 11.0, "pre_close": 10.0, "amount": 1e6},
                    {"ts_code": "600000.SH", "close": 10.5, "pre_close": 10.0, "amount": 1e9},
                ]
            )

    stock_basic = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "name": "金龙鱼",
                "exchange": "SZSE",
                "list_date": 20200101,
                "delist_date": 99999999,
            },
            {
                "ts_code": "600000.SH",
                "name": "熊猫",
                "exchange": "SSE",
                "list_date": 20200101,
                "delist_date": 99999999,
            },
        ]
    )
    namechange = pd.DataFrame(columns=["ts_code", "name", "start_date", "end_date"])
    rules = _rules_with(min_daily_amount=5e8)

    strict_df, extended_df = _get_constituents_for_rebalance(
        {}, stock_basic, namechange, rules, "20240102", client=LiquidityClient()
    )
    # 金龙鱼成交额 1e6 < 5e8 被剔除，仅熊猫保留。
    assert set(strict_df["ts_code"]) == {"600000.SH"}
    assert set(extended_df["ts_code"]) == {"600000.SH"}


def test_rebalance_keeps_all_when_amount_missing() -> None:
    class NoAmountClient:
        def get_daily(self, trade_date: str) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {"ts_code": "000001.SZ", "close": 11.0, "pre_close": 10.0},
                    {"ts_code": "600000.SH", "close": 10.5, "pre_close": 10.0},
                ]
            )

    stock_basic = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "name": "金龙鱼",
                "exchange": "SZSE",
                "list_date": 20200101,
                "delist_date": 99999999,
            },
            {
                "ts_code": "600000.SH",
                "name": "熊猫",
                "exchange": "SSE",
                "list_date": 20200101,
                "delist_date": 99999999,
            },
        ]
    )
    namechange = pd.DataFrame(columns=["ts_code", "name", "start_date", "end_date"])
    rules = _rules_with(min_daily_amount=5e8)

    strict_df, _ = _get_constituents_for_rebalance(
        {}, stock_basic, namechange, rules, "20240102", client=NoAmountClient()
    )
    # 无 amount 列时视为不过滤，全部保留。
    assert set(strict_df["ts_code"]) == {"000001.SZ", "600000.SH"}
