from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from zoo_index.config import Rules, load_rules
from zoo_index.data_sources.tushare import TradeCalendarEntry
from zoo_index.runner import _anomalous_codes, compute_day


def _rules() -> Rules:
    return load_rules(Path(__file__).resolve().parent.parent / "rules.yml")


def _benchmark(source: str = "index"):
    from zoo_index.runner import BenchmarkConfig

    return BenchmarkConfig(code="000300.SH", source=source, label="HS300")


def _empty_namechange() -> pd.DataFrame:
    return pd.DataFrame(columns=pd.Index(["ts_code", "name", "start_date", "end_date"]))


def test_anomalous_codes_detects_delist() -> None:
    held = pd.DataFrame([{"ts_code": "000001.SZ", "name": "金龙鱼"}])
    stock_basic = pd.DataFrame([{"ts_code": "000001.SZ", "delist_date": 20240101}])
    anom, streak = _anomalous_codes(
        held, stock_basic, _empty_namechange(), "20240102", set(), {}, 0
    )
    assert anom == {"000001.SZ"}
    assert streak.get("000001.SZ") == 0


def test_anomalous_codes_detects_st() -> None:
    held = pd.DataFrame([{"ts_code": "000001.SZ", "name": "金龙鱼"}])
    stock_basic = pd.DataFrame([{"ts_code": "000001.SZ", "delist_date": 99999999}])
    namechange = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "name": "*ST金龙鱼",
                "start_date": "20240101",
                "end_date": "99999999",
            }
        ]
    )
    anom, _ = _anomalous_codes(held, stock_basic, namechange, "20240102", set(), {}, 0)
    assert anom == {"000001.SZ"}


def test_anomalous_codes_detects_long_suspension() -> None:
    held = pd.DataFrame([{"ts_code": "000001.SZ", "name": "金龙鱼"}])
    stock_basic = pd.DataFrame([{"ts_code": "000001.SZ", "delist_date": 99999999}])
    # 连续停牌达阈值（3）触发；streak 累加为 3。
    anom, streak = _anomalous_codes(
        held, stock_basic, _empty_namechange(), "20240105", {"000001.SZ"}, {"000001.SZ": 2}, 3
    )
    assert anom == {"000001.SZ"}
    assert streak["000001.SZ"] == 3
    # 未达阈值不触发。
    anom2, _ = _anomalous_codes(
        held, stock_basic, _empty_namechange(), "20240105", {"000001.SZ"}, {"000001.SZ": 1}, 3
    )
    assert anom2 == set()


def test_anomalous_codes_long_suspension_disabled_when_zero() -> None:
    held = pd.DataFrame([{"ts_code": "000001.SZ", "name": "金龙鱼"}])
    stock_basic = pd.DataFrame([{"ts_code": "000001.SZ", "delist_date": 99999999}])
    anom, _ = _anomalous_codes(
        held, stock_basic, _empty_namechange(), "20240105", {"000001.SZ"}, {"000001.SZ": 99}, 0
    )
    assert anom == set()


class DelistClient:
    """两日客户端：熊猫在 20240103 退市，触发异常再平衡。"""

    def __init__(self) -> None:
        self.open_dates = ["20240101", "20240102", "20240103"]
        self.prices = {
            "20240101": {
                "000001.SZ": (10.0, 10.0),
                "600000.SH": (10.0, 10.0),
            },
            "20240102": {
                "000001.SZ": (11.0, 10.0),
                "600000.SH": (10.5, 10.0),
            },
            "20240103": {
                "000001.SZ": (12.1, 11.0),
                "600000.SH": (11.55, 10.5),
            },
        }

    def get_trade_calendar(self, date: str) -> TradeCalendarEntry:
        return TradeCalendarEntry(date=date, is_open=date in self.open_dates)

    def get_trade_calendar_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        rows = [
            {"cal_date": d, "is_open": 1} for d in self.open_dates if start_date <= d <= end_date
        ]
        return pd.DataFrame(rows)

    def get_recent_open_dates(self, end_date: str, count: int, lookback_days=None) -> list[str]:
        dates = [d for d in self.open_dates if d <= end_date]
        if len(dates) < count:
            raise ValueError("no open trading day found")
        return dates[-count:]

    def get_stock_basic(self) -> pd.DataFrame:
        return pd.DataFrame(
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
                    "delist_date": 20240103,
                },
            ]
        )

    def get_namechange(self) -> pd.DataFrame:
        return _empty_namechange()

    def get_daily(self, trade_date: str) -> pd.DataFrame:
        day = self.prices.get(trade_date, {})
        return pd.DataFrame(
            [
                {"ts_code": code, "close": close, "pre_close": pre_close}
                for code, (close, pre_close) in day.items()
            ]
        )

    def get_adj_factor(self, trade_date: str) -> pd.DataFrame:
        codes = self.prices.get(trade_date, {})
        return pd.DataFrame([{"ts_code": code, "adj_factor": 1.0} for code in codes])

    def get_index_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        return pd.DataFrame([{"ts_code": ts_code, "close": 1.01, "pre_close": 1.0}])

    def get_fund_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        return pd.DataFrame([{"ts_code": ts_code, "close": 1.01, "pre_close": 1.0}])

    def get_fund_adj(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        return pd.DataFrame([{"ts_code": ts_code, "adj_factor": 1.0}])

    def get_suspension(self, trade_date: str) -> pd.DataFrame:
        return pd.DataFrame(columns=pd.Index(["ts_code", "suspend_date", "suspend_type"]))


def test_compute_day_exception_rebalance_on_delist() -> None:
    client = DelistClient()
    rules = _rules()
    # 首日建仓：篮子 {金龙鱼,熊猫}，收益 (10%+5%)/2。
    day1 = compute_day(
        client, rules, _benchmark(), "20240102", client.get_stock_basic(), client.get_namechange()
    )
    assert day1.strict_ret == pytest.approx(0.075)
    assert set(day1.strict_holdings["ts_code"]) == {"000001.SZ", "600000.SH"}

    # 次日：熊猫退市触发异常再平衡，剔除后仅金龙鱼（权重 1.0，收益 +10%）。
    day2 = compute_day(
        client,
        rules,
        _benchmark(),
        "20240103",
        client.get_stock_basic(),
        client.get_namechange(),
        prev_state=day1.state,
    )
    assert set(day2.strict_holdings["ts_code"]) == {"000001.SZ"}
    assert day2.strict_holdings["weight"].iloc[0] == pytest.approx(1.0)
    assert day2.strict_ret == pytest.approx(0.10)
    assert day2.state is not None
    assert day2.state.strict.reason == "exception"
