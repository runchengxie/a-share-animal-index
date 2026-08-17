from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from zoo_index.config import Rules, load_rules
from zoo_index.data_sources.tushare import TradeCalendarEntry
from zoo_index.runner import (
    BenchmarkConfig,
    RunConfig,
    _get_benchmark_return,
    _snapshot_rules,
    compute_day,
    run_backfill,
    run_daily,
)


class FakeClient:
    """内存版 Tushare 客户端，仅供测试，不发起任何网络请求。"""

    def __init__(self, open_dates: list[str], prices: dict[str, tuple[float, float]]) -> None:
        self.open_dates = open_dates
        self.prices = prices
        self.benchmark_code = "000300.SH"
        self.prev_date = "20240102"

    def _price_row(self, ts_code: str, as_prev: bool = False) -> dict:
        close, pre_close = self.prices.get(ts_code, (1.0, 1.0))
        effective_close = pre_close if as_prev else close
        return {"ts_code": ts_code, "close": effective_close, "pre_close": pre_close}

    def get_trade_calendar(self, date: str) -> TradeCalendarEntry:
        return TradeCalendarEntry(date=date, is_open=date in self.open_dates)

    def get_trade_calendar_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        rows = [
            {"cal_date": d, "is_open": 1} for d in self.open_dates if start_date <= d <= end_date
        ]
        return pd.DataFrame(rows)

    def get_recent_open_dates(
        self, end_date: str, count: int, lookback_days: int | None = None
    ) -> list[str]:
        if count <= 0:
            raise ValueError("count must be positive")
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
                    "delist_date": 99999999,
                },
                {
                    "ts_code": "000002.SZ",
                    "name": "马钢",
                    "exchange": "SSE",
                    "list_date": 20200101,
                    "delist_date": 99999999,
                },
                {
                    "ts_code": "000003.SZ",
                    "name": "龙湖",
                    "exchange": "SZSE",
                    "list_date": 20200101,
                    "delist_date": 99999999,
                },
                {
                    "ts_code": "000004.SZ",
                    "name": "中国银行",
                    "exchange": "SZSE",
                    "list_date": 20200101,
                    "delist_date": 99999999,
                },
            ]
        )

    def get_namechange(self) -> pd.DataFrame:
        return pd.DataFrame(columns=["ts_code", "name", "start_date", "end_date"])  # ty: ignore[invalid-argument-type]

    def get_daily(self, trade_date: str) -> pd.DataFrame:
        as_prev = trade_date == self.prev_date
        return pd.DataFrame([self._price_row(code, as_prev) for code in self.prices])

    def get_adj_factor(self, trade_date: str) -> pd.DataFrame:
        return pd.DataFrame([{"ts_code": code, "adj_factor": 1.0} for code in self.prices])

    def get_index_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        return pd.DataFrame([{"ts_code": ts_code, "close": 1.01, "pre_close": 1.0}])

    def get_fund_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        return pd.DataFrame([{"ts_code": ts_code, "close": 1.01, "pre_close": 1.0}])

    def get_fund_adj(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        return pd.DataFrame([{"ts_code": ts_code, "adj_factor": 1.0}])

    def get_suspension(self, trade_date: str) -> pd.DataFrame:
        return pd.DataFrame(columns=pd.Index(["ts_code", "suspend_date", "suspend_type"]))


def _rules() -> Rules:
    return load_rules(Path(__file__).resolve().parent.parent / "rules.yml")


def _rules_with_forces(
    force_include: tuple[str, ...] = (), force_exclude: tuple[str, ...] = ()
) -> Rules:
    base = _rules()
    return Rules(
        strict_keywords=base.strict_keywords,
        extended_keywords=base.extended_keywords,
        exclude_patterns=base.exclude_patterns,
        force_include=force_include,
        force_exclude=force_exclude,
        exclude_st=base.exclude_st,
        allow_beijing=base.allow_beijing,
    )


def _make_client() -> FakeClient:
    open_dates = ["20231229", "20240102", "20240103", "20240104", "20240105", "20240108"]
    prices = {
        "000001.SZ": (11.0, 10.0),
        "600000.SH": (10.5, 10.0),
        "000002.SZ": (10.0, 10.0),
        "000003.SZ": (10.0, 10.0),
        "000004.SZ": (10.5, 10.0),
    }
    return FakeClient(open_dates, prices)


def _benchmark(source: str = "index") -> BenchmarkConfig:
    return BenchmarkConfig(code="000300.SH", source=source, label="HS300")


def test_compute_day_matches_animal_words_and_force_lists() -> None:
    client = _make_client()
    rules = _rules_with_forces(force_include=("000004.SZ",), force_exclude=("600000.SH",))
    result = compute_day(
        client, rules, _benchmark(), "20240103", client.get_stock_basic(), client.get_namechange()
    )

    strict_codes = set(result.strict_df["ts_code"])
    extended_codes = set(result.extended_df["ts_code"])
    # 金龙鱼命中；中国银行经 force_include 纳入；熊猫经 force_exclude 剔除；马钢/龙湖被排除项过滤。
    assert strict_codes == {"000001.SZ", "000004.SZ"}
    assert extended_codes == {"000001.SZ", "000004.SZ"}
    assert "600000.SH" not in strict_codes

    # 金龙鱼 +10%，中国银行 +5%，等权平均 +7.5%。
    assert result.strict_ret == pytest.approx(0.075)
    assert result.extended_ret == pytest.approx(0.075)
    # 指数基准 +1%。
    assert result.benchmark_ret == pytest.approx(0.01)


def test_run_daily_writes_benchmark_named_outputs(tmp_path: Path) -> None:
    client = _make_client()
    config = RunConfig(
        repo_root=tmp_path,
        output_dir=tmp_path,
        rules_path=Path(__file__).resolve().parent.parent / "rules.yml",
        token="dummy",
        benchmark=_benchmark(),
        date="20240103",
    )
    assert run_daily(config, client) == 0

    nav = pd.read_csv(tmp_path / "data" / "nav.csv")
    assert "benchmark_ret" in nav.columns
    assert "benchmark_nav" in nav.columns
    assert "hs300_ret" not in nav.columns

    latest = __import__("json").loads(
        (tmp_path / "data" / "latest.json").read_text(encoding="utf-8")
    )
    assert latest["benchmark_nav"] == pytest.approx(1.01, rel=1e-6)
    assert (tmp_path / "data" / "constituents.json").exists()
    assert (tmp_path / "data" / "metadata.json").exists()
    assert (tmp_path / "data" / "history.json").exists()

    # holdings 快照应写出持仓（含权重），而非成分。
    holdings = pd.read_csv(tmp_path / "manifests" / "holdings_20240103.csv")
    assert "weight" in holdings.columns
    # 同一文件含 strict 与 extended 两种组合，各自等权，权重分别合计为 1.0。
    for variant in ("strict", "extended"):
        variant_weights = holdings.loc[holdings["variant"] == variant, "weight"]
        assert variant_weights.sum() == pytest.approx(1.0)


def test_run_backfill_then_missing_is_idempotent(tmp_path: Path) -> None:
    client = _make_client()
    rules_path = Path(__file__).resolve().parent.parent / "rules.yml"

    def _config() -> RunConfig:
        return RunConfig(
            repo_root=tmp_path,
            output_dir=tmp_path,
            rules_path=rules_path,
            token="dummy",
            benchmark=_benchmark(),
            date="20240108",
            backfill_requested=True,
            backfill_days=4,
        )

    assert run_backfill(_config(), client) == 0
    nav = pd.read_csv(tmp_path / "data" / "nav.csv")
    assert len(nav) == 4
    assert "benchmark_ret" in nav.columns

    # 再次以 missing 模式运行，区间已存在，应直接跳过。
    assert run_backfill(_config(), client) == 0


def test_run_backfill_all_mode_recomputes(tmp_path: Path) -> None:
    client = _make_client()
    rules_path = Path(__file__).resolve().parent.parent / "rules.yml"
    config = RunConfig(
        repo_root=tmp_path,
        output_dir=tmp_path,
        rules_path=rules_path,
        token="dummy",
        benchmark=_benchmark(),
        date="20240108",
        backfill_requested=True,
        backfill_days=4,
        backfill_mode="all",
    )
    assert run_backfill(config, client) == 0
    nav = pd.read_csv(tmp_path / "data" / "nav.csv")
    assert len(nav) == 4


def test_snapshot_rules_creates_missing_data_dir(tmp_path: Path) -> None:
    rules = tmp_path / "rules.yml"
    rules.write_text("constituents: []\n", encoding="utf-8")
    data_dir = tmp_path / "out" / "data"
    assert not data_dir.exists()

    snapshot = _snapshot_rules(rules, data_dir, "20240101", "20240108")

    assert data_dir.is_dir()
    assert snapshot.is_file()
    assert snapshot.read_text(encoding="utf-8") == "constituents: []\n"


class TwoMonthClient:
    """跨月客户端：一月篮子 {金龙鱼,熊猫}，二月新增 海豚（严格关键词命中）。"""

    def __init__(self) -> None:
        self.open_dates = ["20240129", "20240130", "20240131", "20240201"]
        # (close, pre_close) 按日；相邻日 close 形成 +10%/+5%/+100%(龙凤仅二月) 阶梯。
        self.prices = {
            "20240129": {
                "000001.SZ": (10.0, 10.0),
                "600000.SH": (10.0, 10.0),
                "000009.SZ": (10.0, 10.0),
            },
            "20240130": {
                "000001.SZ": (11.0, 10.0),
                "600000.SH": (10.5, 10.0),
                "000009.SZ": (10.0, 10.0),
            },
            "20240131": {
                "000001.SZ": (12.1, 11.0),
                "600000.SH": (11.025, 10.5),
                "000009.SZ": (10.0, 10.0),
            },
            "20240201": {
                "000001.SZ": (13.31, 12.1),
                "600000.SH": (11.57625, 11.025),
                "000009.SZ": (20.0, 10.0),
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
        if count <= 0:
            raise ValueError("count must be positive")
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
                    "delist_date": 99999999,
                },
                {
                    "ts_code": "000009.SZ",
                    "name": "无名",
                    "exchange": "SZSE",
                    "list_date": 20200101,
                    "delist_date": 99999999,
                },
            ]
        )

    def get_namechange(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ts_code": "000009.SZ",
                    "name": "海豚",
                    "start_date": "20240201",
                    "end_date": "99999999",
                }
            ]
        )

    def get_daily(self, trade_date: str) -> pd.DataFrame:
        day = self.prices.get(trade_date, {})
        return pd.DataFrame(
            [
                {"ts_code": code, "close": close, "pre_close": pre_close}
                for code, (close, pre_close) in day.items()
            ]
        )

    def get_adj_factor(self, trade_date: str) -> pd.DataFrame:
        return pd.DataFrame(
            [{"ts_code": code, "adj_factor": 1.0} for code in self.prices.get(trade_date, {})]
        )

    def get_index_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        return pd.DataFrame([{"ts_code": ts_code, "close": 1.01, "pre_close": 1.0}])

    def get_fund_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        return pd.DataFrame([{"ts_code": ts_code, "close": 1.01, "pre_close": 1.0}])

    def get_fund_adj(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        return pd.DataFrame([{"ts_code": ts_code, "adj_factor": 1.0}])

    def get_suspension(self, trade_date: str) -> pd.DataFrame:
        return pd.DataFrame(columns=pd.Index(["ts_code", "suspend_date", "suspend_type"]))


def test_compute_day_monthly_rebalance_removes_lookahead() -> None:
    # 月度再平衡（去前视）：再平衡日当天收益用上一篮子，新篮子次日生效。
    client = TwoMonthClient()
    rules = _rules()
    # 一月 31 日：建仓首日，用一月篮子（金龙鱼 +10%、熊猫 +5%）。
    jan = compute_day(
        client, rules, _benchmark(), "20240131", client.get_stock_basic(), client.get_namechange()
    )
    assert jan.strict_ret == pytest.approx(0.075)
    assert "000009.SZ" not in set(jan.strict_df["ts_code"])

    # 二月 1 日：新月份触发再平衡。当日收益沿用一月篮子（不含海豚 +100%），
    # 但快照写入的新篮子应包含海豚。
    feb = compute_day(
        client,
        rules,
        _benchmark(),
        "20240201",
        client.get_stock_basic(),
        client.get_namechange(),
        prev_state=jan.state,
    )
    assert "000009.SZ" in set(feb.strict_df["ts_code"])
    # 若前视未去除，海豚 +100% 会使收益约 0.383；去除后仅一月篮子 ~0.075。
    assert feb.strict_ret == pytest.approx(0.075)
    # 新篮子权重等权（3 只），合计 1.0。
    feb_weights = feb.strict_holdings[feb.strict_holdings["ts_code"] == "000009.SZ"]["weight"]
    assert feb_weights.iloc[0] == pytest.approx(1 / 3)


def test_backfill_carries_state_across_months(tmp_path: Path) -> None:
    # 回填跨月时，状态按日传递：二月首日收益用一月篮子（去前视）。
    client = TwoMonthClient()
    rules_path = Path(__file__).resolve().parent.parent / "rules.yml"
    config = RunConfig(
        repo_root=tmp_path,
        output_dir=tmp_path,
        rules_path=rules_path,
        token="dummy",
        benchmark=_benchmark(),
        date="20240201",
        backfill_requested=True,
        backfill_days=3,
    )
    assert run_backfill(config, client) == 0
    nav = pd.read_csv(tmp_path / "data" / "nav.csv")
    # 三个交易日：20240130(建仓,0.075)、20240131(月内,0.075)、20240201(再平衡,0.075)。
    assert len(nav) == 3
    assert list(nav["zoo_strict_ret"]) == pytest.approx([0.075, 0.075, 0.075])
    # 二月首日快照含新成分海豚，证明状态正确重建并再平衡。
    holdings = pd.read_csv(tmp_path / "manifests" / "holdings_20240201.csv")
    assert "000009.SZ" in set(holdings["ts_code"])


def test_benchmark_return_uses_adjusted_ratio_for_fund() -> None:
    # 基准与指数同口径：fund 源用 close*adj/(pre_close*prev_adj)-1。
    # 除息日 adj 跳升，单纯 close/pre_close 会漏掉分红。
    class FundAdjClient:
        def get_fund_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame:
            data = {"20240102": (10.0, 10.0), "20240103": (11.0, 10.0)}
            close, pre = data[trade_date]
            return pd.DataFrame([{"ts_code": ts_code, "close": close, "pre_close": pre}])

        def get_fund_adj(self, trade_date: str, ts_code: str) -> pd.DataFrame:
            adj = {"20240102": 1.0, "20240103": 1.05}[trade_date]
            return pd.DataFrame([{"ts_code": ts_code, "adj_factor": adj}])

    client = FundAdjClient()
    bench = BenchmarkConfig(code="510300.SH", source="fund", label="HS300 ETF")
    # (11*1.05)/(10*1.0) - 1 = 0.155，而非 close/pre_close 给出的 0.10。
    ret = _get_benchmark_return(client, "20240103", "20240102", bench)
    assert ret == pytest.approx(0.155)
