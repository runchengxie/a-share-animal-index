import pandas as pd
import pytest

from zoo_index.index import compute_equal_weight_return


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_compute_equal_weight_return_keeps_suspended_stock() -> None:
    # 停牌股无当日行情，但持仓与权重保留，当日收益计为 0。
    constituents = _frame(
        [
            {"ts_code": "000001.SZ", "name": "Alpha", "keyword": "CAT", "forced": False},
            {"ts_code": "000002.SZ", "name": "Beta", "keyword": "DOG", "forced": False},
        ]
    )
    daily_prices = _frame(
        [
            {"ts_code": "000001.SZ", "close": 11.0, "pre_close": 10.0},
        ]
    )
    prev_daily_prices = _frame(
        [
            {"ts_code": "000001.SZ", "close": 10.0},
            {"ts_code": "000002.SZ", "close": 10.0},
        ]
    )
    suspended = {"000002.SZ"}

    index_ret, holdings, stats = compute_equal_weight_return(
        constituents, daily_prices, prev_daily_prices, suspended=suspended
    )

    # 000001 涨 10%，000002 停牌计 0，等权平均为 5%。
    assert index_ret == pytest.approx(0.05)
    assert stats.total_constituents == 2
    assert stats.priced_constituents == 2
    assert stats.missing_prices == 0
    assert holdings["weight"].sum() == pytest.approx(1.0)


def test_compute_equal_weight_return_excludes_genuine_missing() -> None:
    # 无行情且非停牌属于真实缺失，排除出当日指数并提示。
    constituents = _frame(
        [
            {"ts_code": "000001.SZ", "name": "Alpha", "keyword": "CAT", "forced": False},
            {"ts_code": "000002.SZ", "name": "Beta", "keyword": "DOG", "forced": False},
        ]
    )
    daily_prices = _frame(
        [
            {"ts_code": "000001.SZ", "close": 11.0, "pre_close": 10.0},
        ]
    )
    prev_daily_prices = _frame(
        [
            {"ts_code": "000001.SZ", "close": 10.0},
            {"ts_code": "000002.SZ", "close": 10.0},
        ]
    )

    index_ret, _, stats = compute_equal_weight_return(constituents, daily_prices, prev_daily_prices)

    assert index_ret == pytest.approx(0.10)
    assert stats.total_constituents == 2
    assert stats.priced_constituents == 1
    assert stats.missing_prices == 1


def test_compute_equal_weight_return_uses_adjusted_price_ratio() -> None:
    # 复权价比率：(close_t * adj_t) / (close_prev * adj_prev) - 1。
    # 除息日 close 与前收相等（当日价格收益 0），复权因子跳升体现分红，
    # 总收益应为约 5.263%，而不是 close/pre_close 给出的 0。
    constituents = _frame(
        [
            {"ts_code": "000001.SZ", "name": "Alpha", "keyword": "CAT", "forced": False},
        ]
    )
    daily_prices = _frame(
        [
            {"ts_code": "000001.SZ", "close": 9.5, "pre_close": 9.5},
        ]
    )
    prev_daily_prices = _frame(
        [
            {"ts_code": "000001.SZ", "close": 9.5},
        ]
    )
    adj_factors = _frame([{"ts_code": "000001.SZ", "adj_factor": 1.05263}])
    prev_adj_factors = _frame([{"ts_code": "000001.SZ", "adj_factor": 1.0}])

    index_ret, _, stats = compute_equal_weight_return(
        constituents, daily_prices, prev_daily_prices, adj_factors, prev_adj_factors
    )

    assert stats.priced_constituents == 1
    assert index_ret == pytest.approx(0.05263, abs=1e-4)
