import pandas as pd
import pytest

from zoo_index.index import compute_equal_weight_return


def test_compute_equal_weight_return_reweights_missing_prices() -> None:
    constituents = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "name": "Alpha", "keyword": "CAT", "forced": False},
            {"ts_code": "000002.SZ", "name": "Beta", "keyword": "DOG", "forced": False},
        ]
    )
    daily_prices = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "close": 10.0, "pre_close": 9.0},
            {"ts_code": "000002.SZ", "close": None, "pre_close": None},
        ]
    )

    index_ret, holdings, stats = compute_equal_weight_return(constituents, daily_prices)

    assert stats.total_constituents == 2
    assert stats.priced_constituents == 1
    assert stats.missing_prices == 1
    assert holdings["weight"].sum() == 1.0
    assert index_ret == (10.0 / 9.0 - 1)


def test_compute_equal_weight_return_uses_adj_factor() -> None:
    constituents = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "name": "Alpha", "keyword": "CAT", "forced": False},
        ]
    )
    daily_prices = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "close": 10.0, "pre_close": 9.0},
        ]
    )
    adj_factors = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "adj_factor": 2.0},
        ]
    )
    prev_adj_factors = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "adj_factor": 1.0},
        ]
    )

    index_ret, _, stats = compute_equal_weight_return(
        constituents, daily_prices, adj_factors, prev_adj_factors
    )

    assert stats.priced_constituents == 1
    assert index_ret == pytest.approx(10.0 / 9.0 * 2.0 - 1)
