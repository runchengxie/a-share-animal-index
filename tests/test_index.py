import pandas as pd

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
