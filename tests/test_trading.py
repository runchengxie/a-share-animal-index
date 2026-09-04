import pandas as pd
import pytest

from zoo_index.trading import (
    TradeCostConfig,
    compute_trade_accounting,
    drift_weights,
)


def test_drift_weights_reflects_price_changes_before_rebalance() -> None:
    previous = pd.Series({"A": 0.5, "B": 0.5})
    previous_prices = pd.Series({"A": 10.0, "B": 10.0})
    current_prices = pd.Series({"A": 20.0, "B": 10.0})

    drifted = drift_weights(previous, previous_prices, current_prices)

    assert drifted.to_dict() == pytest.approx({"A": 2 / 3, "B": 1 / 3})


def test_trade_accounting_calculates_turnover_and_side_specific_costs() -> None:
    accounting = compute_trade_accounting(
        previous_weights=pd.Series({"A": 0.5, "B": 0.5}),
        target_weights=pd.Series({"A": 0.5, "B": 0.5}),
        previous_prices=pd.Series({"A": 10.0, "B": 10.0}),
        current_prices=pd.Series({"A": 20.0, "B": 10.0}),
        tradable=pd.Series({"A": True, "B": True}),
        costs=TradeCostConfig(commission_rate=0.001, stamp_tax_rate=0.005, slippage_rate=0.002),
    )

    assert accounting.turnover == pytest.approx(1 / 6)
    assert accounting.commission == pytest.approx(1 / 3000)
    assert accounting.stamp_tax == pytest.approx(1 / 1200)
    assert accounting.slippage == pytest.approx(1 / 1500)
    assert accounting.total_cost == pytest.approx(1 / 3000 + 1 / 1200 + 1 / 1500)


def test_untradable_trade_is_pending_and_has_no_cost() -> None:
    accounting = compute_trade_accounting(
        previous_weights=pd.Series({"A": 0.5, "B": 0.5}),
        target_weights=pd.Series({"A": 1.0}),
        previous_prices=pd.Series({"A": 10.0, "B": 10.0}),
        current_prices=pd.Series({"A": 10.0, "B": 10.0}),
        tradable=pd.Series({"A": True, "B": False}),
        costs=TradeCostConfig(commission_rate=0.001, stamp_tax_rate=0.005, slippage_rate=0.002),
    )

    assert accounting.executed_weights.to_dict() == pytest.approx({"A": 0.5})
    assert accounting.pending_weights.to_dict() == pytest.approx({"B": -0.5})
    assert accounting.turnover == pytest.approx(0.25)
    assert accounting.stamp_tax == pytest.approx(0.0)
