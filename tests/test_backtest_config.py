from pathlib import Path

from zoo_index.config import BacktestConfig, load_backtest_config


def test_backtest_config_defaults_to_zero_costs(tmp_path: Path) -> None:
    config = load_backtest_config(tmp_path / "missing.yml")

    assert config.enabled is False
    assert config.commission_rate == 0.0
    assert config.stamp_tax_rate == 0.0
    assert config.slippage_rate == 0.0


def test_backtest_config_loads_cost_rates(tmp_path: Path) -> None:
    path = tmp_path / "backtest.yml"
    path.write_text(
        "enabled: true\ncommission_rate: 0.001\nstamp_tax_rate: 0.005\nslippage_rate: 0.002\n",
        encoding="utf-8",
    )

    config = load_backtest_config(path)

    assert config == BacktestConfig(True, 0.001, 0.005, 0.002)
