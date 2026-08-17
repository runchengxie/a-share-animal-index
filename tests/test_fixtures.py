from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from zoo_index.outputs import load_nav

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def test_sample_nav_normalizes_legacy_hs300_columns() -> None:
    nav = load_nav(FIXTURES_DIR / "sample_nav.csv")
    # 历史 nav.csv 使用 hs300_ret / hs300_nav，读取时应归一化为 benchmark_*。
    assert "benchmark_ret" in nav.columns
    assert "benchmark_nav" in nav.columns
    assert "hs300_ret" not in nav.columns
    assert not nav.empty
    assert (nav["benchmark_nav"] > 0).all()


def test_sample_constituents_loads() -> None:
    df = pd.read_csv(FIXTURES_DIR / "sample_constituents_20251219.csv")
    assert {"ts_code", "name", "variant"}.issubset(df.columns)
    assert set(df["variant"]).issubset({"strict", "extended"})


def test_sample_changes_json_loads() -> None:
    payload = json.loads(
        (FIXTURES_DIR / "sample_changes_20251219.json").read_text(encoding="utf-8")
    )
    assert "date" in payload
    assert "changes" in payload
    assert "suspected_noise" in payload
