from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .config import Rules
from .matcher import Matcher


@dataclass(frozen=True)
class IndexStats:
    total_constituents: int
    priced_constituents: int
    missing_prices: int


def _filter_exchange(df: pd.DataFrame, allow_beijing: bool) -> pd.DataFrame:
    allowed = {"SSE", "SZSE"}
    if allow_beijing:
        allowed.add("BSE")
    return df[df["exchange"].isin(allowed)].copy()


def _filter_st(df: pd.DataFrame, exclude_st: bool) -> pd.DataFrame:
    if not exclude_st:
        return df
    mask = ~df["name"].str.contains("ST", na=False)
    return df[mask].copy()


def prepare_universe(stock_basic: pd.DataFrame, rules: Rules) -> pd.DataFrame:
    filtered = _filter_exchange(stock_basic, rules.allow_beijing)
    filtered = _filter_st(filtered, rules.exclude_st)
    return filtered


def build_constituents(stock_basic: pd.DataFrame, rules: Rules) -> tuple[pd.DataFrame, pd.DataFrame]:
    matcher = Matcher(rules)
    strict_rows: list[dict] = []
    extended_rows: list[dict] = []

    for row in stock_basic.itertuples(index=False):
        ts_code = row.ts_code
        name = row.name
        if pd.isna(name):
            name = ""
        if not isinstance(name, str):
            name = str(name)
        result = matcher.classify(ts_code, name)

        if result.strict:
            strict_rows.append(
                {
                    "ts_code": ts_code,
                    "name": name,
                    "keyword": result.strict_keyword or "",
                    "forced": result.forced,
                }
            )
        if result.extended:
            extended_rows.append(
                {
                    "ts_code": ts_code,
                    "name": name,
                    "keyword": result.extended_keyword or "",
                    "forced": result.forced,
                }
            )

    strict_df = pd.DataFrame(strict_rows)
    extended_df = pd.DataFrame(extended_rows)
    return strict_df, extended_df


def compute_equal_weight_return(
    constituents: pd.DataFrame, daily_prices: pd.DataFrame
) -> tuple[float, pd.DataFrame, IndexStats]:
    if constituents.empty:
        return 0.0, constituents, IndexStats(0, 0, 0)

    merged = constituents.merge(daily_prices, on="ts_code", how="left")
    merged["ret"] = merged["close"] / merged["pre_close"] - 1

    valid = merged.dropna(subset=["ret", "close", "pre_close"]).copy()
    valid = valid[valid["pre_close"] > 0]

    total = len(merged)
    priced = len(valid)
    missing = total - priced

    if priced == 0:
        return 0.0, merged, IndexStats(total, 0, missing)

    valid["weight"] = 1.0 / priced
    index_return = float(valid["ret"].mean())

    holdings = valid[
        ["ts_code", "name", "keyword", "forced", "weight", "ret", "close", "pre_close"]
    ].copy()
    return index_return, holdings, IndexStats(total, priced, missing)
