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


def _normalize_date_series(series: pd.Series, default: int) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values.fillna(default).astype(int)


def _filter_listed_asof(df: pd.DataFrame, as_of: str) -> pd.DataFrame:
    if "list_date" not in df.columns or "delist_date" not in df.columns:
        return df.copy()
    as_of_value = int(as_of)
    list_dates = _normalize_date_series(df["list_date"], 99999999)
    delist_dates = _normalize_date_series(df["delist_date"], 99999999)
    mask = (list_dates <= as_of_value) & (delist_dates >= as_of_value)
    return df[mask].copy()


def _apply_namechange(df: pd.DataFrame, namechange: pd.DataFrame, as_of: str) -> pd.DataFrame:
    if namechange.empty:
        return df.copy()
    required = ["ts_code", "name", "start_date", "end_date"]
    if not set(required).issubset(namechange.columns):
        return df.copy()
    changes = namechange[required].copy()
    changes["start_date_int"] = _normalize_date_series(changes["start_date"], 0)
    changes["end_date_int"] = _normalize_date_series(changes["end_date"], 99999999)
    as_of_value = int(as_of)
    active = changes[
        (changes["start_date_int"] <= as_of_value) & (changes["end_date_int"] >= as_of_value)
    ]
    if active.empty:
        return df.copy()
    active = (
        active.sort_values(["ts_code", "start_date_int"])
        .drop_duplicates(subset=["ts_code"], keep="last")
        .loc[:, ["ts_code", "name"]]
    )
    merged = df.merge(active, on="ts_code", how="left", suffixes=("", "_asof"))
    merged["name"] = merged["name_asof"].fillna(merged["name"])
    return merged.drop(columns=["name_asof"])


def prepare_universe(stock_basic: pd.DataFrame, rules: Rules) -> pd.DataFrame:
    filtered = _filter_exchange(stock_basic, rules.allow_beijing)
    filtered = _filter_st(filtered, rules.exclude_st)
    return filtered


def prepare_universe_asof(
    stock_basic: pd.DataFrame, namechange: pd.DataFrame, as_of: str, rules: Rules
) -> pd.DataFrame:
    filtered = _filter_listed_asof(stock_basic, as_of)
    filtered = _apply_namechange(filtered, namechange, as_of)
    filtered = _filter_exchange(filtered, rules.allow_beijing)
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
    constituents: pd.DataFrame,
    daily_prices: pd.DataFrame,
    adj_factors: pd.DataFrame | None = None,
    prev_adj_factors: pd.DataFrame | None = None,
) -> tuple[float, pd.DataFrame, IndexStats]:
    if constituents.empty:
        return 0.0, constituents, IndexStats(0, 0, 0)

    merged = constituents.merge(daily_prices, on="ts_code", how="left")
    if adj_factors is not None and prev_adj_factors is not None:
        merged = merged.merge(
            adj_factors[["ts_code", "adj_factor"]],
            on="ts_code",
            how="left",
        )
        prev_factors = prev_adj_factors[["ts_code", "adj_factor"]].rename(
            columns={"adj_factor": "prev_adj_factor"}
        )
        merged = merged.merge(prev_factors, on="ts_code", how="left")
        merged["adj_factor"] = pd.to_numeric(merged["adj_factor"], errors="coerce")
        merged["prev_adj_factor"] = pd.to_numeric(merged["prev_adj_factor"], errors="coerce")
        merged.loc[merged["adj_factor"] <= 0, "adj_factor"] = pd.NA
        merged.loc[merged["prev_adj_factor"] <= 0, "prev_adj_factor"] = pd.NA
        merged["ret"] = merged["close"] / merged["pre_close"] * (
            merged["adj_factor"] / merged["prev_adj_factor"]
        ) - 1
    else:
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
