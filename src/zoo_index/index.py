from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

from .config import Rules
from .matcher import Matcher


@dataclass(frozen=True)
class IndexStats:
    total_constituents: int
    priced_constituents: int
    missing_prices: int


@dataclass
class VariantState:
    """单一变体（strict / extended）的有状态组合。"""

    weights: dict[str, float]
    constituents: pd.DataFrame
    reason: str
    susp_days: dict[str, int] = field(default_factory=dict)


@dataclass
class PortfolioState:
    """双变体组合在某一交易日的快照，供次日计算去前视使用。"""

    date: str
    strict: VariantState
    extended: VariantState


def _equal_weights(constituents: pd.DataFrame) -> dict[str, float]:
    codes = constituents["ts_code"].tolist()
    if not codes:
        return {}
    weight = 1.0 / len(codes)
    return {code: weight for code in codes}


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


def _filter_min_listing_age(df: pd.DataFrame, as_of: str, min_listing_days: int) -> pd.DataFrame:
    if min_listing_days <= 0 or "list_date" not in df.columns:
        return df.copy()
    as_of_date = datetime.strptime(as_of, "%Y%m%d").date()
    list_dates = pd.to_datetime(
        df["list_date"].astype(str), format="%Y%m%d", errors="coerce"
    ).dt.date
    # 上市满 min_listing_days 个自然日才纳入（按真实日历差，非 YYYYMMDD 整数差）。
    days_listed = [(as_of_date - d).days if pd.notna(d) else 10**9 for d in list_dates]
    mask = pd.Series(days_listed) >= min_listing_days
    return df[mask].copy()


def prepare_universe_asof(
    stock_basic: pd.DataFrame, namechange: pd.DataFrame, as_of: str, rules: Rules
) -> pd.DataFrame:
    filtered = _filter_listed_asof(stock_basic, as_of)
    filtered = _apply_namechange(filtered, namechange, as_of)
    filtered = _filter_exchange(filtered, rules.allow_beijing)
    filtered = _filter_st(filtered, rules.exclude_st)
    filtered = _filter_min_listing_age(filtered, as_of, rules.min_listing_days)
    return filtered


def build_constituents(
    stock_basic: pd.DataFrame, rules: Rules
) -> tuple[pd.DataFrame, pd.DataFrame]:
    matcher = Matcher(rules)
    strict_rows: list[dict] = []
    extended_rows: list[dict] = []

    for row in stock_basic.itertuples(index=False):
        ts_code = row.ts_code  # ty: ignore[unresolved-attribute]
        name = row.name  # ty: ignore[unresolved-attribute]
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
    prev_daily_prices: pd.DataFrame,
    adj_factors: pd.DataFrame | None = None,
    prev_adj_factors: pd.DataFrame | None = None,
    suspended: set[str] | None = None,
    weights: dict[str, float] | None = None,
) -> tuple[float, pd.DataFrame, IndexStats]:
    if constituents.empty:
        return 0.0, constituents, IndexStats(0, 0, 0)

    suspended = suspended or set()
    stateful = weights is not None

    merged = constituents.merge(daily_prices, on="ts_code", how="left")
    merged = merged.merge(
        prev_daily_prices[["ts_code", "close"]].rename(columns={"close": "prev_close_actual"}),
        on="ts_code",
        how="left",
    )

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
        merged["ret"] = (
            merged["close"]
            * merged["adj_factor"]
            / (merged["prev_close_actual"] * merged["prev_adj_factor"])
            - 1
        )
    else:
        merged["ret"] = merged["close"] / merged["prev_close_actual"] - 1

    no_price = merged["close"].isna()
    suspended_mask = no_price & merged["ts_code"].isin(suspended)
    if suspended_mask.any():
        # 停牌：无当日行情，但持仓与权重保留，当日收益计为 0。
        merged.loc[suspended_mask, "ret"] = 0.0
        merged.loc[suspended_mask, "close"] = merged.loc[suspended_mask, "prev_close_actual"]
        merged.loc[suspended_mask, "pre_close"] = merged.loc[suspended_mask, "prev_close_actual"]

    # 真实缺失：无行情且非停牌，warning 后排除出收益计算，但权重保留（不静默再分配）。
    genuine_missing = no_price & ~merged["ts_code"].isin(suspended)
    if genuine_missing.any():
        codes = merged.loc[genuine_missing, "ts_code"].tolist()
        print(f"警告：以下成分无行情且非停牌，已排除出当日指数收益：{codes}")

    total = len(merged)
    valid_ret = merged["ret"].notna() & (merged["prev_close_actual"] > 0)
    priced = int(valid_ret.sum())
    missing = total - priced

    if stateful:
        # 月度固定权重：指数收益 = Σ weight_i * ret_i，缺失/停牌成分贡献 0，不重新均分。
        merged["weight"] = merged["ts_code"].map(weights).fillna(0.0)
        merged["contrib"] = merged["weight"] * merged["ret"].fillna(0.0)
        index_return = float(merged["contrib"].sum())
        holdings = merged[
            ["ts_code", "name", "keyword", "forced", "weight", "ret", "close", "pre_close"]
        ].copy()
        # 缺失成分保留权重，收益记为 0。
        holdings["ret"] = holdings["ret"].fillna(0.0)
        return index_return, holdings, IndexStats(total, priced, missing)

    valid = merged[~genuine_missing].copy()
    valid = valid.dropna(subset=["ret"])
    valid = valid[valid["prev_close_actual"] > 0]

    if priced == 0:
        return 0.0, merged, IndexStats(total, 0, missing)

    valid["weight"] = 1.0 / priced
    index_return = float(valid["ret"].mean())

    holdings = valid[
        ["ts_code", "name", "keyword", "forced", "weight", "ret", "close", "pre_close"]
    ].copy()
    return index_return, holdings, IndexStats(total, priced, missing)
