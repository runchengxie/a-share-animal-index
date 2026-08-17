from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from zoo_index.config import load_rules
from zoo_index.data_sources.tushare import TushareClient, TushareLike
from zoo_index.index import (
    IndexStats,
    build_constituents,
    compute_equal_weight_return,
    prepare_universe_asof,
)
from zoo_index.outputs import (
    compute_changes,
    compute_suspected_noise,
    generate_badges,
    generate_changes_json,
    generate_chart,
    generate_constituents_json,
    generate_history_json,
    generate_latest_json,
    generate_metadata_json,
    load_nav,
    save_changes,
    save_constituents,
    save_holdings,
    update_nav,
)

DEFAULT_BACKFILL_YEARS = 5
DEFAULT_COMPLETE_LOOKBACK = 10
DEFAULT_BENCHMARK_CODE = "510300.SH"
DEFAULT_BENCHMARK_SOURCE = "fund"
DEFAULT_BENCHMARK_LABEL = "HS300 ETF"
DEFAULT_INDEX_BENCHMARK_CODE = "000300.SH"
DEFAULT_INDEX_BENCHMARK_LABEL = "HS300"


@dataclass(frozen=True)
class BenchmarkConfig:
    code: str
    source: str
    label: str


@dataclass
class DailyResult:
    date: str
    strict_ret: float
    extended_ret: float
    benchmark_ret: float
    strict_df: pd.DataFrame
    extended_df: pd.DataFrame
    strict_holdings: pd.DataFrame
    extended_holdings: pd.DataFrame
    strict_stats: IndexStats
    extended_stats: IndexStats
    benchmark_code: str
    benchmark_label: str


@dataclass
class RunConfig:
    repo_root: Path
    output_dir: Path
    rules_path: Path
    token: str
    benchmark: BenchmarkConfig
    date: str = ""
    backfill_requested: bool = False
    backfill_years: int = 0
    backfill_days: int = 0
    backfill_mode: str = "missing"
    backfill_write_snapshots: bool = False
    no_rules_snapshot: bool = False
    no_cache: bool = False
    force_refresh: bool = False


def _current_shanghai_date() -> str:
    return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")


def _print_recent_complete_date_error(end_date: str, exc: Exception) -> None:
    print(f"获取最近完整交易日失败：{exc}")
    print(
        "提示：如果系统时间不准确或指定日期太新，"
        f"请用 --date 指定一个已存在数据的交易日（当前为 {end_date}），"
        "例如 --date 20240102。"
    )


def _shift_years(date_value: str, years: int) -> str:
    if years <= 0:
        raise ValueError("years must be positive")
    current = datetime.strptime(date_value, "%Y%m%d")
    target_year = current.year - years
    try:
        shifted = current.replace(year=target_year)
    except ValueError:
        shifted = current.replace(year=target_year, month=2, day=28)
    return shifted.strftime("%Y%m%d")


def _get_open_dates_in_range(client: TushareLike, start_date: str, end_date: str) -> list[str]:
    df = client.get_trade_calendar_range(start_date, end_date)
    open_days = df[df["is_open"] == 1].copy()
    if open_days.empty:
        return []
    open_days["cal_date"] = open_days["cal_date"].astype(str)
    return open_days.sort_values("cal_date")["cal_date"].tolist()


def _is_benchmark_data_ready(
    client: TushareLike,
    trade_date: str,
    benchmark: BenchmarkConfig,
    daily_prices: pd.DataFrame | None = None,
) -> bool:
    if benchmark.source == "index":
        df = client.get_index_daily(trade_date, benchmark.code)
        if df.empty:
            return False
        row = df.iloc[0]
    elif benchmark.source == "fund":
        df = client.get_fund_daily(trade_date, benchmark.code)
        if df.empty:
            return False
        row = df.iloc[0]
    elif benchmark.source == "stock":
        if daily_prices is None:
            daily_prices = client.get_daily(trade_date)
        row_slice = daily_prices[daily_prices["ts_code"] == benchmark.code]
        if row_slice.empty:
            return False
        row = row_slice.iloc[0]
    else:
        raise ValueError(f"unknown benchmark source: {benchmark.source}")

    return not (pd.isna(row["pre_close"]) or float(row["pre_close"]) <= 0)


def _is_trade_data_ready(client: TushareLike, trade_date: str, benchmark: BenchmarkConfig) -> bool:
    daily = client.get_daily(trade_date)
    if daily.empty:
        return False
    return _is_benchmark_data_ready(client, trade_date, benchmark, daily)


def _resolve_recent_complete_date(
    client: TushareLike,
    end_date: str,
    benchmark: BenchmarkConfig,
    lookback_open_days: int = DEFAULT_COMPLETE_LOOKBACK,
) -> str:
    open_dates = client.get_recent_open_dates(end_date, lookback_open_days)
    for trade_date in reversed(open_dates):
        if _is_trade_data_ready(client, trade_date, benchmark):
            return trade_date
    raise ValueError("no complete trading day found")


def _resolve_previous_open_date(client: TushareLike, trade_date: str) -> str:
    recent = client.get_recent_open_dates(trade_date, 2)
    if len(recent) < 2:
        raise ValueError("not enough open trading days")
    return recent[-2]


def _compute_benchmark_daily_return(close: float, pre_close: float) -> float:
    if pre_close <= 0:
        raise ValueError("基准前收异常")
    return close / pre_close - 1


def _index_benchmark_return(client: TushareLike, trade_date: str, code: str) -> float:
    df = client.get_index_daily(trade_date, code)
    if df.empty:
        raise ValueError("基准行情为空")
    row = df.iloc[0]
    if pd.isna(row["pre_close"]):
        raise ValueError("基准前收异常")
    return _compute_benchmark_daily_return(float(row["close"]), float(row["pre_close"]))


def _fund_benchmark_return(client: TushareLike, trade_date: str, code: str) -> float:
    df = client.get_fund_daily(trade_date, code)
    if df.empty:
        raise ValueError("基准行情为空")
    row = df.iloc[0]
    if pd.isna(row["pre_close"]):
        raise ValueError("基准前收异常")
    return _compute_benchmark_daily_return(float(row["close"]), float(row["pre_close"]))


def _stock_benchmark_return(
    client: TushareLike,
    trade_date: str,
    code: str,
    daily_prices: pd.DataFrame | None,
) -> float:
    if daily_prices is None:
        daily_prices = client.get_daily(trade_date)
    row_slice = daily_prices[daily_prices["ts_code"] == code]
    if row_slice.empty:
        raise ValueError("基准行情为空")
    row = row_slice.iloc[0]
    if pd.isna(row["pre_close"]):
        raise ValueError("基准前收异常")
    return _compute_benchmark_daily_return(float(row["close"]), float(row["pre_close"]))


def _get_benchmark_return(
    client: TushareLike,
    trade_date: str,
    prev_date: str,
    benchmark: BenchmarkConfig,
    daily_prices: pd.DataFrame | None = None,
) -> float:
    source = benchmark.source
    code = benchmark.code
    if source == "index":
        return _index_benchmark_return(client, trade_date, code)
    if source == "fund":
        return _fund_benchmark_return(client, trade_date, code)
    if source == "stock":
        return _stock_benchmark_return(client, trade_date, code, daily_prices)
    raise ValueError(f"unknown benchmark source: {source}")


def _ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def _snapshot_rules(rules_path: Path, data_dir: Path, start_date: str, end_date: str) -> Path:
    timestamp = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d%H%M%S")
    snapshot_path = data_dir / f"rules_snapshot_{start_date}_{end_date}_{timestamp}.yml"
    import shutil

    _ensure_dirs(data_dir)
    shutil.copy2(rules_path, snapshot_path)
    return snapshot_path


def _find_previous_snapshot(data_dir: Path, prefix: str, date: str) -> Path | None:
    candidates: list[tuple[str, Path]] = []
    prefix_value = f"{prefix}_"
    for path in data_dir.glob(f"{prefix}_*.csv"):
        stem = path.stem
        if not stem.startswith(prefix_value):
            continue
        file_date = stem[len(prefix_value) :]
        if file_date.isdigit() and file_date < date:
            candidates.append((file_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _month_first_open_date(client: TushareLike, date: str, cache: dict[str, str]) -> str:
    month_key = date[:6]
    if month_key in cache:
        return cache[month_key]
    start_date = f"{month_key}01"
    df = client.get_trade_calendar_range(start_date, date)
    open_days = df[df["is_open"] == 1].copy()
    if open_days.empty:
        raise ValueError("no open trading day found")
    open_days["cal_date"] = open_days["cal_date"].astype(str)
    first_date = open_days.sort_values("cal_date").iloc[0]["cal_date"]
    cache[month_key] = first_date
    return first_date


def _get_constituents_for_rebalance(
    cache: dict[str, tuple[pd.DataFrame, pd.DataFrame]],
    stock_basic: pd.DataFrame,
    namechange: pd.DataFrame,
    rules,
    rebalance_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if rebalance_date in cache:
        return cache[rebalance_date]
    universe = prepare_universe_asof(stock_basic, namechange, rebalance_date, rules)
    strict_df, extended_df = build_constituents(universe, rules)
    if strict_df.empty or extended_df.empty:
        raise ValueError("constituents is empty")
    cache[rebalance_date] = (strict_df, extended_df)
    return cache[rebalance_date]


def _build_nav_from_returns(ret_df: pd.DataFrame) -> pd.DataFrame:
    nav_df = ret_df.sort_values("date").copy()
    nav_df["zoo_strict_nav"] = (1 + nav_df["zoo_strict_ret"]).cumprod()
    nav_df["zoo_extended_nav"] = (1 + nav_df["zoo_extended_ret"]).cumprod()
    nav_df["benchmark_nav"] = (1 + nav_df["benchmark_ret"]).cumprod()
    return nav_df


def compute_day(
    client: TushareLike,
    rules,
    benchmark: BenchmarkConfig,
    date: str,
    stock_basic: pd.DataFrame,
    namechange: pd.DataFrame,
    constituents_cache: dict[str, tuple[pd.DataFrame, pd.DataFrame]] | None = None,
) -> DailyResult:
    """计算指定交易日的双指数收益与成分。daily 与 backfill 共用此函数。"""
    month_cache: dict[str, str] = {}
    rebalance_date = _month_first_open_date(client, date, month_cache)
    strict_df, extended_df = _get_constituents_for_rebalance(
        constituents_cache if constituents_cache is not None else {},
        stock_basic,
        namechange,
        rules,
        rebalance_date,
    )

    daily_prices = client.get_daily(date)
    if daily_prices.empty:
        raise ValueError(f"{date} 日行情为空，无法计算指数。")

    prev_date = _resolve_previous_open_date(client, date)
    adj_factors = client.get_adj_factor(date)
    prev_adj_factors = client.get_adj_factor(prev_date)
    if adj_factors.empty or prev_adj_factors.empty:
        raise ValueError(f"{date} 复权因子为空，无法计算指数。")

    prev_daily = client.get_daily(prev_date)
    suspended: set[str] = set()
    try:
        suspension_df = client.get_suspension(date)
        if not suspension_df.empty and "ts_code" in suspension_df.columns:
            suspended = set(suspension_df["ts_code"].astype(str).tolist())
    except Exception as exc:
        print(f"获取停牌信息失败，当日按无停牌处理：{exc}")

    strict_ret, strict_holdings, strict_stats = compute_equal_weight_return(
        strict_df,
        daily_prices,
        prev_daily,
        adj_factors,
        prev_adj_factors,
        suspended=suspended,
    )
    extended_ret, extended_holdings, extended_stats = compute_equal_weight_return(
        extended_df,
        daily_prices,
        prev_daily,
        adj_factors,
        prev_adj_factors,
        suspended=suspended,
    )

    if strict_stats.priced_constituents == 0 or extended_stats.priced_constituents == 0:
        raise ValueError(f"{date} 成分股行情为空，无法计算指数。")

    benchmark_ret = _get_benchmark_return(
        client,
        date,
        prev_date,
        benchmark,
        daily_prices=daily_prices,
    )

    return DailyResult(
        date=date,
        strict_ret=strict_ret,
        extended_ret=extended_ret,
        benchmark_ret=benchmark_ret,
        strict_df=strict_df,
        extended_df=extended_df,
        strict_holdings=strict_holdings,
        extended_holdings=extended_holdings,
        strict_stats=strict_stats,
        extended_stats=extended_stats,
        benchmark_code=benchmark.code,
        benchmark_label=benchmark.label,
    )


def _write_public_outputs(
    output_dir: Path,
    nav_df: pd.DataFrame,
    latest: pd.Series,
    result: DailyResult,
    benchmark_source: str,
) -> None:
    docs_dir = output_dir
    badges_dir = docs_dir / "badges"
    _ensure_dirs(badges_dir)

    generate_latest_json(
        docs_dir / "latest.json",
        latest,
        result.benchmark_code,
        result.benchmark_label,
    )
    generate_history_json(docs_dir / "history.json", nav_df, result.benchmark_label)
    generate_constituents_json(
        docs_dir / "constituents.json",
        result.date,
        result.strict_df,
        result.extended_df,
    )
    generate_metadata_json(
        docs_dir / "metadata.json",
        result.benchmark_code,
        result.benchmark_label,
        benchmark_source,
        result.date,
    )
    generate_badges(badges_dir, latest, result.benchmark_label)
    generate_chart(docs_dir / "chart.png", nav_df, result.benchmark_label)


def _write_changes_snapshot(
    output_dir: Path,
    date: str,
    strict_df: pd.DataFrame,
    extended_df: pd.DataFrame,
    strict_holdings: pd.DataFrame,
    extended_holdings: pd.DataFrame,
) -> None:
    constituents_path = output_dir / f"constituents_{date}.csv"
    today_constituents = save_constituents(constituents_path, strict_df, extended_df)

    holdings_path = output_dir / f"holdings_{date}.csv"
    save_holdings(holdings_path, strict_holdings, extended_holdings)

    previous_constituents_path = _find_previous_snapshot(output_dir, "constituents", date)
    previous_constituents = (
        pd.read_csv(previous_constituents_path) if previous_constituents_path else pd.DataFrame()
    )

    changes = compute_changes(today_constituents, previous_constituents)
    suspected_noise = compute_suspected_noise(today_constituents)
    changes_path = output_dir / f"changes_{date}.json"
    save_changes(changes_path, date, changes, suspected_noise)

    generate_changes_json(output_dir / "changes.json", date, changes, suspected_noise)


def run_daily(config: RunConfig, client: TushareLike | None = None) -> int:
    if client is None:
        client = _build_client(config)
    rules = load_rules(config.rules_path)

    date_arg = config.date.strip()
    if date_arg:
        date = date_arg
    else:
        try:
            date = _resolve_recent_complete_date(client, _current_shanghai_date(), config.benchmark)
        except Exception as exc:
            _print_recent_complete_date_error(_current_shanghai_date(), exc)
            return 1

    try:
        calendar = client.get_trade_calendar(date)
    except Exception as exc:
        print(f"获取交易日历失败：{exc}")
        return 1

    if not calendar.is_open:
        print(f"{date} 非交易日，已跳过。")
        return 0

    nav_path = config.output_dir / "nav.csv"
    existing_nav = load_nav(nav_path)
    if not existing_nav.empty:
        latest_date = max(existing_nav["date"])
        if date < latest_date:
            print(f"{date} 早于现有净值最新日期 {latest_date}，请使用回填模式重算历史区间。")
            return 1

    try:
        stock_basic = client.get_stock_basic()
        namechange = client.get_namechange()
    except Exception as exc:
        print(f"获取股票列表失败：{exc}")
        return 1

    try:
        result = compute_day(client, rules, config.benchmark, date, stock_basic, namechange)
    except Exception as exc:
        print(f"计算指数失败（{date}）：{exc}")
        return 1

    _ensure_dirs(config.output_dir)
    nav_df, latest = update_nav(
        nav_path,
        result.date,
        result.strict_ret,
        result.extended_ret,
        result.benchmark_ret,
    )

    _write_changes_snapshot(
        config.output_dir,
        result.date,
        result.strict_df,
        result.extended_df,
        result.strict_holdings,
        result.extended_holdings,
    )
    _write_public_outputs(config.output_dir, nav_df, latest, result, config.benchmark.source)

    print(
        "已更新："
        f"日期 {result.date}，严格 {latest['zoo_strict_nav']:.4f}，"
        f"扩展 {latest['zoo_extended_nav']:.4f}，"
        f"{result.benchmark_label} {latest['benchmark_nav']:.4f}。"
    )
    return 0


def _backfill_run_days(
    client: TushareLike,
    rules,
    benchmark: BenchmarkConfig,
    run_dates: list[str],
    stock_basic: pd.DataFrame,
    namechange: pd.DataFrame,
    output_dir: Path,
    write_snapshots: bool,
) -> tuple[list[dict], DailyResult | None]:
    constituents_cache: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    ret_rows: list[dict] = []
    last_result: DailyResult | None = None

    for trade_date in run_dates:
        result = compute_day(
            client, rules, benchmark, trade_date, stock_basic, namechange, constituents_cache
        )
        ret_rows.append(
            {
                "date": result.date,
                "zoo_strict_ret": result.strict_ret,
                "zoo_extended_ret": result.extended_ret,
                "benchmark_ret": result.benchmark_ret,
            }
        )
        if write_snapshots:
            save_holdings(
                output_dir / f"holdings_{trade_date}.csv",
                result.strict_holdings,
                result.extended_holdings,
            )
        last_result = result
        print(
            "回填："
            f"日期 {trade_date}，严格 {result.strict_ret:.4%}，"
            f"扩展 {result.extended_ret:.4%}，{result.benchmark_label} {result.benchmark_ret:.4%}。"
        )

    return ret_rows, last_result


def _resolve_backfill_dates(client: TushareLike, config: RunConfig) -> list[str]:
    date_arg = config.date.strip()
    if date_arg:
        end_date = date_arg
    else:
        try:
            end_date = _resolve_recent_complete_date(
                client, _current_shanghai_date(), config.benchmark
            )
        except Exception as exc:
            _print_recent_complete_date_error(_current_shanghai_date(), exc)
            return []

    try:
        if config.backfill_years > 0:
            start_date = _shift_years(end_date, config.backfill_years)
            open_dates = _get_open_dates_in_range(client, start_date, end_date)
        else:
            open_dates = client.get_recent_open_dates(end_date, config.backfill_days)
    except Exception as exc:
        print(f"获取交易日历失败：{exc}")
        return []

    return sorted(set(open_dates))


def _write_backfill_outputs(
    output_dir: Path,
    nav_path: Path,
    existing_nav: pd.DataFrame,
    ret_rows: list[dict],
    last_result: DailyResult,
    benchmark_source: str,
) -> int:
    existing_returns = (
        existing_nav[["date", "zoo_strict_ret", "zoo_extended_ret", "benchmark_ret"]]
        if not existing_nav.empty
        else pd.DataFrame()
    )
    combined_returns = pd.concat([existing_returns, pd.DataFrame(ret_rows)], ignore_index=True)
    combined_returns = combined_returns.drop_duplicates(subset=["date"], keep="last")
    if combined_returns.empty:
        print("回填失败，缺少收益数据。")
        return 1
    nav_df = _build_nav_from_returns(combined_returns)
    nav_df.to_csv(nav_path, index=False)
    latest = nav_df.iloc[-1]

    _write_changes_snapshot(
        output_dir,
        last_result.date,
        last_result.strict_df,
        last_result.extended_df,
        last_result.strict_holdings,
        last_result.extended_holdings,
    )
    _write_public_outputs(output_dir, nav_df, latest, last_result, benchmark_source)

    print(
        "回填完成："
        f"{len(nav_df)} 个交易日，最新 {latest['date']}，"
        f"严格 {latest['zoo_strict_nav']:.4f}，"
        f"扩展 {latest['zoo_extended_nav']:.4f}，"
        f"{last_result.benchmark_label} {latest['benchmark_nav']:.4f}。"
    )
    return 0


def run_backfill(config: RunConfig, client: TushareLike | None = None) -> int:
    if client is None:
        client = _build_client(config)
    rules = load_rules(config.rules_path)

    open_dates = _resolve_backfill_dates(client, config)
    if not open_dates:
        print("回填区间为空，未找到交易日。")
        return 1

    output_dir = config.output_dir
    nav_path = output_dir / "nav.csv"
    existing_nav = load_nav(nav_path)
    existing_dates = set(existing_nav["date"]) if not existing_nav.empty else set()
    if config.backfill_mode == "missing":
        run_dates = [date for date in open_dates if date not in existing_dates]
        if not run_dates:
            print("回填跳过：指定区间已存在，无需更新。")
            return 0
    else:
        run_dates = open_dates

    if not config.no_rules_snapshot:
        snapshot_path = _snapshot_rules(config.rules_path, output_dir, run_dates[0], run_dates[-1])
        print(f"回填规则快照已保存：{snapshot_path}")

    try:
        stock_basic = client.get_stock_basic()
        namechange = client.get_namechange()
    except Exception as exc:
        print(f"获取股票列表失败：{exc}")
        return 1

    try:
        ret_rows, last_result = _backfill_run_days(
            client,
            rules,
            config.benchmark,
            run_dates,
            stock_basic,
            namechange,
            output_dir,
            config.backfill_write_snapshots,
        )
    except Exception as exc:
        print(f"回填计算失败：{exc}")
        return 1

    if last_result is None:
        print("回填失败，缺少收益数据。")
        return 1

    return _write_backfill_outputs(
        output_dir, nav_path, existing_nav, ret_rows, last_result, config.benchmark.source
    )


def _build_client(config: RunConfig) -> TushareClient:
    cache_dir = config.repo_root / "data" / "cache"
    # 备用 Token（token2）走转发代理，主 Token 失败时回退；两者皆空则只走官方。
    token2 = os.getenv("TUSHARE_TOKEN_2", "").strip() or None
    api_url2 = os.getenv("TUSHARE_API_URL", "").strip() or None
    return TushareClient(
        config.token,
        cache_dir=cache_dir,
        use_cache=not config.no_cache,
        force_refresh=config.force_refresh,
        token2=token2,
        api_url2=api_url2,
    )


def run(config: RunConfig) -> int:
    if config.backfill_requested:
        return run_backfill(config)
    return run_daily(config)
