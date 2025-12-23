from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from zoo_index.config import load_rules
from zoo_index.data_sources.tushare import TushareClient
from zoo_index.index import build_constituents, compute_equal_weight_return, prepare_universe_asof
from zoo_index.outputs import (
    compute_changes,
    compute_suspected_noise,
    generate_chart,
    generate_badges,
    generate_index_html,
    generate_latest_json,
    load_nav,
    save_constituents,
    save_changes,
    save_holdings,
    update_nav,
)

DEFAULT_BACKFILL_YEARS = 2
DEFAULT_COMPLETE_LOOKBACK = 10


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A-share zoo index daily runner")
    parser.add_argument("--date", type=str, default="", help="交易日 YYYYMMDD")
    parser.add_argument("--rules", type=str, default="", help="规则文件路径")
    parser.add_argument("--token", type=str, default="", help="Tushare Token")
    parser.add_argument(
        "--backfill",
        type=int,
        nargs="?",
        const=-1,
        default=None,
        help="回填最近N个交易日（省略N则默认回填最近2年）",
    )
    parser.add_argument("--backfill-years", type=int, default=0, help="回填最近N年（按交易日历）")
    parser.add_argument(
        "--backfill-mode",
        type=str,
        choices=("missing", "all"),
        default="missing",
        help="回填模式：missing补缺，all全量重算",
    )
    parser.add_argument(
        "--backfill-write-snapshots",
        action="store_true",
        help="回填时写每日持仓快照",
    )
    parser.add_argument("--no-cache", action="store_true", help="不使用本地缓存")
    parser.add_argument("--force-refresh", action="store_true", help="忽略缓存并重新拉取")
    return parser.parse_args()


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


def _get_open_dates_in_range(
    client: TushareClient, start_date: str, end_date: str
) -> list[str]:
    df = client.get_trade_calendar_range(start_date, end_date)
    open_days = df[df["is_open"] == 1].copy()
    if open_days.empty:
        return []
    open_days["cal_date"] = open_days["cal_date"].astype(str)
    return open_days.sort_values("cal_date")["cal_date"].tolist()


def _is_trade_data_ready(client: TushareClient, trade_date: str) -> bool:
    daily = client.get_daily(trade_date)
    if daily.empty:
        return False
    hs300_df = client.get_index_daily(trade_date, "000300.SH")
    if hs300_df.empty:
        return False
    hs300_row = hs300_df.iloc[0]
    if pd.isna(hs300_row["pre_close"]) or float(hs300_row["pre_close"]) <= 0:
        return False
    return True


def _resolve_recent_complete_date(
    client: TushareClient,
    end_date: str,
    lookback_open_days: int = DEFAULT_COMPLETE_LOOKBACK,
) -> str:
    open_dates = client.get_recent_open_dates(end_date, lookback_open_days)
    for trade_date in reversed(open_dates):
        if _is_trade_data_ready(client, trade_date):
            return trade_date
    raise ValueError("no complete trading day found")


def _ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


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


def _month_first_open_date(
    client: TushareClient, date: str, cache: dict[str, str]
) -> str:
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
    nav_df["hs300_nav"] = (1 + nav_df["hs300_ret"]).cumprod()
    return nav_df


def _run_backfill(
    client: TushareClient,
    rules,
    target_dates: list[str],
    repo_root: Path,
    write_snapshots: bool,
    backfill_mode: str,
) -> int:
    target_dates = sorted(set(target_dates))
    if not target_dates:
        print("回填区间为空，未找到交易日。")
        return 1

    data_dir = repo_root / "data"
    docs_dir = repo_root / "docs"
    badges_dir = docs_dir / "badges"
    _ensure_dirs(data_dir, docs_dir, badges_dir)

    nav_path = docs_dir / "nav.csv"
    existing_nav = load_nav(nav_path)
    existing_dates = set(existing_nav["date"]) if not existing_nav.empty else set()
    if backfill_mode == "missing":
        run_dates = [date for date in target_dates if date not in existing_dates]
        if not run_dates:
            print("回填跳过：指定区间已存在，无需更新。")
            return 0
    else:
        run_dates = target_dates

    try:
        stock_basic = client.get_stock_basic()
    except Exception as exc:
        print(f"获取股票列表失败：{exc}")
        return 1

    try:
        namechange = client.get_namechange()
    except Exception as exc:
        print(f"获取历史简称失败：{exc}")
        return 1

    ret_rows: list[dict] = []
    last_date = ""
    last_strict_holdings = pd.DataFrame()
    last_extended_holdings = pd.DataFrame()
    last_strict_stats = None
    last_extended_stats = None
    last_strict_constituents = pd.DataFrame()
    last_extended_constituents = pd.DataFrame()
    month_cache: dict[str, str] = {}
    constituents_cache: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}

    for trade_date in run_dates:
        try:
            rebalance_date = _month_first_open_date(client, trade_date, month_cache)
            strict_df, extended_df = _get_constituents_for_rebalance(
                constituents_cache,
                stock_basic,
                namechange,
                rules,
                rebalance_date,
            )
        except Exception as exc:
            print(f"获取成分股失败({trade_date})：{exc}")
            return 1

        try:
            daily_prices = client.get_daily(trade_date)
        except Exception as exc:
            print(f"获取日行情失败({trade_date})：{exc}")
            return 1

        if daily_prices.empty:
            print(f"{trade_date} 日行情为空，无法计算指数。")
            return 1

        strict_ret, strict_holdings, strict_stats = compute_equal_weight_return(
            strict_df, daily_prices
        )
        extended_ret, extended_holdings, extended_stats = compute_equal_weight_return(
            extended_df, daily_prices
        )

        if strict_stats.priced_constituents == 0 or extended_stats.priced_constituents == 0:
            print(f"{trade_date} 成分股行情为空，无法计算指数。")
            return 1

        try:
            hs300_df = client.get_index_daily(trade_date, "000300.SH")
        except Exception as exc:
            print(f"获取沪深300行情失败({trade_date})：{exc}")
            return 1

        if hs300_df.empty:
            print(f"{trade_date} 沪深300行情为空，无法计算基准。")
            return 1

        hs300_row = hs300_df.iloc[0]
        if pd.isna(hs300_row["pre_close"]) or float(hs300_row["pre_close"]) <= 0:
            print(f"{trade_date} 沪深300前收异常，无法计算基准。")
            return 1

        hs300_ret = float(hs300_row["close"] / hs300_row["pre_close"] - 1)

        ret_rows.append(
            {
                "date": trade_date,
                "zoo_strict_ret": strict_ret,
                "zoo_extended_ret": extended_ret,
                "hs300_ret": hs300_ret,
            }
        )

        if write_snapshots:
            holdings_path = data_dir / f"holdings_{trade_date}.csv"
            save_holdings(holdings_path, strict_holdings, extended_holdings)

        last_date = trade_date
        last_strict_holdings = strict_holdings
        last_extended_holdings = extended_holdings
        last_strict_stats = strict_stats
        last_extended_stats = extended_stats
        last_strict_constituents = strict_df
        last_extended_constituents = extended_df

        print(
            "回填："
            f"日期 {trade_date}，严格 {strict_ret:.4%}，"
            f"扩展 {extended_ret:.4%}，沪深300 {hs300_ret:.4%}。"
        )

    existing_returns = (
        existing_nav[["date", "zoo_strict_ret", "zoo_extended_ret", "hs300_ret"]]
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

    if last_date:
        constituents_path = data_dir / f"constituents_{last_date}.csv"
        today_constituents = save_constituents(
            constituents_path, last_strict_constituents, last_extended_constituents
        )

        holdings_path = data_dir / f"holdings_{last_date}.csv"
        save_holdings(holdings_path, last_strict_holdings, last_extended_holdings)

        previous_constituents_path = _find_previous_snapshot(data_dir, "constituents", last_date)
        previous_constituents = (
            pd.read_csv(previous_constituents_path)
            if previous_constituents_path
            else pd.DataFrame()
        )

        changes = compute_changes(today_constituents, previous_constituents)
        suspected_noise = compute_suspected_noise(today_constituents)
        changes_path = data_dir / f"changes_{last_date}.json"
        save_changes(changes_path, last_date, changes, suspected_noise)

    generate_latest_json(docs_dir / "latest.json", latest)
    generate_badges(badges_dir, latest)
    generate_chart(docs_dir / "chart.png", nav_df)
    if last_strict_stats is None or last_extended_stats is None:
        print("回填失败，缺少统计数据。")
        return 1
    if last_date == latest["date"]:
        generate_index_html(docs_dir / "index.html", latest, last_strict_stats, last_extended_stats)
    else:
        print("回填已补充历史区间，最新日期未变化，跳过主页统计更新。")

    print(
        "回填完成："
        f"{len(nav_df)} 个交易日，最新 {latest['date']}，"
        f"严格 {latest['zoo_strict_nav']:.4f}，"
        f"扩展 {latest['zoo_extended_nav']:.4f}，"
        f"沪深300 {latest['hs300_nav']:.4f}。"
    )
    return 0


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    rules_path = Path(args.rules).resolve() if args.rules else repo_root / "rules.yml"
    token = args.token.strip() or os.getenv("TUSHARE_TOKEN", "").strip()

    if not rules_path.exists():
        print("规则文件不存在，请检查 rules.yml 路径。")
        return 1

    if not token:
        print("缺少 Tushare Token，请设置环境变量 TUSHARE_TOKEN 或传入 --token。")
        return 1

    rules = load_rules(rules_path)
    cache_dir = repo_root / "data" / "cache"
    client = TushareClient(
        token,
        cache_dir=cache_dir,
        use_cache=not args.no_cache,
        force_refresh=args.force_refresh,
    )

    date_arg = args.date.strip()
    backfill_days = 0
    backfill_years = 0
    backfill_requested = False

    if args.backfill_years < 0:
        print("回填年份必须大于 0。")
        return 1
    if args.backfill_years > 0:
        backfill_requested = True
        backfill_years = args.backfill_years

    if args.backfill is not None:
        backfill_requested = True
        if args.backfill == -1:
            if backfill_years > 0:
                print("请勿同时指定 --backfill 和 --backfill-years。")
                return 1
            backfill_years = DEFAULT_BACKFILL_YEARS
        elif args.backfill > 0:
            if backfill_years > 0:
                print("请勿同时指定 --backfill 和 --backfill-years。")
                return 1
            backfill_days = args.backfill
        else:
            print("回填天数必须大于 0。")
            return 1

    if backfill_requested:
        if date_arg:
            end_date = date_arg
        else:
            try:
                end_date = _resolve_recent_complete_date(client, _current_shanghai_date())
            except Exception as exc:
                _print_recent_complete_date_error(_current_shanghai_date(), exc)
                return 1
        try:
            if backfill_years > 0:
                start_date = _shift_years(end_date, backfill_years)
                open_dates = _get_open_dates_in_range(client, start_date, end_date)
            else:
                open_dates = client.get_recent_open_dates(end_date, backfill_days)
        except Exception as exc:
            print(f"获取交易日历失败：{exc}")
            return 1
        return _run_backfill(
            client,
            rules,
            open_dates,
            repo_root,
            args.backfill_write_snapshots,
            args.backfill_mode,
        )

    if date_arg:
        date = date_arg
    else:
        try:
            date = _resolve_recent_complete_date(client, _current_shanghai_date())
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

    data_dir = repo_root / "data"
    docs_dir = repo_root / "docs"
    nav_path = docs_dir / "nav.csv"
    existing_nav = load_nav(nav_path)
    if not existing_nav.empty:
        latest_date = max(existing_nav["date"])
        if date < latest_date:
            print(
                f"{date} 早于现有净值最新日期 {latest_date}，"
                "请使用回填模式重算历史区间。"
            )
            return 1

    try:
        stock_basic = client.get_stock_basic()
    except Exception as exc:
        print(f"获取股票列表失败：{exc}")
        return 1

    try:
        namechange = client.get_namechange()
    except Exception as exc:
        print(f"获取历史简称失败：{exc}")
        return 1

    month_cache: dict[str, str] = {}
    constituents_cache: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    try:
        rebalance_date = _month_first_open_date(client, date, month_cache)
        strict_df, extended_df = _get_constituents_for_rebalance(
            constituents_cache,
            stock_basic,
            namechange,
            rules,
            rebalance_date,
        )
    except Exception as exc:
        print(f"获取成分股失败：{exc}")
        return 1

    try:
        daily_prices = client.get_daily(date)
    except Exception as exc:
        print(f"获取日行情失败：{exc}")
        return 1

    if daily_prices.empty:
        print("日行情为空，无法计算指数。")
        return 1

    strict_ret, strict_holdings, strict_stats = compute_equal_weight_return(
        strict_df, daily_prices
    )
    extended_ret, extended_holdings, extended_stats = compute_equal_weight_return(
        extended_df, daily_prices
    )

    if strict_stats.priced_constituents == 0 or extended_stats.priced_constituents == 0:
        print("成分股行情为空，无法计算指数。")
        return 1

    try:
        hs300_df = client.get_index_daily(date, "000300.SH")
    except Exception as exc:
        print(f"获取沪深300行情失败：{exc}")
        return 1

    if hs300_df.empty:
        print("沪深300行情为空，无法计算基准。")
        return 1

    hs300_row = hs300_df.iloc[0]
    if pd.isna(hs300_row["pre_close"]) or float(hs300_row["pre_close"]) <= 0:
        print("沪深300前收异常，无法计算基准。")
        return 1

    hs300_ret = float(hs300_row["close"] / hs300_row["pre_close"] - 1)

    badges_dir = docs_dir / "badges"
    _ensure_dirs(data_dir, docs_dir, badges_dir)

    nav_df, latest = update_nav(nav_path, date, strict_ret, extended_ret, hs300_ret)

    constituents_path = data_dir / f"constituents_{date}.csv"
    today_constituents = save_constituents(constituents_path, strict_df, extended_df)

    holdings_path = data_dir / f"holdings_{date}.csv"
    save_holdings(holdings_path, strict_holdings, extended_holdings)

    previous_constituents_path = _find_previous_snapshot(data_dir, "constituents", date)
    previous_constituents = (
        pd.read_csv(previous_constituents_path)
        if previous_constituents_path
        else pd.DataFrame()
    )

    changes = compute_changes(today_constituents, previous_constituents)
    suspected_noise = compute_suspected_noise(today_constituents)
    changes_path = data_dir / f"changes_{date}.json"
    save_changes(changes_path, date, changes, suspected_noise)

    generate_latest_json(docs_dir / "latest.json", latest)
    generate_badges(badges_dir, latest)
    generate_chart(docs_dir / "chart.png", nav_df)
    generate_index_html(docs_dir / "index.html", latest, strict_stats, extended_stats)

    print(
        "已更新："
        f"日期 {date}，严格 {latest['zoo_strict_nav']:.4f}，"
        f"扩展 {latest['zoo_extended_nav']:.4f}，沪深300 {latest['hs300_nav']:.4f}。"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
