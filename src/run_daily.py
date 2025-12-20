from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from zoo_index.config import load_rules
from zoo_index.data_sources.tushare import TushareClient
from zoo_index.index import build_constituents, compute_equal_weight_return, prepare_universe
from zoo_index.outputs import (
    compute_changes,
    generate_chart,
    generate_index_html,
    generate_latest_json,
    save_changes,
    save_holdings,
    update_nav,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A-share zoo index daily runner")
    parser.add_argument("--date", type=str, default="", help="交易日 YYYYMMDD")
    parser.add_argument("--rules", type=str, default="", help="规则文件路径")
    parser.add_argument("--token", type=str, default="", help="Tushare Token")
    return parser.parse_args()


def _today() -> str:
    return datetime.now().strftime("%Y%m%d")


def _ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def _find_previous_holdings(data_dir: Path, date: str) -> Path | None:
    candidates: list[tuple[str, Path]] = []
    for path in data_dir.glob("holdings_*.csv"):
        stem = path.stem
        parts = stem.split("_")
        if len(parts) != 2:
            continue
        file_date = parts[1]
        if file_date.isdigit() and file_date < date:
            candidates.append((file_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    date = args.date.strip() or _today()
    rules_path = Path(args.rules).resolve() if args.rules else repo_root / "rules.yml"
    token = args.token.strip() or os.getenv("TUSHARE_TOKEN", "").strip()

    if not rules_path.exists():
        print("规则文件不存在，请检查 rules.yml 路径。")
        return 1

    if not token:
        print("缺少 Tushare Token，请设置环境变量 TUSHARE_TOKEN 或传入 --token。")
        return 1

    rules = load_rules(rules_path)
    client = TushareClient(token)

    try:
        calendar = client.get_trade_calendar(date)
    except Exception as exc:
        print(f"获取交易日历失败：{exc}")
        return 1

    if not calendar.is_open:
        print(f"{date} 非交易日，已跳过。")
        return 0

    try:
        stock_basic = client.get_stock_basic()
    except Exception as exc:
        print(f"获取股票列表失败：{exc}")
        return 1

    universe = prepare_universe(stock_basic, rules)
    strict_df, extended_df = build_constituents(universe, rules)

    if strict_df.empty or extended_df.empty:
        print("成分股为空，请检查规则配置或股票列表。")
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

    data_dir = repo_root / "data"
    docs_dir = repo_root / "docs"
    _ensure_dirs(data_dir, docs_dir)

    nav_path = data_dir / "nav.csv"
    nav_df, latest = update_nav(nav_path, date, strict_ret, extended_ret, hs300_ret)

    holdings_path = data_dir / f"holdings_{date}.csv"
    today_holdings = save_holdings(holdings_path, strict_holdings, extended_holdings)

    previous_holdings_path = _find_previous_holdings(data_dir, date)
    previous_holdings = (
        pd.read_csv(previous_holdings_path) if previous_holdings_path else pd.DataFrame()
    )

    changes = compute_changes(today_holdings, previous_holdings)
    changes_path = data_dir / f"changes_{date}.json"
    save_changes(changes_path, date, changes)

    generate_latest_json(docs_dir / "latest.json", latest)
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
