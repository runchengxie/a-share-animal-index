from __future__ import annotations

import argparse
import os
from pathlib import Path

from zoo_index.runner import (
    DEFAULT_BACKFILL_YEARS,
    DEFAULT_BENCHMARK_CODE,
    DEFAULT_BENCHMARK_LABEL,
    DEFAULT_BENCHMARK_SOURCE,
    DEFAULT_INDEX_BENCHMARK_CODE,
    DEFAULT_INDEX_BENCHMARK_LABEL,
    BenchmarkConfig,
    RunConfig,
    run,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A股动物园指数每日运行脚本")
    parser.add_argument("--date", type=str, default="", help="交易日 YYYYMMDD")
    parser.add_argument("--rules", type=str, default="", help="规则文件路径")
    parser.add_argument("--token", type=str, default="", help="Tushare Token")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="",
        help="产物输出目录（默认仓库 docs 目录）",
    )
    parser.add_argument(
        "--backfill",
        type=int,
        nargs="?",
        const=-1,
        default=None,
        help="回填最近 N 个交易日（省略 N 则默认回填最近 5 年）",
    )
    parser.add_argument("--backfill-years", type=int, default=0, help="回填最近 N 年（按交易日历）")
    parser.add_argument(
        "--backfill-mode",
        type=str,
        choices=("missing", "all"),
        default="missing",
        help="回填模式：missing 补缺，all 全量重算",
    )
    parser.add_argument(
        "--backfill-write-snapshots",
        action="store_true",
        help="回填时写每日持仓快照",
    )
    parser.add_argument(
        "--no-rules-snapshot",
        action="store_true",
        help="回填时不写规则快照",
    )
    parser.add_argument(
        "--benchmark",
        type=str,
        default=DEFAULT_BENCHMARK_CODE,
        help="基准代码（默认 510300.SH，沪深300ETF）",
    )
    parser.add_argument(
        "--benchmark-source",
        type=str,
        choices=("index", "fund", "stock"),
        default=DEFAULT_BENCHMARK_SOURCE,
        help="基准数据源：index 指数 / fund ETF / stock A股",
    )
    parser.add_argument(
        "--benchmark-label",
        type=str,
        default="",
        help="基准展示名称（可选）",
    )
    parser.add_argument("--no-cache", action="store_true", help="不使用本地缓存")
    parser.add_argument("--force-refresh", action="store_true", help="忽略缓存并重新拉取")
    return parser.parse_args()


def _resolve_benchmark(args: argparse.Namespace) -> BenchmarkConfig:
    code = args.benchmark.strip().upper() or DEFAULT_BENCHMARK_CODE
    source = args.benchmark_source
    if source == "index" and code == DEFAULT_BENCHMARK_CODE:
        code = DEFAULT_INDEX_BENCHMARK_CODE

    if args.benchmark_label.strip():
        label = args.benchmark_label.strip()
    elif source == "fund" and code == DEFAULT_BENCHMARK_CODE:
        label = DEFAULT_BENCHMARK_LABEL
    elif source == "index" and code == DEFAULT_INDEX_BENCHMARK_CODE:
        label = DEFAULT_INDEX_BENCHMARK_LABEL
    else:
        label = f"Benchmark {code}"
    return BenchmarkConfig(code, source, label)


def build_run_config(args: argparse.Namespace, repo_root: Path) -> RunConfig | None:
    rules_path = Path(args.rules).resolve() if args.rules else repo_root / "rules.yml"
    token = args.token.strip() or os.getenv("TUSHARE_TOKEN", "").strip()

    if not rules_path.exists():
        print("规则文件不存在，请检查 rules.yml 路径。")
        return None

    if not token:
        print("缺少 Tushare Token，请设置环境变量 TUSHARE_TOKEN 或传入 --token。")
        return None

    benchmark = _resolve_benchmark(args)

    backfill_days = 0
    backfill_years = 0
    backfill_requested = False

    if args.backfill_years < 0:
        print("回填年份必须大于 0。")
        return None
    if args.backfill_years > 0:
        backfill_requested = True
        backfill_years = args.backfill_years

    if args.backfill is not None:
        backfill_requested = True
        if args.backfill == -1:
            if backfill_years > 0:
                print("请勿同时指定 --backfill 和 --backfill-years。")
                return None
            backfill_years = DEFAULT_BACKFILL_YEARS
        elif args.backfill > 0:
            if backfill_years > 0:
                print("请勿同时指定 --backfill 和 --backfill-years。")
                return None
            backfill_days = args.backfill
        else:
            print("回填天数必须大于 0。")
            return None

    output_dir = Path(args.output_dir).resolve() if args.output_dir else repo_root / "docs"

    return RunConfig(
        repo_root=repo_root,
        output_dir=output_dir,
        rules_path=rules_path,
        token=token,
        benchmark=benchmark,
        date=args.date.strip(),
        backfill_requested=backfill_requested,
        backfill_years=backfill_years,
        backfill_days=backfill_days,
        backfill_mode=args.backfill_mode,
        backfill_write_snapshots=args.backfill_write_snapshots,
        no_rules_snapshot=args.no_rules_snapshot,
        no_cache=args.no_cache,
        force_refresh=args.force_refresh,
    )


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    config = build_run_config(args, repo_root)
    if config is None:
        return 1
    return run(config)


if __name__ == "__main__":
    raise SystemExit(main())
