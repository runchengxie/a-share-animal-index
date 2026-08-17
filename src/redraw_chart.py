from __future__ import annotations

import argparse
from pathlib import Path

from zoo_index.outputs import generate_chart, load_nav


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="使用已有 nav.csv 重新绘制净值曲线图。")
    parser.add_argument(
        "--nav",
        type=str,
        default="docs/nav.csv",
        help="nav.csv 路径。",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="docs/chart.png",
        help="输出图片路径。",
    )
    parser.add_argument(
        "--benchmark-label",
        type=str,
        default="HS300 ETF",
        help="图片中基准的展示名称。",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    nav_path = Path(args.nav)
    if not nav_path.exists():
        print(f"未找到 nav 文件：{nav_path}")
        return 1

    nav_df = load_nav(nav_path)
    if nav_df.empty:
        print(f"nav 文件为空：{nav_path}")
        return 1

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    generate_chart(out_path, nav_df, args.benchmark_label)
    print(f"图表已保存：{out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
