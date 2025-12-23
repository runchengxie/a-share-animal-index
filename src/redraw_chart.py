from __future__ import annotations

import argparse
from pathlib import Path

from zoo_index.outputs import generate_chart, load_nav


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Redraw the chart using an existing nav.csv file."
    )
    parser.add_argument(
        "--nav",
        type=str,
        default="docs/nav.csv",
        help="Path to nav.csv.",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="docs/chart.png",
        help="Output chart path.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    nav_path = Path(args.nav)
    if not nav_path.exists():
        print(f"nav file not found: {nav_path}")
        return 1

    nav_df = load_nav(nav_path)
    if nav_df.empty:
        print(f"nav file is empty: {nav_path}")
        return 1

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    generate_chart(out_path, nav_df)
    print(f"chart saved: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
