from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

# 兼容旧字段名：历史 nav.csv 可能仍使用 hs300_ret / hs300_nav。
_LEGACY_COLUMNS = {
    "hs300_ret": "benchmark_ret",
    "hs300_nav": "benchmark_nav",
}


def _variant_slice(df: pd.DataFrame, variant: str) -> pd.DataFrame:
    if df.empty or "variant" not in df.columns:
        return df.iloc[0:0]
    return df[df["variant"] == variant]


def load_nav(nav_path: Path) -> pd.DataFrame:
    if not nav_path.exists():
        return pd.DataFrame()
    df = pd.read_csv(nav_path, dtype={"date": str})
    for legacy, new in _LEGACY_COLUMNS.items():
        if legacy in df.columns and new not in df.columns:
            df = df.rename(columns={legacy: new})
    return df


def update_nav(
    nav_path: Path,
    date: str,
    strict_ret: float,
    extended_ret: float,
    benchmark_ret: float,
) -> tuple[pd.DataFrame, pd.Series]:
    nav_df = load_nav(nav_path)
    if not nav_df.empty:
        nav_df = nav_df[nav_df["date"] != date].copy()

    if nav_df.empty:
        prev_strict = 1.0
        prev_extended = 1.0
        prev_benchmark = 1.0
    else:
        nav_df = nav_df.sort_values("date")
        last = nav_df.iloc[-1]
        prev_strict = float(last["zoo_strict_nav"])
        prev_extended = float(last["zoo_extended_nav"])
        prev_benchmark = float(last["benchmark_nav"])

    row = {
        "date": date,
        "zoo_strict_ret": strict_ret,
        "zoo_extended_ret": extended_ret,
        "benchmark_ret": benchmark_ret,
        "zoo_strict_nav": prev_strict * (1 + strict_ret),
        "zoo_extended_nav": prev_extended * (1 + extended_ret),
        "benchmark_nav": prev_benchmark * (1 + benchmark_ret),
    }

    nav_df = pd.concat([nav_df, pd.DataFrame([row])], ignore_index=True)
    nav_df = nav_df.sort_values("date")
    nav_df.to_csv(nav_path, index=False)
    latest = nav_df[nav_df["date"] == date].iloc[0]
    return nav_df, latest


def save_holdings(
    path: Path, strict_holdings: pd.DataFrame, extended_holdings: pd.DataFrame
) -> pd.DataFrame:
    strict = strict_holdings.copy()
    strict["variant"] = "strict"
    extended = extended_holdings.copy()
    extended["variant"] = "extended"
    combined = pd.concat([strict, extended], ignore_index=True)
    combined.to_csv(path, index=False)
    return combined


def save_constituents(
    path: Path, strict_constituents: pd.DataFrame, extended_constituents: pd.DataFrame
) -> pd.DataFrame:
    strict = strict_constituents.copy()
    strict["variant"] = "strict"
    extended = extended_constituents.copy()
    extended["variant"] = "extended"
    combined = pd.concat([strict, extended], ignore_index=True)
    combined.to_csv(path, index=False)
    return combined


def compute_changes(today: pd.DataFrame, previous: pd.DataFrame) -> dict:
    def _variant_changes(variant: str) -> dict:
        today_slice = _variant_slice(today, variant)
        prev_slice = _variant_slice(previous, variant)
        today_set = set(today_slice["ts_code"]) if "ts_code" in today_slice.columns else set()
        prev_set = set(prev_slice["ts_code"]) if "ts_code" in prev_slice.columns else set()
        new_codes = today_set - prev_set
        removed_codes = prev_set - today_set

        def _to_records(df: pd.DataFrame, codes: set[str]) -> list[dict]:
            if not codes or "ts_code" not in df.columns or "name" not in df.columns:
                return []
            filtered = df[df["ts_code"].isin(codes)][["ts_code", "name"]]
            return filtered.drop_duplicates().to_dict(orient="records")

        return {
            "new_in": _to_records(today, new_codes),
            "removed": _to_records(previous, removed_codes),
        }

    return {
        "strict": _variant_changes("strict"),
        "extended": _variant_changes("extended"),
    }


def compute_suspected_noise(constituents: pd.DataFrame) -> dict:
    def _variant_noise(variant: str) -> list[dict]:
        slice_df = _variant_slice(constituents, variant)
        if slice_df.empty or "keyword" not in slice_df.columns:
            return []
        keyword = slice_df["keyword"].fillna("").astype(str)
        mask = keyword.str.len() == 1
        if "forced" in slice_df.columns:
            mask &= ~slice_df["forced"].fillna(False).astype(bool)
        filtered = slice_df[mask]
        return filtered[["ts_code", "name", "keyword"]].drop_duplicates().to_dict(orient="records")

    return {
        "strict": _variant_noise("strict"),
        "extended": _variant_noise("extended"),
    }


def save_changes(path: Path, date: str, changes: dict, suspected_noise: dict | None = None) -> None:
    payload = {"date": date, "changes": changes}
    if suspected_noise is not None:
        payload["suspected_noise"] = suspected_noise
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def generate_changes_json(
    path: Path, date: str, changes: dict, suspected_noise: dict | None = None
) -> None:
    save_changes(path, date, changes, suspected_noise)


def generate_latest_json(
    path: Path,
    latest: pd.Series,
    benchmark_code: str = "510300.SH",
    benchmark_label: str = "HS300 ETF",
) -> None:
    payload = {
        "date": latest["date"],
        "zoo_strict_nav": round(float(latest["zoo_strict_nav"]), 6),
        "zoo_extended_nav": round(float(latest["zoo_extended_nav"]), 6),
        "benchmark_nav": round(float(latest["benchmark_nav"]), 6),
        "zoo_strict_daily": round(float(latest["zoo_strict_ret"]), 6),
        "zoo_extended_daily": round(float(latest["zoo_extended_ret"]), 6),
        "benchmark_daily": round(float(latest["benchmark_ret"]), 6),
        "zoo_strict_excess": round(float(latest["zoo_strict_ret"] - latest["benchmark_ret"]), 6),
        "zoo_extended_excess": round(
            float(latest["zoo_extended_ret"] - latest["benchmark_ret"]), 6
        ),
        "benchmark_code": benchmark_code,
        "benchmark_label": benchmark_label,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def generate_history_json(
    path: Path, nav_df: pd.DataFrame, benchmark_label: str = "HS300 ETF"
) -> None:
    if nav_df.empty:
        payload: list[dict] = []
    else:
        ordered = nav_df.sort_values("date")
        payload = [
            {
                "date": row["date"],
                "zoo_strict_ret": round(float(row["zoo_strict_ret"]), 8),
                "zoo_extended_ret": round(float(row["zoo_extended_ret"]), 8),
                "benchmark_ret": round(float(row["benchmark_ret"]), 8),
                "zoo_strict_nav": round(float(row["zoo_strict_nav"]), 6),
                "zoo_extended_nav": round(float(row["zoo_extended_nav"]), 6),
                "benchmark_nav": round(float(row["benchmark_nav"]), 6),
            }
            for _, row in ordered.iterrows()
        ]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def generate_constituents_json(
    path: Path, date: str, strict_df: pd.DataFrame, extended_df: pd.DataFrame
) -> None:
    def _to_records(df: pd.DataFrame) -> list[dict]:
        if df.empty:
            return []
        cols = [c for c in ("ts_code", "name", "keyword", "forced") if c in df.columns]
        return df[cols].to_dict(orient="records")

    payload = {
        "date": date,
        "strict": _to_records(strict_df),
        "extended": _to_records(extended_df),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def generate_metadata_json(
    path: Path,
    benchmark_code: str,
    benchmark_label: str,
    benchmark_source: str,
    date: str,
) -> None:
    payload = {
        "updated": date,
        "benchmark": {
            "code": benchmark_code,
            "label": benchmark_label,
            "source": benchmark_source,
        },
        "variants": ["strict", "extended"],
        "rebalance": "monthly",
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def generate_badges(
    badges_dir: Path, latest: pd.Series, benchmark_label: str = "HS300 ETF"
) -> None:
    badges_dir.mkdir(parents=True, exist_ok=True)
    items = [
        ("zoo_strict_nav", "Zoo Strict NAV", f"{latest['zoo_strict_nav']:.4f}", "2f855a"),
        (
            "zoo_extended_nav",
            "Zoo Extended NAV",
            f"{latest['zoo_extended_nav']:.4f}",
            "c05621",
        ),
        ("benchmark_nav", f"{benchmark_label} NAV", f"{latest['benchmark_nav']:.4f}", "3182ce"),
    ]
    for name, label, message, color in items:
        payload = {
            "schemaVersion": 1,
            "label": label,
            "message": message,
            "color": color,
        }
        path = badges_dir / f"{name}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    # 兼容期：保留旧名 hs300_nav.json，供既有 shields.io 链接过渡。
    legacy = {
        "schemaVersion": 1,
        "label": f"{benchmark_label} NAV",
        "message": f"{latest['benchmark_nav']:.4f}",
        "color": "3182ce",
    }
    (badges_dir / "hs300_nav.json").write_text(
        json.dumps(legacy, ensure_ascii=False), encoding="utf-8"
    )


def generate_chart(path: Path, nav_df: pd.DataFrame, benchmark_label: str = "HS300 ETF") -> None:
    if nav_df.empty:
        return

    nav_df = nav_df.sort_values("date").copy()
    dates = pd.to_datetime(nav_df["date"], format="%Y%m%d")

    import matplotlib.dates as mdates

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(dates, nav_df["zoo_strict_nav"], label="严格动物园", linewidth=1.6)
    ax.plot(dates, nav_df["zoo_extended_nav"], label="扩展动物园", linewidth=1.6)
    ax.plot(dates, nav_df["benchmark_nav"], label=benchmark_label, linewidth=1.6)

    locator = mdates.AutoDateLocator(minticks=6, maxticks=10)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))

    ax.set_xlabel("日期")
    ax.set_ylabel("净值")
    ax.set_title("A股动物园指数")
    ax.legend()
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)
