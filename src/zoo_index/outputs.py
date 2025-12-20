from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from .index import IndexStats


def _variant_slice(df: pd.DataFrame, variant: str) -> pd.DataFrame:
    if df.empty or "variant" not in df.columns:
        return df.iloc[0:0]
    return df[df["variant"] == variant]


def load_nav(nav_path: Path) -> pd.DataFrame:
    if not nav_path.exists():
        return pd.DataFrame()
    return pd.read_csv(nav_path, dtype={"date": str})


def update_nav(
    nav_path: Path,
    date: str,
    strict_ret: float,
    extended_ret: float,
    hs300_ret: float,
) -> tuple[pd.DataFrame, pd.Series]:
    nav_df = load_nav(nav_path)
    if not nav_df.empty:
        nav_df = nav_df[nav_df["date"] != date].copy()

    if nav_df.empty:
        prev_strict = 1.0
        prev_extended = 1.0
        prev_hs300 = 1.0
    else:
        nav_df = nav_df.sort_values("date")
        last = nav_df.iloc[-1]
        prev_strict = float(last["zoo_strict_nav"])
        prev_extended = float(last["zoo_extended_nav"])
        prev_hs300 = float(last["hs300_nav"])

    row = {
        "date": date,
        "zoo_strict_ret": strict_ret,
        "zoo_extended_ret": extended_ret,
        "hs300_ret": hs300_ret,
        "zoo_strict_nav": prev_strict * (1 + strict_ret),
        "zoo_extended_nav": prev_extended * (1 + extended_ret),
        "hs300_nav": prev_hs300 * (1 + hs300_ret),
    }

    nav_df = pd.concat([nav_df, pd.DataFrame([row])], ignore_index=True)
    nav_df = nav_df.sort_values("date")
    nav_df.to_csv(nav_path, index=False)
    latest = nav_df[nav_df["date"] == date].iloc[0]
    return nav_df, latest


def save_holdings(path: Path, strict_holdings: pd.DataFrame, extended_holdings: pd.DataFrame) -> pd.DataFrame:
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
        return (
            filtered[["ts_code", "name", "keyword"]]
            .drop_duplicates()
            .to_dict(orient="records")
        )

    return {
        "strict": _variant_noise("strict"),
        "extended": _variant_noise("extended"),
    }


def save_changes(path: Path, date: str, changes: dict, suspected_noise: dict | None = None) -> None:
    payload = {"date": date, "changes": changes}
    if suspected_noise is not None:
        payload["suspected_noise"] = suspected_noise
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def generate_latest_json(path: Path, latest: pd.Series) -> None:
    payload = {
        "date": latest["date"],
        "zoo_strict_nav": round(float(latest["zoo_strict_nav"]), 6),
        "zoo_extended_nav": round(float(latest["zoo_extended_nav"]), 6),
        "hs300_nav": round(float(latest["hs300_nav"]), 6),
        "zoo_strict_daily": round(float(latest["zoo_strict_ret"]), 6),
        "zoo_extended_daily": round(float(latest["zoo_extended_ret"]), 6),
        "hs300_daily": round(float(latest["hs300_ret"]), 6),
        "zoo_strict_excess": round(
            float(latest["zoo_strict_ret"] - latest["hs300_ret"]), 6
        ),
        "zoo_extended_excess": round(
            float(latest["zoo_extended_ret"] - latest["hs300_ret"]), 6
        ),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def generate_badges(badges_dir: Path, latest: pd.Series) -> None:
    badges_dir.mkdir(parents=True, exist_ok=True)
    items = [
        ("zoo_strict_nav", "Zoo Strict NAV", f"{latest['zoo_strict_nav']:.4f}", "2f855a"),
        (
            "zoo_extended_nav",
            "Zoo Extended NAV",
            f"{latest['zoo_extended_nav']:.4f}",
            "c05621",
        ),
        ("hs300_nav", "HS300 NAV", f"{latest['hs300_nav']:.4f}", "3182ce"),
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


def generate_chart(path: Path, nav_df: pd.DataFrame) -> None:
    if nav_df.empty:
        return

    nav_df = nav_df.sort_values("date")
    dates = nav_df["date"].tolist()

    plt.figure(figsize=(10, 6))
    line_kwargs = {"marker": "o", "markersize": 3}
    plt.plot(dates, nav_df["zoo_strict_nav"], label="Zoo Strict", **line_kwargs)
    plt.plot(dates, nav_df["zoo_extended_nav"], label="Zoo Extended", **line_kwargs)
    plt.plot(dates, nav_df["hs300_nav"], label="HS300", **line_kwargs)
    plt.xlabel("Date")
    plt.ylabel("NAV")
    plt.title("A-share Zoo Index")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def generate_index_html(
    path: Path,
    latest: pd.Series,
    strict_stats: IndexStats,
    extended_stats: IndexStats,
) -> None:
    payload = {
        "date": latest["date"],
        "zoo_strict_nav": f"{latest['zoo_strict_nav']:.4f}",
        "zoo_extended_nav": f"{latest['zoo_extended_nav']:.4f}",
        "hs300_nav": f"{latest['hs300_nav']:.4f}",
        "zoo_strict_daily": f"{latest['zoo_strict_ret']:.2%}",
        "zoo_extended_daily": f"{latest['zoo_extended_ret']:.2%}",
        "hs300_daily": f"{latest['hs300_ret']:.2%}",
    }

    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>A股动物园指数</title>
  <style>
    :root {{
      color-scheme: light;
      font-family: "Noto Sans SC", "Microsoft YaHei", sans-serif;
      --bg: #f6f5f1;
      --card: #ffffff;
      --text: #2b2b2b;
      --muted: #666;
      --accent: #1f6f54;
    }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
    }}
    header {{
      padding: 32px 20px 12px;
      text-align: center;
    }}
    h1 {{
      margin: 0;
      font-size: 28px;
      letter-spacing: 1px;
    }}
    .subtitle {{
      margin-top: 6px;
      color: var(--muted);
      font-size: 14px;
    }}
    main {{
      max-width: 980px;
      margin: 0 auto;
      padding: 12px 20px 40px;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }}
    .card {{
      background: var(--card);
      border-radius: 12px;
      padding: 16px;
      box-shadow: 0 6px 16px rgba(0,0,0,0.06);
    }}
    .card h3 {{
      margin: 0 0 12px;
      font-size: 16px;
      color: var(--accent);
    }}
    .stat {{
      font-size: 22px;
      font-weight: 600;
      margin-bottom: 6px;
    }}
    .stat small {{
      font-size: 12px;
      color: var(--muted);
    }}
    .chart {{
      background: var(--card);
      border-radius: 12px;
      padding: 16px;
      box-shadow: 0 6px 16px rgba(0,0,0,0.06);
    }}
    .chart img {{
      width: 100%;
      border-radius: 8px;
    }}
    .notes {{
      margin-top: 16px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
    }}
  </style>
</head>
<body>
  <header>
    <h1>A股动物园指数</h1>
    <div class="subtitle">最近更新：{payload['date']}</div>
  </header>
  <main>
    <section class="cards">
      <div class="card">
        <h3>严格动物园</h3>
        <div class="stat">{payload['zoo_strict_nav']}</div>
        <div class="stat"><small>今日涨跌</small> {payload['zoo_strict_daily']}</div>
        <div class="stat"><small>成分股</small> {strict_stats.priced_constituents}/{strict_stats.total_constituents}</div>
      </div>
      <div class="card">
        <h3>扩展动物园</h3>
        <div class="stat">{payload['zoo_extended_nav']}</div>
        <div class="stat"><small>今日涨跌</small> {payload['zoo_extended_daily']}</div>
        <div class="stat"><small>成分股</small> {extended_stats.priced_constituents}/{extended_stats.total_constituents}</div>
      </div>
      <div class="card">
        <h3>沪深300</h3>
        <div class="stat">{payload['hs300_nav']}</div>
        <div class="stat"><small>今日涨跌</small> {payload['hs300_daily']}</div>
      </div>
    </section>
    <section class="chart">
      <img src="chart.png" alt="动物园指数曲线" />
    </section>
    <section class="notes">
      <p>说明：严格动物园仅收录明确动物词汇，扩展动物园包含单字动物/神兽词，噪声更高但更热闹。</p>
      <p>净值为价格指数口径，未做分红送转调整。</p>
    </section>
  </main>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")
