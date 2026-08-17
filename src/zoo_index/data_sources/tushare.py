from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol

import pandas as pd
import tushare as ts


@dataclass(frozen=True)
class TradeCalendarEntry:
    date: str
    is_open: bool


class TushareLike(Protocol):
    """runner 计算净值所需的 Tushare 接口子集，便于依赖注入与测试替换。"""

    def get_trade_calendar(self, date: str) -> TradeCalendarEntry: ...
    def get_trade_calendar_range(self, start_date: str, end_date: str) -> pd.DataFrame: ...
    def get_recent_open_dates(
        self, end_date: str, count: int, lookback_days: int | None = None
    ) -> list[str]: ...
    def get_stock_basic(self) -> pd.DataFrame: ...
    def get_namechange(self) -> pd.DataFrame: ...
    def get_daily(self, trade_date: str) -> pd.DataFrame: ...
    def get_adj_factor(self, trade_date: str) -> pd.DataFrame: ...
    def get_index_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame: ...
    def get_fund_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame: ...
    def get_fund_adj(self, trade_date: str, ts_code: str) -> pd.DataFrame: ...
    def get_suspension(self, trade_date: str) -> pd.DataFrame: ...


class TushareClient:
    # 进程内只提示一次：主 Token 失败已回退到备用 Token。
    _fallback_warned = False

    def __init__(
        self,
        token: str,
        cache_dir: Path | None = None,
        use_cache: bool = True,
        force_refresh: bool = False,
        reference_cache_ttl: int | None = 86400,
        api_url: str | None = None,
        token2: str | None = None,
        api_url2: str | None = None,
    ) -> None:
        self._pro = ts.pro_api(token)
        if api_url:
            self._pro._DataApi__http_url = api_url.rstrip("/")
        self._cache_dir = cache_dir
        self._use_cache = use_cache
        self._force_refresh = force_refresh
        self._reference_cache_ttl = reference_cache_ttl
        self._fallback: TushareClient | None = None
        if token2:
            # 递归构建备用客户端（备用端不再二次 fallback，避免死循环）。
            self._fallback = TushareClient(
                token2,
                cache_dir=cache_dir,
                use_cache=use_cache,
                force_refresh=force_refresh,
                reference_cache_ttl=reference_cache_ttl,
                api_url=api_url2,
                token2=None,
                api_url2=None,
            )

    def _call(self, method: str, *args, **kwargs):
        return getattr(self._pro, method)(*args, **kwargs)

    def _api(self, method: str, *args, **kwargs):
        try:
            return self._call(method, *args, **kwargs)
        except Exception:
            if self._fallback is not None:
                if not TushareClient._fallback_warned:
                    TushareClient._fallback_warned = True
                    print("主 Token 请求失败，已回退到备用 Token（token2）。")
                return self._fallback._call(method, *args, **kwargs)
            raise

    def _cache_path(self, *parts: str) -> Path | None:
        if self._cache_dir is None:
            return None
        return self._cache_dir.joinpath(*parts)

    def _read_cache(self, path: Path | None, ttl: int | None = None) -> pd.DataFrame | None:
        if path is None or not self._use_cache or self._force_refresh:
            return None
        if not path.exists():
            return None
        if ttl is not None:
            age = time.time() - path.stat().st_mtime
            if age > ttl:
                return None
        return pd.read_parquet(path)

    def _write_cache(self, path: Path | None, df: pd.DataFrame) -> None:
        if path is None or not self._use_cache or df.empty:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)

    def _trade_cal_with_retry(self, **kwargs) -> pd.DataFrame:
        last_df = pd.DataFrame()
        for attempt in range(3):
            df = self._api("trade_cal", **kwargs)
            if not df.empty:
                return df
            last_df = df
            time.sleep(0.5 * (2**attempt))
        return last_df

    def get_trade_calendar(self, date: str) -> TradeCalendarEntry:
        df = self._trade_cal_with_retry(
            exchange="",
            start_date=date,
            end_date=date,
            fields="cal_date,is_open",
        )
        if df.empty:
            raise ValueError("trade calendar is empty")
        row = df.iloc[0]
        return TradeCalendarEntry(date=row["cal_date"], is_open=bool(row["is_open"]))

    def get_trade_calendar_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        df = self._trade_cal_with_retry(
            exchange="",
            start_date=start_date,
            end_date=end_date,
            fields="cal_date,is_open",
        )
        if df.empty:
            raise ValueError("trade calendar is empty")
        df["cal_date"] = df["cal_date"].astype(str)
        return df

    def get_recent_open_dates(
        self, end_date: str, count: int, lookback_days: int | None = None
    ) -> list[str]:
        if count <= 0:
            raise ValueError("count must be positive")
        if lookback_days is None:
            lookback_days = max(count * 2, 60)

        end = datetime.strptime(end_date, "%Y%m%d")
        attempts = 0
        while True:
            start = end - timedelta(days=lookback_days)
            df = self._trade_cal_with_retry(
                exchange="",
                start_date=start.strftime("%Y%m%d"),
                end_date=end_date,
                fields="cal_date,is_open",
            )
            if df.empty:
                raise ValueError("trade calendar is empty")
            open_days = df[df["is_open"] == 1].copy()
            if open_days.empty:
                raise ValueError("no open trading day found")
            open_days["cal_date"] = open_days["cal_date"].astype(str)
            dates = open_days.sort_values("cal_date")["cal_date"].tolist()
            if len(dates) >= count:
                return dates[-count:]
            attempts += 1
            if attempts >= 5 or lookback_days >= 3650:
                raise ValueError("not enough open trading days found")
            lookback_days *= 2

    def get_stock_basic(self) -> pd.DataFrame:
        cache_path = self._cache_path("stock_basic.parquet")
        cached = self._read_cache(cache_path, ttl=self._reference_cache_ttl)
        if cached is not None:
            return cached
        fields = "ts_code,name,exchange,market,list_date,delist_date"
        frames: list[pd.DataFrame] = []
        for status in ("L", "D", "P"):
            df = self._api("stock_basic", list_status=status, fields=fields)
            if not df.empty:
                frames.append(df)
        if frames:
            df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts_code"])
        else:
            df = pd.DataFrame(columns=fields.split(","))  # ty: ignore[invalid-argument-type]
        self._write_cache(cache_path, df)
        return df

    def get_namechange(self) -> pd.DataFrame:
        cache_path = self._cache_path("namechange.parquet")
        cached = self._read_cache(cache_path, ttl=self._reference_cache_ttl)
        if cached is not None:
            return cached
        df = self._api("namechange", fields="ts_code,name,start_date,end_date")
        if not df.empty:
            df = df.drop_duplicates()
        self._write_cache(cache_path, df)
        return df

    def get_daily(self, trade_date: str) -> pd.DataFrame:
        cache_path = self._cache_path("daily", f"{trade_date}.parquet")
        cached = self._read_cache(cache_path)
        if cached is not None:
            return cached
        last = pd.DataFrame()
        for attempt in range(5):
            df = self._api(
                "daily",
                trade_date=trade_date,
                fields="ts_code,close,pre_close",
            )
            if not df.empty:
                self._write_cache(cache_path, df)
                return df
            last = df
            time.sleep(0.6 * (2**attempt))
        return last

    def get_adj_factor(self, trade_date: str) -> pd.DataFrame:
        cache_path = self._cache_path("adj_factor", f"{trade_date}.parquet")
        cached = self._read_cache(cache_path)
        if cached is not None:
            return cached
        last = pd.DataFrame()
        for attempt in range(5):
            df = self._api(
                "adj_factor",
                trade_date=trade_date,
                fields="ts_code,trade_date,adj_factor",
            )
            if not df.empty:
                df = df.drop_duplicates(subset=["ts_code"])
                self._write_cache(cache_path, df)
                return df
            last = df
            time.sleep(0.6 * (2**attempt))
        return last

    def get_index_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        cache_path = self._cache_path("index_daily", ts_code, f"{trade_date}.parquet")
        cached = self._read_cache(cache_path)
        if cached is not None:
            return cached
        last = pd.DataFrame()
        for attempt in range(5):
            df = self._api(
                "index_daily",
                ts_code=ts_code,
                trade_date=trade_date,
                fields="ts_code,close,pre_close",
            )
            if not df.empty:
                self._write_cache(cache_path, df)
                return df
            last = df
            time.sleep(0.6 * (2**attempt))
        return last

    def get_fund_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        cache_path = self._cache_path("fund_daily", ts_code, f"{trade_date}.parquet")
        cached = self._read_cache(cache_path)
        if cached is not None:
            return cached
        last = pd.DataFrame()
        for attempt in range(5):
            df = self._api(
                "fund_daily",
                ts_code=ts_code,
                trade_date=trade_date,
                fields="ts_code,trade_date,close,pre_close",
            )
            if not df.empty:
                self._write_cache(cache_path, df)
                return df
            last = df
            time.sleep(0.6 * (2**attempt))
        return last

    def get_fund_adj(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        cache_path = self._cache_path("fund_adj", ts_code, f"{trade_date}.parquet")
        cached = self._read_cache(cache_path)
        if cached is not None:
            return cached
        last = pd.DataFrame()
        for attempt in range(5):
            df = self._api(
                "fund_adj",
                ts_code=ts_code,
                trade_date=trade_date,
                fields="ts_code,trade_date,adj_factor",
            )
            if not df.empty:
                self._write_cache(cache_path, df)
                return df
            last = df
            time.sleep(0.6 * (2**attempt))
        return last

    def get_suspension(self, trade_date: str) -> pd.DataFrame:
        cache_path = self._cache_path("suspension", f"{trade_date}.parquet")
        cached = self._read_cache(cache_path)
        if cached is not None:
            return cached
        df = self._api(
            "suspend_d",
            trade_date=trade_date,
            fields="ts_code,suspend_date,suspend_type",
        )
        if df.empty:
            self._write_cache(cache_path, df)
            return df
        df = df.drop_duplicates(subset=["ts_code"])
        self._write_cache(cache_path, df)
        return df
