from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import time

import pandas as pd
import tushare as ts


@dataclass(frozen=True)
class TradeCalendarEntry:
    date: str
    is_open: bool


class TushareClient:
    def __init__(
        self,
        token: str,
        cache_dir: Path | None = None,
        use_cache: bool = True,
        force_refresh: bool = False,
    ) -> None:
        self._pro = ts.pro_api(token)
        self._cache_dir = cache_dir
        self._use_cache = use_cache
        self._force_refresh = force_refresh

    def _cache_path(self, *parts: str) -> Path | None:
        if self._cache_dir is None:
            return None
        return self._cache_dir.joinpath(*parts)

    def _read_cache(self, path: Path | None) -> pd.DataFrame | None:
        if path is None or not self._use_cache or self._force_refresh:
            return None
        if not path.exists():
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
            df = self._pro.trade_cal(**kwargs)
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

    def get_recent_open_date(self, end_date: str, lookback_days: int = 30) -> str:
        end = datetime.strptime(end_date, "%Y%m%d")
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
        return open_days.sort_values("cal_date").iloc[-1]["cal_date"]

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
        cached = self._read_cache(cache_path)
        if cached is not None:
            return cached
        fields = "ts_code,name,exchange,market,list_date,delist_date"
        frames: list[pd.DataFrame] = []
        for status in ("L", "D", "P"):
            df = self._pro.stock_basic(list_status=status, fields=fields)
            if not df.empty:
                frames.append(df)
        if frames:
            df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts_code"])
        else:
            df = pd.DataFrame(columns=fields.split(","))
        self._write_cache(cache_path, df)
        return df

    def get_namechange(self) -> pd.DataFrame:
        cache_path = self._cache_path("namechange.parquet")
        cached = self._read_cache(cache_path)
        if cached is not None:
            return cached
        df = self._pro.namechange(fields="ts_code,name,start_date,end_date")
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
            df = self._pro.daily(
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
            df = self._pro.adj_factor(
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
            df = self._pro.index_daily(
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
            df = self._pro.fund_daily(
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
            df = self._pro.fund_adj(
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
