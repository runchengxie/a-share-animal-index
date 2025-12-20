from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
import tushare as ts


@dataclass(frozen=True)
class TradeCalendarEntry:
    date: str
    is_open: bool


class TushareClient:
    def __init__(self, token: str) -> None:
        self._pro = ts.pro_api(token)

    def get_trade_calendar(self, date: str) -> TradeCalendarEntry:
        df = self._pro.trade_cal(
            exchange="",
            start_date=date,
            end_date=date,
            fields="cal_date,is_open",
        )
        if df.empty:
            raise ValueError("trade calendar is empty")
        row = df.iloc[0]
        return TradeCalendarEntry(date=row["cal_date"], is_open=bool(row["is_open"]))

    def get_recent_open_date(self, end_date: str, lookback_days: int = 30) -> str:
        end = datetime.strptime(end_date, "%Y%m%d")
        start = end - timedelta(days=lookback_days)
        df = self._pro.trade_cal(
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
            df = self._pro.trade_cal(
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
        df = self._pro.stock_basic(
            list_status="L",
            fields="ts_code,name,exchange,market",
        )
        return df.drop_duplicates(subset=["ts_code"])

    def get_daily(self, trade_date: str) -> pd.DataFrame:
        df = self._pro.daily(
            trade_date=trade_date,
            fields="ts_code,close,pre_close",
        )
        return df

    def get_index_daily(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        df = self._pro.index_daily(
            ts_code=ts_code,
            trade_date=trade_date,
            fields="ts_code,close,pre_close",
        )
        return df
