from __future__ import annotations

from dataclasses import dataclass

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
