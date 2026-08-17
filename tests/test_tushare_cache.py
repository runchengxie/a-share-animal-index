from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import pytest

from zoo_index.data_sources.tushare import TushareClient


def _client(
    tmp_path: Path, reference_cache_ttl: int = 60, force_refresh: bool = False
) -> TushareClient:
    return TushareClient(
        "dummy-token",
        cache_dir=tmp_path,
        use_cache=True,
        force_refresh=force_refresh,
        reference_cache_ttl=reference_cache_ttl,
    )


def test_reference_cache_expires_after_ttl(tmp_path: Path) -> None:
    client = _client(tmp_path, reference_cache_ttl=60)
    path = tmp_path / "stock_basic.parquet"
    pd.DataFrame({"ts_code": ["000001.SZ"]}).to_parquet(path, index=False)

    # 新鲜缓存（带 TTL）命中。
    assert client._read_cache(path, ttl=60) is not None

    # 把修改时间往前推 100 秒，超过 TTL，应视为过期。
    old = time.time() - 100
    os.utime(path, (old, old))
    assert client._read_cache(path, ttl=60) is None

    # 不带 TTL 的永久缓存不受修改时间影响。
    assert client._read_cache(path) is not None


def test_force_refresh_ignores_cache(tmp_path: Path) -> None:
    client = _client(tmp_path, force_refresh=True)
    path = tmp_path / "namechange.parquet"
    pd.DataFrame({"ts_code": ["000001.SZ"]}).to_parquet(path, index=False)
    assert client._read_cache(path, ttl=60) is None


def test_missing_cache_file_returns_none(tmp_path: Path) -> None:
    client = _client(tmp_path)
    assert client._read_cache(tmp_path / "missing.parquet", ttl=60) is None


class _FakePro:
    def __init__(self, tag: str) -> None:
        self.tag = tag

    def daily(self, **kwargs: object) -> pd.DataFrame:
        if self.tag == "primary":
            raise RuntimeError("quota exceeded")
        return pd.DataFrame({"ts_code": ["000001.SZ"]})


def test_fallback_uses_token2_when_primary_fails(tmp_path: Path) -> None:
    import zoo_index.data_sources.tushare as tushare_mod

    primary = _FakePro("primary")
    secondary = _FakePro("secondary")
    state = {"n": 0}

    def fake_pro_api(token: str = "", timeout: int = 30) -> object:
        state["n"] += 1
        return primary if state["n"] == 1 else secondary

    original = tushare_mod.ts.pro_api
    tushare_mod.ts.pro_api = fake_pro_api  # ty: ignore[invalid-assignment]
    try:
        client = TushareClient(
            "primary-token",
            cache_dir=tmp_path,
            token2="secondary-token",
            api_url2="https://example.com",
        )
        df = client._api("daily", ts_code="000001.SZ")
        # 主 Token 抛错，应回退到备用 Token 拿到数据。
        assert not df.empty
        assert df.iloc[0]["ts_code"] == "000001.SZ"
    finally:
        tushare_mod.ts.pro_api = original


def test_no_fallback_reraises_when_primary_fails(tmp_path: Path) -> None:
    import zoo_index.data_sources.tushare as tushare_mod

    def fake_pro_api(token: str = "", timeout: int = 30) -> object:
        return _FakePro("primary")

    original = tushare_mod.ts.pro_api
    tushare_mod.ts.pro_api = fake_pro_api  # ty: ignore[invalid-assignment]
    try:
        client = TushareClient("primary-token", cache_dir=tmp_path)
        with pytest.raises(RuntimeError):
            client._api("daily")
    finally:
        tushare_mod.ts.pro_api = original


def test_get_suspension_calls_suspend_d_and_caches(tmp_path: Path) -> None:
    import zoo_index.data_sources.tushare as tushare_mod

    class _SuspPro:
        def suspend_d(self, **kwargs: object) -> pd.DataFrame:
            return pd.DataFrame(
                {"ts_code": ["000001.SZ"], "suspend_date": ["20240102"], "suspend_type": "S"}
            )

    original = tushare_mod.ts.pro_api
    tushare_mod.ts.pro_api = lambda token="", timeout=30: _SuspPro()  # ty: ignore[invalid-assignment]
    try:
        client = TushareClient("token", cache_dir=tmp_path)
        df = client.get_suspension("20240102")
        assert not df.empty
        assert df.iloc[0]["ts_code"] == "000001.SZ"
        assert (tmp_path / "suspension" / "20240102.parquet").exists()
    finally:
        tushare_mod.ts.pro_api = original
