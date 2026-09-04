import json
from pathlib import Path

import pandas as pd

from zoo_index.outputs import compute_changes, compute_suspected_noise, generate_badges


def _code_set(records) -> set[str]:
    return {row["ts_code"] for row in records}


def test_compute_changes_uses_variants() -> None:
    today = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "name": "Alpha", "variant": "strict"},
            {"ts_code": "000002.SZ", "name": "Beta", "variant": "strict"},
            {"ts_code": "000003.SZ", "name": "Gamma", "variant": "extended"},
        ]
    )
    previous = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "name": "Alpha", "variant": "strict"},
            {"ts_code": "000004.SZ", "name": "Delta", "variant": "extended"},
        ]
    )

    changes = compute_changes(today, previous)

    assert _code_set(changes["strict"]["new_in"]) == {"000002.SZ"}
    assert _code_set(changes["strict"]["removed"]) == set()
    assert _code_set(changes["extended"]["new_in"]) == {"000003.SZ"}
    assert _code_set(changes["extended"]["removed"]) == {"000004.SZ"}


def test_compute_changes_handles_empty_previous() -> None:
    today = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "name": "Alpha", "variant": "strict"},
        ]
    )

    changes = compute_changes(today, pd.DataFrame())

    assert _code_set(changes["strict"]["new_in"]) == {"000001.SZ"}
    assert _code_set(changes["strict"]["removed"]) == set()


def test_update_nav_keeps_gross_fields_and_writes_net_fields(tmp_path: Path) -> None:
    from zoo_index.outputs import update_nav

    nav, latest = update_nav(
        tmp_path / "nav.csv",
        "20240102",
        0.1,
        0.2,
        0.05,
        strict_net_ret=0.09,
        extended_net_ret=0.18,
        strict_turnover=0.2,
        extended_turnover=0.3,
        strict_cost=0.01,
        extended_cost=0.02,
    )

    assert latest["zoo_strict_ret"] == 0.1
    assert latest["zoo_strict_net_ret"] == 0.09
    assert latest["zoo_strict_net_nav"] == 1.09
    assert latest["zoo_strict_turnover"] == 0.2
    assert latest["zoo_strict_cost"] == 0.01
    assert "zoo_extended_net_nav" in nav.columns


def test_compute_suspected_noise_filters_single_keywords() -> None:
    constituents = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "name": "Alpha",
                "keyword": "C",
                "forced": False,
                "variant": "extended",
            },
            {
                "ts_code": "000002.SZ",
                "name": "Beta",
                "keyword": "CAT",
                "forced": False,
                "variant": "extended",
            },
            {
                "ts_code": "000003.SZ",
                "name": "Gamma",
                "keyword": "D",
                "forced": True,
                "variant": "extended",
            },
        ]
    )

    noise = compute_suspected_noise(constituents)

    assert _code_set(noise["extended"]) == {"000001.SZ"}


def test_generate_badges_writes_schema(tmp_path) -> None:
    latest = pd.Series(
        {
            "zoo_strict_nav": 1.23456,
            "zoo_extended_nav": 0.98765,
            "benchmark_nav": 1.11111,
        }
    )

    generate_badges(tmp_path, latest)

    payload = json.loads((tmp_path / "zoo_strict_nav.json").read_text(encoding="utf-8"))
    assert payload["schemaVersion"] == 1
    assert payload["label"] == "Zoo Strict NAV"
    assert payload["message"] == "1.2346"
