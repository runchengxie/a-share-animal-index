import json

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
            "hs300_nav": 1.11111,
        }
    )

    generate_badges(tmp_path, latest)

    payload = json.loads((tmp_path / "zoo_strict_nav.json").read_text(encoding="utf-8"))
    assert payload["schemaVersion"] == 1
    assert payload["label"] == "Zoo Strict NAV"
    assert payload["message"] == "1.2346"
