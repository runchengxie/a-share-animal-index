import pandas as pd

from zoo_index.audit import build_audit_candidates, build_audit_result, write_audit_report
from zoo_index.config import load_rules


def _stock_basic() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "002081.SZ",
                "name": "金螳螂",
                "exchange": "SZSE",
                "list_date": "20100101",
                "delist_date": "",
            },
            {
                "ts_code": "605199.SH",
                "name": "ST葫芦娃",
                "exchange": "SSE",
                "list_date": "20210601",
                "delist_date": "",
            },
            {
                "ts_code": "000001.SZ",
                "name": "某某科技",
                "exchange": "SZSE",
                "list_date": "20100101",
                "delist_date": "",
            },
            {
                "ts_code": "430001.BJ",
                "name": "熊猫科技",
                "exchange": "BSE",
                "list_date": "20100101",
                "delist_date": "",
            },
        ]
    )


def test_audit_keeps_theme_membership_separate_from_eligibility() -> None:
    stock = _stock_basic()
    rules = load_rules(__import__("pathlib").Path("plant_rules.yml"))

    candidates = build_audit_candidates(stock, pd.DataFrame(), "20260904", rules)
    by_code = {item.ts_code: item for item in candidates}

    assert by_code["605199.SH"].strict
    assert not by_code["605199.SH"].eligible
    assert "st_excluded" in by_code["605199.SH"].eligibility_reasons
    assert by_code["000001.SZ"].review_scope == "recall"
    assert "beijing_not_allowed" in by_code["430001.BJ"].eligibility_reasons


def test_audit_modes_and_order_are_deterministic() -> None:
    stock = _stock_basic()
    rules = load_rules(__import__("pathlib").Path("rules.yml"))

    precision = build_audit_candidates(stock, pd.DataFrame(), "20260904", rules, "precision")
    recall = build_audit_candidates(stock, pd.DataFrame(), "20260904", rules, "recall")

    assert [item.ts_code for item in precision] == ["002081.SZ", "430001.BJ"]
    assert [item.ts_code for item in recall] == ["000001.SZ", "605199.SH"]


def test_audit_report_has_stable_json_and_markdown_sections(tmp_path) -> None:
    stock = _stock_basic()
    rules = load_rules(__import__("pathlib").Path("rules.yml"))
    candidates = build_audit_candidates(stock, pd.DataFrame(), "20260904", rules)
    result = build_audit_result(stock, candidates, "20260904", rules, "all")

    json_path, markdown_path = write_audit_report(result, tmp_path)

    assert json_path.exists()
    assert markdown_path.exists()
    assert "Potential missing" in markdown_path.read_text(encoding="utf-8")
    assert "input_hash" in json_path.read_text(encoding="utf-8")
