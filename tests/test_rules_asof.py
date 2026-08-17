from pathlib import Path

from zoo_index.config import load_rules, load_rules_asof


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _history_text() -> str:
    return (
        '- effective_from: "20200101"\n'
        "  rules:\n"
        "    strict_keywords: [老虎]\n"
        "    extended_keywords: []\n"
        '- effective_from: "20210101"\n'
        "  rules:\n"
        "    strict_keywords: [狮子]\n"
        "    extended_keywords: []\n"
    )


def test_range_selection_history_governs(tmp_path: Path) -> None:
    # rules.yml 与最后一版历史规则一致（狮子），验证区间选择而非被取代。
    rules_path = tmp_path / "rules.yml"
    history_path = tmp_path / "rules_history.yml"
    _write(rules_path, "strict_keywords: [狮子]\nextended_keywords: []\n")
    _write(history_path, _history_text())

    assert load_rules_asof("20191231", rules_path).strict_keywords == ("老虎",)
    assert load_rules_asof("20200301", rules_path).strict_keywords == ("老虎",)
    # 落在第二版生效区间内（含生效日）。
    assert load_rules_asof("20210101", rules_path).strict_keywords == ("狮子",)
    assert load_rules_asof("20210201", rules_path).strict_keywords == ("狮子",)
    # 超过所有历史版本：回退到最新（rules.yml 的狮子）。
    assert load_rules_asof("20300101", rules_path).strict_keywords == ("狮子",)


def test_current_rules_supersede_last_history(tmp_path: Path) -> None:
    # rules.yml（熊猫）是当前状态，从最后一版历史规则生效日起取代狮子。
    rules_path = tmp_path / "rules.yml"
    history_path = tmp_path / "rules_history.yml"
    _write(rules_path, "strict_keywords: [熊猫]\nextended_keywords: []\n")
    _write(history_path, _history_text())

    # 早于所有历史版本：取最早一条（老虎），不把当前规则错配到远古。
    assert load_rules_asof("20191231", rules_path).strict_keywords == ("老虎",)
    # 第一版区间内：老虎。
    assert load_rules_asof("20200301", rules_path).strict_keywords == ("老虎",)
    # 从最后一版历史生效日起：当前规则（熊猫）取代狮子。
    assert load_rules_asof("20210201", rules_path).strict_keywords == ("熊猫",)
    assert load_rules_asof("20300101", rules_path).strict_keywords == ("熊猫",)


def test_load_rules_asof_no_history_falls_back_to_latest(tmp_path: Path) -> None:
    rules_path = tmp_path / "rules.yml"
    _write(rules_path, "strict_keywords: [熊猫]\nextended_keywords: []\n")

    assert load_rules_asof("20200101", rules_path).strict_keywords == ("熊猫",)
    assert load_rules(rules_path).strict_keywords == ("熊猫",)


def test_load_rules_asof_new_fields_default(tmp_path: Path) -> None:
    rules_path = tmp_path / "rules.yml"
    _write(rules_path, "strict_keywords: [熊猫]\nextended_keywords: []\n")

    rules = load_rules_asof("20200101", rules_path)
    assert rules.min_listing_days == 0
    assert rules.min_daily_amount == 0.0
    assert rules.max_suspension_days == 0
