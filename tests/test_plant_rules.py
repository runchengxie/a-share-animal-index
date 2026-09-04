from pathlib import Path

from zoo_index.config import load_rules
from zoo_index.matcher import Matcher

ROOT = Path(__file__).resolve().parents[1]


def test_plant_rules_have_conservative_first_version() -> None:
    rules = load_rules(ROOT / "plant_rules.yml")
    matcher = Matcher(rules)

    assert matcher.classify("000001.SZ", "兰花科创").strict
    assert matcher.classify("000002.SZ", "梅花生物").strict
    assert not matcher.classify("000003.SZ", "桂林旅游").strict
    assert not matcher.classify("000004.SZ", "某某科技").strict
    assert rules.extended_keywords == rules.strict_keywords
