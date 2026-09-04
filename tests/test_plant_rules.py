from pathlib import Path

from zoo_index.config import load_rules
from zoo_index.matcher import Matcher

ROOT = Path(__file__).resolve().parents[1]


def test_plant_rules_keep_core_and_expanded_words_separate() -> None:
    rules = load_rules(ROOT / "plant_rules.yml")
    matcher = Matcher(rules)

    assert matcher.classify("000001.SZ", "兰花科创").strict
    assert matcher.classify("000002.SZ", "梅花生物").strict
    assert not matcher.classify("000003.SZ", "桂林旅游").strict
    assert not matcher.classify("000004.SZ", "某某科技").strict
    assert matcher.classify("000005.SZ", "桃李面包").extended
    assert matcher.classify("000006.SZ", "百合股份").extended
    assert matcher.classify("300111.SZ", "向日葵").extended
    assert matcher.classify("002737.SZ", "葵花药业").extended
    assert matcher.classify("000716.SZ", "黑芝麻").extended
    assert matcher.classify("000789.SZ", "万年青").extended
    assert matcher.classify("603737.SH", "三棵树").extended
    assert matcher.classify("600080.SH", "金花股份").extended
    assert not matcher.classify("000007.SZ", "松芝股份").extended
    assert not matcher.classify("002050.SZ", "三花智控").extended
    assert not matcher.classify("002577.SZ", "雷柏科技").extended
    assert set(rules.strict_keywords).issubset(rules.extended_keywords)
    assert set(rules.extended_keywords) - set(rules.strict_keywords)
