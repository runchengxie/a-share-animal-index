from pathlib import Path

from zoo_index.config import load_rules
from zoo_index.matcher import Matcher

ROOT = Path(__file__).resolve().parents[1]


def test_explicit_animal_candidates_are_collected() -> None:
    matcher = Matcher(load_rules(ROOT / "rules.yml"))

    assert matcher.classify("002081.SZ", "金螳螂").strict
    assert matcher.classify("601311.SH", "骆驼股份").strict


def test_mythical_animals_are_extended_candidates() -> None:
    matcher = Matcher(load_rules(ROOT / "rules.yml"))

    qilin = matcher.classify("603586.SH", "金麒麟")
    phoenix = matcher.classify("601928.SH", "凤凰传媒")

    assert qilin.extended and not qilin.strict
    assert phoenix.extended and not phoenix.strict


def test_plant_candidates_are_members_even_when_st_is_ineligible_later() -> None:
    matcher = Matcher(load_rules(ROOT / "plant_rules.yml"))

    gourd = matcher.classify("605199.SH", "ST葫芦娃")
    banyan = matcher.classify("002474.SZ", "榕基软件")
    tung = matcher.classify("601233.SH", "桐昆股份")

    assert gourd.strict
    assert banyan.extended
    assert tung.extended
