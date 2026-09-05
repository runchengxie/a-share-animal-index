import pytest

from zoo_index.config import load_rules


def test_parse_typed_keywords_and_overrides(tmp_path) -> None:
    path = tmp_path / "rules.yml"
    path.write_text(
        """
theme: animal
keywords:
  - term: 螳螂
    tier: strict
    reality: real
    ambiguity: low
  - term: 龙
    tier: extended
    reality: mythical
    ambiguity: medium
overrides:
  - ts_code: 002081.SZ
    action: include
    tier: strict
    theme: animal
    term: 螳螂
    reason: 明确现实动物名
""",
        encoding="utf-8",
    )

    rules = load_rules(path)

    assert rules.theme == "animal"
    assert rules.keyword_rules[0].term == "螳螂"
    assert rules.keyword_rules[1].reality == "mythical"
    assert rules.overrides[0].ts_code == "002081.SZ"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tier", "unknown"),
        ("reality", "unknown"),
        ("ambiguity", "unknown"),
    ],
)
def test_reject_invalid_keyword_metadata(tmp_path, field: str, value: str) -> None:
    path = tmp_path / "rules.yml"
    path.write_text(
        f"theme: animal\nkeywords:\n  - term: 螳螂\n    tier: strict\n    {field}: {value}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=field):
        load_rules(path)


def test_reject_non_ts_code_override(tmp_path) -> None:
    path = tmp_path / "rules.yml"
    path.write_text(
        """
theme: animal
overrides:
  - ts_code: 金螳螂
    action: include
    tier: strict
    reason: invalid code
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="ts_code"):
        load_rules(path)


def test_legacy_rules_are_converted_to_typed_keywords(tmp_path) -> None:
    path = tmp_path / "plant_rules.yml"
    path.write_text(
        "strict_keywords: [兰花]\nextended_keywords: [桃李]\n",
        encoding="utf-8",
    )

    rules = load_rules(path)

    assert rules.theme == "plant"
    assert [(item.term, item.tier) for item in rules.keyword_rules] == [
        ("兰花", "strict"),
        ("桃李", "extended"),
    ]


def test_legacy_mythical_animal_terms_get_mythical_metadata(tmp_path) -> None:
    path = tmp_path / "rules.yml"
    path.write_text(
        "strict_keywords: []\nextended_keywords: [龙]\n",
        encoding="utf-8",
    )

    rules = load_rules(path)

    assert rules.keyword_rules[0].reality == "mythical"
