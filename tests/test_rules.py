import warnings

from zoo_index.config import load_rules


def test_load_rules_merges_strict_into_extended(tmp_path) -> None:
    rules_path = tmp_path / "rules.yml"
    rules_path.write_text(
        "\n".join(
            [
                "strict_keywords:",
                "  - CATFISH",
                "extended_keywords:",
                "  - CAT",
            ]
        ),
        encoding="utf-8",
    )

    rules = load_rules(rules_path)

    assert rules.strict_keywords == ("CATFISH",)
    assert rules.extended_keywords == ("CATFISH", "CAT")


def test_load_rules_filters_force_names(tmp_path) -> None:
    rules_path = tmp_path / "rules.yml"
    rules_path.write_text(
        "\n".join(
            [
                "force_include:",
                "  - 000001.SZ",
                "  - 名称",
                "force_exclude:",
                "  - 000002.SZ",
                "  - Beta",
            ]
        ),
        encoding="utf-8",
    )

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        rules = load_rules(rules_path)

    assert rules.force_include == ("000001.SZ",)
    assert rules.force_exclude == ("000002.SZ",)
    assert any("force_include" in str(item.message) for item in captured)
    assert any("force_exclude" in str(item.message) for item in captured)
