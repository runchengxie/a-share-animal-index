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
