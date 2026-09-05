from zoo_index.config import KeywordRule, OverrideRule, Rules
from zoo_index.matcher import Matcher


def _rules(**overrides) -> Rules:
    return Rules(
        strict_keywords=tuple(overrides.get("strict_keywords", ("CATFISH", "CAT"))),
        extended_keywords=tuple(overrides.get("extended_keywords", ("CATFISH", "CAT"))),
        exclude_patterns=tuple(overrides.get("exclude_patterns", ())),
        force_include=tuple(overrides.get("force_include", ())),
        force_exclude=tuple(overrides.get("force_exclude", ())),
        exclude_st=bool(overrides.get("exclude_st", False)),
        allow_beijing=bool(overrides.get("allow_beijing", True)),
        theme=overrides.get("theme", "animal"),
        keyword_rules=tuple(overrides.get("keyword_rules", ())),
        overrides=tuple(overrides.get("overrides", ())),
    )


def test_matcher_prefers_longest_keyword() -> None:
    matcher = Matcher(_rules())
    result = matcher.classify("000001.SZ", "ACME CATFISH LTD")

    assert result.strict
    assert result.extended
    assert result.strict_keyword == "CATFISH"
    assert result.extended_keyword == "CATFISH"


def test_matcher_exclude_pattern_overrides_match() -> None:
    matcher = Matcher(_rules(exclude_patterns=("BAD",)))
    result = matcher.classify("000001.SZ", "BAD CATFISH LTD")

    assert not result.strict
    assert not result.extended


def test_mythical_dragon_is_extended_not_strict() -> None:
    rules = _rules(
        strict_keywords=(),
        extended_keywords=(),
        keyword_rules=(KeywordRule("龙", "animal", "extended", "mythical", "medium"),),
    )

    result = Matcher(rules).classify("000001.SZ", "飞龙股份")

    assert result.extended
    assert not result.strict
    assert result.reality == "mythical"
    assert result.matched_term == "龙"


def test_override_can_include_only_extended() -> None:
    rules = _rules(
        strict_keywords=(),
        extended_keywords=(),
        overrides=(
            OverrideRule(
                "603586.SH",
                "include",
                "extended",
                "animal",
                "麒麟",
                "神话动物扩展候选",
            ),
        ),
    )

    result = Matcher(rules).classify("603586.SH", "金麒麟")

    assert not result.strict
    assert result.extended
    assert result.decision == "override"
    assert result.eligibility_reason == "神话动物扩展候选"


def test_force_exclude_has_a_distinct_decision() -> None:
    result = Matcher(_rules(force_exclude=("000001.SZ",))).classify("000001.SZ", "ACME CATFISH LTD")

    assert not result.extended
    assert result.decision == "force_exclude"
