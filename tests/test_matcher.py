from zoo_index.config import Rules
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
