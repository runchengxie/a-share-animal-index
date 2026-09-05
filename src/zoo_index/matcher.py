from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

from .config import KeywordRule, OverrideRule, Rules

_TS_CODE_RE = re.compile(r"^\d{6}\.(SZ|SH|BJ)$")


@dataclass(frozen=True)
class MatchResult:
    strict: bool
    extended: bool
    strict_keyword: str | None
    extended_keyword: str | None
    forced: bool
    theme: str = "animal"
    matched_term: str | None = None
    reality: str | None = None
    ambiguity: str | None = None
    decision: str = "none"
    eligibility_reason: str | None = None


def _split_force_items(items: Iterable[str]) -> set[str]:
    codes: set[str] = set()
    for item in items:
        normalized = item.strip().upper()
        if _TS_CODE_RE.match(normalized):
            codes.add(normalized)
    return codes


def _sorted_keywords(keywords: Iterable[str]) -> list[str]:
    return sorted({kw.strip() for kw in keywords if kw.strip()}, key=len, reverse=True)


def _match_keyword(name: str, keywords: list[str]) -> str | None:
    for keyword in keywords:
        if keyword and keyword in name:
            return keyword
    return None


def _legacy_keyword_rules(rules: Rules) -> tuple[KeywordRule, ...]:
    strict = set(rules.strict_keywords)
    terms = _sorted_keywords([*rules.strict_keywords, *rules.extended_keywords])
    return tuple(
        KeywordRule(
            term=term,
            theme=rules.theme,
            tier="strict" if term in strict else "extended",
            reality="real",
            ambiguity="low" if term in strict else "medium",
        )
        for term in terms
    )


def _sorted_keyword_rules(rules: Rules) -> tuple[KeywordRule, ...]:
    keyword_rules = rules.keyword_rules or _legacy_keyword_rules(rules)
    return tuple(sorted(keyword_rules, key=lambda item: len(item.term), reverse=True))


def _matched_rule(name: str, rules: Iterable[KeywordRule], tiers: set[str]) -> KeywordRule | None:
    for rule in rules:
        if rule.tier in tiers and rule.term in name:
            return rule
    return None


def _override_result(rules: Rules, override: OverrideRule, *, forced: bool = False) -> MatchResult:
    strict = override.tier in {"strict", "both"}
    extended = override.tier in {"extended", "both", "strict"}
    if override.action == "exclude":
        strict = False
        extended = False
    return MatchResult(
        strict=strict,
        extended=extended,
        strict_keyword=override.term if strict else None,
        extended_keyword=override.term if extended else None,
        forced=forced,
        theme=rules.theme,
        matched_term=override.term,
        decision="override",
        eligibility_reason=override.reason,
    )


def _hit_exclude_pattern(name: str, patterns: Iterable[str]) -> bool:
    return any(pattern and pattern in name for pattern in patterns)


class Matcher:
    def __init__(self, rules: Rules) -> None:
        self._rules = rules
        self._include_codes = _split_force_items(rules.force_include)
        self._exclude_codes = _split_force_items(rules.force_exclude)
        self._keyword_rules = _sorted_keyword_rules(rules)
        self._strict_keywords = _sorted_keywords(rules.strict_keywords)
        self._extended_keywords = _sorted_keywords(rules.extended_keywords)
        self._overrides = {override.ts_code: override for override in rules.overrides}

    def classify(self, ts_code: str, name: str) -> MatchResult:
        code = ts_code.upper()
        safe_name = name or ""

        if code in self._exclude_codes:
            return MatchResult(
                False,
                False,
                None,
                None,
                False,
                theme=self._rules.theme,
                decision="force_exclude",
            )

        override = self._overrides.get(code)
        if override is not None:
            return _override_result(self._rules, override)

        if code in self._include_codes:
            return MatchResult(
                True,
                True,
                "forced",
                "forced",
                True,
                theme=self._rules.theme,
                matched_term="forced",
                decision="force_include",
            )

        if _hit_exclude_pattern(safe_name, self._rules.exclude_patterns):
            return MatchResult(
                False,
                False,
                None,
                None,
                False,
                theme=self._rules.theme,
                decision="exclude_pattern",
            )

        strict_rule = _matched_rule(safe_name, self._keyword_rules, {"strict"})
        extended_rule = _matched_rule(safe_name, self._keyword_rules, {"strict", "extended"})
        matched_rule = strict_rule or extended_rule

        return MatchResult(
            strict=bool(strict_rule),
            extended=bool(extended_rule),
            strict_keyword=strict_rule.term if strict_rule else None,
            extended_keyword=extended_rule.term if extended_rule else None,
            forced=False,
            theme=self._rules.theme,
            matched_term=matched_rule.term if matched_rule else None,
            reality=matched_rule.reality if matched_rule else None,
            ambiguity=matched_rule.ambiguity if matched_rule else None,
            decision="matched" if matched_rule else "none",
        )
