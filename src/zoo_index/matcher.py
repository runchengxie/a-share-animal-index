from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

from .config import Rules

_TS_CODE_RE = re.compile(r"^\d{6}\.(SZ|SH|BJ)$")


@dataclass(frozen=True)
class MatchResult:
    strict: bool
    extended: bool
    strict_keyword: str | None
    extended_keyword: str | None
    forced: bool


def _split_force_items(items: Iterable[str]) -> tuple[set[str], set[str]]:
    codes: set[str] = set()
    names: set[str] = set()
    for item in items:
        normalized = item.strip().upper()
        if _TS_CODE_RE.match(normalized):
            codes.add(normalized)
        else:
            names.add(item.strip())
    return codes, names


def _sorted_keywords(keywords: Iterable[str]) -> list[str]:
    return sorted({kw.strip() for kw in keywords if kw.strip()}, key=len, reverse=True)


def _match_keyword(name: str, keywords: list[str]) -> str | None:
    for keyword in keywords:
        if keyword and keyword in name:
            return keyword
    return None


def _hit_exclude_pattern(name: str, patterns: Iterable[str]) -> bool:
    for pattern in patterns:
        if pattern and pattern in name:
            return True
    return False


class Matcher:
    def __init__(self, rules: Rules) -> None:
        self._rules = rules
        self._include_codes, self._include_names = _split_force_items(rules.force_include)
        self._exclude_codes, self._exclude_names = _split_force_items(rules.force_exclude)
        self._strict_keywords = _sorted_keywords(rules.strict_keywords)
        self._extended_keywords = _sorted_keywords(rules.extended_keywords)

    def classify(self, ts_code: str, name: str) -> MatchResult:
        code = ts_code.upper()
        safe_name = name or ""

        if code in self._exclude_codes or safe_name in self._exclude_names:
            return MatchResult(False, False, None, None, False)

        if code in self._include_codes or safe_name in self._include_names:
            return MatchResult(True, True, "forced", "forced", True)

        if _hit_exclude_pattern(safe_name, self._rules.exclude_patterns):
            return MatchResult(False, False, None, None, False)

        strict_keyword = _match_keyword(safe_name, self._strict_keywords)
        extended_keyword = _match_keyword(safe_name, self._extended_keywords)

        return MatchResult(
            strict=bool(strict_keyword),
            extended=bool(extended_keyword),
            strict_keyword=strict_keyword,
            extended_keyword=extended_keyword,
            forced=False,
        )


def classify_stock(ts_code: str, name: str, rules: Rules) -> MatchResult:
    return Matcher(rules).classify(ts_code, name)
