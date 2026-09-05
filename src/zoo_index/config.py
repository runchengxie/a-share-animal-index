from __future__ import annotations

import re
import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import yaml

_THEMES = {"animal", "plant"}
_TIERS = {"strict", "extended"}
_REALITIES = {"real", "mythical", "collective"}
_AMBIGUITIES = {"low", "medium", "high"}
_OVERRIDE_ACTIONS = {"include", "exclude"}
_OVERRIDE_TIERS = {"strict", "extended", "both"}
_LEGACY_MYTHICAL_ANIMALS = {"龙", "鹏", "麒麟", "凤凰", "鲲"}


@dataclass(frozen=True)
class KeywordRule:
    term: str
    theme: str
    tier: str
    reality: str
    ambiguity: str


@dataclass(frozen=True)
class OverrideRule:
    ts_code: str
    action: str
    tier: str
    theme: str
    term: str | None
    reason: str


@dataclass(frozen=True)
class Rules:
    strict_keywords: tuple[str, ...]
    extended_keywords: tuple[str, ...]
    exclude_patterns: tuple[str, ...]
    force_include: tuple[str, ...]
    force_exclude: tuple[str, ...]
    exclude_st: bool
    allow_beijing: bool
    min_listing_days: int = 0
    min_daily_amount: float = 0.0
    max_suspension_days: int = 0
    theme: str = "animal"
    keyword_rules: tuple[KeywordRule, ...] = ()
    overrides: tuple[OverrideRule, ...] = ()


@dataclass(frozen=True)
class BacktestConfig:
    enabled: bool = False
    commission_rate: float = 0.0
    stamp_tax_rate: float = 0.0
    slippage_rate: float = 0.0


_TS_CODE_RE = re.compile(r"^\d{6}\.(SZ|SH|BJ)$", re.IGNORECASE)


def _as_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _unique_preserve(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _filter_ts_codes(items: Iterable[str], field_name: str) -> list[str]:
    valid: list[str] = []
    invalid: list[str] = []
    for item in items:
        normalized = item.strip().upper()
        if _TS_CODE_RE.match(normalized):
            valid.append(normalized)
        else:
            invalid.append(item)
    if invalid:
        warnings.warn(
            f"{field_name} 仅支持 ts_code，已忽略：{', '.join(invalid)}",
            RuntimeWarning,
            stacklevel=2,
        )
    return _unique_preserve(valid)


def _validated_choice(value: object, choices: set[str], field_name: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in choices:
        expected = ", ".join(sorted(choices))
        raise ValueError(f"{field_name} must be one of: {expected}")
    return normalized


def _parse_keyword_rules(
    data: dict[str, object], theme: str, strict: list[str], extended: list[str]
) -> tuple[KeywordRule, ...]:
    raw_keywords = data.get("keywords")
    if raw_keywords is None:
        strict_set = set(strict)
        return tuple(
            KeywordRule(
                term=term,
                theme=theme,
                tier="strict" if term in strict_set else "extended",
                reality=(
                    "mythical" if theme == "animal" and term in _LEGACY_MYTHICAL_ANIMALS else "real"
                ),
                ambiguity="low" if term in strict_set else "medium",
            )
            for term in _unique_preserve([*strict, *extended])
        )

    if not isinstance(raw_keywords, list):
        raise ValueError("keywords must be a list")

    parsed: list[KeywordRule] = []
    for index, raw in enumerate(raw_keywords):
        if not isinstance(raw, dict):
            raise ValueError(f"keywords[{index}] must be a mapping")
        term = str(raw.get("term", "")).strip()
        if not term:
            raise ValueError(f"keywords[{index}].term must not be empty")
        item_theme = _validated_choice(raw.get("theme", theme), _THEMES, f"keywords[{index}].theme")
        if item_theme != theme:
            raise ValueError(f"keywords[{index}].theme must match rules theme {theme}")
        parsed.append(
            KeywordRule(
                term=term,
                theme=item_theme,
                tier=_validated_choice(raw.get("tier"), _TIERS, f"keywords[{index}].tier"),
                reality=_validated_choice(
                    raw.get("reality", "real"), _REALITIES, f"keywords[{index}].reality"
                ),
                ambiguity=_validated_choice(
                    raw.get("ambiguity", "medium"),
                    _AMBIGUITIES,
                    f"keywords[{index}].ambiguity",
                ),
            )
        )
    return tuple(parsed)


def _parse_overrides(data: dict[str, object], theme: str) -> tuple[OverrideRule, ...]:
    raw_overrides = data.get("overrides") or []
    if not isinstance(raw_overrides, list):
        raise ValueError("overrides must be a list")

    parsed: list[OverrideRule] = []
    for index, raw in enumerate(raw_overrides):
        if not isinstance(raw, dict):
            raise ValueError(f"overrides[{index}] must be a mapping")
        codes = _filter_ts_codes([str(raw.get("ts_code", ""))], f"overrides[{index}].ts_code")
        if len(codes) != 1:
            raise ValueError(f"overrides[{index}].ts_code must be a valid ts_code")
        override_theme = _validated_choice(
            raw.get("theme", theme), _THEMES, f"overrides[{index}].theme"
        )
        if override_theme != theme:
            raise ValueError(f"overrides[{index}].theme must match rules theme {theme}")
        reason = str(raw.get("reason", "")).strip()
        if not reason:
            raise ValueError(f"overrides[{index}].reason must not be empty")
        term = raw.get("term")
        parsed.append(
            OverrideRule(
                ts_code=codes[0],
                action=_validated_choice(
                    raw.get("action"), _OVERRIDE_ACTIONS, f"overrides[{index}].action"
                ),
                tier=_validated_choice(
                    raw.get("tier", "both"), _OVERRIDE_TIERS, f"overrides[{index}].tier"
                ),
                theme=override_theme,
                term=str(term).strip() if term is not None else None,
                reason=reason,
            )
        )
    return tuple(parsed)


def _rules_from_dict(data: object, *, default_theme: str = "animal") -> Rules:
    data = data or {}
    if not isinstance(data, dict):
        data = {}

    theme = _validated_choice(data.get("theme", default_theme), _THEMES, "theme")
    strict = _as_list(data.get("strict_keywords"))
    extended = _as_list(data.get("extended_keywords"))
    exclude_patterns = _as_list(data.get("exclude_patterns"))
    force_include = _filter_ts_codes(_as_list(data.get("force_include")), "force_include")
    force_exclude = _filter_ts_codes(_as_list(data.get("force_exclude")), "force_exclude")

    keyword_rules = _parse_keyword_rules(data, theme, strict, extended)
    if data.get("keywords") is not None:
        strict = [item.term for item in keyword_rules if item.tier == "strict"]
        extended = [item.term for item in keyword_rules if item.tier == "extended"]
    merged_extended = _unique_preserve([*strict, *extended])

    return Rules(
        strict_keywords=tuple(strict),
        extended_keywords=tuple(merged_extended),
        exclude_patterns=tuple(exclude_patterns),
        force_include=tuple(force_include),
        force_exclude=tuple(force_exclude),
        exclude_st=bool(data.get("exclude_st", True)),
        allow_beijing=bool(data.get("allow_beijing", False)),
        min_listing_days=int(data.get("min_listing_days", 0) or 0),
        min_daily_amount=float(data.get("min_daily_amount", 0.0) or 0.0),
        max_suspension_days=int(data.get("max_suspension_days", 0) or 0),
        theme=theme,
        keyword_rules=keyword_rules,
        overrides=_parse_overrides(data, theme),
    )


def load_rules(path: Path) -> Rules:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    default_theme = "plant" if path.name == "plant_rules.yml" else "animal"
    return _rules_from_dict(data, default_theme=default_theme)


def load_backtest_config(path: Path) -> BacktestConfig:
    if not path.exists():
        return BacktestConfig()
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        data = {}
    rates = {
        name: float(data.get(name, 0.0) or 0.0)
        for name in ("commission_rate", "stamp_tax_rate", "slippage_rate")
    }
    if any(value < 0 for value in rates.values()):
        raise ValueError("backtest cost rates must be non-negative")
    return BacktestConfig(enabled=bool(data.get("enabled", False)), **rates)


def load_rules_asof(
    as_of: str,
    rules_path: Path,
    history_path: Path | None = None,
) -> Rules:
    """按生效日选取规则版本，支持 point-in-time 回放。

    rules.yml 视作最新版本（effective_from 视为最大）。若存在 rules_history.yml，
    其中每个条目含 effective_from 与该时点生效的规则，选取 effective_from <= as_of
    中最大的一条；若 as_of 早于所有历史版本，则取最早一条，避免把当前规则
    错配到没有对应历史记录的远古区间。
    """
    if history_path is None:
        history_path = rules_path.with_name("rules_history.yml")

    history_versions: list[tuple[str, Rules]] = []
    default_theme = load_rules(rules_path).theme
    if history_path.exists():
        try:
            raw = yaml.safe_load(history_path.read_text(encoding="utf-8")) or []
        except yaml.YAMLError:
            raw = []
        if isinstance(raw, list):
            for entry in raw:
                if not isinstance(entry, dict):
                    continue
                effective_from = str(entry.get("effective_from", "00000000"))
                history_versions.append(
                    (
                        effective_from,
                        _rules_from_dict(entry.get("rules", {}), default_theme=default_theme),
                    )
                )

    # rules.yml 作为最新版本：当 as_of 达到或超过最后一版历史规则的生效日时，
    # 当前规则自然胜出；as_of 落在历史版本区间内时取对应版本；早于所有历史版本
    # 时取最早已知版本，避免把当前规则错配到没有对应记录的远古区间。
    latest = load_rules(rules_path)
    return _select_rules_version(as_of, history_versions, latest)


def _select_rules_version(
    as_of: str,
    history_versions: list[tuple[str, Rules]],
    latest: Rules,
) -> Rules:
    as_of_value = str(as_of)
    if not history_versions:
        return latest

    max_hist_eff = max(eff for eff, _ in history_versions)
    if as_of_value >= max_hist_eff:
        return latest

    candidates = [(eff, rules) for eff, rules in history_versions if eff <= as_of_value]
    if candidates:
        return max(candidates, key=lambda item: item[0])[1]
    return min(history_versions, key=lambda item: item[0])[1]
