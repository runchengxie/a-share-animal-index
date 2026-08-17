from __future__ import annotations

import re
import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import yaml


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


def _rules_from_dict(data: object) -> Rules:
    data = data or {}
    if not isinstance(data, dict):
        data = {}

    strict = _as_list(data.get("strict_keywords"))
    extended = _as_list(data.get("extended_keywords"))
    exclude_patterns = _as_list(data.get("exclude_patterns"))
    force_include = _filter_ts_codes(_as_list(data.get("force_include")), "force_include")
    force_exclude = _filter_ts_codes(_as_list(data.get("force_exclude")), "force_exclude")

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
    )


def load_rules(path: Path) -> Rules:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return _rules_from_dict(data)


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
                history_versions.append((effective_from, _rules_from_dict(entry.get("rules", {}))))

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
