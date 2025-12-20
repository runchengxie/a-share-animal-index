from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

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


def load_rules(path: Path) -> Rules:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    strict = _as_list(data.get("strict_keywords"))
    extended = _as_list(data.get("extended_keywords"))
    exclude_patterns = _as_list(data.get("exclude_patterns"))
    force_include = _as_list(data.get("force_include"))
    force_exclude = _as_list(data.get("force_exclude"))

    merged_extended = _unique_preserve([*strict, *extended])

    return Rules(
        strict_keywords=tuple(strict),
        extended_keywords=tuple(merged_extended),
        exclude_patterns=tuple(exclude_patterns),
        force_include=tuple(force_include),
        force_exclude=tuple(force_exclude),
        exclude_st=bool(data.get("exclude_st", True)),
        allow_beijing=bool(data.get("allow_beijing", False)),
    )
