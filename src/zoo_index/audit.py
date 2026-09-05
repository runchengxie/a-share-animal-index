from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import Rules
from .index import _apply_namechange, prepare_universe_asof
from .llm import PROMPT_VERSION, FallbackChain, ProviderAttempt
from .matcher import Matcher, MatchResult


@dataclass(frozen=True)
class AuditCandidate:
    ts_code: str
    name: str
    theme: str
    strict: bool
    extended: bool
    matched_term: str | None
    reality: str | None
    ambiguity: str | None
    decision: str
    eligible: bool
    eligibility_reasons: tuple[str, ...]
    review_scope: str


@dataclass(frozen=True)
class AuditResult:
    schema_version: str
    generated_at: str
    as_of: str
    input_hash: str
    rules_hash: str
    mode: str
    status: str
    candidates: tuple[AuditCandidate, ...]
    prompt_version: str | None = None
    llm_rows: tuple[dict[str, object], ...] = ()
    provider_attempts: tuple[ProviderAttempt, ...] = ()


def _canonical_frame_hash(df: pd.DataFrame) -> str:
    columns = sorted(str(column) for column in df.columns)
    normalized = df.loc[:, columns].fillna("").astype(str).sort_values(columns)
    payload = normalized.to_json(orient="records", force_ascii=False, date_format="iso")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _rules_hash(rules: Rules) -> str:
    payload = {
        "theme": rules.theme,
        "keyword_rules": [asdict(item) for item in rules.keyword_rules],
        "overrides": [asdict(item) for item in rules.overrides],
        "strict_keywords": rules.strict_keywords,
        "extended_keywords": rules.extended_keywords,
        "exclude_patterns": rules.exclude_patterns,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _eligibility_reasons(
    row: pd.Series, as_of: str, rules: Rules, eligible_codes: set[str]
) -> tuple[str, ...]:
    code = str(row["ts_code"])
    reasons: list[str] = []
    if code in eligible_codes:
        return ()
    if rules.allow_beijing is False and code.endswith(".BJ"):
        reasons.append("beijing_not_allowed")
    name = str(row.get("name", "") or "")
    if rules.exclude_st and "ST" in name.upper():
        reasons.append("st_excluded")
    delist_date = str(row.get("delist_date", "") or "")
    if delist_date and delist_date != "nan" and delist_date <= as_of:
        reasons.append("delisted")
    list_date = str(row.get("list_date", "") or "")
    if rules.min_listing_days > 0 and list_date and list_date != "nan":
        try:
            listed = datetime.strptime(list_date, "%Y%m%d")
            observed = datetime.strptime(as_of, "%Y%m%d")
            if (observed - listed).days < rules.min_listing_days:
                reasons.append("listing_age_too_short")
        except ValueError:
            reasons.append("invalid_list_date")
    if not reasons:
        reasons.append("filtered_from_eligible_universe")
    return tuple(reasons)


def _candidate_from_match(
    row: pd.Series,
    result: MatchResult,
    eligible_codes: set[str],
    as_of: str,
    rules: Rules,
    review_scope: str,
) -> AuditCandidate:
    code = str(row["ts_code"])
    return AuditCandidate(
        ts_code=code,
        name=str(row.get("name", "") or ""),
        theme=result.theme,
        strict=result.strict,
        extended=result.extended,
        matched_term=result.matched_term,
        reality=result.reality,
        ambiguity=result.ambiguity,
        decision=result.decision,
        eligible=code in eligible_codes,
        eligibility_reasons=_eligibility_reasons(row, as_of, rules, eligible_codes),
        review_scope=review_scope,
    )


def build_audit_candidates(
    stock_basic: pd.DataFrame,
    namechange: pd.DataFrame,
    as_of: str,
    rules: Rules,
    mode: str = "all",
) -> list[AuditCandidate]:
    if mode not in {"all", "recall", "precision"}:
        raise ValueError("mode must be one of: all, recall, precision")
    if "ts_code" not in stock_basic.columns or "name" not in stock_basic.columns:
        raise ValueError("stock_basic must contain ts_code and name columns")

    named = _apply_namechange(stock_basic.copy(), namechange, as_of)
    eligible = prepare_universe_asof(stock_basic, namechange, as_of, rules)
    eligible_codes = set(eligible["ts_code"].astype(str))
    matcher = Matcher(rules)
    candidates: list[AuditCandidate] = []

    ordered = named.sort_values("ts_code")
    for row in ordered.itertuples(index=False, name=None):
        row_series = pd.Series(row, index=ordered.columns)
        result = matcher.classify(str(row_series["ts_code"]), str(row_series.get("name", "") or ""))
        scope = "precision" if result.matched_term or result.decision != "none" else "recall"
        if mode != "all" and scope != mode:
            continue
        candidates.append(
            _candidate_from_match(row_series, result, eligible_codes, as_of, rules, scope)
        )
    return candidates


def build_audit_result(
    stock_basic: pd.DataFrame,
    candidates: list[AuditCandidate],
    as_of: str,
    rules: Rules,
    mode: str,
    llm_rows: tuple[dict[str, object], ...] = (),
    provider_attempts: tuple[ProviderAttempt, ...] = (),
    llm_status: str = "not_requested",
) -> AuditResult:
    return AuditResult(
        schema_version="1",
        generated_at=datetime.now(timezone.utc).isoformat(),
        as_of=as_of,
        input_hash=_canonical_frame_hash(stock_basic),
        rules_hash=_rules_hash(rules),
        mode=mode,
        status=llm_status,
        candidates=tuple(candidates),
        prompt_version=PROMPT_VERSION if llm_status != "not_requested" else None,
        llm_rows=llm_rows,
        provider_attempts=provider_attempts,
    )


def run_llm_audit(
    candidates: list[AuditCandidate], chain: FallbackChain, batch_size: int = 200
) -> tuple[tuple[dict[str, object], ...], tuple[ProviderAttempt, ...], str]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    rows: list[dict[str, object]] = []
    attempts: list[ProviderAttempt] = []
    status = "succeeded"
    for start in range(0, len(candidates), batch_size):
        batch = candidates[start : start + batch_size]
        payload: list[dict[str, object]] = [
            {"ts_code": item.ts_code, "name": item.name, "theme": item.theme} for item in batch
        ]
        result = chain.audit_batch(payload)
        rows.extend(result.rows)
        attempts.extend(result.attempts)
        if result.status != "succeeded":
            status = "provider_unavailable" if not rows else "partial"
    return tuple(rows), tuple(attempts), status


def write_audit_report(result: AuditResult, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"audit_{result.as_of}.json"
    markdown_path = output_dir / f"audit_{result.as_of}.md"
    payload = asdict(result)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    groups = {
        "Potential missing": [item for item in result.candidates if item.review_scope == "recall"],
        "Potential false positives": [
            item
            for item in result.candidates
            if item.review_scope == "precision" and item.ambiguity in {"medium", "high"}
        ],
    }
    lines = [
        f"# Zoo audit {result.as_of}",
        "",
        f"- Status: `{result.status}`",
        f"- Mode: `{result.mode}`",
        f"- Input hash: `{result.input_hash}`",
        f"- Rules hash: `{result.rules_hash}`",
    ]
    for title, items in groups.items():
        lines.extend(["", f"## {title}", ""])
        if not items:
            lines.append("无候选。")
            continue
        for item in items:
            term = item.matched_term or "未命中"
            lines.append(
                f"- `{item.ts_code}` {item.name} → {term} "
                f"({item.theme}, {item.reality or 'unknown'}, "
                f"eligible={str(item.eligible).lower()})"
            )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, markdown_path
