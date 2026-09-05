from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

PROMPT_VERSION = "animal-plant-audit-v1"


class ProviderError(RuntimeError):
    pass


@dataclass(frozen=True)
class ProviderAttempt:
    provider: str
    model_requested: str
    status: str
    error: str | None = None
    model_actual: str | None = None


@dataclass(frozen=True)
class ProviderResult:
    provider: str
    model_requested: str
    model_actual: str | None
    status: str
    rows: tuple[dict[str, object], ...]
    attempts: tuple[ProviderAttempt, ...] = ()


class LLMProvider(Protocol):
    name: str
    model: str

    def audit_batch(self, candidates: list[dict[str, object]]) -> ProviderResult: ...


def _request_json(
    url: str, headers: Mapping[str, str], payload: Mapping[str, object], timeout: int
) -> dict[str, object]:
    request = Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={**headers, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except (HTTPError, URLError, TimeoutError) as exc:
        raise ProviderError(str(exc)) from exc
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ProviderError("provider response is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise ProviderError("provider response must be a JSON object")
    return decoded


def _prompt(candidates: list[dict[str, object]]) -> str:
    return (
        "你是证券简称语义审核员。只根据证券简称本身判断，不参考主营业务。\n"
        "请返回 JSON 数组，每个输入 ts_code 恰好一行。\n"
        "classification 只能是 explicit、symbolic、ambiguous、incidental、none。\n"
        "theme 只能是 animal、plant、none；reality 只能是 real、mythical、collective、none。\n"
        "recommended_tier 只能是 strict、extended、manual、reject。confidence 仅用于排序。\n"
        f"输入：{json.dumps(candidates, ensure_ascii=False, sort_keys=True)}"
    )


def _normalize_rows(raw: object, expected_codes: set[str]) -> tuple[dict[str, object], ...]:
    rows = raw.get("results") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        raise ProviderError("structured response must contain a list or results list")
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ProviderError("each audit result must be an object")
        code = str(row.get("ts_code", "")).strip().upper()
        if code not in expected_codes or code in seen:
            raise ProviderError("provider returned unknown or duplicate ts_code")
        seen.add(code)
        normalized.append(dict(row))
    if seen != expected_codes:
        raise ProviderError("provider did not return one result for every input")
    return tuple(normalized)


class GeminiProvider:
    name = "gemini"

    def __init__(self, api_key: str, model: str, timeout_seconds: int = 30) -> None:
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = timeout_seconds

    def audit_batch(self, candidates: list[dict[str, object]]) -> ProviderResult:
        payload = {
            "contents": [{"parts": [{"text": _prompt(candidates)}]}],
            "generationConfig": {"responseMimeType": "application/json", "temperature": 0},
        }
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self.model}:generateContent?key={self.api_key}"
        )
        response = _request_json(url, {}, payload, self.timeout_seconds)
        try:
            response_candidates = cast(list[dict[str, object]], response["candidates"])
            content = cast(dict[str, object], response_candidates[0]["content"])
            parts = cast(list[dict[str, object]], content["parts"])
            text = cast(str, parts[0]["text"])
            rows = _normalize_rows(json.loads(text), {str(item["ts_code"]) for item in candidates})
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            raise ProviderError("invalid Gemini structured response") from exc
        return ProviderResult(self.name, self.model, self.model, "succeeded", rows)


class OpenRouterProvider:
    name = "openrouter"

    def __init__(self, api_key: str, model: str, timeout_seconds: int = 30) -> None:
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = timeout_seconds

    def audit_batch(self, candidates: list[dict[str, object]]) -> ProviderResult:
        payload = {
            "model": self.model,
            "temperature": 0,
            "messages": [{"role": "user", "content": _prompt(candidates)}],
            "response_format": {"type": "json_object"},
        }
        response = _request_json(
            "https://openrouter.ai/api/v1/chat/completions",
            {"Authorization": f"Bearer {self.api_key}"},
            payload,
            self.timeout_seconds,
        )
        try:
            choices = cast(list[dict[str, object]], response["choices"])
            message = cast(dict[str, object], choices[0]["message"])
            text = cast(str, message["content"])
            rows = _normalize_rows(json.loads(text), {str(item["ts_code"]) for item in candidates})
            actual = str(response.get("model", self.model))
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            raise ProviderError("invalid OpenRouter structured response") from exc
        return ProviderResult(self.name, self.model, actual, "succeeded", rows)


class FallbackChain:
    def __init__(self, providers: list[LLMProvider], retries: int = 1) -> None:
        self.providers = providers
        self.retries = max(0, retries)

    def audit_batch(self, candidates: list[dict[str, object]]) -> ProviderResult:
        attempts: list[ProviderAttempt] = []
        for provider in self.providers:
            for attempt in range(self.retries + 1):
                try:
                    result = provider.audit_batch(candidates)
                    attempts.append(
                        ProviderAttempt(
                            provider.name,
                            provider.model,
                            "succeeded",
                            model_actual=result.model_actual,
                        )
                    )
                    return ProviderResult(
                        result.provider,
                        result.model_requested,
                        result.model_actual,
                        result.status,
                        result.rows,
                        tuple(attempts),
                    )
                except Exception as exc:  # noqa: PERF203 - provider fallback is intentionally isolated
                    attempts.append(
                        ProviderAttempt(provider.name, provider.model, "failed", str(exc))
                    )
                    if attempt < self.retries:
                        time.sleep(0.2 * (attempt + 1))
        return ProviderResult(
            provider="",
            model_requested="",
            model_actual=None,
            status="provider_unavailable",
            rows=(),
            attempts=tuple(attempts),
        )
