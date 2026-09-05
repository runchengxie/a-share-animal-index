import pytest

from zoo_index.llm import FallbackChain, ProviderError, ProviderResult, _normalize_rows


class FakeProvider:
    def __init__(
        self,
        name: str,
        response: list[dict[str, object]] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.name = name
        self.model = f"{name}-model"
        self.response = response
        self.error = error

    def audit_batch(self, candidates):
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return ProviderResult(self.name, self.model, self.model, "succeeded", tuple(self.response))


def test_fallback_uses_openrouter_after_gemini_429() -> None:
    valid: list[dict[str, object]] = [{"ts_code": "002081.SZ", "classification": "explicit"}]
    chain = FallbackChain(
        [
            FakeProvider("gemini", error=ProviderError("429")),
            FakeProvider("openrouter", response=valid),
        ],
        retries=0,
    )

    result = chain.audit_batch([{"ts_code": "002081.SZ", "name": "金螳螂"}])

    assert result.provider == "openrouter"
    assert [attempt.status for attempt in result.attempts] == ["failed", "succeeded"]


def test_fallback_returns_nonfatal_failure_when_all_providers_fail() -> None:
    chain = FallbackChain(
        [FakeProvider("gemini", error=TimeoutError("timeout"))],
        retries=0,
    )

    result = chain.audit_batch([{"ts_code": "002081.SZ", "name": "金螳螂"}])

    assert result.status == "provider_unavailable"
    assert result.rows == ()
    assert result.attempts[0].status == "failed"


def test_structured_rows_must_cover_exact_input_codes() -> None:
    with pytest.raises(ProviderError, match="every input"):
        _normalize_rows([], {"002081.SZ"})

    with pytest.raises(ProviderError, match="unknown or duplicate"):
        _normalize_rows(
            [{"ts_code": "000001.SZ"}],
            {"002081.SZ"},
        )
