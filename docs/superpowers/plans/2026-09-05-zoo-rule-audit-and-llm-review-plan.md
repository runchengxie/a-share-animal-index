# Zoo Rule Audit and LLM Review Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将动物园与植物园的收录规则整理为可解释、可审计、可复现的词典与人工例外体系，并增加 Gemini/OpenRouter 审核候选生成功能，但不让 LLM 直接改变正式指数成分。

**Architecture:** 规则层分为生物词典、简称级人工覆盖和投资资格过滤三部分。现有确定性 matcher 继续负责正式指数，新的 audit 命令读取同一批证券简称，分别生成规则命中、未命中和歧义候选，再通过可插拔的 LLM provider 批量审核，结果落盘为带模型元数据的 JSON/Markdown。只有人工审核后才修改正式规则文件；LLM 不参与每日指数计算路径。

**Tech Stack:** Python 3.10+, pandas, PyYAML, pytest, uv, Gemini Developer API, OpenRouter OpenAI-compatible HTTP API。

**Spec:** 本计划直接落实用户关于“收录规则优化、漏收检查、龙的分类、Gemini/OpenRouter fallback”和此前对话中形成的审计架构；现有方法定义见 `docs/methodology.md`。

## Global Constraints

- 正式指数生成必须继续只依赖版本化的本地规则和确定性 matcher。
- LLM 结果必须保存原始输入摘要、provider、请求模型、实际模型（如可获得）、prompt 版本、时间、响应状态和结果哈希。
- 不允许在线实时 LLM 调用决定每日或历史指数成分。
- 第一版只根据证券简称本身判断，不把主营业务、行业分类或公司实际经营内容当作名称命中依据。
- `force_include` / `force_exclude` 仍只接受 `ts_code`，新人工例外不得通过伪造关键词实现。
- ST、交易所、上市时间、成交额和停牌等投资资格过滤继续由 `index.py` 与规则字段负责，不与主题成员身份混合。
- provider 全部失败时 audit 命令仍应生成失败记录并以成功退出或明确的非致命状态结束，不能影响 `zoo-index`。
- 不在仓库提交 API key、完整 prompt 中的 secret 或未审查的外部响应。

## 现状盘点与范围

当前仓库已经具备：

- `rules.yml` 与 `plant_rules.yml` 两套词表。
- `Matcher.classify()` 的最长词优先子串匹配。
- `rules_history.yml` 的 point-in-time 规则回放。
- `force_include` / `force_exclude`、ST 与北交所过滤。
- 独立的植物园产物和可交易回测层。

当前真正的问题是：

1. `plant_rules.yml` 的 `extended_keywords` 同时承担植物词和公司专名模式，例如 `雷柏`、`柳钢`、`艾迪`，语义边界已不清楚。
2. `force_include` 只能同时进入 strict 与 extended，不能表达“只进扩展层”或“这是人工确认的歧义词”。
3. `exclude_st` 和 `allow_beijing` 在成员筛选阶段生效，无法报告“名称符合但当前不可投资”的股票。
4. matcher 只返回命中词，无法表达现实动物、植物、神话生物、歧义和偶然字符命中。
5. 没有候选审计命令，无法系统找出 `金螳螂`、`骆驼股份`、`金麒麟`、`凤凰传媒` 等可能漏项，也无法批量复核 `龙`、`马`、`柏` 等高歧义命中。

本计划不在第一阶段承诺一次性决定全部词表。先建立数据结构和审计能力，再用审计结果逐批批准规则。

## 文件结构与职责

计划中的文件边界如下：

- Modify `rules.yml`, `plant_rules.yml`: 迁移为带语义元数据的词典配置，保留兼容读取路径。
- Modify `src/zoo_index/config.py`: 增加词典项和简称覆盖的数据类、校验和向后兼容解析。
- Modify `src/zoo_index/matcher.py`: 返回主题、命中词、语义类别和人工决策；保持旧调用字段兼容。
- Modify `src/zoo_index/index.py`: 继续只使用 matcher 的正式层级结果，确认资格过滤不被主题成员审计改写。
- Create `src/zoo_index/audit.py`: 生成确定性 audit candidates、调用 provider、合并结果和写出 JSON/Markdown。
- Create `src/zoo_index/llm.py`: 定义 provider 协议、Gemini provider、OpenRouter provider、重试与 fallback。
- Modify `src/zoo_index/cli.py`: 增加 `zoo-audit` 入口和参数，不改变 `zoo-index` 参数行为。
- Create `tests/test_rule_schema.py`, `tests/test_audit.py`, `tests/test_llm.py`: 锁定新规则结构、纯本地审计逻辑和 provider fallback。
- Modify `tests/test_matcher.py`, `tests/test_plant_rules.py`, `tests/test_rules.py`: 更新兼容性测试和新增真实候选案例。
- Modify `docs/methodology.md`, `docs/architecture.md`, `README.md`: 解释分类边界、审计流程、凭据和输出物。
- Keep `artifacts/audit/` as the local, ignored audit output directory; do not add unreviewed audit results to `published/data/`.

### Task 1: 固化分类政策与兼容的规则数据模型

**Files:**
- Create: `docs/rule-policy.md`
- Modify: `src/zoo_index/config.py`
- Create: `tests/test_rule_schema.py`

**Interfaces:**
- Produces `KeywordRule(term: str, theme: Literal["animal", "plant"], tier: Literal["strict", "extended"], reality: Literal["real", "mythical", "collective"], ambiguity: Literal["low", "medium", "high"])`。
- Produces `OverrideRule(ts_code: str, action: Literal["include", "exclude"], tier: Literal["strict", "extended", "both"], theme: Literal["animal", "plant"], term: str | None, reason: str)`。
- `Rules` exposes `keyword_rules` and `overrides` while preserving `strict_keywords`, `extended_keywords`, `force_include` and `force_exclude` compatibility properties during migration。

- [ ] **Step 1: Write policy examples and failing schema tests**

  Add tests covering these exact cases:

  ```python
  def test_parse_typed_keywords_and_overrides(tmp_path):
      path = tmp_path / "rules.yml"
      path.write_text(
          """
          theme: animal
          keywords:
            - term: 螳螂
              tier: strict
              reality: real
              ambiguity: low
            - term: 龙
              tier: extended
              reality: mythical
              ambiguity: medium
          overrides:
            - ts_code: 002081.SZ
              action: include
              tier: strict
              theme: animal
              term: 螳螂
              reason: 明确现实动物名
          """,
          encoding="utf-8",
      )
      rules = load_rules(path)
      assert rules.theme == "animal"
      assert rules.keyword_rules[0].term == "螳螂"
      assert rules.keyword_rules[1].reality == "mythical"
      assert rules.overrides[0].ts_code == "002081.SZ"
  ```

  Also assert that malformed tier, reality, action, or non-`ts_code` override raises `ValueError`.

- [ ] **Step 2: Run the focused tests and verify they fail**

  Run: `uv run pytest tests/test_rule_schema.py -q`

  Expected: FAIL because typed keyword and override parsing do not exist.

- [ ] **Step 3: Implement typed parsing without removing legacy fields**

  Extend `config.py` with frozen dataclasses and parse `theme`, a new `keywords` list, and an `overrides` list. If a file has no typed `keywords`, convert the existing `strict_keywords` and `extended_keywords` lists into low/medium ambiguity `real` entries under the file's declared or inferred theme. Preserve the current merge invariant: every strict term is available to extended matching.

  Do not interpret a company-specific pattern such as `雷柏` as a plant term during migration. Put such existing behavior into explicit overrides only after it is reviewed.

- [ ] **Step 4: Run schema and legacy tests**

  Run: `uv run pytest tests/test_rule_schema.py tests/test_rules.py tests/test_rules_asof.py -q`

  Expected: PASS, including current legacy YAML behavior and point-in-time loading.

- [ ] **Step 5: Commit the data-model change**

  Run:

  ```bash
  git add src/zoo_index/config.py tests/test_rule_schema.py tests/test_rules.py tests/test_rules_asof.py docs/rule-policy.md
  git commit -m "refactor: add typed zoo rule schema"
  ```

### Task 2: Separate semantic matching from investment eligibility

**Files:**
- Modify: `src/zoo_index/matcher.py`
- Modify: `src/zoo_index/index.py`
- Modify: `tests/test_matcher.py`
- Modify: `tests/test_plant_rules.py`
- Modify: `tests/test_runner.py`

**Interfaces:**
- `MatchResult` gains `theme`, `matched_term`, `reality`, `ambiguity`, `decision`, and `eligibility_reason` fields with safe defaults.
- `Matcher.classify(ts_code, name)` returns theme membership independently of ST and exchange eligibility.
- `build_constituents()` continues to emit only eligible strict/extended rows, but audit code can call the matcher before eligibility filtering.

- [ ] **Step 1: Add failing matcher tests**

  Add exact cases:

  ```python
  def test_mythical_dragon_is_extended_not_strict():
      result = Matcher(typed_rules).classify("000001.SZ", "飞龙股份")
      assert result.extended
      assert not result.strict
      assert result.reality == "mythical"
      assert result.matched_term == "龙"

  def test_override_can_include_only_extended():
      result = Matcher(typed_rules_with_override).classify("603586.SH", "金麒麟")
      assert not result.strict
      assert result.extended
      assert result.decision == "override"

  def test_st_is_not_a_name_membership_decision():
      result = Matcher(typed_rules).classify("605199.SH", "ST葫芦娃")
      assert result.extended
      assert result.matched_term == "葫芦"
  ```

- [ ] **Step 2: Run focused matcher tests and verify failure**

  Run: `uv run pytest tests/test_matcher.py tests/test_plant_rules.py -q`

  Expected: FAIL because the current result cannot express semantic category or tier-specific overrides.

- [ ] **Step 3: Implement longest typed-term matching and overrides**

  Keep deterministic longest-term ordering. A typed term with `tier: strict` sets both strict and extended membership; `tier: extended` sets extended only. Mythical terms such as `龙`, `凤凰`, `麒麟`, `鹏` are extended candidates unless an explicit policy says otherwise. Apply `force_exclude` first, then typed overrides, then exclusion patterns, then dictionary matching.

  Keep `exclude_patterns` as deterministic name exclusions, but expose a reason instead of silently erasing the candidate from audit output.

- [ ] **Step 4: Confirm eligibility remains separate**

  In `index.py`, retain `_filter_exchange`, `_filter_st`, listing age and liquidity filtering in the universe preparation path. Add a test showing `ST葫芦娃` can be a semantic plant member but is absent from an index constituent frame when `exclude_st=True`.

- [ ] **Step 5: Run the full Python suite**

  Run: `uv run pytest -q`

  Expected: PASS with current animal/plant outputs unchanged except for explicitly approved new rules.

- [ ] **Step 6: Commit the matcher boundary**

  Run:

  ```bash
  git add src/zoo_index/matcher.py src/zoo_index/index.py tests/test_matcher.py tests/test_plant_rules.py tests/test_runner.py
  git commit -m "refactor: separate theme membership from eligibility"
  ```

### Task 3: Migrate and audit the first deterministic rule set

**Files:**
- Modify: `rules.yml`
- Modify: `plant_rules.yml`
- Modify: `rules_history.yml`
- Modify: `tests/test_plant_rules.py`
- Create: `tests/test_rule_candidates.py`
- Modify: `docs/methodology.md`

**Interfaces:**
- Produces a typed, reviewable rule set.
- Adds explicit cases for `螳螂`, `骆驼`, `獐`, `麒麟`, `凤凰`, `葫芦`, `榕`, and `桐` at the policy-selected tier.
- Keeps company-specific strings such as `雷柏`, `柳钢`, `艾迪` out of the biological dictionary unless represented as a reviewed override with a reason.

- [ ] **Step 1: Add regression tests for approved candidates**

  Add tests for `002081.SZ 金螳螂`, `601311.SH 骆驼股份`, `603586.SH 金麒麟`, `601928.SH 凤凰传媒`, `605199.SH ST葫芦娃`, `002474.SZ 榕基软件`, and `601233.SH 桐昆股份`. Test strict/extended membership separately from eligibility.

- [ ] **Step 2: Run the candidate tests and verify missing cases**

  Run: `uv run pytest tests/test_rule_candidates.py tests/test_plant_rules.py -q`

  Expected: FAIL for any candidate not yet represented in the new typed rules.

- [ ] **Step 3: Update typed YAML and rule history**

  Add only terms whose policy classification is decided. Use `reality: mythical` for dragon, phoenix, qilin, and peng-type entries. Use `ambiguity: high` for single-character plant candidates. Add a dated entry to `rules_history.yml` so backfills do not silently rewrite the old index.

  Do not enable broad terms such as `草`, `柏`, `柳`, `桐`, or `燕` globally in the strict layer. Represent them as extended candidate terms or reviewed overrides with explicit ambiguity.

- [ ] **Step 4: Update method documentation**

  Document the policy: real low-ambiguity multi-character organisms may be strict; mythical creatures are extended symbolic candidates; ambiguous single characters are extended/manual-review candidates; company business relevance alone does not qualify.

- [ ] **Step 5: Run regression tests and inspect constituent diff**

  Run: `uv run pytest -q`.

  Then run the repository's normal deterministic calculation against a fixed cached fixture or existing published snapshot and inspect the changed constituent codes before any public backfill.

- [ ] **Step 6: Commit the approved rule migration separately from code refactoring**

  Run:

  ```bash
  git add rules.yml plant_rules.yml rules_history.yml tests/test_rule_candidates.py tests/test_plant_rules.py docs/methodology.md
  git commit -m "data: expand reviewed animal and plant candidates"
  ```

### Task 4: Build the deterministic audit candidate generator

**Files:**
- Create: `src/zoo_index/audit.py`
- Modify: `src/zoo_index/data_sources/tushare.py` only if a narrow reference-data method is needed
- Create: `tests/test_audit.py`
- Modify: `src/zoo_index/cli.py`

**Interfaces:**
- `AuditCandidate` contains `ts_code`, `name`, `current_membership`, `matched_terms`, `eligible`, `eligibility_reasons`, and `review_scope`.
- `build_audit_candidates(stock_basic, namechange, as_of, rules, mode="all") -> list[AuditCandidate]`.
- `write_audit_report(result, output_dir) -> tuple[Path, Path]` writes JSON and Markdown.
- CLI command: `uv run zoo-audit --date YYYYMMDD --output-dir artifacts/audit --mode all`.

- [ ] **Step 1: Write pure-function tests**

  Test that the generator emits:

  - matched candidates such as `龙` and `柏` for precision review;
  - unmatched candidates for recall review;
  - ST and Beijing candidates with eligibility reasons instead of deleting them;
  - deterministic ordering by `ts_code` and stable JSON serialization;
  - no Tushare or LLM calls in the pure candidate builder.

- [ ] **Step 2: Run tests and verify failure**

  Run: `uv run pytest tests/test_audit.py -q`

  Expected: FAIL because the audit module and CLI do not exist.

- [ ] **Step 3: Implement candidate generation from existing reference data**

  Reuse `prepare_universe_asof` for eligibility metadata but run the matcher against the full as-of stock list before ST/exchange filtering. Define `mode` values explicitly: `recall` for unmatched names, `precision` for matched names, and `all` for both.

  The report must distinguish `theme_member`, `index_eligible`, `excluded_by_rule`, and `needs_llm_review` so `ST葫芦娃` is not reported as a missing plant term.

- [ ] **Step 4: Register the CLI entry point and add offline fixture support**

  Add `zoo-audit = "zoo_index.cli:audit_main"` to `pyproject.toml` and implement a `zoo-audit` console entry point that accepts a local CSV/Parquet input for tests and offline review, plus the normal Tushare-backed path for real runs. A missing API token must be an error only for the data-fetching path, not for tests using `--input`.

- [ ] **Step 5: Run focused and full tests**

  Run: `uv run pytest tests/test_audit.py tests/test_matcher.py -q` and then `uv run pytest -q`.

- [ ] **Step 6: Commit the deterministic audit layer**

  Run:

  ```bash
  git add src/zoo_index/audit.py src/zoo_index/cli.py tests/test_audit.py
  git commit -m "feat: add deterministic zoo audit candidates"
  ```

### Task 5: Add provider-neutral LLM review with Gemini and OpenRouter fallback

**Files:**
- Create: `src/zoo_index/llm.py`
- Create: `tests/test_llm.py`
- Modify: `src/zoo_index/audit.py`
- Modify: `src/zoo_index/cli.py`
- Modify: `pyproject.toml`
- Modify: `.env.example`

**Interfaces:**
- `LLMProvider.audit_batch(candidates: list[dict]) -> ProviderResult`.
- `GeminiProvider(api_key, model, timeout_seconds=30)`.
- `OpenRouterProvider(api_key, model, timeout_seconds=30)`.
- `FallbackChain(providers).audit_batch(...)` tries providers in order and records each attempt.
- Provider result schema: `classification` in `explicit|symbolic|ambiguous|incidental|none`, `theme` in `animal|plant|none`, `term`, `reality`, `recommended_tier`, `reason`, and `confidence` as an ordering hint only.

- [ ] **Step 1: Write provider contract tests with fake transports**

  Cover these exact cases:

  ```python
  def test_fallback_uses_openrouter_after_gemini_429():
      chain = FallbackChain([
          FakeProvider("gemini", error=ProviderError("429")),
          FakeProvider("openrouter", response=valid_result),
      ])
      result = chain.audit_batch([{"ts_code": "002081.SZ", "name": "金螳螂"}])
      assert result.provider == "openrouter"
      assert [attempt.status for attempt in result.attempts] == ["failed", "succeeded"]
  ```

  Also test malformed JSON, timeout, empty response, all providers failing, and that no exception escapes into `zoo-index`.

- [ ] **Step 2: Run focused tests and verify failure**

  Run: `uv run pytest tests/test_llm.py -q`

  Expected: FAIL because the provider protocol and fallback chain do not exist.

- [ ] **Step 3: Implement one normalized request/response contract**

  Use structured JSON instructions with a pinned `PROMPT_VERSION`. Batch 100–300 candidate names per request, include only `ts_code` and `name`, and require one output row per input code. Validate response codes against the request and mark missing rows as `invalid_response` rather than guessing.

  Keep Gemini and OpenRouter implementations behind the same interface. The OpenRouter chain should support a pinned free model first and `openrouter/free` last. Record `model_requested` and `model_actual` when response metadata exposes it.

- [ ] **Step 4: Add retry and fallback policy**

  Retry transient 429/5xx/timeout once with bounded backoff per provider. Move to the next provider after the retry. Never retry validation errors indefinitely. If all providers fail, write an audit with `status: provider_unavailable` and leave formal rules untouched.

- [ ] **Step 5: Add optional dependency/configuration**

  Prefer the standard library HTTP client or a small existing dependency; do not add a provider SDK unless it is needed for structured output. Read `GEMINI_API_KEY`, `OPENROUTER_API_KEY`, `ZOO_AUDIT_GEMINI_MODEL`, and `ZOO_AUDIT_OPENROUTER_MODEL` from the environment. Keep the base index dependencies usable when no LLM key is configured.

- [ ] **Step 6: Run provider tests and static checks**

  Run: `uv run pytest tests/test_llm.py tests/test_audit.py -q`, `uv run ruff check .`, and `uv run ty check`.

- [ ] **Step 7: Commit the provider layer**

  Run:

  ```bash
  git add src/zoo_index/llm.py src/zoo_index/audit.py src/zoo_index/cli.py tests/test_llm.py pyproject.toml .env.example
  git commit -m "feat: add LLM audit providers with fallback"
  ```

### Task 6: Persist review artifacts and document the human approval loop

**Files:**
- Modify: `src/zoo_index/audit.py`
- Modify: `docs/methodology.md`
- Modify: `docs/architecture.md`
- Modify: `README.md`
- Create: `docs/audit-review.md`
- Create: `tests/test_audit_artifacts.py`

**Interfaces:**
- JSON artifact fields include `schema_version`, `generated_at`, `as_of`, `input_hash`, `rules_hash`, `prompt_version`, `mode`, `status`, `providers`, `candidates`, and `approved_changes`.
- Markdown artifact has sections `Potential missing`, `Potential false positives`, `Mythical candidates`, `Manual review`, and `Provider failures`.
- Approval is a manual edit to typed YAML or a reviewed override; the audit artifact itself never becomes active configuration.

- [ ] **Step 1: Write artifact contract tests**

  Assert stable top-level keys, deterministic candidate ordering, redaction of API keys, and Markdown sections for successful, partial, and all-provider-failed runs.

- [ ] **Step 2: Run tests and verify failure**

  Run: `uv run pytest tests/test_audit_artifacts.py -q`

  Expected: FAIL until artifact serialization and rendering are implemented.

- [ ] **Step 3: Implement versioned JSON/Markdown output**

  Use UTC timestamps, SHA-256 hashes of canonical input/rules/prompt strings, and JSON `ensure_ascii=False`. Store local review runs under `artifacts/audit/` by default. Only copy an explicitly reviewed report to a public location.

- [ ] **Step 4: Document the review procedure**

  Explain the two-pass workflow: `recall` for unmatched names and `precision` for current matches. Explain that model agreement raises review priority but does not auto-approve, and that `confidence` is a ranking signal rather than a calibrated probability. State that `龙` is a mythical animal candidate in extended tier, not a real-animal strict member.

- [ ] **Step 5: Run all tests and quality gates**

  Run: `uv run pytest -q`, `uv run ruff format --check .`, `uv run ruff check .`, and `uv run ty check`.

- [ ] **Step 6: Commit the artifact and documentation contract**

  Run:

  ```bash
  git add src/zoo_index/audit.py tests/test_audit_artifacts.py docs/audit-review.md docs/methodology.md docs/architecture.md README.md
  git commit -m "docs: define reproducible zoo audit review loop"
  ```

### Task 7: Run a real audit and review the index impact before backfill

**Files:**
- Create locally, do not commit until reviewed: `artifacts/audit/<date>.json`, `artifacts/audit/<date>.md`
- Modify only after approval: `rules.yml`, `plant_rules.yml`, `rules_history.yml`
- Regenerate only after approval: `published/` data and manifests

- [ ] **Step 1: Run deterministic candidate generation offline first**

  Run:

  ```bash
  uv run zoo-audit --date 20260904 --mode all --input data/cache/stock_basic.parquet --output-dir artifacts/audit
  ```

  Verify that the report contains both currently matched high-ambiguity names and unmatched recall candidates.

- [ ] **Step 2: Run Gemini primary with OpenRouter fallback**

  Run with keys supplied only through the environment:

  ```bash
  uv run zoo-audit --date 20260904 --mode all --llm --provider-chain gemini,openrouter-pinned,openrouter-free --output-dir artifacts/audit
  ```

  Verify provider attempts, actual model metadata, input hash, and prompt version. Confirm a provider outage still leaves a usable deterministic report.

- [ ] **Step 3: Review candidate classes manually**

  Approve only explicit real terms, clearly documented symbolic/mythical extended terms, and high-confidence overrides. Reject company-industry matches such as livestock companies whose names contain no organism word. Review `龙`, `马`, `象`, `柏`, `柳`, `桐`, `燕` separately.

- [ ] **Step 4: Measure constituent diff**

  Run the existing deterministic calculation with the proposed rule version and compare strict/extended code sets, counts, monthly rebalance dates, and historical NAV. Do not publish a backfill if the diff includes an unexplained name-change or eligibility change.

- [ ] **Step 5: Add the approved rule version and full backfill**

  After review, add the dated `rules_history.yml` version and run:

  ```bash
  uv run python -m zoo_index --output-dir published --backfill --backfill-mode all
  ```

  Inspect `published/data/constituents.json`, `published/data/changes.json`, and the generated manifests before committing generated data.

- [ ] **Step 6: Run final verification before completion**

  Run the full Python and frontend quality gates from `AGENTS.md`. Confirm `zoo-index` does not import or require LLM credentials and that removing both LLM keys does not change deterministic index output.

## Rollout order

1. Land Tasks 1–2 as a compatibility-preserving code change.
2. Land Task 3 as a separately reviewed data/rule change.
3. Land Task 4 before enabling any external provider, so the candidate set is independently inspectable.
4. Land Tasks 5–6 with LLM disabled by default.
5. Run Task 7 manually once, review the report, then decide whether to schedule periodic audit generation. Do not put LLM calls in the daily index GitHub Action until provider cost, rate limits, artifact retention, and failure behavior have been observed for several runs.

## Success criteria

- `zoo-index` remains deterministic and works without any LLM key.
- A name can be a theme member while being ineligible for the current investable index, and the audit report explains both facts.
- Mythical terms are represented explicitly; `龙` is extended/mythical rather than strict/real.
- Company-specific exceptions are recorded as reviewed overrides instead of disguised biological keywords.
- `zoo-audit` can produce a deterministic candidate report offline and can use Gemini, then pinned OpenRouter free, then `openrouter/free` without changing formal rules.
- Every audit result is reproducible from its input hash, rules hash, prompt version, provider/model metadata, and saved response.
- Rule changes are applied only through reviewed YAML/history changes followed by an explicit constituent diff and, when necessary, a full backfill.
