# Zoo Garden Expansion and Tradable Backtest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the plant universe, correct the two-theme editorial UI, and add an opt-in transaction-accounting layer without changing the historical theoretical index fields.

**Architecture:** Keep theme matching and the existing gross index engine intact. Add a focused trade-accounting module that consumes prior holdings, prices, target weights, and execution constraints, then attach its diagnostics and net-return fields to the existing daily/backfill pipeline. The public page continues to present animal and plant snapshots separately.

**Tech Stack:** Python 3, pandas, PyYAML, pytest, React 18, TypeScript, Vite, Node test runner.

**Spec:** `docs/superpowers/specs/2026-09-05-zoo-garden-expansion-and-tradable-backtest-design.md`

## Global Constraints

- Existing `zoo_strict_ret`, `zoo_extended_ret`, and corresponding NAV fields remain the theoretical gross-index fields.
- New transaction costs default to zero and are configured independently from theme keyword rules.
- Plant single-character keywords remain disabled.
- Public outputs must not expose tokens, proxy addresses, or local paths.
- Follow TDD: each production behavior starts with a failing focused test.

### Task 1: Complete plant universe and editorial copy

**Files:**
- Modify: `plant_rules.yml`
- Modify: `tests/test_plant_rules.py`
- Modify: `web/index.html`
- Modify: `web/src/pages/Changes.tsx`
- Modify: `web/src/editorialUi.test.mjs`

**Interfaces:**
- Produces the expanded `Rules.extended_keywords` set and two-theme copy contracts.

- [ ] **Step 1: Run the focused failing tests**

  Run `uv run pytest tests/test_plant_rules.py` and `cd web && npm test`.
  Expected: plant assertions fail for the newly requested names; the page-copy assertion fails before the explanatory sentence exists.

- [ ] **Step 2: Add the requested multi-character plant keywords and copy**

  Add `三花`, `杉杉`, `三柏`, `雷柏`, `艾艾`, `艾迪`, `艾维`, `利柏`, `柏星`, `柏堡`, `松发`, `柳钢`, `柳工`, and `柳化` to `extended_keywords`. Keep the existing negative `松芝股份` case. Add the title and changes-page explanatory copy already specified by the failing tests.

- [ ] **Step 3: Run focused tests**

  Run `uv run pytest tests/test_plant_rules.py` and `cd web && npm test`.
  Expected: both pass.

- [ ] **Step 4: Commit the bounded content change**

  Run `git add plant_rules.yml tests/test_plant_rules.py web/index.html web/src/pages/Changes.tsx web/src/editorialUi.test.mjs && git commit -m "feat: expand plant universe and theme copy"`.

### Task 2: Add transaction-accounting primitives

**Files:**
- Create: `src/zoo_index/trading.py`
- Create: `tests/test_trading.py`

**Interfaces:**
- Produces `TradeCostConfig` with `commission_rate`, `stamp_tax_rate`, and `slippage_rate`.
- Produces `drift_weights(prev_weights, prev_prices, current_prices) -> pd.Series`.
- Produces `compute_trade_accounting(prev_weights, target_weights, prev_prices, current_prices, tradable) -> TradeAccounting`.
- `TradeAccounting` exposes `turnover`, `commission`, `stamp_tax`, `slippage`, `total_cost`, `executed_weights`, and `pending_weights`.

- [ ] **Step 1: Write failing tests**

  Cover: prior equal weights drift after unequal price returns; target-vs-drift turnover uses half absolute trade notional; buy/sell costs use the configured rates; an untradable symbol remains in `pending_weights` and is not counted as executed.

- [ ] **Step 2: Run `uv run pytest tests/test_trading.py`**

  Expected: import or assertion failures because the module and functions do not exist.

- [ ] **Step 3: Implement the minimal pure accounting module**

  Normalize positive finite weights, align symbols by union, drift available prior holdings by `current_price / prev_price`, calculate signed trades, count executed trades only for `tradable=True`, and derive commission from absolute executed trade weights, stamp tax from executed sells, and slippage from absolute executed trade weights.

- [ ] **Step 4: Run the focused tests**

  Run `uv run pytest tests/test_trading.py`.
  Expected: PASS.

- [ ] **Step 5: Commit the primitive**

  Run `git add src/zoo_index/trading.py tests/test_trading.py && git commit -m "feat: add trade accounting primitives"`.

### Task 3: Integrate optional net backtest fields

**Files:**
- Create: `backtest.yml`
- Modify: `src/zoo_index/config.py`
- Modify: `src/zoo_index/runner.py`
- Modify: `src/zoo_index/outputs.py`
- Modify: `tests/test_runner.py`
- Modify: `tests/test_outputs.py`

**Interfaces:**
- `Rules` loading remains unchanged for theme matching.
- Add `BacktestConfig` and `load_backtest_config(path)` with zero-valued cost defaults.
- `DailyResult` carries per-variant trade accounting.
- NAV rows gain `zoo_strict_net_ret`, `zoo_extended_net_ret`, `zoo_strict_net_nav`, `zoo_extended_net_nav`, variant turnover, and total-cost fields while preserving existing columns.

- [ ] **Step 1: Write failing tests**

  Add a config test for zero defaults and a runner/output test proving that gross returns remain unchanged while net returns subtract configured costs and NAV output serializes the new fields.

- [ ] **Step 2: Run focused tests and confirm failure**

  Run `uv run pytest tests/test_runner.py tests/test_outputs.py`.
  Expected: failures for missing config/result/output fields.

- [ ] **Step 3: Implement config and integration**

  Load `backtest.yml` from the command/config path with zero defaults. At each scheduled target-weight change, call the pure accounting module with the prior snapshot and current prices. Keep the existing gross return calculation untouched. Compute net return as gross return minus total transaction cost and carry net NAV independently.

- [ ] **Step 4: Run focused tests**

  Run `uv run pytest tests/test_runner.py tests/test_outputs.py`.
  Expected: PASS.

### Task 4: Add delayed execution diagnostics

**Files:**
- Modify: `src/zoo_index/trading.py`
- Modify: `src/zoo_index/runner.py`
- Modify: `tests/test_trading.py`
- Modify: `tests/test_exception_rebalance.py`

**Interfaces:**
- `tradable` is derived from suspension and daily limit information when available.
- Pending trades are carried into the next execution date.
- Exception removals become next-day targets in the trade layer; gross index behavior remains explicitly tested.

- [ ] **Step 1: Write failing tests**

  Add cases where a suspended/limit-locked stock cannot be sold on the discovery day, then is executed on the next tradable day; assert no same-day trade cost is charged.

- [ ] **Step 2: Run focused tests and confirm failure**

  Run `uv run pytest tests/test_trading.py tests/test_exception_rebalance.py`.
  Expected: failure because pending execution is not yet integrated.

- [ ] **Step 3: Implement pending target carry-forward**

  Store pending target trades in the portfolio state or daily manifest, merge them with the next day’s target, and only charge costs for executed portions. Preserve existing suspension streak handling and ensure a removed symbol is not silently reintroduced.

- [ ] **Step 4: Run focused tests**

  Run `uv run pytest tests/test_trading.py tests/test_exception_rebalance.py`.
  Expected: PASS.

### Task 5: Documentation and full verification

**Files:**
- Modify: `README.md`
- Modify: `docs/methodology.md`
- Modify: `docs/architecture.md`
- Test: `tests/`, `web/src/*.test.mjs`

- [ ] **Step 1: Document the two index layers**

  State that gross fields are the theoretical public index and net fields are opt-in execution diagnostics, list zero-default cost parameters, and explain delayed execution for non-tradable events.

- [ ] **Step 2: Run Python quality checks**

  Run `uv run ruff format --check .`, `uv run ruff check .`, `uv run ty check`, and `uv run pytest`.

- [ ] **Step 3: Run frontend checks**

  Run `cd web && npm test && npm run build`.

- [ ] **Step 4: Inspect the final diff and status**

  Run `git diff --check` and `git status --short`. Confirm only the approved files and generated test outputs changed.

- [ ] **Step 5: Commit documentation and verified integration**

  Run `git add README.md docs/methodology.md docs/architecture.md src/zoo_index tests web && git commit -m "feat: add opt-in tradable backtest layer"`.
