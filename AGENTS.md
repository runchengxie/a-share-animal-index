# 维护须知

本文件供参与本仓库维护的协作者与自动化工具阅读，说明关键约束与约定。

## 目录职责

* `src/zoo_index/cli.py`：命令行入口，仅负责解析参数并产出 `RunConfig`。
* `src/zoo_index/runner.py`：编排逻辑，核心函数 `compute_day` 同时服务于每日与回填两条路径。
* `src/zoo_index/config.py`：加载 `rules.yml`，产出 `Rules`。
* `src/zoo_index/matcher.py`：按规则把股票分为严格 / 扩展 / 排除，处理强制名单。
* `src/zoo_index/index.py`：构建成分、计算等权收益。
* `src/zoo_index/outputs.py`：写出 nav.csv 与 5 个公开 JSON、图表、徽章。
* `src/zoo_index/data_sources/tushare.py`：Tushare 客户端与缓存；`TushareLike` 是其结构化接口协议。
* `rules.yml`：动物词表、排除项、强制名单（字段名保持英文，注释用中文）。
* `web/`：TypeScript 前端，只展示公开 JSON，不接触 Token，不在浏览器计算指数。
* `docs/`、`web/dist/`、`web/public/data/`、`.build/`：生成物，不入库。

## 标准开发命令

```bash
uv sync
uv run ruff format .
uv run ruff check .
uv run ty check
uv run pytest
```

前端检查：

```bash
cd web
npm ci
npm test
npm run build
```

每日计算：`uv run zoo-index --output-dir .build/data --backfill`
本地预览网页：`cd web && npm install && npm run dev`

## 产物清单

公开 JSON（前端消费）：`latest.json`、`history.json`、`constituents.json`、`changes.json`、`metadata.json`。
其余：`nav.csv`（净值源）、`chart.png`（分享用）、`badges/*.json`（徽章）。

## 基准字段约定

列名为 `benchmark_ret` / `benchmark_nav`，JSON 字段为 `benchmark_nav` / `benchmark_daily` / `benchmark_excess`。
旧版 `hs300_*` 仅保留在读取 `nav.csv` 时的兼容归一化，新写入一律用 `benchmark_*`。

## 规则不变量

* 扩展动物园默认包含严格关键词，规则文件只需各自维护词表。
* `force_include` / `force_exclude` 仅接受 ts_code。
* 成分按每月首个交易日重算，简称用 `namechange` 的 as-of 口径。
* 缺失行情的成分从当日等权中剔除并重新归一化（已知约定，不静默改口径）。

## 测试要求

单元测试禁止依赖真实 Tushare Token。使用合成 DataFrame、`tests/fixtures/` 与 `tests/test_runner.py` 内的 `FakeClient`（满足 `TushareLike` 协议）。
新增计算逻辑时，优先在 `compute_day` 上补测试，保证每日与回填路径被同一组测试覆盖。

前端视觉行为使用 Node 内建测试锁住关键页面结构、研究型视觉 token、响应式约束与图表基础语义。测试不能依赖真实网络请求。生产构建必须继续通过 TypeScript 严格检查与 Vite build。

## 缓存策略

* 股票列表与更名（`stock_basic` / `namechange`）按 TTL（默认 1 天）刷新，避免长期运行看不到新股 / 退市 / 更名。
* 按日期不可变的日行情、复权、指数、基金数据保持永久缓存。
* `--force-refresh` 强制全部刷新，`--no-cache` 关闭缓存。
* 可选备用 Token：环境变量 `TUSHARE_TOKEN_2` 配合 `TUSHARE_API_URL`（转发代理地址）。主 Token 请求失败时由 `TushareClient` 自动回退到备用 Token，官方口径不受影响。

## 中文文档约定

中文文档使用中文标点，不使用英文双引号、不使用 `**` 强调、不使用分号、不使用破折号。
术语统一：规则词表 / 排除项 / 强制名单、严格动物园 / 扩展动物园、误匹配。

## CI 门禁

`.github/workflows/ci.yml` 在推送与 PR 时执行 Python 质量门禁与前端质量门禁。Python 包括 ruff format 检查、ruff check、ty、pytest、uv audit。前端包括 `npm test` 与 `npm run build`。
`.github/workflows/daily.yml` 在交易日计算并部署到 GitHub Pages，main 分支不提交任何生成物。
