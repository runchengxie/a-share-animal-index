# 动物园指数

把 A 股简称里含动物词的股票收编成组合，按固定规则每日更新，并与沪深300对比。可能是全网第一个 A 股动物园指数。

图像由每日脚本生成，初次运行后才会显示。

## 功能概览

- 支持严格动物园 / 扩展动物园双指数
- 使用规则词表加强制名单，结果可复现
- 每日生成净值、曲线图、徽章数据与网页

## 快速开始

1. 安装依赖（使用 uv）

```bash
uv sync
```

2. 配置 Tushare Token

```bash
export TUSHARE_TOKEN=你的token
```

备用 Token（可选）：再设一个 `TUSHARE_TOKEN_2`，并配 `TUSHARE_API_URL` 指向转发代理。主 Token 失败时自动回退到备用 Token。

```bash
export TUSHARE_TOKEN_2=转发代理给你的key
export TUSHARE_API_URL=https://fast.xiaodefa.cn
```

3. 运行每日更新

```bash
uv run zoo-index --date 20240102
```

未指定日期时，默认使用上海时区下最近一个完整交易日（当天数据未就绪会回退到上一交易日）。

4. 回填历史区间（可选）

```bash
uv run zoo-index --backfill
```

默认回填最近 5 年交易日。也可 `uv run zoo-index --backfill 250` 指定天数，或 `--backfill-years 5` 按年份回填。需要全量重算历史区间时加 `--backfill-mode all`。

回填会增量更新 `artifacts/data/nav.csv` 并刷新 `artifacts/` 产物，默认只写回填区间最后一天的快照。加 `--backfill-write-snapshots` 可生成每日持仓快照，加 `--no-rules-snapshot` 可关闭规则快照。

本地缓存默认启用（目录 `data/cache`），可用 `--no-cache` 禁用，`--force-refresh` 强制刷新。

如需切换基准，使用 `--benchmark / --benchmark-source / --benchmark-label`。ETF 基准需要 `fund_daily / fund_adj` 权限（约 2000 积分）；权限不足可用 `--benchmark-source index --benchmark 000300.SH` 回退到价格口径。切换基准或口径后建议用 `--backfill-mode all` 全量重算。

5. 仅重绘图表（不调用 Tushare，可选）

```bash
uv run zoo-chart
```

若 `nav.csv` 或输出路径不在默认位置：

```bash
uv run zoo-chart --nav artifacts/data/nav.csv --out artifacts/data/chart.png
```

## Makefile 快捷命令（可选）

```bash
make daily
make backfill
make chart
make test
make lint
```

Makefile 默认调用 `uv run zoo-index` / `uv run zoo-chart`。

## 规则配置

`rules.yml` 控制动物词、排除项、强制收编与剔除等规则。具体词表与设计说明见 [docs/methodology.md](docs/methodology.md)。

- `strict_keywords`：明确动物词（严格动物园）
- `extended_keywords`：扩展词（扩展动物园，可能噪声更高）
- `exclude_patterns`：包含这些词的简称会被剔除
- `force_include / force_exclude`：仅支持 ts_code 强制处理

## 产物说明

本地运行默认输出到 `artifacts/`，分两类目录（可用 `--output-dir` 修改）：

- `artifacts/data/`：对外公开的数据与图表，供网页读取
  - `nav.csv`：净值与每日收益
  - `latest.json`：首页数据（最新 NAV 与当日涨跌）
  - `history.json`：完整净值序列
  - `constituents.json`：当前成分
  - `changes.json`：最近调仓与单字疑似误伤
  - `metadata.json`：基准与更新信息
  - `badges/*.json`：徽章专用 JSON
  - `chart.png`：净值对比曲线
- `artifacts/manifests/`：审计与快照，不对外
  - `constituents_YYYYMMDD.csv`、`holdings_YYYYMMDD.csv`、`changes_YYYYMMDD.json`：每日快照
  - `rules_snapshot_*.yml`：回填时的规则快照（可用 `--no-rules-snapshot` 关闭）

`data/cache/` 是 Tushare 原始数据缓存（默认不提交）。

说明：旧版 `hs300_*` 字段已重命名为 `benchmark_*`，过渡期内仍兼容读取旧 nav.csv。

更完整的产物结构与数据流见 [docs/architecture.md](docs/architecture.md)。

## 网页前端

`web/` 是 TypeScript 前端（Vite + React + ECharts），只负责展示公开 JSON。Tushare Token 只在服务端使用，指数也在服务端计算。

本地预览：

```bash
cd web
npm install
npm run dev
```

## 部署

### GitHub Pages（默认）

仓库已包含 `.github/workflows/daily.yml`，在交易日收盘后自动完成：

1. 用 uv 计算指数，写出 `web/public/data/*.json`
2. 用 npm 构建网页到 `web/dist`
3. 通过 `actions/deploy-pages` 部署到 GitHub Pages

你需要做两件事：

1. 在仓库 Secrets 里添加 `TUSHARE_TOKEN`
2. 在仓库 Settings 的 Pages 设置里，把来源改为 GitHub Actions

注意：cron 使用 UTC（示例为北京时间 16:10）。main 分支不再提交任何生成物，产物走 Pages 构建，降低提交噪音。部署细节见 [docs/architecture.md](docs/architecture.md)。

### Cloudflare Pages（替代）

仓库提供 `web/wrangler.toml` 与 `.github/workflows/deploy-cloudflare.yml` 作为替代路径。Cloudflare 部署需要你的账号与 `CLOUDFLARE_API_TOKEN`，且普通 Worker 不一定位于中国网络节点，境内访问速度可能不及预期。该路径需你自备凭证，本仓库内不验证实际上线。

## 开发与测试

```bash
uv sync
uv run pytest
```

质量门禁（ruff format / ruff check / ty / pytest / uv audit）由 `.github/workflows/ci.yml` 在每次推送与 PR 时执行。

## 待办

已完成：

- 拆分 `run_daily.py`，抽出 `compute_day` 供每日与回填共用
- 参考数据的本地缓存增加 TTL，避免长期运行看不到新股、退市、更名
- 基准字段由 `hs300_*` 重命名为 `benchmark_*`（保留兼容读取）
- 移除每日提交 main 的产物，改为 Pages 构建产物
- 增加 CI 质量门禁（ruff / ty / pytest / uv audit）
- 新增 TypeScript 前端与 GitHub Pages 部署
- 修正收益公式与停牌处理（P0），详见 [docs/methodology.md](docs/methodology.md)
- 输出目录由 `docs/` 调整为 `artifacts/`，内含 `data/` 与 `manifests/`

进行中：

- Cloudflare 替代部署配置（已提供文件，待用户凭证验证）

## 免责声明

本项目仅为娱乐用途，不构成任何投资建议。
