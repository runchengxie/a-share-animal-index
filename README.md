# 动物园指数

可能是全网第一个 A 股动物园指数：把 A 股简称里含动物词的股票收编成组合，按固定规则每日更新，并与沪深300对比。

说明：图像由每日脚本生成，初次运行后才会显示。

## 功能概览

* 支持严格动物园 / 扩展动物园双指数
* 使用规则词表 + 强制名单，结果可复现
* 每日生成净值、曲线图、徽章数据与网页

## 快速开始

1. 安装依赖（使用 uv）

```bash
uv sync
```

2. 配置 Tushare Token

```bash
export TUSHARE_TOKEN=你的token
```

备用 Token（可选）：再设一个 `TUSHARE_TOKEN_2`，并配 `TUSHARE_API_URL` 指向转发代理地址。主 Token 请求失败时，会自动回退到该备用 Token（官方口径不变，默认行为不受影响）。

```bash
export TUSHARE_TOKEN_2=转发代理给你的key
export TUSHARE_API_URL=https://fast.xiaodefa.cn
```

3. 运行每日更新

```bash
uv run zoo-index --date 20240102
```

未指定日期时会默认使用上海时区下最近一个完整交易日（若当天数据未就绪会回退到上一个交易日）。

4. 回填历史区间（可选）

```bash
uv run zoo-index --backfill 250
```

也可以直接 `uv run zoo-index --backfill`，默认回填最近 5 年交易日。
如需按年份回填，使用 `--backfill-years 5`；需要全量重算区间时加 `--backfill-mode all`。
回填会增量更新 `docs/nav.csv` 并刷新 `docs/` 产物，默认只写回填区间最后一天的快照。
如需生成每日持仓快照，可加 `--backfill-write-snapshots`。
如需禁用规则快照，可加 `--no-rules-snapshot`。
本地缓存默认启用（目录 `data/cache`），可用 `--no-cache` 禁用，`--force-refresh` 强制刷新。
如需切换基准，使用 `--benchmark / --benchmark-source / --benchmark-label`。
ETF 基准需要 `fund_daily / fund_adj` 权限（约 2000 积分）；权限不足可用 `--benchmark-source index --benchmark 000300.SH` 回退到价格口径。
切换基准或口径后建议用 `--backfill-mode all` 全量重算历史区间。

5. 仅重绘图表（不调用 Tushare，可选）

```bash
uv run zoo-chart
```

如果 `nav.csv` 或输出路径不在默认位置，可用：

```bash
uv run zoo-chart --nav docs/nav.csv --out docs/chart.png
```

如需调整图表中的基准名称，可加 `--benchmark-label`。

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

`rules.yml` 控制动物词、排除项、强制收编 / 剔除等规则。

* `strict_keywords`：明确动物词（严格动物园）
* `extended_keywords`：扩展词（扩展动物园，可能噪声更高）
* `exclude_patterns`：包含这些词的简称会被剔除
* `force_include / force_exclude`：仅支持 ts_code 强制处理

## 产物说明

本地运行默认输出到 `docs/`（可用 `--output-dir` 修改），主要内容如下：

* `docs/nav.csv`：净值与每日收益
* `docs/latest.json`：首页数据（最新 NAV 与当日涨跌）
* `docs/history.json`：完整净值序列
* `docs/constituents.json`：当前成分
* `docs/changes.json`：最近调仓与单字疑似误伤
* `docs/metadata.json`：基准与更新信息
* `docs/badges/*.json`：徽章专用 JSON
* `docs/chart.png`：净值对比曲线
* `data/constituents_YYYYMMDD.csv` 等：当日成分与持仓快照
* `data/rules_snapshot_*.yml`：回填时的规则快照（可用 `--no-rules-snapshot` 关闭）
* `data/cache/`：Tushare 原始数据缓存（默认不提交）

说明：旧版 `hs300_*` 字段已重命名为 `benchmark_*`，过渡期内仍兼容读取旧 nav.csv。

## 徽章展示（可选）

如果启用了 GitHub Pages（由 daily.yml 部署到 Pages），可以用 shields.io 读取徽章 JSON：

```text
https://img.shields.io/endpoint?url=https://<user>.github.io/<repo>/data/benchmark_nav.json
```

也可以在 Pages 页面里直接展示 `latest.json` 的数值。

## 方法备注

* 成分按每月首个交易日重算，简称使用 `namechange` 的 as-of 口径。
* 回填使用 `list_date / delist_date` 过滤存量股票，减少幸存者偏差。
* 指数收益使用复权因子（`adj_factor / fund_adj`）还原分红送转影响，基准默认使用沪深300 ETF（510300.SH）复权口径。
* 默认等权，遇到缺少行情的成分会自动剔除并重新归一化权重；成分变更以 constituents 为准。

## 开发与测试

安装开发依赖并运行测试：

```bash
uv sync
uv run pytest
```

质量门禁（ruff format / ruff check / ty / pytest / uv audit）由 `.github/workflows/ci.yml` 在每次推送与 PR 时执行。

## 网页前端

`web/` 是 TypeScript 前端（Vite + React + ECharts），只负责展示公开 JSON，不接触 Tushare Token，也不在浏览器中计算指数。

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

注意：cron 使用 UTC（示例为北京时间 16:10）。main 分支不再提交任何生成物，产物走 Pages 构建，降低提交噪音。

### Cloudflare Pages（替代）

仓库提供了 `web/wrangler.toml` 与 `.github/workflows/deploy-cloudflare.yml` 作为替代路径。Cloudflare 部署需要你的账号与 `CLOUDFLARE_API_TOKEN`，且普通 Worker 不一定位于中国网络节点，境内访问速度可能不及预期。该路径需你自备凭证，本仓库内不验证实际上线。

## 待办

已完成：

* 拆分 `run_daily.py`，抽出 `compute_day` 供每日与回填共用
* 参考数据的本地缓存增加 TTL，避免长期运行看不到新股 / 退市 / 更名
* 基准字段由 `hs300_*` 重命名为 `benchmark_*`（保留兼容读取）
* 移除每日提交 main 的产物，改为 Pages 构建产物
* 增加 CI 质量门禁（ruff / ty / pytest / uv audit）
* 新增 TypeScript 前端与 GitHub Pages 部署

进行中：

* Cloudflare 替代部署配置（已提供文件，待用户凭证验证）

## 收录规则

### 严格动物园

* 设计初衷：追求精准，减少误伤。只收录多字、明确的动物名称。
* 收录关键词（共 20 个）：熊猫、海豚、海鸥、白鹤、天鹅、仙鹤、金龙鱼、海马、海象、猛虎、雄鹰、飞鹰、蝴蝶、蜻蜓、斑马、蜘蛛、松鼠、猫头鹰、鹦鹉、蜜蜂
* 特点：关键词通常由 2 个或以上汉字组成。极少出现歧义（例如熊猫几乎只指代动物，不会像单字马那样容易匹配到马钢等非动物股）。

### 扩展动物园

* 设计初衷：追求热闹，覆盖面广。收录了大量单字、泛指的动物名称。
* 收录关键词（共 28 个）：龙、马、牛、鱼、鸟、鹰、虎、猫、狗、狼、蛇、蝎、豹、象、猴、猪、鹏、鹤、龟、熊、鹅、鸭、鹿、兔、蜂、蝶、豚、鲨
* 特点：包含大量单字，容易产生噪音（误伤）。扩展动物园会默认合并严格关键词，因此扩展必然包含严格；规则里只需维护各自词表即可。

### 通用规则

* 两套指数都遵循以下通用过滤：
  * 排除规则（exclude_patterns）：剔除如马钢、龙湖、龙光等已知非动物的干扰项。
  * 强制名单（force_include / force_exclude）：可在 `rules.yml` 中通过 ts_code 强制将某只股票同时加入或踢出两个动物园。
  * 排除 ST：默认排除 ST 股票。
  * 交易所：默认包含沪深，可选是否包含北交所。

简而言之，严格是精品小团，扩展是热闹大群（纳入规则更松散）。

## 免责声明

本项目仅为娱乐用途，不构成任何投资建议。
