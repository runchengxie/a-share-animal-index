# 架构与产物

技术细节收录于此，README 仅保留上手所需的最低信息。

## 目录结构

本地运行默认输出到 `artifacts/`，分两类目录（可用 `--output-dir` 修改）：

- `artifacts/data/`：对外公开的数据与图表，供网页读取
  - `nav.csv`：净值与每日收益（也是下次运行的输入，作为历史净值源）
  - `latest.json`：首页数据（最新 NAV 与当日涨跌）
  - `history.json`：完整净值序列
  - `constituents.json`：当前成分
  - `changes.json`：最近调仓与单字疑似误伤
  - `metadata.json`：基准与更新信息
  - `badges/*.json`：徽章专用 JSON（shields.io 格式）
  - `chart.png`：净值对比曲线
- `artifacts/manifests/`：审计与快照，不对外
  - `constituents_YYYYMMDD.csv`、`holdings_YYYYMMDD.csv`、`changes_YYYYMMDD.json`：每日快照
  - `rules_snapshot_*.yml`：回填时的规则快照（可用 `--no-rules-snapshot` 关闭）

`data/cache/` 是 Tushare 原始数据缓存（默认不提交），目录结构按接口与交易日分文件。

说明：旧版 `hs300_*` 字段已重命名为 `benchmark_*`，过渡期内仍兼容读取旧 nav.csv。

## 数据流

1. `uv run python -m zoo_index` 读取 `rules.yml` 与 `data/cache/` 中的原始行情，计算指数。
2. 结果写入 `artifacts/data/` 与 `artifacts/manifests/`。
3. `daily.yml` 在 CI 中把 `artifacts/data/*.json` 复制到 `web/public/data/`，再构建网页。
4. 网页（Vite + React + ECharts）只读取 `web/public/data/` 下的公开 JSON，不接触 Tushare Token，也不在浏览器中计算指数。
5. 构建产物 `web/dist` 通过 `actions/deploy-pages` 部署到 GitHub Pages。

## 计算模型

单日计算由 `runner.compute_day` 完成，核心是带状态的 `PortfolioState`。

- `PortfolioState` 记录上一交易日的双变体组合（strict / extended），含各成分固定权重、成分表与停牌连续天数。
- 月度首个交易日触发再平衡：用上一篮子计算当日收益（去前视），新篮子等权后于次日生效。
- 月内沿用上一篮子与固定权重，不再每日重算。
- 异常再平衡：持有成分出现退市、新 ST 或连续停牌超阈值时，剔除触发成分并重新等权，新权重当日生效。
- 规则时点化：`_get_constituents_for_rebalance` 在每次再平衡时按 `rules_path` 调用 `load_rules_asof`，使用当时生效的规则版本。
- 状态重建：`run_daily` 从上一交易日的 `holdings_YYYYMMDD.csv` 读取权重与停牌天数重建 `PortfolioState`；`run_backfill` 在回填循环中按日向后传递状态，保证历史可复现且无前视。

## 徽章

`artifacts/data/badges/*.json` 是 shields.io 格式的徽章数据。若要在自有站点展示，把 `badges/` 一并发布，再用：

`https://img.shields.io/endpoint?url=https://<你的站点>/badges/benchmark_nav.json`

## 数据源与 Token

指数计算依赖 Tushare 行情，由 `src/zoo_index/data_sources/tushare.py` 的 `TushareClient` 封装。

环境变量：

- `TUSHARE_TOKEN`：主 Token（必填）。
- `TUSHARE_TOKEN_2`：备用 Token（可选），走转发代理。
- `TUSHARE_API_URL`：转发代理地址（可选），仅作用于备用 Token。

回退逻辑：`TushareClient` 在主 Token 请求失败时，自动用备用 Token（经 `TUSHARE_API_URL`）重试一次；备用端不再二次回退，避免死循环。回退只改变数据获取路径，指数计算口径不受影响。两者皆未设置时，只走官方接口。

```bash
export TUSHARE_TOKEN=你的token
export TUSHARE_TOKEN_2=转发代理给你的key
export TUSHARE_API_URL=https://<转发代理地址>
```

## 使用细节

`uv run zoo-index` 常用参数：

- 日期：`--date YYYYMMDD` 指定交易日；缺省为上海时区最近完整交易日。
- 回填：`--backfill`（默认最近 5 年）、`--backfill N`（按天数）、`--backfill-years Y`（按年）。
- 重算模式：`--backfill-mode all` 全量重算历史区间（切换基准或口径后建议用）。
- 快照：`--backfill-write-snapshots` 生成每日持仓快照；`--no-rules-snapshot` 关闭规则快照。
- 缓存：默认启用 `data/cache/`；`--no-cache` 禁用，`--force-refresh` 强制刷新。
- 基准：`--benchmark` / `--benchmark-source` / `--benchmark-label` 切换基准。ETF 基准需 `fund_daily` / `fund_adj` 权限（约 2000 积分）；权限不足可回退到 `--benchmark-source index --benchmark 000300.SH` 的价格口径。

仅重绘图表（不调用 Tushare）：

```bash
uv run zoo-chart --nav artifacts/data/nav.csv --out artifacts/data/chart.png
```

Makefile 快捷命令：`make daily` / `make backfill` / `make chart` / `make test` / `make lint`。

## 部署

### GitHub Pages（默认）

`.github/workflows/daily.yml` 在交易日收盘后自动运行（cron 使用 UTC，示例为北京时间 16:10）：

1. 用 uv 计算指数，写出 `web/public/data/*.json`
2. 用 npm 构建网页到 `web/dist`
3. 通过 `actions/deploy-pages` 部署到 GitHub Pages

使用前需两步：

1. 在仓库 Secrets 里添加 `TUSHARE_TOKEN`
2. 在仓库 Settings 的 Pages 设置里，把来源改为 GitHub Actions

main 分支不再提交任何生成物，产物走 Pages 构建，降低提交噪音。

### Cloudflare Pages（替代）

仓库提供 `web/wrangler.toml` 与 `.github/workflows/deploy-cloudflare.yml` 作为替代路径。Cloudflare 部署需要你的账号与 `CLOUDFLARE_API_TOKEN`，且普通 Worker 不一定位于中国网络节点，境内访问速度可能不及预期。该路径需你自备凭证，本仓库内不验证实际上线。
