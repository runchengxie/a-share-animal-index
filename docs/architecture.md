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

## 徽章

`artifacts/data/badges/*.json` 是 shields.io 格式的徽章数据。若要在自有站点展示，把 `badges/` 一并发布，再用：

`https://img.shields.io/endpoint?url=https://<你的站点>/badges/benchmark_nav.json`

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
