# 动物园指数

可能是全网第一个A股动物园指数：把A股简称里含动物词的股票收编成组合，按固定规则每日更新，并与沪深300对比。

![动物园指数曲线](docs/chart.png)

说明：图像由每日脚本生成，初次运行后才会显示。

## 功能概览

- 支持“严格动物园 / 扩展动物园”双指数
- 使用规则词表 + 黑白名单，结果可复现
- 每日生成净值、曲线图、徽章数据与静态页面

## 快速开始

1. 安装依赖

```bash
pip install -r requirements.txt
```

2. 配置 Tushare Token

```bash
export TUSHARE_TOKEN=你的token
```

3. 运行每日更新

```bash
python src/run_daily.py --date 20240102
```

未指定日期时会默认使用上海时区下最近一个交易日。

4. 回填最近 250 个交易日（可选）

```bash
python src/run_daily.py --backfill 250
```

回填会增量更新 `docs/nav.csv` 并刷新 `docs/` 产物，默认只写回填区间最后一天的快照。
如需生成每日持仓快照，可加 `--backfill-write-snapshots`。
默认启用本地缓存（`data/cache`），可用 `--no-cache` 禁用，`--force-refresh` 强制刷新。

## 规则配置

`rules.yml` 控制动物词、排除项、强制收编/剔除等规则。

- `strict_keywords`：明确动物词（严格动物园）
- `extended_keywords`：扩展词（扩展动物园，可能噪声更高）
- `exclude_patterns`：包含这些词的简称会被剔除
- `force_include / force_exclude`：按股票代码或完整简称强制处理

严格动物园是更保守的版本，扩展动物园更热闹，但更可能出现误伤。

## 产物说明

- `docs/nav.csv`：净值与每日收益
- `data/constituents_YYYYMMDD.csv`：当日成分（不含行情过滤）
- `data/holdings_YYYYMMDD.csv`：当日成分与权重
- `data/changes_YYYYMMDD.json`：成分变化摘要（基于 constituents，含单字词疑似误伤清单）
- `data/cache/`：Tushare 原始数据缓存（默认不提交）
- `docs/chart.png`：净值对比曲线
- `docs/latest.json`：首页数据
- `docs/badges/*.json`：徽章专用 JSON
- `docs/index.html`：静态页面（可用于 GitHub Pages）

## 徽章展示（可选）

如果你启用了 GitHub Pages（指向 `docs/`），可以用 shields.io 读取徽章 JSON：  

```text
https://img.shields.io/endpoint?url=https://<user>.github.io/<repo>/badges/zoo_strict_nav.json
```

也可以在 Pages 页面里直接展示 `docs/latest.json` 的数值。

## 方法备注

- 成分按每月首个交易日重算，简称使用 `namechange` 的 as-of 口径。
- 回填使用 `list_date` / `delist_date` 过滤存量股票，减少幸存者偏差。
- 当前净值为价格指数口径，未做分红送转调整。
- 默认等权，遇到缺少行情的成分会自动剔除并重新归一化权重；成分变更以 constituents 为准。

## 开发与测试

安装开发依赖并运行测试：

```bash
pip install -e ".[dev]"
pytest
```

## GitHub Actions（可选）

可以在 GitHub Actions 中设置每日跑一次，更新 `docs/` 并提交回仓库，用于 Pages 展示。

仓库内已包含 `/.github/workflows/daily.yml`，你只需要：

1. 在仓库 Secrets 里添加 `TUSHARE_TOKEN`  
2. 确保 Pages 指向 `docs/` 目录  
3. 了解 cron 使用 UTC（示例为北京时间 16:10）

## 免责声明

本项目仅为娱乐用途，不构成任何投资建议。
