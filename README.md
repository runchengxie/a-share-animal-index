# A 股动物园与植物园

[GitHub Pages 在线预览](https://runchengxie.github.io/a-share-zoo-garden/) · [Cloudflare Pages 在线预览](https://a-share-zoo-garden.pages.dev/)

把 A 股简称里含动物词或植物词的股票收编成主题组合，按固定规则每日更新，并与沪深300对比。动物园是成熟主线，植物园目前先以保守词表作为候选实验。

> 本项目仅供娱乐与研究，不构成任何投资建议。

## 它能做什么

- 严格动物园 / 扩展动物园双指数，与沪深300对照
- 第一版植物园规则：只启用明确的多字植物词，暂不纳入容易误匹配的单字词
- 规则词表 + 强制名单，结果可复现
- 每日生成净值、曲线图、徽章与网页
- 网页按研究报告式信息层级展示指数快照、净值、调仓、成分与方法

## 快速开始

### 1. 安装依赖

需要 [uv](https://docs.astral.sh/uv/)。

```bash
uv sync
```

### 2. 配置 Tushare Token

```bash
export TUSHARE_TOKEN=你的token
```

主 Token 偶发限流或失败时，可再设一个备用 Token 走转发代理，由程序自动回退（机制见 [docs/architecture.md](docs/architecture.md#数据源与-token)）：

```bash
export TUSHARE_TOKEN_2=转发代理给你的key
export TUSHARE_API_URL=https://<转发代理地址>
```

### 3. 跑一次

```bash
uv run zoo-index
```

不指定日期时，默认使用上海时区下最近一个完整交易日（当天数据未就绪会回退到上一交易日）。图像与网页数据由脚本生成，初次运行后才会显示。

### 4. 看结果（本地预览网页）

```bash
cd web
npm install
npm run dev
```

## 常用命令

| 命令 | 作用 |
| --- | --- |
| `uv run zoo-index` | 更新最近一个交易日 |
| `uv run zoo-index --backfill` | 回填最近 5 年历史 |
| `uv run zoo-chart` | 仅重绘图表，不调用 Tushare |
| `make daily` / `make backfill` / `make chart` / `make test` | Makefile 快捷命令 |

更多命令行参数（回填天数、缓存、基准切换、快照等）见 [docs/architecture.md](docs/architecture.md#使用细节)。

植物园当前使用独立的 [plant_rules.yml](plant_rules.yml) 词表。它与动物园共享匹配、上市状态、ST、流动性和复权收益计算逻辑。当前主输出结构仍按严格、扩展两种变体组织，植物园先作为保守候选层推进，后续再将主题变体抽象成可配置的多主题输出。

## 规则、产物与架构

- 收录规则与指数方法：见 [docs/methodology.md](docs/methodology.md)
- 产物结构、数据流、部署与 Token 机制：见 [docs/architecture.md](docs/architecture.md)

## 部署

项目同时部署到 GitHub Pages 和 Cloudflare Pages。`.github/workflows/daily.yml` 在每个交易日收盘后计算数据并部署 GitHub Pages。`.github/workflows/deploy-cloudflare.yml` 在 `main` 更新后使用仓库中的 `published/data` 构建并部署 Cloudflare Pages。GitHub Actions 需要配置 `TUSHARE_TOKEN`、`CLOUDFLARE_API_TOKEN` 与 `CLOUDFLARE_ACCOUNT_ID`。细节见 [docs/architecture.md](docs/architecture.md#部署)。

## 开发与测试

Python 计算与数据逻辑：

```bash
uv run pytest
```

前端视觉契约与生产构建：

```bash
cd web
npm ci
npm test
npm run build
```

质量门禁由 `.github/workflows/ci.yml` 在每次推送与 PR 时执行，包括 ruff、ty、pytest、uv audit，以及前端 Node 契约测试与 Vite production build。
