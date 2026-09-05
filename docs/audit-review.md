# 名称审计审核流程

## 运行

先运行不调用模型的确定性审计：

```bash
uv run zoo-audit --date 20260904 --mode all
```

需要语义复核时，通过环境变量提供 API key，再运行：

```bash
uv run zoo-audit --date 20260904 --mode all --llm
```

报告输出到 `artifacts/audit/`，包括 JSON 和 Markdown 两种格式。报告中的输入哈希、规则哈希、provider、模型和 prompt 版本用于复现和比较。

## 审核顺序

先看 `Potential missing`，确认未命中简称是否包含明确的现实生物名称。再看 `Potential false positives`，重点复核 `龙`、`马`、`象`、`柏`、`柳`、`桐`、`燕` 等高歧义词。

模型之间意见一致只能提高审核优先级，不能直接批准。`confidence` 只用于排序，不代表经过校准的概率。

批准结果应写回 typed rule 或带股票代码、层级和原因的 override，并追加 `rules_history.yml` 生效日期。不要把 audit JSON 直接复制成正式规则。
