// 构建期把已追踪的 published/data/*.json 取到 web/public/data（单一数据源）。
// 这样 daily.yml 不再需要 cp 步骤：网页数据在构建时由本脚本直接从 published/data 取得。
// 网页只消费 *.json（nav.csv / 图片等不需要进网页），与旧 daily.yml 的 cp *.json 口径一致。
import { existsSync, mkdirSync, readdirSync, copyFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

const SRC = resolve(__dirname, "../../published/data");
const DEST = resolve(__dirname, "../public/data");

if (!existsSync(SRC)) {
  console.error(
    `[copy-published-data] 未找到数据源目录：${SRC}\n` +
      `请先在本机执行 uv run python -m zoo_index --output-dir published --backfill 并 push published/，再运行构建。`
  );
  process.exit(1);
}

mkdirSync(DEST, { recursive: true });

const files = readdirSync(SRC).filter((f) => f.endsWith(".json"));
if (files.length === 0) {
  console.warn(`[copy-published-data] ${SRC} 下没有 *.json，网页可能取不到数据`);
}

for (const f of files) {
  copyFileSync(join(SRC, f), join(DEST, f));
}

const PLANT_SRC = resolve(__dirname, "../../published/plant/data");
const PLANT_DEST = resolve(__dirname, "../public/data/plant");
if (existsSync(PLANT_SRC)) {
  mkdirSync(PLANT_DEST, { recursive: true });
  for (const f of readdirSync(PLANT_SRC).filter((name) => name.endsWith(".json"))) {
    copyFileSync(join(PLANT_SRC, f), join(PLANT_DEST, f));
  }
  console.log(`[copy-published-data] 已拷贝植物园数据到 ${PLANT_DEST}`);
}

console.log(`[copy-published-data] 已拷贝 ${files.length} 个 json 到 ${DEST}`);
