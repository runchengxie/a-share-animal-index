import { useEffect, useMemo, useState } from "react";
import { fetchThemeConstituents, type Constituents as ConstituentsData, type IndexTheme } from "../api";
import ConstituentsTable from "../components/ConstituentsTable";

type State =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ok"; data: Record<IndexTheme, ConstituentsData> };

const THEME_LABELS: Record<IndexTheme, string> = { animal: "动物园", plant: "植物园" };

export default function Constituents() {
  const [state, setState] = useState<State>({ status: "loading" });
  const [query, setQuery] = useState("");

  useEffect(() => {
    Promise.all([fetchThemeConstituents("animal"), fetchThemeConstituents("plant")])
      .then(([animal, plant]) => setState({ status: "ok", data: { animal, plant } }))
      .catch((err) => setState({ status: "error", message: String(err) }));
  }, []);

  const filtered = useMemo(() => {
    if (state.status !== "ok") return { animal: { strict: [], extended: [] }, plant: { strict: [], extended: [] } };
    const filterItems = (items: ConstituentsData["strict"]) => {
      if (!query.trim()) return items;
      const q = query.trim();
      return items.filter((c) => c.name.includes(q) || c.ts_code.includes(q) || c.keyword.includes(q));
    };
    return {
      animal: { strict: filterItems(state.data.animal.strict), extended: filterItems(state.data.animal.extended) },
      plant: { strict: filterItems(state.data.plant.strict), extended: filterItems(state.data.plant.extended) },
    };
  }, [state, query]);

  if (state.status === "loading") return <p>数据加载中…</p>;
  if (state.status === "error") return <p className="muted">数据加载失败：{state.message}</p>;

  return (
    <div className="page-constituents">
      <h2>当前成分</h2>
      <p className="muted">动物园和植物园分别展示严格、扩展两组结果。展开分组查看成分明细。</p>
      <div className="toolbar">
        <span className="toolbar-hint">搜索会同时筛选两个主题</span>
        <input type="text" placeholder="筛选名称、代码或匹配词" value={query} onChange={(e) => setQuery(e.target.value)} />
      </div>
      {(["animal", "plant"] as IndexTheme[]).map((theme) => (
        <section className="theme-constituents" key={theme} aria-labelledby={`${theme}-constituents-heading`}>
          <h3 id={`${theme}-constituents-heading`}>{THEME_LABELS[theme]}</h3>
          <ConstituentsTable variant="strict" items={filtered[theme].strict} themeLabel={THEME_LABELS[theme]} />
          <ConstituentsTable variant="extended" items={filtered[theme].extended} themeLabel={THEME_LABELS[theme]} />
        </section>
      ))}
    </div>
  );
}
