import { useEffect, useMemo, useState } from "react";
import { fetchConstituents, type Constituents as ConstituentsData } from "../api";
import ConstituentsTable from "../components/ConstituentsTable";

type State =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ok"; data: ConstituentsData };

export default function Constituents() {
  const [state, setState] = useState<State>({ status: "loading" });
  const [query, setQuery] = useState("");

  useEffect(() => {
    fetchConstituents()
      .then((data) => setState({ status: "ok", data }))
      .catch((err) => setState({ status: "error", message: String(err) }));
  }, []);

  const filterItems = (items: ConstituentsData["strict"]) => {
    if (!query.trim()) return items;
    const q = query.trim();
    return items.filter(
      (c) => c.name.includes(q) || c.ts_code.includes(q) || c.keyword.includes(q)
    );
  };

  const filtered = useMemo(() => {
    if (state.status !== "ok") return { strict: [], extended: [] };
    return { strict: filterItems(state.data.strict), extended: filterItems(state.data.extended) };
  }, [state, query]);

  if (state.status === "loading") return <p>数据加载中…</p>;
  if (state.status === "error") return <p className="muted">数据加载失败：{state.message}</p>;

  return (
    <div className="page-constituents">
      <h2>当前成分</h2>
      <p className="muted">成分日期：{state.data.date}</p>
      <div className="toolbar">
        <span className="toolbar-hint">展开下方分组查看成分</span>
        <input
          type="text"
          placeholder="筛选名称/代码/匹配词"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
      </div>
      <ConstituentsTable variant="strict" items={filtered.strict} />
      <ConstituentsTable variant="extended" items={filtered.extended} />
    </div>
  );
}
