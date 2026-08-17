import { useEffect, useMemo, useState } from "react";
import { fetchConstituents, type Constituents as ConstituentsData } from "../api";
import ConstituentsTable from "../components/ConstituentsTable";

type State =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ok"; data: ConstituentsData };

export default function Constituents() {
  const [state, setState] = useState<State>({ status: "loading" });
  const [variant, setVariant] = useState<"strict" | "extended">("strict");
  const [query, setQuery] = useState("");

  useEffect(() => {
    fetchConstituents()
      .then((data) => setState({ status: "ok", data }))
      .catch((err) => setState({ status: "error", message: String(err) }));
  }, []);

  const items = useMemo(() => {
    if (state.status !== "ok") return [];
    const list = variant === "strict" ? state.data.strict : state.data.extended;
    if (!query.trim()) return list;
    const q = query.trim();
    return list.filter(
      (c) => c.name.includes(q) || c.ts_code.includes(q) || c.keyword.includes(q)
    );
  }, [state, variant, query]);

  if (state.status === "loading") return <p>数据加载中…</p>;
  if (state.status === "error") return <p className="muted">数据加载失败：{state.message}</p>;

  return (
    <div className="page-constituents">
      <h2>当前成分</h2>
      <p className="muted">成分日期：{state.data.date}</p>
      <div className="toolbar">
        <label>
          <input
            type="radio"
            checked={variant === "strict"}
            onChange={() => setVariant("strict")}
          />
          严格
        </label>
        <label>
          <input
            type="radio"
            checked={variant === "extended"}
            onChange={() => setVariant("extended")}
          />
          扩展
        </label>
        <input
          type="text"
          placeholder="筛选名称/代码/匹配词"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
      </div>
      <ConstituentsTable variant={variant} items={items} />
    </div>
  );
}
