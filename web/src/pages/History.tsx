import { useEffect, useState } from "react";
import { fetchHistory, fetchLatest, NavPoint, Latest } from "../api";
import ZooChart from "../components/ZooChart";

type State =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ok"; history: NavPoint[]; latest: Latest };

export default function History() {
  const [state, setState] = useState<State>({ status: "loading" });

  useEffect(() => {
    Promise.all([fetchHistory(), fetchLatest()])
      .then(([history, latest]) => setState({ status: "ok", history, latest }))
      .catch((err) => setState({ status: "error", message: String(err) }));
  }, []);

  if (state.status === "loading") return <p>数据加载中…</p>;
  if (state.status === "error") return <p className="muted">数据加载失败：{state.message}</p>;

  return (
    <div className="page-history">
      <h2>历史净值</h2>
      <ZooChart history={state.history} benchmarkLabel={state.latest.benchmark_label} />
      <section>
        <h3>数据明细</h3>
        <table className="history-table">
          <thead>
            <tr>
              <th>日期</th>
              <th>严格净值</th>
              <th>扩展净值</th>
              <th>{state.latest.benchmark_label}净值</th>
            </tr>
          </thead>
          <tbody>
            {state.history
              .slice()
              .reverse()
              .slice(0, 200)
              .map((p) => (
                <tr key={p.date}>
                  <td>{p.date}</td>
                  <td>{p.zoo_strict_nav.toFixed(4)}</td>
                  <td>{p.zoo_extended_nav.toFixed(4)}</td>
                  <td>{p.benchmark_nav.toFixed(4)}</td>
                </tr>
              ))}
          </tbody>
        </table>
        <p className="muted">仅展示最近 200 个交易日，完整序列见 history.json。</p>
      </section>
    </div>
  );
}
