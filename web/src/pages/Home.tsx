import { useEffect, useState } from "react";
import { fetchLatest, fetchHistory, fetchChanges, Latest, NavPoint, Changes } from "../api";
import NavCards from "../components/NavCards";
import ZooChart from "../components/ZooChart";
import ChangesList from "../components/ChangesList";

type State =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ok"; latest: Latest; history: NavPoint[]; changes: Changes };

export default function Home() {
  const [state, setState] = useState<State>({ status: "loading" });

  useEffect(() => {
    Promise.all([fetchLatest(), fetchHistory(), fetchChanges()])
      .then(([latest, history, changes]) =>
        setState({ status: "ok", latest, history, changes })
      )
      .catch((err) => setState({ status: "error", message: String(err) }));
  }, []);

  if (state.status === "loading") return <p>数据加载中…</p>;
  if (state.status === "error") return <p className="muted">数据加载失败：{state.message}</p>;

  return (
    <div className="page-home">
      <NavCards latest={state.latest} />
      <section>
        <h2>净值走势</h2>
        <ZooChart history={state.history} benchmarkLabel={state.latest.benchmark_label} />
      </section>
      <section>
        <h2>最近调仓</h2>
        <ChangesList changes={state.changes} />
      </section>
    </div>
  );
}
