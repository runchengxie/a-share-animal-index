import { useEffect, useState } from "react";
import { fetchChanges, type Changes as ChangesData } from "../api";
import ChangesList from "../components/ChangesList";

type State =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ok"; changes: ChangesData };

export default function Changes() {
  const [state, setState] = useState<State>({ status: "loading" });

  useEffect(() => {
    fetchChanges()
      .then((changes) => setState({ status: "ok", changes }))
      .catch((err) => setState({ status: "error", message: String(err) }));
  }, []);

  if (state.status === "loading") return <p>数据加载中…</p>;
  if (state.status === "error") return <p className="muted">数据加载失败：{state.message}</p>;

  return (
    <div className="page-changes">
      <h2>调仓记录</h2>
      <ChangesList changes={state.changes} />
    </div>
  );
}
