import { useEffect, useState } from "react";
import { fetchThemeChanges, type Changes as ChangesData, type IndexTheme } from "../api";
import ChangesList from "../components/ChangesList";

const THEME_LABELS: Record<IndexTheme, string> = { animal: "动物园", plant: "植物园" };

type State =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ok"; changes: Record<IndexTheme, ChangesData> };

export default function Changes() {
  const [state, setState] = useState<State>({ status: "loading" });

  useEffect(() => {
    Promise.all([fetchThemeChanges("animal"), fetchThemeChanges("plant")])
      .then(([animal, plant]) => setState({ status: "ok", changes: { animal, plant } }))
      .catch((err) => setState({ status: "error", message: String(err) }));
  }, []);

  if (state.status === "loading") return <p>数据加载中…</p>;
  if (state.status === "error") return <p className="muted">数据加载失败：{state.message}</p>;

  return (
    <div className="page-changes">
      <h2>调仓记录</h2>
      <p className="muted">动物园与植物园分别展示最近一次成分变化及疑似误匹配。</p>
      {(["animal", "plant"] as IndexTheme[]).map((theme) => (
        <section className="theme-record" key={theme}>
          <h3>{THEME_LABELS[theme]}</h3>
          <ChangesList changes={state.changes[theme]} themeLabel={THEME_LABELS[theme]} />
        </section>
      ))}
    </div>
  );
}
