import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  Changes,
  Constituents,
  Latest,
  NavPoint,
  fetchThemeChanges,
  fetchThemeConstituents,
  fetchThemeHistory,
  fetchThemeLatest,
  IndexTheme,
} from "../api";
import ChangesList from "../components/ChangesList";
import NavCards from "../components/NavCards";
import ZooChart from "../components/ZooChart";

type State =
  | { status: "loading" }
  | { status: "error"; message: string }
  | {
      status: "ok";
      latest: Latest;
      history: NavPoint[];
      changes: Changes;
      constituents: Constituents;
    };

export default function Home({ theme = "animal" }: { theme?: IndexTheme }) {
  const themeLabel = theme === "plant" ? "植物园" : "动物园";
  const [state, setState] = useState<State>({ status: "loading" });

  useEffect(() => {
    Promise.all([
      fetchThemeLatest(theme),
      fetchThemeHistory(theme),
      fetchThemeChanges(theme),
      fetchThemeConstituents(theme),
    ])
      .then(([latest, history, changes, constituents]) =>
        setState({ status: "ok", latest, history, changes, constituents })
      )
      .catch((error) => setState({ status: "error", message: String(error) }));
  }, [theme]);

  if (state.status === "loading") {
    return <p className="page-status">正在读取{themeLabel}今日档案…</p>;
  }
  if (state.status === "error") {
    return <p className="page-status muted">数据加载失败：{state.message}</p>;
  }

  return (
    <div className="page-home">
      <section className="home-hero">
        <div className="section-kicker">研究问题</div>
        <h2>{theme === "plant" ? "如果只买名字里带植物的上市公司，会发生什么？" : "如果只买名字里带动物的上市公司，会发生什么？"}</h2>
        <p>
          这里把一个缺少传统经济学依据的分类，按固定规则编成两个 A 股指数，持续记录它们相对
          {state.latest.benchmark_label} 的表现。曲线只能说明历史现象，不能直接说明规律。
        </p>
        <div className="hero-meta" aria-label="指数研究元数据">
          <span>截至 {state.latest.date}</span>
          <span>收盘数据</span>
          <span>规则构建</span>
          <span>仅供研究</span>
        </div>
      </section>

      <section className="index-snapshot" aria-labelledby="snapshot-heading">
        <div className="section-heading compact-heading">
          <div>
            <div className="section-kicker">指数快照</div>
            <h2 id="snapshot-heading">今日指数快照</h2>
          </div>
          <span className="section-asof">{state.latest.date}</span>
        </div>
        <NavCards
          latest={state.latest}
          strictCount={state.constituents.strict.length}
          extendedCount={state.constituents.extended.length}
          themeLabel={themeLabel}
        />
      </section>

      <section className="research-section" aria-labelledby="performance-heading">
        <div className="section-heading">
          <div>
            <div className="section-kicker">表现 / 标准化净值</div>
            <h2 id="performance-heading">净值与基准</h2>
            <p className="section-deck">
              同一张图里比较严格{themeLabel}、扩展{themeLabel}和基准。缩放区间只改变视图，不改变指数口径。
            </p>
          </div>
        </div>
        <ZooChart
          history={state.history}
          benchmarkLabel={state.latest.benchmark_label}
          themeLabel={themeLabel}
        />
      </section>

      <div className="home-research-grid">
        <section className="research-section zoo-today" aria-labelledby="changes-heading">
          <div className="section-heading compact-heading">
            <div>
              <div className="section-kicker">今日{themeLabel}</div>
              <h2 id="changes-heading">最近调仓</h2>
            </div>
            <Link className="text-link" to="/changes">
              查看完整记录
            </Link>
          </div>
          <ChangesList changes={state.changes} themeLabel={themeLabel} />
        </section>

        <aside className="research-note" aria-labelledby="why-heading">
          <div className="section-kicker">项目缘起</div>
          <h2 id="why-heading">一个故意有点荒谬的量化实验</h2>
          <p>
            如果一个缺少经济机制的分类也出现了漂亮曲线，应先检查数据挖掘、选择偏差和回测过拟合，再讨论背后的解释。
          </p>
          <p>
            这个项目把规则、成分、历史和调仓全部公开，让梗停留在标题，让证据留在页面里。
          </p>
          <div className="research-note-links">
            <Link to="/methodology">阅读构建方法</Link>
            <Link to="/constituents">查看当前成分</Link>
            <Link to="/history">检查历史数据</Link>
          </div>
        </aside>
      </div>
    </div>
  );
}
