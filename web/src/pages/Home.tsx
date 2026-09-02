import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  Changes,
  Constituents,
  Latest,
  NavPoint,
  fetchChanges,
  fetchConstituents,
  fetchHistory,
  fetchLatest,
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

export default function Home() {
  const [state, setState] = useState<State>({ status: "loading" });

  useEffect(() => {
    Promise.all([
      fetchLatest(),
      fetchHistory(),
      fetchChanges(),
      fetchConstituents(),
    ])
      .then(([latest, history, changes, constituents]) =>
        setState({ status: "ok", latest, history, changes, constituents })
      )
      .catch((error) => setState({ status: "error", message: String(error) }));
  }, []);

  if (state.status === "loading") {
    return <p className="page-status">正在读取动物园今日档案…</p>;
  }
  if (state.status === "error") {
    return <p className="page-status muted">数据加载失败：{state.message}</p>;
  }

  return (
    <div className="page-home">
      <section className="home-hero">
        <div className="section-kicker">QUESTION / 研究问题</div>
        <h2>如果只买名字里带动物的上市公司，会发生什么？</h2>
        <p>
          这里把一个没有传统经济学理由的分类，按固定规则编成两个 A 股指数，持续记录它们相对
          {state.latest.benchmark_label} 的表现。好看的曲线先当现象，不急着当规律。
        </p>
        <div className="hero-meta" aria-label="指数研究元数据">
          <span>AS OF {state.latest.date}</span>
          <span>DAILY CLOSE</span>
          <span>RULE-BASED</span>
          <span>RESEARCH ONLY</span>
        </div>
      </section>

      <section className="index-snapshot" aria-labelledby="snapshot-heading">
        <div className="section-heading compact-heading">
          <div>
            <div className="section-kicker">INDEX SNAPSHOT</div>
            <h2 id="snapshot-heading">今日指数快照</h2>
          </div>
          <span className="section-asof">{state.latest.date}</span>
        </div>
        <NavCards
          latest={state.latest}
          strictCount={state.constituents.strict.length}
          extendedCount={state.constituents.extended.length}
        />
      </section>

      <section className="research-section" aria-labelledby="performance-heading">
        <div className="section-heading">
          <div>
            <div className="section-kicker">PERFORMANCE / NORMALIZED NAV</div>
            <h2 id="performance-heading">净值与基准</h2>
            <p className="section-deck">
              同一张图里比较严格动物园、扩展动物园和基准。缩放区间只改变视图，不改变指数口径。
            </p>
          </div>
        </div>
        <ZooChart
          history={state.history}
          benchmarkLabel={state.latest.benchmark_label}
        />
      </section>

      <div className="home-research-grid">
        <section className="research-section zoo-today" aria-labelledby="changes-heading">
          <div className="section-heading compact-heading">
            <div>
              <div className="section-kicker">THE ZOO TODAY</div>
              <h2 id="changes-heading">最近调仓</h2>
            </div>
            <Link className="text-link" to="/changes">
              查看完整记录
            </Link>
          </div>
          <ChangesList changes={state.changes} />
        </section>

        <aside className="research-note" aria-labelledby="why-heading">
          <div className="section-kicker">WHY THIS EXISTS</div>
          <h2 id="why-heading">一个故意有点荒谬的量化实验</h2>
          <p>
            如果一个缺乏经济机制的分类也能得到漂亮历史曲线，那更值得追问的是数据挖掘、选择偏差和回测过拟合，而不是立刻发明一个故事解释它。
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
