import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  Changes,
  Constituents,
  IndexTheme,
  Latest,
  NavPoint,
  fetchThemeChanges,
  fetchThemeConstituents,
  fetchThemeHistory,
  fetchThemeLatest,
} from "../api";
import ChangesList from "../components/ChangesList";
import NavCards from "../components/NavCards";
import ZooChart from "../components/ZooChart";

type ThemeData = { latest: Latest; history: NavPoint[]; changes: Changes; constituents: Constituents };
type State =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ok"; themes: Record<IndexTheme, ThemeData> };

const THEME_LABELS: Record<IndexTheme, string> = { animal: "动物园", plant: "植物园" };

async function fetchThemeData(theme: IndexTheme): Promise<ThemeData> {
  const [latest, history, changes, constituents] = await Promise.all([
    fetchThemeLatest(theme), fetchThemeHistory(theme), fetchThemeChanges(theme), fetchThemeConstituents(theme),
  ]);
  return { latest, history, changes, constituents };
}

function ThemeDashboard({ theme, data }: { theme: IndexTheme; data: ThemeData }) {
  const themeLabel = THEME_LABELS[theme];
  return (
    <section className={`theme-dashboard theme-dashboard-${theme}`} id={`${theme}-panel`} aria-labelledby={`${theme}-dashboard-heading`}>
      <div className="theme-dashboard-heading">
        <div>
          <div className="section-kicker">{themeLabel}</div>
          <h2 id={`${theme}-dashboard-heading`}>{theme === "plant" ? "植物名称里的投资实验" : "动物名称里的投资实验"}</h2>
          <p>只按股票简称筛选成分，以等权方式构建指数，观察它相对 {data.latest.benchmark_label} 的历史表现。</p>
        </div>
        <span className="section-asof">截至 {data.latest.date}</span>
      </div>

      <section className="index-snapshot" aria-labelledby={`${theme}-snapshot-heading`}>
        <div className="section-heading compact-heading"><div><div className="section-kicker">指数快照</div><h3 id={`${theme}-snapshot-heading`}>今日数据</h3></div></div>
        <NavCards latest={data.latest} strictCount={data.constituents.strict.length} extendedCount={data.constituents.extended.length} themeLabel={themeLabel} />
      </section>

      <section className="research-section" aria-labelledby={`${theme}-performance-heading`}>
        <div className="section-heading"><div><div className="section-kicker">表现 / 标准化净值</div><h3 id={`${theme}-performance-heading`}>净值与基准</h3><p className="section-deck">比较严格{themeLabel}、扩展{themeLabel}和基准。缩放区间只改变视图，不改变指数口径。</p></div></div>
        <ZooChart history={data.history} benchmarkLabel={data.latest.benchmark_label} themeLabel={themeLabel} />
      </section>

      <div className="home-research-grid">
        <section className="research-section zoo-today" aria-labelledby={`${theme}-changes-heading`}>
          <div className="section-heading compact-heading"><div><div className="section-kicker">最近调仓</div><h3 id={`${theme}-changes-heading`}>成分变化</h3></div><Link className="text-link" to="/changes">查看完整记录</Link></div>
          <ChangesList changes={data.changes} themeLabel={themeLabel} />
        </section>
        <aside className="research-note" aria-labelledby={`${theme}-note-heading`}>
          <div className="section-kicker">阅读提示</div><h3 id={`${theme}-note-heading`}>先看现象，再查口径</h3>
          <p>这类名称分类缺少明确的经济机制。看到漂亮曲线后，应优先检查数据挖掘、选择偏差和回测过拟合，再讨论背后的解释。</p>
          <div className="research-note-links"><Link to="/methodology">阅读构建方法</Link><Link to="/constituents">查看当前成分</Link><Link to="/history">检查历史数据</Link></div>
        </aside>
      </div>
    </section>
  );
}

export default function Home() {
  const [state, setState] = useState<State>({ status: "loading" });
  useEffect(() => {
    Promise.all([fetchThemeData("animal"), fetchThemeData("plant")])
      .then(([animal, plant]) => setState({ status: "ok", themes: { animal, plant } }))
      .catch((error) => setState({ status: "error", message: String(error) }));
  }, []);

  if (state.status === "loading") return <p className="page-status">正在读取动物园和植物园数据…</p>;
  if (state.status === "error") return <p className="page-status muted">数据加载失败：{state.message}</p>;
  return (
    <div className="page-home">
      <section className="home-hero">
        <div className="section-kicker">研究问题</div><h2>名字里的动物和植物，能不能组成有意义的指数？</h2>
        <p>这里把两种有趣的名称分类按固定规则编成 A 股指数，持续记录它们相对基准的表现。页面同时展示两套结果，方便比较成分数量、净值走势和调仓情况。</p>
        <div className="hero-meta" aria-label="指数研究元数据"><span>截至 {state.themes.animal.latest.date}</span><span>收盘数据</span><span>规则构建</span><span>仅供研究</span></div>
      </section>
      <div className="theme-dashboards"><ThemeDashboard theme="animal" data={state.themes.animal} /><ThemeDashboard theme="plant" data={state.themes.plant} /></div>
    </div>
  );
}
