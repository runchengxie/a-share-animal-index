import { Routes, Route, NavLink } from "react-router-dom";
import Home from "./pages/Home";
import Methodology from "./pages/Methodology";
import Constituents from "./pages/Constituents";
import History from "./pages/History";
import Changes from "./pages/Changes";
import About from "./pages/About";
import { fetchMetadata } from "./api";
import { useEffect, useState } from "react";

export default function App() {
  const [updated, setUpdated] = useState<string>("");

  useEffect(() => {
    fetchMetadata()
      .then((m) => setUpdated(m.updated))
      .catch(() => undefined);
  }, []);

  return (
    <div className="app">
      <header className="site-header">
        <h1>A股动物园指数</h1>
        <nav className="site-nav">
          <NavLink to="/">首页</NavLink>
          <NavLink to="/methodology">方法</NavLink>
          <NavLink to="/constituents">成分</NavLink>
          <NavLink to="/history">历史</NavLink>
          <NavLink to="/changes">调仓</NavLink>
          <NavLink to="/about">关于</NavLink>
        </nav>
      </header>
      <main className="site-main">
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/methodology" element={<Methodology />} />
          <Route path="/constituents" element={<Constituents />} />
          <Route path="/history" element={<History />} />
          <Route path="/changes" element={<Changes />} />
          <Route path="/about" element={<About />} />
        </Routes>
      </main>
      <footer className="site-footer">
        数据每日收盘后更新，仅供研究，不构成投资建议。
        {updated ? `（更新于 ${updated}）` : ""}
      </footer>
    </div>
  );
}
