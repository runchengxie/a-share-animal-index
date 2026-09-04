import { useEffect, useState } from "react";
import { NavLink, Route, Routes } from "react-router-dom";
import { fetchMetadata } from "./api";
import About from "./pages/About";
import Changes from "./pages/Changes";
import Constituents from "./pages/Constituents";
import History from "./pages/History";
import Home from "./pages/Home";
import Methodology from "./pages/Methodology";

export default function App() {
  const [updated, setUpdated] = useState<string>("");
  useEffect(() => {
    fetchMetadata()
      .then((metadata) => setUpdated(metadata.updated))
      .catch(() => undefined);
  }, []);

  return (
    <div className="app">
      <header className="site-header">
        <div className="site-masthead">
          <div>
            <div className="brand-kicker">A 股动物园与植物园 · 规则化研究</div>
            <h1>A 股动物园与植物园</h1>
            <p className="site-deck">
              把一个看似荒谬的股票分类，做成公开规则、每日更新、可以复查的指数实验。
            </p>
          </div>
          <div className="site-meta" aria-label="数据更新时间">
            <span>研究指数</span>
            <strong>{updated || "等待数据"}</strong>
          </div>
        </div>
        <nav className="site-nav" aria-label="主导航">
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
        <span>数据每日收盘后更新 · 规则公开 · 仅供研究</span>
        <span>不构成投资建议</span>
      </footer>
    </div>
  );
}
