import { Latest, formatNav, formatPercent } from "../api";

interface CardProps {
  title: string;
  nav: number;
  daily: number;
  color: string;
}

function Card({ title, nav, daily, color }: CardProps) {
  const up = daily >= 0;
  return (
    <div className="nav-card">
      <div className="nav-card-title" style={{ color }}>
        {title}
      </div>
      <div className="nav-card-nav">{formatNav(nav)}</div>
      <div className={`nav-card-daily ${up ? "up" : "down"}`}>
        当日 {formatPercent(daily)}
      </div>
    </div>
  );
}

export default function NavCards({ latest }: { latest: Latest }) {
  return (
    <div className="nav-cards">
      <Card
        title="严格动物园"
        nav={latest.zoo_strict_nav}
        daily={latest.zoo_strict_daily}
        color="#2f855a"
      />
      <Card
        title="扩展动物园"
        nav={latest.zoo_extended_nav}
        daily={latest.zoo_extended_daily}
        color="#c05621"
      />
      <Card
        title={latest.benchmark_label}
        nav={latest.benchmark_nav}
        daily={latest.benchmark_daily}
        color="#3182ce"
      />
    </div>
  );
}
