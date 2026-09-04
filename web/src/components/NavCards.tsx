import { Latest, formatNav, formatPercent } from "../api";

interface NavMetricProps {
  title: string;
  nav: number;
  daily: number;
  detail: string;
  variant: "strict" | "extended" | "benchmark";
}

function NavMetric({ title, nav, daily, detail, variant }: NavMetricProps) {
  const direction = daily >= 0 ? "up" : "down";
  return (
    <article className={`metric-cell metric-cell-${variant}`}>
      <div className="metric-label">{title}</div>
      <div className="metric-value">{formatNav(nav)}</div>
      <div className={`metric-change ${direction}`}>
        当日 {formatPercent(daily)}
      </div>
      <div className="metric-detail">{detail}</div>
    </article>
  );
}

function CountMetric({ title, value, detail }: { title: string; value: number; detail: string }) {
  return (
    <article className="metric-cell metric-cell-count">
      <div className="metric-label">{title}</div>
      <div className="metric-value">{value}</div>
      <div className="metric-detail">{detail}</div>
    </article>
  );
}

interface Props {
  latest: Latest;
  strictCount: number;
  extendedCount: number;
  themeLabel?: string;
}

export default function NavCards({ latest, strictCount, extendedCount, themeLabel = "动物园" }: Props) {
  return (
    <div className="metric-strip">
      <NavMetric
        title={`严格${themeLabel}`}
        nav={latest.zoo_strict_nav}
        daily={latest.zoo_strict_daily}
        detail={`相对基准 ${formatPercent(latest.zoo_strict_excess)}`}
        variant="strict"
      />
      <NavMetric
        title={`扩展${themeLabel}`}
        nav={latest.zoo_extended_nav}
        daily={latest.zoo_extended_daily}
        detail={`相对基准 ${formatPercent(latest.zoo_extended_excess)}`}
        variant="extended"
      />
      <NavMetric
        title={latest.benchmark_label}
        nav={latest.benchmark_nav}
        daily={latest.benchmark_daily}
        detail={latest.benchmark_code}
        variant="benchmark"
      />
      <CountMetric title={`严格${themeLabel}成分`} value={strictCount} detail="当前股票数" />
      <CountMetric title={`扩展${themeLabel}成分`} value={extendedCount} detail="当前股票数" />
    </div>
  );
}
