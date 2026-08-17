import { useEffect, useRef } from "react";
import * as echarts from "echarts";
import { NavPoint } from "../api";

interface Props {
  history: NavPoint[];
  benchmarkLabel: string;
}

export default function ZooChart({ history, benchmarkLabel }: Props) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!ref.current) return;
    const chart = echarts.init(ref.current);
    const dates = history.map((p) => p.date);
    const strict = history.map((p) => p.zoo_strict_nav);
    const extended = history.map((p) => p.zoo_extended_nav);
    const benchmark = history.map((p) => p.benchmark_nav);

    chart.setOption({
      tooltip: { trigger: "axis" },
      legend: { data: ["严格动物园", "扩展动物园", benchmarkLabel], top: 0 },
      grid: { left: 50, right: 20, top: 40, bottom: 40 },
      xAxis: {
        type: "category",
        data: dates,
        axisLabel: { formatter: (value: string) => value.slice(0, 6) },
      },
      yAxis: { type: "value", scale: true },
      dataZoom: [{ type: "inside" }, { type: "slider" }],
      series: [
        { name: "严格动物园", type: "line", data: strict, showSymbol: false, lineStyle: { width: 1.5 } },
        { name: "扩展动物园", type: "line", data: extended, showSymbol: false, lineStyle: { width: 1.5 } },
        { name: benchmarkLabel, type: "line", data: benchmark, showSymbol: false, lineStyle: { width: 1.5 } },
      ],
    });

    const onResize = () => chart.resize();
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      chart.dispose();
    };
  }, [history, benchmarkLabel]);

  return <div ref={ref} className="zoo-chart" />;
}
