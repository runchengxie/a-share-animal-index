import { useEffect, useRef } from "react";
import * as echarts from "echarts";
import { NavPoint } from "../api";

interface Props {
  history: NavPoint[];
  benchmarkLabel: string;
  themeLabel?: string;
}

const STRICT_COLOR = "#1267d6";
const EXTENDED_COLOR = "#b96800";
const BENCHMARK_COLOR = "#68717d";
const RULE_COLOR = "#d9ddd9";
const MUTED_COLOR = "#7a828c";

export default function ZooChart({ history, benchmarkLabel, themeLabel = "动物园" }: Props) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!ref.current) return;
    const chart = echarts.init(ref.current);
    const dates = history.map((point) => point.date);
    const strict = history.map((point) => point.zoo_strict_nav);
    const extended = history.map((point) => point.zoo_extended_nav);
    const benchmark = history.map((point) => point.benchmark_nav);

    chart.setOption({
      animationDuration: 280,
      backgroundColor: "transparent",
      color: [STRICT_COLOR, EXTENDED_COLOR, BENCHMARK_COLOR],
      tooltip: {
        trigger: "axis",
        backgroundColor: "#232a33",
        borderWidth: 0,
        padding: [10, 12],
        textStyle: { color: "#f8f7f2", fontSize: 12 },
        axisPointer: {
          type: "line",
          lineStyle: { color: "#9aa1a8", type: "dashed", width: 1 },
        },
      },
      legend: {
        data: [`严格${themeLabel}`, `扩展${themeLabel}`, benchmarkLabel],
        top: 0,
        left: 0,
        itemWidth: 18,
        itemHeight: 2,
        textStyle: { color: MUTED_COLOR, fontSize: 11 },
      },
      grid: { left: 52, right: 24, top: 44, bottom: 58 },
      xAxis: {
        type: "category",
        boundaryGap: false,
        data: dates,
        axisLine: { lineStyle: { color: RULE_COLOR } },
        axisTick: { show: false },
        axisLabel: {
          color: MUTED_COLOR,
          fontSize: 10,
          hideOverlap: true,
          formatter: (value: string) => value.slice(0, 7),
        },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: MUTED_COLOR, fontSize: 10 },
        splitLine: { lineStyle: { color: "#e5e6e2", width: 1 } },
      },
      dataZoom: [
        { type: "inside", filterMode: "none" },
        {
          type: "slider",
          height: 14,
          bottom: 12,
          borderColor: "transparent",
          backgroundColor: "#eeede8",
          fillerColor: "rgba(18, 103, 214, 0.12)",
          handleSize: "90%",
          showDetail: false,
          moveHandleSize: 4,
          textStyle: { color: MUTED_COLOR },
        },
      ],
      series: [
        {
          name: `严格${themeLabel}`,
          type: "line",
          data: strict,
          showSymbol: false,
          connectNulls: false,
          lineStyle: { width: 2.2, color: STRICT_COLOR },
          emphasis: { focus: "series" },
        },
        {
          name: `扩展${themeLabel}`,
          type: "line",
          data: extended,
          showSymbol: false,
          connectNulls: false,
          lineStyle: { width: 2, color: EXTENDED_COLOR },
          emphasis: { focus: "series" },
        },
        {
          name: benchmarkLabel,
          type: "line",
          data: benchmark,
          showSymbol: false,
          connectNulls: false,
          lineStyle: { width: 1.5, color: BENCHMARK_COLOR, type: "dashed" },
          emphasis: { focus: "series" },
        },
      ],
    });

    const onResize = () => chart.resize();
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      chart.dispose();
    };
  }, [history, benchmarkLabel]);

  return <div ref={ref} className="zoo-chart" role="img" aria-label={`${themeLabel}指数净值走势图`} />;
}
