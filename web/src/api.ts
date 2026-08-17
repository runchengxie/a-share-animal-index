export interface NavPoint {
  date: string;
  zoo_strict_ret: number;
  zoo_extended_ret: number;
  benchmark_ret: number;
  zoo_strict_nav: number;
  zoo_extended_nav: number;
  benchmark_nav: number;
}

export interface Latest {
  date: string;
  zoo_strict_nav: number;
  zoo_extended_nav: number;
  benchmark_nav: number;
  zoo_strict_daily: number;
  zoo_extended_daily: number;
  benchmark_daily: number;
  zoo_strict_excess: number;
  zoo_extended_excess: number;
  benchmark_code: string;
  benchmark_label: string;
}

export interface Constituent {
  ts_code: string;
  name: string;
  keyword: string;
  forced: boolean;
}

export interface Constituents {
  date: string;
  strict: Constituent[];
  extended: Constituent[];
}

export interface ChangeEntry {
  ts_code: string;
  name: string;
}

export interface ChangeSet {
  new_in: ChangeEntry[];
  removed: ChangeEntry[];
}

export interface Changes {
  date: string;
  changes: { strict: ChangeSet; extended: ChangeSet };
  suspected_noise: { strict: Constituent[]; extended: Constituent[] };
}

export interface Metadata {
  updated: string;
  benchmark: { code: string; label: string; source: string };
  variants: string[];
  rebalance: string;
}

const DATA_BASE = import.meta.env.BASE_URL;

async function getJson<T>(file: string): Promise<T> {
  const res = await fetch(`${DATA_BASE}data/${file}`);
  if (!res.ok) {
    throw new Error(`无法加载 ${file}（${res.status}）`);
  }
  return (await res.json()) as T;
}

export const fetchLatest = () => getJson<Latest>("latest.json");
export const fetchHistory = () => getJson<NavPoint[]>("history.json");
export const fetchConstituents = () => getJson<Constituents>("constituents.json");
export const fetchChanges = () => getJson<Changes>("changes.json");
export const fetchMetadata = () => getJson<Metadata>("metadata.json");

export function formatPercent(value: number): string {
  const sign = value > 0 ? "+" : "";
  return `${sign}${(value * 100).toFixed(2)}%`;
}

export function formatNav(value: number): string {
  return value.toFixed(4);
}
