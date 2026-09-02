import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const app = readFileSync(new URL("./App.tsx", import.meta.url), "utf8");
const home = readFileSync(new URL("./pages/Home.tsx", import.meta.url), "utf8");
const styles = readFileSync(new URL("./styles.css", import.meta.url), "utf8");
const chart = readFileSync(new URL("./components/ZooChart.tsx", import.meta.url), "utf8");

test("site shell exposes an editorial masthead and research navigation", () => {
  assert.match(app, /brand-kicker/);
  assert.match(app, /A-SHARE ZOO INDEX/);
  assert.match(app, /site-deck/);
  assert.match(app, /site-nav/);
});

test("home page leads with a research question and index snapshot", () => {
  assert.match(home, /home-hero/);
  assert.match(home, /如果只买名字里带动物的上市公司/);
  assert.match(home, /index-snapshot/);
  assert.match(home, /fetchConstituents/);
  assert.match(home, /research-section/);
});

test("editorial stylesheet uses paper, rules, tabular numerals and mobile safeguards", () => {
  assert.match(styles, /--paper:/);
  assert.match(styles, /--rule:/);
  assert.match(styles, /font-variant-numeric:\s*tabular-nums/);
  assert.match(styles, /background-image:/);
  assert.match(styles, /\.metric-strip/);
  assert.match(styles, /@media\s*\(max-width:\s*640px\)/);
  assert.match(styles, /overflow-x:\s*auto/);
});

test("NAV chart follows the low-noise research palette", () => {
  assert.match(chart, /axisPointer/);
  assert.match(chart, /splitLine/);
  assert.match(chart, /#1267d6/);
  assert.match(chart, /#b96800/);
  assert.match(chart, /#68717d/);
});
