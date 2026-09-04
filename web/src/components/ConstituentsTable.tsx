import { Constituent } from "../api";

interface Props {
  variant: "strict" | "extended";
  items: Constituent[];
  themeLabel?: string;
}

export default function ConstituentsTable({ variant, items, themeLabel = "动物园" }: Props) {
  const title = variant === "strict" ? `严格${themeLabel}成分` : `扩展${themeLabel}成分`;
  return (
    <details className="constituents-table" open={variant === "strict"}>
      <summary>{title}（{items.length}）</summary>
      {items.length === 0 ? (
        <p className="muted">暂无成分。</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>代码</th>
              <th>名称</th>
              <th>匹配词</th>
              <th>类型</th>
            </tr>
          </thead>
          <tbody>
            {items.map((c) => (
              <tr key={c.ts_code}>
                <td>{c.ts_code}</td>
                <td>{c.name}</td>
                <td>{c.keyword || "无"}</td>
                <td>{c.forced ? "强制" : "匹配"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </details>
  );
}
