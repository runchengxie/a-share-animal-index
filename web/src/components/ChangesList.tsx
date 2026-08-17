import { Changes, ChangeSet, Constituent } from "../api";

function ChangeSection({ title, set }: { title: string; set: ChangeSet }) {
  return (
    <div className="change-section">
      <h4>{title}</h4>
      {set.new_in.length === 0 && set.removed.length === 0 ? (
        <p className="muted">无变动。</p>
      ) : (
        <ul>
          {set.new_in.map((e) => (
            <li key={`in-${e.ts_code}`} className="change-in">
              纳入：{e.name}（{e.ts_code}）
            </li>
          ))}
          {set.removed.map((e) => (
            <li key={`out-${e.ts_code}`} className="change-out">
              移除：{e.name}（{e.ts_code}）
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function NoiseSection({ title, items }: { title: string; items: Constituent[] }) {
  return (
    <div className="change-section">
      <h4>{title}</h4>
      {items.length === 0 ? (
        <p className="muted">无单字疑似误伤。</p>
      ) : (
        <ul>
          {items.map((c) => (
            <li key={c.ts_code}>
              {c.name}（{c.ts_code}）匹配词：{c.keyword}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export default function ChangesList({ changes }: { changes: Changes }) {
  return (
    <div className="changes-list">
      <p className="muted">最近调仓日期：{changes.date}</p>
      <ChangeSection title="严格动物园" set={changes.changes.strict} />
      <ChangeSection title="扩展动物园" set={changes.changes.extended} />
      <NoiseSection title="单字疑似误伤（严格）" items={changes.suspected_noise.strict} />
      <NoiseSection title="单字疑似误伤（扩展）" items={changes.suspected_noise.extended} />
    </div>
  );
}
