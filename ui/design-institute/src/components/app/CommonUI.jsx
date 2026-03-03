import { pickField, renderValue } from "./utils";

export function MetricCard({ label, value }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-3">
      <div className="text-xs text-slate">{label}</div>
      <div className="mt-1 text-lg font-semibold">{value}</div>
    </div>
  );
}

export function DataTable({ title, rows, columns, emptyHint = "暂无数据" }) {
  return (
    <article className="rounded-xl border border-slate-200 bg-white p-3">
      <div className="mb-2 text-sm font-medium">
        {title} <span className="text-xs text-slate">({rows.length})</span>
      </div>
      <div className="max-h-64 overflow-auto rounded border border-slate-200">
        <table className="w-full min-w-[680px] border-collapse text-left text-xs">
          <thead className="bg-slate-100 text-slate-700">
            <tr>
              {columns.map((col) => (
                <th key={col.key} className="border-b border-slate-200 px-2 py-2 font-medium">
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 && (
              <tr>
                <td colSpan={columns.length} className="px-2 py-4 text-center text-slate-500">
                  {emptyHint}
                </td>
              </tr>
            )}
            {rows.map((row, idx) => (
              <tr key={`${title}-${idx}`} className="odd:bg-white even:bg-slate-50">
                {columns.map((col) => {
                  const value = pickField(row, col.keys, "");
                  const codeClass = col.nowrap
                    ? "whitespace-nowrap break-normal text-[11px] text-slate-700"
                    : "break-all whitespace-pre-wrap text-[11px] text-slate-700";
                  return (
                    <td key={`${title}-${idx}-${col.key}`} className="border-b border-slate-100 px-2 py-2 align-top">
                      <code className={codeClass}>{renderValue(value)}</code>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </article>
  );
}

export function Input({ label, value, onChange }) {
  return (
    <label className="block">
      <span className="mb-1 block text-xs text-slate">{label}</span>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm"
      />
    </label>
  );
}

export function StatusTag({ status }) {
  const map = {
    pending: "bg-slate-100 text-slate-700 border-slate-300",
    running: "bg-sky-100 text-sky-700 border-sky-300",
    done: "bg-emerald-100 text-emerald-700 border-emerald-300",
    failed: "bg-red-100 text-red-700 border-red-300",
  };
  return (
    <span className={`rounded-full border px-2 py-0.5 text-xs ${map[status] || map.pending}`}>
      {status}
    </span>
  );
}
