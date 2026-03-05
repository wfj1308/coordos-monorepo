import { pickField, renderValue } from "./utils";

export function MetricCard({ label, value }) {
  return (
    <div className="di-metric-card">
      <div className="di-metric-label">{label}</div>
      <div className="di-metric-value">{value}</div>
    </div>
  );
}

export function DataTable({
  title,
  rows,
  columns,
  emptyHint = "暂无数据",
  totalCount,
  page = 1,
  pageSize = 20,
  onPageChange,
  maxHeightClass = "max-h-64",
}) {
  const safeTotal = Number.isFinite(Number(totalCount)) ? Number(totalCount) : rows.length;
  const safePageSize = Math.max(1, Number(pageSize) || 20);
  const totalPages = Math.max(1, Math.ceil(safeTotal / safePageSize) || 1);
  const currentPage = Math.min(Math.max(1, Number(page) || 1), totalPages);
  const rangeStart = safeTotal === 0 ? 0 : (currentPage - 1) * safePageSize + 1;
  const rangeEnd = safeTotal === 0 ? 0 : Math.min(currentPage * safePageSize, safeTotal);

  return (
    <article className="di-table-card">
      <div className="mb-2 flex items-center justify-between gap-2 text-sm font-medium">
        <span className="di-table-title">{title}</span>
        <div className="flex items-center gap-2">
          <span className="di-table-range">
            {rangeStart}-{rangeEnd}
          </span>
          <span className="di-table-total">{safeTotal}</span>
        </div>
      </div>
      <div className={`di-table-scroll ${maxHeightClass}`}>
        <table className="di-data-table w-full min-w-[680px] border-collapse text-left text-xs">
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
              <tr key={`${title}-${idx}`} className="di-data-row odd:bg-white even:bg-slate-50">
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
      {typeof onPageChange === "function" && (
        <div className="mt-2 flex items-center justify-end gap-2 text-xs">
          <button
            type="button"
            onClick={() => onPageChange(currentPage - 1)}
            disabled={currentPage <= 1}
            className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-50"
          >
            上一页
          </button>
          <span className="text-slate-600">
            第 {currentPage}/{totalPages} 页
          </span>
          <button
            type="button"
            onClick={() => onPageChange(currentPage + 1)}
            disabled={currentPage >= totalPages}
            className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-50"
          >
            下一页
          </button>
        </div>
      )}
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
  return <span className={`rounded-full border px-2 py-0.5 text-xs ${map[status] || map.pending}`}>{status}</span>;
}

function toLayerKey(node) {
  const type = String(node?.library_type || node?.node_type || "").toLowerCase();
  if (type.includes("regulation_version")) return "regulation_version";
  if (type.includes("regulation")) return "regulation";
  if (type.includes("qualification")) return "qualification";
  if (type.includes("project")) return "project";
  if (type.includes("standard") || type.includes("drawing")) return "standard";
  if (type.includes("executor")) return "executor";
  return "other";
}

function layerTitle(key) {
  const map = {
    regulation: "法规",
    regulation_version: "法规版本",
    qualification: "资质",
    project: "项目",
    standard: "工程标准",
    executor: "执行体",
    other: "其他",
  };
  return map[key] || key;
}

function nodeColor(node) {
  const key = toLayerKey(node);
  const map = {
    regulation: { fill: "#fdf2e9", stroke: "#f97316" },
    regulation_version: { fill: "#fff7ed", stroke: "#fb923c" },
    qualification: { fill: "#ecfeff", stroke: "#06b6d4" },
    project: { fill: "#eef2ff", stroke: "#6366f1" },
    standard: { fill: "#ecfccb", stroke: "#84cc16" },
    executor: { fill: "#f3e8ff", stroke: "#a855f7" },
    other: { fill: "#f8fafc", stroke: "#64748b" },
  };
  return map[key] || map.other;
}

function safeNodeRef(node) {
  return String(node?.node_ref || node?.nodeRef || "");
}

function shortText(value, limit = 24) {
  const text = String(value || "");
  if (text.length <= limit) return text;
  return `${text.slice(0, limit - 1)}…`;
}

export function RelationGraphCanvas({ graph, onNodeClick }) {
  const payload = graph && typeof graph === "object" && graph.data ? graph.data : graph;
  const nodes = Array.isArray(payload?.nodes) ? payload.nodes : [];
  const edges = Array.isArray(payload?.edges) ? payload.edges : [];
  if (nodes.length === 0) {
    return (
      <div className="rounded-lg border border-slate-200 bg-white px-3 py-6 text-center text-xs text-slate-500">
        暂无关系图谱节点
      </div>
    );
  }

  const order = ["regulation", "regulation_version", "qualification", "project", "standard", "executor", "other"];
  const grouped = order.reduce((acc, key) => {
    acc[key] = [];
    return acc;
  }, {});
  const extraOrder = [];
  nodes.forEach((node) => {
    const key = toLayerKey(node);
    if (!Object.prototype.hasOwnProperty.call(grouped, key)) {
      grouped[key] = [];
      extraOrder.push(key);
    }
    grouped[key].push(node);
  });

  const activeLayers = [...order, ...extraOrder].filter((key) => grouped[key] && grouped[key].length > 0);
  const layerCount = Math.max(1, activeLayers.length);
  const maxRows = Math.max(1, ...activeLayers.map((key) => grouped[key].length));
  const width = Math.max(920, layerCount * 200);
  const height = Math.max(360, maxRows * 96 + 120);

  const positions = new Map();
  activeLayers.forEach((layer, layerIndex) => {
    const items = grouped[layer];
    const x = layerCount === 1 ? width / 2 : 80 + (layerIndex * (width - 160)) / (layerCount - 1);
    const rowCount = items.length;
    items.forEach((node, rowIndex) => {
      const y = rowCount === 1 ? height / 2 : 76 + (rowIndex * (height - 152)) / (rowCount - 1);
      positions.set(safeNodeRef(node), { x, y });
    });
  });

  const cardWidth = 176;
  const cardHeight = 56;

  return (
    <div className="space-y-2">
      <div className="di-relation-shell overflow-auto rounded-lg border border-slate-200 bg-white">
        <svg width={width} height={height} className="block">
          <defs>
            <marker
              id="graph-arrow"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="5"
              markerHeight="5"
              orient="auto-start-reverse"
            >
              <path d="M 0 0 L 10 5 L 0 10 z" fill="#64748b" />
            </marker>
          </defs>
          {activeLayers.map((layer, layerIndex) => {
            const x = layerCount === 1 ? width / 2 : 80 + (layerIndex * (width - 160)) / (layerCount - 1);
            return (
              <g key={`layer-${layer}`}>
                <text x={x} y={24} textAnchor="middle" className="fill-slate-600 text-xs">
                  {layerTitle(layer)}
                </text>
              </g>
            );
          })}
          {edges.map((edge, idx) => {
            const fromRef = String(edge?.from || "");
            const toRef = String(edge?.to || "");
            const from = positions.get(fromRef);
            const to = positions.get(toRef);
            if (!from || !to) return null;
            const startX = from.x + cardWidth / 2 - 4;
            const startY = from.y;
            const endX = to.x - cardWidth / 2 + 4;
            const endY = to.y;
            const ctrlX = (startX + endX) / 2;
            const relation = String(edge?.relation || "");
            return (
              <g key={`edge-${fromRef}-${toRef}-${idx}`}>
                <path
                  d={`M ${startX} ${startY} C ${ctrlX} ${startY}, ${ctrlX} ${endY}, ${endX} ${endY}`}
                  fill="none"
                  stroke="#94a3b8"
                  strokeWidth="1.5"
                  markerEnd="url(#graph-arrow)"
                />
                <text x={ctrlX} y={(startY + endY) / 2 - 4} textAnchor="middle" className="fill-slate-500 text-[10px]">
                  {shortText(relation, 18)}
                </text>
              </g>
            );
          })}
          {nodes.map((node, idx) => {
            const ref = safeNodeRef(node);
            const pos = positions.get(ref);
            if (!pos) return null;
            const palette = nodeColor(node);
            const label = String(node?.label || ref);
            const subtitle = String(node?.node_type || node?.library_type || "");
            const tail = node?.id ? `#${node.id}` : shortText(node?.ref || "", 18);
            return (
              <g
                key={`node-${ref}-${idx}`}
                transform={`translate(${pos.x - cardWidth / 2}, ${pos.y - cardHeight / 2})`}
                className={typeof onNodeClick === "function" ? "cursor-pointer" : ""}
                onClick={() => (typeof onNodeClick === "function" ? onNodeClick(node) : null)}
              >
                <title>{`${label}\n${subtitle} ${tail}`}</title>
                <rect
                  width={cardWidth}
                  height={cardHeight}
                  rx="10"
                  fill={palette.fill}
                  stroke={palette.stroke}
                  strokeWidth="1.2"
                />
                <text x="10" y="22" className="fill-slate-800 text-[11px] font-medium">
                  {shortText(label, 26)}
                </text>
                <text x="10" y="40" className="fill-slate-500 text-[10px]">
                  {shortText(`${subtitle}${tail ? ` ${tail}` : ""}`, 30)}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
      <p className="text-[11px] text-slate-500">提示：点击节点可快速跳转到“详情查询”或“执行体证书仓库”。</p>
    </div>
  );
}
