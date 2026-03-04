import { useEffect, useMemo, useState } from "react";

function readLocal(key, fallback) {
  const v = localStorage.getItem(key);
  return v == null ? fallback : v;
}

function saveLocal(key, value) {
  localStorage.setItem(key, value);
}

function trimTrailingSlash(v) {
  return String(v || "").replace(/\/+$/, "");
}

function formatTime(v) {
  if (!v) return "-";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleString("zh-CN");
}

function formatDate(v) {
  if (!v) return "-";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleDateString("zh-CN");
}

function toInt(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function asArray(v) {
  return Array.isArray(v) ? v : [];
}

function getIn(obj, path, fallback = undefined) {
  let cur = obj;
  for (const key of path) {
    if (cur == null || typeof cur !== "object") return fallback;
    if (!Object.prototype.hasOwnProperty.call(cur, key)) return fallback;
    cur = cur[key];
  }
  return cur == null ? fallback : cur;
}

async function apiRequest(url) {
  const res = await fetch(url, {
    method: "GET",
    headers: { "Content-Type": "application/json" },
  });
  const text = await res.text();
  let data = text;
  try {
    data = JSON.parse(text);
  } catch {}
  if (!res.ok) {
    const detail =
      (data && typeof data === "object" && (data.error || data.detail || data.message)) ||
      text ||
      "request failed";
    throw new Error(`[GET] ${url} -> ${res.status}: ${detail}`);
  }
  return data;
}

function renderCell(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.join(", ");
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return JSON.stringify(v);
}

const TARGET_AUDIENCE_LABEL = {
  COOPERATIVE_DESIGN_INSTITUTES: "协同设计院",
};

const CAPABILITY_LEVEL_LABEL = {
  PLATFORM_ENGINE: "平台引擎级",
  REGISTERED_STRUCTURAL_ENGINEER: "注册结构工程师级",
  REGISTERED_ENGINEER: "注册工程师级",
  SENIOR_ENGINEER: "高级工程师级",
  ENGINEER: "工程师级",
  ASSISTANT_ENGINEER: "助理工程师级",
  NONE: "暂无",
};

const QUAL_TYPE_LABEL = {
  REG_STRUCTURE: "一级注册结构工程师",
  REG_STRUCTURE_2: "二级注册结构工程师",
  REG_ARCH: "一级注册建筑师",
  REG_CIVIL_GEOTEC: "注册土木工程师（岩土）",
  REG_CIVIL_WATER: "注册土木工程师（水利水电）",
  REG_COST: "一级注册造价工程师",
  REG_ELECTRIC_POWER: "注册电气工程师（供配电）",
  REG_ELECTRIC_TRANS: "注册电气工程师（发输变电）",
  REG_MEP_POWER: "注册公用设备工程师（动力）",
  REG_MEP_WATER: "注册公用设备工程师（给水排水）",
  REG_MEP_HVAC: "注册公用设备工程师（暖通空调）",
  QUAL_HIGHWAY_INDUSTRY_A: "公路行业甲级",
  QUAL_MUNICIPAL_INDUSTRY_A: "市政行业甲级",
  QUAL_ARCH_COMPREHENSIVE_A: "建筑行业甲级",
  QUAL_LANDSCAPE_SPECIAL_A: "风景园林专项甲级",
  QUAL_WATER_INDUSTRY_B: "水利行业乙级",
};

const RULE_LABEL = {
  "RULE-001": "规则001",
  "RULE-002": "规则002",
  "RULE-003": "规则003",
  "RULE-004": "规则004",
  "RULE-005": "规则005",
};

function toTargetAudienceLabel(v) {
  const key = String(v || "").trim();
  return TARGET_AUDIENCE_LABEL[key] || key || "-";
}

function toCapabilityLevelLabel(v) {
  const key = String(v || "").trim();
  return CAPABILITY_LEVEL_LABEL[key] || key || "-";
}

function toQualTypeLabel(v) {
  const key = String(v || "").trim();
  if (!key) return "-";
  return QUAL_TYPE_LABEL[key] ? `${QUAL_TYPE_LABEL[key]}（${key}）` : key;
}

function toRuleLabels(v) {
  return asArray(v).map((it) => RULE_LABEL[String(it || "").trim()] || String(it || "").trim());
}

function MetricCard({ label, value, hint }) {
  return (
    <article className="rounded-xl border border-slate-200 bg-white p-4">
      <div className="text-xs text-slate">{label}</div>
      <div className="mt-1 text-lg font-semibold">{value}</div>
      {hint ? <div className="mt-1 text-[11px] text-slate">{hint}</div> : null}
    </article>
  );
}

function SimpleTable({ title, rows, columns, emptyHint }) {
  return (
    <article className="rounded-xl border border-slate-200 bg-white p-4">
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-semibold">{title}</h3>
        <span className="text-xs text-slate">{rows.length} 条</span>
      </div>
      <div className="max-h-72 overflow-auto rounded border border-slate-200">
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
            {rows.length === 0 ? (
              <tr>
                <td colSpan={columns.length} className="px-2 py-6 text-center text-slate-500">
                  {emptyHint || "暂无数据"}
                </td>
              </tr>
            ) : (
              rows.map((row, idx) => (
                <tr key={`${title}-${idx}`} className="odd:bg-white even:bg-slate-50">
                  {columns.map((col) => {
                    const val = col.render ? col.render(row) : row[col.key];
                    return (
                      <td key={`${title}-${idx}-${col.key}`} className="border-b border-slate-100 px-2 py-2 align-top">
                        <code className="break-all whitespace-pre-wrap text-[11px] text-slate-700">
                          {renderCell(val)}
                        </code>
                      </td>
                    );
                  })}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </article>
  );
}

export default function PartnerProfilePage() {
  const namespaceParam = useMemo(
    () => new URLSearchParams(window.location.search).get("namespace") || "cn.zhongbei",
    [],
  );
  const [diBase, setDiBase] = useState(readLocal("coordos.di.base", "/di"));
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [profile, setProfile] = useState(null);
  const [verifyInput, setVerifyInput] = useState("");
  const [verifyLoading, setVerifyLoading] = useState(false);
  const [verifyError, setVerifyError] = useState("");
  const [verifyResult, setVerifyResult] = useState(null);

  const endpoint = useMemo(() => `${trimTrailingSlash(diBase.trim())}/public/v1/partner-profile/${namespaceParam}`, [diBase, namespaceParam]);

  const refresh = async () => {
    if (!trimTrailingSlash(diBase.trim())) {
      setError("请先填写 Design-Ins 服务地址");
      return;
    }
    setLoading(true);
    setError("");
    try {
      const data = await apiRequest(endpoint);
      setProfile(data);
    } catch (err) {
      setProfile(null);
      setError(String(err));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const useFirstProofHash = () => {
    const items = asArray(getIn(profile, ["achievement_layer", "items"], []));
    let first = "";
    for (const item of items) {
      const hashes = asArray(item?.proof_hashes);
      first = String(hashes[0] || "").trim();
      if (first) break;
    }
    if (!first) {
      setVerifyError("当前没有可用的 proof_hash");
      return;
    }
    setVerifyError("");
    setVerifyInput(first);
  };

  const runVerify = async () => {
    const di = trimTrailingSlash(diBase.trim());
    const raw = String(verifyInput || "").trim();
    if (!di) {
      setVerifyError("请先填写 Design-Ins 服务地址");
      return;
    }
    if (!raw) {
      setVerifyError("请输入 ref 或 proof_hash");
      return;
    }
    setVerifyLoading(true);
    setVerifyError("");
    setVerifyResult(null);
    try {
      const isHash = raw.startsWith("sha256:") || /^[a-fA-F0-9]{64}$/.test(raw);
      const url = isHash
        ? `${di}/public/v1/verify/achievement/${encodeURIComponent(raw)}`
        : `${di}/api/v1/achievement/verify?ref=${encodeURIComponent(raw)}`;
      const data = await apiRequest(url);
      setVerifyResult(data);
    } catch (err) {
      setVerifyError(String(err));
    } finally {
      setVerifyLoading(false);
    }
  };

  return (
    <main className="min-h-full px-4 py-6 md:px-8">
      <section className="mx-auto max-w-6xl space-y-6">
        <header className="panel p-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h1 className="text-2xl font-semibold tracking-tight">{namespaceParam} 合作能力画像</h1>
              <p className="mt-2 text-sm text-slate">
                资质、能力、业绩与当前产能四层信息，用于对外协作评估。
              </p>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={refresh}
                disabled={loading}
                className="rounded-lg bg-skyline px-4 py-2 text-sm font-medium text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {loading ? "刷新中..." : "刷新"}
              </button>
              <a
                href="/"
                className="rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm transition hover:border-sky-400 hover:bg-sky-50"
              >
                返回
              </a>
            </div>
          </div>
        </header>

        <section className="panel p-6">
          <label className="block">
            <span className="mb-1 block text-xs text-slate">Design-Ins 服务地址</span>
            <input
              value={diBase}
              onChange={(e) => {
                const v = e.target.value;
                setDiBase(v);
                saveLocal("coordos.di.base", v);
              }}
              className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm"
            />
          </label>
          <p className="mt-2 text-xs text-slate">
            接口: <code>{endpoint}</code>
          </p>
          {error ? (
            <pre className="mt-3 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">{error}</pre>
          ) : null}
          <div className="mt-4 rounded-lg border border-slate-200 bg-slate-50 p-3">
            <div className="mb-2 text-sm font-medium">UTXO 独立核验</div>
            <div className="grid gap-2 md:grid-cols-[1fr_auto_auto]">
              <input
                value={verifyInput}
                onChange={(e) => setVerifyInput(e.target.value)}
                className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder="输入 ref / utxo_ref / proof_hash"
              />
              <button
                onClick={useFirstProofHash}
                className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm transition hover:border-sky-400 hover:bg-sky-50"
              >
                使用首个 proof_hash
              </button>
              <button
                onClick={runVerify}
                disabled={verifyLoading}
                className="rounded-lg bg-skyline px-4 py-2 text-sm font-medium text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {verifyLoading ? "核验中..." : "执行核验"}
              </button>
            </div>
            {verifyError ? (
              <pre className="mt-3 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">{verifyError}</pre>
            ) : null}
            {verifyResult ? (
              <pre className="mt-3 overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100">
                {JSON.stringify(verifyResult, null, 2)}
              </pre>
            ) : null}
          </div>
        </section>

        {profile ? (
          <>
            <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              <MetricCard label="命名空间" value={getIn(profile, ["tenant_ref"], "-")} />
              <MetricCard label="目标对象" value={toTargetAudienceLabel(getIn(profile, ["target_audience"], "-"))} />
              <MetricCard label="SPU 类型数" value={toInt(getIn(profile, ["capability_layer", "spu_type_count"], 0))} />
              <MetricCard
                label="近1年执行次数"
                value={toInt(getIn(profile, ["capability_layer", "executions_last_1y"], 0))}
                hint="含未结算 UTXO"
              />
            </section>

            <section className="panel p-6">
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                <MetricCard
                  label="可执行执行体"
                  value={toInt(getIn(profile, ["capability_layer", "executable_executor_count"], 0))}
                />
                <MetricCard
                  label="注册工程师"
                  value={toInt(getIn(profile, ["capability_layer", "registered_engineer_count"], 0))}
                />
                <MetricCard
                  label="平均能力等级"
                  value={toCapabilityLevelLabel(getIn(profile, ["capability_layer", "average_capability_level"], "-"))}
                />
                <MetricCard label="生成时间" value={formatTime(getIn(profile, ["generated_at"], ""))} />
                <MetricCard
                  label="在手项目"
                  value={toInt(getIn(profile, ["capacity_layer", "in_hand_project_count"], 0))}
                />
                <MetricCard
                  label="总产能"
                  value={toInt(getIn(profile, ["capacity_layer", "total_capacity_limit"], 0))}
                />
                <MetricCard
                  label="剩余产能"
                  value={toInt(getIn(profile, ["capacity_layer", "remaining_capacity"], 0))}
                />
              </div>
            </section>

            <section className="grid gap-4 lg:grid-cols-2">
              <SimpleTable
                title="资质层"
                rows={asArray(getIn(profile, ["qualification_layer", "items"], []))}
                columns={[
                  { key: "label", label: "资质名称" },
                  { key: "cert_no", label: "证书编号" },
                  { key: "valid_until", label: "有效期", render: (row) => formatDate(row.valid_until) },
                  { key: "credit_code", label: "信用代码" },
                  { key: "scope", label: "业务范围" },
                  { key: "rule_binding", label: "绑定规则", render: (row) => toRuleLabels(row.rule_binding) },
                  { key: "verify_url", label: "核验地址" },
                ]}
              />
              <SimpleTable
                title="业绩层（近3年已结算）"
                rows={asArray(getIn(profile, ["achievement_layer", "items"], []))}
                columns={[
                  { key: "project_ref", label: "项目" },
                  { key: "settled_utxo_count", label: "结算 UTXO 数" },
                  { key: "latest_settled_at", label: "最近结算时间", render: (row) => formatTime(row.latest_settled_at) },
                  { key: "proof_hashes", label: "证明哈希" },
                ]}
                emptyHint="暂无已结算业绩"
              />
              <SimpleTable
                title="工程师资质分布"
                rows={Object.entries(
                  getIn(profile, ["capability_layer", "qualification_type_counts"], {}) || {},
                )
                  .map(([qualType, count]) => ({ qual_type: qualType, count }))
                  .sort((a, b) => Number(b.count) - Number(a.count))}
                columns={[
                  { key: "qual_type", label: "资质类型", render: (row) => toQualTypeLabel(row.qual_type) },
                  { key: "count", label: "人数" },
                ]}
              />
              <SimpleTable
                title="产能层（按专业）"
                rows={asArray(getIn(profile, ["capacity_layer", "by_specialty"], []))}
                columns={[
                  { key: "specialty", label: "专业" },
                  { key: "qualified_executors", label: "执行体数" },
                  { key: "capacity_limit", label: "总产能" },
                  { key: "occupied_estimate", label: "已占用" },
                  { key: "remaining_capacity", label: "剩余" },
                ]}
              />
              <SimpleTable
                title="能力层（SPU 引用）"
                rows={asArray(getIn(profile, ["capability_layer", "spu_types"], [])).map((spu, idx) => ({
                  seq: idx + 1,
                  spu_ref: spu,
                }))}
                columns={[
                  { key: "seq", label: "#" },
                  { key: "spu_ref", label: "SPU 引用" },
                ]}
              />
            </section>
          </>
        ) : null}
      </section>
    </main>
  );
}
