import { DataTable, MetricCard } from "./CommonUI";
import { asArray, getIn, toInt } from "./utils";

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

function toTargetAudienceLabel(v) {
  const key = String(v || "").trim();
  return TARGET_AUDIENCE_LABEL[key] || key || "-";
}

function toCapabilityLevelLabel(v) {
  const key = String(v || "").trim();
  return CAPABILITY_LEVEL_LABEL[key] || key || "-";
}

export default function PartnerProfileSection({
  namespace,
  onNamespaceChange,
  onRefresh,
  loading,
  error,
  profile,
  verifyInput,
  verifyLoading,
  verifyError,
  verifyResult,
  onVerifyInputChange,
  onVerify,
  onUseFirstProofHash,
}) {
  return (
    <section className="panel p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">对外能力声明（合作设计院）</h2>
          <p className="mt-1 text-xs text-slate">
            对应文档四层结构：资质层 / 能力层 / 业绩层 / 当前产能。
          </p>
        </div>
        <div className="flex items-center gap-2">
          <input
            value={namespace}
            onChange={(e) => onNamespaceChange(e.target.value)}
            placeholder="cn.zhongbei"
            className="w-40 rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
          />
          <button
            onClick={onRefresh}
            disabled={loading}
            className="rounded-lg bg-skyline px-4 py-2 text-sm font-medium text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {loading ? "刷新中..." : "刷新能力声明"}
          </button>
        </div>
      </div>

      {error && (
        <pre className="mt-3 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">{error}</pre>
      )}

      {!profile && !error && (
        <p className="mt-3 text-xs text-slate">点击“刷新能力声明”加载 /public/v1/partner-profile/{`{namespace}`}。</p>
      )}

      {profile && (
        <div className="mt-4 space-y-4">
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <MetricCard label="租户" value={getIn(profile, ["tenant_ref"], "-")} />
            <MetricCard
              label="目标受众"
              value={toTargetAudienceLabel(getIn(profile, ["target_audience"], "-"))}
            />
            <MetricCard
              label="SPU 类型数"
              value={toInt(getIn(profile, ["capability_layer", "spu_type_count"], 0))}
            />
            <MetricCard
              label="近1年执行次数"
              value={toInt(getIn(profile, ["capability_layer", "executions_last_1y"], 0))}
            />
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
            <DataTable
              title="资质层"
              rows={asArray(getIn(profile, ["qualification_layer", "items"], []))}
              columns={[
                { key: "label", label: "资质", keys: ["label"] },
                { key: "cert_no", label: "证书号", keys: ["cert_no"], nowrap: true },
                { key: "scope", label: "范围", keys: ["scope"] },
                { key: "verify_url", label: "核查链接", keys: ["verify_url"] },
              ]}
            />
            <DataTable
              title="业绩层（近3年）"
              rows={asArray(getIn(profile, ["achievement_layer", "items"], []))}
              emptyHint="暂无近3年已结算业绩；完成结算后将自动出现。"
              columns={[
                { key: "project_ref", label: "项目", keys: ["project_ref"] },
                { key: "settled_utxo_count", label: "已结算UTXO", keys: ["settled_utxo_count"] },
                { key: "latest_settled_at", label: "最近结算", keys: ["latest_settled_at"] },
                { key: "proof_hashes", label: "proof_hash", keys: ["proof_hashes"] },
              ]}
            />
            <DataTable
              title="产能层（分专业）"
              rows={asArray(getIn(profile, ["capacity_layer", "by_specialty"], []))}
              columns={[
                { key: "specialty", label: "专业方向", keys: ["specialty"] },
                { key: "qualified_executors", label: "可执行人数", keys: ["qualified_executors"] },
                { key: "capacity_limit", label: "容量上限", keys: ["capacity_limit"] },
                { key: "remaining_capacity", label: "剩余容量", keys: ["remaining_capacity"] },
              ]}
            />
            <article className="rounded-xl border border-slate-200 bg-white p-3">
              <div className="mb-2 text-sm font-medium">能力层</div>
              <div className="grid gap-2 text-xs text-slate-700">
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-2">
                  平均能力等级：
                  <code>
                    {toCapabilityLevelLabel(
                      getIn(profile, ["capability_layer", "average_capability_level"], "-"),
                    )}
                  </code>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-2">
                  可执行执行体：
                  <code>{toInt(getIn(profile, ["capability_layer", "executable_executor_count"], 0))}</code>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-2">
                  当前在手项目：
                  <code>{toInt(getIn(profile, ["capacity_layer", "in_hand_project_count"], 0))}</code>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-2">
                  总承接上限：
                  <code>{toInt(getIn(profile, ["capacity_layer", "total_capacity_limit"], 0))}</code>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-2">
                  总剩余容量：
                  <code>{toInt(getIn(profile, ["capacity_layer", "remaining_capacity"], 0))}</code>
                </div>
              </div>
            </article>
          </div>
        </div>
      )}

      <article className="mt-4 rounded-xl border border-slate-200 bg-white p-3">
        <div className="mb-2 text-sm font-medium">UTXO 独立核验</div>
        <p className="mb-3 text-xs text-slate">
          输入 `ref / utxo_ref / proof_hash`，直接调用校验接口返回 `verified` 与重算哈希。
        </p>
        <div className="grid gap-2 md:grid-cols-[1fr_auto_auto]">
          <input
            value={verifyInput}
            onChange={(e) => onVerifyInputChange(e.target.value)}
            placeholder="如 v://cn.zhongbei/utxo/... 或 sha256:..."
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
          />
          <button
            onClick={onUseFirstProofHash}
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm transition hover:border-skyline hover:text-skyline"
          >
            使用首个 proof_hash
          </button>
          <button
            onClick={onVerify}
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
      </article>
    </section>
  );
}
