import { DataTable, MetricCard } from "./CommonUI";
import { mergeFinanceRows, pickField } from "./utils";

export default function DashboardSection({
  dashboard,
  dashboardLoading,
  dashboardError,
  loadDashboardData,
  selectedProjectRef,
  setSelectedProjectRef,
  loadProjectDetail,
  projectDetailLoading,
}) {
  return (
    <section className="panel p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">联调数据看板（读取现有数据库数据）</h2>
          <p className="mt-1 text-xs text-slate">直接调用 design-institute 接口，展示当前 PG 已落库的业务数据。</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={loadDashboardData}
            disabled={dashboardLoading}
            className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-medium text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {dashboardLoading ? "刷新中..." : "刷新看板"}
          </button>
        </div>
      </div>

      <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <MetricCard label="项目" value={dashboard.projects.length} />
        <MetricCard label="合同" value={dashboard.contracts.length} />
        <MetricCard label="人员" value={dashboard.employees.length} />
        <MetricCard label="资质" value={dashboard.qualifications.length} />
        <MetricCard label="业绩" value={dashboard.achievements.length} />
        <MetricCard label="回款" value={dashboard.gatherings.length} />
        <MetricCard label="发票" value={dashboard.invoices.length} />
        <MetricCard label="结算" value={dashboard.settlements.length} />
      </div>
      {dashboardError && (
        <pre className="mt-3 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">{dashboardError}</pre>
      )}

      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <DataTable
          title="项目"
          rows={dashboard.projects}
          columns={[
            { key: "ref", label: "Ref", keys: ["ref", "Ref"] },
            { key: "name", label: "名称", keys: ["name", "Name"] },
            { key: "status", label: "状态", keys: ["status", "Status"] },
            { key: "contract_ref", label: "ContractRef", keys: ["contract_ref", "ContractRef"] },
          ]}
        />
        <DataTable
          title="合同"
          rows={dashboard.contracts}
          columns={[
            { key: "id", label: "ID", keys: ["id", "ID"] },
            { key: "num", label: "编号", keys: ["num", "Num"] },
            { key: "name", label: "名称", keys: ["contract_name", "ContractName"] },
            { key: "project_ref", label: "ProjectRef", keys: ["project_ref", "ProjectRef"] },
            { key: "state", label: "状态", keys: ["state", "State"] },
          ]}
        />
        <DataTable
          title="人员"
          rows={dashboard.employees}
          columns={[
            { key: "id", label: "ID", keys: ["id", "ID"] },
            { key: "name", label: "姓名", keys: ["name", "Name"] },
            { key: "executor_ref", label: "ExecutorRef", keys: ["executor_ref", "ExecutorRef"] },
            { key: "position", label: "岗位", keys: ["position", "Position"] },
          ]}
        />
        <DataTable
          title="资质"
          rows={dashboard.qualifications}
          columns={[
            { key: "id", label: "ID", keys: ["id", "ID"] },
            { key: "qual_type", label: "类型", keys: ["qual_type", "QualType"] },
            { key: "executor_ref", label: "ExecutorRef", keys: ["executor_ref", "ExecutorRef"] },
            { key: "status", label: "状态", keys: ["status", "Status"] },
            { key: "valid_until", label: "有效期", keys: ["valid_until", "ValidUntil"] },
          ]}
        />
        <DataTable
          title="业绩"
          rows={dashboard.achievements}
          columns={[
            { key: "id", label: "ID", keys: ["id", "ID"] },
            { key: "spu_ref", label: "SPU", keys: ["spu_ref", "SpuRef"] },
            { key: "project_ref", label: "ProjectRef", keys: ["project_ref", "ProjectRef"] },
            { key: "executor_ref", label: "ExecutorRef", keys: ["executor_ref", "ExecutorRef"] },
          ]}
        />
        <DataTable
          title="财务流转"
          rows={mergeFinanceRows(dashboard.gatherings, dashboard.invoices, dashboard.settlements)}
          columns={[
            { key: "type", label: "类型", keys: ["type"] },
            { key: "id", label: "ID", keys: ["id"] },
            { key: "project_ref", label: "ProjectRef", keys: ["project_ref"] },
            { key: "contract_id", label: "ContractID", keys: ["contract_id"] },
            { key: "state", label: "状态", keys: ["state"] },
          ]}
        />
      </div>

      <div className="mt-5 rounded-xl border border-slate-200 bg-slate-50 p-4">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-sm font-medium">项目资源回读</span>
          <select
            value={selectedProjectRef}
            onChange={(e) => setSelectedProjectRef(e.target.value)}
            className="min-w-80 rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
          >
            <option value="">请选择项目</option>
            {dashboard.projects.map((it, idx) => {
              const ref = String(pickField(it, ["ref", "Ref"], ""));
              const name = String(pickField(it, ["name", "Name"], ""));
              if (!ref) return null;
              return (
                <option key={`${ref}-${idx}`} value={ref}>
                  {name ? `${name} (${ref})` : ref}
                </option>
              );
            })}
          </select>
          <button
            onClick={() => loadProjectDetail(selectedProjectRef)}
            disabled={!selectedProjectRef || projectDetailLoading}
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs disabled:cursor-not-allowed disabled:opacity-60"
          >
            {projectDetailLoading ? "读取中..." : "读取该项目资源"}
          </button>
        </div>
        {dashboard.updatedAt && <p className="mt-2 text-xs text-slate">最近刷新: {dashboard.updatedAt}</p>}
        <div className="mt-3 grid gap-3 lg:grid-cols-2">
          <pre className="min-h-40 overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100">
            {JSON.stringify(
              {
                project_ref: selectedProjectRef || null,
                qualification_assignment_count: dashboard.qualificationAssignments.length,
                assignments: dashboard.qualificationAssignments,
              },
              null,
              2,
            )}
          </pre>
          <pre className="min-h-40 overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100">
            {JSON.stringify(dashboard.projectResources || { hint: "先刷新看板或选择项目后读取资源" }, null, 2)}
          </pre>
        </div>
      </div>
    </section>
  );
}
