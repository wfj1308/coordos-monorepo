import { useEffect, useMemo, useState } from "react";
import { DataTable, MetricCard } from "./CommonUI";
import { mergeFinanceRows, pickField } from "./utils";

export default function DashboardSection({
  dashboard,
  dashboardLoading,
  dashboardError,
  libraryDetail,
  libraryDetailLoading,
  libraryDetailError,
  libraryRelations,
  libraryRelationsLoading,
  libraryRelationsError,
  executorVault,
  executorVaultLoading,
  executorVaultError,
  librarySearch,
  librarySearchLoading,
  librarySearchError,
  loadDashboardData,
  loadLibraryDetail,
  loadLibraryRelations,
  loadExecutorVault,
  searchLibraries,
  selectedProjectRef,
  setSelectedProjectRef,
  loadProjectDetail,
  projectDetailLoading,
  tablePages,
  tablePageSize,
  changeTablePage,
}) {
  const [financePage, setFinancePage] = useState(1);
  const [detailType, setDetailType] = useState("regulation");
  const [detailID, setDetailID] = useState("");
  const [relationLimit, setRelationLimit] = useState("30");
  const [vaultExecutorRef, setVaultExecutorRef] = useState("");
  const [searchKeyword, setSearchKeyword] = useState("");
  const [searchType, setSearchType] = useState("");
  const [searchStatus, setSearchStatus] = useState("");
  const [searchHasExecutor, setSearchHasExecutor] = useState("");
  const [searchUpdatedFrom, setSearchUpdatedFrom] = useState("");
  const [searchUpdatedTo, setSearchUpdatedTo] = useState("");
  const [searchPage, setSearchPage] = useState(1);

  const metricValue = (key, rows) => {
    const total = Number(dashboard?.totals?.[key]);
    if (Number.isFinite(total) && total >= 0) return total;
    return Array.isArray(rows) ? rows.length : 0;
  };

  const financeRows = useMemo(
    () => mergeFinanceRows(dashboard.gatherings, dashboard.invoices, dashboard.settlements),
    [dashboard.gatherings, dashboard.invoices, dashboard.settlements],
  );
  const financePageSize = Math.max(1, Number(tablePageSize) || 20);
  const financeTotalPages = Math.max(1, Math.ceil(financeRows.length / financePageSize) || 1);

  useEffect(() => {
    if (financePage > financeTotalPages) {
      setFinancePage(financeTotalPages);
    }
  }, [financePage, financeTotalPages]);

  const financePageRows = useMemo(() => {
    const start = (financePage - 1) * financePageSize;
    return financeRows.slice(start, start + financePageSize);
  }, [financeRows, financePage, financePageSize]);

  useEffect(() => {
    if (vaultExecutorRef.trim()) return;
    const first = dashboard.libraryQualifications.find((it) => {
      const ref = String(pickField(it, ["executor_ref", "ExecutorRef"], "")).trim();
      return Boolean(ref);
    });
    if (!first) return;
    setVaultExecutorRef(String(pickField(first, ["executor_ref", "ExecutorRef"], "")));
  }, [dashboard.libraryQualifications, vaultExecutorRef]);

  const searchPageSize = 20;
  const searchTotal = Number.isFinite(Number(librarySearch?.total)) ? Number(librarySearch.total) : 0;
  const searchTotalPages = Math.max(1, Math.ceil(searchTotal / searchPageSize) || 1);
  const searchCurrentPage = Math.min(Math.max(1, searchPage), searchTotalPages);

  const runResourceSearch = (targetPage = 1) => {
    const page = Math.max(1, Number(targetPage) || 1);
    setSearchPage(page);
    searchLibraries({
      keyword: searchKeyword,
      type: searchType,
      status: searchStatus,
      hasExecutor: searchHasExecutor,
      updatedFrom: searchUpdatedFrom,
      updatedTo: searchUpdatedTo,
      limit: searchPageSize,
      offset: (page - 1) * searchPageSize,
    });
  };

  const openLibraryDetail = (type, id) => {
    const normalizedType = String(type || "").trim();
    const normalizedID = Number(id);
    if (!normalizedType || !Number.isFinite(normalizedID) || normalizedID <= 0) return;
    setDetailType(normalizedType);
    setDetailID(String(Math.trunc(normalizedID)));
    loadLibraryDetail(normalizedType, normalizedID);
  };

  const openLibraryRelations = (type, id) => {
    const normalizedType = String(type || "").trim();
    const normalizedID = Number(id);
    if (!normalizedType || !Number.isFinite(normalizedID) || normalizedID <= 0) return;
    setDetailType(normalizedType);
    setDetailID(String(Math.trunc(normalizedID)));
    loadLibraryRelations(normalizedType, normalizedID, relationLimit);
  };

  const openExecutorVault = (executorRef) => {
    const ref = String(executorRef || "").trim();
    if (!ref) return;
    setVaultExecutorRef(ref);
    loadExecutorVault(ref);
  };

  const searchRows = Array.isArray(librarySearch?.items) ? librarySearch.items : [];

  return (
    <section className="panel p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">联调数据看板（读取现有数据库数据）</h2>
          <p className="mt-1 text-xs text-slate">直接调用 design-institute 接口，展示当前 PG 已落库的业务数据。</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => loadDashboardData()}
            disabled={dashboardLoading}
            className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-medium text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {dashboardLoading ? "刷新中..." : "刷新看板"}
          </button>
        </div>
      </div>

      <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <MetricCard label="项目" value={metricValue("projects", dashboard.projects)} />
        <MetricCard label="合同" value={metricValue("contracts", dashboard.contracts)} />
        <MetricCard label="人员" value={metricValue("employees", dashboard.employees)} />
        <MetricCard label="资质" value={metricValue("qualifications", dashboard.qualifications)} />
        <MetricCard label="业绩" value={metricValue("achievements", dashboard.achievements)} />
        <MetricCard label="回款" value={metricValue("gatherings", dashboard.gatherings)} />
        <MetricCard label="发票" value={metricValue("invoices", dashboard.invoices)} />
        <MetricCard label="结算" value={metricValue("settlements", dashboard.settlements)} />
        <MetricCard label="资质库" value={metricValue("libraryQualifications", dashboard.libraryQualifications)} />
        <MetricCard label="工程标准库" value={metricValue("engineeringStandards", dashboard.engineeringStandards)} />
        <MetricCard label="法规库" value={metricValue("regulations", dashboard.regulations)} />
      </div>
      {dashboardError && (
        <pre className="mt-3 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">{dashboardError}</pre>
      )}

      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <DataTable
          title="项目"
          rows={dashboard.projects}
          totalCount={dashboard.totals.projects}
          page={tablePages.projects}
          pageSize={tablePageSize}
          onPageChange={(nextPage) => changeTablePage("projects", nextPage)}
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
          totalCount={dashboard.totals.contracts}
          page={tablePages.contracts}
          pageSize={tablePageSize}
          onPageChange={(nextPage) => changeTablePage("contracts", nextPage)}
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
          totalCount={dashboard.totals.employees}
          page={tablePages.employees}
          pageSize={tablePageSize}
          onPageChange={(nextPage) => changeTablePage("employees", nextPage)}
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
          totalCount={dashboard.totals.qualifications}
          page={tablePages.qualifications}
          pageSize={tablePageSize}
          onPageChange={(nextPage) => changeTablePage("qualifications", nextPage)}
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
          totalCount={dashboard.totals.achievements}
          page={tablePages.achievements}
          pageSize={tablePageSize}
          onPageChange={(nextPage) => changeTablePage("achievements", nextPage)}
          columns={[
            { key: "id", label: "ID", keys: ["id", "ID"] },
            { key: "spu_ref", label: "SPU", keys: ["spu_ref", "SpuRef"] },
            { key: "project_ref", label: "ProjectRef", keys: ["project_ref", "ProjectRef"] },
            { key: "executor_ref", label: "ExecutorRef", keys: ["executor_ref", "ExecutorRef"] },
          ]}
        />
        <DataTable
          title="财务流转"
          rows={financePageRows}
          totalCount={financeRows.length}
          page={financePage}
          pageSize={financePageSize}
          onPageChange={(nextPage) => setFinancePage(Math.min(Math.max(1, nextPage), financeTotalPages))}
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
        <div className="mb-3 text-sm font-medium">三库映射（旧系统落库）</div>
        <div className="grid gap-4 lg:grid-cols-2">
          <DataTable
            title="资质库"
            rows={dashboard.libraryQualifications}
            totalCount={dashboard.totals.libraryQualifications}
            page={tablePages.libraryQualifications}
            pageSize={tablePageSize}
            onPageChange={(nextPage) => changeTablePage("libraryQualifications", nextPage)}
            columns={[
              { key: "id", label: "ID", keys: ["id", "ID"] },
              { key: "qual_type", label: "类型", keys: ["qual_type", "QualType"] },
              { key: "holder_name", label: "持有人", keys: ["holder_name", "HolderName"] },
              { key: "executor_ref", label: "ExecutorRef", keys: ["executor_ref", "ExecutorRef"] },
              { key: "status", label: "状态", keys: ["status", "Status"] },
              { key: "valid_until", label: "有效期", keys: ["valid_until", "ValidUntil"] },
            ]}
          />
          <DataTable
            title="工程标准库（图纸）"
            rows={dashboard.engineeringStandards}
            totalCount={dashboard.totals.engineeringStandards}
            page={tablePages.engineeringStandards}
            pageSize={tablePageSize}
            onPageChange={(nextPage) => changeTablePage("engineeringStandards", nextPage)}
            columns={[
              { key: "id", label: "ID", keys: ["id", "ID"] },
              { key: "drawing_no", label: "图纸编号", keys: ["drawing_no", "DrawingNo", "num", "Num"] },
              { key: "major", label: "专业", keys: ["major", "Major"] },
              { key: "status", label: "状态", keys: ["status", "Status", "state", "State"] },
              { key: "project_ref", label: "ProjectRef", keys: ["project_ref", "ProjectRef"] },
              { key: "attachment_count", label: "附件数", keys: ["attachment_count", "AttachmentCount"] },
            ]}
          />
          <DataTable
            title="法规库"
            rows={dashboard.regulations}
            totalCount={dashboard.totals.regulations}
            page={tablePages.regulations}
            pageSize={tablePageSize}
            onPageChange={(nextPage) => changeTablePage("regulations", nextPage)}
            columns={[
              { key: "id", label: "ID", keys: ["id", "ID"] },
              { key: "doc_no", label: "文号", keys: ["doc_no", "DocNo"] },
              { key: "title", label: "标题", keys: ["title", "Title"] },
              { key: "category", label: "分类", keys: ["category", "Category"] },
              { key: "publisher", label: "发布单位", keys: ["publisher", "Publisher"] },
              { key: "status", label: "状态", keys: ["status", "Status"] },
            ]}
          />
        </div>
      </div>

      <div className="mt-5 rounded-xl border border-slate-200 bg-slate-50 p-4">
        <div className="mb-3 text-sm font-medium">资源中心（全局搜索）</div>
        <div className="grid gap-2 lg:grid-cols-6">
          <input
            value={searchKeyword}
            onChange={(e) => setSearchKeyword(e.target.value)}
            placeholder="关键字（标题/文号/证书/执行体/项目）"
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs lg:col-span-2"
          />
          <select
            value={searchType}
            onChange={(e) => setSearchType(e.target.value)}
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
          >
            <option value="">全部库类型</option>
            <option value="qualification">资质库</option>
            <option value="standard">工程标准库</option>
            <option value="regulation">法规库</option>
          </select>
          <input
            value={searchStatus}
            onChange={(e) => setSearchStatus(e.target.value)}
            placeholder="状态（VALID/EFFECTIVE/...）"
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
          />
          <select
            value={searchHasExecutor}
            onChange={(e) => setSearchHasExecutor(e.target.value)}
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
          >
            <option value="">是否有关联执行体（全部）</option>
            <option value="true">有</option>
            <option value="false">无</option>
          </select>
          <button
            onClick={() => runResourceSearch(1)}
            disabled={librarySearchLoading}
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs disabled:cursor-not-allowed disabled:opacity-60"
          >
            {librarySearchLoading ? "搜索中..." : "搜索资源"}
          </button>
        </div>
        <div className="mt-2 grid gap-2 lg:grid-cols-6">
          <input
            value={searchUpdatedFrom}
            onChange={(e) => setSearchUpdatedFrom(e.target.value)}
            placeholder="更新时间起（YYYY-MM-DD 或 RFC3339）"
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs lg:col-span-3"
          />
          <input
            value={searchUpdatedTo}
            onChange={(e) => setSearchUpdatedTo(e.target.value)}
            placeholder="更新时间止（YYYY-MM-DD 或 RFC3339）"
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs lg:col-span-3"
          />
        </div>
        {librarySearchError ? (
          <pre className="mt-2 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">{librarySearchError}</pre>
        ) : null}
        <div className="mt-3 max-h-80 overflow-auto rounded-lg border border-slate-200 bg-white">
          <table className="w-full min-w-[980px] border-collapse text-left text-xs">
            <thead className="bg-slate-100 text-slate-700">
              <tr>
                <th className="border-b border-slate-200 px-2 py-2 font-medium">类型</th>
                <th className="border-b border-slate-200 px-2 py-2 font-medium">ID</th>
                <th className="border-b border-slate-200 px-2 py-2 font-medium">标题</th>
                <th className="border-b border-slate-200 px-2 py-2 font-medium">副标题</th>
                <th className="border-b border-slate-200 px-2 py-2 font-medium">状态</th>
                <th className="border-b border-slate-200 px-2 py-2 font-medium">执行体</th>
                <th className="border-b border-slate-200 px-2 py-2 font-medium">项目</th>
                <th className="border-b border-slate-200 px-2 py-2 font-medium">更新时间</th>
                <th className="border-b border-slate-200 px-2 py-2 font-medium">操作</th>
              </tr>
            </thead>
            <tbody>
              {searchRows.length === 0 && (
                <tr>
                  <td colSpan={9} className="px-2 py-4 text-center text-slate-500">
                    暂无搜索结果
                  </td>
                </tr>
              )}
              {searchRows.map((row, idx) => {
                const type = String(pickField(row, ["type", "Type"], ""));
                const id = Number(pickField(row, ["id", "ID"], 0));
                const title = String(pickField(row, ["title", "Title"], ""));
                const subtitle = String(pickField(row, ["subtitle", "Subtitle"], ""));
                const status = String(pickField(row, ["status", "Status"], ""));
                const executorRef = String(pickField(row, ["executor_ref", "executorRef", "ExecutorRef"], ""));
                const projectRef = String(pickField(row, ["project_ref", "projectRef", "ProjectRef"], ""));
                const updatedAt = String(pickField(row, ["updated_at", "updatedAt", "UpdatedAt"], ""));
                return (
                  <tr key={`library-search-${idx}`} className="odd:bg-white even:bg-slate-50">
                    <td className="border-b border-slate-100 px-2 py-2 align-top">
                      <code className="text-[11px] text-slate-700">{type}</code>
                    </td>
                    <td className="border-b border-slate-100 px-2 py-2 align-top">
                      <code className="text-[11px] text-slate-700">{Number.isFinite(id) ? id : ""}</code>
                    </td>
                    <td className="border-b border-slate-100 px-2 py-2 align-top">
                      <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">{title}</code>
                    </td>
                    <td className="border-b border-slate-100 px-2 py-2 align-top">
                      <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">{subtitle}</code>
                    </td>
                    <td className="border-b border-slate-100 px-2 py-2 align-top">
                      <code className="text-[11px] text-slate-700">{status}</code>
                    </td>
                    <td className="border-b border-slate-100 px-2 py-2 align-top">
                      <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">{executorRef}</code>
                    </td>
                    <td className="border-b border-slate-100 px-2 py-2 align-top">
                      <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">{projectRef}</code>
                    </td>
                    <td className="border-b border-slate-100 px-2 py-2 align-top">
                      <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">{updatedAt}</code>
                    </td>
                    <td className="border-b border-slate-100 px-2 py-2 align-top">
                      <div className="flex flex-wrap items-center gap-1">
                        <button
                          type="button"
                          onClick={() => openLibraryDetail(type, id)}
                          className="rounded border border-slate-300 px-2 py-1 text-[11px]"
                        >
                          查看详情
                        </button>
                        <button
                          type="button"
                          onClick={() => openLibraryRelations(type, id)}
                          className="rounded border border-slate-300 px-2 py-1 text-[11px]"
                        >
                          关系链
                        </button>
                        <button
                          type="button"
                          onClick={() => openExecutorVault(executorRef)}
                          disabled={!executorRef}
                          className="rounded border border-slate-300 px-2 py-1 text-[11px] disabled:cursor-not-allowed disabled:opacity-50"
                        >
                          查执行体
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div className="mt-2 flex items-center justify-end gap-2 text-xs">
          <button
            type="button"
            onClick={() => runResourceSearch(searchCurrentPage - 1)}
            disabled={librarySearchLoading || searchCurrentPage <= 1}
            className="rounded border border-slate-300 px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50"
          >
            上一页
          </button>
          <span className="text-slate-600">
            第 {searchCurrentPage}/{searchTotalPages} 页（{searchTotal}）
          </span>
          <button
            type="button"
            onClick={() => runResourceSearch(searchCurrentPage + 1)}
            disabled={librarySearchLoading || searchCurrentPage >= searchTotalPages}
            className="rounded border border-slate-300 px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50"
          >
            下一页
          </button>
        </div>
      </div>

      <div className="mt-5 rounded-xl border border-slate-200 bg-slate-50 p-4">
        <div className="mb-3 text-sm font-medium">三库统一详情 / 执行体证书仓库</div>
        <div className="grid gap-4 lg:grid-cols-2">
          <article className="rounded-xl border border-slate-200 bg-white p-3">
            <div className="mb-2 text-xs text-slate-600">统一详情查询（qualification / standard / regulation）</div>
            <div className="flex flex-wrap items-center gap-2">
              <select
                value={detailType}
                onChange={(e) => setDetailType(e.target.value)}
                className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
              >
                <option value="qualification">资质</option>
                <option value="standard">工程标准</option>
                <option value="regulation">法规</option>
              </select>
              <input
                value={detailID}
                onChange={(e) => setDetailID(e.target.value)}
                placeholder="输入ID"
                className="w-36 rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
              />
              <button
                onClick={() => loadLibraryDetail(detailType, detailID)}
                disabled={libraryDetailLoading}
                className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs disabled:cursor-not-allowed disabled:opacity-60"
              >
                {libraryDetailLoading ? "查询中..." : "查询详情"}
              </button>
              <input
                value={relationLimit}
                onChange={(e) => setRelationLimit(e.target.value)}
                placeholder="关系链数量"
                className="w-28 rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
              />
              <button
                onClick={() => loadLibraryRelations(detailType, detailID, relationLimit)}
                disabled={libraryRelationsLoading}
                className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs disabled:cursor-not-allowed disabled:opacity-60"
              >
                {libraryRelationsLoading ? "查询中..." : "查询关系链"}
              </button>
            </div>
            {libraryDetailError ? (
              <pre className="mt-2 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">{libraryDetailError}</pre>
            ) : null}
            <pre className="mt-2 min-h-40 overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100">
              {JSON.stringify(libraryDetail || { hint: "输入库类型+ID后查询详情" }, null, 2)}
            </pre>
          </article>
          <article className="rounded-xl border border-slate-200 bg-white p-3">
            <div className="mb-2 text-xs text-slate-600">执行体证书仓库（executor_ref）</div>
            <div className="flex flex-wrap items-center gap-2">
              <input
                value={vaultExecutorRef}
                onChange={(e) => setVaultExecutorRef(e.target.value)}
                placeholder="v://.../executor/..."
                className="min-w-80 rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
              />
              <button
                onClick={() => loadExecutorVault(vaultExecutorRef)}
                disabled={executorVaultLoading}
                className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs disabled:cursor-not-allowed disabled:opacity-60"
              >
                {executorVaultLoading ? "查询中..." : "查询仓库"}
              </button>
            </div>
            {executorVaultError ? (
              <pre className="mt-2 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">{executorVaultError}</pre>
            ) : null}
            <pre className="mt-2 min-h-40 overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100">
              {JSON.stringify(executorVault || { hint: "输入 executor_ref 后查询证书仓库" }, null, 2)}
            </pre>
          </article>
        </div>
        <article className="mt-4 rounded-xl border border-slate-200 bg-white p-3">
          <div className="mb-2 text-xs text-slate-600">关系链（上游/下游跳转）</div>
          {libraryRelationsError ? (
            <pre className="mt-2 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">{libraryRelationsError}</pre>
          ) : null}
          <pre className="mt-2 min-h-40 overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100">
            {JSON.stringify(libraryRelations || { hint: "输入库类型+ID后查询关系链" }, null, 2)}
          </pre>
        </article>
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
