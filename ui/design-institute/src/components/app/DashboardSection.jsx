import { useEffect, useMemo, useRef, useState } from "react";
import { DataTable, MetricCard, RelationGraphCanvas } from "./CommonUI";
import { mergeFinanceRows, pickField } from "./utils";

export default function DashboardSection({
  dashboard,
  dashboardLoading,
  dashboardError,
  libraryDetail,
  libraryDetailLoading,
  libraryDetailError,
  libraryChanges,
  libraryChangesLoading,
  libraryChangesError,
  libraryRelations,
  libraryRelationsLoading,
  libraryRelationsError,
  executorVault,
  executorVaultLoading,
  executorVaultError,
  libraryQuality,
  libraryQualityLoading,
  libraryQualityError,
  librarySearch,
  librarySearchLoading,
  librarySearchError,
  libraryViewerRole,
  setLibraryViewerRole,
  libraryViewerExecutorRef,
  setLibraryViewerExecutorRef,
  libraryIncludeHistory,
  setLibraryIncludeHistory,
  libraryValidOn,
  setLibraryValidOn,
  loadDashboardData,
  loadLibraryDetail,
  loadLibraryChanges,
  loadLibraryRelations,
  loadExecutorVault,
  loadLibrariesQuality,
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
  const [changesFrom, setChangesFrom] = useState("");
  const [changesTo, setChangesTo] = useState("");
  const [changesPage, setChangesPage] = useState(1);
  const [relationLimit, setRelationLimit] = useState("30");
  const [vaultExecutorRef, setVaultExecutorRef] = useState("");
  const [searchKeyword, setSearchKeyword] = useState("");
  const [searchType, setSearchType] = useState("");
  const [searchStatus, setSearchStatus] = useState("");
  const [searchHasExecutor, setSearchHasExecutor] = useState("");
  const [searchUpdatedFrom, setSearchUpdatedFrom] = useState("");
  const [searchUpdatedTo, setSearchUpdatedTo] = useState("");
  const [searchPage, setSearchPage] = useState(1);
  const [showRelationJSON, setShowRelationJSON] = useState(false);
  const [qualitySampleLimit, setQualitySampleLimit] = useState("20");
  const [selectedQualityCode, setSelectedQualityCode] = useState("");
  const detailSectionRef = useRef(null);
  const projectSectionRef = useRef(null);
  const focusTimerRef = useRef(null);
  const [focusedPanel, setFocusedPanel] = useState("");
  const [activeWorkspace, setActiveWorkspace] = useState("overview");
  const [compactMode, setCompactMode] = useState(true);
  const [showOverviewTables, setShowOverviewTables] = useState(false);
  const [locateMessage, setLocateMessage] = useState("");
  const [locateHistory, setLocateHistory] = useState([]);

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
  const qualityChecks = useMemo(
    () => (Array.isArray(libraryQuality?.checks) ? libraryQuality.checks : []),
    [libraryQuality],
  );

  useEffect(() => {
    if (vaultExecutorRef.trim()) return;
    const first = dashboard.libraryQualifications.find((it) => {
      const ref = String(pickField(it, ["executor_ref", "ExecutorRef"], "")).trim();
      return Boolean(ref);
    });
    if (!first) return;
    setVaultExecutorRef(String(pickField(first, ["executor_ref", "ExecutorRef"], "")));
  }, [dashboard.libraryQualifications, vaultExecutorRef]);

  useEffect(() => {
    if (!selectedQualityCode) return;
    const exists = qualityChecks.some(
      (check) => String(pickField(check, ["code", "Code"], "")).trim() === selectedQualityCode,
    );
    if (!exists) setSelectedQualityCode("");
  }, [qualityChecks, selectedQualityCode]);

  useEffect(
    () => () => {
      if (focusTimerRef.current) {
        clearTimeout(focusTimerRef.current);
        focusTimerRef.current = null;
      }
    },
    [],
  );

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

  const changesPageSize = Math.max(1, Number(pickField(libraryChanges, ["limit", "Limit"], 20)) || 20);
  const changesTotal = Number.isFinite(Number(pickField(libraryChanges, ["total", "Total"], 0)))
    ? Number(pickField(libraryChanges, ["total", "Total"], 0))
    : 0;
  const changesTotalPages = Math.max(1, Math.ceil(changesTotal / changesPageSize) || 1);
  const changesCurrentPage = Math.min(Math.max(1, changesPage), changesTotalPages);
  const changeRows = Array.isArray(pickField(libraryChanges, ["items", "Items"], []))
    ? pickField(libraryChanges, ["items", "Items"], [])
    : [];

  const openLibraryDetail = (type, id) => {
    const normalizedType = String(type || "").trim();
    const normalizedID = Number(id);
    if (!normalizedType || !Number.isFinite(normalizedID) || normalizedID <= 0) return;
    setActiveWorkspace("inspect");
    setDetailType(normalizedType);
    setDetailID(String(Math.trunc(normalizedID)));
    loadLibraryDetail(normalizedType, normalizedID);
  };

  const openLibraryRelations = (type, id) => {
    const normalizedType = String(type || "").trim();
    const normalizedID = Number(id);
    if (!normalizedType || !Number.isFinite(normalizedID) || normalizedID <= 0) return;
    setActiveWorkspace("inspect");
    setDetailType(normalizedType);
    setDetailID(String(Math.trunc(normalizedID)));
    loadLibraryRelations(normalizedType, normalizedID, relationLimit);
  };

  const openLibraryChanges = (type, id, targetPage = 1) => {
    const normalizedType = String(type || "").trim();
    const normalizedID = Number(id);
    if (!normalizedType || !Number.isFinite(normalizedID) || normalizedID <= 0) return;
    setActiveWorkspace("inspect");
    const page = Math.max(1, Number(targetPage) || 1);
    const offset = (page - 1) * changesPageSize;
    setDetailType(normalizedType);
    setDetailID(String(Math.trunc(normalizedID)));
    setChangesPage(page);
    loadLibraryChanges(normalizedType, normalizedID, {
      limit: changesPageSize,
      offset,
      from: changesFrom,
      to: changesTo,
    });
  };

  const openExecutorVault = (executorRef) => {
    const ref = String(executorRef || "").trim();
    if (!ref) return;
    setActiveWorkspace("inspect");
    setVaultExecutorRef(ref);
    loadExecutorVault(ref);
  };

  const searchRows = Array.isArray(librarySearch?.items) ? librarySearch.items : [];
  const relationGraph = useMemo(() => {
    if (!libraryRelations || typeof libraryRelations !== "object") return null;
    if (libraryRelations.data && typeof libraryRelations.data === "object") return libraryRelations.data;
    return libraryRelations;
  }, [libraryRelations]);
  const relationNodes = Array.isArray(relationGraph?.nodes) ? relationGraph.nodes : [];
  const relationEdges = Array.isArray(relationGraph?.edges) ? relationGraph.edges : [];
  const qualityStatus = String(pickField(libraryQuality, ["status", "Status"], "UNKNOWN")).toUpperCase();
  const selectedQualityCheck = useMemo(() => {
    if (!selectedQualityCode) return null;
    return (
      qualityChecks.find((check) => String(pickField(check, ["code", "Code"], "")).trim() === selectedQualityCode) ||
      null
    );
  }, [qualityChecks, selectedQualityCode]);
  const selectedQualitySamples = useMemo(() => {
    if (!selectedQualityCheck || typeof selectedQualityCheck !== "object") return [];
    const raw = pickField(selectedQualityCheck, ["samples", "Samples"], []);
    return Array.isArray(raw) ? raw : [];
  }, [selectedQualityCheck]);
  const qualityStatusClass = (() => {
    if (qualityStatus === "GREEN") return "border-emerald-200 bg-emerald-50 text-emerald-700";
    if (qualityStatus === "YELLOW") return "border-amber-200 bg-amber-50 text-amber-700";
    if (qualityStatus === "RED") return "border-rose-200 bg-rose-50 text-rose-700";
    return "border-slate-200 bg-slate-100 text-slate-700";
  })();

  const handleRelationNodeClick = (node) => {
    if (!node || typeof node !== "object") return;
    const ref = String(pickField(node, ["ref", "Ref"], "")).trim();
    const nodeType = String(pickField(node, ["node_type", "nodeType"], ""))
      .trim()
      .toLowerCase();
    const rawLibraryType = String(pickField(node, ["library_type", "libraryType"], ""))
      .trim()
      .toLowerCase();
    let normalizedLibraryType = rawLibraryType;
    if (!normalizedLibraryType) {
      if (nodeType.includes("qualification")) normalizedLibraryType = "qualification";
      if (nodeType.includes("standard") || nodeType.includes("drawing")) normalizedLibraryType = "standard";
      if (nodeType === "regulation") normalizedLibraryType = "regulation";
    }
    const id = Number(pickField(node, ["id", "ID"], 0));

    if (nodeType.includes("executor") || ref.includes("/executor/")) {
      openExecutorVault(ref);
      return;
    }
    if (nodeType.includes("project") && ref) {
      setSelectedProjectRef(ref);
      loadProjectDetail(ref);
      return;
    }
    if (["qualification", "standard", "regulation"].includes(normalizedLibraryType) && Number.isFinite(id) && id > 0) {
      openLibraryDetail(normalizedLibraryType, id);
      openLibraryRelations(normalizedLibraryType, id);
      openLibraryChanges(normalizedLibraryType, id, 1);
    }
  };

  const inferQualitySampleTarget = (sample, codeHint = selectedQualityCode) => {
    if (!sample || typeof sample !== "object")
      return { libraryType: "", libraryID: 0, projectRef: "", executorRef: "" };
    const projectRef = String(pickField(sample, ["project_ref", "projectRef", "ProjectRef"], "")).trim();
    const executorRef = String(pickField(sample, ["executor_ref", "executorRef", "ExecutorRef"], "")).trim();
    let libraryType = String(pickField(sample, ["library_type", "libraryType"], ""))
      .trim()
      .toLowerCase();
    let libraryID = Number(pickField(sample, ["library_id", "libraryId"], 0));
    if (
      (!libraryType || !Number.isFinite(libraryID) || libraryID <= 0) &&
      Number.isFinite(Number(sample.qualification_id))
    ) {
      libraryType = "qualification";
      libraryID = Number(sample.qualification_id);
    }
    if ((!libraryType || !Number.isFinite(libraryID) || libraryID <= 0) && codeHint === "EMPTY_EXECUTOR_REF") {
      libraryType = "qualification";
      libraryID = Number(pickField(sample, ["id", "ID"], 0));
    }
    if ((!libraryType || !Number.isFinite(libraryID) || libraryID <= 0) && codeHint === "REGULATION_WITHOUT_VERSION") {
      libraryType = "regulation";
      libraryID = Number(pickField(sample, ["id", "ID"], 0));
    }
    return {
      libraryType,
      libraryID: Number.isFinite(libraryID) && libraryID > 0 ? Math.trunc(libraryID) : 0,
      projectRef,
      executorRef,
    };
  };

  const scrollToSection = (ref) => {
    if (!ref || !ref.current || typeof ref.current.scrollIntoView !== "function") return;
    ref.current.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  const triggerPanelFocus = (panel) => {
    setFocusedPanel(panel);
    if (focusTimerRef.current) {
      clearTimeout(focusTimerRef.current);
      focusTimerRef.current = null;
    }
    focusTimerRef.current = setTimeout(() => {
      setFocusedPanel("");
      focusTimerRef.current = null;
    }, 2600);
  };

  const summarizeLocateTarget = (target) => {
    if (!target || typeof target !== "object") return "";
    const segments = [];
    if (
      ["qualification", "standard", "regulation"].includes(target.libraryType) &&
      Number.isFinite(target.libraryID) &&
      target.libraryID > 0
    ) {
      segments.push(`${target.libraryType}#${target.libraryID}`);
    }
    if (target.projectRef) segments.push(`project:${target.projectRef}`);
    if (target.executorRef) segments.push(`executor:${target.executorRef}`);
    return segments.join(" | ");
  };

  const executeLocateTarget = (target) => {
    if (!target || typeof target !== "object") return { ok: false, actions: [] };
    const actions = [];
    const canOpenDetail =
      ["qualification", "standard", "regulation"].includes(target.libraryType) &&
      Number.isFinite(target.libraryID) &&
      target.libraryID > 0;
    let focusPanel = "";

    if (canOpenDetail) {
      openLibraryDetail(target.libraryType, target.libraryID);
      openLibraryRelations(target.libraryType, target.libraryID);
      openLibraryChanges(target.libraryType, target.libraryID, 1);
      actions.push("详情/关系链/变更记录");
      focusPanel = "detail";
    }
    if (target.executorRef) {
      openExecutorVault(target.executorRef);
      actions.push("执行体证书仓库");
      if (!canOpenDetail) {
        focusPanel = "detail";
      }
    }
    if (target.projectRef) {
      setSelectedProjectRef(target.projectRef);
      loadProjectDetail(target.projectRef);
      actions.push("项目资源");
      if (!canOpenDetail && !target.executorRef) {
        focusPanel = "project";
      }
    }
    if (focusPanel === "detail") {
      setActiveWorkspace("inspect");
      scrollToSection(detailSectionRef);
      triggerPanelFocus("detail");
    }
    if (focusPanel === "project") {
      setActiveWorkspace("project");
      scrollToSection(projectSectionRef);
      triggerPanelFocus("project");
    }
    return { ok: actions.length > 0, actions };
  };

  const appendLocateHistory = (entry) => {
    if (!entry || typeof entry !== "object") return;
    setLocateHistory((previous) => {
      const key = String(entry.key || "");
      const head = key ? previous.filter((item) => String(item.key || "") !== key) : previous;
      return [entry, ...head].slice(0, 10);
    });
  };

  const locateQualitySample = (sample) => {
    const target = inferQualitySampleTarget(sample);
    const result = executeLocateTarget(target);
    const targetSummary = summarizeLocateTarget(target);
    const code = String(selectedQualityCode || "").trim();
    const historyEntry = {
      id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
      key: `${code}|${targetSummary}`,
      code,
      target,
      targetSummary,
      actions: result.actions,
      locatedAt: new Date().toISOString(),
    };
    if (!result.ok) {
      setLocateMessage("该样本缺少可定位字段（library_id / project_ref / executor_ref）。");
      return;
    }
    appendLocateHistory(historyEntry);
    setLocateMessage(`已定位：${result.actions.join(" / ")}${targetSummary ? ` · ${targetSummary}` : ""}`);
  };

  const replayLocateHistory = (item) => {
    if (!item || typeof item !== "object") return;
    const target = item.target && typeof item.target === "object" ? item.target : null;
    const result = executeLocateTarget(target);
    if (!result.ok) {
      setLocateMessage("该历史记录无法重放，目标字段不完整。");
      return;
    }
    const targetSummary = summarizeLocateTarget(target);
    setLocateMessage(`已重放：${result.actions.join(" / ")}${targetSummary ? ` · ${targetSummary}` : ""}`);
  };

  const formatLocateTime = (timeValue) => {
    if (!timeValue) return "-";
    const date = new Date(timeValue);
    if (Number.isNaN(date.getTime())) return String(timeValue);
    return date.toLocaleString();
  };

  const formatDashboardTime = (timeValue) => {
    if (!timeValue) return "-";
    const date = new Date(timeValue);
    if (Number.isNaN(date.getTime())) return String(timeValue);
    return date.toLocaleString();
  };
  const lastRefreshText = formatDashboardTime(dashboard?.updatedAt);
  const workspaceBadge = {
    overview: `${metricValue("projects", dashboard.projects)} 项目`,
    libraries: `${metricValue("engineeringStandards", dashboard.engineeringStandards)} 标准`,
    quality: `${Number(pickField(libraryQuality, ["failed_checks", "failedChecks"], 0))} 失败`,
    inspect: `${relationNodes.length}/${relationEdges.length}`,
    project: `${Array.isArray(dashboard?.qualificationAssignments) ? dashboard.qualificationAssignments.length : 0} 关联`,
  };

  const workspaceTabs = [
    { key: "overview", label: "总览数据", hint: "业务表格与核心指标" },
    { key: "libraries", label: "三库映射", hint: "资质库/工程标准库/法规库" },
    { key: "quality", label: "质量与搜索", hint: "质量闸门与全局检索" },
    { key: "inspect", label: "详情与关系链", hint: "详情/变更/关系链/执行体仓库" },
    { key: "project", label: "项目资源", hint: "项目维度资源回读" },
  ];
  const recordFootprint = [
    metricValue("projects", dashboard.projects),
    metricValue("contracts", dashboard.contracts),
    metricValue("employees", dashboard.employees),
    metricValue("qualifications", dashboard.qualifications),
    metricValue("achievements", dashboard.achievements),
    metricValue("gatherings", dashboard.gatherings),
    metricValue("invoices", dashboard.invoices),
    metricValue("settlements", dashboard.settlements),
    metricValue("libraryQualifications", dashboard.libraryQualifications),
    metricValue("engineeringStandards", dashboard.engineeringStandards),
    metricValue("regulations", dashboard.regulations),
  ].reduce((sum, item) => sum + Number(item || 0), 0);
  const runtimeIndicators = [
    {
      key: "api",
      label: "接口状态",
      value: dashboardError ? "异常" : "稳定",
      tone: dashboardError ? "error" : "ok",
    },
    {
      key: "quality",
      label: "质量闸门",
      value: qualityStatus,
      tone: qualityStatus === "RED" ? "error" : qualityStatus === "YELLOW" ? "warn" : "ok",
    },
    {
      key: "relation",
      label: "关系链节点",
      value: String(relationNodes.length),
      tone: relationNodes.length === 0 ? "warn" : "neutral",
    },
    {
      key: "records",
      label: "已读记录",
      value: String(recordFootprint),
      tone: "neutral",
    },
  ];
  const tableMaxHeightClass = compactMode ? "max-h-44" : "max-h-64";

  return (
    <section className={`panel di-console-shell ${compactMode ? "p-4" : "p-6"}`}>
      <div className="di-console-hero">
        <div>
          <h2 className="di-console-title">设计院管理系统联调看板</h2>
          <p className="di-console-subtitle">
            统一读取 Design-Institute 接口与 PostgreSQL 落库数据，面向管理端联调与验收。
          </p>
          <div className="di-console-meta">
            <span>数据域: `cn.zhongbei`</span>
            <span>更新时间: {lastRefreshText}</span>
          </div>
        </div>
        <div className="di-console-actions">
          <button
            type="button"
            onClick={() => {
              setCompactMode((v) => !v);
              if (compactMode) setShowOverviewTables(true);
            }}
            className={`di-btn di-btn-secondary ${compactMode ? "is-active" : ""}`}
          >
            {compactMode ? "布局: 紧凑" : "布局: 标准"}
          </button>
          <button
            onClick={() => loadDashboardData()}
            disabled={dashboardLoading}
            className="di-btn di-btn-primary disabled:cursor-not-allowed disabled:opacity-60"
          >
            {dashboardLoading ? "刷新中..." : "刷新看板"}
          </button>
        </div>
      </div>

      <div className="di-runtime-strip mt-3">
        {runtimeIndicators.map((item) => (
          <article key={item.key} className={`di-runtime-card tone-${item.tone}`}>
            <div className="di-runtime-label">{item.label}</div>
            <div className="di-runtime-value">{item.value}</div>
          </article>
        ))}
      </div>

      <div className="di-workspace-nav is-sticky mt-4">
        <div className="grid gap-2 md:grid-cols-3 xl:grid-cols-5">
          {workspaceTabs.map((tab) => {
            const active = activeWorkspace === tab.key;
            return (
              <button
                key={tab.key}
                type="button"
                onClick={() => setActiveWorkspace(tab.key)}
                className={`di-workspace-tab ${active ? "is-active" : ""}`}
              >
                <div className="di-workspace-tab-title">{tab.label}</div>
                <div className="di-workspace-tab-hint">{tab.hint}</div>
                <div className="di-workspace-tab-badge">{workspaceBadge[tab.key] || "-"}</div>
              </button>
            );
          })}
        </div>
      </div>

      {dashboardLoading && (
        <div className="di-loading-line mt-3" role="status" aria-live="polite" aria-label="看板加载中">
          <span />
        </div>
      )}
      {dashboardError && (
        <pre className="mt-3 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">{dashboardError}</pre>
      )}

      {activeWorkspace === "overview" && (
        <>
          <div className="di-inline-head mt-4">
            <div>
              <h3 className="di-inline-title">核心业务指标</h3>
              <p className="di-inline-hint">总览当前项目、履约、财务和三库落库数据。</p>
            </div>
            <span className="di-inline-badge">记录总量 {recordFootprint}</span>
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
            <MetricCard
              label="工程标准库"
              value={metricValue("engineeringStandards", dashboard.engineeringStandards)}
            />
            <MetricCard label="法规库" value={metricValue("regulations", dashboard.regulations)} />
          </div>
          <div className="mt-3 flex items-center justify-end gap-2">
            <button
              type="button"
              onClick={() => loadDashboardData()}
              disabled={dashboardLoading}
              className="di-btn di-btn-primary di-btn-xs disabled:cursor-not-allowed disabled:opacity-60"
            >
              {dashboardLoading ? "刷新中..." : "刷新总览数据"}
            </button>
            <button
              type="button"
              onClick={() => setShowOverviewTables((v) => !v)}
              className="di-btn di-btn-muted di-btn-xs"
            >
              {showOverviewTables ? "折叠总览明细" : "展开总览明细"}
            </button>
          </div>
          {showOverviewTables ? (
            <div className="mt-4 grid gap-4 lg:grid-cols-2">
              <DataTable
                title="项目"
                rows={dashboard.projects}
                totalCount={dashboard.totals.projects}
                page={tablePages.projects}
                pageSize={tablePageSize}
                maxHeightClass={tableMaxHeightClass}
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
                maxHeightClass={tableMaxHeightClass}
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
                maxHeightClass={tableMaxHeightClass}
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
                maxHeightClass={tableMaxHeightClass}
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
                maxHeightClass={tableMaxHeightClass}
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
                maxHeightClass={tableMaxHeightClass}
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
          ) : (
            <div className="mt-3 rounded-lg border border-dashed border-slate-300 bg-slate-50 px-3 py-4 text-xs text-slate-600">
              总览明细已折叠。点击“展开总览明细”可查看项目/合同/人员/资质/业绩/财务流转表。
            </div>
          )}
        </>
      )}

      {activeWorkspace === "libraries" && (
        <div className="di-section-surface mt-5">
          <div className="di-section-title">三库映射（旧系统落库）</div>
          <div className="grid gap-4 lg:grid-cols-2">
            <DataTable
              title="资质库"
              rows={dashboard.libraryQualifications}
              totalCount={dashboard.totals.libraryQualifications}
              page={tablePages.libraryQualifications}
              pageSize={tablePageSize}
              maxHeightClass={tableMaxHeightClass}
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
              maxHeightClass={tableMaxHeightClass}
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
              maxHeightClass={tableMaxHeightClass}
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
      )}

      {activeWorkspace === "quality" && (
        <div className="di-section-surface mt-5">
          <div className="di-section-title">资源中心（全局搜索）</div>
          <div className="grid gap-4 xl:grid-cols-[360px_minmax(0,1fr)]">
            <div className="space-y-3 xl:sticky xl:top-4 xl:self-start">
              <div className="rounded-xl border border-slate-200 bg-white p-3">
                <div className="mb-2 text-xs text-slate-600">访问视角（权限分层）</div>
                <div className="grid gap-2">
                  <select
                    value={libraryViewerRole}
                    onChange={(e) => setLibraryViewerRole(e.target.value)}
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
                  >
                    <option value="admin">管理端（全量）</option>
                    <option value="manager">管理端（经理）</option>
                    <option value="executor">执行体（仅本人）</option>
                  </select>
                  <input
                    value={libraryViewerExecutorRef}
                    onChange={(e) => setLibraryViewerExecutorRef(e.target.value)}
                    placeholder="viewer_executor_ref（执行体必填）"
                    disabled={libraryViewerRole !== "executor"}
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs disabled:cursor-not-allowed disabled:bg-slate-100"
                  />
                  <div className="text-[11px] text-slate-500">
                    当前视角会自动附加到三库列表、详情、关系链、搜索和执行体仓库接口。
                  </div>
                </div>
                <div className="mt-2 grid gap-2">
                  <label className="flex items-center gap-2 rounded-lg border border-slate-300 bg-slate-50 px-3 py-2 text-xs">
                    <input
                      type="checkbox"
                      checked={Boolean(libraryIncludeHistory)}
                      onChange={(e) => setLibraryIncludeHistory(Boolean(e.target.checked))}
                    />
                    历史追溯（include_history=true）
                  </label>
                  <input
                    value={libraryValidOn}
                    onChange={(e) => setLibraryValidOn(e.target.value)}
                    placeholder="valid_on（YYYY-MM-DD / RFC3339）"
                    disabled={Boolean(libraryIncludeHistory)}
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs disabled:cursor-not-allowed disabled:bg-slate-100"
                  />
                  <div className="text-[11px] text-slate-500">
                    默认仅看当前有效版本；可指定 valid_on 做时间点追溯。勾选“历史追溯”后返回历史版本集合。
                  </div>
                </div>
              </div>
              <article className="rounded-xl border border-slate-200 bg-white p-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-slate-600">数据质量闸门</span>
                    <span className={`rounded border px-2 py-1 text-[11px] font-medium ${qualityStatusClass}`}>
                      {qualityStatus}
                    </span>
                    <span className="text-[11px] text-slate-500">
                      检查项 {Number(pickField(libraryQuality, ["total_checks", "totalChecks"], qualityChecks.length))}{" "}
                      / 失败 {Number(pickField(libraryQuality, ["failed_checks", "failedChecks"], 0))} / 预警{" "}
                      {Number(pickField(libraryQuality, ["warning_checks", "warningChecks"], 0))}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    <input
                      value={qualitySampleLimit}
                      onChange={(e) => setQualitySampleLimit(e.target.value)}
                      placeholder="样本条数"
                      className="w-24 rounded-lg border border-slate-300 bg-white px-2 py-1 text-xs"
                    />
                    <button
                      type="button"
                      onClick={() => loadLibrariesQuality(qualitySampleLimit)}
                      disabled={libraryQualityLoading}
                      className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {libraryQualityLoading ? "检查中..." : "执行检查"}
                    </button>
                  </div>
                </div>
                {libraryQualityError ? (
                  <pre className="mt-2 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">
                    {libraryQualityError}
                  </pre>
                ) : null}
                {qualityStatus === "RED" ? (
                  <div className="mt-2 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">
                    当前三库查询启用了阻断模式（`quality_gate=block`），失败项未清理前，三库列表接口可能返回 409。
                  </div>
                ) : null}
                <div
                  className={`mt-2 overflow-auto rounded-lg border border-slate-200 ${compactMode ? "max-h-44" : "max-h-52"}`}
                >
                  <table className="w-full border-collapse text-left text-xs">
                    <thead className="bg-slate-100 text-slate-700">
                      <tr>
                        <th className="border-b border-slate-200 px-2 py-2 font-medium">检查项</th>
                        <th className="border-b border-slate-200 px-2 py-2 font-medium">级别</th>
                        <th className="border-b border-slate-200 px-2 py-2 font-medium">状态</th>
                        <th className="border-b border-slate-200 px-2 py-2 font-medium">数量</th>
                        <th className="border-b border-slate-200 px-2 py-2 font-medium">说明</th>
                        <th className="border-b border-slate-200 px-2 py-2 font-medium">样本</th>
                      </tr>
                    </thead>
                    <tbody>
                      {qualityChecks.length === 0 ? (
                        <tr>
                          <td colSpan={6} className="px-2 py-3 text-center text-slate-500">
                            暂无质量检查数据
                          </td>
                        </tr>
                      ) : (
                        qualityChecks.map((check, idx) => {
                          const status = String(pickField(check, ["status", "Status"], "")).toUpperCase();
                          const code = String(pickField(check, ["code", "Code"], "")).trim();
                          const samplesRaw = pickField(check, ["samples", "Samples"], []);
                          const samples = Array.isArray(samplesRaw) ? samplesRaw : [];
                          const selected = selectedQualityCode === code;
                          return (
                            <tr key={`quality-check-${idx}`} className="odd:bg-white even:bg-slate-50">
                              <td className="border-b border-slate-100 px-2 py-2 align-top">
                                <code className="text-[11px] text-slate-700">{code}</code>
                              </td>
                              <td className="border-b border-slate-100 px-2 py-2 align-top">
                                <code className="text-[11px] text-slate-700">
                                  {String(pickField(check, ["severity", "Severity"], ""))}
                                </code>
                              </td>
                              <td className="border-b border-slate-100 px-2 py-2 align-top">
                                <code
                                  className={
                                    status === "FAIL" ? "text-[11px] text-rose-600" : "text-[11px] text-slate-700"
                                  }
                                >
                                  {status}
                                </code>
                              </td>
                              <td className="border-b border-slate-100 px-2 py-2 align-top">
                                <code className="text-[11px] text-slate-700">
                                  {Number(pickField(check, ["count", "Count"], 0))}
                                </code>
                              </td>
                              <td className="border-b border-slate-100 px-2 py-2 align-top">
                                <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">
                                  {String(pickField(check, ["message", "Message"], ""))}
                                </code>
                              </td>
                              <td className="border-b border-slate-100 px-2 py-2 align-top">
                                <button
                                  type="button"
                                  onClick={() => setSelectedQualityCode(selected ? "" : code)}
                                  disabled={samples.length === 0}
                                  className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-40"
                                >
                                  {samples.length > 0 ? `样本(${samples.length})` : "无"}
                                </button>
                              </td>
                            </tr>
                          );
                        })
                      )}
                    </tbody>
                  </table>
                </div>
                {selectedQualityCheck ? (
                  <div className="mt-2 space-y-2">
                    <div className="text-xs text-slate-600">
                      当前样本：{String(pickField(selectedQualityCheck, ["code", "Code"], ""))}（
                      {selectedQualitySamples.length}）
                    </div>
                    {locateMessage ? (
                      <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-700">
                        {locateMessage}
                      </div>
                    ) : null}
                    <div className="rounded-lg border border-slate-200 bg-white p-2">
                      <div className="mb-2 flex items-center justify-between">
                        <span className="text-[11px] text-slate-600">定位历史（最近10条）</span>
                        <button
                          type="button"
                          onClick={() => setLocateHistory([])}
                          disabled={locateHistory.length === 0}
                          className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-40"
                        >
                          清空
                        </button>
                      </div>
                      {locateHistory.length === 0 ? (
                        <div className="rounded border border-dashed border-slate-200 px-2 py-2 text-[11px] text-slate-500">
                          暂无记录
                        </div>
                      ) : (
                        <div className={`space-y-1 overflow-auto ${compactMode ? "max-h-28" : "max-h-40"}`}>
                          {locateHistory.map((item) => (
                            <div key={item.id} className="rounded border border-slate-200 bg-slate-50 px-2 py-1">
                              <div className="flex items-center justify-between gap-2">
                                <div className="min-w-0">
                                  <div className="truncate text-[11px] text-slate-700">
                                    <code>{item.code || "UNKNOWN"}</code>
                                    <span className="mx-1 text-slate-400">|</span>
                                    <span>{item.targetSummary || "-"}</span>
                                  </div>
                                  <div className="text-[11px] text-slate-500">{formatLocateTime(item.locatedAt)}</div>
                                </div>
                                <button
                                  type="button"
                                  onClick={() => replayLocateHistory(item)}
                                  className="di-btn di-btn-secondary di-btn-xs"
                                >
                                  重放
                                </button>
                              </div>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                    {selectedQualitySamples.length === 0 ? (
                      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-500">
                        无样本数据
                      </div>
                    ) : (
                      selectedQualitySamples.map((sample, idx) => {
                        const target = inferQualitySampleTarget(sample);
                        const canOpenDetail =
                          ["qualification", "standard", "regulation"].includes(target.libraryType) &&
                          Number.isFinite(target.libraryID) &&
                          target.libraryID > 0;
                        return (
                          <article
                            key={`quality-sample-${idx}`}
                            className="rounded-lg border border-slate-200 bg-slate-50 p-2"
                          >
                            <div className="mb-2 flex flex-wrap gap-1">
                              <button
                                type="button"
                                onClick={() => locateQualitySample(sample)}
                                className="di-btn di-btn-secondary di-btn-xs"
                              >
                                一键定位
                              </button>
                              <button
                                type="button"
                                onClick={() => openLibraryDetail(target.libraryType, target.libraryID)}
                                disabled={!canOpenDetail}
                                className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-40"
                              >
                                查看详情
                              </button>
                              <button
                                type="button"
                                onClick={() => openLibraryRelations(target.libraryType, target.libraryID)}
                                disabled={!canOpenDetail}
                                className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-40"
                              >
                                关系链
                              </button>
                              <button
                                type="button"
                                onClick={() => openLibraryChanges(target.libraryType, target.libraryID, 1)}
                                disabled={!canOpenDetail}
                                className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-40"
                              >
                                变更记录
                              </button>
                              <button
                                type="button"
                                onClick={() => {
                                  if (!target.projectRef) return;
                                  setSelectedProjectRef(target.projectRef);
                                  loadProjectDetail(target.projectRef);
                                }}
                                disabled={!target.projectRef}
                                className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-40"
                              >
                                打开项目
                              </button>
                              <button
                                type="button"
                                onClick={() => openExecutorVault(target.executorRef)}
                                disabled={!target.executorRef}
                                className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-40"
                              >
                                执行体仓库
                              </button>
                            </div>
                            <pre
                              className={`overflow-auto rounded bg-slate-900 p-2 text-[11px] text-slate-100 ${compactMode ? "max-h-32" : "max-h-44"}`}
                            >
                              {JSON.stringify(sample, null, 2)}
                            </pre>
                          </article>
                        );
                      })
                    )}
                  </div>
                ) : null}
              </article>
            </div>
            <div className="space-y-3">
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
                  className="di-btn di-btn-muted disabled:cursor-not-allowed disabled:opacity-60"
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
                <pre className="mt-2 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">
                  {librarySearchError}
                </pre>
              ) : null}
              <div
                className={`mt-3 overflow-auto rounded-lg border border-slate-200 bg-white ${compactMode ? "max-h-64" : "max-h-80"}`}
              >
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
                            <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">
                              {executorRef}
                            </code>
                          </td>
                          <td className="border-b border-slate-100 px-2 py-2 align-top">
                            <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">
                              {projectRef}
                            </code>
                          </td>
                          <td className="border-b border-slate-100 px-2 py-2 align-top">
                            <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">
                              {updatedAt}
                            </code>
                          </td>
                          <td className="border-b border-slate-100 px-2 py-2 align-top">
                            <div className="flex flex-wrap items-center gap-1">
                              <button
                                type="button"
                                onClick={() => openLibraryDetail(type, id)}
                                className="di-btn di-btn-muted di-btn-xs"
                              >
                                查看详情
                              </button>
                              <button
                                type="button"
                                onClick={() => openLibraryRelations(type, id)}
                                className="di-btn di-btn-muted di-btn-xs"
                              >
                                关系链
                              </button>
                              <button
                                type="button"
                                onClick={() => openLibraryChanges(type, id, 1)}
                                className="di-btn di-btn-muted di-btn-xs"
                              >
                                变更记录
                              </button>
                              <button
                                type="button"
                                onClick={() => openExecutorVault(executorRef)}
                                disabled={!executorRef}
                                className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-50"
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
                  className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-50"
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
                  className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-50"
                >
                  下一页
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {activeWorkspace === "inspect" && (
        <div
          ref={detailSectionRef}
          className={`di-section-surface mt-5 transition ${
            focusedPanel === "detail" ? "ring-2 ring-emerald-400 ring-offset-2 ring-offset-white" : ""
          }`}
        >
          <div className="di-section-title">三库统一详情 / 执行体证书仓库</div>
          <div className="grid gap-4 xl:grid-cols-[360px_minmax(0,1fr)]">
            <aside className="space-y-3 xl:sticky xl:top-4 xl:self-start">
              <article className="rounded-xl border border-slate-200 bg-white p-3">
                <div className="mb-2 text-xs font-semibold text-slate-700">查询参数</div>
                <div className="space-y-2">
                  <select
                    value={detailType}
                    onChange={(e) => setDetailType(e.target.value)}
                    className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
                  >
                    <option value="qualification">资质</option>
                    <option value="standard">工程标准</option>
                    <option value="regulation">法规</option>
                  </select>
                  <input
                    value={detailID}
                    onChange={(e) => setDetailID(e.target.value)}
                    placeholder="输入 ID"
                    className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
                  />
                  <input
                    value={relationLimit}
                    onChange={(e) => setRelationLimit(e.target.value)}
                    placeholder="关系链数量（默认30）"
                    className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
                  />
                </div>
                <div className="mt-2 grid gap-2 sm:grid-cols-2 xl:grid-cols-1">
                  <button
                    onClick={() => loadLibraryDetail(detailType, detailID)}
                    disabled={libraryDetailLoading}
                    className="di-btn di-btn-primary disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {libraryDetailLoading ? "查询中..." : "查询详情"}
                  </button>
                  <button
                    onClick={() => loadLibraryRelations(detailType, detailID, relationLimit)}
                    disabled={libraryRelationsLoading}
                    className="di-btn di-btn-muted disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {libraryRelationsLoading ? "查询中..." : "查询关系链"}
                  </button>
                  <button
                    onClick={() => openLibraryChanges(detailType, detailID, 1)}
                    disabled={libraryChangesLoading}
                    className="di-btn di-btn-muted sm:col-span-2 xl:col-span-1 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {libraryChangesLoading ? "查询中..." : "查询变更记录"}
                  </button>
                </div>
                <div className="mt-2 grid gap-2">
                  <input
                    value={changesFrom}
                    onChange={(e) => setChangesFrom(e.target.value)}
                    placeholder="变更时间起 from（YYYY-MM-DD / RFC3339）"
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
                  />
                  <input
                    value={changesTo}
                    onChange={(e) => setChangesTo(e.target.value)}
                    placeholder="变更时间止 to（YYYY-MM-DD / RFC3339）"
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
                  />
                </div>
                {libraryDetailError ? (
                  <pre className="mt-2 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">
                    {libraryDetailError}
                  </pre>
                ) : null}
                {libraryChangesError ? (
                  <pre className="mt-2 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">
                    {libraryChangesError}
                  </pre>
                ) : null}
              </article>

              <article className="rounded-xl border border-slate-200 bg-white p-3">
                <div className="mb-2 text-xs font-semibold text-slate-700">执行体证书仓库（executor_ref）</div>
                <div className="space-y-2">
                  <input
                    value={vaultExecutorRef}
                    onChange={(e) => setVaultExecutorRef(e.target.value)}
                    placeholder="v://.../executor/..."
                    className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
                  />
                  <button
                    onClick={() => loadExecutorVault(vaultExecutorRef)}
                    disabled={executorVaultLoading}
                    className="di-btn di-btn-muted w-full disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {executorVaultLoading ? "查询中..." : "查询仓库"}
                  </button>
                </div>
                {executorVaultError ? (
                  <pre className="mt-2 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">
                    {executorVaultError}
                  </pre>
                ) : null}
              </article>
            </aside>

            <section className="space-y-3">
              <article className="rounded-xl border border-slate-200 bg-white p-3">
                <div className="mb-2 text-xs font-semibold text-slate-700">统一详情 JSON</div>
                <pre
                  className={`overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100 ${compactMode ? "max-h-64" : "max-h-80"}`}
                >
                  {JSON.stringify(libraryDetail || { hint: "输入库类型+ID后查询详情" }, null, 2)}
                </pre>
              </article>

              <article className="rounded-xl border border-slate-200 bg-white p-3">
                <div className="mb-2 text-xs text-slate-600">
                  变更记录（共 {changesTotal} 条，当前第 {changesCurrentPage}/{changesTotalPages} 页）
                </div>
                <div
                  className={`overflow-auto rounded border border-slate-200 bg-white ${compactMode ? "max-h-48" : "max-h-60"}`}
                >
                  <table className="w-full min-w-[760px] border-collapse text-left text-[11px]">
                    <thead className="bg-slate-100 text-slate-700">
                      <tr>
                        <th className="border-b border-slate-200 px-2 py-2 font-medium">时间</th>
                        <th className="border-b border-slate-200 px-2 py-2 font-medium">事件</th>
                        <th className="border-b border-slate-200 px-2 py-2 font-medium">来源</th>
                        <th className="border-b border-slate-200 px-2 py-2 font-medium">ID</th>
                        <th className="border-b border-slate-200 px-2 py-2 font-medium">摘要</th>
                      </tr>
                    </thead>
                    <tbody>
                      {changeRows.length === 0 ? (
                        <tr>
                          <td colSpan={5} className="px-2 py-3 text-center text-slate-500">
                            暂无变更记录
                          </td>
                        </tr>
                      ) : (
                        changeRows.map((row, idx) => {
                          const changedAt = String(pickField(row, ["changed_at", "changedAt", "ChangedAt"], ""));
                          const eventType = String(pickField(row, ["event_type", "eventType", "EventType"], ""));
                          const source = String(pickField(row, ["source", "Source"], ""));
                          const eventID = Number(pickField(row, ["id", "ID"], 0));
                          const summary = String(pickField(row, ["summary", "Summary"], ""));
                          return (
                            <tr key={`library-change-${idx}`} className="di-data-row odd:bg-white even:bg-slate-50">
                              <td className="border-b border-slate-100 px-2 py-2 align-top">
                                <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">
                                  {changedAt}
                                </code>
                              </td>
                              <td className="border-b border-slate-100 px-2 py-2 align-top">
                                <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">
                                  {eventType}
                                </code>
                              </td>
                              <td className="border-b border-slate-100 px-2 py-2 align-top">
                                <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">
                                  {source}
                                </code>
                              </td>
                              <td className="border-b border-slate-100 px-2 py-2 align-top">
                                <code className="text-[11px] text-slate-700">
                                  {Number.isFinite(eventID) ? eventID : ""}
                                </code>
                              </td>
                              <td className="border-b border-slate-100 px-2 py-2 align-top">
                                <code className="whitespace-pre-wrap break-all text-[11px] text-slate-700">
                                  {summary}
                                </code>
                              </td>
                            </tr>
                          );
                        })
                      )}
                    </tbody>
                  </table>
                </div>
                <div className="mt-2 flex items-center justify-end gap-2 text-xs">
                  <button
                    type="button"
                    onClick={() => openLibraryChanges(detailType, detailID, changesCurrentPage - 1)}
                    disabled={libraryChangesLoading || changesCurrentPage <= 1}
                    className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    上一页
                  </button>
                  <span className="text-slate-600">
                    第 {changesCurrentPage}/{changesTotalPages} 页（{changesTotal}）
                  </span>
                  <button
                    type="button"
                    onClick={() => openLibraryChanges(detailType, detailID, changesCurrentPage + 1)}
                    disabled={libraryChangesLoading || changesCurrentPage >= changesTotalPages}
                    className="di-btn di-btn-muted di-btn-xs disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    下一页
                  </button>
                </div>
              </article>

              <article className="rounded-xl border border-slate-200 bg-white p-3">
                <div className="mb-2 text-xs font-semibold text-slate-700">执行体仓库 JSON</div>
                <pre
                  className={`overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100 ${compactMode ? "max-h-56" : "max-h-72"}`}
                >
                  {JSON.stringify(executorVault || { hint: "输入 executor_ref 后查询证书仓库" }, null, 2)}
                </pre>
              </article>

              <article className="rounded-xl border border-slate-200 bg-white p-3">
                <div className="mb-2 flex flex-wrap items-center justify-between gap-2 text-xs text-slate-600">
                  <span>
                    关系链（上游/下游跳转） 节点 {relationNodes.length} / 关系 {relationEdges.length}
                  </span>
                  <button
                    type="button"
                    onClick={() => setShowRelationJSON((v) => !v)}
                    className="di-btn di-btn-muted di-btn-xs"
                  >
                    {showRelationJSON ? "隐藏 JSON" : "显示 JSON"}
                  </button>
                </div>
                {libraryRelationsError ? (
                  <pre className="mt-2 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">
                    {libraryRelationsError}
                  </pre>
                ) : null}
                <RelationGraphCanvas graph={relationGraph} onNodeClick={handleRelationNodeClick} />
                {showRelationJSON ? (
                  <pre
                    className={`mt-2 overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100 ${compactMode ? "max-h-56" : "max-h-72"}`}
                  >
                    {JSON.stringify(libraryRelations || { hint: "输入库类型+ID后查询关系链" }, null, 2)}
                  </pre>
                ) : null}
              </article>
            </section>
          </div>
        </div>
      )}

      {activeWorkspace === "project" && (
        <div
          ref={projectSectionRef}
          className={`di-section-surface mt-5 transition ${
            focusedPanel === "project" ? "ring-2 ring-emerald-400 ring-offset-2 ring-offset-white" : ""
          }`}
        >
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
              className="di-btn di-btn-muted disabled:cursor-not-allowed disabled:opacity-60"
            >
              {projectDetailLoading ? "读取中..." : "读取该项目资源"}
            </button>
          </div>
          {dashboard.updatedAt && <p className="mt-2 text-xs text-slate">最近刷新: {dashboard.updatedAt}</p>}
          <div className="mt-3 grid gap-3 lg:grid-cols-2">
            <pre
              className={`overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100 ${compactMode ? "min-h-28" : "min-h-40"}`}
            >
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
            <pre
              className={`overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100 ${compactMode ? "min-h-28" : "min-h-40"}`}
            >
              {JSON.stringify(dashboard.projectResources || { hint: "先刷新看板或选择项目后读取资源" }, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </section>
  );
}
