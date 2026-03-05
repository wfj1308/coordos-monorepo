import { useState } from "react";
import { apiRequest, normalizeListData, pickField, trimTrailingSlash } from "../components/app/utils";

const TABLE_PAGE_SIZE = 20;
const TABLE_KEYS = [
  "projects",
  "contracts",
  "employees",
  "qualifications",
  "achievements",
  "gatherings",
  "invoices",
  "settlements",
  "libraryQualifications",
  "engineeringStandards",
  "regulations",
];

function buildDefaultTablePages() {
  return TABLE_KEYS.reduce((acc, key) => {
    acc[key] = 1;
    return acc;
  }, {});
}

function buildDashboardData() {
  return {
    totals: {
      projects: 0,
      contracts: 0,
      employees: 0,
      qualifications: 0,
      achievements: 0,
      gatherings: 0,
      invoices: 0,
      settlements: 0,
      libraryQualifications: 0,
      engineeringStandards: 0,
      regulations: 0,
    },
    projects: [],
    contracts: [],
    employees: [],
    qualifications: [],
    achievements: [],
    gatherings: [],
    invoices: [],
    settlements: [],
    libraryQualifications: [],
    engineeringStandards: [],
    regulations: [],
    projectResources: null,
    qualificationAssignments: [],
    updatedAt: "",
  };
}

function buildLibraryQualityGate() {
  return {
    status: "UNKNOWN",
    total_checks: 0,
    failed_checks: 0,
    warning_checks: 0,
    checks: [],
    updated_at: "",
  };
}

function buildLibraryChanges() {
  return {
    total: 0,
    limit: TABLE_PAGE_SIZE,
    offset: 0,
    items: [],
    updated_at: "",
  };
}

function toPagedPath(path, page, pageSize) {
  const safePage = Math.max(1, Number(page) || 1);
  const safeSize = Math.max(1, Number(pageSize) || TABLE_PAGE_SIZE);
  const offset = (safePage - 1) * safeSize;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}limit=${safeSize}&offset=${offset}`;
}

export default function useDashboardData({ diBase, useAuth, token }) {
  const [dashboard, setDashboard] = useState(buildDashboardData());
  const [dashboardLoading, setDashboardLoading] = useState(false);
  const [dashboardError, setDashboardError] = useState("");
  const [libraryDetail, setLibraryDetail] = useState(null);
  const [libraryDetailLoading, setLibraryDetailLoading] = useState(false);
  const [libraryDetailError, setLibraryDetailError] = useState("");
  const [libraryChanges, setLibraryChanges] = useState(buildLibraryChanges());
  const [libraryChangesLoading, setLibraryChangesLoading] = useState(false);
  const [libraryChangesError, setLibraryChangesError] = useState("");
  const [libraryRelations, setLibraryRelations] = useState(null);
  const [libraryRelationsLoading, setLibraryRelationsLoading] = useState(false);
  const [libraryRelationsError, setLibraryRelationsError] = useState("");
  const [executorVault, setExecutorVault] = useState(null);
  const [executorVaultLoading, setExecutorVaultLoading] = useState(false);
  const [executorVaultError, setExecutorVaultError] = useState("");
  const [libraryQuality, setLibraryQuality] = useState(buildLibraryQualityGate());
  const [libraryQualityLoading, setLibraryQualityLoading] = useState(false);
  const [libraryQualityError, setLibraryQualityError] = useState("");
  const [librarySearch, setLibrarySearch] = useState({
    total: 0,
    limit: TABLE_PAGE_SIZE,
    offset: 0,
    items: [],
    updated_at: "",
  });
  const [librarySearchLoading, setLibrarySearchLoading] = useState(false);
  const [librarySearchError, setLibrarySearchError] = useState("");
  const [libraryViewerRole, setLibraryViewerRole] = useState("admin");
  const [libraryViewerExecutorRef, setLibraryViewerExecutorRef] = useState("");
  const [libraryIncludeHistory, setLibraryIncludeHistory] = useState(false);
  const [libraryValidOn, setLibraryValidOn] = useState("");
  const [selectedProjectRef, setSelectedProjectRef] = useState("");
  const [projectDetailLoading, setProjectDetailLoading] = useState(false);
  const [tablePages, setTablePages] = useState(buildDefaultTablePages());

  const normalizeListPayload = (data) => {
    const rows = normalizeListData(data);
    const totalRaw = pickField(data, ["total", "Total", "count", "Count"], rows.length);
    const total = Number.isFinite(Number(totalRaw)) ? Number(totalRaw) : rows.length;
    return {
      rows,
      total: total >= 0 ? total : rows.length,
    };
  };

  const normalizeQualityGatePayload = (data) => {
    const payload = data && typeof data === "object" ? data : {};
    const rawChecks = pickField(payload, ["checks", "Checks"], []);
    const checks = Array.isArray(rawChecks) ? rawChecks : [];
    const totalRaw = pickField(payload, ["total_checks", "totalChecks", "TotalChecks"], checks.length);
    const failedRaw = pickField(payload, ["failed_checks", "failedChecks", "FailedChecks"], 0);
    const warningRaw = pickField(payload, ["warning_checks", "warningChecks", "WarningChecks"], 0);
    return {
      status: String(pickField(payload, ["status", "Status"], "UNKNOWN")).toUpperCase() || "UNKNOWN",
      total_checks: Number.isFinite(Number(totalRaw)) ? Math.max(0, Number(totalRaw)) : checks.length,
      failed_checks: Number.isFinite(Number(failedRaw)) ? Math.max(0, Number(failedRaw)) : 0,
      warning_checks: Number.isFinite(Number(warningRaw)) ? Math.max(0, Number(warningRaw)) : 0,
      checks,
      updated_at: String(pickField(payload, ["updated_at", "updatedAt", "UpdatedAt"], "")),
    };
  };

  const fetchLibrariesQuality = async (sampleLimit = TABLE_PAGE_SIZE) => {
    const di = trimTrailingSlash(diBase.trim());
    if (!di) {
      throw new Error("Design-Ins Base URL 不能为空");
    }
    const limit = Math.max(1, Math.min(200, Number(sampleLimit) || TABLE_PAGE_SIZE));
    const res = await apiRequest({
      method: "GET",
      url: `${di}/api/v1/libraries/quality-gate?sample_limit=${limit}`,
      token: useAuth ? token : "",
    });
    return normalizeQualityGatePayload(res.data);
  };

  const applyLibraryScope = (qs) => {
    if (!qs || typeof qs.set !== "function") return;
    const role = String(libraryViewerRole || "admin")
      .trim()
      .toLowerCase();
    const normalizedRole = role === "executor" || role === "manager" ? role : "admin";
    qs.set("viewer_role", normalizedRole);
    if (normalizedRole === "executor") {
      const executorRef = String(libraryViewerExecutorRef || "").trim();
      if (executorRef) qs.set("viewer_executor_ref", executorRef);
    }
  };

  const applyLibraryVersionOptions = (qs) => {
    if (!qs || typeof qs.set !== "function") return;
    const includeHistory = Boolean(libraryIncludeHistory);
    qs.set("include_history", includeHistory ? "true" : "false");
    const validOn = String(libraryValidOn || "").trim();
    if (!includeHistory && validOn) {
      qs.set("valid_on", validOn);
    }
  };

  const validateLibraryScope = () => {
    const role = String(libraryViewerRole || "admin")
      .trim()
      .toLowerCase();
    if (role === "executor" && !String(libraryViewerExecutorRef || "").trim()) {
      throw new Error("执行体视角需要填写 viewer_executor_ref");
    }
  };

  const loadProjectDetail = async (projectRef) => {
    const di = trimTrailingSlash(diBase.trim());
    if (!di || !projectRef) return;

    setProjectDetailLoading(true);
    setDashboardError("");
    try {
      const [resourcesRes, assignmentsRes] = await Promise.all([
        apiRequest({
          method: "GET",
          url: `${di}/api/v1/projects/${encodeURIComponent(projectRef)}/resources`,
          token: useAuth ? token : "",
        }),
        apiRequest({
          method: "GET",
          url: `${di}/api/v1/projects/${encodeURIComponent(projectRef)}/qualification-assignments`,
          token: useAuth ? token : "",
        }),
      ]);

      setDashboard((prev) => ({
        ...prev,
        projectResources: resourcesRes.data,
        qualificationAssignments: normalizeListData(assignmentsRes.data),
        updatedAt: new Date().toISOString(),
      }));
    } catch (err) {
      setDashboardError(String(err));
    } finally {
      setProjectDetailLoading(false);
    }
  };

  const listWithFallback = async (path, warnings) => {
    const primary = trimTrailingSlash(diBase.trim());
    const direct = "http://127.0.0.1:8090";
    const authToken = useAuth ? token : "";

    const tryBase = async (base) => {
      const res = await apiRequest({
        method: "GET",
        url: `${base}${path}`,
        token: authToken,
      });
      return res.data;
    };

    const requestWithFallback = async () => {
      try {
        const primaryData = await tryBase(primary);
        if (primary.startsWith("/") && primary !== direct) {
          const primaryRows = normalizeListData(primaryData);
          if (primaryRows.length === 0) {
            const directData = await tryBase(direct);
            if (normalizeListData(directData).length > 0) {
              warnings.push(`${path}: 代理返回空，已回退直连 ${direct}`);
              return directData;
            }
          }
        }
        return primaryData;
      } catch (primaryErr) {
        if (primary.startsWith("/") && primary !== direct) {
          try {
            const directData = await tryBase(direct);
            warnings.push(`${path}: 代理失败，已回退直连 ${direct}`);
            return directData;
          } catch (directErr) {
            throw new Error(`${String(primaryErr)} | fallback: ${String(directErr)}`);
          }
        }
        throw primaryErr;
      }
    };

    const data = await requestWithFallback();
    return normalizeListPayload(data);
  };

  const loadDashboardData = async (pageOverrides = null) => {
    const di = trimTrailingSlash(diBase.trim());
    if (!di) {
      setDashboardError("Design-Ins Base URL 不能为空");
      return;
    }

    const activePages = pageOverrides ? { ...tablePages, ...pageOverrides } : tablePages;
    if (pageOverrides) {
      setTablePages(activePages);
    }

    setDashboardLoading(true);
    setDashboardError("");
    try {
      const warnings = [];
      const listJobs = [
        { key: "projects", path: "/api/v1/projects" },
        { key: "contracts", path: "/api/v1/contracts" },
        { key: "employees", path: "/api/v1/employees" },
        { key: "qualifications", path: "/api/v1/qualifications" },
        { key: "gatherings", path: "/api/v1/gatherings" },
        { key: "invoices", path: "/api/v1/invoices" },
        { key: "settlements", path: "/api/v1/settlements" },
      ];

      const settled = await Promise.allSettled(
        listJobs.map((job) => listWithFallback(toPagedPath(job.path, activePages[job.key], TABLE_PAGE_SIZE), warnings)),
      );

      const next = {
        totals: {
          projects: 0,
          contracts: 0,
          employees: 0,
          qualifications: 0,
          achievements: 0,
          gatherings: 0,
          invoices: 0,
          settlements: 0,
          libraryQualifications: 0,
          engineeringStandards: 0,
          regulations: 0,
        },
        projects: [],
        contracts: [],
        employees: [],
        qualifications: [],
        achievements: [],
        gatherings: [],
        invoices: [],
        settlements: [],
        libraryQualifications: [],
        engineeringStandards: [],
        regulations: [],
      };

      settled.forEach((result, idx) => {
        const key = listJobs[idx].key;
        if (result.status === "fulfilled") {
          next[key] = result.value.rows;
          next.totals[key] = result.value.total;
          return;
        }
        warnings.push(`${key}: ${String(result.reason)}`);
      });

      try {
        const achievementsPath = toPagedPath("/api/v1/achievements", activePages.achievements, TABLE_PAGE_SIZE);
        const achievementsPayload = await listWithFallback(achievementsPath, warnings);
        next.achievements = achievementsPayload.rows;
        next.totals.achievements = achievementsPayload.total;
      } catch (achErr) {
        try {
          const fallbackPage = Math.max(1, Number(activePages.achievements) || 1);
          const fallbackOffset = (fallbackPage - 1) * TABLE_PAGE_SIZE;
          const publicAchievementsRes = await apiRequest({
            method: "GET",
            url: `${di}/public/v1/achievements?limit=${TABLE_PAGE_SIZE}&offset=${fallbackOffset}`,
            token: useAuth ? token : "",
          });
          const parsed = normalizeListPayload(publicAchievementsRes.data);
          next.achievements = parsed.rows;
          next.totals.achievements = parsed.total;
          warnings.push("achievements: 私有接口返回错误，已降级使用 public/v1/achievements");
        } catch (publicErr) {
          warnings.push(`achievements: ${String(achErr)} | fallback: ${String(publicErr)}`);
          next.achievements = [];
          next.totals.achievements = 0;
        }
      }

      try {
        validateLibraryScope();
        const qualificationOffset = (Math.max(1, Number(activePages.libraryQualifications) || 1) - 1) * TABLE_PAGE_SIZE;
        const standardOffset = (Math.max(1, Number(activePages.engineeringStandards) || 1) - 1) * TABLE_PAGE_SIZE;
        const regulationOffset = (Math.max(1, Number(activePages.regulations) || 1) - 1) * TABLE_PAGE_SIZE;
        const libsParams = new URLSearchParams({
          qualification_limit: String(TABLE_PAGE_SIZE),
          qualification_offset: String(qualificationOffset),
          standard_limit: String(TABLE_PAGE_SIZE),
          standard_offset: String(standardOffset),
          regulation_limit: String(TABLE_PAGE_SIZE),
          regulation_offset: String(regulationOffset),
          quality_gate: "block",
          quality_sample_limit: String(TABLE_PAGE_SIZE),
        });
        applyLibraryScope(libsParams);
        applyLibraryVersionOptions(libsParams);
        const libsQuery = libsParams.toString();

        const libsPath = `/api/v1/reports/three-libraries?${libsQuery}`;
        const libsRes = await apiRequest({
          method: "GET",
          url: `${di}${libsPath}`,
          token: useAuth ? token : "",
        });
        const libsData = libsRes.data && typeof libsRes.data === "object" ? libsRes.data : {};

        const qualityPayload = pickField(libsData, ["qualifications", "Qualifications"], {});
        const standardsPayload = pickField(libsData, ["engineering_standards", "engineeringStandards"], {});
        const regulationsPayload = pickField(libsData, ["regulations", "Regulations"], {});

        const qualityParsed = normalizeListPayload(qualityPayload);
        const standardsParsed = normalizeListPayload(standardsPayload);
        const regulationsParsed = normalizeListPayload(regulationsPayload);

        next.libraryQualifications = qualityParsed.rows;
        next.totals.libraryQualifications = qualityParsed.total;
        next.engineeringStandards = standardsParsed.rows;
        next.totals.engineeringStandards = standardsParsed.total;
        next.regulations = regulationsParsed.rows;
        next.totals.regulations = regulationsParsed.total;
      } catch (libraryErr) {
        const msg = String(libraryErr || "");
        if (msg.includes("409") && msg.toLowerCase().includes("quality gate is red")) {
          warnings.push("three-libraries: 数据质量闸门为 RED，三库列表已阻断（409）；请先处理上方失败检查项。");
        } else {
          warnings.push(`three-libraries: ${msg}`);
        }
      }

      try {
        const qualityGate = await fetchLibrariesQuality(TABLE_PAGE_SIZE);
        setLibraryQuality(qualityGate);
        setLibraryQualityError("");
      } catch (qualityErr) {
        setLibraryQuality(buildLibraryQualityGate());
        setLibraryQualityError(String(qualityErr));
        warnings.push(`quality-gate: ${String(qualityErr)}`);
      }

      let pickedProjectRef = selectedProjectRef;
      if (!pickedProjectRef) {
        pickedProjectRef = String(pickField(next.projects[0], ["ref", "Ref"], ""));
      }
      setSelectedProjectRef(pickedProjectRef);

      let projectResources = null;
      let qualificationAssignments = [];
      if (pickedProjectRef) {
        try {
          const [resourcesRes, assignmentsRes] = await Promise.all([
            apiRequest({
              method: "GET",
              url: `${di}/api/v1/projects/${encodeURIComponent(pickedProjectRef)}/resources`,
              token: useAuth ? token : "",
            }),
            apiRequest({
              method: "GET",
              url: `${di}/api/v1/projects/${encodeURIComponent(pickedProjectRef)}/qualification-assignments`,
              token: useAuth ? token : "",
            }),
          ]);
          projectResources = resourcesRes.data;
          qualificationAssignments = normalizeListData(assignmentsRes.data);
        } catch (err) {
          warnings.push(`projectDetail: ${String(err)}`);
        }
      }

      setDashboard({
        ...next,
        projectResources,
        qualificationAssignments,
        updatedAt: new Date().toISOString(),
      });
      if (warnings.length > 0) {
        setDashboardError(warnings.join("\n"));
      }
    } catch (err) {
      setDashboardError(String(err));
    } finally {
      setDashboardLoading(false);
    }
  };

  const changeTablePage = (key, page) => {
    if (!TABLE_KEYS.includes(key)) return;
    const total = Number(dashboard?.totals?.[key] ?? 0);
    const maxPage = Math.max(1, Math.ceil(total / TABLE_PAGE_SIZE) || 1);
    const nextPage = Math.min(Math.max(1, Number(page) || 1), maxPage);
    void loadDashboardData({ [key]: nextPage });
  };

  const loadLibraryDetail = async (libraryType, rawID) => {
    const di = trimTrailingSlash(diBase.trim());
    if (!di) {
      setLibraryDetailError("Design-Ins Base URL 不能为空");
      return;
    }
    const type = String(libraryType || "").trim();
    const id = Number(rawID);
    if (!type) {
      setLibraryDetailError("请输入库类型");
      return;
    }
    if (!Number.isFinite(id) || id <= 0) {
      setLibraryDetailError("请输入有效ID");
      return;
    }

    setLibraryDetailLoading(true);
    setLibraryDetailError("");
    try {
      validateLibraryScope();
      const qs = new URLSearchParams();
      applyLibraryScope(qs);
      applyLibraryVersionOptions(qs);
      const suffix = qs.toString();
      const res = await apiRequest({
        method: "GET",
        url: `${di}/api/v1/libraries/${encodeURIComponent(type)}/${Math.trunc(id)}${suffix ? `?${suffix}` : ""}`,
        token: useAuth ? token : "",
      });
      setLibraryDetail(res.data);
    } catch (err) {
      setLibraryDetail(null);
      setLibraryDetailError(String(err));
    } finally {
      setLibraryDetailLoading(false);
    }
  };

  const loadLibraryChanges = async (libraryType, rawID, input = {}) => {
    const di = trimTrailingSlash(diBase.trim());
    if (!di) {
      setLibraryChangesError("Design-Ins Base URL 不能为空");
      return;
    }
    const type = String(libraryType || "").trim();
    const id = Number(rawID);
    if (!type) {
      setLibraryChangesError("请输入库类型");
      return;
    }
    if (!Number.isFinite(id) || id <= 0) {
      setLibraryChangesError("请输入有效ID");
      return;
    }
    const limit = Math.max(1, Math.min(200, Number(input.limit) || TABLE_PAGE_SIZE));
    const offset = Math.max(0, Number(input.offset) || 0);
    const from = String(input.from || "").trim();
    const to = String(input.to || "").trim();

    setLibraryChangesLoading(true);
    setLibraryChangesError("");
    try {
      validateLibraryScope();
      const qs = new URLSearchParams({
        limit: String(limit),
        offset: String(offset),
      });
      if (from) qs.set("from", from);
      if (to) qs.set("to", to);
      applyLibraryScope(qs);
      applyLibraryVersionOptions(qs);
      const res = await apiRequest({
        method: "GET",
        url: `${di}/api/v1/libraries/${encodeURIComponent(type)}/${Math.trunc(id)}/changes?${qs.toString()}`,
        token: useAuth ? token : "",
      });
      const payloadRaw = res.data && typeof res.data === "object" ? res.data : {};
      const payload = payloadRaw.data && typeof payloadRaw.data === "object" ? payloadRaw.data : payloadRaw;
      const items = normalizeListData(payload);
      const totalRaw = pickField(payload, ["total", "Total"], items.length);
      const payloadTotal = Number.isFinite(Number(totalRaw)) ? Number(totalRaw) : items.length;
      const limitRaw = pickField(payload, ["limit", "Limit"], limit);
      const payloadLimit = Number.isFinite(Number(limitRaw)) ? Number(limitRaw) : limit;
      const offsetRaw = pickField(payload, ["offset", "Offset"], offset);
      const payloadOffset = Number.isFinite(Number(offsetRaw)) ? Number(offsetRaw) : offset;
      const updatedAt = String(pickField(payload, ["updated_at", "updatedAt", "UpdatedAt"], ""));
      setLibraryChanges({
        total: payloadTotal >= 0 ? payloadTotal : items.length,
        limit: payloadLimit > 0 ? payloadLimit : limit,
        offset: payloadOffset >= 0 ? payloadOffset : offset,
        items,
        updated_at: updatedAt,
      });
    } catch (err) {
      setLibraryChanges(buildLibraryChanges());
      setLibraryChangesError(String(err));
    } finally {
      setLibraryChangesLoading(false);
    }
  };

  const loadExecutorVault = async (executorRef, drawingLimit = 20) => {
    const di = trimTrailingSlash(diBase.trim());
    if (!di) {
      setExecutorVaultError("Design-Ins Base URL 不能为空");
      return;
    }
    const ref = String(executorRef || "").trim();
    if (!ref) {
      setExecutorVaultError("请输入 executor_ref");
      return;
    }
    const limit = Math.max(1, Math.min(200, Number(drawingLimit) || 20));

    setExecutorVaultLoading(true);
    setExecutorVaultError("");
    try {
      validateLibraryScope();
      const qs = new URLSearchParams({
        executor_ref: ref,
        drawing_limit: String(limit),
      });
      applyLibraryScope(qs);
      const res = await apiRequest({
        method: "GET",
        url: `${di}/api/v1/libraries/executor-vault?${qs.toString()}`,
        token: useAuth ? token : "",
      });
      setExecutorVault(res.data);
    } catch (err) {
      setExecutorVault(null);
      setExecutorVaultError(String(err));
    } finally {
      setExecutorVaultLoading(false);
    }
  };

  const loadLibraryRelations = async (libraryType, rawID, limit = 30) => {
    const di = trimTrailingSlash(diBase.trim());
    if (!di) {
      setLibraryRelationsError("Design-Ins Base URL 不能为空");
      return;
    }
    const type = String(libraryType || "").trim();
    const id = Number(rawID);
    if (!type) {
      setLibraryRelationsError("请输入库类型");
      return;
    }
    if (!Number.isFinite(id) || id <= 0) {
      setLibraryRelationsError("请输入有效ID");
      return;
    }
    const safeLimit = Math.max(1, Math.min(200, Number(limit) || 30));

    setLibraryRelationsLoading(true);
    setLibraryRelationsError("");
    try {
      validateLibraryScope();
      const qs = new URLSearchParams({ limit: String(safeLimit) });
      applyLibraryScope(qs);
      applyLibraryVersionOptions(qs);
      const res = await apiRequest({
        method: "GET",
        url: `${di}/api/v1/libraries/${encodeURIComponent(type)}/${Math.trunc(id)}/relations?${qs.toString()}`,
        token: useAuth ? token : "",
      });
      setLibraryRelations(res.data);
    } catch (err) {
      setLibraryRelations(null);
      setLibraryRelationsError(String(err));
    } finally {
      setLibraryRelationsLoading(false);
    }
  };

  const searchLibraries = async (input = {}) => {
    const di = trimTrailingSlash(diBase.trim());
    if (!di) {
      setLibrarySearchError("Design-Ins Base URL 不能为空");
      return;
    }

    const keyword = String(input.keyword || "").trim();
    const type = String(input.type || "").trim();
    const status = String(input.status || "").trim();
    const updatedFrom = String(input.updatedFrom || "").trim();
    const updatedTo = String(input.updatedTo || "").trim();
    const hasExecutor = String(input.hasExecutor || "").trim();
    const limit = Math.max(1, Math.min(200, Number(input.limit) || TABLE_PAGE_SIZE));
    const offset = Math.max(0, Number(input.offset) || 0);

    setLibrarySearchLoading(true);
    setLibrarySearchError("");
    try {
      validateLibraryScope();
      const qs = new URLSearchParams({
        limit: String(limit),
        offset: String(offset),
      });
      if (keyword) qs.set("keyword", keyword);
      if (type) qs.set("type", type);
      if (status) qs.set("status", status);
      if (updatedFrom) qs.set("updated_from", updatedFrom);
      if (updatedTo) qs.set("updated_to", updatedTo);
      if (hasExecutor) qs.set("has_executor", hasExecutor);
      applyLibraryScope(qs);
      applyLibraryVersionOptions(qs);

      const res = await apiRequest({
        method: "GET",
        url: `${di}/api/v1/libraries/search?${qs.toString()}`,
        token: useAuth ? token : "",
      });
      const payload = res.data && typeof res.data === "object" ? res.data : {};
      const items = normalizeListData(payload);
      const totalRaw = pickField(payload, ["total", "Total"], items.length);
      const total = Number.isFinite(Number(totalRaw)) ? Number(totalRaw) : items.length;
      const limitRaw = pickField(payload, ["limit", "Limit"], limit);
      const payloadLimit = Number.isFinite(Number(limitRaw)) ? Number(limitRaw) : limit;
      const offsetRaw = pickField(payload, ["offset", "Offset"], offset);
      const payloadOffset = Number.isFinite(Number(offsetRaw)) ? Number(offsetRaw) : offset;
      const updatedAt = String(pickField(payload, ["updated_at", "updatedAt", "UpdatedAt"], ""));
      setLibrarySearch({
        total: total >= 0 ? total : items.length,
        limit: payloadLimit > 0 ? payloadLimit : limit,
        offset: payloadOffset >= 0 ? payloadOffset : offset,
        items,
        updated_at: updatedAt,
      });
    } catch (err) {
      setLibrarySearch({
        total: 0,
        limit,
        offset,
        items: [],
        updated_at: "",
      });
      setLibrarySearchError(String(err));
    } finally {
      setLibrarySearchLoading(false);
    }
  };

  const loadLibrariesQuality = async (sampleLimit = TABLE_PAGE_SIZE) => {
    setLibraryQualityLoading(true);
    setLibraryQualityError("");
    try {
      const payload = await fetchLibrariesQuality(sampleLimit);
      setLibraryQuality(payload);
    } catch (err) {
      setLibraryQuality(buildLibraryQualityGate());
      setLibraryQualityError(String(err));
    } finally {
      setLibraryQualityLoading(false);
    }
  };

  return {
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
    selectedProjectRef,
    projectDetailLoading,
    tablePages,
    tablePageSize: TABLE_PAGE_SIZE,
    setSelectedProjectRef,
    loadProjectDetail,
    loadDashboardData,
    changeTablePage,
    loadLibraryDetail,
    loadLibraryChanges,
    loadLibraryRelations,
    loadExecutorVault,
    loadLibrariesQuality,
    searchLibraries,
  };
}
