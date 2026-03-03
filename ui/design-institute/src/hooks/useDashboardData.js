import { useState } from "react";
import {
  apiRequest,
  normalizeListData,
  pickField,
  trimTrailingSlash,
} from "../components/app/utils";

function buildDashboardData() {
  return {
    projects: [],
    contracts: [],
    employees: [],
    qualifications: [],
    achievements: [],
    gatherings: [],
    invoices: [],
    settlements: [],
    projectResources: null,
    qualificationAssignments: [],
    updatedAt: "",
  };
}

export default function useDashboardData({ diBase, useAuth, token }) {
  const [dashboard, setDashboard] = useState(buildDashboardData());
  const [dashboardLoading, setDashboardLoading] = useState(false);
  const [dashboardError, setDashboardError] = useState("");
  const [selectedProjectRef, setSelectedProjectRef] = useState("");
  const [projectDetailLoading, setProjectDetailLoading] = useState(false);

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
      return normalizeListData(res.data);
    };

    try {
      const primaryRows = await tryBase(primary);
      if (primaryRows.length > 0) return primaryRows;
      if (primary.startsWith("/") && primary !== direct) {
        const directRows = await tryBase(direct);
        if (directRows.length > 0) {
          warnings.push(`${path}: 代理返回空，已回退直连 ${direct}`);
          return directRows;
        }
      }
      return primaryRows;
    } catch (primaryErr) {
      if (primary.startsWith("/") && primary !== direct) {
        try {
          const directRows = await tryBase(direct);
          warnings.push(`${path}: 代理失败，已回退直连 ${direct}`);
          return directRows;
        } catch (directErr) {
          throw new Error(`${String(primaryErr)} | fallback: ${String(directErr)}`);
        }
      }
      throw primaryErr;
    }
  };

  const loadDashboardData = async () => {
    const di = trimTrailingSlash(diBase.trim());
    if (!di) {
      setDashboardError("Design-Ins Base URL 不能为空");
      return;
    }

    setDashboardLoading(true);
    setDashboardError("");
    try {
      const warnings = [];
      const listJobs = [
        { key: "projects", path: "/api/v1/projects?limit=20&offset=0" },
        { key: "contracts", path: "/api/v1/contracts?limit=20&offset=0" },
        { key: "employees", path: "/api/v1/employees?limit=20&offset=0" },
        { key: "qualifications", path: "/api/v1/qualifications?limit=20&offset=0" },
        { key: "gatherings", path: "/api/v1/gatherings?limit=20&offset=0" },
        { key: "invoices", path: "/api/v1/invoices?limit=20&offset=0" },
        { key: "settlements", path: "/api/v1/settlements?limit=20&offset=0" },
      ];

      const settled = await Promise.allSettled(
        listJobs.map((job) => listWithFallback(job.path, warnings)),
      );

      const next = {
        projects: [],
        contracts: [],
        employees: [],
        qualifications: [],
        achievements: [],
        gatherings: [],
        invoices: [],
        settlements: [],
      };

      settled.forEach((result, idx) => {
        const key = listJobs[idx].key;
        if (result.status === "fulfilled") {
          next[key] = result.value;
          return;
        }
        warnings.push(`${key}: ${String(result.reason)}`);
      });

      try {
        next.achievements = await listWithFallback("/api/v1/achievements?limit=20&offset=0", warnings);
      } catch (achErr) {
        try {
          const publicAchievementsRes = await apiRequest({
            method: "GET",
            url: `${di}/public/v1/achievements?limit=20&offset=0`,
            token: useAuth ? token : "",
          });
          next.achievements = normalizeListData(publicAchievementsRes.data);
          warnings.push("achievements: 私有接口返回错误，已降级使用 public/v1/achievements");
        } catch (publicErr) {
          warnings.push(`achievements: ${String(achErr)} | fallback: ${String(publicErr)}`);
          next.achievements = [];
        }
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

  return {
    dashboard,
    dashboardLoading,
    dashboardError,
    selectedProjectRef,
    projectDetailLoading,
    setSelectedProjectRef,
    loadProjectDetail,
    loadDashboardData,
  };
}
