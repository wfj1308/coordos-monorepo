import { useState } from "react";
import DashboardSection from "./components/app/DashboardSection";
import SystemSectionHeader from "./components/app/SystemSectionHeader";
import { readLocal, saveLocal } from "./components/app/utils";
import useDashboardData from "./hooks/useDashboardData";

export default function DashboardPage() {
  const [diBase, setDiBase] = useState(readLocal("coordos.di.base", "/di"));
  const [useAuth, setUseAuth] = useState(readLocal("coordos.use.auth", "0") === "1");
  const [token, setToken] = useState(readLocal("coordos.token", ""));

  const {
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
    tablePageSize,
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
  } = useDashboardData({ diBase, useAuth, token });

  return (
    <main className="di-page-shell">
      <section className="di-content-wrap space-y-4">
        <SystemSectionHeader
          kicker="CoordOS / Data Cockpit"
          title="业务数据看板"
          subtitle="三库映射、质量闸门、关系链追踪与项目资源回读统一入口。"
          actions={[
            { to: "/partner-profile", label: "能力画像", tone: "di-btn-muted" },
            { to: "/api-console", label: "API 控制台", tone: "di-btn-muted" },
          ]}
        />

        <section className="panel p-4">
          <div className="grid gap-3 md:grid-cols-4">
            <label className="md:col-span-2">
              <span className="mb-1 block text-xs text-slate-600">Design-Ins Base URL</span>
              <input
                value={diBase}
                onChange={(e) => {
                  const v = e.target.value;
                  setDiBase(v);
                  saveLocal("coordos.di.base", v);
                }}
                className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
              />
            </label>
            <label className="flex items-end gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700">
              <input
                type="checkbox"
                checked={useAuth}
                onChange={(e) => {
                  const checked = e.target.checked;
                  setUseAuth(checked);
                  saveLocal("coordos.use.auth", checked ? "1" : "0");
                }}
              />
              请求附带 Bearer Token
            </label>
            <label>
              <span className="mb-1 block text-xs text-slate-600">Bearer Token</span>
              <input
                value={token}
                onChange={(e) => {
                  const v = e.target.value;
                  setToken(v);
                  saveLocal("coordos.token", v);
                }}
                className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
              />
            </label>
          </div>
        </section>

        <DashboardSection
          dashboard={dashboard}
          dashboardLoading={dashboardLoading}
          dashboardError={dashboardError}
          libraryDetail={libraryDetail}
          libraryDetailLoading={libraryDetailLoading}
          libraryDetailError={libraryDetailError}
          libraryChanges={libraryChanges}
          libraryChangesLoading={libraryChangesLoading}
          libraryChangesError={libraryChangesError}
          libraryRelations={libraryRelations}
          libraryRelationsLoading={libraryRelationsLoading}
          libraryRelationsError={libraryRelationsError}
          executorVault={executorVault}
          executorVaultLoading={executorVaultLoading}
          executorVaultError={executorVaultError}
          libraryQuality={libraryQuality}
          libraryQualityLoading={libraryQualityLoading}
          libraryQualityError={libraryQualityError}
          librarySearch={librarySearch}
          librarySearchLoading={librarySearchLoading}
          librarySearchError={librarySearchError}
          libraryViewerRole={libraryViewerRole}
          setLibraryViewerRole={setLibraryViewerRole}
          libraryViewerExecutorRef={libraryViewerExecutorRef}
          setLibraryViewerExecutorRef={setLibraryViewerExecutorRef}
          libraryIncludeHistory={libraryIncludeHistory}
          setLibraryIncludeHistory={setLibraryIncludeHistory}
          libraryValidOn={libraryValidOn}
          setLibraryValidOn={setLibraryValidOn}
          loadDashboardData={loadDashboardData}
          loadLibraryDetail={loadLibraryDetail}
          loadLibraryChanges={loadLibraryChanges}
          loadLibraryRelations={loadLibraryRelations}
          loadExecutorVault={loadExecutorVault}
          loadLibrariesQuality={loadLibrariesQuality}
          searchLibraries={searchLibraries}
          selectedProjectRef={selectedProjectRef}
          setSelectedProjectRef={setSelectedProjectRef}
          loadProjectDetail={loadProjectDetail}
          projectDetailLoading={projectDetailLoading}
          tablePages={tablePages}
          tablePageSize={tablePageSize}
          changeTablePage={changeTablePage}
        />
      </section>
    </main>
  );
}
