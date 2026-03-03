import { useMemo, useState } from "react";
import DashboardSection from "./components/app/DashboardSection";
import EnvironmentTemplatesSection from "./components/app/EnvironmentTemplatesSection";
import MainFlowSection from "./components/app/MainFlowSection";
import PartnerProfileSection from "./components/app/PartnerProfileSection";
import RequestConsoleSection from "./components/app/RequestConsoleSection";
import { readLocal, saveLocal, trimTrailingSlash, tryParse } from "./components/app/utils";
import useDashboardData from "./hooks/useDashboardData";
import useMainFlow from "./hooks/useMainFlow";
import usePartnerProfile from "./hooks/usePartnerProfile";

const quickTemplates = [
  {
    name: "DI Health",
    method: "GET",
    url: "{DI}/health",
    body: "",
  },
  {
    name: "Vault Health",
    method: "GET",
    url: "{VAULT}/health",
    body: "",
  },
  {
    name: "Project Resources",
    method: "GET",
    url: "{DI}/api/v1/projects/{ref}/resources",
    body: "",
  },
  {
    name: "Partner Profile",
    method: "GET",
    url: "{DI}/public/v1/partner-profile/cn.zhongbei",
    body: "",
  },
  {
    name: "Manual Achievement",
    method: "POST",
    url: "{DI}/api/v1/achievements/manual",
    body: JSON.stringify(
      {
        spu_ref: "v://zhongbei/spu/bridge/pile_foundation_drawing@v1",
        project_ref: "v://10000/project/demo",
        executor_ref: "v://person/11010519900101123X/executor",
        payload: { amount: 500000, stage: "review-finish" },
      },
      null,
      2,
    ),
  },
];

export default function App() {
  const [diBase, setDiBase] = useState(readLocal("coordos.di.base", "/di"));
  const [vaultBase, setVaultBase] = useState(readLocal("coordos.vault.base", "/vault"));
  const [token, setToken] = useState(readLocal("coordos.token", ""));
  const [useAuth, setUseAuth] = useState(readLocal("coordos.use.auth", "0") === "1");

  const [method, setMethod] = useState("GET");
  const [url, setUrl] = useState("{DI}/health");
  const [body, setBody] = useState("");
  const [response, setResponse] = useState("");
  const [pending, setPending] = useState(false);

  const {
    flowSteps,
    flowRunning,
    flowSummary,
    runMainFlow,
    resetFlow,
  } = useMainFlow({ diBase, useAuth, token, onResponse: setResponse });

  const {
    dashboard,
    dashboardLoading,
    dashboardError,
    selectedProjectRef,
    projectDetailLoading,
    setSelectedProjectRef,
    loadProjectDetail,
    loadDashboardData,
  } = useDashboardData({ diBase, useAuth, token });

  const {
    partnerProfile,
    partnerProfileLoading,
    partnerProfileError,
    partnerProfileNamespace,
    loadPartnerProfile,
    handlePartnerProfileNamespaceChange,
  } = usePartnerProfile({ diBase, useAuth, token });

  const finalUrl = useMemo(
    () =>
      url
        .replaceAll("{DI}", trimTrailingSlash(diBase.trim()))
        .replaceAll("{VAULT}", trimTrailingSlash(vaultBase.trim())),
    [url, diBase, vaultBase],
  );

  const applyTemplate = (tpl) => {
    setMethod(tpl.method);
    setUrl(tpl.url);
    setBody(tpl.body);
  };

  const run = async () => {
    setPending(true);
    setResponse("");
    try {
      const headers = { "Content-Type": "application/json" };
      if (useAuth && token.trim()) headers.Authorization = `Bearer ${token.trim()}`;
      const init = { method, headers };
      if (method !== "GET" && method !== "HEAD") {
        init.body = body.trim() || "{}";
      }
      const resp = await fetch(finalUrl, init);
      const text = await resp.text();
      setResponse(
        JSON.stringify(
          {
            status: resp.status,
            ok: resp.ok,
            url: finalUrl,
            method,
            body: tryParse(text),
          },
          null,
          2,
        ),
      );
    } catch (err) {
      setResponse(
        JSON.stringify(
          {
            status: 0,
            ok: false,
            url: finalUrl,
            method,
            error: String(err),
          },
          null,
          2,
        ),
      );
    } finally {
      setPending(false);
    }
  };

  const handleDiBaseChange = (v) => {
    setDiBase(v);
    saveLocal("coordos.di.base", v);
  };

  const handleVaultBaseChange = (v) => {
    setVaultBase(v);
    saveLocal("coordos.vault.base", v);
  };

  const handleUseAuthChange = (checked) => {
    setUseAuth(checked);
    saveLocal("coordos.use.auth", checked ? "1" : "0");
  };

  const handleTokenChange = (v) => {
    setToken(v);
    saveLocal("coordos.token", v);
  };

  const switchProxyMode = () => {
    setDiBase("/di");
    setVaultBase("/vault");
    saveLocal("coordos.di.base", "/di");
    saveLocal("coordos.vault.base", "/vault");
  };

  const switchDirectMode = () => {
    setDiBase("http://127.0.0.1:8090");
    setVaultBase("http://127.0.0.1:8080");
    saveLocal("coordos.di.base", "http://127.0.0.1:8090");
    saveLocal("coordos.vault.base", "http://127.0.0.1:8080");
  };

  const clearResponse = () => setResponse("");

  return (
    <main className="min-h-full px-4 py-6 md:px-8">
      <section className="mx-auto max-w-6xl space-y-6">
        <header className="panel p-6">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <h1 className="text-2xl font-semibold tracking-tight">中北设计院管理系统 · 业务联调台</h1>
              <p className="mt-2 text-sm text-slate">
                提供“项目到合同到人员到资质到业绩到发票到结算到证据包”的核心闭环演示。
              </p>
            </div>
            <a
              href="/join/"
              target="_blank"
              rel="noreferrer"
              className="cursor-pointer rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-800 transition hover:border-skyline hover:text-skyline"
            >
              打开 v:// 入网工具
            </a>
          </div>
        </header>

        <PartnerProfileSection
          namespace={partnerProfileNamespace}
          onNamespaceChange={handlePartnerProfileNamespaceChange}
          onRefresh={loadPartnerProfile}
          loading={partnerProfileLoading}
          error={partnerProfileError}
          profile={partnerProfile}
        />

        <MainFlowSection
          flowRunning={flowRunning}
          runMainFlow={runMainFlow}
          resetFlow={resetFlow}
          flowSteps={flowSteps}
          flowSummary={flowSummary}
        />

        <DashboardSection
          dashboard={dashboard}
          dashboardLoading={dashboardLoading}
          dashboardError={dashboardError}
          loadDashboardData={loadDashboardData}
          selectedProjectRef={selectedProjectRef}
          setSelectedProjectRef={setSelectedProjectRef}
          loadProjectDetail={loadProjectDetail}
          projectDetailLoading={projectDetailLoading}
        />

        <EnvironmentTemplatesSection
          diBase={diBase}
          vaultBase={vaultBase}
          useAuth={useAuth}
          token={token}
          onDiBaseChange={handleDiBaseChange}
          onVaultBaseChange={handleVaultBaseChange}
          onUseAuthChange={handleUseAuthChange}
          onTokenChange={handleTokenChange}
          onSwitchProxyMode={switchProxyMode}
          onSwitchDirectMode={switchDirectMode}
          quickTemplates={quickTemplates}
          applyTemplate={applyTemplate}
        />

        <RequestConsoleSection
          method={method}
          setMethod={setMethod}
          url={url}
          setUrl={setUrl}
          body={body}
          setBody={setBody}
          finalUrl={finalUrl}
          run={run}
          pending={pending}
          clearResponse={clearResponse}
          response={response}
        />
      </section>
    </main>
  );
}
