import { useMemo, useState } from "react";
import EnvironmentTemplatesSection from "./components/app/EnvironmentTemplatesSection";
import RequestConsoleSection from "./components/app/RequestConsoleSection";
import SystemSectionHeader from "./components/app/SystemSectionHeader";
import { readLocal, saveLocal, trimTrailingSlash, tryParse } from "./components/app/utils";

const quickTemplates = [
  { name: "DI Health", method: "GET", url: "{DI}/health", body: "" },
  { name: "Vault Health", method: "GET", url: "{VAULT}/health", body: "" },
  { name: "Project Resources", method: "GET", url: "{DI}/api/v1/projects/{ref}/resources", body: "" },
  { name: "Partner Profile", method: "GET", url: "{DI}/public/v1/partner-profile/cn.zhongbei", body: "" },
  {
    name: "Verify Achievement Ref",
    method: "GET",
    url: "{DI}/api/v1/achievement/verify?ref=v://cn.zhongbei/utxo/achievement/highway/2024/001",
    body: "",
  },
  {
    name: "Verify Proof Hash",
    method: "GET",
    url: "{DI}/public/v1/verify/achievement/sha256:replace_with_hash",
    body: "",
  },
  {
    name: "Manual Achievement",
    method: "POST",
    url: "{DI}/api/v1/achievements/manual",
    body: JSON.stringify(
      {
        spu_ref: "v://cn.zhongbei/spu/bridge/pile_foundation_drawing@v1",
        project_ref: "v://cn.zhongbei/project/replace-with-real-project",
        executor_ref: "v://cn.zhongbei/executor/person/replace-with-real-executor@v1",
        payload: { amount: 500000, stage: "review-finish" },
      },
      null,
      2,
    ),
  },
];

export default function ApiConsolePage() {
  const [diBase, setDiBase] = useState(readLocal("coordos.di.base", "/di"));
  const [vaultBase, setVaultBase] = useState(readLocal("coordos.vault.base", "/vault"));
  const [token, setToken] = useState(readLocal("coordos.token", ""));
  const [useAuth, setUseAuth] = useState(readLocal("coordos.use.auth", "0") === "1");
  const [method, setMethod] = useState("GET");
  const [url, setUrl] = useState("{DI}/health");
  const [body, setBody] = useState("");
  const [response, setResponse] = useState("");
  const [pending, setPending] = useState(false);

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
        JSON.stringify({ status: resp.status, ok: resp.ok, url: finalUrl, method, body: tryParse(text) }, null, 2),
      );
    } catch (err) {
      setResponse(JSON.stringify({ status: 0, ok: false, url: finalUrl, method, error: String(err) }, null, 2));
    } finally {
      setPending(false);
    }
  };

  return (
    <main className="di-page-shell">
      <section className="di-content-wrap space-y-4">
        <SystemSectionHeader
          kicker="CoordOS / API Console"
          title="API 联调控制台"
          subtitle="统一管理 DI/Vault 调试环境、模板请求与响应回放。"
          actions={[
            { to: "/dashboard", label: "打开看板", tone: "di-btn-muted" },
            { to: "/main-flow", label: "主流程联调", tone: "di-btn-muted" },
          ]}
        />

        <EnvironmentTemplatesSection
          diBase={diBase}
          vaultBase={vaultBase}
          useAuth={useAuth}
          token={token}
          onDiBaseChange={(v) => {
            setDiBase(v);
            saveLocal("coordos.di.base", v);
          }}
          onVaultBaseChange={(v) => {
            setVaultBase(v);
            saveLocal("coordos.vault.base", v);
          }}
          onUseAuthChange={(checked) => {
            setUseAuth(checked);
            saveLocal("coordos.use.auth", checked ? "1" : "0");
          }}
          onTokenChange={(v) => {
            setToken(v);
            saveLocal("coordos.token", v);
          }}
          onSwitchProxyMode={() => {
            setDiBase("/di");
            setVaultBase("/vault");
            saveLocal("coordos.di.base", "/di");
            saveLocal("coordos.vault.base", "/vault");
          }}
          onSwitchDirectMode={() => {
            setDiBase("http://127.0.0.1:8090");
            setVaultBase("http://127.0.0.1:8080");
            saveLocal("coordos.di.base", "http://127.0.0.1:8090");
            saveLocal("coordos.vault.base", "http://127.0.0.1:8080");
          }}
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
          clearResponse={() => setResponse("")}
          response={response}
        />
      </section>
    </main>
  );
}
