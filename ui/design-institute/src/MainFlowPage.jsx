import { useState } from "react";
import MainFlowSection from "./components/app/MainFlowSection";
import SystemSectionHeader from "./components/app/SystemSectionHeader";
import { readLocal, saveLocal } from "./components/app/utils";
import useMainFlow from "./hooks/useMainFlow";

export default function MainFlowPage() {
  const [diBase, setDiBase] = useState(readLocal("coordos.di.base", "/di"));
  const [useAuth, setUseAuth] = useState(readLocal("coordos.use.auth", "0") === "1");
  const [token, setToken] = useState(readLocal("coordos.token", ""));
  const [response, setResponse] = useState("");

  const { flowSteps, flowRunning, flowSummary, runMainFlow, resetFlow } = useMainFlow({
    diBase,
    useAuth,
    token,
    onResponse: setResponse,
  });

  return (
    <main className="di-page-shell">
      <section className="di-content-wrap space-y-4">
        <SystemSectionHeader
          kicker="CoordOS / Workflow"
          title="Phase0-7 主流程联调"
          subtitle="用于验证从注册入网到业绩入池的闭环动作与关键产出。"
          actions={[
            { to: "/dashboard", label: "打开看板", tone: "di-btn-muted" },
            { to: "/join", label: "入网流程", tone: "di-btn-muted" },
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

        <MainFlowSection
          flowRunning={flowRunning}
          runMainFlow={runMainFlow}
          resetFlow={resetFlow}
          flowSteps={flowSteps}
          flowSummary={flowSummary}
        />

        <section className="panel p-4">
          <div className="mb-2 text-sm font-semibold text-slate-800">执行输出（原始响应）</div>
          <pre className="di-json-shell min-h-52">{response || '{\n  "hint": "flow output"\n}'}</pre>
        </section>
      </section>
    </main>
  );
}
