import { StatusTag } from "./CommonUI";

export default function MainFlowSection({ flowRunning, runMainFlow, resetFlow, flowSteps, flowSummary }) {
  return (
    <section className="panel di-console-shell p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold text-slate-900">Phase0-7 主流程场景（中北）</h2>
          <p className="mt-1 text-xs text-slate-600">覆盖注册入网、招标投标、中标履约、审图验收、结算入池七个阶段。</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            onClick={runMainFlow}
            disabled={flowRunning}
            className="di-btn di-btn-primary disabled:cursor-not-allowed disabled:opacity-60"
          >
            {flowRunning ? "流程执行中..." : "执行 Phase0-7"}
          </button>
          <button
            onClick={resetFlow}
            disabled={flowRunning}
            className="di-btn di-btn-muted disabled:cursor-not-allowed disabled:opacity-60"
          >
            重置步骤
          </button>
        </div>
      </div>

      <div className="mt-4 grid gap-3">
        {flowSteps.map((step, idx) => (
          <article key={step.key} className="di-step-card">
            <div className="di-step-head">
              <div className="text-base font-semibold text-slate-900">
                {idx + 1}. {step.title}
              </div>
              <StatusTag status={step.status} />
            </div>
            <p className="mt-1 text-sm text-slate-700">{step.detail}</p>
            {step.elapsedMs != null && <p className="mt-1 text-xs text-slate-500">耗时: {step.elapsedMs} ms</p>}
            {step.result && <pre className="di-json-shell mt-2">{JSON.stringify(step.result, null, 2)}</pre>}
            {step.error && (
              <pre className="mt-2 overflow-auto rounded-lg bg-red-950 p-3 text-xs text-red-100">{step.error}</pre>
            )}
          </article>
        ))}
      </div>

      {flowSummary && (
        <div className="mt-4 rounded-xl border border-indigo-200 bg-indigo-50 p-4 text-xs">
          <div className="font-semibold text-indigo-900">运行摘要</div>
          <pre className="di-json-shell mt-2">{JSON.stringify(flowSummary, null, 2)}</pre>
        </div>
      )}
    </section>
  );
}
