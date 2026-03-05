export default function RequestConsoleSection({
  method,
  setMethod,
  url,
  setUrl,
  body,
  setBody,
  finalUrl,
  run,
  pending,
  clearResponse,
  response,
}) {
  return (
    <section className="panel di-console-shell p-6">
      <h2 className="text-lg font-semibold text-slate-900">通用请求控制台</h2>
      <div className="mt-4 grid gap-4 md:grid-cols-6">
        <label className="md:col-span-1">
          <span className="mb-1 block text-xs text-slate-600">Method</span>
          <select
            value={method}
            onChange={(e) => setMethod(e.target.value)}
            className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
          >
            <option>GET</option>
            <option>POST</option>
            <option>PUT</option>
            <option>DELETE</option>
          </select>
        </label>
        <label className="md:col-span-5">
          <span className="mb-1 block text-xs text-slate-600">URL</span>
          <input
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
            placeholder="{DI}/health"
          />
        </label>
      </div>
      <p className="mt-2 text-xs text-slate-600">
        解析后 URL: <code>{finalUrl}</code>
      </p>

      <label className="mt-4 block">
        <span className="mb-1 block text-xs text-slate-600">Body (JSON)</span>
        <textarea
          value={body}
          onChange={(e) => setBody(e.target.value)}
          rows={10}
          className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 font-mono text-xs"
        />
      </label>

      <div className="mt-4 flex gap-3">
        <button
          onClick={run}
          disabled={pending}
          className="di-btn di-btn-primary disabled:cursor-not-allowed disabled:opacity-60"
        >
          {pending ? "请求中..." : "发送请求"}
        </button>
        <button onClick={clearResponse} className="di-btn di-btn-muted">
          清空响应
        </button>
      </div>

      <pre className="di-json-shell mt-4 min-h-64">{response || '{\n  "hint": "Response will appear here"\n}'}</pre>
    </section>
  );
}
