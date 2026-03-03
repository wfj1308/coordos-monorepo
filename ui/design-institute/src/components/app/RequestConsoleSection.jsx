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
    <section className="panel p-6">
      <h2 className="text-lg font-semibold">通用请求控制台</h2>
      <div className="mt-4 grid gap-4 md:grid-cols-6">
        <label className="md:col-span-1">
          <span className="mb-1 block text-xs text-slate">Method</span>
          <select
            value={method}
            onChange={(e) => setMethod(e.target.value)}
            className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            <option>GET</option>
            <option>POST</option>
            <option>PUT</option>
            <option>DELETE</option>
          </select>
        </label>
        <label className="md:col-span-5">
          <span className="mb-1 block text-xs text-slate">URL</span>
          <input
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder="{DI}/health"
          />
        </label>
      </div>
      <p className="mt-2 text-xs text-slate">
        解析后 URL: <code>{finalUrl}</code>
      </p>
      <label className="mt-4 block">
        <span className="mb-1 block text-xs text-slate">Body (JSON)</span>
        <textarea
          value={body}
          onChange={(e) => setBody(e.target.value)}
          rows={10}
          className="w-full rounded-lg border border-slate-300 px-3 py-2 font-mono text-xs"
        />
      </label>

      <div className="mt-4 flex gap-3">
        <button
          onClick={run}
          disabled={pending}
          className="rounded-lg bg-skyline px-4 py-2 text-sm font-medium text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {pending ? "请求中..." : "发送请求"}
        </button>
        <button onClick={clearResponse} className="rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm">
          清空响应
        </button>
      </div>

      <pre className="mt-4 min-h-64 overflow-auto rounded-lg bg-ink p-4 text-xs text-mist">
        {response || "{\n  \"hint\": \"Response will appear here\"\n}"}
      </pre>
    </section>
  );
}
