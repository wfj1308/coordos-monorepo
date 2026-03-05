import { Input } from "./CommonUI";

export default function EnvironmentTemplatesSection({
  diBase,
  vaultBase,
  useAuth,
  token,
  onDiBaseChange,
  onVaultBaseChange,
  onUseAuthChange,
  onTokenChange,
  onSwitchProxyMode,
  onSwitchDirectMode,
  quickTemplates,
  applyTemplate,
}) {
  return (
    <section className="grid gap-5 xl:grid-cols-[1.1fr_0.9fr]">
      <div className="panel di-console-shell p-6">
        <h2 className="text-lg font-semibold text-slate-900">环境配置</h2>
        <div className="mt-4 space-y-4">
          <Input label="Design-Ins Base URL" value={diBase} onChange={onDiBaseChange} />
          <Input label="Vault Base URL" value={vaultBase} onChange={onVaultBaseChange} />
          <label className="flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700">
            <input type="checkbox" checked={useAuth} onChange={(e) => onUseAuthChange(e.target.checked)} />
            请求附带 Bearer Token
          </label>
          <Input label="Bearer Token（通用控制台）" value={token} onChange={onTokenChange} />
          <div className="flex flex-wrap gap-2">
            <button onClick={onSwitchProxyMode} className="di-btn di-btn-muted di-btn-xs">
              切换代理模式(/di,/vault)
            </button>
            <button onClick={onSwitchDirectMode} className="di-btn di-btn-muted di-btn-xs">
              切换直连模式(127.0.0.1)
            </button>
          </div>
          <p className="text-xs text-slate-600">建议开发联调使用代理模式，避免浏览器跨域导致看板全 0。</p>
        </div>
      </div>

      <div className="panel di-console-shell p-6">
        <h2 className="text-lg font-semibold text-slate-900">快捷模板</h2>
        <div className="mt-4 grid gap-3 sm:grid-cols-2">
          {quickTemplates.map((tpl) => (
            <button
              key={tpl.name}
              className="rounded-xl border border-indigo-100 bg-white px-4 py-3 text-left text-sm font-medium text-slate-800 transition hover:-translate-y-0.5 hover:border-indigo-300 hover:bg-indigo-50"
              onClick={() => applyTemplate(tpl)}
            >
              {tpl.name}
            </button>
          ))}
        </div>
      </div>
    </section>
  );
}
