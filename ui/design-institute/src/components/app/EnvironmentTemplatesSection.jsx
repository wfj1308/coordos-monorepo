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
    <section className="grid gap-6 lg:grid-cols-2">
      <div className="panel p-6">
        <h2 className="text-lg font-semibold">环境配置</h2>
        <div className="mt-4 space-y-4">
          <Input label="Design-Ins Base URL" value={diBase} onChange={onDiBaseChange} />
          <Input label="Vault Base URL" value={vaultBase} onChange={onVaultBaseChange} />
          <label className="flex items-center gap-2 text-xs text-slate">
            <input type="checkbox" checked={useAuth} onChange={(e) => onUseAuthChange(e.target.checked)} />
            请求附带 Bearer Token
          </label>
          <Input label="Bearer Token (通用控制台用)" value={token} onChange={onTokenChange} />
          <div className="flex flex-wrap gap-2">
            <button onClick={onSwitchProxyMode} className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs">
              切换代理模式(/di,/vault)
            </button>
            <button onClick={onSwitchDirectMode} className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs">
              切换直连模式(127.0.0.1)
            </button>
          </div>
          <p className="text-xs text-slate">
            建议开发联调使用代理模式，避免浏览器跨域导致看板全 0。
          </p>
        </div>
      </div>

      <div className="panel p-6">
        <h2 className="text-lg font-semibold">快捷模板</h2>
        <div className="mt-4 grid gap-3 sm:grid-cols-2">
          {quickTemplates.map((tpl) => (
            <button
              key={tpl.name}
              className="rounded-xl border border-slate-300 bg-white px-4 py-3 text-left text-sm transition hover:border-sky-500 hover:bg-sky-50"
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
