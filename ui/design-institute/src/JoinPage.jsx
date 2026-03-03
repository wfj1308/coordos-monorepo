import { useMemo, useState } from "react";

function readLocal(key, fallback) {
  const value = localStorage.getItem(key);
  return value == null ? fallback : value;
}

function saveLocal(key, value) {
  localStorage.setItem(key, value);
}

function trimTrailingSlash(v) {
  return String(v || "").replace(/\/+$/, "");
}

function slugifyNamespace(value) {
  const raw = String(value || "").trim().toLowerCase().replace(/^v:\/\//, "");
  if (!raw) return "";
  let out = "";
  let prevSep = false;
  for (const ch of raw) {
    const isAlphaNum = (ch >= "a" && ch <= "z") || (ch >= "0" && ch <= "9");
    if (isAlphaNum) {
      out += ch;
      prevSep = false;
      continue;
    }
    if (ch === "." || ch === "-" || ch === "_") {
      if (!out || prevSep) continue;
      out += ch === "." ? "." : "-";
      prevSep = true;
    }
  }
  return out.replace(/^[.-]+|[.-]+$/g, "");
}

const QUAL_PRESETS = [
  {
    resourceType: "QUAL_HIGHWAY_INDUSTRY_A",
    qualType: "QUAL_HIGHWAY_INDUSTRY_A",
    name: "公路行业（公路、特大桥梁、特长隧道、交通工程）专业甲级",
    grade: "A",
    industry: "HIGHWAY",
    suffix: "qual/highway_a",
    scopes: ["公路", "特大桥梁", "特长隧道", "交通工程"],
  },
  {
    resourceType: "QUAL_MUNICIPAL_INDUSTRY_A",
    qualType: "QUAL_MUNICIPAL_INDUSTRY_A",
    name: "市政行业（排水工程、城镇燃气工程、道路工程、桥梁工程）专业甲级",
    grade: "A",
    industry: "MUNICIPAL",
    suffix: "qual/municipal_a",
    scopes: ["排水工程", "城镇燃气工程", "道路工程", "桥梁工程"],
  },
  {
    resourceType: "QUAL_ARCH_COMPREHENSIVE_A",
    qualType: "QUAL_ARCH_COMPREHENSIVE_A",
    name: "建筑行业（建筑工程）甲级",
    grade: "A",
    industry: "ARCHITECTURE",
    suffix: "qual/arch_a",
    scopes: ["建筑工程", "建筑装饰工程", "建筑幕墙工程", "轻型钢结构工程", "建筑智能化系统", "照明工程", "消防设施工程"],
  },
  {
    resourceType: "QUAL_LANDSCAPE_SPECIAL_A",
    qualType: "QUAL_LANDSCAPE_SPECIAL_A",
    name: "风景园林工程设计专项甲级",
    grade: "A",
    industry: "LANDSCAPE",
    suffix: "qual/landscape_a",
    scopes: ["风景园林工程设计"],
  },
  {
    resourceType: "QUAL_WATER_INDUSTRY_B",
    qualType: "QUAL_WATER_INDUSTRY_B",
    name: "水利行业乙级",
    grade: "B",
    industry: "WATER",
    suffix: "qual/water_b",
    scopes: ["水利工程"],
  },
];

const ENGINEER_TYPES = [
  "一级注册结构工程师",
  "二级注册结构工程师",
  "一级注册建筑师",
  "注册土木工程师（岩土）",
  "注册土木工程师（水利水电工程）",
  "一级注册造价工程师",
  "注册电气工程师（供配电）",
  "注册电气工程师（发输变电）",
  "注册公用设备工程师（动力）",
  "注册公用设备工程师（给水排水）",
  "注册公用设备工程师（暖通空调）",
];

const EXECUTOR_ROLES = [
  { code: "ROLE_TAX_FILER", label: "税务申报员" },
  { code: "ROLE_ACCOUNTANT", label: "会计" },
  { code: "ROLE_CASHIER", label: "出纳" },
  { code: "ROLE_CONTRACT_ADMIN", label: "合同管理员" },
  { code: "ROLE_PROJECT_MANAGER", label: "项目经理" },
  { code: "ROLE_QUALITY_MANAGER", label: "质量负责人" },
  { code: "ROLE_SCHEDULE_MANAGER", label: "进度管理员" },
  { code: "ROLE_CAD_DRAFTER", label: "CAD制图员" },
  { code: "ROLE_DOC_CONTROLLER", label: "资料员" },
  { code: "ROLE_EXTERNAL_REVIEW_COORDINATOR", label: "外审协调员" },
  { code: "ROLE_MARKET_MANAGER", label: "市场经理" },
  { code: "ROLE_BID_SPECIALIST", label: "投标专员" },
];

function createQualFromPreset(resourceType = QUAL_PRESETS[0].resourceType) {
  const preset = QUAL_PRESETS.find((it) => it.resourceType === resourceType) || QUAL_PRESETS[0];
  return {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    resourceType: preset.resourceType,
    qualType: preset.qualType,
    name: preset.name,
    grade: preset.grade,
    industry: preset.industry,
    scope: [...preset.scopes],
    issuedBy: "住房和城乡建设部",
    verifyUrl: "",
    ruleBinding: ["RULE-001", "RULE-002"],
  };
}

function createManualRow(kind = "engineer") {
  return {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    kind,
    name: "",
    idCard: "",
    registerType: ENGINEER_TYPES[0],
    roleCode: EXECUTOR_ROLES[0].code,
    specialty: "",
    certNo: "",
    position: "",
    maxConcurrent: "",
    validUntil: "",
  };
}

function jsonPreview(v) {
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v);
  }
}

async function requestJSON(url, init) {
  const res = await fetch(url, init);
  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = text;
  }
  if (!res.ok) {
    const detail =
      (data && typeof data === "object" && (data.error || data.message || data.detail)) || text || `HTTP ${res.status}`;
    throw new Error(String(detail));
  }
  return data;
}

export default function JoinPage() {
  const [diBase, setDiBase] = useState(readLocal("coordos.di.base", "/di"));
  const [step, setStep] = useState(1);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [latestResult, setLatestResult] = useState(null);

  const [org, setOrg] = useState({
    shortCode: "cn.zhongbei",
    parentRef: "",
    orgType: "HEAD_OFFICE",
    companyName: "",
    creditCode: "",
    certNo: "",
    certValidUntil: "",
    regCapital: "",
    legalRep: "",
    techDirector: "",
    address: "",
    establishedAt: "",
  });
  const [qualifications, setQualifications] = useState([createQualFromPreset()]);
  const [qualPreset, setQualPreset] = useState(QUAL_PRESETS[0].resourceType);
  const [registerResult, setRegisterResult] = useState(null);

  const [engineerFile, setEngineerFile] = useState(null);
  const [executorFile, setExecutorFile] = useState(null);
  const [defaultValidUntil, setDefaultValidUntil] = useState("2029-12-31");
  const [defaultMaxConcurrent, setDefaultMaxConcurrent] = useState("5");
  const [importMode, setImportMode] = useState("file");
  const [manualRows, setManualRows] = useState([createManualRow("engineer"), createManualRow("executor")]);
  const [engineerImportResult, setEngineerImportResult] = useState(null);
  const [executorImportResult, setExecutorImportResult] = useState(null);

  const [resolverPayload, setResolverPayload] = useState({
    spu_ref: "v://cn.zhongbei/spu/bid/preparation@v1",
    required_quals: "REG_STRUCTURE",
    tenant_id: "10000",
  });
  const [resolverResult, setResolverResult] = useState(null);

  const namespaceCode = useMemo(() => slugifyNamespace(org.shortCode), [org.shortCode]);
  const namespaceRef = namespaceCode ? `v://${namespaceCode}` : "";
  const endpointBase = useMemo(() => trimTrailingSlash(diBase.trim()), [diBase]);
  const importedCount = (engineerImportResult?.success_count || 0) + (executorImportResult?.success_count || 0);
  const allFailures = [
    ...(Array.isArray(engineerImportResult?.failures) ? engineerImportResult.failures : []),
    ...(Array.isArray(executorImportResult?.failures) ? executorImportResult.failures : []),
  ];

  const updateOrg = (key, value) => {
    setOrg((prev) => ({ ...prev, [key]: value }));
  };

  const updateQual = (id, key, value) => {
    setQualifications((prev) => prev.map((q) => (q.id === id ? { ...q, [key]: value } : q)));
  };

  const replaceQualByPreset = (id, resourceType) => {
    const preset = QUAL_PRESETS.find((it) => it.resourceType === resourceType);
    if (!preset) return;
    setQualifications((prev) =>
      prev.map((q) =>
        q.id === id
          ? {
              ...q,
              resourceType: preset.resourceType,
              qualType: preset.qualType,
              name: preset.name,
              grade: preset.grade,
              industry: preset.industry,
              scope: [...preset.scopes],
            }
          : q,
      ),
    );
  };

  const toggleScope = (id, scopeValue, checked) => {
    setQualifications((prev) =>
      prev.map((q) => {
        if (q.id !== id) return q;
        const set = new Set(q.scope || []);
        if (checked) set.add(scopeValue);
        else set.delete(scopeValue);
        return { ...q, scope: [...set] };
      }),
    );
  };

  const callExtract = async (file) => {
    if (!file) return;
    if (!endpointBase) {
      setError("请先设置 Design-Ins Base URL");
      return;
    }
    setBusy(true);
    setError("");
    try {
      const fd = new FormData();
      fd.append("file", file);
      const data = await requestJSON(`${endpointBase}/api/v1/register/cert/extract`, { method: "POST", body: fd });
      setLatestResult(data);
      setOrg((prev) => ({
        ...prev,
        companyName: data.company_name || prev.companyName,
        creditCode: data.credit_code || prev.creditCode,
        certNo: data.cert_no || prev.certNo,
        certValidUntil: data.cert_valid_until || prev.certValidUntil,
        legalRep: data.legal_rep || prev.legalRep,
        techDirector: data.tech_director || prev.techDirector,
        address: data.address || prev.address,
      }));
      if (Array.isArray(data.qualifications) && data.qualifications.length > 0) {
        const next = data.qualifications.map((item) => {
          const preset = QUAL_PRESETS.find((q) => q.resourceType === item.resource_key || q.resourceType === item.qual_type);
          return {
            ...createQualFromPreset(preset?.resourceType || QUAL_PRESETS[0].resourceType),
            resourceType: item.resource_key || item.qual_type || preset?.resourceType || QUAL_PRESETS[0].resourceType,
            qualType: item.qual_type || item.resource_key || preset?.qualType || "",
            name: item.name || preset?.name || "",
            scope: Array.isArray(item.scope) ? item.scope : preset?.scopes || [],
            grade: item.grade || preset?.grade || "",
            industry: item.industry || preset?.industry || "",
          };
        });
        setQualifications(next);
      }
    } catch (err) {
      setError(String(err));
    } finally {
      setBusy(false);
    }
  };

  const registerOrg = async () => {
    if (!endpointBase) {
      setError("请先设置 Design-Ins Base URL");
      return;
    }
    if (!namespaceCode) {
      setError("short code 不能为空");
      return;
    }
    if (qualifications.length === 0) {
      setError("至少添加一条资质");
      return;
    }
    const payload = {
      short_code: namespaceCode,
      namespace_ref: namespaceRef,
      parent_ref: org.parentRef.trim(),
      org_type: org.orgType,
      company_name: org.companyName.trim(),
      credit_code: org.creditCode.trim(),
      cert_no: org.certNo.trim(),
      cert_valid_until: org.certValidUntil.trim(),
      reg_capital: Number(org.regCapital || 0),
      legal_rep: org.legalRep.trim(),
      tech_director: org.techDirector.trim(),
      address: org.address.trim(),
      established_at: org.establishedAt.trim(),
      qualifications: qualifications.map((q) => ({
        resource_type: q.resourceType,
        qual_type: q.qualType,
        name: q.name,
        scope: q.scope || [],
        grade: q.grade,
        industry: q.industry,
        issued_by: q.issuedBy,
        verify_url: q.verifyUrl,
        rule_binding: q.ruleBinding || [],
      })),
    };
    setBusy(true);
    setError("");
    try {
      const data = await requestJSON(`${endpointBase}/api/v1/register/org`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setRegisterResult(data);
      setLatestResult(data);
      setStep(3);
    } catch (err) {
      setError(String(err));
    } finally {
      setBusy(false);
    }
  };

  const uploadImport = async (path, file, extraFields = {}) => {
    const fd = new FormData();
    fd.append("file", file);
    Object.entries(extraFields).forEach(([k, v]) => {
      if (String(v || "").trim()) fd.append(k, String(v));
    });
    return requestJSON(`${endpointBase}${path}`, { method: "POST", body: fd });
  };

  const uploadEngineerFile = async () => {
    if (!engineerFile) return;
    setBusy(true);
    setError("");
    try {
      const data = await uploadImport(`/api/v1/register/org/${namespaceCode}/engineers`, engineerFile, {
        default_valid_until: defaultValidUntil,
      });
      setEngineerImportResult(data);
      setLatestResult(data);
      setStep(4);
    } catch (err) {
      setError(String(err));
    } finally {
      setBusy(false);
    }
  };

  const uploadExecutorFile = async () => {
    if (!executorFile) return;
    setBusy(true);
    setError("");
    try {
      const data = await uploadImport(`/api/v1/register/org/${namespaceCode}/executors`, executorFile, {
        default_valid_until: defaultValidUntil,
        default_max_concurrent_tasks: defaultMaxConcurrent,
      });
      setExecutorImportResult(data);
      setLatestResult(data);
      setStep(4);
    } catch (err) {
      setError(String(err));
    } finally {
      setBusy(false);
    }
  };

  const importManual = async () => {
    const rows = manualRows.filter((row) => row.name.trim() && row.idCard.trim());
    if (rows.length === 0) {
      setError("请至少填写一条执行体记录");
      return;
    }
    const engineers = rows.filter((row) => row.kind === "engineer");
    const executors = rows.filter((row) => row.kind === "executor");
    setBusy(true);
    setError("");
    try {
      if (engineers.length > 0) {
        const lines = ["name,id_card,register_type,specialty,cert_no"];
        engineers.forEach((row) => {
          lines.push([row.name, row.idCard, row.registerType, row.specialty, row.certNo].map((v) => `"${String(v || "").replace(/"/g, "\"\"")}"`).join(","));
        });
        const file = new File([lines.join("\n")], "manual-engineers.csv", { type: "text/csv;charset=utf-8" });
        const out = await uploadImport(`/api/v1/register/org/${namespaceCode}/engineers`, file, {
          default_valid_until: defaultValidUntil,
        });
        setEngineerImportResult(out);
      }
      if (executors.length > 0) {
        const lines = ["name,id_card,role,specialty,position,max_concurrent_projects,cert_no,valid_until"];
        executors.forEach((row) => {
          lines.push(
            [row.name, row.idCard, row.roleCode, row.specialty, row.position, row.maxConcurrent, row.certNo, row.validUntil]
              .map((v) => `"${String(v || "").replace(/"/g, "\"\"")}"`)
              .join(","),
          );
        });
        const file = new File([lines.join("\n")], "manual-executors.csv", { type: "text/csv;charset=utf-8" });
        const out = await uploadImport(`/api/v1/register/org/${namespaceCode}/executors`, file, {
          default_valid_until: defaultValidUntil,
          default_max_concurrent_tasks: defaultMaxConcurrent,
        });
        setExecutorImportResult(out);
      }
      setStep(4);
    } catch (err) {
      setError(String(err));
    } finally {
      setBusy(false);
    }
  };

  const runResolver = async () => {
    setBusy(true);
    setError("");
    try {
      const payload = {
        spu_ref: resolverPayload.spu_ref.trim(),
        required_quals: resolverPayload.required_quals
          .split(",")
          .map((v) => v.trim())
          .filter(Boolean),
        tenant_id: Number(resolverPayload.tenant_id || 10000),
      };
      const out = await requestJSON(`${endpointBase}/api/v1/resolve/resolve`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setResolverResult(out);
      setLatestResult(out);
    } catch (err) {
      setError(String(err));
    } finally {
      setBusy(false);
    }
  };

  const stepDone = {
    1: Boolean(namespaceRef && org.companyName && org.certNo),
    2: Boolean(registerResult?.namespace_ref),
    3: importedCount > 0 || allFailures.length > 0,
    4: step >= 4,
  };

  return (
    <main className="join-page">
      <div className="join-bg-orb join-bg-orb-a" />
      <div className="join-bg-orb join-bg-orb-b" />

      <div className="join-wrap">
        <section className="panel join-hero">
          <p className="join-kicker">coordos.io/join</p>
          <h1>设计院入网注册</h1>
          <p className="join-subtitle">
            四步完成入网: 组织信息 → 资质注册 → 执行体导入 → 激活上线。资质将注册为 Genesis UTXO，人员将注册为可寻址执行体。
          </p>
          <div className="join-badges">
            {[1, 2, 3, 4].map((i) => (
              <button
                key={i}
                type="button"
                className={`join-badge ${stepDone[i] ? "is-done" : "is-todo"}`}
                onClick={() => setStep(i)}
              >
                第{i}步
              </button>
            ))}
          </div>
        </section>

        <section className="panel join-card">
          <div className="join-grid">
            <label className="join-field">
              <span>Design-Ins Base URL</span>
              <input
                value={diBase}
                onChange={(e) => {
                  setDiBase(e.target.value);
                  saveLocal("coordos.di.base", e.target.value);
                }}
                placeholder="/di"
              />
            </label>
            <label className="join-field">
              <span>命名空间预览</span>
              <input value={namespaceRef || "v://{short_code}"} readOnly />
            </label>
          </div>
          {error ? <pre className="join-error">{error}</pre> : null}
        </section>

        {step === 1 ? (
          <section className="panel join-card">
            <div className="join-card-head">
              <h2>步骤一: 组织信息</h2>
              <button className="join-btn ghost" type="button" onClick={() => setStep(2)}>
                下一步
              </button>
            </div>

            <label className="join-field">
              <span>上传资质证书图片 / PDF（自动 OCR）</span>
              <input type="file" accept=".png,.jpg,.jpeg,.pdf,.webp" onChange={(e) => callExtract(e.target.files?.[0])} />
            </label>

            <div className="join-grid">
              <label className="join-field">
                <span>short code</span>
                <input value={org.shortCode} onChange={(e) => updateOrg("shortCode", e.target.value)} placeholder="zhongbei" />
              </label>
              <label className="join-field">
                <span>组织类型</span>
                <select value={org.orgType} onChange={(e) => updateOrg("orgType", e.target.value)}>
                  <option value="HEAD_OFFICE">HEAD_OFFICE</option>
                  <option value="BRANCH">BRANCH</option>
                </select>
              </label>
              <label className="join-field">
                <span>上级命名空间（分院必填）</span>
                <input value={org.parentRef} onChange={(e) => updateOrg("parentRef", e.target.value)} placeholder="v://cn.zhongbei" />
              </label>
              <label className="join-field">
                <span>公司名称</span>
                <input value={org.companyName} onChange={(e) => updateOrg("companyName", e.target.value)} />
              </label>
              <label className="join-field">
                <span>统一社会信用代码</span>
                <input value={org.creditCode} onChange={(e) => updateOrg("creditCode", e.target.value)} />
              </label>
              <label className="join-field">
                <span>证书号</span>
                <input value={org.certNo} onChange={(e) => updateOrg("certNo", e.target.value)} />
              </label>
              <label className="join-field">
                <span>证书有效期</span>
                <input type="date" value={org.certValidUntil} onChange={(e) => updateOrg("certValidUntil", e.target.value)} />
              </label>
              <label className="join-field">
                <span>注册资本（元）</span>
                <input value={org.regCapital} onChange={(e) => updateOrg("regCapital", e.target.value)} />
              </label>
              <label className="join-field">
                <span>法定代表人</span>
                <input value={org.legalRep} onChange={(e) => updateOrg("legalRep", e.target.value)} />
              </label>
              <label className="join-field">
                <span>技术负责人</span>
                <input value={org.techDirector} onChange={(e) => updateOrg("techDirector", e.target.value)} />
              </label>
              <label className="join-field">
                <span>成立日期</span>
                <input type="date" value={org.establishedAt} onChange={(e) => updateOrg("establishedAt", e.target.value)} />
              </label>
              <label className="join-field">
                <span>地址</span>
                <input value={org.address} onChange={(e) => updateOrg("address", e.target.value)} />
              </label>
            </div>
          </section>
        ) : null}

        {step === 2 ? (
          <section className="panel join-card">
            <div className="join-card-head">
              <h2>步骤二: 资质注册</h2>
              <div className="join-actions">
                <button className="join-btn ghost" type="button" onClick={() => setStep(1)}>
                  上一步
                </button>
                <button className="join-btn" type="button" onClick={registerOrg} disabled={busy}>
                  {busy ? "注册中..." : "提交组织注册"}
                </button>
              </div>
            </div>

            <div className="join-qual-head">
              <h3>资质清单</h3>
              <div className="join-actions">
                <select value={qualPreset} onChange={(e) => setQualPreset(e.target.value)}>
                  {QUAL_PRESETS.map((it) => (
                    <option key={it.resourceType} value={it.resourceType}>
                      {it.name}
                    </option>
                  ))}
                </select>
                <button className="join-btn ghost" type="button" onClick={() => setQualifications((prev) => [...prev, createQualFromPreset(qualPreset)])}>
                  新增资质
                </button>
              </div>
            </div>

            <div className="join-qual-list">
              {qualifications.map((q, idx) => {
                const preset = QUAL_PRESETS.find((it) => it.resourceType === q.resourceType) || QUAL_PRESETS[0];
                return (
                  <article key={q.id} className="join-qual-item">
                    <div className="join-qual-grid">
                      <label className="join-field">
                        <span>资质类型</span>
                        <select value={q.resourceType} onChange={(e) => replaceQualByPreset(q.id, e.target.value)}>
                          {QUAL_PRESETS.map((it) => (
                            <option key={it.resourceType} value={it.resourceType}>
                              {it.name}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="join-field">
                        <span>资质名称</span>
                        <input value={q.name} onChange={(e) => updateQual(q.id, "name", e.target.value)} />
                      </label>
                      <label className="join-field">
                        <span>等级</span>
                        <input value={q.grade} onChange={(e) => updateQual(q.id, "grade", e.target.value)} />
                      </label>
                      <label className="join-field">
                        <span>行业</span>
                        <input value={q.industry} onChange={(e) => updateQual(q.id, "industry", e.target.value)} />
                      </label>
                    </div>
                    <div className="join-scope-group">
                      <div className="join-scope-title">业务范围</div>
                      <div className="join-scope-options">
                        {preset.scopes.map((scope) => (
                          <label key={`${q.id}-${scope}`} className="join-scope-option">
                            <input type="checkbox" checked={(q.scope || []).includes(scope)} onChange={(e) => toggleScope(q.id, scope, e.target.checked)} />
                            <span>{scope}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                    <p className="join-hint">
                      Genesis 预览:{" "}
                      <code>
                        {namespaceRef || "v://{short_code}"}/genesis/{preset.suffix}
                      </code>
                    </p>
                    <button
                      className="join-link-btn danger"
                      type="button"
                      onClick={() => setQualifications((prev) => prev.filter((item) => item.id !== q.id))}
                      disabled={qualifications.length <= 1}
                    >
                      删除该资质 #{idx + 1}
                    </button>
                  </article>
                );
              })}
            </div>

            {registerResult ? <pre className="join-json">{jsonPreview(registerResult)}</pre> : null}
          </section>
        ) : null}

        {step === 3 ? (
          <section className="panel join-card">
            <div className="join-card-head">
              <h2>步骤三: 执行体导入</h2>
              <div className="join-actions">
                <button className="join-btn ghost" type="button" onClick={() => setStep(2)}>
                  上一步
                </button>
                <button className="join-btn ghost" type="button" onClick={() => setStep(4)}>
                  跳到激活结果
                </button>
              </div>
            </div>

            <div className="join-mode-tabs">
              <button type="button" className={`join-tab ${importMode === "file" ? "active" : ""}`} onClick={() => setImportMode("file")}>
                CSV/XLSX 批量导入
              </button>
              <button type="button" className={`join-tab ${importMode === "manual" ? "active" : ""}`} onClick={() => setImportMode("manual")}>
                手动逐行录入
              </button>
            </div>

            <div className="join-grid">
              <label className="join-field">
                <span>默认证书有效期</span>
                <input type="date" value={defaultValidUntil} onChange={(e) => setDefaultValidUntil(e.target.value)} />
              </label>
              <label className="join-field">
                <span>默认并发上限（非工程角色）</span>
                <input value={defaultMaxConcurrent} onChange={(e) => setDefaultMaxConcurrent(e.target.value)} />
              </label>
            </div>

            {importMode === "file" ? (
              <div className="join-mode-content">
                <div className="join-grid">
                  <article className="join-mini-preview">
                    <h3>导入注册工程师</h3>
                    <p>支持 CSV/XLSX，自动识别注册类别文字。</p>
                    <input type="file" accept=".csv,.xlsx" onChange={(e) => setEngineerFile(e.target.files?.[0] || null)} />
                    <button className="join-btn" type="button" onClick={uploadEngineerFile} disabled={!engineerFile || busy}>
                      上传工程师文件
                    </button>
                  </article>
                  <article className="join-mini-preview">
                    <h3>导入非工程执行体</h3>
                    <p>税务、财务、项目管理、经营、资料等角色。</p>
                    <input type="file" accept=".csv,.xlsx" onChange={(e) => setExecutorFile(e.target.files?.[0] || null)} />
                    <button className="join-btn" type="button" onClick={uploadExecutorFile} disabled={!executorFile || busy}>
                      上传执行体文件
                    </button>
                  </article>
                </div>
              </div>
            ) : (
              <div className="join-mode-content">
                <div className="join-rows-actions">
                  <button className="join-btn ghost" type="button" onClick={() => setManualRows((prev) => [...prev, createManualRow("engineer")])}>
                    新增工程师行
                  </button>
                  <button className="join-btn ghost" type="button" onClick={() => setManualRows((prev) => [...prev, createManualRow("executor")])}>
                    新增执行体行
                  </button>
                  <button className="join-btn" type="button" onClick={importManual} disabled={busy}>
                    {busy ? "导入中..." : "提交手动录入"}
                  </button>
                </div>
                <div className="join-manual-table">
                  <div className="join-manual-head">
                    <span>类型</span>
                    <span>姓名</span>
                    <span>身份证号</span>
                    <span>注册类别/角色</span>
                    <span>专业方向</span>
                    <span>证号</span>
                    <span>岗位</span>
                    <span>并发</span>
                    <span>有效期</span>
                    <span>操作</span>
                  </div>
                  {manualRows.map((row) => (
                    <div className="join-manual-row" key={row.id}>
                      <select value={row.kind} onChange={(e) => setManualRows((prev) => prev.map((it) => (it.id === row.id ? { ...it, kind: e.target.value } : it)))}>
                        <option value="engineer">工程师</option>
                        <option value="executor">执行体</option>
                      </select>
                      <input value={row.name} onChange={(e) => setManualRows((prev) => prev.map((it) => (it.id === row.id ? { ...it, name: e.target.value } : it)))} />
                      <input value={row.idCard} onChange={(e) => setManualRows((prev) => prev.map((it) => (it.id === row.id ? { ...it, idCard: e.target.value } : it)))} />
                      {row.kind === "engineer" ? (
                        <select value={row.registerType} onChange={(e) => setManualRows((prev) => prev.map((it) => (it.id === row.id ? { ...it, registerType: e.target.value } : it)))}>
                          {ENGINEER_TYPES.map((it) => (
                            <option key={it} value={it}>
                              {it}
                            </option>
                          ))}
                        </select>
                      ) : (
                        <select value={row.roleCode} onChange={(e) => setManualRows((prev) => prev.map((it) => (it.id === row.id ? { ...it, roleCode: e.target.value } : it)))}>
                          {EXECUTOR_ROLES.map((it) => (
                            <option key={it.code} value={it.code}>
                              {it.label}
                            </option>
                          ))}
                        </select>
                      )}
                      <input value={row.specialty} onChange={(e) => setManualRows((prev) => prev.map((it) => (it.id === row.id ? { ...it, specialty: e.target.value } : it)))} />
                      <input value={row.certNo} onChange={(e) => setManualRows((prev) => prev.map((it) => (it.id === row.id ? { ...it, certNo: e.target.value } : it)))} />
                      <input value={row.position} onChange={(e) => setManualRows((prev) => prev.map((it) => (it.id === row.id ? { ...it, position: e.target.value } : it)))} />
                      <input value={row.maxConcurrent} onChange={(e) => setManualRows((prev) => prev.map((it) => (it.id === row.id ? { ...it, maxConcurrent: e.target.value } : it)))} />
                      <input value={row.validUntil} onChange={(e) => setManualRows((prev) => prev.map((it) => (it.id === row.id ? { ...it, validUntil: e.target.value } : it)))} />
                      <button className="join-link-btn danger" type="button" onClick={() => setManualRows((prev) => prev.filter((it) => it.id !== row.id))}>
                        删除
                      </button>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {engineerImportResult ? <pre className="join-json">{jsonPreview(engineerImportResult)}</pre> : null}
            {executorImportResult ? <pre className="join-json">{jsonPreview(executorImportResult)}</pre> : null}
          </section>
        ) : null}

        {step === 4 ? (
          <section className="panel join-card">
            <div className="join-card-head">
              <h2>步骤四: 激活完成</h2>
              <div className="join-actions">
                <a className="join-link-btn" href={`/partner-profile.html?namespace=${namespaceCode}`} target="_blank" rel="noreferrer">
                  打开能力声明页
                </a>
              </div>
            </div>

            <div className="join-result-grid">
              <article>
                <p>命名空间</p>
                <strong>{registerResult?.namespace_ref || namespaceRef || "-"}</strong>
              </article>
              <article>
                <p>激活资质 UTXO</p>
                <strong>{Array.isArray(registerResult?.qualification_genesis) ? registerResult.qualification_genesis.length : 0}</strong>
              </article>
              <article>
                <p>注册人员数</p>
                <strong>{importedCount}</strong>
              </article>
              <article>
                <p>导入失败数</p>
                <strong>{allFailures.length}</strong>
              </article>
            </div>

            <div className="join-grid">
              <article className="join-log-card">
                <h3>Genesis UTXO 列表</h3>
                <ul className="join-ref-list">
                  {[
                    ...(Array.isArray(registerResult?.qualification_genesis) ? registerResult.qualification_genesis : []),
                    ...(Array.isArray(registerResult?.right_genesis) ? registerResult.right_genesis : []),
                  ].map((ref) => (
                    <li key={ref}>
                      <code>{ref}</code>
                    </li>
                  ))}
                </ul>
              </article>
              <article className="join-log-card">
                <h3>导入失败明细</h3>
                <ul className="join-ref-list">
                  {allFailures.length === 0 ? <li>无失败记录</li> : null}
                  {allFailures.map((it, idx) => (
                    <li key={`f-${idx}`}>
                      <code>{`row=${it.row || "-"} ${it.name || ""} ${it.reason || ""}`.trim()}</code>
                    </li>
                  ))}
                </ul>
              </article>
            </div>

            <article className="join-resolver">
              <h3>Resolver 实测</h3>
              <div className="join-grid">
                <label className="join-field">
                  <span>spu_ref</span>
                  <input value={resolverPayload.spu_ref} onChange={(e) => setResolverPayload((prev) => ({ ...prev, spu_ref: e.target.value }))} />
                </label>
                <label className="join-field">
                  <span>required_quals（逗号分隔）</span>
                  <input
                    value={resolverPayload.required_quals}
                    onChange={(e) => setResolverPayload((prev) => ({ ...prev, required_quals: e.target.value }))}
                  />
                </label>
                <label className="join-field">
                  <span>tenant_id</span>
                  <input value={resolverPayload.tenant_id} onChange={(e) => setResolverPayload((prev) => ({ ...prev, tenant_id: e.target.value }))} />
                </label>
              </div>
              <div className="join-actions">
                <button className="join-btn" type="button" onClick={runResolver} disabled={busy}>
                  {busy ? "执行中..." : "执行 Resolver 测试"}
                </button>
              </div>
              {resolverResult ? <pre className="join-json">{jsonPreview(resolverResult)}</pre> : null}
            </article>
          </section>
        ) : null}

        {latestResult ? (
          <section className="panel join-card">
            <div className="join-card-head">
              <h2>最近一次响应</h2>
            </div>
            <pre className="join-json">{jsonPreview(latestResult)}</pre>
          </section>
        ) : null}
      </div>
    </main>
  );
}
