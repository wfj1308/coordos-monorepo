import { useMemo, useState } from "react";

function readLocal(key, fallback) {
  const v = localStorage.getItem(key);
  return v == null ? fallback : v;
}

function saveLocal(key, value) {
  localStorage.setItem(key, value);
}

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

const flowBlueprint = [
  { key: "health", title: "服务健康检查", detail: "确认 design-institute 可用" },
  { key: "companies", title: "读取公司基座", detail: "取公司列表用于后续人员归属" },
  { key: "project", title: "创建项目", detail: "创建根项目节点（ProjectNode）" },
  { key: "projectStatus", title: "推进项目状态", detail: "INITIATED -> TENDERING -> CONTRACTED -> IN_PROGRESS" },
  { key: "contract", title: "创建合同", detail: "合同绑定 project_ref，形成项目-合同锚点" },
  { key: "employee", title: "创建人员", detail: "创建核心执行人员并生成 canonical executor_ref" },
  { key: "qualification", title: "创建资质", detail: "给执行体挂资质证书（REG_STRUCTURE）" },
  { key: "qualAssign", title: "项目用证绑定", detail: "把证书绑定到该项目（assignment）" },
  { key: "achievement", title: "录入业绩", detail: "手动产出 achievement UTXO（业绩）" },
  { key: "gathering", title: "创建收款", detail: "合同/项目维度录入回款" },
  { key: "invoice", title: "发票流转", detail: "创建发票并 submit/approve/issue" },
  { key: "settlement", title: "结算流转", detail: "创建结算并 submit/approve/pay" },
  { key: "resources", title: "项目证据包", detail: "读取资源聚合，验证闭环结果" },
  { key: "dbVerify", title: "回读落库校验", detail: "逐条回读后端数据并校验项目/合同/财务关联" },
];

function buildFlowSteps() {
  return flowBlueprint.map((it) => ({
    ...it,
    status: "pending",
    elapsedMs: null,
    result: null,
    error: "",
  }));
}

function buildDashboardData() {
  return {
    projects: [],
    contracts: [],
    employees: [],
    qualifications: [],
    achievements: [],
    gatherings: [],
    invoices: [],
    settlements: [],
    projectResources: null,
    qualificationAssignments: [],
    updatedAt: "",
  };
}

export default function App() {
  const [diBase, setDiBase] = useState(readLocal("coordos.di.base", "/di"));
  const [vaultBase, setVaultBase] = useState(readLocal("coordos.vault.base", "/vault"));
  const [token, setToken] = useState(
    readLocal("coordos.token", ""),
  );
  const [useAuth, setUseAuth] = useState(readLocal("coordos.use.auth", "0") === "1");

  const [method, setMethod] = useState("GET");
  const [url, setUrl] = useState("{DI}/health");
  const [body, setBody] = useState("");
  const [response, setResponse] = useState("");
  const [pending, setPending] = useState(false);

  const [flowSteps, setFlowSteps] = useState(buildFlowSteps());
  const [flowRunning, setFlowRunning] = useState(false);
  const [flowSummary, setFlowSummary] = useState(null);
  const [dashboard, setDashboard] = useState(buildDashboardData());
  const [dashboardLoading, setDashboardLoading] = useState(false);
  const [dashboardError, setDashboardError] = useState("");
  const [selectedProjectRef, setSelectedProjectRef] = useState("");
  const [projectDetailLoading, setProjectDetailLoading] = useState(false);

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

  const runMainFlow = async () => {
    if (flowRunning) return;

    const di = trimTrailingSlash(diBase.trim());
    if (!di) {
      setResponse(JSON.stringify({ error: "Design-Ins Base URL 不能为空" }, null, 2));
      return;
    }

    const runCode = `ZB-${Date.now()}`;
    const today = new Date();
    const isoNow = today.toISOString();
    const dateOnly = isoNow.slice(0, 10);

    const ctx = {
      runCode,
      startedAt: isoNow,
      companyId: null,
      projectRef: "",
      contractId: null,
      contractRef: "",
      employeeId: null,
      executorRef: "",
      qualificationId: null,
      achievementId: null,
      gatheringId: null,
      invoiceId: null,
      settlementId: null,
      resources: null,
    };

    setFlowSteps(buildFlowSteps());
    setFlowSummary(null);
    setFlowRunning(true);

    const markStep = (idx, patch) => {
      setFlowSteps((prev) => prev.map((s, i) => (i === idx ? { ...s, ...patch } : s)));
    };

    const runStep = async (idx, task) => {
      markStep(idx, { status: "running", error: "", result: null, elapsedMs: null });
      const started = performance.now();
      try {
        const result = await task();
        markStep(idx, {
          status: "done",
          result,
          elapsedMs: Math.round(performance.now() - started),
        });
      } catch (err) {
        markStep(idx, {
          status: "failed",
          error: String(err),
          elapsedMs: Math.round(performance.now() - started),
        });
        throw err;
      }
    };

    try {
      await runStep(0, async () => {
        const res = await apiRequest({ method: "GET", url: `${di}/health` });
        return { status: res.status, service: pickField(res.data, ["service"], "design-institute") };
      });

      await runStep(1, async () => {
        const res = await apiRequest({ method: "GET", url: `${di}/api/v1/companies?limit=5&offset=0` });
        const items = asArray(pickField(res.data, ["items", "Items"], []));
        if (items.length > 0) {
          ctx.companyId = toInt(pickField(items[0], ["id", "ID"], 0));
        }
        return { picked_company_id: ctx.companyId, total: toInt(pickField(res.data, ["total", "Total"], items.length)) };
      });

      await runStep(2, async () => {
        const payload = {
          Name: `zb-bridge-main-${runCode}`,
          OwnerRef: "v://10000/company/client-a",
          ContractorRef: "v://10000/company/head-office",
          ExecutorRef: "v://zhongbei/executor/headquarters",
          PlatformRef: "v://10000/platform/coordos",
        };
        const res = await apiRequest({ method: "POST", url: `${di}/api/v1/projects`, body: payload });
        ctx.projectRef = String(pickField(res.data, ["Ref", "ref"], ""));
        if (!ctx.projectRef) throw new Error("创建项目成功但未返回 ref");
        return { project_ref: ctx.projectRef };
      });

      await runStep(3, async () => {
        await apiRequest({
          method: "PUT",
          url: `${di}/api/v1/projects/status?ref=${encodeURIComponent(ctx.projectRef)}`,
          body: { status: "TENDERING" },
        });
        await apiRequest({
          method: "PUT",
          url: `${di}/api/v1/projects/status?ref=${encodeURIComponent(ctx.projectRef)}`,
          body: { status: "CONTRACTED" },
        });
        await apiRequest({
          method: "PUT",
          url: `${di}/api/v1/projects/status?ref=${encodeURIComponent(ctx.projectRef)}`,
          body: { status: "IN_PROGRESS" },
        });
        return { status: "IN_PROGRESS" };
      });

      await runStep(4, async () => {
        const payload = {
          Num: `ZB-CON-${runCode}`,
          ContractName: `zb-bridge-contract-${runCode}`,
          ContractBalance: 12800000,
          ManageRatio: 6,
          SigningSubject: "Zhongbei Design Institute",
          PayType: 1,
          ContractType: "BID",
          project_ref: ctx.projectRef,
        };
        if (ctx.companyId) payload.CompanyID = ctx.companyId;
        const res = await apiRequest({ method: "POST", url: `${di}/api/v1/contracts`, body: payload });
        ctx.contractId = toInt(pickField(res.data, ["ID", "id"], 0));
        if (!ctx.contractId) throw new Error("创建合同成功但未返回 id");
        ctx.contractRef = `v://10000/contract/${ctx.contractId}`;
        return { contract_id: ctx.contractId, contract_ref: ctx.contractRef };
      });

      await runStep(5, async () => {
        const payload = {
          Name: `结构负责人-${runCode.slice(-6)}`,
          Phone: "13800001234",
          Position: "结构负责人",
          PersonIdentity: `11010519900101${runCode.slice(-4)}X`,
        };
        if (ctx.companyId) payload.CompanyID = ctx.companyId;
        const res = await apiRequest({ method: "POST", url: `${di}/api/v1/employees`, body: payload });
        ctx.employeeId = toInt(pickField(res.data, ["ID", "id"], 0));
        ctx.executorRef = String(pickField(res.data, ["ExecutorRef", "executor_ref"], ""));
        if (!ctx.employeeId) throw new Error("创建人员成功但未返回 id");
        if (!ctx.executorRef) {
          const g = await apiRequest({ method: "GET", url: `${di}/api/v1/employees/${ctx.employeeId}` });
          ctx.executorRef = String(pickField(g.data, ["ExecutorRef", "executor_ref"], ""));
        }
        if (!ctx.executorRef) throw new Error("人员缺少 executor_ref");
        return { employee_id: ctx.employeeId, executor_ref: ctx.executorRef };
      });

      await runStep(6, async () => {
        const payload = {
          HolderType: "PERSON",
          HolderID: ctx.employeeId,
          ExecutorRef: ctx.executorRef,
          QualType: "REG_STRUCTURE",
          CertNo: `REG-ST-${runCode}`,
          IssuedBy: "住建主管部门",
          ValidFrom: "2025-01-01T00:00:00Z",
          ValidUntil: "2030-12-31T00:00:00Z",
          Specialty: "桥梁",
          Level: "一级",
          Scope: "桥梁结构设计/审图",
        };
        const res = await apiRequest({ method: "POST", url: `${di}/api/v1/qualifications`, body: payload });
        ctx.qualificationId = toInt(pickField(res.data, ["ID", "id"], 0));
        if (!ctx.qualificationId) throw new Error("创建资质成功但未返回 id");
        return { qualification_id: ctx.qualificationId, qual_type: "REG_STRUCTURE" };
      });

      await runStep(7, async () => {
        const payload = {
          qualification_id: ctx.qualificationId,
          executor_ref: ctx.executorRef,
          project_ref: ctx.projectRef,
        };
        const res = await apiRequest({ method: "POST", url: `${di}/api/v1/qualifications/assignments`, body: payload });
        return {
          assignment_id: toInt(pickField(res.data, ["ID", "id"], 0)),
          project_ref: ctx.projectRef,
        };
      });

      await runStep(8, async () => {
        const payload = {
          spu_ref: "v://zhongbei/spu/bridge/pile_foundation_drawing@v1",
          project_ref: ctx.projectRef,
          executor_ref: ctx.executorRef,
          contract_id: ctx.contractId,
          payload: {
            drawing_no: `BRIDGE-DRW-${runCode}`,
            amount: 520000,
            stage: "结构审图完成",
          },
        };
        const res = await apiRequest({ method: "POST", url: `${di}/api/v1/achievements/manual`, body: payload });
        ctx.achievementId = toInt(pickField(res.data, ["ID", "id"], 0));
        return {
          achievement_id: ctx.achievementId,
          utxo_ref: pickField(res.data, ["UTXORef", "utxo_ref"], ""),
        };
      });

      await runStep(9, async () => {
        const payload = {
          GatheringMoney: 1200000,
          GatheringDate: dateOnly,
          GatheringType: "BANK",
          GatheringPerson: "财务主管",
          ContractID: ctx.contractId,
          ProjectRef: ctx.projectRef,
          ManageRatio: 6,
          Note: "首期回款",
        };
        if (ctx.companyId) payload.CompanyID = ctx.companyId;
        if (ctx.employeeId) payload.EmployeeID = ctx.employeeId;
        const res = await apiRequest({ method: "POST", url: `${di}/api/v1/gatherings`, body: payload });
        ctx.gatheringId = toInt(pickField(res.data, ["ID", "id"], 0));
        return { gathering_id: ctx.gatheringId, amount: 1200000 };
      });

      await runStep(10, async () => {
        const invoiceRes = await apiRequest({
          method: "POST",
          url: `${di}/api/v1/invoices`,
          body: {
            InvoiceType: "VAT",
            InvoiceContent: "桥梁设计服务费",
            CurAmount: 300000,
            ContractID: ctx.contractId,
            ProjectRef: ctx.projectRef,
            EmployeeID: ctx.employeeId,
            Note: "首期开票",
          },
        });
        ctx.invoiceId = toInt(pickField(invoiceRes.data, ["ID", "id"], 0));
        if (!ctx.invoiceId) throw new Error("创建发票成功但未返回 id");

        await apiRequest({ method: "PUT", url: `${di}/api/v1/invoices/${ctx.invoiceId}/submit`, body: {} });
        await apiRequest({ method: "PUT", url: `${di}/api/v1/invoices/${ctx.invoiceId}/approve`, body: {} });
        await apiRequest({
          method: "PUT",
          url: `${di}/api/v1/invoices/${ctx.invoiceId}/issue`,
          body: {
            code: `INV-${runCode.slice(-8)}`,
            number: `NO-${runCode.slice(-8)}`,
            date: dateOnly,
          },
        });
        return { invoice_id: ctx.invoiceId, state: "ISSUED" };
      });

      await runStep(11, async () => {
        const settlementRes = await apiRequest({
          method: "POST",
          url: `${di}/api/v1/settlements`,
          body: {
            ContractID: ctx.contractId,
            GatheringID: ctx.gatheringId,
            ProjectRef: ctx.projectRef,
            GatheringMoney: 1200000,
            BankSettlement: 800000,
            CashSettlement: 0,
            VATRate: "6",
            DeductRate: "0",
            ManagementCostSum: 48000,
            EmployeeID: ctx.employeeId,
            Note: "首期结算",
          },
        });
        ctx.settlementId = toInt(pickField(settlementRes.data, ["ID", "id"], 0));
        if (!ctx.settlementId) throw new Error("创建结算成功但未返回 id");

        await apiRequest({ method: "PUT", url: `${di}/api/v1/settlements/${ctx.settlementId}/submit`, body: {} });
        await apiRequest({ method: "PUT", url: `${di}/api/v1/settlements/${ctx.settlementId}/approve`, body: {} });
        await apiRequest({
          method: "PUT",
          url: `${di}/api/v1/settlements/${ctx.settlementId}/pay`,
          body: {
            bank_id: 1,
            pay_date: `${dateOnly}T10:00:00Z`,
          },
        });
        return { settlement_id: ctx.settlementId, state: "PAID" };
      });

      await runStep(12, async () => {
        const url = `${di}/api/v1/projects/${encodeURIComponent(ctx.projectRef)}/resources`;
        const res = await apiRequest({ method: "GET", url });
        ctx.resources = res.data;
        return {
          project_ref: ctx.projectRef,
          contracts: safeLen(pickField(res.data, ["contracts"], [])),
          achievements: safeLen(pickField(res.data, ["achievements"], [])),
          settlements: safeLen(pickField(res.data, ["settlements"], [])),
          invoices: safeLen(pickField(res.data, ["invoices"], [])),
        };
      });

      await runStep(13, async () => {
        const projectGet = await apiRequest({
          method: "GET",
          url: `${di}/api/v1/projects/get?ref=${encodeURIComponent(ctx.projectRef)}`,
        });
        const contractGet = await apiRequest({ method: "GET", url: `${di}/api/v1/contracts/${ctx.contractId}` });
        const employeeGet = await apiRequest({ method: "GET", url: `${di}/api/v1/employees/${ctx.employeeId}` });
        const qualificationGet = await apiRequest({
          method: "GET",
          url: `${di}/api/v1/qualifications/${ctx.qualificationId}`,
        });
        const achievementGet = await apiRequest({
          method: "GET",
          url: `${di}/api/v1/achievements/${ctx.achievementId}`,
        });
        const gatheringGet = await apiRequest({ method: "GET", url: `${di}/api/v1/gatherings/${ctx.gatheringId}` });
        const invoiceGet = await apiRequest({ method: "GET", url: `${di}/api/v1/invoices/${ctx.invoiceId}` });
        const settlementGet = await apiRequest({
          method: "GET",
          url: `${di}/api/v1/settlements/${ctx.settlementId}`,
        });

        const projectContractRef = String(
          pickField(projectGet.data, ["ContractRef", "contract_ref"], ""),
        );
        const contractProjectRef = String(
          pickField(contractGet.data, ["ProjectRef", "project_ref"], ""),
        );
        const employeeExecutorRef = String(
          pickField(employeeGet.data, ["ExecutorRef", "executor_ref"], ""),
        );
        const qualExecutorRef = String(
          pickField(qualificationGet.data, ["ExecutorRef", "executor_ref"], ""),
        );
        const achievementProjectRef = String(
          pickField(achievementGet.data, ["ProjectRef", "project_ref"], ""),
        );
        const achievementContractId = toInt(
          pickField(achievementGet.data, ["ContractID", "contract_id"], 0),
        );
        const gatheringContractId = toInt(pickField(gatheringGet.data, ["ContractID", "contract_id"], 0));
        const invoiceContractId = toInt(pickField(invoiceGet.data, ["ContractID", "contract_id"], 0));
        const settlementContractId = toInt(
          pickField(settlementGet.data, ["ContractID", "contract_id"], 0),
        );
        const settlementGatheringId = toInt(
          pickField(settlementGet.data, ["GatheringID", "gathering_id"], 0),
        );
        const settlementState = String(pickField(settlementGet.data, ["State", "state"], ""));

        const checks = {
          project_contract_ref_ok: projectContractRef === ctx.contractRef,
          contract_project_ref_ok: contractProjectRef === ctx.projectRef,
          employee_executor_ref_ok: employeeExecutorRef === ctx.executorRef,
          qualification_executor_ref_ok: qualExecutorRef === ctx.executorRef,
          achievement_project_ref_ok: achievementProjectRef === ctx.projectRef,
          achievement_contract_ok: achievementContractId === ctx.contractId,
          gathering_contract_ok: gatheringContractId === ctx.contractId,
          invoice_contract_ok: invoiceContractId === ctx.contractId,
          settlement_contract_ok: settlementContractId === ctx.contractId,
          settlement_gathering_ok: settlementGatheringId === ctx.gatheringId,
          settlement_paid_ok: settlementState === "PAID",
          resources_has_contract: hasEntityWithId(pickField(ctx.resources, ["contracts"], []), ctx.contractId),
          resources_has_achievement: hasEntityWithId(
            pickField(ctx.resources, ["achievements"], []),
            ctx.achievementId,
          ),
          resources_has_invoice: hasEntityWithId(pickField(ctx.resources, ["invoices"], []), ctx.invoiceId),
          resources_has_settlement: hasEntityWithId(
            pickField(ctx.resources, ["settlements"], []),
            ctx.settlementId,
          ),
        };

        const failed = Object.entries(checks)
          .filter(([, ok]) => !ok)
          .map(([k]) => k);
        if (failed.length > 0) {
          throw new Error(`落库校验失败: ${failed.join(", ")}`);
        }

        return {
          linked: true,
          project_ref: ctx.projectRef,
          contract_ref: ctx.contractRef,
          contract_id: ctx.contractId,
          employee_id: ctx.employeeId,
          qualification_id: ctx.qualificationId,
          achievement_id: ctx.achievementId,
          gathering_id: ctx.gatheringId,
          invoice_id: ctx.invoiceId,
          settlement_id: ctx.settlementId,
          checks,
        };
      });

      const doneAt = new Date().toISOString();
      setFlowSummary({ ...ctx, doneAt, status: "success" });
      setResponse(JSON.stringify({ scenario: "中北设计院业务闭环", status: "success", context: ctx }, null, 2));
    } catch (err) {
      setFlowSummary({ ...ctx, doneAt: new Date().toISOString(), status: "failed", error: String(err) });
      setResponse(JSON.stringify({ scenario: "中北设计院业务闭环", status: "failed", error: String(err), context: ctx }, null, 2));
    } finally {
      setFlowRunning(false);
    }
  };

  const loadProjectDetail = async (projectRef) => {
    const di = trimTrailingSlash(diBase.trim());
    if (!di || !projectRef) return;

    setProjectDetailLoading(true);
    setDashboardError("");
    try {
      const [resourcesRes, assignmentsRes] = await Promise.all([
        apiRequest({
          method: "GET",
          url: `${di}/api/v1/projects/${encodeURIComponent(projectRef)}/resources`,
          token: useAuth ? token : "",
        }),
        apiRequest({
          method: "GET",
          url: `${di}/api/v1/projects/${encodeURIComponent(projectRef)}/qualification-assignments`,
          token: useAuth ? token : "",
        }),
      ]);

      setDashboard((prev) => ({
        ...prev,
        projectResources: resourcesRes.data,
        qualificationAssignments: normalizeListData(assignmentsRes.data),
        updatedAt: new Date().toISOString(),
      }));
    } catch (err) {
      setDashboardError(String(err));
    } finally {
      setProjectDetailLoading(false);
    }
  };

  const listWithFallback = async (path, warnings) => {
    const primary = trimTrailingSlash(diBase.trim());
    const direct = "http://127.0.0.1:8090";
    const authToken = useAuth ? token : "";

    const tryBase = async (base) => {
      const res = await apiRequest({
        method: "GET",
        url: `${base}${path}`,
        token: authToken,
      });
      return normalizeListData(res.data);
    };

    try {
      const primaryRows = await tryBase(primary);
      if (primaryRows.length > 0) return primaryRows;
      if (primary.startsWith("/") && primary !== direct) {
        const directRows = await tryBase(direct);
        if (directRows.length > 0) {
          warnings.push(`${path}: 代理返回空，已回退直连 ${direct}`);
          return directRows;
        }
      }
      return primaryRows;
    } catch (primaryErr) {
      if (primary.startsWith("/") && primary !== direct) {
        try {
          const directRows = await tryBase(direct);
          warnings.push(`${path}: 代理失败，已回退直连 ${direct}`);
          return directRows;
        } catch (directErr) {
          throw new Error(`${String(primaryErr)} | fallback: ${String(directErr)}`);
        }
      }
      throw primaryErr;
    }
  };

  const loadDashboardData = async () => {
    const di = trimTrailingSlash(diBase.trim());
    if (!di) {
      setDashboardError("Design-Ins Base URL 不能为空");
      return;
    }

    setDashboardLoading(true);
    setDashboardError("");
    try {
      const warnings = [];
      const listJobs = [
        { key: "projects", path: "/api/v1/projects?limit=20&offset=0" },
        { key: "contracts", path: "/api/v1/contracts?limit=20&offset=0" },
        { key: "employees", path: "/api/v1/employees?limit=20&offset=0" },
        { key: "qualifications", path: "/api/v1/qualifications?limit=20&offset=0" },
        { key: "gatherings", path: "/api/v1/gatherings?limit=20&offset=0" },
        { key: "invoices", path: "/api/v1/invoices?limit=20&offset=0" },
        { key: "settlements", path: "/api/v1/settlements?limit=20&offset=0" },
      ];

      const settled = await Promise.allSettled(
        listJobs.map((job) => listWithFallback(job.path, warnings)),
      );

      const next = {
        projects: [],
        contracts: [],
        employees: [],
        qualifications: [],
        achievements: [],
        gatherings: [],
        invoices: [],
        settlements: [],
      };

      settled.forEach((result, idx) => {
        const key = listJobs[idx].key;
        if (result.status === "fulfilled") {
          next[key] = result.value;
          return;
        }
        warnings.push(`${key}: ${String(result.reason)}`);
      });

      try {
        next.achievements = await listWithFallback("/api/v1/achievements?limit=20&offset=0", warnings);
      } catch (achErr) {
        try {
          const publicAchievementsRes = await apiRequest({
            method: "GET",
            url: `${di}/public/v1/achievements?limit=20&offset=0`,
            token: useAuth ? token : "",
          });
          next.achievements = normalizeListData(publicAchievementsRes.data);
          warnings.push("achievements: 私有接口返回错误，已降级使用 public/v1/achievements");
        } catch (publicErr) {
          warnings.push(`achievements: ${String(achErr)} | fallback: ${String(publicErr)}`);
          next.achievements = [];
        }
      }

      let pickedProjectRef = selectedProjectRef;
      if (!pickedProjectRef) {
        pickedProjectRef = String(pickField(next.projects[0], ["ref", "Ref"], ""));
      }
      setSelectedProjectRef(pickedProjectRef);

      let projectResources = null;
      let qualificationAssignments = [];
      if (pickedProjectRef) {
        try {
          const [resourcesRes, assignmentsRes] = await Promise.all([
            apiRequest({
              method: "GET",
              url: `${di}/api/v1/projects/${encodeURIComponent(pickedProjectRef)}/resources`,
              token: useAuth ? token : "",
            }),
            apiRequest({
              method: "GET",
              url: `${di}/api/v1/projects/${encodeURIComponent(pickedProjectRef)}/qualification-assignments`,
              token: useAuth ? token : "",
            }),
          ]);
          projectResources = resourcesRes.data;
          qualificationAssignments = normalizeListData(assignmentsRes.data);
        } catch (err) {
          warnings.push(`projectDetail: ${String(err)}`);
        }
      }

      setDashboard({
        ...next,
        projectResources,
        qualificationAssignments,
        updatedAt: new Date().toISOString(),
      });
      if (warnings.length > 0) {
        setDashboardError(warnings.join("\n"));
      }
    } catch (err) {
      setDashboardError(String(err));
    } finally {
      setDashboardLoading(false);
    }
  };

  const resetFlow = () => {
    setFlowSteps(buildFlowSteps());
    setFlowSummary(null);
  };

  return (
    <main className="min-h-full px-4 py-6 md:px-8">
      <section className="mx-auto max-w-6xl space-y-6">
        <header className="panel p-6">
          <h1 className="text-2xl font-semibold tracking-tight">中北设计院管理系统 · 业务联调台</h1>
          <p className="mt-2 text-sm text-slate">
            提供“项目到合同到人员到资质到业绩到发票到结算到证据包”的核心闭环演示。
          </p>
        </header>

        <section className="panel p-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold">核心主流程场景（中北桥梁项目）</h2>
              <p className="mt-1 text-xs text-slate">
                场景固定为 PG 原生业务链路，不依赖 legacy MySQL 导入。
              </p>
            </div>
            <div className="flex gap-2">
              <button
                onClick={runMainFlow}
                disabled={flowRunning}
                className="rounded-lg bg-mint px-4 py-2 text-sm font-medium text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {flowRunning ? "闭环执行中..." : "一键执行闭环"}
              </button>
              <button
                onClick={resetFlow}
                disabled={flowRunning}
                className="rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm"
              >
                重置步骤
              </button>
            </div>
          </div>

          <div className="mt-4 grid gap-3">
            {flowSteps.map((step, idx) => (
              <article key={step.key} className="rounded-xl border border-slate-200 bg-white p-4">
                <div className="flex items-center justify-between gap-2">
                  <div className="text-sm font-medium">
                    {idx + 1}. {step.title}
                  </div>
                  <StatusTag status={step.status} />
                </div>
                <p className="mt-1 text-xs text-slate">{step.detail}</p>
                {step.elapsedMs != null && (
                  <p className="mt-1 text-xs text-slate">耗时: {step.elapsedMs} ms</p>
                )}
                {step.result && (
                  <pre className="mt-2 overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100">
                    {JSON.stringify(step.result, null, 2)}
                  </pre>
                )}
                {step.error && (
                  <pre className="mt-2 overflow-auto rounded-lg bg-red-950 p-3 text-xs text-red-100">{step.error}</pre>
                )}
              </article>
            ))}
          </div>

          {flowSummary && (
            <div className="mt-4 rounded-xl border border-slate-300 bg-slate-50 p-4 text-xs">
              <div className="font-semibold">运行摘要</div>
              <pre className="mt-2 overflow-auto rounded bg-white p-3 text-xs text-slate-700">
                {JSON.stringify(flowSummary, null, 2)}
              </pre>
            </div>
          )}
        </section>

        <section className="panel p-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold">联调数据看板（读取现有数据库数据）</h2>
              <p className="mt-1 text-xs text-slate">直接调用 design-institute 接口，展示当前 PG 已落库的业务数据。</p>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={loadDashboardData}
                disabled={dashboardLoading}
                className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-medium text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {dashboardLoading ? "刷新中..." : "刷新看板"}
              </button>
            </div>
          </div>

          <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <MetricCard label="项目" value={dashboard.projects.length} />
            <MetricCard label="合同" value={dashboard.contracts.length} />
            <MetricCard label="人员" value={dashboard.employees.length} />
            <MetricCard label="资质" value={dashboard.qualifications.length} />
            <MetricCard label="业绩" value={dashboard.achievements.length} />
            <MetricCard label="回款" value={dashboard.gatherings.length} />
            <MetricCard label="发票" value={dashboard.invoices.length} />
            <MetricCard label="结算" value={dashboard.settlements.length} />
          </div>
          {dashboardError && (
            <pre className="mt-3 overflow-auto rounded-lg bg-amber-950 p-3 text-xs text-amber-100">{dashboardError}</pre>
          )}

          <div className="mt-4 grid gap-4 lg:grid-cols-2">
            <DataTable
              title="项目"
              rows={dashboard.projects}
              columns={[
                { key: "ref", label: "Ref", keys: ["ref", "Ref"] },
                { key: "name", label: "名称", keys: ["name", "Name"] },
                { key: "status", label: "状态", keys: ["status", "Status"] },
                { key: "contract_ref", label: "ContractRef", keys: ["contract_ref", "ContractRef"] },
              ]}
            />
            <DataTable
              title="合同"
              rows={dashboard.contracts}
              columns={[
                { key: "id", label: "ID", keys: ["id", "ID"] },
                { key: "num", label: "编号", keys: ["num", "Num"] },
                { key: "name", label: "名称", keys: ["contract_name", "ContractName"] },
                { key: "project_ref", label: "ProjectRef", keys: ["project_ref", "ProjectRef"] },
                { key: "state", label: "状态", keys: ["state", "State"] },
              ]}
            />
            <DataTable
              title="人员"
              rows={dashboard.employees}
              columns={[
                { key: "id", label: "ID", keys: ["id", "ID"] },
                { key: "name", label: "姓名", keys: ["name", "Name"] },
                { key: "executor_ref", label: "ExecutorRef", keys: ["executor_ref", "ExecutorRef"] },
                { key: "position", label: "岗位", keys: ["position", "Position"] },
              ]}
            />
            <DataTable
              title="资质"
              rows={dashboard.qualifications}
              columns={[
                { key: "id", label: "ID", keys: ["id", "ID"] },
                { key: "qual_type", label: "类型", keys: ["qual_type", "QualType"] },
                { key: "executor_ref", label: "ExecutorRef", keys: ["executor_ref", "ExecutorRef"] },
                { key: "status", label: "状态", keys: ["status", "Status"] },
                { key: "valid_until", label: "有效期", keys: ["valid_until", "ValidUntil"] },
              ]}
            />
            <DataTable
              title="业绩"
              rows={dashboard.achievements}
              columns={[
                { key: "id", label: "ID", keys: ["id", "ID"] },
                { key: "spu_ref", label: "SPU", keys: ["spu_ref", "SpuRef"] },
                { key: "project_ref", label: "ProjectRef", keys: ["project_ref", "ProjectRef"] },
                { key: "executor_ref", label: "ExecutorRef", keys: ["executor_ref", "ExecutorRef"] },
              ]}
            />
            <DataTable
              title="财务流转"
              rows={mergeFinanceRows(dashboard.gatherings, dashboard.invoices, dashboard.settlements)}
              columns={[
                { key: "type", label: "类型", keys: ["type"] },
                { key: "id", label: "ID", keys: ["id"] },
                { key: "project_ref", label: "ProjectRef", keys: ["project_ref"] },
                { key: "contract_id", label: "ContractID", keys: ["contract_id"] },
                { key: "state", label: "状态", keys: ["state"] },
              ]}
            />
          </div>

          <div className="mt-5 rounded-xl border border-slate-200 bg-slate-50 p-4">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm font-medium">项目资源回读</span>
              <select
                value={selectedProjectRef}
                onChange={(e) => setSelectedProjectRef(e.target.value)}
                className="min-w-80 rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
              >
                <option value="">请选择项目</option>
                {dashboard.projects.map((it, idx) => {
                  const ref = String(pickField(it, ["ref", "Ref"], ""));
                  const name = String(pickField(it, ["name", "Name"], ""));
                  if (!ref) return null;
                  return (
                    <option key={`${ref}-${idx}`} value={ref}>
                      {name ? `${name} (${ref})` : ref}
                    </option>
                  );
                })}
              </select>
              <button
                onClick={() => loadProjectDetail(selectedProjectRef)}
                disabled={!selectedProjectRef || projectDetailLoading}
                className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs disabled:cursor-not-allowed disabled:opacity-60"
              >
                {projectDetailLoading ? "读取中..." : "读取该项目资源"}
              </button>
            </div>
            {dashboard.updatedAt && <p className="mt-2 text-xs text-slate">最近刷新: {dashboard.updatedAt}</p>}
            <div className="mt-3 grid gap-3 lg:grid-cols-2">
              <pre className="min-h-40 overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100">
                {JSON.stringify(
                  {
                    project_ref: selectedProjectRef || null,
                    qualification_assignment_count: dashboard.qualificationAssignments.length,
                    assignments: dashboard.qualificationAssignments,
                  },
                  null,
                  2,
                )}
              </pre>
              <pre className="min-h-40 overflow-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-100">
                {JSON.stringify(dashboard.projectResources || { hint: "先刷新看板或选择项目后读取资源" }, null, 2)}
              </pre>
            </div>
          </div>
        </section>

        <section className="grid gap-6 lg:grid-cols-2">
          <div className="panel p-6">
            <h2 className="text-lg font-semibold">环境配置</h2>
            <div className="mt-4 space-y-4">
              <Input
                label="Design-Ins Base URL"
                value={diBase}
                onChange={(v) => {
                  setDiBase(v);
                  saveLocal("coordos.di.base", v);
                }}
              />
              <Input
                label="Vault Base URL"
                value={vaultBase}
                onChange={(v) => {
                  setVaultBase(v);
                  saveLocal("coordos.vault.base", v);
                }}
              />
              <label className="flex items-center gap-2 text-xs text-slate">
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
              <Input
                label="Bearer Token (通用控制台用)"
                value={token}
                onChange={(v) => {
                  setToken(v);
                  saveLocal("coordos.token", v);
                }}
              />
              <div className="flex flex-wrap gap-2">
                <button
                  onClick={() => {
                    setDiBase("/di");
                    setVaultBase("/vault");
                    saveLocal("coordos.di.base", "/di");
                    saveLocal("coordos.vault.base", "/vault");
                  }}
                  className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
                >
                  切换代理模式(/di,/vault)
                </button>
                <button
                  onClick={() => {
                    setDiBase("http://127.0.0.1:8090");
                    setVaultBase("http://127.0.0.1:8080");
                    saveLocal("coordos.di.base", "http://127.0.0.1:8090");
                    saveLocal("coordos.vault.base", "http://127.0.0.1:8080");
                  }}
                  className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
                >
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
            <button
              onClick={() => setResponse("")}
              className="rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm"
            >
              清空响应
            </button>
          </div>

          <pre className="mt-4 min-h-64 overflow-auto rounded-lg bg-ink p-4 text-xs text-mist">
            {response || "{\n  \"hint\": \"Response will appear here\"\n}"}
          </pre>
        </section>
      </section>
    </main>
  );
}

function MetricCard({ label, value }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-3">
      <div className="text-xs text-slate">{label}</div>
      <div className="mt-1 text-lg font-semibold">{value}</div>
    </div>
  );
}

function DataTable({ title, rows, columns }) {
  return (
    <article className="rounded-xl border border-slate-200 bg-white p-3">
      <div className="mb-2 text-sm font-medium">
        {title} <span className="text-xs text-slate">({rows.length})</span>
      </div>
      <div className="max-h-64 overflow-auto rounded border border-slate-200">
        <table className="w-full min-w-[680px] border-collapse text-left text-xs">
          <thead className="bg-slate-100 text-slate-700">
            <tr>
              {columns.map((col) => (
                <th key={col.key} className="border-b border-slate-200 px-2 py-2 font-medium">
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 && (
              <tr>
                <td colSpan={columns.length} className="px-2 py-4 text-center text-slate-500">
                  暂无数据
                </td>
              </tr>
            )}
            {rows.map((row, idx) => (
              <tr key={`${title}-${idx}`} className="odd:bg-white even:bg-slate-50">
                {columns.map((col) => {
                  const value = pickField(row, col.keys, "");
                  return (
                    <td key={`${title}-${idx}-${col.key}`} className="border-b border-slate-100 px-2 py-2 align-top">
                      <code className="break-all whitespace-pre-wrap text-[11px] text-slate-700">{renderValue(value)}</code>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </article>
  );
}

function Input({ label, value, onChange }) {
  return (
    <label className="block">
      <span className="mb-1 block text-xs text-slate">{label}</span>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm"
      />
    </label>
  );
}

function StatusTag({ status }) {
  const map = {
    pending: "bg-slate-100 text-slate-700 border-slate-300",
    running: "bg-sky-100 text-sky-700 border-sky-300",
    done: "bg-emerald-100 text-emerald-700 border-emerald-300",
    failed: "bg-red-100 text-red-700 border-red-300",
  };
  return (
    <span className={`rounded-full border px-2 py-0.5 text-xs ${map[status] || map.pending}`}>
      {status}
    </span>
  );
}

function trimTrailingSlash(v) {
  return v.replace(/\/+$/, "");
}

function normalizeListData(data) {
  if (Array.isArray(data)) return data;
  const maybeItems = pickField(data, ["items", "Items", "list", "List", "rows", "Rows", "data", "Data"], []);
  return Array.isArray(maybeItems) ? maybeItems : [];
}

function mergeFinanceRows(gatherings, invoices, settlements) {
  const gs = asArray(gatherings).map((it) => ({
    type: "GATHERING",
    id: toInt(pickField(it, ["id", "ID"], 0)),
    project_ref: String(pickField(it, ["project_ref", "ProjectRef"], "")),
    contract_id: toInt(pickField(it, ["contract_id", "ContractID"], 0)),
    state: String(pickField(it, ["state", "State"], "")),
  }));
  const is = asArray(invoices).map((it) => ({
    type: "INVOICE",
    id: toInt(pickField(it, ["id", "ID"], 0)),
    project_ref: String(pickField(it, ["project_ref", "ProjectRef"], "")),
    contract_id: toInt(pickField(it, ["contract_id", "ContractID"], 0)),
    state: String(pickField(it, ["state", "State"], "")),
  }));
  const ss = asArray(settlements).map((it) => ({
    type: "SETTLEMENT",
    id: toInt(pickField(it, ["id", "ID"], 0)),
    project_ref: String(pickField(it, ["project_ref", "ProjectRef"], "")),
    contract_id: toInt(pickField(it, ["contract_id", "ContractID"], 0)),
    state: String(pickField(it, ["state", "State"], "")),
  }));
  return [...gs, ...is, ...ss].slice(0, 30);
}

function renderValue(value) {
  if (value == null) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

function asArray(v) {
  return Array.isArray(v) ? v : [];
}

function safeLen(v) {
  return Array.isArray(v) ? v.length : 0;
}

function hasEntityWithId(list, id) {
  const items = asArray(list);
  const target = toInt(id);
  if (!target) return false;
  return items.some((it) => toInt(pickField(it, ["id", "ID"], 0)) === target);
}

function toInt(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function pickField(obj, keys, fallback = undefined) {
  if (obj == null || typeof obj !== "object") return fallback;
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, key) && obj[key] != null) {
      return obj[key];
    }
  }
  const entries = Object.entries(obj);
  for (const key of keys) {
    const found = entries.find(([k]) => k.toLowerCase() === String(key).toLowerCase());
    if (found && found[1] != null) return found[1];
  }
  return fallback;
}

async function apiRequest({ method, url, body, token }) {
  const headers = { "Content-Type": "application/json" };
  if (token && token.trim()) {
    headers.Authorization = `Bearer ${token.trim()}`;
  }
  const init = { method, headers };
  if (body != null && method !== "GET" && method !== "HEAD") {
    init.body = JSON.stringify(body);
  }
  const res = await fetch(url, init);
  const text = await res.text();
  const data = tryParse(text);
  if (!res.ok) {
    const detail =
      (data && typeof data === "object" && (data.error || data.detail || data.message)) ||
      text ||
      "request failed";
    throw new Error(`[${method}] ${url} -> ${res.status}: ${detail}`);
  }
  return { status: res.status, data };
}

function tryParse(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}
