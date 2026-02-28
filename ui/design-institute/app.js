/*
  CoordOS Design Institute Console
  --------------------------------
  目标：
  1) 让开发/测试可以快速调用后端接口。
  2) 把环境配置、请求参数、响应结果放在一个页面里。
  3) 保留足够注释，便于后续扩展为更正式的前端应用。
*/

const KEY = {
  designBaseUrl: "coordos_ui_design_base_url",
  vaultBaseUrl: "coordos_ui_vault_base_url",
  vaultToken: "coordos_ui_vault_token",
  lastVaultProjectRef: "coordos_ui_last_vault_project_ref",
};

const el = {
  designBaseUrl: document.getElementById("designBaseUrl"),
  vaultBaseUrl: document.getElementById("vaultBaseUrl"),
  vaultToken: document.getElementById("vaultToken"),
  saveConfigBtn: document.getElementById("saveConfigBtn"),
  quickButtons: [...document.querySelectorAll("[data-action]")],
  requestService: document.getElementById("requestService"),
  requestMethod: document.getElementById("requestMethod"),
  requestPath: document.getElementById("requestPath"),
  requestBody: document.getElementById("requestBody"),
  runRequestBtn: document.getElementById("runRequestBtn"),
  presetProjectBtn: document.getElementById("presetProjectBtn"),
  presetEventBtn: document.getElementById("presetEventBtn"),
  responseMeta: document.getElementById("responseMeta"),
  responseBody: document.getElementById("responseBody"),
  requestLog: document.getElementById("requestLog"),
};

function boot() {
  // 启动时从 localStorage 恢复配置，减少重复输入。
  el.designBaseUrl.value = localStorage.getItem(KEY.designBaseUrl) || "http://localhost:8090";
  el.vaultBaseUrl.value = localStorage.getItem(KEY.vaultBaseUrl) || "http://localhost:8080";
  el.vaultToken.value =
    localStorage.getItem(KEY.vaultToken) ||
    "10000:v://10000/person/hq:HEAD_OFFICE,PLATFORM";

  bindEvents();
  appendLog("控制台已启动，等待请求。", "ok");
}

function bindEvents() {
  el.saveConfigBtn.addEventListener("click", () => {
    localStorage.setItem(KEY.designBaseUrl, el.designBaseUrl.value.trim());
    localStorage.setItem(KEY.vaultBaseUrl, el.vaultBaseUrl.value.trim());
    localStorage.setItem(KEY.vaultToken, el.vaultToken.value.trim());
    appendLog("环境配置已保存。", "ok");
  });

  el.quickButtons.forEach((btn) => {
    btn.addEventListener("click", () => runQuickAction(btn.dataset.action));
  });

  el.runRequestBtn.addEventListener("click", async () => {
    const service = el.requestService.value;
    const method = el.requestMethod.value.toUpperCase();
    const path = el.requestPath.value.trim();
    const bodyRaw = el.requestBody.value.trim();

    let parsedBody = undefined;
    if (bodyRaw) {
      try {
        parsedBody = JSON.parse(bodyRaw);
      } catch (err) {
        setResponse(
          `JSON 解析失败: ${err.message}`,
          bodyRaw,
          false
        );
        appendLog("请求中断：JSON 解析失败。", "bad");
        return;
      }
    }
    await sendRequest({ service, method, path, body: parsedBody });
  });

  // 预设 1：创建 Vault 根项目，快速验证主链路起点。
  el.presetProjectBtn.addEventListener("click", () => {
    el.requestService.value = "vault";
    el.requestMethod.value = "POST";
    el.requestPath.value = "/api/v1/projects";
    el.requestBody.value = JSON.stringify(
      {
        ID: "bridge-demo-" + Date.now(),
        Name: "Bridge Demo",
        OwnerRef: "v://10000/org/owner",
      },
      null,
      2
    );
  });

  // 预设 2：提交项目事件，快速验证规则引擎入口。
  el.presetEventBtn.addEventListener("click", () => {
    const lastRef = localStorage.getItem(KEY.lastVaultProjectRef) || "v://10000/project/bridge-demo";
    el.requestService.value = "vault";
    el.requestMethod.value = "POST";
    el.requestPath.value = "/api/v1/events";
    el.requestBody.value = JSON.stringify(
      {
        event_id: "evt-ui-" + Date.now(),
        project_ref: lastRef,
        tenant_id: "10000",
        actor_ref: "v://10000/person/hq",
        verb: "CONFIGURE",
        timestamp: new Date().toISOString(),
        payload: { source: "ui-console" },
      },
      null,
      2
    );
  });
}

async function runQuickAction(action) {
  switch (action) {
    case "di-health":
      await sendRequest({ service: "di", method: "GET", path: "/health" });
      break;
    case "vault-health":
      await sendRequest({ service: "vault", method: "GET", path: "/health" });
      break;
    case "di-project-list":
      await sendRequest({ service: "di", method: "GET", path: "/api/v1/projects?limit=10&offset=0" });
      break;
    case "di-report-overview": {
      const now = new Date();
      const from = new Date(now.getFullYear(), now.getMonth(), 1).toISOString();
      const to = now.toISOString();
      await sendRequest({
        service: "di",
        method: "GET",
        path: `/api/v1/reports/overview?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`,
      });
      break;
    }
    case "vault-root-project":
      await sendRequest({
        service: "vault",
        method: "POST",
        path: "/api/v1/projects",
        body: {
          ID: "bridge-quick-" + Date.now(),
          Name: "Bridge Quick",
          OwnerRef: "v://10000/org/owner",
        },
      });
      break;
    default:
      appendLog(`未知快捷动作: ${action}`, "bad");
  }
}

async function sendRequest({ service, method, path, body }) {
  const baseUrl = service === "di" ? el.designBaseUrl.value.trim() : el.vaultBaseUrl.value.trim();
  if (!baseUrl) {
    setResponse("Base URL 为空，请先保存配置。", "", false);
    appendLog("请求失败：Base URL 为空。", "bad");
    return;
  }

  const url = safeJoin(baseUrl, path);
  const headers = {
    "Content-Type": "application/json",
  };

  // Vault 端点需要 Bearer Token；DI 当前无需鉴权头。
  if (service === "vault") {
    const token = el.vaultToken.value.trim();
    if (token) headers.Authorization = `Bearer ${token}`;
  }

  const startedAt = performance.now();

  try {
    const resp = await fetch(url, {
      method,
      headers,
      body: method === "GET" || method === "DELETE" ? undefined : JSON.stringify(body ?? {}),
    });

    const text = await resp.text();
    const ms = (performance.now() - startedAt).toFixed(1);
    const ok = resp.ok;
    const statusText = `${method} ${path} -> ${resp.status} (${ms} ms)`;
    const pretty = tryPrettyJSON(text);

    // 成功创建项目后，缓存最新 project_ref，供事件示例自动复用。
    if (ok && service === "vault" && method === "POST" && path.startsWith("/api/v1/projects")) {
      const maybeRef = extractProjectRef(text);
      if (maybeRef) {
        localStorage.setItem(KEY.lastVaultProjectRef, maybeRef);
        appendLog(`已缓存最新项目引用: ${maybeRef}`, "ok");
        // 若当前编辑器已经停留在事件请求，自动把 project_ref 改成最新创建的项目，
        // 避免“先填事件再创建项目”导致继续提交旧引用。
        syncEventEditorProjectRef(maybeRef);
      }
    }

    // 当事件请求引用了不存在项目时，明确提示应使用最新创建的 ref。
    if (!ok && service === "vault" && method === "POST" && path === "/api/v1/events") {
      const lastRef = localStorage.getItem(KEY.lastVaultProjectRef);
      if (lastRef) {
        appendLog(`事件 project_ref 可能不匹配，建议改为: ${lastRef}`, "bad");
      }
    }

    setResponse(statusText, pretty, ok);
    appendLog(statusText, ok ? "ok" : "bad");
  } catch (err) {
    const message = `请求异常: ${err.message}`;
    setResponse(message, "", false);
    appendLog(message, "bad");
  }
}

function safeJoin(base, path) {
  const b = base.replace(/\/+$/, "");
  const p = path.startsWith("/") ? path : `/${path}`;
  return `${b}${p}`;
}

function tryPrettyJSON(text) {
  if (!text) return "";
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

function extractProjectRef(text) {
  if (!text) return "";
  try {
    const payload = JSON.parse(text);
    if (payload && typeof payload.ref === "string" && payload.ref.length > 0) {
      return payload.ref;
    }
    // 兼容某些包装响应结构: { "node": { "ref": "..." } }
    if (payload && payload.node && typeof payload.node.ref === "string" && payload.node.ref.length > 0) {
      return payload.node.ref;
    }
  } catch {
    return "";
  }
  return "";
}

function syncEventEditorProjectRef(projectRef) {
  if (!projectRef) return;
  if (el.requestService.value !== "vault") return;
  if (el.requestPath.value.trim() !== "/api/v1/events") return;

  const raw = el.requestBody.value.trim();
  if (!raw) return;

  try {
    const body = JSON.parse(raw);
    if (!body || typeof body !== "object" || Array.isArray(body)) return;
    body.project_ref = projectRef;
    el.requestBody.value = JSON.stringify(body, null, 2);
    appendLog("事件请求已自动同步为最新 project_ref", "ok");
  } catch {
    // 保持静默：不覆盖用户手写的非 JSON 草稿。
  }
}

function setResponse(meta, body, ok) {
  el.responseMeta.textContent = meta;
  el.responseMeta.className = `response-meta ${ok ? "status-ok" : "status-bad"}`;
  el.responseBody.textContent = body || "(empty)";
}

function appendLog(text, kind = "ok") {
  const item = document.createElement("li");
  item.className = "log-item";
  item.innerHTML = `<strong>${new Date().toLocaleTimeString()}</strong> · <span class="${kind === "ok" ? "status-ok" : "status-bad"}">${escapeHTML(text)}</span>`;
  el.requestLog.prepend(item);

  // 控制日志长度，防止长时间使用后页面变卡。
  while (el.requestLog.childElementCount > 20) {
    el.requestLog.removeChild(el.requestLog.lastElementChild);
  }
}

function escapeHTML(text) {
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

boot();
