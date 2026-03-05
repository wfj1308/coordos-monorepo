export function readLocal(key, fallback) {
  const v = localStorage.getItem(key);
  return v == null ? fallback : v;
}

export function saveLocal(key, value) {
  localStorage.setItem(key, value);
}

export function normalizeNamespaceCode(v) {
  const raw = String(v || "")
    .trim()
    .replace(/^v:\/\//, "");
  if (!raw) return "";
  return raw.split("/")[0].trim();
}

export function trimTrailingSlash(v) {
  return String(v || "").replace(/\/+$/, "");
}

export function normalizeListData(data) {
  if (Array.isArray(data)) return data;
  const maybeItems = pickField(data, ["items", "Items", "list", "List", "rows", "Rows", "data", "Data"], []);
  return Array.isArray(maybeItems) ? maybeItems : [];
}

export function mergeFinanceRows(gatherings, invoices, settlements) {
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
  const out = [];
  const max = Math.max(gs.length, is.length, ss.length);
  for (let i = 0; i < max; i += 1) {
    if (gs[i]) out.push(gs[i]);
    if (is[i]) out.push(is[i]);
    if (ss[i]) out.push(ss[i]);
  }
  return out;
}

export function renderValue(value) {
  if (value == null) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

export function asArray(v) {
  return Array.isArray(v) ? v : [];
}

export function toInt(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export function pickField(obj, keys, fallback = undefined) {
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

export function getIn(obj, path, fallback = undefined) {
  let cur = obj;
  for (const key of path) {
    if (cur == null || typeof cur !== "object") return fallback;
    if (!Object.prototype.hasOwnProperty.call(cur, key)) return fallback;
    cur = cur[key];
  }
  return cur == null ? fallback : cur;
}

export async function apiRequest({ method, url, body, token }) {
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
      (data && typeof data === "object" && (data.error || data.detail || data.message)) || text || "request failed";
    throw new Error(`[${method}] ${url} -> ${res.status}: ${detail}`);
  }
  return { status: res.status, data };
}

export function tryParse(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}
