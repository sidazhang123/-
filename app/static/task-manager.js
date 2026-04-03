/**
 * task-manager.js — 任务管理页逻辑。
 *
 * 功能：
 * 1. 列出所有筛选任务（分页）。
 * 2. 勾选终态任务批量删除（带二次确认弹窗）。
 * 3. 点击"参"查看任务参数快照（只读模态框）。
 * 4. 列出回测任务，支持批量删除与参数快照查看。
 */
const $ = (id) => document.getElementById(id);
const escapeHtml = ParamRender.escapeHtml;

const STATUS_TEXT = {
  queued: "排队中",
  running: "运行中",
  paused: "已暂停",
  stopping: "停止中",
  stopped: "已停止",
  completed: "已完成",
  failed: "失败",
};
const TERMINAL_STATUSES = new Set(["completed", "failed", "stopped"]);

let tasks = [];
let selectedIds = new Set();

async function getJSON(url) {
  const resp = await fetch(url);
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`请求失败(${resp.status})：${text}`);
  }
  return resp.json();
}

function formatDateTime(v) {
  if (!v) return "-";
  return new Date(v).toLocaleString();
}

function isTerminal(status) {
  return TERMINAL_STATUSES.has(status);
}

// ── Task list ──

async function loadTasks() {
  const data = await getJSON("/api/tasks?limit=500");
  tasks = data.items || [];
  selectedIds.clear();
  renderTable();
  updateSelectionHint();
}

function renderTable() {
  const tbody = $("taskTable").querySelector("tbody");
  tbody.innerHTML = "";
  const empty = $("emptyHint");
  if (!tasks.length) {
    empty.hidden = false;
    return;
  }
  empty.hidden = true;
  for (const t of tasks) {
    const tr = document.createElement("tr");
    const terminal = isTerminal(t.status);
    const checked = selectedIds.has(t.task_id);
    tr.innerHTML = `
      <td><input type="checkbox" class="row-check" data-id="${escapeHtml(t.task_id)}" ${terminal ? "" : "disabled"} ${checked ? "checked" : ""} /></td>
      <td>${escapeHtml(t.strategy_group_id || "-")}</td>
      <td>${escapeHtml(t.strategy_name || "-")}</td>
      <td><span class="status-badge status-${escapeHtml(t.status)}">${STATUS_TEXT[t.status] || t.status}</span></td>
      <td>${t.result_count ?? 0}</td>
      <td>${Math.round(t.progress || 0)}%</td>
      <td>${formatDateTime(t.created_at)}</td>
      <td>${formatDateTime(t.finished_at)}</td>
      <td><button type="button" class="param-badge-btn view-params-btn" data-id="${escapeHtml(t.task_id)}" title="查看任务参数">参</button></td>
    `;
    tbody.appendChild(tr);
  }
  // bind checkbox events
  tbody.querySelectorAll(".row-check").forEach((cb) => {
    cb.addEventListener("change", () => {
      if (cb.checked) {
        selectedIds.add(cb.dataset.id);
      } else {
        selectedIds.delete(cb.dataset.id);
      }
      updateSelectionHint();
    });
  });
  // bind view-params buttons
  tbody.querySelectorAll(".view-params-btn").forEach((btn) => {
    btn.addEventListener("click", () => openParamModal(btn.dataset.id));
  });
}

function updateSelectionHint() {
  const btn = $("deleteSelectedBtn");
  const hint = $("selectionHint");
  const count = selectedIds.size;
  btn.disabled = count === 0;
  hint.textContent = count > 0 ? `已选 ${count} 个任务` : "";
  // sync header checkbox
  const terminalCount = tasks.filter((t) => isTerminal(t.status)).length;
  const hcb = $("headerCheckbox");
  hcb.checked = terminalCount > 0 && count === terminalCount;
  hcb.indeterminate = count > 0 && count < terminalCount;
}

function selectAllTerminal() {
  selectedIds.clear();
  for (const t of tasks) {
    if (isTerminal(t.status)) selectedIds.add(t.task_id);
  }
  renderTable();
  updateSelectionHint();
}

function deselectAll() {
  selectedIds.clear();
  renderTable();
  updateSelectionHint();
}

// ── Delete with confirmation ──

let _pendingDeleteIds = [];

function showDeleteConfirm() {
  const ids = Array.from(selectedIds);
  if (!ids.length) return;
  _pendingDeleteIds = ids;
  $("deleteConfirmText").textContent = `确定要删除选中的 ${ids.length} 个任务及其日志和结果吗？此操作不可撤销。`;
  $("deleteConfirmOverlay").hidden = false;
  $("deleteConfirmYesBtn").dataset.target = "screening";
}

function hideDeleteConfirm() {
  $("deleteConfirmOverlay").hidden = true;
  _pendingDeleteIds = [];
  _pendingBtDeleteIds = [];
}

async function executeDelete() {
  const ids = _pendingDeleteIds.slice();
  hideDeleteConfirm();
  if (!ids.length) return;
  try {
    const resp = await fetch("/api/tasks", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ task_ids: ids }),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`删除失败(${resp.status})：${text}`);
    }
    const data = await resp.json();
    alert(`已删除 ${data.deleted} 个任务。`);
    await loadTasks();
  } catch (err) {
    alert(err.message);
  }
}

// ── Param modal ──

let _paramCache = {};

async function loadTaskParams(taskId) {
  if (_paramCache[taskId]) return _paramCache[taskId];
  const data = await getJSON(`/api/tasks/${taskId}/params`);
  _paramCache[taskId] = data;
  return data;
}

const _RUN_MODE_TEXT = { full: "全量", sample20: "随机抽样" };

function renderParamMeta(data) {
  const parts = [];
  if (data.strategy_name) parts.push(ParamRender.escapeHtml(data.strategy_name));
  const modeLabel = _RUN_MODE_TEXT[data.run_mode] || data.run_mode || "";
  if (modeLabel) {
    let text = modeLabel;
    if (data.run_mode === "sample20" && data.sample_size) text += ` (${data.sample_size})`;
    parts.push(ParamRender.escapeHtml(text));
  }
  if (!parts.length) return "";
  return `<div class="param-modal-meta">${parts.join(" · ")}</div>`;
}

function renderOverview(paramHelp) {
  if (!paramHelp || !paramHelp._overview) return "";
  const text = ParamRender.helpTextFromNode({ _overview: paramHelp._overview });
  if (!text) return "";
  const lines = text.split("\n").map((l) => `<p>${ParamRender.escapeHtml(l)}</p>`).join("");
  return `<div class="param-modal-overview">${lines}</div>`;
}

function renderReadonlyParams(data) {
  const groupParams = data.group_params;
  const paramHelp = data.param_help;
  if (!groupParams || !Object.keys(groupParams).length) {
    return '<div class="param-modal-empty">无参数数据</div>';
  }
  const meta = renderParamMeta(data);
  const overview = renderOverview(paramHelp);
  const keys = Object.keys(groupParams);
  const fields = keys.map((key) => {
    const helpNode = paramHelp ? paramHelp[key] : null;
    return ParamRender.renderParamField([key], groupParams[key], helpNode, key, 0, { readonly: true });
  }).join("");
  return meta + overview + fields;
}

function openParamModal(taskId) {
  if (!taskId) return;
  const overlay = $("paramModalOverlay");
  const body = $("paramModalBody");
  overlay.hidden = false;
  body.innerHTML = '<div class="param-modal-loading">加载中…</div>';

  loadTaskParams(taskId).then((data) => {
    body.innerHTML = renderReadonlyParams(data);
  }).catch((err) => {
    body.innerHTML = `<div class="param-modal-empty">加载失败: ${escapeHtml(err.message || "未知错误")}</div>`;
  });
}

function closeParamModal() {
  $("paramModalOverlay").hidden = true;
  $("paramModalBody").innerHTML = "";
}

// ── Backtest task list ──

const MODE_TEXT = { fixed: "固定参数", sweep: "参数扫描" };
let btJobs = [];
let btSelectedIds = new Set();

async function loadBtJobs() {
  const data = await getJSON("/api/backtests");
  btJobs = data.jobs || [];
  btSelectedIds.clear();
  renderBtTable();
  updateBtSelectionHint();
}

function renderBtTable() {
  const tbody = $("btTaskTable").querySelector("tbody");
  tbody.innerHTML = "";
  const empty = $("btEmptyHint");
  if (!btJobs.length) {
    empty.hidden = false;
    return;
  }
  empty.hidden = true;
  for (const j of btJobs) {
    const tr = document.createElement("tr");
    const terminal = isTerminal(j.status);
    const checked = btSelectedIds.has(j.job_id);
    tr.innerHTML = `
      <td><input type="checkbox" class="bt-row-check" data-id="${escapeHtml(j.job_id)}" ${terminal ? "" : "disabled"} ${checked ? "checked" : ""} /></td>
      <td>${escapeHtml(j.strategy_group_id || "-")}</td>
      <td>${escapeHtml(j.strategy_name || "-")}</td>
      <td>${escapeHtml(MODE_TEXT[j.mode] || j.mode || "-")}</td>
      <td><span class="status-badge status-${escapeHtml(j.status)}">${STATUS_TEXT[j.status] || j.status}</span></td>
      <td>${Math.round((j.progress || 0) * 100)}%</td>
      <td>${formatDateTime(j.created_at)}</td>
      <td>${formatDateTime(j.finished_at)}</td>
      <td><button type="button" class="param-badge-btn bt-view-params-btn" data-id="${escapeHtml(j.job_id)}" title="查看回测参数">参</button></td>
    `;
    tbody.appendChild(tr);
  }
  tbody.querySelectorAll(".bt-row-check").forEach((cb) => {
    cb.addEventListener("change", () => {
      if (cb.checked) btSelectedIds.add(cb.dataset.id);
      else btSelectedIds.delete(cb.dataset.id);
      updateBtSelectionHint();
    });
  });
  tbody.querySelectorAll(".bt-view-params-btn").forEach((btn) => {
    btn.addEventListener("click", () => openBtParamModal(btn.dataset.id));
  });
}

function updateBtSelectionHint() {
  const btn = $("btDeleteSelectedBtn");
  const hint = $("btSelectionHint");
  const count = btSelectedIds.size;
  btn.disabled = count === 0;
  hint.textContent = count > 0 ? `已选 ${count} 个任务` : "";
  const terminalCount = btJobs.filter((j) => isTerminal(j.status)).length;
  const hcb = $("btHeaderCheckbox");
  hcb.checked = terminalCount > 0 && count === terminalCount;
  hcb.indeterminate = count > 0 && count < terminalCount;
}

function btSelectAllTerminal() {
  btSelectedIds.clear();
  for (const j of btJobs) {
    if (isTerminal(j.status)) btSelectedIds.add(j.job_id);
  }
  renderBtTable();
  updateBtSelectionHint();
}

function btDeselectAll() {
  btSelectedIds.clear();
  renderBtTable();
  updateBtSelectionHint();
}

let _pendingBtDeleteIds = [];

function showBtDeleteConfirm() {
  const ids = Array.from(btSelectedIds);
  if (!ids.length) return;
  _pendingBtDeleteIds = ids;
  $("deleteConfirmText").textContent = `确定要删除选中的 ${ids.length} 个回测任务及其日志和缓存吗？此操作不可撤销。`;
  $("deleteConfirmOverlay").hidden = false;
  // 标记当前删除目标为回测
  $("deleteConfirmYesBtn").dataset.target = "backtest";
}

async function executeBtDelete() {
  const ids = _pendingBtDeleteIds.slice();
  hideDeleteConfirm();
  if (!ids.length) return;
  try {
    const resp = await fetch("/api/backtests", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ job_ids: ids }),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`删除失败(${resp.status})：${text}`);
    }
    const data = await resp.json();
    alert(`已删除 ${data.deleted} 个回测任务。`);
    await loadBtJobs();
  } catch (err) {
    alert(err.message);
  }
}

let _btParamCache = {};

async function loadBtParams(jobId) {
  if (_btParamCache[jobId]) return _btParamCache[jobId];
  const data = await getJSON(`/api/backtests/${jobId}/params`);
  _btParamCache[jobId] = data;
  return data;
}

function renderBtParamMeta(data) {
  const parts = [];
  if (data.strategy_name) parts.push(escapeHtml(data.strategy_name));
  const modeLabel = MODE_TEXT[data.mode] || data.mode || "";
  if (modeLabel) parts.push(escapeHtml(modeLabel));
  if (data.forward_bars && data.forward_bars.length === 3) {
    parts.push(escapeHtml(`前瞻: ${data.forward_bars.join("/")} bar`));
  }
  if (data.slide_step) parts.push(escapeHtml(`步进: ${data.slide_step}`));
  if (!parts.length) return "";
  return `<div class="param-modal-meta">${parts.join(" · ")}</div>`;
}

function renderBtReadonlyParams(data) {
  const groupParams = data.group_params;
  const paramHelp = data.param_help;
  if (!groupParams || !Object.keys(groupParams).length) {
    return '<div class="param-modal-empty">无参数数据</div>';
  }
  const meta = renderBtParamMeta(data);
  const overview = renderOverview(paramHelp);
  const keys = Object.keys(groupParams);
  const fields = keys.map((key) => {
    const helpNode = paramHelp ? paramHelp[key] : null;
    return ParamRender.renderParamField([key], groupParams[key], helpNode, key, 0, { readonly: true, hideScopeParams: true });
  }).join("");
  return meta + overview + fields;
}

/** sweep 模式下向只读参数字段旁注入 min/max/step 标签 */
function injectReadonlyRanges(container, ranges) {
  if (!ranges || !Object.keys(ranges).length) return;
  for (const [path, r] of Object.entries(ranges)) {
    const el = container.querySelector(`[data-param-path="${path}"]`);
    if (!el) continue;
    const badge = document.createElement("span");
    badge.className = "bt-range-row";
    badge.innerHTML =
      `<label>min <span class="bt-rng-val">${r.min}</span></label>` +
      `<label>max <span class="bt-rng-val">${r.max}</span></label>` +
      `<label>step <span class="bt-rng-val">${r.step}</span></label>`;
    el.insertAdjacentElement("afterend", badge);
  }
}

function openBtParamModal(jobId) {
  if (!jobId) return;
  const overlay = $("paramModalOverlay");
  const body = $("paramModalBody");
  overlay.hidden = false;
  body.innerHTML = '<div class="param-modal-loading">加载中…</div>';

  loadBtParams(jobId).then((data) => {
    body.innerHTML = renderBtReadonlyParams(data);
    if (data.mode === "sweep") injectReadonlyRanges(body, data.param_ranges);
  }).catch((err) => {
    body.innerHTML = `<div class="param-modal-empty">加载失败: ${escapeHtml(err.message || "未知错误")}</div>`;
  });
}

// ── Init ──

function bindEvents() {
  $("refreshBtn").addEventListener("click", () => loadTasks().catch((e) => alert(e.message)));
  $("selectAllBtn").addEventListener("click", selectAllTerminal);
  $("deselectAllBtn").addEventListener("click", deselectAll);
  $("deleteSelectedBtn").addEventListener("click", showDeleteConfirm);

  // header checkbox
  $("headerCheckbox").addEventListener("change", (e) => {
    if (e.target.checked) {
      selectAllTerminal();
    } else {
      deselectAll();
    }
  });

  // param modal
  $("paramModalCloseBtn").addEventListener("click", closeParamModal);
  $("paramModalOverlay").addEventListener("click", (e) => {
    if (e.target === $("paramModalOverlay")) closeParamModal();
  });

  // delete confirm（共用弹窗，按 data-target 区分筛选/回测）
  $("deleteConfirmCancelBtn").addEventListener("click", hideDeleteConfirm);
  $("deleteConfirmNoBtn").addEventListener("click", hideDeleteConfirm);
  $("deleteConfirmYesBtn").addEventListener("click", () => {
    const target = $("deleteConfirmYesBtn").dataset.target;
    if (target === "backtest") {
      executeBtDelete().catch((e) => alert(e.message));
    } else {
      executeDelete().catch((e) => alert(e.message));
    }
  });
  $("deleteConfirmOverlay").addEventListener("click", (e) => {
    if (e.target === $("deleteConfirmOverlay")) hideDeleteConfirm();
  });

  // 回测任务列表事件
  $("btRefreshBtn").addEventListener("click", () => loadBtJobs().catch((e) => alert(e.message)));
  $("btSelectAllBtn").addEventListener("click", btSelectAllTerminal);
  $("btDeselectAllBtn").addEventListener("click", btDeselectAll);
  $("btDeleteSelectedBtn").addEventListener("click", showBtDeleteConfirm);
  $("btHeaderCheckbox").addEventListener("change", (e) => {
    if (e.target.checked) btSelectAllTerminal();
    else btDeselectAll();
  });

  window.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      if (!$("deleteConfirmOverlay").hidden) {
        hideDeleteConfirm();
      } else if (!$("paramModalOverlay").hidden) {
        closeParamModal();
      }
    }
  });
}

async function init() {
  bindEvents();
  await Promise.all([loadTasks(), loadBtJobs()]);
}

init().catch((err) => {
  console.error("页面初始化失败", err);
  alert(`页面初始化失败：${err.message}`);
});
