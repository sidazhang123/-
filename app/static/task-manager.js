/**
 * task-manager.js — 任务管理页逻辑。
 *
 * 功能：
 * 1. 列出所有筛选任务（分页）。
 * 2. 勾选终态任务批量删除（带二次确认弹窗）。
 * 3. 点击"参"查看任务参数快照（只读模态框）。
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
}

function hideDeleteConfirm() {
  $("deleteConfirmOverlay").hidden = true;
  _pendingDeleteIds = [];
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

  // delete confirm
  $("deleteConfirmCancelBtn").addEventListener("click", hideDeleteConfirm);
  $("deleteConfirmNoBtn").addEventListener("click", hideDeleteConfirm);
  $("deleteConfirmYesBtn").addEventListener("click", () => executeDelete().catch((e) => alert(e.message)));
  $("deleteConfirmOverlay").addEventListener("click", (e) => {
    if (e.target === $("deleteConfirmOverlay")) hideDeleteConfirm();
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
  await loadTasks();
}

init().catch((err) => {
  console.error("页面初始化失败", err);
  alert(`页面初始化失败：${err.message}`);
});
