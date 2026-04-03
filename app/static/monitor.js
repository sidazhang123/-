const state = {
  taskId: null,
  eventSource: null,
  heartbeatSource: null,
  pollInFlight: false,
  lastTask: null,
  lastInfoLogId: null,
  lastErrorLogId: null,
  infoLogs: [],
  errorLogs: [],
  infoLogAutoScroll: true,
  errorLogAutoScroll: true,
  strategyGroups: [],
  settingsDirty: false,
  groupParams: {},
  /** 服务端持久化的每策略参数缓存 { groupId: groupParams } */
  perStrategyParams: {},
};

const $ = (id) => document.getElementById(id);

const STATUS_TEXT = {
  queued: "排队中",
  running: "运行中",
  paused: "已暂停",
  stopping: "停止中",
  stopped: "已停止",
  completed: "已完成",
  failed: "失败",
};

const MODE_TEXT = {
  full: "全量",
  sample20: "随机抽样",
};

const LOG_LEVEL_TEXT = {
  info: "信息",
  error: "错误",
};

const LOG_DISPLAY_LIMIT = 200;
const LOG_AUTO_SCROLL_THRESHOLD = 24;
const SAVE_HINT_CLASS_WARN = "hint warn";
const SAVE_HINT_CLASS_OK = "hint ok";
const SAVE_HINT_CLASS_ERROR = "hint error";
const SHARED_TASK_KEY = "screening:selected_task_id";
const SYNC_CHANNEL_NAME = "screening-task-sync";
const TAB_ID = `monitor-${Date.now()}-${Math.random().toString(16).slice(2)}`;
const syncChannel = "BroadcastChannel" in window ? new BroadcastChannel(SYNC_CHANNEL_NAME) : null;

function setStreamState(text, kind) {
  const el = $("monitorStreamState");
  if (!el) return;
  el.textContent = text;
  el.className = `stream-state ${kind}`;
}

function getErrorTabButton() {
  return document.querySelector('.log-tab.error-tab[data-log-target="errorPane"]');
}

function selectedTaskHasErrors() {
  if (!state.taskId) return false;
  if (Number(state.lastTask?.error_log_count || 0) > 0) return true;
  if (String(state.lastTask?.error_message || "").trim()) return true;
  return state.errorLogs.length > 0;
}

function syncErrorTabAlert() {
  const button = getErrorTabButton();
  if (!button) return;
  button.classList.toggle("has-error-alert", selectedTaskHasErrors());
}

function splitStocks(text) {
  return String(text || "")
    .split(/[\n,\s，]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function selectedGroupId() {
  return $("strategyGroupSelect")?.value || "";
}

function cloneValue(value) {
  return JSON.parse(JSON.stringify(value));
}

function deepMerge(target, source) {
  for (const key of Object.keys(source)) {
    if (
      source[key] !== null &&
      typeof source[key] === "object" &&
      !Array.isArray(source[key]) &&
      target[key] !== null &&
      typeof target[key] === "object" &&
      !Array.isArray(target[key])
    ) {
      deepMerge(target[key], source[key]);
    } else {
      target[key] = cloneValue(source[key]);
    }
  }
  return target;
}

// Delegate shared rendering to ParamRender (param-render.js)
const escapeHtml = ParamRender.escapeHtml;

const helpTextFromNode = ParamRender.helpTextFromNode;

function renderGroupDescription(group) {
  if (!group) return "-";
  const lines = [`说明：${group.description || "-"}`];
  if (group.engine) {
    lines.push(`执行引擎：${group.engine}`);
  }
  if (group.execution && typeof group.execution === "object") {
    lines.push(
      `执行特性：任务内并行=${group.execution.supports_intra_task_parallel ? "是" : "否"}，` +
      `缓存=${group.execution.cache_scope || "none"}`
    );
  }
  const overview = Array.isArray(group.param_help?._overview) ? group.param_help._overview : [];
  if (overview.length) {
    lines.push("参数说明：");
    for (const paragraph of overview) {
      lines.push(String(paragraph));
    }
  }
  return lines.map(escapeHtml).join("<br>");
}

function updateStrategyGroupDescription(groupId) {
  const target = $("strategyGroupDesc");
  if (!target) return;
  target.innerHTML = renderGroupDescription(findGroup(groupId));
}

function normalizeSampleSize(rawValue) {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) return 20;
  return Math.max(1, Math.min(5000, Math.floor(parsed)));
}

function getCurrentSampleSize() {
  const input = $("sampleSize");
  if (!input) return 20;
  const normalized = normalizeSampleSize(input.value);
  input.value = String(normalized);
  return normalized;
}

function setSaveHint(text, cssClass = "hint") {
  const hint = $("settingsSaveHint");
  if (!hint) return;
  hint.className = cssClass;
  hint.textContent = text;
}

function markSettingsDirty() {
  state.settingsDirty = true;
  setSaveHint("参数有未保存修改。", SAVE_HINT_CLASS_WARN);
}

function markSettingsSaved() {
  state.settingsDirty = false;
  setSaveHint(`已保存（${new Date().toLocaleTimeString()}）`, SAVE_HINT_CLASS_OK);
}

function findGroup(groupId) {
  return state.strategyGroups.find((group) => group.id === groupId) || null;
}

const pathToString = ParamRender.pathToString;

const getValueAtPath = ParamRender.getValueAtPath;

function setValueAtPath(root, path, value) {
  if (!path.length) return;
  let current = root;
  for (let index = 0; index < path.length - 1; index += 1) {
    const key = path[index];
    if (!current[key] || typeof current[key] !== "object" || Array.isArray(current[key])) {
      current[key] = {};
    }
    current = current[key];
  }
  current[path[path.length - 1]] = value;
}

// renderParamField and renderInlineTemplateSection delegated to ParamRender
const renderParamField = ParamRender.renderParamField;
const renderInlineTemplateSection = ParamRender.renderInlineTemplateSection;

function refreshSectionDisableStates(root) {
  if (!root) return;
  // 处理所有抽屉式 section 的开关（标准 + 内嵌模板共用 data-drawer-section）
  root.querySelectorAll("[data-drawer-section]").forEach((body) => {
    const sectionKey = body.dataset.drawerSection;
    const enabledPath = sectionKey.split(".").concat("enabled").filter(Boolean);
    const enabled = getValueAtPath(state.groupParams, enabledPath);
    body.classList.toggle("param-drawer-closed", enabled !== true);
  });
  // 处理内嵌模板的 toggle 行
  root.querySelectorAll("[data-inline-toggle-body]").forEach((body) => {
    const toggleKey = body.dataset.inlineToggleBody;
    const togglePath = toggleKey.split(".").filter(Boolean);
    const toggleVal = getValueAtPath(state.groupParams, togglePath);
    body.classList.toggle("param-inline-toggle-disabled", toggleVal !== true);
  });
}

function renderTagEditor(container, path, items) {
  const safeItems = Array.isArray(items) ? items.filter((item) => typeof item === "string" && item.trim()) : [];
  container.innerHTML = `
    <div class="param-tags-shell">
      ${safeItems.map((item) => `
        <button
          type="button"
          class="param-tag-chip"
          data-param-tag-remove="${escapeHtml(pathToString(path))}"
          data-param-tag-value="${escapeHtml(item)}"
        >
          <span>${escapeHtml(item)}</span>
          <span class="param-tag-chip-x">x</span>
        </button>
      `).join("")}
      <input
        type="text"
        class="param-tag-input"
        data-param-tag-input="${escapeHtml(pathToString(path))}"
        placeholder="回车或逗号新增"
      />
    </div>
  `;
}

function refreshTagEditors(root = $("groupParamsForm")) {
  if (!root) return;
  root.querySelectorAll(".param-tags-editor[data-param-path]").forEach((node) => {
    const path = String(node.dataset.paramPath || "").split(".").filter(Boolean);
    renderTagEditor(node, path, getValueAtPath(state.groupParams, path));
  });
}

function renderGroupParamsForm() {
  const container = $("groupParamsForm");
  if (!container) return;
  const group = findGroup(selectedGroupId());
  if (!group) {
    container.innerHTML = '<div class="param-panel-empty">请选择策略组</div>';
    return;
  }
  container.innerHTML = renderParamField([], state.groupParams, group.param_help || {}, "", 0);
  refreshTagEditors(container);
  refreshSectionDisableStates(container);
}

function resetGroupParamsFromDefault(groupId) {
  const group = findGroup(groupId);
  if (!group) {
    state.groupParams = {};
    renderGroupParamsForm();
    const desc = $("strategyGroupDesc");
    if (desc) desc.textContent = "-";
    return;
  }
  const base = cloneValue(group.default_params || {});
  // 从服务端加载的 perStrategyParams 恢复已保存的参数
  const saved = state.perStrategyParams[groupId];
  console.log("[DBG reset]", groupId, "default=", cloneValue(base), "saved=", cloneValue(saved || null));
  if (saved && typeof saved === "object") {
    deepMerge(base, saved);
  }
  console.log("[DBG reset] merged=", cloneValue(base));
  state.groupParams = base;
  updateStrategyGroupDescription(groupId);
  renderGroupParamsForm();
}

function collectMonitorSettings() {
  // 先把当前策略的参数同步到 perStrategyParams
  const gid = selectedGroupId();
  if (gid && Object.keys(state.groupParams).length) {
    state.perStrategyParams[gid] = cloneValue(state.groupParams);
  }
  return {
    source_db: $("sourceDb")?.value || "",
    stocks_input: $("stocksInput")?.value || "",
    sample_size: getCurrentSampleSize(),
    strategy_group_id: gid,
    group_params: cloneValue(state.groupParams),
    per_strategy_params: cloneValue(state.perStrategyParams),
  };
}

async function getJSON(url) {
  const resp = await fetch(url);
  if (!resp.ok) {
    const detail = await readErrorDetail(resp);
    throw new Error(`请求失败(${resp.status})：${detail}`);
  }
  return resp.json();
}

async function postJSON(url, payload = {}) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    const detail = await readErrorDetail(resp);
    throw new Error(`请求失败(${resp.status})：${detail}`);
  }
  return resp.json();
}

async function readErrorDetail(resp) {
  const text = await resp.text();
  const contentType = (resp.headers.get("content-type") || "").toLowerCase();
  if (contentType.includes("application/json")) {
    try {
      const payload = JSON.parse(text);
      if (typeof payload?.detail === "string" && payload.detail.trim()) {
        return payload.detail;
      }
      if (payload?.detail !== undefined) {
        return JSON.stringify(payload.detail);
      }
    } catch {
      // ignore invalid JSON
    }
  }
  return text || "未知错误";
}

function extractErrorDetail(message) {
  const match = /^请求失败\(\d+\)：([\s\S]*)$/.exec(String(message || ""));
  return (match ? match[1] : String(message || "")).trim() || "未知错误";
}

function applyMonitorSettings(settings) {
  if (!settings || typeof settings !== "object") return;
  if (typeof settings.source_db === "string" && $("sourceDb")) {
    $("sourceDb").value = settings.source_db;
  }
  if (typeof settings.stocks_input === "string" && $("stocksInput")) {
    $("stocksInput").value = settings.stocks_input;
  }
  const sampleInput = $("sampleSize");
  if (sampleInput) {
    sampleInput.value = String(normalizeSampleSize(settings.sample_size));
  }

  // 加载服务端存储的每策略参数
  const psp = settings.per_strategy_params;
  if (psp && typeof psp === "object") {
    state.perStrategyParams = cloneValue(psp);
  }

  const savedGroupId = typeof settings.strategy_group_id === "string" ? settings.strategy_group_id : "";
  const savedGroup = savedGroupId ? findGroup(savedGroupId) : null;
  if (savedGroup) {
    $("strategyGroupSelect").value = savedGroupId;
    updateStrategyGroupDescription(savedGroupId);
    const base = cloneValue(savedGroup.default_params || {});
    // 使用 perStrategyParams 中当前策略的参数
    const saved = state.perStrategyParams[savedGroupId];
    if (saved && typeof saved === "object") {
      deepMerge(base, saved);
    }
    state.groupParams = base;
    state._prevGroupId = savedGroupId;
    renderGroupParamsForm();
    return;
  }

  updateStrategyGroupDescription(selectedGroupId());
  renderGroupParamsForm();
}

async function loadMonitorSettings() {
  const data = await getJSON("/api/ui-settings/monitor");
  applyMonitorSettings(data.settings);
  state.settingsDirty = false;
  setSaveHint("参数修改后请点“保存参数设置”。", "hint");
}

async function saveMonitorSettings() {
  const payload = collectMonitorSettings();
  payload.sample_size = normalizeSampleSize(payload.sample_size);
  const sampleInput = $("sampleSize");
  if (sampleInput) {
    sampleInput.value = String(payload.sample_size);
  }
  const resp = await postJSON("/api/ui-settings/monitor", payload);
  // 服务端返回的 per_strategy_params 是权威数据，回写本地
  if (resp.settings && resp.settings.per_strategy_params) {
    state.perStrategyParams = cloneValue(resp.settings.per_strategy_params);
  }
  markSettingsSaved();
}

async function onSaveSettingsClick() {
  try {
    await saveMonitorSettings();
  } catch (err) {
    const detail = extractErrorDetail(err?.message || "");
    setSaveHint(`保存失败：${detail}`, SAVE_HINT_CLASS_ERROR);
    alert(`保存失败：${detail}`);
  }
}

function formatDateTime(value) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function formatLogLine(item) {
  const ts = item.ts ? new Date(item.ts).toLocaleString() : "-";
  const detail = item.detail ? ` | ${JSON.stringify(item.detail)}` : "";
  return `${ts} [${LOG_LEVEL_TEXT[item.level] || item.level}] ${item.message}${detail}`;
}

function isNearBottom(el) {
  return el.scrollHeight - (el.scrollTop + el.clientHeight) <= LOG_AUTO_SCROLL_THRESHOLD;
}

function syncLogBox(el, text, autoScroll) {
  const nearBottom = isNearBottom(el);
  const prevTop = el.scrollTop;
  el.textContent = text;
  if (autoScroll || nearBottom) {
    el.scrollTop = el.scrollHeight;
    return;
  }
  const maxTop = Math.max(0, el.scrollHeight - el.clientHeight);
  el.scrollTop = Math.min(prevTop, maxTop);
}

function renderControlButtons(task) {
  const pauseBtn = $("pauseResumeBtn");
  const stopBtn = $("stopTaskBtn");
  const status = task?.status;

  if (status === "paused") {
    pauseBtn.disabled = false;
    pauseBtn.textContent = "恢复任务";
  } else if (status === "running" || status === "queued") {
    pauseBtn.disabled = false;
    pauseBtn.textContent = "暂停任务";
  } else {
    pauseBtn.disabled = true;
    pauseBtn.textContent = "暂停任务";
  }

  stopBtn.disabled = !(status === "running" || status === "queued" || status === "paused" || status === "stopping");
}

function renderStatus(task) {
  state.lastTask = task;
  $("statusText").textContent = STATUS_TEXT[task.status] || task.status;
  $("modeText").textContent = MODE_TEXT[task.run_mode] || task.run_mode || "-";
  $("strategyNameText").textContent = task.strategy_name || "-";
  $("strategyDescText").textContent = task.strategy_description || "-";
  $("progressText").textContent = `${Number(task.progress || 0).toFixed(2)}%`;
  $("stocksProgressText").textContent = `${task.processed_stocks} / ${task.total_stocks}`;
  $("resultCountText").textContent = `${task.result_count}`;
  $("logCountText").textContent = `信息 ${task.info_log_count} / 错误 ${task.error_log_count}`;
  $("currentCodeText").textContent = task.current_code || "-";
  $("unresolvedText").textContent = (task.unresolved_inputs || []).join(", ") || "-";
  $("fatalErrorText").textContent = task.error_message || "-";
  $("progressBarInner").style.width = `${Math.max(0, Math.min(100, Number(task.progress || 0)))}%`;
  renderControlButtons(task);
  syncErrorTabAlert();
}

async function loadLogs(taskId, infoTotalCount, errorTotalCount) {
  const safeInfoTotal = Math.max(0, Number(infoTotalCount || 0));
  const safeErrorTotal = Math.max(0, Number(errorTotalCount || 0));
  const infoBootstrapOffset = Math.max(0, safeInfoTotal - LOG_DISPLAY_LIMIT);
  const errorBootstrapOffset = Math.max(0, safeErrorTotal - LOG_DISPLAY_LIMIT);
  const baseInfoUrl = `/api/tasks/${taskId}/logs?level=info&limit=${LOG_DISPLAY_LIMIT}`;
  const baseErrorUrl = `/api/tasks/${taskId}/logs?level=error&limit=${LOG_DISPLAY_LIMIT}`;
  const infoUrl = state.lastInfoLogId == null
    ? `${baseInfoUrl}&offset=${infoBootstrapOffset}`
    : `${baseInfoUrl}&after_log_id=${Math.max(0, Number(state.lastInfoLogId) || 0)}`;
  const errorUrl = state.lastErrorLogId == null
    ? `${baseErrorUrl}&offset=${errorBootstrapOffset}`
    : `${baseErrorUrl}&after_log_id=${Math.max(0, Number(state.lastErrorLogId) || 0)}`;

  const [info, error] = await Promise.all([getJSON(infoUrl), getJSON(errorUrl)]);
  const infoItems = Array.isArray(info.items) ? info.items : [];
  const errorItems = Array.isArray(error.items) ? error.items : [];

  state.infoLogs = state.infoLogs.concat(infoItems).slice(-LOG_DISPLAY_LIMIT);
  state.errorLogs = state.errorLogs.concat(errorItems).slice(-LOG_DISPLAY_LIMIT);

  const infoFallback = infoItems.length ? Number(infoItems[infoItems.length - 1].log_id || 0) : 0;
  const errorFallback = errorItems.length ? Number(errorItems[errorItems.length - 1].log_id || 0) : 0;
  state.lastInfoLogId = Number.isFinite(Number(info.next_after_log_id))
    ? Number(info.next_after_log_id)
    : (state.lastInfoLogId ?? infoFallback);
  state.lastErrorLogId = Number.isFinite(Number(error.next_after_log_id))
    ? Number(error.next_after_log_id)
    : (state.lastErrorLogId ?? errorFallback);
  if (state.lastInfoLogId == null) state.lastInfoLogId = infoFallback;
  if (state.lastErrorLogId == null) state.lastErrorLogId = errorFallback;

  syncLogBox($("infoLogs"), state.infoLogs.map(formatLogLine).join("\n"), state.infoLogAutoScroll);
  syncLogBox($("errorLogs"), state.errorLogs.map(formatLogLine).join("\n"), state.errorLogAutoScroll);
  syncErrorTabAlert();
}

function resetTaskLogCursor() {
  state.lastInfoLogId = null;
  state.lastErrorLogId = null;
  state.infoLogs = [];
  state.errorLogs = [];
  state.infoLogAutoScroll = true;
  state.errorLogAutoScroll = true;
  $("infoLogs").textContent = "";
  $("errorLogs").textContent = "";
  syncErrorTabAlert();
}

async function loadTaskStatus(taskId) {
  const task = await getJSON(`/api/tasks/${taskId}`);
  renderStatus(task);
  await loadLogs(taskId, task.info_log_count, task.error_log_count);
}

function stopPolling() {
  if (state.eventSource) {
    state.eventSource.close();
    state.eventSource = null;
  }
}

function stopHeartbeatStream() {
  if (state.heartbeatSource) {
    state.heartbeatSource.close();
    state.heartbeatSource = null;
  }
}

function startHeartbeatStream() {
  stopHeartbeatStream();
  const es = new EventSource("/api/stream/heartbeat");
  state.heartbeatSource = es;
  setStreamState("SSE: 连接中", "is-connecting");
  es.onopen = () => setStreamState("SSE: 已连接", "is-connected");
  es.onerror = () => setStreamState("SSE: 重连中", "is-connecting");
}

function startPolling() {
  stopPolling();
  if (!state.taskId) {
    startHeartbeatStream();
    return;
  }

  stopHeartbeatStream();
  const es = new EventSource(`/api/tasks/${state.taskId}/stream`);
  state.eventSource = es;
  setStreamState("SSE: 连接中", "is-connecting");

  es.onopen = () => setStreamState("SSE: 已连接", "is-connected");
  es.onerror = () => setStreamState("SSE: 重连中", "is-connecting");

  es.addEventListener("task-status", (event) => {
    renderStatus(JSON.parse(event.data));
  });

  es.addEventListener("logs-info", (event) => {
    const items = JSON.parse(event.data);
    state.infoLogs = state.infoLogs.concat(items).slice(-LOG_DISPLAY_LIMIT);
    syncLogBox($("infoLogs"), state.infoLogs.map(formatLogLine).join("\n"), state.infoLogAutoScroll);
    syncErrorTabAlert();
  });

  es.addEventListener("logs-error", (event) => {
    const items = JSON.parse(event.data);
    state.errorLogs = state.errorLogs.concat(items).slice(-LOG_DISPLAY_LIMIT);
    syncLogBox($("errorLogs"), state.errorLogs.map(formatLogLine).join("\n"), state.errorLogAutoScroll);
    syncErrorTabAlert();
  });

  es.addEventListener("done", () => {
    setStreamState("SSE: 任务结束", "is-idle");
    if (state.eventSource === es) {
      es.close();
      state.eventSource = null;
    }
  });

  es.addEventListener("stream-error", () => {
    setStreamState("SSE: 任务不可用", "is-error");
    if (state.eventSource === es) {
      es.close();
      state.eventSource = null;
    }
    startHeartbeatStream();
  });
}

function parseSharedTaskPayload(raw) {
  if (!raw) return null;
  try {
    const payload = JSON.parse(raw);
    const taskId = typeof payload?.task_id === "string" && payload.task_id ? payload.task_id : null;
    const source = typeof payload?.source === "string" ? payload.source : "";
    if (!taskId) return null;
    return { taskId, source };
  } catch {
    return null;
  }
}

function publishTaskSelection(taskId) {
  if (!taskId) return;
  const payload = JSON.stringify({ task_id: taskId, source: TAB_ID, ts: Date.now() });
  try {
    localStorage.setItem(SHARED_TASK_KEY, payload);
  } catch {
    // ignore storage errors
  }
  if (syncChannel) {
    syncChannel.postMessage(payload);
  }
}

async function applySharedTaskSelection(rawPayload) {
  const parsed = parseSharedTaskPayload(rawPayload);
  if (!parsed || parsed.source === TAB_ID || parsed.taskId === state.taskId) return;
  await refreshTaskList(parsed.taskId, false);
}

async function refreshTaskList(selectTaskId = null, shouldBroadcast = true) {
  if (state.pollInFlight) return;
  state.pollInFlight = true;
  try {
    const data = await getJSON("/api/tasks?offset=0&limit=150");
    const select = $("taskSelect");
    select.innerHTML = "";

    for (const item of data.items || []) {
      const option = document.createElement("option");
      option.value = item.task_id;
      const d = item.created_at ? new Date(item.created_at) : null;
      const ts = d ? ` | ${String(d.getMonth()+1).padStart(2,"0")}/${String(d.getDate()).padStart(2,"0")} ${String(d.getHours()).padStart(2,"0")}:${String(d.getMinutes()).padStart(2,"0")}` : "";
      option.textContent =
        `${item.task_id.slice(0, 8)} | ${item.strategy_name || "-"} | ${STATUS_TEXT[item.status] || item.status} | ` +
        `${item.processed_stocks}/${item.total_stocks} | 命中${item.result_count}${ts}`;
      select.appendChild(option);
    }

    const targetTaskId = selectTaskId || state.taskId || (data.items?.[0]?.task_id ?? null);
    if (!targetTaskId) {
      state.taskId = null;
      state.lastTask = null;
      resetTaskLogCursor();
      renderControlButtons(null);
      startPolling();
      return;
    }

    select.value = targetTaskId;
    state.taskId = targetTaskId;
    if (shouldBroadcast) {
      publishTaskSelection(state.taskId);
    }
    resetTaskLogCursor();
    await loadTaskStatus(state.taskId);
    startPolling();
  } finally {
    state.pollInFlight = false;
  }
}

async function createTask(runMode = "full") {
  const groupId = selectedGroupId();
  if (!groupId) {
    alert("请选择策略组");
    return;
  }

  const payload = {
    stocks: splitStocks($("stocksInput")?.value || ""),
    source_db: $("sourceDb")?.value.trim() || null,
    run_mode: runMode,
    sample_size: getCurrentSampleSize(),
    strategy_group_id: groupId,
    group_params: cloneValue(state.groupParams),
    skip_coverage_filter: $("skipCoverageFilter")?.checked ?? true,
  };

  const resp = await postJSON("/api/tasks", payload);
  await refreshTaskList(resp.task_id);
}

async function pauseOrResumeTask() {
  if (!state.taskId || !state.lastTask || state.pollInFlight) return;
  state.pollInFlight = true;
  try {
    const url = state.lastTask.status === "paused"
      ? `/api/tasks/${state.taskId}/resume`
      : `/api/tasks/${state.taskId}/pause`;
    await postJSON(url, {});
    const task = await getJSON(`/api/tasks/${state.taskId}`);
    renderStatus(task);
  } finally {
    state.pollInFlight = false;
  }
}

async function stopTask() {
  if (!state.taskId || state.pollInFlight) return;
  state.pollInFlight = true;
  try {
    await postJSON(`/api/tasks/${state.taskId}/stop`, {});
    const task = await getJSON(`/api/tasks/${state.taskId}`);
    renderStatus(task);
  } finally {
    state.pollInFlight = false;
  }
}

async function loadStrategyGroups() {
  const data = await getJSON("/api/strategy-groups");
  state.strategyGroups = (data.items || []).filter((group) => group && (group.usage || []).includes("screening"));
  const select = $("strategyGroupSelect");
  select.innerHTML = "";

  for (const group of state.strategyGroups) {
    const option = document.createElement("option");
    option.value = group.id;
    option.textContent = `${group.name} (${group.id})`;
    select.appendChild(option);
  }

  if (!state.strategyGroups.length) {
    $("groupParamsForm").innerHTML = '<div class="param-panel-empty">未发现可用策略组</div>';
    $("strategyGroupDesc").innerHTML = "未发现可用策略组";
    return;
  }

  const first = state.strategyGroups[0];
  select.value = first.id;
  state._prevGroupId = first.id;
  resetGroupParamsFromDefault(first.id);
}

function commitTagInput(inputEl) {
  const path = String(inputEl.dataset.paramTagInput || "").split(".").filter(Boolean);
  const rawText = String(inputEl.value || "");
  const tokens = rawText
    .split(/[,\n，]+/)
    .map((item) => item.trim())
    .filter(Boolean);
  if (!tokens.length) {
    inputEl.value = "";
    return;
  }

  const currentRaw = getValueAtPath(state.groupParams, path);
  const current = Array.isArray(currentRaw) ? [...currentRaw] : [];
  for (const token of tokens) {
    if (!current.includes(token)) current.push(token);
  }
  setValueAtPath(state.groupParams, path, current);

  const editor = inputEl.closest(".param-tags-editor");
  if (editor) {
    renderTagEditor(editor, path, current);
  }
  markSettingsDirty();
}

function bindEvents() {
  $("saveFormSettingsBtn").addEventListener("click", () => {
    onSaveSettingsClick().catch((err) => alert(err.message));
  });
  $("createTaskFullBtn").addEventListener("click", () => {
    createTask().catch((err) => alert(err.message));
  });
  $("refreshTasksBtn").addEventListener("click", () => {
    refreshTaskList().catch((err) => alert(err.message));
  });
  $("pauseResumeBtn").addEventListener("click", () => {
    pauseOrResumeTask().catch((err) => alert(err.message));
  });
  $("stopTaskBtn").addEventListener("click", () => {
    stopTask().catch((err) => alert(err.message));
  });

  $("taskSelect").addEventListener("change", (event) => {
    state.taskId = event.target.value;
    state.lastTask = null;
    publishTaskSelection(state.taskId);
    refreshTaskList(state.taskId, false).catch((err) => alert(err.message));
  });

  $("strategyGroupSelect").addEventListener("change", (event) => {
    // 切换前：将当前策略参数写入 perStrategyParams
    const prevId = state._prevGroupId;
    console.log("[DBG switch] prevId=", prevId, "groupParams=", cloneValue(state.groupParams));
    if (prevId && Object.keys(state.groupParams).length) {
      state.perStrategyParams[prevId] = cloneValue(state.groupParams);
      console.log("[DBG switch] saved perStrategyParams[", prevId, "]=", cloneValue(state.perStrategyParams[prevId]));
    } else {
      console.warn("[DBG switch] SKIP save — prevId=", prevId, "keys=", Object.keys(state.groupParams).length);
    }
    const newId = event.target.value;
    console.log("[DBG switch] newId=", newId, "perStrategyParams[newId]=", cloneValue(state.perStrategyParams[newId] || null));
    resetGroupParamsFromDefault(newId);
    state._prevGroupId = newId;
  });

  ["sourceDb", "stocksInput", "skipCoverageFilter", "sampleSize"].forEach((id) => {
    const el = $(id);
    if (!el) return;
    el.addEventListener("input", markSettingsDirty);
    el.addEventListener("change", markSettingsDirty);
  });

  const form = $("groupParamsForm");
  form.addEventListener("input", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (!target.dataset.paramPath) return;
    const path = String(target.dataset.paramPath).split(".").filter(Boolean);

    if (target.type === "checkbox") {
      setValueAtPath(state.groupParams, path, target.checked);
      const valueNode = target.closest(".checkbox-field")?.querySelector(".param-checkbox-value")
        || target.closest(".param-checkbox-row")?.querySelector(".param-checkbox-value");
      if (valueNode) valueNode.textContent = target.checked ? "开启" : "关闭";
      // 级联禁用：checkbox 变化时刷新 section / toggle 的灰化状态
      refreshSectionDisableStates(form);
    } else if (target.type === "number") {
      if (target.value === "") return;
      const parsed = target.dataset.paramNumberKind === "int"
        ? parseInt(target.value, 10)
        : parseFloat(target.value);
      if (!Number.isFinite(parsed)) return;
      setValueAtPath(state.groupParams, path, parsed);
    } else {
      setValueAtPath(state.groupParams, path, target.value);
    }
    markSettingsDirty();
  });

  form.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (!target.dataset.paramPath || target.type !== "number") return;
    const parsed = target.dataset.paramNumberKind === "int"
      ? parseInt(target.value, 10)
      : parseFloat(target.value);
    if (!Number.isFinite(parsed)) return;
    const path = String(target.dataset.paramPath).split(".").filter(Boolean);
    setValueAtPath(state.groupParams, path, parsed);
    target.value = String(parsed);
    markSettingsDirty();
  });

  form.addEventListener("keydown", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (!target.dataset.paramTagInput) return;

    if (event.key === "Enter" || event.key === "," || event.key === "，") {
      event.preventDefault();
      commitTagInput(target);
      return;
    }

    if (event.key === "Backspace" && !target.value.trim()) {
      const path = String(target.dataset.paramTagInput).split(".").filter(Boolean);
      const currentRaw = getValueAtPath(state.groupParams, path);
      const current = Array.isArray(currentRaw) ? [...currentRaw] : [];
      if (!current.length) return;
      current.pop();
      setValueAtPath(state.groupParams, path, current);
      const editor = target.closest(".param-tags-editor");
      if (editor) renderTagEditor(editor, path, current);
      markSettingsDirty();
    }
  });

  form.addEventListener("blur", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (!target.dataset.paramTagInput) return;
    commitTagInput(target);
  }, true);

  form.addEventListener("click", (event) => {
    const button = event.target instanceof Element ? event.target.closest("[data-param-tag-remove]") : null;
    if (!button) return;
    const path = String(button.dataset.paramTagRemove || "").split(".").filter(Boolean);
    const value = String(button.dataset.paramTagValue || "");
    const currentRaw = getValueAtPath(state.groupParams, path);
    const current = Array.isArray(currentRaw) ? [...currentRaw] : [];
    const next = current.filter((item) => item !== value);
    setValueAtPath(state.groupParams, path, next);
    const editor = button.closest(".param-tags-editor");
    if (editor) renderTagEditor(editor, path, next);
    markSettingsDirty();
  });
}

function bindCrossTabSync() {
  window.addEventListener("storage", (event) => {
    if (event.key !== SHARED_TASK_KEY || !event.newValue) return;
    applySharedTaskSelection(event.newValue).catch((err) => {
      console.error("跨标签任务同步失败(storage)", err);
    });
  });

  if (!syncChannel) return;
  syncChannel.addEventListener("message", (event) => {
    applySharedTaskSelection(typeof event.data === "string" ? event.data : "").catch((err) => {
      console.error("跨标签任务同步失败(channel)", err);
    });
  });
  window.addEventListener("beforeunload", () => syncChannel.close(), { once: true });
}

function bindLogAutoScroll() {
  const infoEl = $("infoLogs");
  const errorEl = $("errorLogs");
  if (infoEl) infoEl.addEventListener("scroll", () => { state.infoLogAutoScroll = isNearBottom(infoEl); });
  if (errorEl) errorEl.addEventListener("scroll", () => { state.errorLogAutoScroll = isNearBottom(errorEl); });
}

async function init() {
  bindEvents();
  bindLogAutoScroll();
  bindCrossTabSync();
  startHeartbeatStream();
  await loadStrategyGroups();
  try {
    await loadMonitorSettings();
  } catch (err) {
    console.warn("加载监控页参数设置失败，将使用页面默认值", err);
    setSaveHint("参数修改后请点“保存参数设置”。", "hint");
  }
  const taskIdFromUrl = new URLSearchParams(window.location.search).get("task_id");
  await refreshTaskList(taskIdFromUrl || null);
}

init().catch((err) => {
  console.error("页面初始化失败", err);
  alert(`页面初始化失败：${err.message}`);
});
