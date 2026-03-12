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
  groupParamsEditor: null,
  groupParamsMuteChange: false,
  groupParamsCommentLines: [],
  groupParamsFlashLineHandle: null,
  stockStatesRaw: [],
  stockStateSortKey: "",
  stockStateSortDirection: "asc",
  stockStateFilters: {
    code: "",
    name: "",
    status: "",
    processed_bars: "",
    signal_count: "",
    last_dt: "",
    error_message: "",
  },
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
const SAVE_HINT_CLASS_WARN = "hint warn";
const SAVE_HINT_CLASS_OK = "hint ok";
const SAVE_HINT_CLASS_ERROR = "hint error";

const SHARED_TASK_KEY = "screening:selected_task_id";
const SYNC_CHANNEL_NAME = "screening-task-sync";
const TAB_ID = `monitor-${Date.now()}-${Math.random().toString(16).slice(2)}`;
const syncChannel = "BroadcastChannel" in window ? new BroadcastChannel(SYNC_CHANNEL_NAME) : null;
let groupParamsFlashTimer = null;

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
  return text
    .split(/[\n,，\s]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function localDateTimeOrNull(v) {
  if (!v) return null;
  return v.length === 16 ? `${v}:00` : v;
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function datePartFromDate(value) {
  return `${value.getFullYear()}-${pad2(value.getMonth() + 1)}-${pad2(value.getDate())}`;
}

function calendarMonthAgo(value) {
  const base = value instanceof Date ? value : new Date();
  const monthAnchor = new Date(base.getFullYear(), base.getMonth() - 1, 1);
  const maxDay = new Date(monthAnchor.getFullYear(), monthAnchor.getMonth() + 1, 0).getDate();
  const day = Math.min(base.getDate(), maxDay);
  return new Date(monthAnchor.getFullYear(), monthAnchor.getMonth(), day);
}

function setDefaultTaskTimeRange(force = false) {
  const startInput = $("startTs");
  const endInput = $("endTs");
  if (!startInput || !endInput) return;

  if (force || !startInput.value) {
    const monthAgo = calendarMonthAgo(new Date());
    startInput.value = `${datePartFromDate(monthAgo)}T09:30`;
  }
  if (force || !endInput.value) {
    endInput.value = `${datePartFromDate(new Date())}T15:00`;
  }
}

function selectedGroupId() {
  return $("strategyGroupSelect").value;
}

function getGroupParamsText() {
  if (state.groupParamsEditor) {
    return state.groupParamsEditor.getValue();
  }
  return $("groupParams").value || "";
}

function clearGroupParamsCommentLineClasses() {
  if (!state.groupParamsEditor) return;
  for (const handle of state.groupParamsCommentLines) {
    state.groupParamsEditor.removeLineClass(handle, "text", "cm-comment-line");
  }
  state.groupParamsCommentLines = [];
}

function refreshGroupParamsCommentLineHighlights() {
  if (!state.groupParamsEditor) return;
  clearGroupParamsCommentLineClasses();
  const editor = state.groupParamsEditor;
  const re = /"__comment[^"]*"\s*:/;
  for (let i = 0; i < editor.lineCount(); i += 1) {
    const text = editor.getLine(i) || "";
    if (!re.test(text)) continue;
    const handle = editor.getLineHandle(i);
    editor.addLineClass(handle, "text", "cm-comment-line");
    state.groupParamsCommentLines.push(handle);
  }
}

function setGroupParamsText(value) {
  const safeValue = typeof value === "string" ? value : "{}";
  if (state.groupParamsEditor) {
    state.groupParamsMuteChange = true;
    state.groupParamsEditor.setValue(safeValue);
    state.groupParamsMuteChange = false;
    $("groupParams").value = safeValue;
    refreshGroupParamsCommentLineHighlights();
    return;
  }
  $("groupParams").value = safeValue;
}

function initGroupParamsEditor() {
  const textarea = $("groupParams");
  if (!textarea || !window.CodeMirror) {
    return;
  }
  const editor = window.CodeMirror.fromTextArea(textarea, {
    mode: { name: "javascript", json: true },
    theme: "material-darker",
    lineNumbers: true,
    indentUnit: 2,
    tabSize: 2,
    lineWrapping: false,
    matchBrackets: true,
  });
  state.groupParamsEditor = editor;
  editor.on("change", () => {
    textarea.value = editor.getValue();
    refreshGroupParamsCommentLineHighlights();
    if (state.groupParamsMuteChange) return;
    markSettingsDirty();
  });
  refreshGroupParamsCommentLineHighlights();
}

function normalizeSampleSize(rawValue) {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) return 20;
  return Math.max(1, Math.min(5000, Math.floor(parsed)));
}

function setSaveHint(text, cssClass) {
  const hint = $("settingsSaveHint");
  if (!hint) return;
  hint.className = cssClass || "hint";
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

function collectMonitorSettings() {
  return {
    source_db: $("sourceDb").value || "",
    stocks_input: $("stocksInput").value || "",
    start_ts: $("startTs").value || "",
    end_ts: $("endTs").value || "",
    sample_size: getCurrentSampleSize(),
    strategy_group_id: selectedGroupId() || "",
    group_params_text: getGroupParamsText() || "{}",
  };
}

function getCurrentSampleSize() {
  const input = $("sampleSize");
  if (!input) return 20;
  const normalized = normalizeSampleSize(input.value);
  input.value = String(normalized);
  return normalized;
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
  const payload = JSON.stringify({
    task_id: taskId,
    source: TAB_ID,
    ts: Date.now(),
  });
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
  if (!parsed || parsed.source === TAB_ID || parsed.taskId === state.taskId) {
    return;
  }
  await refreshTaskList(parsed.taskId, false);
}

function findGroup(groupId) {
  return state.strategyGroups.find((g) => g.id === groupId) || null;
}

function cloneValue(value) {
  return JSON.parse(JSON.stringify(value));
}

function helpTextFromNode(node) {
  if (!node) return "";
  if (typeof node === "string") return node;
  if (Array.isArray(node)) return node.map((x) => String(x)).join("\n");
  if (typeof node === "object") {
    const blocks = [];
    if (typeof node._comment === "string" && node._comment.trim()) {
      blocks.push(node._comment.trim());
    }
    if (Array.isArray(node._overview) && node._overview.length) {
      blocks.push(node._overview.map((x) => String(x)).join("\n"));
    }
    return blocks.join("\n\n");
  }
  return "";
}

function buildAnnotatedParams(params, helpNode) {
  if (!params || typeof params !== "object" || Array.isArray(params)) {
    return params;
  }

  const result = {};
  const rootHelp = helpTextFromNode(helpNode);
  if (rootHelp) {
    result.__comment__ = rootHelp;
  }

  for (const key of Object.keys(params)) {
    const childHelpNode = helpNode && typeof helpNode === "object" ? helpNode[key] : null;
    const childHelpText = helpTextFromNode(childHelpNode);
    if (childHelpText) {
      result[`__comment_${key}`] = childHelpText;
    }
    result[key] = buildAnnotatedParams(params[key], childHelpNode);
  }
  return result;
}

function stripCommentKeys(value) {
  if (Array.isArray(value)) {
    return value.map(stripCommentKeys);
  }
  if (!value || typeof value !== "object") {
    return value;
  }
  const result = {};
  for (const [key, child] of Object.entries(value)) {
    if (key.startsWith("__comment")) continue;
    result[key] = stripCommentKeys(child);
  }
  return result;
}

function parseGroupParamsTextOrThrow(rawText) {
  let parsed;
  try {
    parsed = JSON.parse(rawText || "{}");
  } catch (err) {
    throw new Error(`组级参数 JSON 格式错误: ${err.message}`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("组级参数 JSON 顶层必须是对象");
  }
  return parsed;
}

function escapeHtml(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function renderGroupDescription(group) {
  if (!group) return "-";
  const lines = [];
  lines.push(`说明：${group.description || "-"}`);

  if (group.engine) {
    lines.push(`执行引擎：${group.engine}`);
  }
  if (group.execution && typeof group.execution === "object") {
    lines.push(
      `执行特性：时间窗必填=${group.execution.requires_time_window ? "是" : "否"}，` +
      `任务内并行=${group.execution.supports_intra_task_parallel ? "是" : "否"}，` +
      `缓存=${group.execution.cache_scope || "none"}`
    );
  }

  const overview = group.param_help && Array.isArray(group.param_help._overview) ? group.param_help._overview : [];
  if (overview.length) {
    lines.push("参数示意：");
    for (const paragraph of overview) {
      lines.push(String(paragraph));
    }
  }
  return lines.map(escapeHtml).join("<br>");
}

function updateStrategyGroupDescription(groupId) {
  const group = findGroup(groupId);
  $("strategyGroupDesc").innerHTML = renderGroupDescription(group);
}

function fillGroupParamsFromDefault(groupId) {
  const group = findGroup(groupId);
  if (!group) {
    setGroupParamsText("{}");
    $("strategyGroupDesc").textContent = "-";
    return;
  }
  const annotated = buildAnnotatedParams(cloneValue(group.default_params || {}), group.param_help || {});
  setGroupParamsText(JSON.stringify(annotated, null, 2));
  updateStrategyGroupDescription(groupId);
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
      // ignore JSON parsing error, fallback to raw text
    }
  }
  return text || "未知错误";
}

function extractErrorDetail(message) {
  if (!message) return "未知错误";
  const match = /^请求失败\(\d+\)：([\s\S]*)$/.exec(String(message));
  return (match ? match[1] : String(message)).trim() || "未知错误";
}

function findLineByKey(lines, key) {
  const quoted = `"${key}"`;
  for (let i = 0; i < lines.length; i += 1) {
    if (lines[i].includes(quoted)) return i + 1;
  }
  return -1;
}

function focusGroupParamsLine(lineNumber, columnNumber = 1) {
  const text = getGroupParamsText();
  if (!text) return;
  const lines = text.split("\n");
  const clampedLine = Math.max(1, Math.min(lines.length, Number(lineNumber) || 1));
  const currentLine = lines[clampedLine - 1] || "";
  const clampedCol = Math.max(1, Math.min(currentLine.length + 1, Number(columnNumber) || 1));

  if (state.groupParamsEditor) {
    const editor = state.groupParamsEditor;
    const lineIdx = clampedLine - 1;
    editor.focus();
    editor.setCursor({ line: lineIdx, ch: clampedCol - 1 });
    editor.scrollIntoView({ line: lineIdx, ch: 0 }, 120);

    if (state.groupParamsFlashLineHandle) {
      editor.removeLineClass(state.groupParamsFlashLineHandle, "wrap", "cm-editor-error-line");
      state.groupParamsFlashLineHandle = null;
    }
    const handle = editor.getLineHandle(lineIdx);
    editor.addLineClass(handle, "wrap", "cm-editor-error-line");
    state.groupParamsFlashLineHandle = handle;

    if (groupParamsFlashTimer) {
      clearTimeout(groupParamsFlashTimer);
    }
    groupParamsFlashTimer = window.setTimeout(() => {
      if (state.groupParamsEditor && state.groupParamsFlashLineHandle) {
        state.groupParamsEditor.removeLineClass(
          state.groupParamsFlashLineHandle,
          "wrap",
          "cm-editor-error-line"
        );
      }
      state.groupParamsFlashLineHandle = null;
      groupParamsFlashTimer = null;
    }, 2000);
    return;
  }

  const textarea = $("groupParams");
  if (!textarea) return;
  let offset = 0;
  for (let i = 0; i < clampedLine - 1; i += 1) {
    offset += lines[i].length + 1;
  }
  offset += clampedCol - 1;
  textarea.focus();
  textarea.setSelectionRange(offset, offset + currentLine.length);
  textarea.classList.remove("focus-error");
  void textarea.offsetWidth;
  textarea.classList.add("focus-error");
  if (groupParamsFlashTimer) {
    clearTimeout(groupParamsFlashTimer);
  }
  groupParamsFlashTimer = window.setTimeout(() => {
    textarea.classList.remove("focus-error");
    groupParamsFlashTimer = null;
  }, 2000);

  const lineHeight = parseFloat(getComputedStyle(textarea).lineHeight) || 20;
  textarea.scrollTop = Math.max(0, (clampedLine - 3) * lineHeight);
}

function tryHighlightGroupParamsError(detail) {
  const text = getGroupParamsText();
  if (!text.trim()) return false;
  const lines = text.split("\n");

  const lineColMatch = /line\s+(\d+)\s*,\s*col\s+(\d+)/i.exec(detail);
  if (lineColMatch) {
    focusGroupParamsLine(Number(lineColMatch[1]), Number(lineColMatch[2]));
    return true;
  }

  const firstError = String(detail).split("；")[0] || String(detail);

  const pathMatch = /group_params(?:\.[A-Za-z0-9_]+)+/.exec(firstError);
  if (pathMatch) {
    const pathSegments = pathMatch[0].split(".");
    const leafKey = pathSegments[pathSegments.length - 1];
    const line = findLineByKey(lines, leafKey);
    if (line > 0) {
      focusGroupParamsLine(line, 1);
      return true;
    }
  }

  const commentKeyMatch = /(__comment[A-Za-z0-9_]*)/.exec(firstError);
  if (commentKeyMatch) {
    const line = findLineByKey(lines, commentKeyMatch[1]);
    if (line > 0) {
      focusGroupParamsLine(line, 1);
      return true;
    }
  }

  const missingFieldMatch = /缺少字段:\s*([A-Za-z0-9_]+)/.exec(firstError);
  if (missingFieldMatch) {
    const line = findLineByKey(lines, missingFieldMatch[1]);
    if (line > 0) {
      focusGroupParamsLine(line, 1);
      return true;
    }
  }

  focusGroupParamsLine(1, 1);
  return false;
}

function applyMonitorSettings(settings) {
  if (!settings || typeof settings !== "object") return;

  if (typeof settings.source_db === "string") {
    $("sourceDb").value = settings.source_db;
  }
  if (typeof settings.stocks_input === "string") {
    $("stocksInput").value = settings.stocks_input;
  }
  if (typeof settings.start_ts === "string") {
    $("startTs").value = settings.start_ts;
  }
  if (typeof settings.end_ts === "string") {
    $("endTs").value = settings.end_ts;
  }
  const sampleInput = $("sampleSize");
  if (sampleInput) {
    sampleInput.value = String(normalizeSampleSize(settings.sample_size));
  }

  const savedGroupId = typeof settings.strategy_group_id === "string" ? settings.strategy_group_id : "";
  if (savedGroupId && findGroup(savedGroupId)) {
    $("strategyGroupSelect").value = savedGroupId;
    updateStrategyGroupDescription(savedGroupId);
    if (typeof settings.group_params_text === "string" && settings.group_params_text.trim()) {
      setGroupParamsText(settings.group_params_text);
    } else {
      fillGroupParamsFromDefault(savedGroupId);
    }
    return;
  }

  if (typeof settings.group_params_text === "string" && settings.group_params_text.trim()) {
    setGroupParamsText(settings.group_params_text);
  }
  updateStrategyGroupDescription(selectedGroupId());
}

async function loadMonitorSettings() {
  const data = await getJSON("/api/ui-settings/monitor");
  applyMonitorSettings(data.settings);
  setDefaultTaskTimeRange(true);
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
  parseGroupParamsTextOrThrow(payload.group_params_text);
  await postJSON("/api/ui-settings/monitor", payload);
  markSettingsSaved();
}

async function onSaveSettingsClick() {
  try {
    await saveMonitorSettings();
  } catch (err) {
    const detail = extractErrorDetail(err?.message || "");
    setSaveHint(`保存失败：${detail}`, SAVE_HINT_CLASS_ERROR);
    alert(`保存失败：${detail}`);
    tryHighlightGroupParamsError(detail);
  }
}

function formatDateTime(v) {
  if (!v) return "-";
  return new Date(v).toLocaleString();
}

function formatLogLine(item) {
  const ts = item.ts ? new Date(item.ts).toLocaleString() : "-";
  const detail = item.detail ? ` | ${JSON.stringify(item.detail)}` : "";
  return `${ts} [${LOG_LEVEL_TEXT[item.level] || item.level}] ${item.message}${detail}`;
}

const LOG_AUTO_SCROLL_THRESHOLD = 24;

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
  const btn = $("pauseResumeBtn");
  const stopBtn = $("stopTaskBtn");
  const status = task?.status;

  if (status === "paused") {
    btn.disabled = false;
    btn.textContent = "恢复任务";
  } else if (status === "running" || status === "queued") {
    btn.disabled = false;
    btn.textContent = "暂停任务";
  } else {
    btn.disabled = true;
    btn.textContent = "暂停任务";
  }

  stopBtn.disabled = !(status === "running" || status === "queued" || status === "paused" || status === "stopping");
}

function renderStatus(task) {
  state.lastTask = task;
  $("statusText").textContent = STATUS_TEXT[task.status] || task.status;
  $("modeText").textContent = MODE_TEXT[task.run_mode] || task.run_mode || "-";
  $("strategyNameText").textContent = task.strategy_name || "-";
  $("strategyDescText").textContent = task.strategy_description || "-";
  $("progressText").textContent = `${task.progress.toFixed(2)}%`;
  $("stocksProgressText").textContent = `${task.processed_stocks} / ${task.total_stocks}`;
  $("resultCountText").textContent = `${task.result_count}`;
  $("logCountText").textContent = `信息 ${task.info_log_count} / 错误 ${task.error_log_count}`;
  $("currentCodeText").textContent = task.current_code || "-";
  $("unresolvedText").textContent = (task.unresolved_inputs || []).join(", ") || "-";
  $("fatalErrorText").textContent = task.error_message || "-";
  $("progressBarInner").style.width = `${Math.max(0, Math.min(100, task.progress))}%`;
  renderControlButtons(task);
  syncErrorTabAlert();
}

function renderStockStates(items) {
  state.stockStatesRaw = Array.isArray(items) ? items.slice() : [];
  renderFilteredStockStates();
}

function getStockStateFieldValue(row, key) {
  if (key === "status") {
    return STATUS_TEXT[row.status] || row.status || "";
  }
  if (key === "processed_bars" || key === "signal_count") {
    return String(row[key] ?? 0);
  }
  if (key === "last_dt") {
    return formatDateTime(row.last_dt);
  }
  return String(row[key] || "");
}

function getStockStateComparableValue(row, key) {
  if (key === "processed_bars" || key === "signal_count") {
    return Number(row[key] ?? 0);
  }
  if (key === "last_dt") {
    const ts = row.last_dt ? new Date(row.last_dt).getTime() : Number.NEGATIVE_INFINITY;
    return Number.isFinite(ts) ? ts : Number.NEGATIVE_INFINITY;
  }
  return String(getStockStateFieldValue(row, key)).toLowerCase();
}

function getFilteredSortedStockStates() {
  let rows = state.stockStatesRaw.slice();

  rows = rows.filter((row) => {
    return Object.entries(state.stockStateFilters).every(([key, rawFilter]) => {
      const filterText = String(rawFilter || "").trim().toLowerCase();
      if (!filterText) return true;
      const valueText = String(getStockStateFieldValue(row, key)).toLowerCase();
      return valueText.includes(filterText);
    });
  });

  if (state.stockStateSortKey) {
    const direction = state.stockStateSortDirection === "desc" ? -1 : 1;
    const sortKey = state.stockStateSortKey;
    rows.sort((left, right) => {
      const a = getStockStateComparableValue(left, sortKey);
      const b = getStockStateComparableValue(right, sortKey);
      if (a < b) return -1 * direction;
      if (a > b) return 1 * direction;
      return String(left.code || "").localeCompare(String(right.code || ""), "zh-CN") * direction;
    });
  }

  return rows;
}

function updateStockStateSortIndicators() {
  document.querySelectorAll("[data-stock-sort]").forEach((button) => {
    button.classList.remove("is-sort-asc", "is-sort-desc");
    if (button.dataset.stockSort !== state.stockStateSortKey) return;
    button.classList.add(state.stockStateSortDirection === "desc" ? "is-sort-desc" : "is-sort-asc");
  });
}

function renderFilteredStockStates() {
  const tbody = $("stockStatesTable").querySelector("tbody");
  const rows = getFilteredSortedStockStates();
  tbody.innerHTML = "";

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td class="stock-states-empty" colspan="7">没有匹配的逐股状态记录</td>';
    tbody.appendChild(tr);
    updateStockStateSortIndicators();
    return;
  }

  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(row.code || "")}</td>
      <td>${escapeHtml(row.name || "")}</td>
      <td>${escapeHtml(STATUS_TEXT[row.status] || row.status || "")}</td>
      <td>${row.processed_bars ?? 0}</td>
      <td>${row.signal_count ?? 0}</td>
      <td>${escapeHtml(formatDateTime(row.last_dt))}</td>
      <td>${escapeHtml(row.error_message || "")}</td>
    `;
    tbody.appendChild(tr);
  }

  updateStockStateSortIndicators();
}

function setStockStateFilter(key, value) {
  state.stockStateFilters[key] = String(value || "");
  renderFilteredStockStates();
}

function toggleStockStateSort(key) {
  if (state.stockStateSortKey === key) {
    state.stockStateSortDirection = state.stockStateSortDirection === "asc" ? "desc" : "asc";
  } else {
    state.stockStateSortKey = key;
    state.stockStateSortDirection = "asc";
  }
  renderFilteredStockStates();
}

async function loadLogs(taskId, infoTotalCount, errorTotalCount) {
  const safeInfoTotal = Math.max(0, Number(infoTotalCount || 0));
  const safeErrorTotal = Math.max(0, Number(errorTotalCount || 0));
  const infoBootstrapOffset = Math.max(0, safeInfoTotal - LOG_DISPLAY_LIMIT);
  const errorBootstrapOffset = Math.max(0, safeErrorTotal - LOG_DISPLAY_LIMIT);
  const baseInfoUrl = `/api/tasks/${taskId}/logs?level=info&limit=${LOG_DISPLAY_LIMIT}`;
  const baseErrorUrl = `/api/tasks/${taskId}/logs?level=error&limit=${LOG_DISPLAY_LIMIT}`;
  const infoUrl = state.lastInfoLogId === null || state.lastInfoLogId === undefined
    ? `${baseInfoUrl}&offset=${infoBootstrapOffset}`
    : `${baseInfoUrl}&after_log_id=${Math.max(0, Number(state.lastInfoLogId) || 0)}`;
  const errorUrl = state.lastErrorLogId === null || state.lastErrorLogId === undefined
    ? `${baseErrorUrl}&offset=${errorBootstrapOffset}`
    : `${baseErrorUrl}&after_log_id=${Math.max(0, Number(state.lastErrorLogId) || 0)}`;

  const [info, error] = await Promise.all([getJSON(infoUrl), getJSON(errorUrl)]);

  const infoItems = Array.isArray(info.items) ? info.items : [];
  const errorItems = Array.isArray(error.items) ? error.items : [];
  state.infoLogs = state.infoLogs.concat(infoItems).slice(-LOG_DISPLAY_LIMIT);
  state.errorLogs = state.errorLogs.concat(errorItems).slice(-LOG_DISPLAY_LIMIT);

  const infoNextRaw = info.next_after_log_id;
  const errorNextRaw = error.next_after_log_id;
  const infoFallback = infoItems.length > 0 ? Number(infoItems[infoItems.length - 1].log_id || 0) : 0;
  const errorFallback = errorItems.length > 0 ? Number(errorItems[errorItems.length - 1].log_id || 0) : 0;
  state.lastInfoLogId = Number.isFinite(Number(infoNextRaw))
    ? Number(infoNextRaw)
    : (state.lastInfoLogId ?? infoFallback);
  state.lastErrorLogId = Number.isFinite(Number(errorNextRaw))
    ? Number(errorNextRaw)
    : (state.lastErrorLogId ?? errorFallback);
  if (state.lastInfoLogId === null || state.lastInfoLogId === undefined) {
    state.lastInfoLogId = infoFallback;
  }
  if (state.lastErrorLogId === null || state.lastErrorLogId === undefined) {
    state.lastErrorLogId = errorFallback;
  }

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

async function loadStockStates(taskId) {
  const data = await getJSON(`/api/tasks/${taskId}/stock-states?offset=0&limit=3000`);
  renderStockStates(data.items || []);
}

async function loadTaskStatus(taskId) {
  const task = await getJSON(`/api/tasks/${taskId}`);
  renderStatus(task);

  await loadLogs(taskId, task.info_log_count, task.error_log_count);
  await loadStockStates(taskId);
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

  es.onopen = () => {
    setStreamState("SSE: 已连接", "is-connected");
  };

  es.onerror = () => {
    setStreamState("SSE: 重连中", "is-connecting");
  };
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

  es.onopen = () => {
    setStreamState("SSE: 已连接", "is-connected");
  };

  es.onerror = () => {
    // EventSource 会自动重连，这里只更新界面状态。
    setStreamState("SSE: 重连中", "is-connecting");
  };

  es.addEventListener("connected", () => {
    state.infoLogs = [];
    state.errorLogs = [];
    $("infoLogs").textContent = "";
    $("errorLogs").textContent = "";
    syncErrorTabAlert();
  });

  es.addEventListener("task-status", (e) => {
    renderStatus(JSON.parse(e.data));
  });

  es.addEventListener("logs-info", (e) => {
    const items = JSON.parse(e.data);
    state.infoLogs = state.infoLogs.concat(items).slice(-LOG_DISPLAY_LIMIT);
    $("infoLogs").textContent = state.infoLogs.map(formatLogLine).join("\n");
    syncErrorTabAlert();
  });

  es.addEventListener("logs-error", (e) => {
    const items = JSON.parse(e.data);
    state.errorLogs = state.errorLogs.concat(items).slice(-LOG_DISPLAY_LIMIT);
    $("errorLogs").textContent = state.errorLogs.map(formatLogLine).join("\n");
    syncErrorTabAlert();
  });

  es.addEventListener("stock-states", (e) => {
    renderStockStates(JSON.parse(e.data));
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
      option.textContent =
        `${item.task_id.slice(0, 8)} | ${item.strategy_name || "-"} | ${STATUS_TEXT[item.status] || item.status} | ` +
        `${item.processed_stocks}/${item.total_stocks} | 命中${item.result_count}`;
      select.appendChild(option);
    }

    const targetTaskId = selectTaskId || state.taskId || (data.items && data.items[0] ? data.items[0].task_id : null);
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
  const group = findGroup(groupId);

  let groupParams;
  try {
    const parsed = parseGroupParamsTextOrThrow(getGroupParamsText() || "{}");
    groupParams = stripCommentKeys(parsed);
  } catch (err) {
    alert(err.message);
    return;
  }

  const startTs = localDateTimeOrNull($("startTs").value);
  const endTs = localDateTimeOrNull($("endTs").value);
  if (group?.execution?.requires_time_window && (!startTs || !endTs)) {
    alert(`策略组 ${group.name || group.id} 要求开始时间和结束时间必填`);
    return;
  }

  const sampleSize = getCurrentSampleSize();
  const payload = {
    stocks: splitStocks($("stocksInput").value),
    start_ts: startTs,
    end_ts: endTs,
    source_db: $("sourceDb").value.trim() || null,
    run_mode: runMode,
    sample_size: sampleSize,
    strategy_group_id: groupId,
    group_params: groupParams,
  };

  const resp = await postJSON("/api/tasks", payload);
  await refreshTaskList(resp.task_id);
}

async function pauseOrResumeTask() {
  if (!state.taskId || !state.lastTask) return;
  if (state.pollInFlight) return;
  state.pollInFlight = true;
  const url = state.lastTask.status === "paused"
    ? `/api/tasks/${state.taskId}/resume`
    : `/api/tasks/${state.taskId}/pause`;
  try {
    await postJSON(url, {});
    const task = await getJSON(`/api/tasks/${state.taskId}`);
    renderStatus(task);
  } finally {
    state.pollInFlight = false;
  }
}

async function stopTask() {
  if (!state.taskId) return;
  if (state.pollInFlight) return;
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
  state.strategyGroups = (data.items || []).filter((group) => group && group.id !== "strategy_2");

  const select = $("strategyGroupSelect");
  select.innerHTML = "";
  for (const group of state.strategyGroups) {
    const option = document.createElement("option");
    option.value = group.id;
    option.textContent = `${group.name} (${group.id})`;
    select.appendChild(option);
  }

  if (!state.strategyGroups.length) {
    setGroupParamsText("{}");
    $("strategyGroupDesc").innerHTML = "未发现可用策略组";
    return;
  }

  const first = state.strategyGroups[0];
  select.value = first.id;
  fillGroupParamsFromDefault(first.id);
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
  $("taskSelect").addEventListener("change", (ev) => {
    state.taskId = ev.target.value;
    state.lastTask = null;
    resetTaskLogCursor();
    publishTaskSelection(state.taskId);
    startPolling();
  });
  document.querySelectorAll("[data-stock-sort]").forEach((button) => {
    button.addEventListener("click", () => {
      toggleStockStateSort(button.dataset.stockSort || "");
    });
  });
  const stockFilterBindings = [
    ["stockFilterCode", "code"],
    ["stockFilterName", "name"],
    ["stockFilterStatus", "status"],
    ["stockFilterProcessedBars", "processed_bars"],
    ["stockFilterSignalCount", "signal_count"],
    ["stockFilterLastDt", "last_dt"],
    ["stockFilterError", "error_message"],
  ];
  for (const [id, key] of stockFilterBindings) {
    const input = $(id);
    if (!input) continue;
    input.addEventListener("input", (ev) => setStockStateFilter(key, ev.target.value));
    input.addEventListener("change", (ev) => setStockStateFilter(key, ev.target.value));
  }
  $("strategyGroupSelect").addEventListener("change", (ev) => {
    fillGroupParamsFromDefault(ev.target.value);
    markSettingsDirty();
  });

  const dirtyInputIds = ["sourceDb", "stocksInput", "startTs", "endTs"];
  for (const id of dirtyInputIds) {
    const el = $(id);
    if (!el) continue;
    el.addEventListener("input", markSettingsDirty);
    el.addEventListener("change", markSettingsDirty);
  }

  if (!state.groupParamsEditor) {
    const groupParamsInput = $("groupParams");
    if (groupParamsInput) {
      groupParamsInput.addEventListener("input", markSettingsDirty);
      groupParamsInput.addEventListener("change", markSettingsDirty);
    }
  }
}

function bindCrossTabSync() {
  window.addEventListener("storage", (ev) => {
    if (ev.key !== SHARED_TASK_KEY || !ev.newValue) return;
    applySharedTaskSelection(ev.newValue).catch((err) => {
      console.error("跨标签任务同步失败(storage)", err);
    });
  });
  if (syncChannel) {
    syncChannel.addEventListener("message", (ev) => {
      applySharedTaskSelection(typeof ev.data === "string" ? ev.data : "").catch((err) => {
        console.error("跨标签任务同步失败(channel)", err);
      });
    });
    window.addEventListener("beforeunload", () => syncChannel.close(), { once: true });
  }
}

function bindLogAutoScroll() {
  const infoEl = $("infoLogs");
  const errorEl = $("errorLogs");
  if (infoEl) infoEl.addEventListener("scroll", () => { state.infoLogAutoScroll = isNearBottom(infoEl); });
  if (errorEl) errorEl.addEventListener("scroll", () => { state.errorLogAutoScroll = isNearBottom(errorEl); });
}

async function init() {
  initGroupParamsEditor();
  bindEvents();
  bindLogAutoScroll();
  bindCrossTabSync();
  startHeartbeatStream();
  await loadStrategyGroups();
  try {
    await loadMonitorSettings();
  } catch (err) {
    console.warn("加载监控页参数设置失败，将使用页面默认值", err);
    setDefaultTaskTimeRange(true);
    setSaveHint("参数修改后请点“保存参数设置”。", "hint");
  }
  const params = new URLSearchParams(window.location.search);
  const taskIdFromUrl = params.get("task_id");
  await refreshTaskList(taskIdFromUrl || null);
}

init().catch((err) => {
  console.error("页面初始化失败", err);
  alert(`页面初始化失败：${err.message}`);
});
