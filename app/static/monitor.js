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

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function helpTextFromNode(node) {
  if (!node) return "";
  if (typeof node === "string") return node;
  if (Array.isArray(node)) return node.map((item) => String(item)).join("\n");
  if (typeof node === "object") {
    const blocks = [];
    if (typeof node._comment === "string" && node._comment.trim()) {
      blocks.push(node._comment.trim());
    }
    if (Array.isArray(node._overview) && node._overview.length) {
      blocks.push(node._overview.map((item) => String(item)).join("\n"));
    }
    return blocks.join("\n\n");
  }
  return "";
}

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

function pathToString(path) {
  return path.join(".");
}

function getValueAtPath(root, path) {
  let current = root;
  for (const key of path) {
    if (!current || typeof current !== "object") return undefined;
    current = current[key];
  }
  return current;
}

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

// 前端显示名映射：key → 显示标签（null = 隐藏标签）
const _LABEL_MAP = {
  "concepts": "概念预筛选",
  "concept_terms": "概念名称",
  "reason_terms": "入选理由",
};
// 需要在 UI 级别展平（不渲染 section 外壳，直接渲染子节点）的 key 集合
const _UNWRAP_SECTIONS = new Set(["universe_filters"]);
// 当 path 匹配时隐藏标签与帮助文本的 key（仅保留控件本身）
const _HIDE_LABEL_KEYS = new Set(["universe_filters.concepts.enabled"]);

function renderParamField(path, value, helpNode, labelText, depth = 0) {
  const pathKey = pathToString(path);
  // 应用显示名映射
  if (labelText in _LABEL_MAP) labelText = _LABEL_MAP[labelText];
  const hideLabelAndHelp = _HIDE_LABEL_KEYS.has(pathKey);
  const helpText = hideLabelAndHelp ? "" : helpTextFromNode(helpNode);

  if (Array.isArray(value)) {
    const isStringArray = value.every((item) => typeof item === "string");
    if (!isStringArray) {
      return `
        <div class="param-field unsupported">
          <div class="param-label-row">
            <div class="param-label">${escapeHtml(labelText)}</div>
          </div>
          ${helpText ? `<div class="param-help">${escapeHtml(helpText)}</div>` : ""}
          <div class="param-readonly">暂不支持的数组类型</div>
        </div>
      `;
    }
    return `
      <div class="param-field">
        <div class="param-label-row">
          <div class="param-label">${escapeHtml(labelText)}</div>
        </div>
        ${helpText ? `<div class="param-help">${escapeHtml(helpText)}</div>` : ""}
        <div class="param-tags-editor" data-param-path="${escapeHtml(pathKey)}"></div>
      </div>
    `;
  }

  if (value && typeof value === "object") {
    // 内嵌模板渲染：当 param_help 声明 _render: "inline_template" 时使用专用渲染器
    if (helpNode && helpNode._render === "inline_template") {
      return renderInlineTemplateSection(path, value, helpNode);
    }
    // 展平 section：不渲染外壳，直接输出子节点
    const lastKey = path.length ? path[path.length - 1] : "";
    if (_UNWRAP_SECTIONS.has(lastKey)) {
      const childKeys = Object.keys(value);
      return childKeys.map((key) => renderParamField(path.concat(key), value[key], helpNode?.[key], key, depth)).join("");
    }
    const childKeys = Object.keys(value);
    const titleClass = depth === 0 ? "param-section-title root" : "param-section-title";
    return `
      <section class="param-section ${depth === 0 ? "is-root" : ""}" data-param-section="${escapeHtml(pathKey)}">
        ${labelText ? `<div class="${titleClass}">${escapeHtml(labelText)}</div>` : ""}
        ${helpText ? `<div class="param-help">${escapeHtml(helpText)}</div>` : ""}
        <div class="param-section-body">
          ${childKeys.map((key) => renderParamField(path.concat(key), value[key], helpNode?.[key], key, depth + 1)).join("")}
        </div>
      </section>
    `;
  }

  if (typeof value === "boolean") {
    return `
      <div class="param-field checkbox-field">
        ${hideLabelAndHelp ? "" : `<span class="param-label">${escapeHtml(labelText)}</span>`}
        ${helpText ? `<span class="param-help">${escapeHtml(helpText)}</span>` : ""}
        <span class="param-checkbox-row">
          <input type="checkbox" data-param-path="${escapeHtml(pathKey)}" ${value ? "checked" : ""} />
          <span class="param-checkbox-value">${value ? "开启" : "关闭"}</span>
        </span>
      </div>
    `;
  }

  if (typeof value === "number") {
    const step = Number.isInteger(value) ? "1" : "any";
    return `
      <label class="param-field">
        <span class="param-label">${escapeHtml(labelText)}</span>
        ${helpText ? `<span class="param-help">${escapeHtml(helpText)}</span>` : ""}
        <input
          type="number"
          data-param-path="${escapeHtml(pathKey)}"
          data-param-number-kind="${Number.isInteger(value) ? "int" : "float"}"
          step="${step}"
          value="${escapeHtml(String(value))}"
        />
      </label>
    `;
  }

  return `
    <label class="param-field">
      <span class="param-label">${escapeHtml(labelText)}</span>
      ${helpText ? `<span class="param-help">${escapeHtml(helpText)}</span>` : ""}
      <input type="text" data-param-path="${escapeHtml(pathKey)}" value="${escapeHtml(String(value ?? ""))}" />
    </label>
  `;
}

function renderInlineTemplateSection(path, sectionValue, helpNode) {
  const sectionKey = pathToString(path);
  const label = helpNode._label || sectionKey;
  const templates = Array.isArray(helpNode._templates) ? helpNode._templates : [];
  const enabledVal = sectionValue.enabled;
  const enabledPath = path.concat("enabled");
  const enabledKey = pathToString(enabledPath);
  const isEnabled = enabledVal === true;

  const headerHtml = `
    <div class="param-inline-section-header">
      <span class="param-inline-section-title">${escapeHtml(label)}</span>
      <span class="param-checkbox-row" style="cursor:default">
        <input type="checkbox" data-param-path="${escapeHtml(enabledKey)}" ${isEnabled ? "checked" : ""} style="cursor:pointer" />
        <span class="param-checkbox-value">${isEnabled ? "开启" : "关闭"}</span>
      </span>
    </div>
  `;

  let rowsHtml = "";
  for (const tpl of templates) {
    const text = tpl.text || "";
    const isRequired = tpl.required === true;
    const toggleField = tpl.toggle || null;
    const helpTip = tpl.help || "";

    // 解析 {field_name} 占位符，替换为内联 input
    const segments = text.split(/\{(\w+)\}/g);
    let lineHtml = "";
    for (let i = 0; i < segments.length; i += 1) {
      if (i % 2 === 0) {
        if (segments[i]) {
          lineHtml += `<span class="param-inline-text">${escapeHtml(segments[i])}</span>`;
        }
      } else {
        const fieldName = segments[i];
        const fieldPath = path.concat(fieldName);
        const fieldKey = pathToString(fieldPath);
        const fieldVal = sectionValue[fieldName];
        if (typeof fieldVal === "number") {
          const step = Number.isInteger(fieldVal) ? "1" : "any";
          const kind = Number.isInteger(fieldVal) ? "int" : "float";
          lineHtml += `<input type="number" class="param-inline-field"
            data-param-path="${escapeHtml(fieldKey)}"
            data-param-number-kind="${kind}"
            step="${step}"
            value="${escapeHtml(String(fieldVal))}" />`;
        } else {
          lineHtml += `<input type="text" class="param-inline-field"
            data-param-path="${escapeHtml(fieldKey)}"
            value="${escapeHtml(String(fieldVal ?? ""))}" />`;
        }
      }
    }

    const badge = isRequired
      ? '<span class="param-inline-badge required">必选</span>'
      : '<span class="param-inline-badge optional">可选</span>';

    if (toggleField) {
      // 可选条件行：带行首勾选框
      const togglePath = path.concat(toggleField);
      const toggleKey = pathToString(togglePath);
      const toggleVal = sectionValue[toggleField] === true;
      const disabledClass = toggleVal ? "" : "param-inline-toggle-disabled";
      rowsHtml += `
        <div class="param-inline-toggle-row" data-inline-toggle="${escapeHtml(toggleKey)}">
          <input type="checkbox" data-param-path="${escapeHtml(toggleKey)}" ${toggleVal ? "checked" : ""} />
          <div class="param-inline-toggle-content ${disabledClass}" data-inline-toggle-body="${escapeHtml(toggleKey)}">
            ${badge} ${lineHtml}
            ${helpTip ? `<span class="param-inline-help">${escapeHtml(helpTip)}</span>` : ""}
          </div>
        </div>
      `;
    } else {
      // 必选/无 toggle 行
      rowsHtml += `<div class="param-inline-row">${badge} ${lineHtml}</div>`;
      if (helpTip) {
        rowsHtml += `<div class="param-inline-help">${escapeHtml(helpTip)}</div>`;
      }
    }
  }

  const bodyDisabledClass = isEnabled ? "" : "param-section-disabled";
  return `
    <section class="param-inline-section" data-param-section="${escapeHtml(sectionKey)}">
      ${headerHtml}
      <div class="param-inline-section-body ${bodyDisabledClass}" data-section-body="${escapeHtml(sectionKey)}">
        ${rowsHtml}
      </div>
    </section>
  `;
}

function refreshSectionDisableStates(root) {
  if (!root) return;
  // 处理 param-inline-section 的 enabled 开关（内嵌模板 section）
  root.querySelectorAll("[data-section-body]").forEach((body) => {
    const sectionKey = body.dataset.sectionBody;
    const enabledPath = sectionKey.split(".").concat("enabled").filter(Boolean);
    const enabled = getValueAtPath(state.groupParams, enabledPath);
    body.classList.toggle("param-section-disabled", enabled !== true);
  });
  // 处理标准 param-section 的 enabled 开关（如 universe_filters.concepts）
  root.querySelectorAll("[data-param-section]").forEach((section) => {
    const sectionKey = section.dataset.paramSection;
    const sectionPath = sectionKey.split(".").filter(Boolean);
    const sectionVal = getValueAtPath(state.groupParams, sectionPath);
    if (!sectionVal || typeof sectionVal !== "object" || !("enabled" in sectionVal)) return;
    // 跳过内嵌模板 section（已在上面处理）
    if (section.classList.contains("param-inline-section")) return;
    const body = section.querySelector(".param-section-body");
    if (!body) return;
    // 灰化 section-body 内除 enabled checkbox 之外的所有 field
    const enabledKey = pathToString(sectionPath.concat("enabled"));
    body.querySelectorAll(".param-field, .param-tags-editor").forEach((field) => {
      const input = field.querySelector(`[data-param-path]`);
      const tagEditor = field.closest(".param-tags-editor") || field.querySelector(".param-tags-editor");
      if (input && input.dataset.paramPath === enabledKey) return;
      if (tagEditor && tagEditor.dataset.paramPath === enabledKey) return;
      field.classList.toggle("param-section-disabled", sectionVal.enabled !== true);
    });
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
  state.groupParams = cloneValue(group.default_params || {});
  updateStrategyGroupDescription(groupId);
  renderGroupParamsForm();
}

function collectMonitorSettings() {
  return {
    source_db: $("sourceDb")?.value || "",
    stocks_input: $("stocksInput")?.value || "",
    sample_size: getCurrentSampleSize(),
    strategy_group_id: selectedGroupId(),
    group_params: cloneValue(state.groupParams),
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

  const savedGroupId = typeof settings.strategy_group_id === "string" ? settings.strategy_group_id : "";
  const savedGroup = savedGroupId ? findGroup(savedGroupId) : null;
  if (savedGroup) {
    $("strategyGroupSelect").value = savedGroupId;
    updateStrategyGroupDescription(savedGroupId);
    state.groupParams = cloneValue(
      settings.group_params && typeof settings.group_params === "object"
        ? settings.group_params
        : (savedGroup.default_params || {})
    );
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
      option.textContent =
        `${item.task_id.slice(0, 8)} | ${item.strategy_name || "-"} | ${STATUS_TEXT[item.status] || item.status} | ` +
        `${item.processed_stocks}/${item.total_stocks} | 命中${item.result_count}`;
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
    $("groupParamsForm").innerHTML = '<div class="param-panel-empty">未发现可用策略组</div>';
    $("strategyGroupDesc").innerHTML = "未发现可用策略组";
    return;
  }

  const first = state.strategyGroups[0];
  select.value = first.id;
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
    resetGroupParamsFromDefault(event.target.value);
    markSettingsDirty();
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
