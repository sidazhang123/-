const state = {
  jobType: "maintenance",
  jobId: null,
  eventSource: null,
  heartbeatSource: null,
  pollInFlight: false,
  lastJob: null,
  logJobId: null,
  logJobType: null,
  lastInfoLogId: null,
  lastErrorLogId: null,
  infoLogs: [],
  errorLogs: [],
  infoLogAutoScroll: true,
  errorLogAutoScroll: true,
};

const $ = (id) => document.getElementById(id);

const JOB_STATUS_TEXT = {
  queued: "排队中",
  running: "运行中",
  stopping: "停止中",
  stopped: "已停止",
  completed: "已完成",
  failed: "失败",
};

const PHASE_TEXT = {
  prepare: "准备阶段",
  running: "执行中",
  done: "已完成",
  stopped: "已停止",
  failed: "失败",
};

const MODE_TEXT = {
  latest_update: "最新数据更新",
  historical_backfill: "历史数据维护",
};

const JOB_KIND_TEXT = {
  maintenance: "K线维护",
  concept: "概念更新",
};

const JOB_KIND_UI = {
  maintenance: {
    selectLabel: "历史维护任务",
    startHint: "可保存当前模式。",
    phaseLabel: "阶段说明",
    modeLabel: "模式",
    stepsLabel: "步骤进度",
    tasksLabel: "抓取进度",
    retryLabel: "重试轮次",
    rowsLabel: "写入统计",
    removedLabel: "异常删除",
    render(job) {
      const summary = job.summary || {};
      $("maintenanceModeText").textContent = formatMode(job.mode);
      $("maintenanceStepsText").textContent = `${Number(summary.steps_completed || 0)}/${Number(summary.steps_total || 0)}`;
      $("maintenanceTasksText").textContent = `总 ${Number(summary.total_tasks || 0)} | 成功 ${Number(summary.success_tasks || 0)} | 失败 ${Number(summary.failed_tasks || 0)}`;
      $("maintenanceRetryText").textContent = `轮次 ${Number(summary.retry_rounds_used || 0)} | 超限剔除 ${Number(summary.retry_skipped_tasks || 0)}`;
      $("maintenanceRowsText").textContent = formatRowsWritten(summary.rows_written);
      $("maintenanceRemovedRowsText").textContent = String(Number(summary.removed_corrupted_rows || 0));
    },
  },
  concept: {
    selectLabel: "历史概念任务",
    startHint: "概念任务不使用运行模式，直接全量刷新 stock_concepts。",
    phaseLabel: "阶段说明",
    modeLabel: "任务类型",
    stepsLabel: "处理进度",
    tasksLabel: "抓取统计",
    retryLabel: "过滤统计",
    rowsLabel: "写入统计",
    removedLabel: "耗时(秒)",
    render(job) {
      const summary = job.summary || {};
      $("maintenanceModeText").textContent = JOB_KIND_TEXT.concept;
      $("maintenanceStepsText").textContent = `${Number(summary.steps_completed || 0)}/${Number(summary.steps_total || 0)}`;
      $("maintenanceTasksText").textContent = `总 ${Number(summary.total_tasks || 0)} | 成功 ${Number(summary.success_tasks || 0)} | 失败 ${Number(summary.failed_tasks || 0)}`;
      $("maintenanceRetryText").textContent = `过滤 ${Number(summary.filtered_records || 0)} 条`;
      $("maintenanceRowsText").textContent = `写入 ${Number(summary.records_written || 0)} 条`;
      $("maintenanceRemovedRowsText").textContent = Number(summary.duration_seconds || 0).toFixed(2);
    },
  },
};

const LOG_FETCH_LIMIT = 300;
const LOG_KEEP_LIMIT = 150;
const LOG_AUTO_SCROLL_THRESHOLD = 24;

function deriveJobErrorText(job, jobType) {
  const explicitError = String(job?.error_message || "").trim();
  if (explicitError) return explicitError;

  const summary = job?.summary || {};
  const failedTasks = Number(summary.failed_tasks || 0);
  if (failedTasks <= 0) return "-";

  if (jobType === "concept") {
    return `本次概念抓取失败 ${failedTasks} 条，详见错误日志`;
  }

  return `本次维护失败 ${failedTasks} 条，详见错误日志`;
}

function currentRunMode() {
  const raw = $("maintenanceRunMode")?.value || "latest_update";
  return raw === "concept" ? "concept" : normalizeMode(raw);
}

function syncJobTypeFromRunMode() {
  state.jobType = currentRunMode() === "concept" ? "concept" : "maintenance";
}

function currentJobType() {
  return state.jobType || (currentRunMode() === "concept" ? "concept" : "maintenance");
}

function getJobApiBase(jobType = currentJobType()) {
  return jobType === "concept" ? "/api/concept/jobs" : "/api/maintenance/jobs";
}

function setStreamState(text, kind) {
  const el = $("maintenanceStreamState");
  if (!el) return;
  el.textContent = text;
  el.className = `stream-state ${kind}`;
}

function getMaintenanceErrorTabButton() {
  return document.querySelector('.log-tab.error-tab[data-log-target="maintErrorPane"]');
}

function selectedJobHasErrors() {
  if (!state.jobId) return false;
  return state.errorLogs.length > 0;
}

function syncMaintenanceErrorTabAlert() {
  const button = getMaintenanceErrorTabButton();
  if (!button) return;
  button.classList.toggle("has-error-alert", selectedJobHasErrors());
}

async function readErrorDetail(resp) {
  const text = await resp.text();
  const type = (resp.headers.get("content-type") || "").toLowerCase();
  if (type.includes("application/json")) {
    try {
      const payload = JSON.parse(text);
      if (typeof payload?.detail === "string") return payload.detail;
      if (payload?.detail !== undefined) return JSON.stringify(payload.detail);
    } catch {
      // ignore
    }
  }
  return text || "未知错误";
}

async function getJSON(url) {
  const resp = await fetch(url);
  if (!resp.ok) {
    throw new Error(await readErrorDetail(resp));
  }
  return resp.json();
}

async function postJSON(url, payload = {}) {
  const options = { method: "POST" };
  if (payload !== null) {
    options.headers = { "Content-Type": "application/json" };
    options.body = JSON.stringify(payload);
  }
  const resp = await fetch(url, options);
  if (!resp.ok) {
    throw new Error(await readErrorDetail(resp));
  }
  return resp.json();
}

function normalizeMode(raw) {
  const token = String(raw || "latest_update").trim().toLowerCase();
  if (token === "latest_update" || token === "historical_backfill") return token;
  return "latest_update";
}

function formatMode(raw) {
  const token = normalizeMode(raw);
  return MODE_TEXT[token] || token;
}

function isNearBottom(el) {
  return el.scrollHeight - (el.scrollTop + el.clientHeight) <= LOG_AUTO_SCROLL_THRESHOLD;
}

function syncLogBox(el, lines, autoScroll) {
  const nearBottom = isNearBottom(el);
  const prevTop = el.scrollTop;
  el.textContent = lines.join("\n");
  if (autoScroll || nearBottom) {
    el.scrollTop = el.scrollHeight;
    return;
  }
  const maxTop = Math.max(0, el.scrollHeight - el.clientHeight);
  el.scrollTop = Math.min(prevTop, maxTop);
}

function renderLogs() {
  const infoEl = $("maintenanceInfoLogs");
  const errorEl = $("maintenanceErrorLogs");
  const infoLines = state.infoLogs.slice(-LOG_KEEP_LIMIT).map((item) => {
    const ts = item.ts ? new Date(item.ts).toLocaleString() : "-";
    const detail = item.detail ? ` | ${JSON.stringify(item.detail)}` : "";
    return `${ts} [${item.level}] ${item.message}${detail}`;
  });
  const errorLines = state.errorLogs.slice(-LOG_KEEP_LIMIT).map((item) => {
    const ts = item.ts ? new Date(item.ts).toLocaleString() : "-";
    const detail = item.detail ? ` | ${JSON.stringify(item.detail)}` : "";
    return `${ts} [${item.level}] ${item.message}${detail}`;
  });
  syncLogBox(infoEl, infoLines, state.infoLogAutoScroll);
  syncLogBox(errorEl, errorLines, state.errorLogAutoScroll);
  syncMaintenanceErrorTabAlert();
}

function resetLogCache(jobId, jobType) {
  state.logJobId = jobId;
  state.logJobType = jobType;
  state.lastInfoLogId = null;
  state.lastErrorLogId = null;
  state.infoLogs = [];
  state.errorLogs = [];
  state.infoLogAutoScroll = true;
  state.errorLogAutoScroll = true;
  renderLogs();
}

async function fetchLogs(jobType, jobId, level, afterLogId) {
  const base = `${getJobApiBase(jobType)}/${jobId}/logs?level=${level}&limit=${LOG_FETCH_LIMIT}`;
  const url = afterLogId === null || afterLogId === undefined
    ? `${base}&offset=0`
    : `${base}&after_log_id=${Math.max(0, Number(afterLogId) || 0)}`;
  const data = await getJSON(url);
  const items = data.items || [];
  const rawNext = data.next_after_log_id;
  let nextAfter = rawNext === null || rawNext === undefined ? afterLogId : Number(rawNext);
  if (!Number.isFinite(nextAfter)) nextAfter = afterLogId;
  if (nextAfter === null || nextAfter === undefined) {
    nextAfter = items.length > 0 ? Number(items[items.length - 1].log_id || 0) : 0;
  }
  return { items, nextAfter };
}

async function loadJobLogs(jobType, jobId, reset = false) {
  if (reset || state.logJobId !== jobId || state.logJobType !== jobType) {
    resetLogCache(jobId, jobType);
  }
  const [infoRes, errorRes] = await Promise.all([
    fetchLogs(jobType, jobId, "info", state.lastInfoLogId),
    fetchLogs(jobType, jobId, "error", state.lastErrorLogId),
  ]);
  state.lastInfoLogId = infoRes.nextAfter;
  state.lastErrorLogId = errorRes.nextAfter;
  if (infoRes.items.length > 0) state.infoLogs = state.infoLogs.concat(infoRes.items).slice(-LOG_KEEP_LIMIT);
  if (errorRes.items.length > 0) state.errorLogs = state.errorLogs.concat(errorRes.items).slice(-LOG_KEEP_LIMIT);
  renderLogs();
}

function formatRowsWritten(rows) {
  if (!rows || typeof rows !== "object") return "-";
  return `15:${rows["15"] || 0} | 30:${rows["30"] || 0} | 60:${rows["60"] || 0} | d:${rows.d || 0} | w:${rows.w || 0}`;
}

function applyJobTypeUi(jobType) {
  const ui = JOB_KIND_UI[jobType] || JOB_KIND_UI.maintenance;
  $("maintenanceJobSelectLabel").textContent = ui.selectLabel;
  $("maintenancePhaseLabelTitle").textContent = ui.phaseLabel;
  $("maintenanceModeLabel").textContent = ui.modeLabel;
  $("maintenanceStepsLabel").textContent = ui.stepsLabel;
  $("maintenanceTasksLabel").textContent = ui.tasksLabel;
  $("maintenanceRetryLabel").textContent = ui.retryLabel;
  $("maintenanceRowsLabel").textContent = ui.rowsLabel;
  $("maintenanceRemovedRowsLabel").textContent = ui.removedLabel;
  if (!$("maintenanceSettingsHint").classList.contains("ok")) {
    $("maintenanceSettingsHint").textContent = jobType === "concept"
      ? "选择“概念更新”后点击启动任务即可。"
      : "选择运行模式后启动任务。";
  }
}

function renderCommonJobStatus(job, jobType) {
  state.lastJob = { ...job, jobType };
  $("maintenanceStatusText").textContent = JOB_STATUS_TEXT[job.status] || job.status || "-";
  $("maintenancePhaseText").textContent = job.phase || "-";
  $("maintenancePhaseLabelText").textContent = PHASE_TEXT[job.phase] || job.phase || "-";
  $("maintenanceProgressText").textContent = `${Number(job.progress || 0).toFixed(2)}%`;
  $("maintenanceProgressBar").style.width = `${Math.max(0, Math.min(100, Number(job.progress || 0)))}%`;
  $("maintenanceErrorText").textContent = deriveJobErrorText(job, jobType);

  const stopEnabled = ["queued", "running", "stopping"].includes(String(job.status || "").toLowerCase());
  $("stopMaintenanceBtn").disabled = !stopEnabled;

  const renderer = JOB_KIND_UI[jobType] || JOB_KIND_UI.maintenance;
  renderer.render(job);
  syncMaintenanceErrorTabAlert();
}

function renderJobStatus(job, jobType = currentJobType()) {
  applyJobTypeUi(jobType);
  renderCommonJobStatus(job, jobType);
}

function resetJobStatusCard(jobType = currentJobType()) {
  applyJobTypeUi(jobType);
  renderJobStatus(
    {
      status: "-",
      phase: null,
      progress: 0,
      error_message: null,
      mode: null,
      summary: {},
    },
    jobType,
  );
  $("stopMaintenanceBtn").disabled = true;
}

async function refreshMaintenanceJobs(selectJobId = null) {
  if (state.pollInFlight) return;
  state.pollInFlight = true;
  try {
    const jobType = currentJobType();
    const data = await getJSON(`${getJobApiBase(jobType)}?offset=0&limit=150`);
    const select = $("maintenanceJobSelect");
    select.innerHTML = "";
    for (const item of data.items || []) {
      const option = document.createElement("option");
      option.value = item.job_id;
      option.textContent = `${item.job_id.slice(0, 8)} | ${JOB_STATUS_TEXT[item.status] || item.status}`;
      select.appendChild(option);
    }

    const target = selectJobId || state.jobId || (data.items && data.items[0] ? data.items[0].job_id : null);
    if (!target) {
      state.jobId = null;
      state.lastJob = null;
      stopPolling();
      resetLogCache(null, jobType);
      resetJobStatusCard(jobType);
      startHeartbeatStream();
      return;
    }

    state.jobId = target;
    select.value = target;
    startPolling();
  } finally {
    state.pollInFlight = false;
  }
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
  if (!state.jobId) {
    startHeartbeatStream();
    return;
  }

  const jobType = currentJobType();
  stopHeartbeatStream();
  const es = new EventSource(`${getJobApiBase(jobType)}/${state.jobId}/stream`);
  state.eventSource = es;
  setStreamState("SSE: 连接中", "is-connecting");

  es.onopen = () => {
    setStreamState("SSE: 已连接", "is-connected");
  };

  es.onerror = () => {
    setStreamState("SSE: 重连中", "is-connecting");
  };

  es.addEventListener("connected", () => {
    resetLogCache(state.jobId, jobType);
  });

  es.addEventListener("job-status", (e) => {
    renderJobStatus(JSON.parse(e.data), jobType);
  });

  es.addEventListener("logs-info", (e) => {
    const items = JSON.parse(e.data);
    if (items.length > 0) {
      state.infoLogs = state.infoLogs.concat(items).slice(-LOG_KEEP_LIMIT);
    }
    renderLogs();
  });

  es.addEventListener("logs-error", (e) => {
    const items = JSON.parse(e.data);
    if (items.length > 0) {
      state.errorLogs = state.errorLogs.concat(items).slice(-LOG_KEEP_LIMIT);
    }
    renderLogs();
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

function currentSettingsPayload() {
  return {
    mode: currentRunMode() === "concept" ? "latest_update" : normalizeMode($("maintenanceRunMode").value),
    info_log_autoscroll: Boolean(state.infoLogAutoScroll),
    error_log_autoscroll: Boolean(state.errorLogAutoScroll),
  };
}

async function loadSettings() {
  const data = await getJSON("/api/ui-settings/maintenance");
  const settings = data.settings || {};
  $("maintenanceRunMode").value = normalizeMode(settings.mode || "latest_update");
  state.infoLogAutoScroll = settings.info_log_autoscroll !== false;
  state.errorLogAutoScroll = settings.error_log_autoscroll !== false;
}

async function startSelectedJob() {
  const runMode = currentRunMode();
  let data;
  if (runMode === "concept") {
    data = await postJSON("/api/concept/jobs", null);
    state.jobType = "concept";
  } else {
    data = await postJSON("/api/maintenance/jobs", { mode: normalizeMode(runMode) });
    state.jobType = "maintenance";
  }
  await refreshMaintenanceJobs(data.job_id);
}

async function stopCurrentJob() {
  if (!state.jobId) return;
  await postJSON(`${getJobApiBase()}/${state.jobId}/stop`, {});
}

function bindLogAutoScroll() {
  const infoEl = $("maintenanceInfoLogs");
  const errorEl = $("maintenanceErrorLogs");
  infoEl.addEventListener("scroll", () => {
    state.infoLogAutoScroll = isNearBottom(infoEl);
  });
  errorEl.addEventListener("scroll", () => {
    state.errorLogAutoScroll = isNearBottom(errorEl);
  });
}

function bindEvents() {
  $("maintenanceRunMode").addEventListener("change", async () => {
    syncJobTypeFromRunMode();
    state.jobId = null;
    state.lastJob = null;
    stopPolling();
    resetLogCache(null, currentJobType());
    resetJobStatusCard(currentJobType());
    await refreshMaintenanceJobs();
  });

  $("startMaintenanceBtn").addEventListener("click", () => {
    startSelectedJob().catch((err) => alert(`启动任务失败：${err.message}`));
  });
  $("refreshMaintenanceJobsBtn").addEventListener("click", () => {
    refreshMaintenanceJobs().catch((err) => alert(`刷新任务列表失败：${err.message}`));
  });
  $("stopMaintenanceBtn").addEventListener("click", () => {
    stopCurrentJob().catch((err) => alert(`停止任务失败：${err.message}`));
  });
  $("maintenanceJobSelect").addEventListener("change", (ev) => {
    state.jobId = ev.target.value;
    state.lastJob = null;
    resetLogCache(state.jobId, currentJobType());
    startPolling();
  });
}

async function loadServerStatus() {
  const container = $("serverStatusContent");
  try {
    const data = await getJSON("/api/maintenance/runtime-metadata");
    if (!data.ok) {
      container.innerHTML = `<span class="server-status-error">TDX 连接失败: ${data.error || "未知错误"}</span>`;
      return;
    }
    const meta = data.metadata || {};
    const items = [];
    const stdHost = meta.std_active_host || "";
    const exHost = meta.ex_active_host || "";
    items.push(renderHostItem("标准行情", stdHost));
    items.push(renderHostItem("扩展行情", exHost));

    let usedHosts = [];
    try { usedHosts = JSON.parse(meta.std_used_hosts || "[]"); } catch { /* ignore */ }
    for (const h of usedHosts) {
      if (h && h !== stdHost) items.push(renderHostItem("标准(历史)", h, true));
    }
    let exUsedHosts = [];
    try { exUsedHosts = JSON.parse(meta.ex_used_hosts || "[]"); } catch { /* ignore */ }
    for (const h of exUsedHosts) {
      if (h && h !== exHost) items.push(renderHostItem("扩展(历史)", h, true));
    }
    container.innerHTML = items.join("");
  } catch (err) {
    container.innerHTML = `<span class="server-status-error">获取服务器状态失败: ${err.message}</span>`;
  }
}

function renderHostItem(tag, host, inactive) {
  const dotClass = !host ? "gray" : inactive ? "gray" : "green";
  const label = host || "未连接";
  return `<span class="server-status-item"><span class="status-dot ${dotClass}"></span><span class="host-label">${label}</span><span class="host-tag">${tag}</span></span>`;
}

async function init() {
  bindEvents();
  bindLogAutoScroll();
  try {
    await loadSettings();
  } catch (err) {
    console.warn("加载维护设置失败，使用默认值", err);
  }
  syncJobTypeFromRunMode();
  applyJobTypeUi(currentJobType());
  resetJobStatusCard(currentJobType());
  startHeartbeatStream();
  loadServerStatus().catch((err) => console.warn("加载服务器状态失败", err));
  setInterval(() => loadServerStatus().catch((err) => console.warn("刷新服务器状态失败", err)), 10000);
  await refreshMaintenanceJobs();
}

init().catch((err) => {
  console.error("维护页面初始化失败", err);
  alert(`维护页面初始化失败：${err.message}`);
});