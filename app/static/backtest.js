/**
 * 策略回测主页逻辑。
 *
 * 功能:
 * 1. 策略下拉框加载 + 参数渲染 (复用 ParamRender)
 * 2. 固定参数回测 (mode=fixed) 和 动态扫描回测 (mode=sweep)
 * 3. SSE 实时进度/日志推送
 * 4. ECharts 统计图表 (直方图 + 正态拟合, 箱线图)
 * 5. sweep 结果表格 (可排序)
 */

/* global ParamRender, echarts */

(function () {
  "use strict";

  // ── helpers ──
  const $ = (id) => document.getElementById(id);
  function cloneValue(v) { return JSON.parse(JSON.stringify(v)); }

  async function getJSON(url) {
    const r = await fetch(url);
    if (!r.ok) {
      const detail = await r.text().catch(() => r.statusText);
      throw new Error(detail);
    }
    return r.json();
  }
  async function postJSON(url, body) {
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const detail = await r.text().catch(() => r.statusText);
      throw new Error(detail);
    }
    return r.json();
  }

  /** 在 obj 中按 path 设值 */
  function setValueAtPath(obj, path, val) {
    let cur = obj;
    for (let i = 0; i < path.length - 1; i++) {
      const key = path[i];
      if (cur[key] === undefined || cur[key] === null) cur[key] = {};
      cur = cur[key];
    }
    cur[path[path.length - 1]] = val;
  }
  function getValueAtPath(obj, path) {
    let cur = obj;
    for (const k of path) {
      if (cur == null) return undefined;
      cur = cur[k];
    }
    return cur;
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

  // ── state ──
  const state = {
    strategyGroups: [],
    groupParams: {},
    monitorSettings: null, // cached monitor page settings
    sweepRanges: {},       // { "path.to.param": { min, max, step } }
    strategySettings: {},  // { groupId: { group_params, sweep_ranges } }
    logCursor: { info: 0, error: 0 },
    currentJobId: null,
    currentMode: null,
    sse: null,
    charts: [],            // ECharts instances for resize
  };

  const { renderParamField } = ParamRender;
  const LOG_KEEP = 200;

  // ── init ──
  async function init() {
    bindEvents();
    await loadMonitorParams();
    await loadStrategyGroups();
    await loadBacktestSettings();
    await tryReconnectJob();
  }

  /** 从监控页持久化设置中读取已保存的 group_params，回测页启动时与 default_params 合并 */
  async function loadMonitorParams() {
    try {
      const data = await getJSON("/api/ui-settings/monitor");
      state.monitorSettings = data.settings || null;
    } catch { state.monitorSettings = null; }
  }

  function bindEvents() {
    $("btStrategySelect").addEventListener("change", onStrategyChange);
    $("btRunFixedBtn").addEventListener("click", () => runBacktest("fixed"));
    $("btRunSweepBtn").addEventListener("click", () => runBacktest("sweep"));
    $("btStopBtn").addEventListener("click", stopBacktest);
    $("btStopSweepBtn").addEventListener("click", stopBacktest);
    $("btViewDetailBtn").addEventListener("click", () => {
      if (state.currentJobId) {
        window.open(`/backtest-detail?job_id=${state.currentJobId}`, "_blank");
      }
    });

    // 保存参数按钮
    $("btSaveParamsBtn").addEventListener("click", async () => {
      try {
        await saveBacktestSettings();
        $("btSaveParamsBtn").textContent = "✓ 已保存";
        setTimeout(() => { $("btSaveParamsBtn").textContent = "保存参数"; }, 1500);
      } catch (err) {
        alert(`保存参数失败: ${err.message}`);
      }
    });
    $("btSaveParamsSweepBtn").addEventListener("click", async () => {
      try {
        await saveBacktestSettings();
        $("btSaveParamsSweepBtn").textContent = "✓ 已保存";
        setTimeout(() => { $("btSaveParamsSweepBtn").textContent = "保存参数"; }, 1500);
      } catch (err) {
        alert(`保存参数失败: ${err.message}`);
      }
    });

    // 任务列表
    $("btRefreshJobsBtn").addEventListener("click", () => loadBacktestJobs());
    $("btJobSelect").addEventListener("change", onJobSelectChange);

    // Tab switching
    document.querySelectorAll(".bt-tab").forEach((tab) => {
      tab.addEventListener("click", () => {
        document.querySelectorAll(".bt-tab").forEach((t) => t.classList.remove("active"));
        document.querySelectorAll(".bt-tab-pane").forEach((p) => p.classList.remove("active"));
        tab.classList.add("active");
        $(tab.dataset.btTab).classList.add("active");
      });
    });

    // Forward bars / step change → save settings
    ["btFwdX", "btFwdY", "btFwdZ", "btSlideStep"].forEach((id) => {
      $(id).addEventListener("change", saveBacktestSettings);
    });
  }

  // ── strategies ──
  async function loadStrategyGroups() {
    const data = await getJSON("/api/strategy-groups");
    state.strategyGroups = (data.items || []).filter(
      (g) => g && g.id !== "strategy_2"
    );
    const sel = $("btStrategySelect");
    sel.innerHTML = "";
    for (const g of state.strategyGroups) {
      const opt = document.createElement("option");
      opt.value = g.id;
      opt.textContent = `${g.name} (${g.id})`;
      sel.appendChild(opt);
    }
    if (state.strategyGroups.length) {
      sel.value = state.strategyGroups[0].id;
      applyGroupParams(state.strategyGroups[0].id);
    }
  }

  function findGroup(id) {
    return state.strategyGroups.find((g) => g.id === id);
  }

  function selectedGroupId() {
    return $("btStrategySelect").value;
  }

  function onStrategyChange() {
    applyGroupParams(selectedGroupId());
  }

  function getSavedStrategySettings(groupId) {
    const all = state.strategySettings;
    if (!all || typeof all !== "object") return null;
    const item = all[groupId];
    return item && typeof item === "object" ? item : null;
  }

  function applyGroupParams(groupId) {
    const group = findGroup(groupId);
    if (!group) return;
    const base = cloneValue(group.default_params || {});
    // 如果监控页对同一策略组保存了自定义参数，合并到 default_params 上
    const ms = state.monitorSettings;
    if (ms && ms.strategy_group_id === groupId && ms.group_params && typeof ms.group_params === "object") {
      deepMerge(base, ms.group_params);
    }
    const saved = getSavedStrategySettings(groupId);
    if (saved && saved.group_params && typeof saved.group_params === "object") {
      deepMerge(base, saved.group_params);
    }
    state.groupParams = base;
    renderFixedParams(group);
    renderSweepParams(group);
    if (saved && saved.sweep_ranges && typeof saved.sweep_ranges === "object") {
      applySavedSweepRanges(saved.sweep_ranges);
    }
  }

  // ── param rendering ──
  function renderFixedParams(group) {
    const container = $("btParamsFixed");
    container.innerHTML = renderParamField(
      [], state.groupParams, group.param_help || {}, "", 0, { readonly: true }
    );
  }

  function renderSweepParams(group) {
    const container = $("btParamsSweep");
    container.innerHTML = renderParamField(
      [], state.groupParams, group.param_help || {}, "", 0
    );
    state.sweepRanges = {};
    injectRangeControls(container);
    bindParamInputs(container);
    updateComboCount();
  }

  function collectAllSweepRanges() {
    const ranges = {};
    document.querySelectorAll(".bt-rng-min[data-range-path]").forEach((minEl) => {
      const path = minEl.dataset.rangePath;
      const maxEl = document.querySelector(`.bt-rng-max[data-range-path="${path}"]`);
      const stepEl = document.querySelector(`.bt-rng-step[data-range-path="${path}"]`);
      if (!maxEl || !stepEl) return;

      const min = parseFloat(minEl.value);
      const max = parseFloat(maxEl.value);
      const step = parseFloat(stepEl.value);
      if (Number.isNaN(min) || Number.isNaN(max) || Number.isNaN(step) || step <= 0) return;

      ranges[path] = { min, max, step };
    });
    return ranges;
  }

  function applySavedSweepRanges(savedRanges) {
    state.sweepRanges = {};
    if (!savedRanges || typeof savedRanges !== "object") {
      updateComboCount();
      return;
    }

    for (const [path, range] of Object.entries(savedRanges)) {
      const minEl = document.querySelector(`.bt-rng-min[data-range-path="${path}"]`);
      const maxEl = document.querySelector(`.bt-rng-max[data-range-path="${path}"]`);
      const stepEl = document.querySelector(`.bt-rng-step[data-range-path="${path}"]`);
      if (!minEl || !maxEl || !stepEl) continue;

      if (range && range.min !== undefined) minEl.value = range.min;
      if (range && range.max !== undefined) maxEl.value = range.max;
      if (range && range.step !== undefined) stepEl.value = range.step;
      updateSweepRange(path);
    }

    updateComboCount();
  }

  /** 为每个数值 input 添加 min/max/step 范围控件 */
  function injectRangeControls(container) {
    container.querySelectorAll("input[type='number'][data-param-path]").forEach((inp) => {
      const path = inp.dataset.paramPath;
      const curVal = parseFloat(inp.value) || 0;
      const rangeDiv = document.createElement("span");
      rangeDiv.className = "bt-range-row";
      rangeDiv.innerHTML =
        `<label>min<input type="number" class="bt-rng-min" data-range-path="${path}" value="${curVal}" step="any"/></label>` +
        `<label>max<input type="number" class="bt-rng-max" data-range-path="${path}" value="${curVal}" step="any"/></label>` +
        `<label>step<input type="number" class="bt-rng-step" data-range-path="${path}" value="1" min="0.001" step="any"/></label>`;
      inp.insertAdjacentElement("afterend", rangeDiv);

      // Listen for range changes
      rangeDiv.querySelectorAll("input").forEach((ri) => {
        ri.addEventListener("change", () => {
          updateSweepRange(path);
          updateComboCount();
        });
      });
    });
  }

  function updateSweepRange(path) {
    const minEl = document.querySelector(`.bt-rng-min[data-range-path="${path}"]`);
    const maxEl = document.querySelector(`.bt-rng-max[data-range-path="${path}"]`);
    const stepEl = document.querySelector(`.bt-rng-step[data-range-path="${path}"]`);
    if (!minEl || !maxEl || !stepEl) return;
    const mn = parseFloat(minEl.value);
    const mx = parseFloat(maxEl.value);
    const st = parseFloat(stepEl.value);
    if (isNaN(mn) || isNaN(mx) || isNaN(st) || st <= 0) return;
    if (mn === mx) {
      delete state.sweepRanges[path];
    } else {
      state.sweepRanges[path] = { min: mn, max: mx, step: st };
    }
  }

  function updateComboCount() {
    const ranges = Object.values(state.sweepRanges);
    if (!ranges.length) {
      $("btComboCount").textContent = "";
      return;
    }
    let total = 1;
    for (const r of ranges) {
      const count = Math.floor((r.max - r.min) / r.step) + 1;
      total *= Math.max(1, count);
    }
    $("btComboCount").textContent = `共 ${total} 种参数组合`;
  }

  function bindParamInputs(container) {
    container.addEventListener("input", (e) => {
      const inp = e.target.closest("[data-param-path]");
      if (!inp || inp.classList.contains("bt-rng-min") ||
          inp.classList.contains("bt-rng-max") || inp.classList.contains("bt-rng-step")) return;
      const path = inp.dataset.paramPath.split(".");
      let val = inp.value;
      if (inp.type === "number") val = parseFloat(val);
      else if (inp.type === "checkbox") val = inp.checked;
      setValueAtPath(state.groupParams, path, val);
    });
  }

  // ── settings persistence ──
  async function loadBacktestSettings() {
    try {
      const data = await getJSON("/api/ui-settings/backtest");
      const s = data.settings;
      if (!s) return;
      state.strategySettings = (s.strategy_settings && typeof s.strategy_settings === "object")
        ? cloneValue(s.strategy_settings)
        : {};

      if (
        s.strategy_group_id &&
        s.group_params &&
        typeof s.group_params === "object" &&
        !state.strategySettings[s.strategy_group_id]
      ) {
        state.strategySettings[s.strategy_group_id] = {
          group_params: cloneValue(s.group_params),
          sweep_ranges: cloneValue(s.sweep_ranges || {}),
        };
      }

      if (s.forward_bars && s.forward_bars.length === 3) {
        $("btFwdX").value = s.forward_bars[0];
        $("btFwdY").value = s.forward_bars[1];
        $("btFwdZ").value = s.forward_bars[2];
      }
      if (s.slide_step) $("btSlideStep").value = s.slide_step;
      if (s.strategy_group_id) {
        const sel = $("btStrategySelect");
        if ([...sel.options].some((o) => o.value === s.strategy_group_id)) {
          sel.value = s.strategy_group_id;
          applyGroupParams(s.strategy_group_id);
        }
      }
      const currentSaved = getSavedStrategySettings(selectedGroupId());
      if (currentSaved && currentSaved.group_params && Object.keys(currentSaved.group_params).length) {
        state.groupParams = deepMerge(state.groupParams, currentSaved.group_params);
        syncGroupParamsToUI();
      }
      if (currentSaved && currentSaved.sweep_ranges && Object.keys(currentSaved.sweep_ranges).length) {
        applySavedSweepRanges(currentSaved.sweep_ranges);
      }
    } catch { /* ignore */ }
  }

  async function saveBacktestSettings() {
    const groupId = selectedGroupId();
    const strategySettings = cloneValue(state.strategySettings || {});
    strategySettings[groupId] = {
      group_params: cloneValue(state.groupParams),
      sweep_ranges: collectAllSweepRanges(),
    };

    const payload = {
      forward_bars: [
        parseInt($("btFwdX").value) || 2,
        parseInt($("btFwdY").value) || 5,
        parseInt($("btFwdZ").value) || 7,
      ],
      slide_step: parseInt($("btSlideStep").value) || 1,
      strategy_group_id: groupId,
      group_params: cloneValue(state.groupParams),
      sweep_ranges: cloneValue(strategySettings[groupId].sweep_ranges),
      strategy_settings: strategySettings,
    };
    return postJSON("/api/ui-settings/backtest", payload).then((resp) => {
      state.strategySettings = cloneValue(strategySettings);
      return resp;
    });
  }

  /** 将 state.groupParams 同步到 UI 输入控件 */
  function syncGroupParamsToUI() {
    document.querySelectorAll("[data-param-path]").forEach((inp) => {
      if (inp.classList.contains("bt-rng-min") ||
          inp.classList.contains("bt-rng-max") ||
          inp.classList.contains("bt-rng-step")) return;
      const path = inp.dataset.paramPath.split(".");
      const val = getValueAtPath(state.groupParams, path);
      if (val === undefined) return;
      if (inp.type === "checkbox") inp.checked = !!val;
      else inp.value = val;
    });
  }

  /** 按嵌套路径读取值 */
  function getValueAtPath(obj, pathArr) {
    let cur = obj;
    for (const k of pathArr) {
      if (cur == null || typeof cur !== "object") return undefined;
      cur = cur[k];
    }
    return cur;
  }

  /** 加载回测任务列表并填充下拉框，返回 { jobs, running_job_id } */
  async function loadBacktestJobs(selectJobId = null) {
    try {
      const data = await getJSON("/api/backtests");
      const sel = $("btJobSelect");
      sel.innerHTML = "";

      const jobs = data.jobs || [];
      if (!jobs.length) {
        const opt = document.createElement("option");
        opt.value = "";
        opt.textContent = "— 无任务 —";
        sel.appendChild(opt);
        return data;
      }

      const STATUS_TEXT = { queued: "排队中", running: "运行中", completed: "已完成", failed: "失败", stopped: "已停止", stopping: "失败" };
      for (const j of jobs) {
        const opt = document.createElement("option");
        opt.value = j.job_id;
        const d = j.created_at ? new Date(j.created_at) : null;
        const ts = d ? `${String(d.getMonth()+1).padStart(2,"0")}/${String(d.getDate()).padStart(2,"0")} ${String(d.getHours()).padStart(2,"0")}:${String(d.getMinutes()).padStart(2,"0")}` : "";
        opt.textContent = `${j.job_id.slice(0,8)} | ${j.strategy_group_id || ""} | ${STATUS_TEXT[j.status] || j.status} | ${ts}`;
        sel.appendChild(opt);
      }

      // 选中指定任务或运行中任务或第一条
      const targetId = selectJobId || data.running_job_id || (jobs.length ? jobs[0].job_id : "");
      if (targetId) sel.value = targetId;

      return data;
    } catch {
      return { jobs: [], running_job_id: null };
    }
  }

  /** 任务下拉框切换 */
  async function onJobSelectChange() {
    const jobId = $("btJobSelect").value;
    if (!jobId) {
      hidePanel("btStatusCard");
      hidePanel("btLogPanel");
      hidePanel("btStatsPanel");
      hidePanel("btSweepPanel");
      if (state.sse) { state.sse.close(); state.sse = null; }
      state.currentJobId = null;
      return;
    }

    if (state.sse) { state.sse.close(); state.sse = null; }
    state.currentJobId = jobId;
    clearLogs();
    showPanel("btStatusCard");
    showPanel("btLogPanel");
    hidePanel("btStatsPanel");
    hidePanel("btSweepPanel");

    try {
      const status = await getJSON(`/api/backtests/${jobId}/status`);
      updateStatus(status);
      state.currentMode = status.mode || "fixed";

      // 加载已有日志
      const [infoResp, errorResp] = await Promise.all([
        getJSON(`/api/backtests/${jobId}/logs?level=info`),
        getJSON(`/api/backtests/${jobId}/logs?level=error`),
      ]);
      if (infoResp.items && infoResp.items.length) appendLogs("btInfoLogs", infoResp.items);
      if (errorResp.items && errorResp.items.length) appendLogs("btErrorLogs", errorResp.items);

      if (["queued", "running"].includes(status.status)) {
        setButtonsRunning(true);
        connectSSE(jobId);
      } else {
        setButtonsRunning(false);
        if (status.status === "completed") {
          await onBacktestCompleted();
        }
      }
    } catch { /* ignore */ }
  }

  /** 页面加载时恢复任务列表并自动选中运行中/最近完成的任务 */
  async function tryReconnectJob() {
    const data = await loadBacktestJobs();
    if (!data) return;

    const sel = $("btJobSelect");
    const selectedId = sel.value;
    if (!selectedId) return;

    // 触发一次选择切换逻辑
    await onJobSelectChange();
  }

  // ── run backtest ──
  async function runBacktest(mode) {
    const groupId = selectedGroupId();
    if (!groupId) { alert("请选择策略组"); return; }

    const fwdBars = [
      parseInt($("btFwdX").value) || 2,
      parseInt($("btFwdY").value) || 5,
      parseInt($("btFwdZ").value) || 7,
    ];
    const slideStep = parseInt($("btSlideStep").value) || 1;

    const payload = {
      strategy_group_id: groupId,
      mode: mode,
      forward_bars: fwdBars,
      slide_step: slideStep,
      group_params: cloneValue(state.groupParams),
      param_ranges: {},
    };

    if (mode === "sweep") {
      for (const [pathStr, range] of Object.entries(state.sweepRanges)) {
        payload.param_ranges[pathStr] = range;
      }
      if (!Object.keys(payload.param_ranges).length) {
        alert("请至少设置一个参数的 min/max 范围 (min ≠ max)");
        return;
      }
    }

    try {
      setButtonsRunning(true);
      showPanel("btStatusCard");
      showPanel("btLogPanel");
      hidePanel("btStatsPanel");
      hidePanel("btSweepPanel");
      clearLogs();

      const resp = await postJSON("/api/backtests", payload);
      state.currentJobId = resp.job_id;
      state.currentMode = mode;
      await loadBacktestJobs(resp.job_id);
      connectSSE(resp.job_id);
      await saveBacktestSettings();
    } catch (err) {
      alert("创建回测失败: " + err.message);
      setButtonsRunning(false);
    }
  }

  async function stopBacktest() {
    if (!state.currentJobId) return;
    try {
      await postJSON(`/api/backtests/${state.currentJobId}/stop`, {});
      setButtonsRunning(false);
      await loadBacktestJobs(state.currentJobId);
    } catch (err) {
      alert("停止失败: " + err.message);
    }
  }

  // ── SSE ──
  function connectSSE(jobId) {
    if (state.sse) { state.sse.close(); state.sse = null; }

    const params = new URLSearchParams({
      info_after_log_id: String(state.logCursor.info || 0),
      error_after_log_id: String(state.logCursor.error || 0),
    });
    const es = new EventSource(`/api/backtests/${jobId}/stream?${params.toString()}`);
    state.sse = es;

    es.addEventListener("job-status", (e) => {
      const d = JSON.parse(e.data);
      updateStatus(d);
    });

    es.addEventListener("logs-info", (e) => {
      appendLogs("btInfoLogs", JSON.parse(e.data));
    });

    es.addEventListener("logs-error", (e) => {
      appendLogs("btErrorLogs", JSON.parse(e.data));
      // Pulse error tab
      const errTab = document.querySelector(".error-tab");
      if (errTab) errTab.classList.add("has-errors");
    });

    es.addEventListener("done", (e) => {
      const d = JSON.parse(e.data);
      es.close();
      state.sse = null;
      setButtonsRunning(false);
      loadBacktestJobs(state.currentJobId);  // 刷新列表状态
      if (d.status === "completed") {
        onBacktestCompleted();
      }
    });

    es.addEventListener("stream-error", (e) => {
      const d = JSON.parse(e.data);
      appendLogLine("btErrorLogs", d.message || "SSE 错误");
      es.close();
      state.sse = null;
      setButtonsRunning(false);
    });

    es.onerror = () => {
      es.close();
      state.sse = null;
      setButtonsRunning(false);
    };
  }

  function updateStatus(d) {
    $("btStatusText").textContent = statusLabel(d.status);
    $("btProgressText").textContent = (d.progress * 100).toFixed(1) + "%";
    $("btProcessedText").textContent = `${d.processed_stocks} / ${d.total_stocks}`;
    $("btProgressBar").style.width = (d.progress * 100) + "%";

    if (d.combo_total > 0) {
      $("btComboRow").style.display = "";
      $("btComboText").textContent = `${d.combo_index} / ${d.combo_total}`;
    }
  }

  function statusLabel(s) {
    const map = { queued: "排队中", running: "运行中", completed: "已完成", failed: "失败", stopped: "已停止", stopping: "失败" };
    return map[s] || s;
  }

  // ── logs ──
  function clearLogs() {
    state.logCursor.info = 0;
    state.logCursor.error = 0;
    $("btInfoLogs").textContent = "";
    $("btErrorLogs").textContent = "";
  }
  function appendLogs(elId, items) {
    const level = elId === "btErrorLogs" ? "error" : "info";
    for (const item of items) {
      const logId = Number(item.log_id || 0);
      if (logId > 0 && logId <= (state.logCursor[level] || 0)) {
        continue;
      }
      appendLogLine(elId, `[${item.ts || ""}] ${item.message}`);
      if (logId > 0) {
        state.logCursor[level] = logId;
      }
    }
  }
  function appendLogLine(elId, text) {
    const el = $(elId);
    el.textContent += text + "\n";
    // Trim
    const lines = el.textContent.split("\n");
    if (lines.length > LOG_KEEP + 20) {
      el.textContent = lines.slice(-LOG_KEEP).join("\n");
    }
    // Auto scroll
    el.scrollTop = el.scrollHeight;
  }

  // ── on complete ──
  async function onBacktestCompleted() {
    if (state.currentMode === "sweep") {
      await loadSweepResults();
    } else {
      await loadStats();
    }
  }

  // ── stats (fixed mode) ──

  /** 将后端嵌套 stats 结构 (profit/drawdown/sharpe 子字典) 转换为前端渲染所需的扁平结构 */
  function transformStatsData(raw) {
    const out = { total_hits: raw.total_hits, unique_stocks: raw.unique_stocks, per_forward: {} };
    for (const [fwdKey, fwdData] of Object.entries(raw.per_forward || {})) {
      const profit = fwdData.profit || {};
      const drawdown = fwdData.drawdown || {};
      const sharpe = fwdData.sharpe || {};
      const flat = {
        hit_count: profit.count ?? raw.total_hits ?? 0,
        profit_mean: profit.mean,
        profit_median: profit.median,
        profit_std: profit.std,
        profit_min: profit.min,
        profit_q25: profit.q25,
        profit_q75: profit.q75,
        profit_max: profit.max,
        win_rate: profit.win_rate,
        skewness: profit.skewness,
        kurtosis: profit.kurtosis,
        drawdown_mean: drawdown.mean,
        sharpe_mean: sharpe.mean,
      };
      // 正态检验
      const nt = {};
      if (profit.normality_test === "shapiro") nt.shapiro = { p_value: profit.normality_p };
      else if (profit.normality_test === "dagostino") nt.dagostino = { p_value: profit.normality_p };
      if (profit.jb_p != null) nt.jarque_bera = { p_value: profit.jb_p };
      if (Object.keys(nt).length) flat.normality_tests = nt;
      // 直方图
      if (Array.isArray(profit.histogram) && profit.histogram.length) {
        const bw = profit.bin_width || 1;
        const bins = profit.histogram.map((h) => h.bin_start);
        bins.push(bins[bins.length - 1] + bw);
        flat.histogram = { bins, counts: profit.histogram.map((h) => h.freq) };
      }
      out.per_forward[fwdKey] = flat;
    }
    return out;
  }

  async function loadStats() {
    if (!state.currentJobId) return;
    try {
      const raw = await getJSON(`/api/backtests/${state.currentJobId}/stats`);
      const data = transformStatsData(raw);
      showPanel("btStatsPanel");
      renderStats(data);
    } catch (err) {
      appendLogLine("btErrorLogs", "加载统计结果失败: " + err.message);
    }
  }

  function renderStats(data) {
    const container = $("btStatsContent");
    container.innerHTML = "";
    disposeCharts();

    const fwdLabels = data.forward_labels || {};

    for (const [fwdKey, fwdStats] of Object.entries(data.per_forward || {})) {
      const label = fwdLabels[fwdKey] || `前瞻 ${fwdKey} bar`;

      // Stats table card
      const card = document.createElement("div");
      card.className = "bt-stat-card";
      card.innerHTML = `<h4>${label} - 统计量</h4>`;
      const table = document.createElement("table");
      table.className = "bt-stat-table";
      const metrics = [
        ["命中数", "hit_count"],
        ["均值 (盈利%)", "profit_mean"],
        ["中位数 (盈利%)", "profit_median"],
        ["标准差 (盈利%)", "profit_std"],
        ["偏度", "skewness"],
        ["峰度", "kurtosis"],
        ["均值 (回撤%)", "drawdown_mean"],
        ["均值 (夏普)", "sharpe_mean"],
      ];
      for (const [lbl, key] of metrics) {
        const v = fwdStats[key];
        const tr = document.createElement("tr");
        tr.innerHTML = `<td>${lbl}</td><td>${fmtStat(v)}</td>`;
        table.appendChild(tr);
      }
      card.appendChild(table);

      // Normality tests
      if (fwdStats.normality_tests) {
        const ntDiv = document.createElement("div");
        ntDiv.style.cssText = "margin-top:6px; font-size:.8rem; color:var(--text-secondary);";
        const tests = fwdStats.normality_tests;
        const lines = [];
        if (tests.shapiro) lines.push(`Shapiro p=${fmtStat(tests.shapiro.p_value)}`);
        if (tests.dagostino) lines.push(`D'Agostino p=${fmtStat(tests.dagostino.p_value)}`);
        if (tests.jarque_bera) lines.push(`Jarque-Bera p=${fmtStat(tests.jarque_bera.p_value)}`);
        ntDiv.textContent = "正态检验: " + lines.join(" | ");
        card.appendChild(ntDiv);
      }

      container.appendChild(card);

      // Histogram chart
      if (fwdStats.histogram) {
        const chartCard = document.createElement("div");
        chartCard.className = "bt-stat-card";
        chartCard.innerHTML = `<h4>${label} - 盈利分布</h4>`;
        const chartEl = document.createElement("div");
        chartEl.className = "bt-chart-box";
        chartCard.appendChild(chartEl);
        container.appendChild(chartCard);
        renderHistogram(chartEl, fwdStats.histogram, fwdStats);
      }
    }

    // Boxplot comparison across forward periods
    if (Object.keys(data.per_forward || {}).length > 1) {
      const boxCard = document.createElement("div");
      boxCard.className = "bt-stat-card";
      boxCard.style.gridColumn = "1 / -1";
      boxCard.innerHTML = "<h4>各前瞻周期盈利对比 (箱线图)</h4>";
      const boxEl = document.createElement("div");
      boxEl.className = "bt-chart-box";
      boxCard.appendChild(boxEl);
      container.appendChild(boxCard);
      renderBoxplot(boxEl, data);
    }
  }

  function fmtStat(v) {
    if (v == null || (typeof v === "number" && isNaN(v))) return "—";
    if (typeof v === "number") return v.toFixed(4);
    return String(v);
  }

  function renderHistogram(el, histogram, fwdStats) {
    const chart = echarts.init(el, "dark");
    state.charts.push(chart);

    const bins = histogram.bins || [];
    const counts = histogram.counts || [];
    const categories = bins.map((b, i) => {
      if (i < bins.length - 1) return `${b.toFixed(1)}~${bins[i + 1].toFixed(1)}`;
      return `>${b.toFixed(1)}`;
    });

    const option = {
      backgroundColor: "transparent",
      tooltip: { trigger: "axis" },
      xAxis: { type: "category", data: categories, axisLabel: { fontSize: 10, rotate: 30 } },
      yAxis: { type: "value", name: "频数" },
      series: [
        {
          type: "bar",
          data: counts,
          itemStyle: { color: "rgba(99,179,237,0.7)" },
          barMaxWidth: 30,
        },
      ],
    };

    // Add mean line
    if (fwdStats.profit_mean != null) {
      const meanIdx = bins.findIndex((b, i) =>
        i < bins.length - 1 && fwdStats.profit_mean >= b && fwdStats.profit_mean < bins[i + 1]
      );
      if (meanIdx >= 0) {
        option.series.push({
          type: "line",
          markLine: {
            silent: true,
            data: [{ xAxis: meanIdx, name: "均值" }],
            lineStyle: { color: "#f59e0b", type: "dashed" },
            label: { formatter: "均值", fontSize: 10 },
          },
          data: [],
        });
      }
    }

    chart.setOption(option);
  }

  function renderBoxplot(el, data) {
    const chart = echarts.init(el, "dark");
    state.charts.push(chart);

    const labels = [];
    const boxData = [];
    for (const [fwdKey, fwdStats] of Object.entries(data.per_forward || {})) {
      const label = (data.forward_labels || {})[fwdKey] || fwdKey;
      labels.push(label);
      // [min, Q1, median, Q3, max]
      boxData.push([
        fwdStats.profit_min ?? 0,
        fwdStats.profit_q25 ?? 0,
        fwdStats.profit_median ?? 0,
        fwdStats.profit_q75 ?? 0,
        fwdStats.profit_max ?? 0,
      ]);
    }

    chart.setOption({
      backgroundColor: "transparent",
      tooltip: {},
      xAxis: { type: "category", data: labels },
      yAxis: { type: "value", name: "盈利 %" },
      series: [
        {
          type: "boxplot",
          data: boxData,
          itemStyle: { borderColor: "#63b3ed", color: "rgba(99,179,237,0.3)" },
        },
      ],
    });
  }

  function disposeCharts() {
    state.charts.forEach((c) => { try { c.dispose(); } catch {} });
    state.charts = [];
  }

  // ── sweep results ──
  async function loadSweepResults() {
    if (!state.currentJobId) return;
    try {
      const data = await getJSON(`/api/backtests/${state.currentJobId}/sweep`);
      showPanel("btSweepPanel");
      renderSweepTable(data);
    } catch (err) {
      appendLogLine("btErrorLogs", "加载扫描结果失败: " + err.message);
    }
  }

  function renderSweepTable(data) {
    const items = data.items || [];
    if (!items.length) {
      $("btSweepBody").innerHTML = "<tr><td>无结果</td></tr>";
      return;
    }

    // Determine columns: param keys + metric keys
    const firstCombo = items[0].param_combo || {};
    const paramKeys = Object.keys(firstCombo);
    const metricKeys = [
      "total_hits", "win_rate_x", "avg_profit_x", "avg_drawdown_x",
      "win_rate_z", "avg_profit_z", "avg_drawdown_z",
    ];
    const metricLabels = {
      total_hits: "命中数",
      win_rate_x: "胜率x%", avg_profit_x: "平均盈利x%", avg_drawdown_x: "平均回撤x%",
      win_rate_z: "胜率z%", avg_profit_z: "平均盈利z%", avg_drawdown_z: "平均回撤z%",
    };

    // Thead
    const head = $("btSweepHead");
    head.innerHTML = "<tr>" +
      paramKeys.map((k) => `<th data-sort-key="param.${k}">${k}</th>`).join("") +
      metricKeys.map((k) => `<th data-sort-key="metric.${k}">${metricLabels[k] || k}</th>`).join("") +
      "<th>详情</th></tr>";

    let sortedItems = [...items];

    function renderBody() {
      const body = $("btSweepBody");
      body.innerHTML = "";
      for (const row of sortedItems) {
        const tr = document.createElement("tr");
        const params = row.param_combo || {};
        tr.innerHTML =
          paramKeys.map((k) => `<td>${params[k] ?? "—"}</td>`).join("") +
          metricKeys.map((k) => `<td>${fmtStat(row[k])}</td>`).join("") +
          `<td><button class="bt-detail-btn" data-combo="${row.combo_index}">详情</button></td>`;
        body.appendChild(tr);
      }
      // Detail buttons
      body.querySelectorAll(".bt-detail-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          const combo = btn.dataset.combo;
          window.open(
            `/backtest-detail?job_id=${state.currentJobId}&combo_index=${combo}`,
            "_blank"
          );
        });
      });
    }

    renderBody();

    // Sort on header click
    head.addEventListener("click", (e) => {
      const th = e.target.closest("th[data-sort-key]");
      if (!th) return;
      const key = th.dataset.sortKey;
      const [type, field] = key.split(".");
      const asc = th.dataset.sortDir !== "asc";
      th.dataset.sortDir = asc ? "asc" : "desc";

      sortedItems.sort((a, b) => {
        let va, vb;
        if (type === "param") {
          va = (a.param_combo || {})[field];
          vb = (b.param_combo || {})[field];
        } else {
          va = a[field];
          vb = b[field];
        }
        if (va == null) va = -Infinity;
        if (vb == null) vb = -Infinity;
        return asc ? va - vb : vb - va;
      });
      renderBody();
    });
  }

  // ── UI helpers ──
  function showPanel(id) { $(id).style.display = ""; }
  function hidePanel(id) { $(id).style.display = "none"; }

  function setButtonsRunning(running) {
    $("btRunFixedBtn").disabled = running;
    $("btRunSweepBtn").disabled = running;
    $("btStopBtn").disabled = !running;
    $("btStopSweepBtn").disabled = !running;
  }

  // ── resize ──
  window.addEventListener("resize", () => {
    state.charts.forEach((c) => { try { c.resize(); } catch {} });
  });

  // ── go ──
  init().catch((err) => console.error("backtest init failed:", err));
})();
