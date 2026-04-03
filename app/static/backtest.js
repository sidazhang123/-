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
    monitorPerStrategyParams: {}, // 服务端持久化的每策略参数 { groupId: params }
    sweepRanges: {},       // { "path.to.param": { min, max, step } }
    perStrategySweepRanges: {},  // 服务端持久化 { groupId: {path: {min,max,step}} }
    perStrategyBacktestParams: {},  // 服务端持久化 { groupId: { forward_bars, slide_step } }
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

  /** 从监控页持久化设置中读取已保存的 per_strategy_params，回测页启动时与 default_params 合并 */
  async function loadMonitorParams() {
    try {
      const data = await getJSON("/api/ui-settings/monitor");
      const s = data.settings || {};
      // 存储每策略参数映射，而不仅是当前策略
      state.monitorPerStrategyParams = (s.per_strategy_params && typeof s.per_strategy_params === "object")
        ? s.per_strategy_params
        : {};
    } catch {
      state.monitorPerStrategyParams = {};
    }
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

    // 参数快照弹窗
    bindBtParamModal();

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
      (g) => g && (g.usage || []).includes("backtest")
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
      state._prevGroupId = state.strategyGroups[0].id;
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
    // 切换前先将当前策略的 btParams 快照到 state，避免丢失未保存的修改
    if (state._prevGroupId) {
      state.perStrategyBacktestParams[state._prevGroupId] = {
        forward_bars: [
          parseInt($("btFwdX").value) || 2,
          parseInt($("btFwdY").value) || 5,
          parseInt($("btFwdZ").value) || 7,
        ],
        slide_step: parseInt($("btSlideStep").value) || 1,
      };
    }
    const newId = selectedGroupId();
    state._prevGroupId = newId;
    applyGroupParams(newId);
  }

  function applyGroupParams(groupId) {
    const group = findGroup(groupId);
    if (!group) return;
    const base = cloneValue(group.default_params || {});
    // 合并服务端持久化的该策略参数（单一来源）
    const saved = (state.monitorPerStrategyParams || {})[groupId];
    if (saved && typeof saved === "object") {
      deepMerge(base, saved);
    }
    state.groupParams = base;
    renderFixedParams(group);
    renderSweepParams(group);
    // 恢复该策略的 sweep ranges
    const sweepRanges = (state.perStrategySweepRanges || {})[groupId];
    if (sweepRanges && typeof sweepRanges === "object" && Object.keys(sweepRanges).length) {
      applySavedSweepRanges(sweepRanges);
    }
    // 恢复该策略的回测参数（forward_bars / slide_step），无记录时重置为默认值
    const btParams = (state.perStrategyBacktestParams || {})[groupId];
    if (btParams && typeof btParams === "object") {
      $("btFwdX").value = (btParams.forward_bars && btParams.forward_bars[0]) || 2;
      $("btFwdY").value = (btParams.forward_bars && btParams.forward_bars[1]) || 5;
      $("btFwdZ").value = (btParams.forward_bars && btParams.forward_bars[2]) || 7;
      $("btSlideStep").value = btParams.slide_step || 1;
    } else {
      $("btFwdX").value = 2;
      $("btFwdY").value = 5;
      $("btFwdZ").value = 7;
      $("btSlideStep").value = 1;
    }
  }

  // ── param rendering ──
  function renderFixedParams(group) {
    const container = $("btParamsFixed");
    container.innerHTML = renderParamField(
      [], state.groupParams, group.param_help || {}, "", 0, { readonly: true, hideScopeParams: true }
    );
  }

  function renderSweepParams(group) {
    const container = $("btParamsSweep");
    container.innerHTML = renderParamField(
      [], state.groupParams, group.param_help || {}, "", 0, { hideScopeParams: true }
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

      state.perStrategySweepRanges = (s.per_strategy_sweep_ranges && typeof s.per_strategy_sweep_ranges === "object")
        ? cloneValue(s.per_strategy_sweep_ranges)
        : {};

      state.perStrategyBacktestParams = (s.per_strategy_backtest_params && typeof s.per_strategy_backtest_params === "object")
        ? cloneValue(s.per_strategy_backtest_params)
        : {};

      // 全局 forward_bars / slide_step 仅作为无每策略记录时的兜底默认值
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
          state._prevGroupId = s.strategy_group_id;
          applyGroupParams(s.strategy_group_id);
        }
      }
    } catch { /* ignore */ }
  }

  async function saveBacktestSettings() {
    const groupId = selectedGroupId();

    // 收集当前 sweep ranges 并过滤掉 scope 路径
    const allRanges = collectAllSweepRanges();
    const group = findGroup(groupId);
    const scopePathsForFilter = new Set();
    if (group) {
      const dp = group.default_params || {};
      for (const [sKey, sec] of Object.entries(dp)) {
        if (sec && typeof sec === "object" && Array.isArray(sec.scope_params)) {
          sec.scope_params.forEach((n) => scopePathsForFilter.add(`${sKey}.${n}`));
        }
      }
    }
    const filteredRanges = {};
    for (const [p, r] of Object.entries(allRanges)) {
      if (!scopePathsForFilter.has(p)) filteredRanges[p] = r;
    }

    // 保存当前策略的 sweep ranges
    const pssr = cloneValue(state.perStrategySweepRanges || {});
    pssr[groupId] = filteredRanges;

    // 保存当前策略的回测参数（forward_bars / slide_step）
    const psbp = cloneValue(state.perStrategyBacktestParams || {});
    const curBtParams = {
      forward_bars: [
        parseInt($("btFwdX").value) || 2,
        parseInt($("btFwdY").value) || 5,
        parseInt($("btFwdZ").value) || 7,
      ],
      slide_step: parseInt($("btSlideStep").value) || 1,
    };
    psbp[groupId] = curBtParams;

    const payload = {
      forward_bars: curBtParams.forward_bars,
      slide_step: curBtParams.slide_step,
      strategy_group_id: groupId,
      group_params: cloneValue(state.groupParams),
      per_strategy_sweep_ranges: pssr,
      per_strategy_backtest_params: psbp,
    };
    const resp = await postJSON("/api/ui-settings/backtest", payload);
    state.perStrategySweepRanges = cloneValue(pssr);
    state.perStrategyBacktestParams = cloneValue(psbp);
    // 同步更新本地 monitorPerStrategyParams（后端已交叉写入 monitor）
    state.monitorPerStrategyParams[groupId] = cloneValue(state.groupParams);
    return resp;
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

  /**
   * 从 group_params 副本中移除所有周期 section 的 scope 参数及 scope_params 声明。
   * 同时返回 scope 参数路径集合，用于过滤 sweepRanges / param_ranges。
   */
  function stripScopeFromParams(params) {
    const stripped = cloneValue(params);
    const scopePaths = new Set();
    for (const [sectionKey, section] of Object.entries(stripped)) {
      if (!section || typeof section !== "object" || !Array.isArray(section.scope_params)) continue;
      for (const name of section.scope_params) {
        scopePaths.add(`${sectionKey}.${name}`);
        delete section[name];
      }
      delete section.scope_params;
    }
    return { params: stripped, scopePaths };
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
    const paramsBtn = $("btShowParamsBtn");
    if (paramsBtn) paramsBtn.disabled = !jobId;
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

    const { params: cleanParams, scopePaths } = stripScopeFromParams(state.groupParams);

    const payload = {
      strategy_group_id: groupId,
      mode: mode,
      forward_bars: fwdBars,
      slide_step: slideStep,
      group_params: cleanParams,
      param_ranges: {},
    };

    if (mode === "sweep") {
      for (const [pathStr, range] of Object.entries(state.sweepRanges)) {
        if (scopePaths.has(pathStr)) continue;
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

  /**
   * 更新回测状态面板：进度百分比、已处理股票数、参数组合进度。
   * sweep 模式：综合 combo 进度和 combo 内股票进度计算精细百分比，显示参数组合行。
   * fixed 模式：仅显示股票级进度，隐藏参数组合行。
   *
   * 输入: d 后端回测状态对象 { status, progress, processed_stocks, total_stocks, combo_index, combo_total, mode }
   * 输出: 更新 DOM 文本和进度条宽度
   * 边界: combo_total 为 0 时隐藏组合行（fixed 模式）
   */
  function updateStatus(d) {
    $("btStatusText").textContent = statusLabel(d.status);
    $("btProcessedText").textContent = `${d.processed_stocks} / ${d.total_stocks}`;

    const isSweep = d.combo_total > 0;
    if (isSweep) {
      // sweep 模式：综合组合级进度 + 组合内股票进度
      const comboBase = d.combo_index > 0 ? (d.combo_index - 1) / d.combo_total : 0;
      const comboSlice = 1 / d.combo_total;
      const stockFrac = d.total_stocks > 0 ? d.processed_stocks / d.total_stocks : 0;
      const pct = Math.min((comboBase + comboSlice * stockFrac) * 100, 100);
      $("btProgressText").textContent = pct.toFixed(1) + "%";
      $("btProgressBar").style.width = pct + "%";
      $("btComboRow").style.display = "";
      $("btComboText").textContent = `${d.combo_index} / ${d.combo_total}`;
    } else {
      // fixed 模式：直接使用后端 progress
      const pct = (d.progress * 100);
      $("btProgressText").textContent = pct.toFixed(1) + "%";
      $("btProgressBar").style.width = pct + "%";
      $("btComboRow").style.display = "none";
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
        drawdown_median: drawdown.median,
        drawdown_std: drawdown.std,
        sharpe_mean: sharpe.mean,
      };
      // 正态检验
      const nt = {};
      if (profit.normality_test === "shapiro") nt.shapiro = { p_value: profit.normality_p };
      else if (profit.normality_test === "dagostino") nt.dagostino = { p_value: profit.normality_p };
      if (profit.jb_p != null) nt.jarque_bera = { p_value: profit.jb_p };
      if (Object.keys(nt).length) flat.normality_tests = nt;
      // 盈利直方图
      if (Array.isArray(profit.histogram) && profit.histogram.length) {
        const bw = profit.bin_width || 1;
        const bins = profit.histogram.map((h) => h.bin_start);
        bins.push(bins[bins.length - 1] + bw);
        flat.histogram = { bins, counts: profit.histogram.map((h) => h.freq) };
      }
      // 回撤直方图
      if (Array.isArray(drawdown.histogram) && drawdown.histogram.length) {
        const bw = drawdown.bin_width || 1;
        const bins = drawdown.histogram.map((h) => h.bin_start);
        bins.push(bins[bins.length - 1] + bw);
        flat.drawdown_histogram = { bins, counts: drawdown.histogram.map((h) => h.freq) };
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
        ["中位数 (回撤%)", "drawdown_median"],
        ["标准差 (回撤%)", "drawdown_std"],
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

      // 盈利直方图
      if (fwdStats.histogram) {
        const chartCard = document.createElement("div");
        chartCard.className = "bt-stat-card";
        chartCard.innerHTML = `<h4>${label} - 盈利分布</h4>`;
        const chartEl = document.createElement("div");
        chartEl.className = "bt-chart-box";
        chartCard.appendChild(chartEl);
        container.appendChild(chartCard);
        renderHistogram(chartEl, fwdStats.histogram, fwdStats.profit_mean, "盈利%", "rgba(99,179,237,0.7)");
      }

      // 回撤直方图
      if (fwdStats.drawdown_histogram) {
        const chartCard = document.createElement("div");
        chartCard.className = "bt-stat-card";
        chartCard.innerHTML = `<h4>${label} - 回撤分布</h4>`;
        const chartEl = document.createElement("div");
        chartEl.className = "bt-chart-box";
        chartCard.appendChild(chartEl);
        container.appendChild(chartCard);
        renderHistogram(chartEl, fwdStats.drawdown_histogram, fwdStats.drawdown_mean, "回撤%", "rgba(245,101,101,0.7)");
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

  function renderHistogram(el, histogram, meanValue, axisName, barColor) {
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
          itemStyle: { color: barColor },
          barMaxWidth: 30,
        },
      ],
    };

    // 均值标记线
    if (meanValue != null) {
      const meanIdx = bins.findIndex((b, i) =>
        i < bins.length - 1 && meanValue >= b && meanValue < bins[i + 1]
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
      const data = await getJSON(`/api/backtests/${state.currentJobId}/sweep-stats`);
      showPanel("btSweepPanel");
      renderSweepStatsTable(data);
      renderSweepCharts(data);
    } catch (err) {
      appendLogLine("btErrorLogs", "加载扫描结果失败: " + err.message);
    }
  }

  // ── sweep 评分与可视化 ──

  /**
   * 计算单个参数组合的综合评分。
   * 取最长可用前瞻周期的：中位盈利 - 中位回撤 + clamp(中位夏普, -10, 10)。
   *
   * 输入: combo (含 per_forward), fwdBars [x, y, z]
   * 输出: number (综合评分)
   * 边界: 全部前瞻缺失时返回 0
   */
  function computeComboScore(combo, fwdBars) {
    const pf = combo.per_forward || {};
    for (let i = fwdBars.length - 1; i >= 0; i--) {
      const d = pf[String(fwdBars[i])];
      if (!d) continue;
      const pm = d.profit_median || 0;
      const dm = d.drawdown_median || 0;
      const sh = d.sharpe_median != null ? Math.max(-10, Math.min(10, d.sharpe_median)) : 0;
      return pm - dm + sh;
    }
    return 0;
  }

  /**
   * 从 combo 列表中提取真正被扫描的参数键（值在各组合间有变化）。
   *
   * 输入: combos (含 param_combo)
   * 输出: string[] 被扫描的参数键
   * 边界: 空列表返回 []
   */
  function extractSweepKeys(combos) {
    if (!combos.length) return [];
    const keys = Object.keys(combos[0].param_combo || {});
    return keys.filter(k => {
      const vals = new Set(combos.map(c => String((c.param_combo || {})[k])));
      return vals.size > 1;
    });
  }

  /**
   * 将参数组合格式化为短标签，仅显示扫描参数的简短名+值。
   *
   * 输入: paramCombo { "section.param": value }, sweepKeys string[]
   * 输出: 如 "min_vol=1.50 | candles=10"
   * 边界: 空参数返回 "—"
   */
  function formatComboLabel(paramCombo, sweepKeys) {
    if (!paramCombo || !sweepKeys || !sweepKeys.length) return "—";
    return sweepKeys.map(k => {
      const shortKey = k.split(".").pop();
      const val = paramCombo[k];
      const vStr = typeof val === "number"
        ? (Number.isInteger(val) ? String(val) : val.toFixed(2))
        : String(val);
      return `${shortKey}=${vStr}`;
    }).join(" | ");
  }

  /**
   * 构建 sweep 汇总表：每个参数组合在各前瞻周期的中位盈利/中位回撤/中位夏普 + 综合评分。
   * 参数值单独列明，默认按综合评分降序排列，最优行高亮。
   *
   * 输入: data { combos, forward_bars, forward_labels }
   * 输出: 渲染到 DOM (#btSweepHead, #btSweepBody)
   * 边界: combos 为空时显示 "无结果"
   */
  function renderSweepStatsTable(data) {
    const combos = data.combos || [];
    if (!combos.length) {
      $("btSweepBody").innerHTML = "<tr><td>无结果</td></tr>";
      return;
    }

    const fwdBars = data.forward_bars || [];
    const fwdLabels = data.forward_labels || {};
    const sweepKeys = extractSweepKeys(combos);
    const paramKeys = sweepKeys.length ? sweepKeys : Object.keys(combos[0].param_combo || {});

    const scored = combos.map(c => ({ ...c, _score: computeComboScore(c, fwdBars) }));
    scored.sort((a, b) => b._score - a._score);
    const bestComboIdx = scored.length ? scored[0].combo_index : -1;

    const metricDefs = [
      { key: "profit_median", label: "中位盈利%", fn: pf => pf.profit_median },
      { key: "drawdown_median", label: "中位回撤%", fn: pf => pf.drawdown_median },
      { key: "sharpe_median", label: "中位夏普", fn: pf => pf.sharpe_median },
    ];
    const metricsPerFwd = metricDefs.length;

    const head = $("btSweepHead");
    let row1 = "<tr>";
    paramKeys.forEach(k => {
      row1 += `<th rowspan="2" data-sort-key="param.${k}">${k.split(".").pop()}</th>`;
    });
    row1 += '<th rowspan="2" data-sort-key="flat.total_hits">命中</th>';
    fwdBars.forEach(fb => {
      const lbl = fwdLabels[String(fb)] || fb;
      row1 += `<th colspan="${metricsPerFwd}" style="text-align:center;border-bottom:none;">前瞻 ${lbl} (${fb}bar)</th>`;
    });
    row1 += '<th rowspan="2" data-sort-key="flat._score">评分</th>';
    row1 += '<th rowspan="2">详情</th></tr>';


    let row2 = "<tr>";
    fwdBars.forEach(() => {
      metricDefs.forEach(md => {
        row2 += `<th data-sort-key="metric.${md.key}" style="font-size:.75rem;">${md.label}</th>`;
      });
    });
    row2 += "</tr>";
    head.innerHTML = row1 + row2;

    function flattenCombo(c) {
      const flat = { _src: c, total_hits: c.total_hits, _score: c._score };
      const pc = c.param_combo || {};
      for (const k of paramKeys) flat[`param.${k}`] = pc[k];
      for (const fb of fwdBars) {
        const pf = (c.per_forward || {})[String(fb)] || {};
        for (const md of metricDefs) flat[`${fb}.${md.key}`] = md.fn(pf);
      }
      return flat;
    }

    let sortedFlat = scored.map(flattenCombo);
    let _lastSortFwd = null;

    function renderBody() {
      const body = $("btSweepBody");
      body.innerHTML = "";
      for (const flat of sortedFlat) {
        const c = flat._src;
        const tr = document.createElement("tr");
        if (c.combo_index === bestComboIdx) tr.className = "bt-sweep-best-row";
        const pc = c.param_combo || {};
        let cells = paramKeys.map(k => `<td>${pc[k] ?? "—"}</td>`).join("");
        cells += `<td>${c.total_hits}</td>`;
        for (const fb of fwdBars) {
          const pf = (c.per_forward || {})[String(fb)] || {};
          for (const md of metricDefs) cells += `<td>${fmtStat(md.fn(pf))}</td>`;
        }
        const scoreColor = c._score >= 0 ? "#68d391" : "#fc8181";
        cells += `<td style="font-weight:600;color:${scoreColor}">${c._score.toFixed(2)}</td>`;
        cells += `<td><button class="bt-detail-btn" data-combo="${c.combo_index}">详情</button></td>`;
        tr.innerHTML = cells;
        body.appendChild(tr);
      }
      body.querySelectorAll(".bt-detail-btn").forEach(btn => {
        btn.addEventListener("click", () => {
          window.open(`/backtest-detail?job_id=${state.currentJobId}&combo_index=${btn.dataset.combo}`, "_blank");
        });
      });
    }

    renderBody();

    head.addEventListener("click", (e) => {
      const th = e.target.closest("th[data-sort-key]");
      if (!th) return;
      const sortKey = th.dataset.sortKey;
      const asc = th.dataset.sortDir !== "asc";
      head.querySelectorAll("th[data-sort-key]").forEach(h => delete h.dataset.sortDir);
      th.dataset.sortDir = asc ? "asc" : "desc";
      const [type, field] = sortKey.split(".");
      let _sortFwd = _lastSortFwd;
      if (type === "metric") {
        const thIdx = [...head.querySelectorAll("th[data-sort-key]")].indexOf(th);
        const paramColCount = paramKeys.length + 1;
        if (thIdx >= paramColCount) {
          const metricGlobalIdx = thIdx - paramColCount;
          const fwdIdx = Math.floor(metricGlobalIdx / metricsPerFwd);
          if (fwdIdx < fwdBars.length) _sortFwd = fwdBars[fwdIdx];
        }
        _lastSortFwd = _sortFwd;
      }
      sortedFlat.sort((a, b) => {
        let va, vb;
        if (type === "param") { va = a[`param.${field}`]; vb = b[`param.${field}`]; }
        else if (type === "flat") { va = a[field]; vb = b[field]; }
        else { va = _sortFwd != null ? a[`${_sortFwd}.${field}`] : null; vb = _sortFwd != null ? b[`${_sortFwd}.${field}`] : null; }
        if (va == null) va = -Infinity;
        if (vb == null) vb = -Infinity;
        return asc ? va - vb : vb - va;
      });
      renderBody();
    });
  }

  /**
   * 渲染 sweep 全部图表：前瞻周期标签 + 散点图 + 评分排行 + 热力图。
   * 缓存数据到 state._sweepChartData 用于切换前瞻周期时重绘散点图。
   *
   * 输入: data { combos, forward_bars, forward_labels }
   * 输出: 渲染到 DOM
   * 边界: 无 combo 或无 fwdBars 时隐藏整个图表区域
   */
  function renderSweepCharts(data) {
    const combos = data.combos || [];
    const fwdBars = data.forward_bars || [];
    const fwdLabels = data.forward_labels || {};

    if (!combos.length || !fwdBars.length) {
      $("btSweepCharts").style.display = "none";
      return;
    }
    $("btSweepCharts").style.display = "";

    const sweepKeys = extractSweepKeys(combos);

    const scored = combos.map(c => ({
      ...c,
      _score: computeComboScore(c, fwdBars),
      _label: formatComboLabel(c.param_combo, sweepKeys),
    }));
    scored.sort((a, b) => b._score - a._score);

    // 缓存用于前瞻周期切换
    state._sweepChartData = { scored, fwdBars, fwdLabels, sweepKeys };

    // 1. 前瞻周期切换标签
    _renderFwdTabs(fwdBars, fwdLabels);

    // 2. 散点图（默认最长前瞻周期）
    _renderComboScatter(fwdBars[fwdBars.length - 1]);

    // 3. 评分排行
    _renderRankingChart(scored, sweepKeys);

    // 4. 参数热力图（仅双参数扫描）
    _renderSweepHeatmap(scored, sweepKeys);
  }

  /**
   * 渲染前瞻周期切换标签行。点击切换时重绘散点图。
   *
   * 输入: fwdBars [x, y, z], fwdLabels { "2": "x", ... }
   * 输出: 渲染到 #btSweepFwdTabs
   * 边界: 单前瞻周期时仍显示（无需切换）
   */
  function _renderFwdTabs(fwdBars, fwdLabels) {
    const container = $("btSweepFwdTabs");
    if (!container) return;
    container.innerHTML = '<span style="font-size:.85rem;color:var(--color-text-secondary);margin-right:8px;">前瞻周期:</span>';
    fwdBars.forEach((fb, i) => {
      const lbl = fwdLabels[String(fb)] || String(fb);
      const btn = document.createElement("button");
      btn.className = "bt-tab" + (i === fwdBars.length - 1 ? " active" : "");
      btn.textContent = `${lbl} (${fb} bar)`;
      btn.dataset.fwdBar = fb;
      btn.addEventListener("click", () => {
        container.querySelectorAll(".bt-tab").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        _renderComboScatter(fb);
      });
      container.appendChild(btn);
    });
  }

  /**
   * 渲染参数组合效果散点图。
   * X 轴 = 中位盈利%，Y 轴 = 中位回撤%（越低越好），颜色 = 中位夏普，气泡大小 = 命中数。
   * 每个气泡 = 一个参数组合，tooltip 显示具体参数值和全部指标。
   * 理想区域在右下方（高盈利、低回撤）。
   *
   * 输入: fwdBar 当前选中的前瞻周期 bar 数
   * 输出: 渲染到 #btSweepScatterMain
   * 边界: _sweepChartData 不存在时跳过
   */
  function _renderComboScatter(fwdBar) {
    const el = $("btSweepScatterMain");
    if (!el || !state._sweepChartData) return;

    const { scored, sweepKeys } = state._sweepChartData;
    const fwdKey = String(fwdBar);

    // 销毁此容器上的旧 chart
    const oldChart = echarts.getInstanceByDom(el);
    if (oldChart) {
      const idx = state.charts.indexOf(oldChart);
      if (idx >= 0) state.charts.splice(idx, 1);
      oldChart.dispose();
    }

    const chart = echarts.init(el, "dark");
    state.charts.push(chart);

    // 组装散点数据: [profit_median, drawdown_median, sharpe_median, total_hits, comboObj]
    const scatterData = [];
    let maxHits = 1;
    for (const c of scored) {
      const pf = (c.per_forward || {})[fwdKey];
      if (!pf) continue;
      const profit = pf.profit_median ?? 0;
      const dd = pf.drawdown_median ?? 0;
      const sharpe = pf.sharpe_median;
      const hits = c.total_hits || 1;
      if (hits > maxHits) maxHits = hits;
      scatterData.push({
        value: [profit, dd, sharpe ?? 0, hits],
        _combo: c,
        _sharpe: sharpe,
      });
    }

    // 气泡大小映射（命中数 → 8~40px）
    const sizeMin = 8, sizeMax = 40;
    function symbolSize(data) {
      const hits = data[3];
      if (maxHits <= 1) return 20;
      const ratio = Math.sqrt(hits / maxHits);
      return sizeMin + ratio * (sizeMax - sizeMin);
    }

    // 夏普范围（用于 visualMap 颜色映射）
    const sharpeVals = scatterData.map(d => d.value[2]).filter(v => v != null);
    const sharpeMin = sharpeVals.length ? Math.min(...sharpeVals) : -1;
    const sharpeMax = sharpeVals.length ? Math.max(...sharpeVals) : 1;

    chart.setOption({
      backgroundColor: "transparent",
      tooltip: {
        trigger: "item",
        formatter: function (params) {
          const c = params.data._combo;
          const pc = c.param_combo || {};
          const paramStr = Object.entries(pc)
            .map(([k, v]) => `<b>${k.split(".").pop()}</b>: ${typeof v === "number" ? (Number.isInteger(v) ? v : v.toFixed(2)) : v}`)
            .join("<br/>");
          const [profit, dd, sharpe] = params.data.value;
          return `<div style="max-width:300px;"><b>参数组合 #${c.combo_index}</b><br/>`
            + paramStr
            + `<br/>─────<br/>`
            + `命中: ${c.total_hits}<br/>`
            + `中位盈利: <b>${fmtStat(profit)}%</b><br/>`
            + `中位回撤: <b>${fmtStat(dd)}%</b><br/>`
            + `中位夏普: <b>${fmtStat(params.data._sharpe)}</b><br/>`
            + `综合评分: <b>${c._score.toFixed(2)}</b></div>`;
        },
      },
      grid: { top: 20, bottom: 45, left: 55, right: 80 },
      xAxis: {
        type: "value",
        name: "中位盈利 %",
        nameLocation: "center",
        nameGap: 30,
        axisLabel: { fontSize: 10 },
        min: function (value) { var pad = Math.max((value.max - value.min) * 0.1, 0.5); return +(value.min - pad).toFixed(2); },
        max: function (value) { var pad = Math.max((value.max - value.min) * 0.1, 0.5); return +(value.max + pad).toFixed(2); },
      },
      yAxis: {
        type: "value",
        name: "中位回撤 %",
        nameLocation: "center",
        nameGap: 40,
        axisLabel: { fontSize: 10 },
        min: function (value) { var pad = Math.max((value.max - value.min) * 0.1, 0.5); return +(value.min - pad).toFixed(2); },
        max: function (value) { var pad = Math.max((value.max - value.min) * 0.1, 0.5); return +(value.max + pad).toFixed(2); },
      },
      visualMap: {
        min: sharpeMin === sharpeMax ? sharpeMin - 1 : sharpeMin,
        max: sharpeMin === sharpeMax ? sharpeMax + 1 : sharpeMax,
        dimension: 2,
        calculable: true,
        orient: "vertical",
        right: 0,
        top: 20,
        text: ["夏普高", "夏普低"],
        inRange: { color: ["#fc8181", "#fbd38d", "#68d391"] },
        textStyle: { fontSize: 10, color: "#ccc" },
      },
      series: [{
        type: "scatter",
        data: scatterData,
        symbolSize: function (data) { return symbolSize(data); },
        emphasis: {
          focus: "self",
          itemStyle: { shadowBlur: 10, shadowColor: "rgba(255,255,255,0.3)" },
        },
        label: {
          show: scatterData.length <= 30,
          position: "top",
          fontSize: 9,
          color: "#aaa",
          formatter: function (params) {
            return params.data._combo._label;
          },
        },
      }],
    });
  }

  /**
   * 渲染水平柱状图：展示每个参数组合的综合评分。
   * Y 轴标签显示具体参数值，颜色红→黄→绿映射评分高低。
   *
   * 输入: scored 已排序的 combo 列表（含 _score, _label）, sweepKeys 扫描参数键
   * 输出: 渲染到 #btSweepRankChart
   * 边界: 超过 20 条时自动添加 dataZoom 滚动条
   */
  function _renderRankingChart(scored, sweepKeys) {
    const el = $("btSweepRankChart");
    if (!el) return;

    const chartH = Math.max(300, Math.min(600, scored.length * 25 + 80));
    el.style.height = chartH + "px";

    const chart = echarts.init(el, "dark");
    state.charts.push(chart);

    // Y 轴从下到上，反转使最优在顶部
    const reversed = [...scored].reverse();
    const labels = reversed.map(c => c._label);
    const values = reversed.map(c => c._score);

    const allScores = scored.map(c => c._score);
    const minScore = Math.min(...allScores);
    const maxScore = Math.max(...allScores);

    const option = {
      backgroundColor: "transparent",
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        formatter: function (params) {
          const idx = params[0].dataIndex;
          const c = reversed[idx];
          const pc = c.param_combo || {};
          const paramStr = Object.entries(pc)
            .map(([k, v]) => `<b>${k.split(".").pop()}</b>: ${typeof v === "number" ? (Number.isInteger(v) ? v : v.toFixed(2)) : v}`)
            .join("<br/>");
          return `<b>${c._label}</b><br/>${paramStr}<br/>命中: ${c.total_hits}<br/>综合评分: <b>${c._score.toFixed(2)}</b>`;
        },
      },
      grid: { top: 15, bottom: 30, left: 10, right: 40, containLabel: true },
      xAxis: {
        type: "value",
        name: "综合评分",
        nameLocation: "end",
        axisLabel: { fontSize: 10 },
      },
      yAxis: {
        type: "category",
        data: labels,
        axisLabel: { fontSize: 10, width: 220, overflow: "truncate" },
        axisTick: { show: false },
      },
      visualMap: {
        show: false,
        min: minScore === maxScore ? minScore - 1 : minScore,
        max: minScore === maxScore ? maxScore + 1 : maxScore,
        dimension: 0,
        inRange: { color: ["#fc8181", "#fbd38d", "#68d391"] },
      },
      series: [{
        type: "bar",
        data: values,
        barMaxWidth: 22,
        label: {
          show: true,
          position: "right",
          fontSize: 10,
          formatter: ({ value }) => value.toFixed(1),
        },
      }],
    };

    if (scored.length > 20) {
      option.dataZoom = [{
        type: "slider",
        yAxisIndex: 0,
        start: 100 - (20 / scored.length) * 100,
        end: 100,
        width: 16,
        right: 5,
      }];
      option.grid.right = 60;
    }

    chart.setOption(option);
  }

  /**
   * 渲染参数热力图，仅在恰好两个扫描参数时显示。
   * X 轴 = 参数 1 各值，Y 轴 = 参数 2 各值，颜色 = 综合评分。
   *
   * 输入: scored (含 _score, param_combo), sweepKeys string[]
   * 输出: 渲染到 #btSweepHeatmap（或隐藏 #btSweepHeatmapCard）
   * 边界: 非双参数时隐藏
   */
  function _renderSweepHeatmap(scored, sweepKeys) {
    const card = $("btSweepHeatmapCard");
    const el = $("btSweepHeatmap");
    if (!card || !el) return;

    if (sweepKeys.length !== 2) {
      card.style.display = "none";
      return;
    }

    card.style.display = "";
    const chart = echarts.init(el, "dark");
    state.charts.push(chart);

    const [keyX, keyY] = sweepKeys;
    const shortX = keyX.split(".").pop();
    const shortY = keyY.split(".").pop();

    const valsX = [...new Set(scored.map(c => (c.param_combo || {})[keyX]))].sort((a, b) => a - b);
    const valsY = [...new Set(scored.map(c => (c.param_combo || {})[keyY]))].sort((a, b) => a - b);

    const heatData = [];
    const scoreMap = {};
    for (const c of scored) {
      const xVal = (c.param_combo || {})[keyX];
      const yVal = (c.param_combo || {})[keyY];
      const xi = valsX.indexOf(xVal);
      const yi = valsY.indexOf(yVal);
      if (xi >= 0 && yi >= 0) {
        heatData.push([xi, yi, c._score]);
        scoreMap[`${xi},${yi}`] = c;
      }
    }

    const allScores = scored.map(c => c._score);
    const minScore = Math.min(...allScores);
    const maxScore = Math.max(...allScores);

    chart.setOption({
      backgroundColor: "transparent",
      tooltip: {
        formatter: function (params) {
          const [xi, yi, score] = params.data;
          const c = scoreMap[`${xi},${yi}`];
          if (!c) return "";
          return `<b>${shortX}=${valsX[xi]}, ${shortY}=${valsY[yi]}</b><br/>综合评分: <b>${score.toFixed(2)}</b><br/>命中: ${c.total_hits}`;
        },
      },
      grid: { top: 10, bottom: 40, left: 80, right: 50 },
      xAxis: {
        type: "category",
        data: valsX.map(v => typeof v === "number" ? (Number.isInteger(v) ? String(v) : v.toFixed(2)) : String(v)),
        name: shortX,
        nameLocation: "center",
        nameGap: 28,
        splitArea: { show: true },
        axisLabel: { fontSize: 10 },
      },
      yAxis: {
        type: "category",
        data: valsY.map(v => typeof v === "number" ? (Number.isInteger(v) ? String(v) : v.toFixed(2)) : String(v)),
        name: shortY,
        nameLocation: "center",
        nameGap: 55,
        splitArea: { show: true },
        axisLabel: { fontSize: 10 },
      },
      visualMap: {
        min: minScore === maxScore ? minScore - 1 : minScore,
        max: minScore === maxScore ? maxScore + 1 : maxScore,
        calculable: true,
        orient: "vertical",
        right: 0,
        top: 10,
        inRange: { color: ["#fc8181", "#fbd38d", "#68d391"] },
        textStyle: { fontSize: 10 },
      },
      series: [{
        type: "heatmap",
        data: heatData,
        label: {
          show: valsX.length * valsY.length <= 100,
          fontSize: 10,
          formatter: ({ data }) => data[2].toFixed(1),
        },
        emphasis: {
          itemStyle: { shadowBlur: 8, shadowColor: "rgba(0,0,0,0.5)" },
        },
      }],
    });
  }

  // ── 参数快照弹窗 ──
  const _btParamCache = {};
  const _BT_MODE_TEXT = { fixed: "固定参数", sweep: "参数扫描" };

  async function loadBtJobParams(jobId) {
    if (_btParamCache[jobId]) return _btParamCache[jobId];
    const data = await getJSON(`/api/backtests/${jobId}/params`);
    _btParamCache[jobId] = data;
    return data;
  }

  function renderBtParamMeta(data) {
    const esc = ParamRender.escapeHtml;
    const parts = [];
    if (data.strategy_name) parts.push(esc(data.strategy_name));
    const modeLabel = _BT_MODE_TEXT[data.mode] || data.mode || "";
    if (modeLabel) parts.push(esc(modeLabel));
    if (data.forward_bars && data.forward_bars.length === 3) {
      parts.push(esc(`前瞻: ${data.forward_bars.join("/")} bar`));
    }
    if (data.slide_step) parts.push(esc(`步进: ${data.slide_step}`));
    if (!parts.length) return "";
    return `<div class="param-modal-meta">${parts.join(" · ")}</div>`;
  }

  function renderBtOverview(paramHelp) {
    if (!paramHelp || !paramHelp._overview) return "";
    const text = ParamRender.helpTextFromNode({ _overview: paramHelp._overview });
    if (!text) return "";
    const lines = text.split("\n").map((l) => `<p>${ParamRender.escapeHtml(l)}</p>`).join("");
    return `<div class="param-modal-overview">${lines}</div>`;
  }

  function renderBtReadonlyParams(data) {
    const groupParams = data.group_params;
    const paramHelp = data.param_help;
    if (!groupParams || !Object.keys(groupParams).length) {
      return '<div class="param-modal-empty">无参数数据</div>';
    }
    // 从当前 manifest 注入 scope_params 声明（向后兼容旧任务快照）
    const group = findGroup(data.strategy_group_id);
    if (group) {
      const dp = group.default_params || {};
      for (const [sKey, sec] of Object.entries(dp)) {
        if (sec && typeof sec === "object" && Array.isArray(sec.scope_params)
            && groupParams[sKey] && typeof groupParams[sKey] === "object") {
          groupParams[sKey].scope_params = sec.scope_params;
        }
      }
    }
    const meta = renderBtParamMeta(data);
    const overview = renderBtOverview(paramHelp);
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

  function openBtParamModal() {
    const jobId = state.currentJobId || $("btJobSelect").value;
    if (!jobId) return;
    const overlay = $("btParamModalOverlay");
    const body = $("btParamModalBody");
    overlay.hidden = false;
    body.innerHTML = '<div class="param-modal-loading">加载中…</div>';

    loadBtJobParams(jobId).then((data) => {
      body.innerHTML = renderBtReadonlyParams(data);
      if (data.mode === "sweep") injectReadonlyRanges(body, data.param_ranges);
    }).catch((err) => {
      body.innerHTML = `<div class="param-modal-empty">加载失败: ${ParamRender.escapeHtml(err.message || "未知错误")}</div>`;
    });
  }

  function closeBtParamModal() {
    $("btParamModalOverlay").hidden = true;
    $("btParamModalBody").innerHTML = "";
  }

  function bindBtParamModal() {
    const btn = $("btShowParamsBtn");
    if (btn) btn.addEventListener("click", openBtParamModal);
    const closeBtn = $("btParamModalCloseBtn");
    if (closeBtn) closeBtn.addEventListener("click", closeBtParamModal);
    const overlay = $("btParamModalOverlay");
    if (overlay) {
      overlay.addEventListener("click", (e) => {
        if (e.target === overlay) closeBtParamModal();
      });
    }
    window.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && overlay && !overlay.hidden) closeBtParamModal();
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
