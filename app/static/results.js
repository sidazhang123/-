const state = {
  taskId: null,
  eventSource: null,
  heartbeatSource: null,
  pollInFlight: false,
  chart: null,
  selectedCode: null,
  selectedName: "",
  resultStocks: [],
  lastResultCount: -1,
  chartTf: "d",
  currentTask: null,
  initialZoomRange: null,
  isChartFullscreen: false,
  isSwitchingStock: false,
  chartLoadSeq: 0,
  zoomLocks: {},
  // lazy-load state
  loadedCandles: [],
  chartIntervals: [],
  chartSignals: [],
  chartMeta: null,
  loadedStartTs: null,
  loadedEndTs: null,
  isLoadingMore: false,
  hasMoreBefore: true,
  hasMoreAfter: true,
  conceptCacheByTask: {},
  conceptFetchPromiseByTask: {},
  tooltipAnchorCode: null,
  tooltipAnchorEl: null,
  tooltipHideTimer: null,
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

const SHARED_TASK_KEY = "screening:selected_task_id";
const CHART_ZOOM_LOCKS_KEY = "screening:results:chart_zoom_locks";
const CHART_LEGEND_KEY = "screening:results:chart_legend_selected";
const SYNC_CHANNEL_NAME = "screening-task-sync";
const TAB_ID = `results-${Date.now()}-${Math.random().toString(16).slice(2)}`;
const syncChannel = "BroadcastChannel" in window ? new BroadcastChannel(SYNC_CHANNEL_NAME) : null;
const CHART_TIMEFRAMES = new Set(["15", "30", "60", "d", "w"]);

function setStreamState(text, kind) {
  const el = $("resultsStreamState");
  if (!el) return;
  el.textContent = text;
  el.className = `stream-state ${kind}`;
}

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

function formatDate(v) {
  if (!v) return "-";
  const d = new Date(v);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function parseTaskIdFromUrl() {
  const params = new URLSearchParams(window.location.search);
  return params.get("task_id");
}

function writeTaskIdToUrl(taskId) {
  const url = new URL(window.location.href);
  if (taskId) {
    url.searchParams.set("task_id", taskId);
  } else {
    url.searchParams.delete("task_id");
  }
  window.history.replaceState({}, "", url.toString());
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

async function ensureEcharts() {
  if (!window.echarts) {
    throw new Error("ECharts 本地资源未加载，请检查 /static/vendor/echarts/echarts.min.js");
  }
}

function normalizeZoomLocks(rawValue) {
  if (!rawValue || typeof rawValue !== "object") return {};
  const result = {};
  for (const [timeframe, snapshot] of Object.entries(rawValue)) {
    if (!CHART_TIMEFRAMES.has(timeframe) || !snapshot || typeof snapshot !== "object") {
      continue;
    }
    const candleCount = Math.max(1, Math.round(Number(snapshot.candleCount) || 0));
    if (!Number.isFinite(candleCount) || candleCount <= 0) {
      continue;
    }
    result[timeframe] = {
      timeframe,
      candleCount,
    };
  }
  return result;
}

function loadZoomLocksFromStorage() {
  try {
    const raw = localStorage.getItem(CHART_ZOOM_LOCKS_KEY);
    if (!raw) return {};
    return normalizeZoomLocks(JSON.parse(raw));
  } catch {
    return {};
  }
}

function persistZoomLocks() {
  try {
    localStorage.setItem(CHART_ZOOM_LOCKS_KEY, JSON.stringify(state.zoomLocks));
  } catch {
    // ignore storage errors
  }
}

function loadLegendSelected() {
  try {
    const raw = localStorage.getItem(CHART_LEGEND_KEY);
    if (!raw) return null;
    const obj = JSON.parse(raw);
    return (obj && typeof obj === 'object') ? obj : null;
  } catch { return null; }
}

function persistLegendSelected(selected) {
  try {
    localStorage.setItem(CHART_LEGEND_KEY, JSON.stringify(selected));
  } catch {}
}

function renderMeta(task, totalStocks) {
  state.currentTask = task;
  const conceptFormula = getCurrentConceptFormula();
  const conceptSummary = isConceptFormulaActive(conceptFormula)
    ? ` | 概念公式: 概念包含 ${formatFormulaTerms(conceptFormula.concept_terms, " & ")} 且 理由包含 ${formatFormulaTerms(conceptFormula.reason_terms, " or ")}`
    : "";
  $("resultMeta").textContent =
    `任务: ${task.task_id} | 状态: ${STATUS_TEXT[task.status] || task.status} | ` +
    `策略组: ${task.strategy_name || "-"} | 命中总条数: ${task.result_count} | ` +
    `命中股票数(去重): ${totalStocks} | 进度: ${task.processed_stocks}/${task.total_stocks} (${task.progress.toFixed(2)}%)`;
  $("resultStrategyDesc").textContent = `策略说明: ${task.strategy_description || "-"}${conceptSummary}`;
  renderTopConceptSummary();
}

function getConceptTooltipElement() {
  return $("conceptTooltip");
}

function clearConceptTooltipHideTimer() {
  if (state.tooltipHideTimer) {
    window.clearTimeout(state.tooltipHideTimer);
    state.tooltipHideTimer = null;
  }
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeRegExp(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function getCurrentConceptPayload() {
  return state.conceptCacheByTask[state.taskId] || { formula: {}, topConcepts: [], items: {} };
}

function getCurrentConceptFormula() {
  const payload = getCurrentConceptPayload();
  return payload.formula || {};
}

function isConceptFormulaActive(formula) {
  const conceptTerms = Array.isArray(formula?.concept_terms) ? formula.concept_terms.filter(Boolean) : [];
  const reasonTerms = Array.isArray(formula?.reason_terms) ? formula.reason_terms.filter(Boolean) : [];
  return Boolean(formula?.enabled) && (conceptTerms.length > 0 || reasonTerms.length > 0);
}

function formatFormulaTerms(terms, separator) {
  const items = Array.isArray(terms) ? terms.filter(Boolean) : [];
  return items.length ? items.join(separator) : "-";
}

function renderTopConceptSummary() {
  const el = $("resultConceptTop3");
  if (!el) return;
  if (!state.taskId) {
    el.textContent = "概念集中度 Top3: -";
    return;
  }
  const payload = getCurrentConceptPayload();
  const topConcepts = Array.isArray(payload.topConcepts) ? payload.topConcepts : [];
  if (!topConcepts.length) {
    el.textContent = "概念集中度 Top3: -";
    return;
  }
  const text = topConcepts
    .map((item) => {
      const hitCount = Number(item.hit_stock_count || 0);
      const totalHitStocks = Number(item.total_hit_stocks || 0);
      const ratio = totalHitStocks > 0 ? Math.round(hitCount / totalHitStocks * 100) : 0;
      return `${item.board_name || "-"} ${hitCount}/${totalHitStocks} (${ratio}%)`;
    })
    .join(" · ");
  el.textContent = `概念集中度 Top3: ${text}`;
}

function highlightTerms(text, terms) {
  const safeText = escapeHtml(text);
  const normalizedTerms = Array.isArray(terms) ? terms.filter(Boolean).sort((a, b) => String(b).length - String(a).length) : [];
  if (!normalizedTerms.length) return safeText;
  const pattern = normalizedTerms.map((term) => escapeRegExp(escapeHtml(term))).join("|");
  if (!pattern) return safeText;
  return safeText.replace(new RegExp(`(${pattern})`, "gi"), '<span class="concept-hit">$1</span>');
}

function buildConceptTooltipHtml(code) {
  const payload = getCurrentConceptPayload();
  const entries = Array.isArray(payload.items?.[code]) ? payload.items[code] : [];
  const formula = payload.formula || {};
  const formulaActive = isConceptFormulaActive(formula);
  const conceptTerms = Array.isArray(formula.concept_terms) ? formula.concept_terms : [];
  const reasonTerms = Array.isArray(formula.reason_terms) ? formula.reason_terms : [];

  if (!entries.length) {
    return '<div class="concept-tooltip-empty">暂无概念数据</div>';
  }

  const summaryHtml = formulaActive
    ? `<div class="concept-tooltip-formula">概念包含 ${highlightTerms(formatFormulaTerms(conceptTerms, " & "), conceptTerms)}；理由包含 ${highlightTerms(formatFormulaTerms(reasonTerms, " or "), reasonTerms)}</div>`
    : "";

  const conceptLabels = entries.map((entry) => escapeHtml(entry.board_name || "")).filter(Boolean);
  const uniqueConceptLabels = Array.from(new Set(conceptLabels));
  const updatedAtCandidates = entries
    .map((entry) => Number(new Date(entry.updated_at || 0).getTime()))
    .filter((value) => Number.isFinite(value) && value > 0);
  const latestUpdatedAt = updatedAtCandidates.length
    ? new Date(Math.max(...updatedAtCandidates)).toLocaleString()
    : "-";
  const labelsHtml = uniqueConceptLabels.length
    ? `<div class="concept-tooltip-labels">${uniqueConceptLabels.map((name) => `<span class="concept-pill">${name}</span>`).join("")}</div>`
    : "";
  const metaHtml = `<div class="concept-tooltip-meta">更新时间: ${escapeHtml(latestUpdatedAt)}</div>`;

  const itemsHtml = entries.map((entry) => {
    const boardName = formulaActive ? highlightTerms(entry.board_name || "", conceptTerms) : escapeHtml(entry.board_name || "");
    const rawReason = String(entry.selected_reason || "").trim();
    const reason = formulaActive ? highlightTerms(rawReason, reasonTerms) : escapeHtml(rawReason);
    return `
      <div class="concept-tooltip-item">
        <div class="concept-tooltip-board">${boardName || "-"}</div>
        ${reason ? `<div class="concept-tooltip-reason">${reason}</div>` : ""}
      </div>
    `;
  }).join("");

  return `
    <div class="concept-tooltip-header">
      ${labelsHtml}
      ${metaHtml}
      ${summaryHtml}
    </div>
    <div class="concept-tooltip-body">${itemsHtml}</div>
  `;
}

function positionConceptTooltip(anchorEl) {
  const tooltip = getConceptTooltipElement();
  if (!tooltip || !anchorEl || tooltip.hidden) return;
  const rect = anchorEl.getBoundingClientRect();
  const tooltipRect = tooltip.getBoundingClientRect();
  const top = Math.max(12, rect.top + window.scrollY - 4);
  let left = rect.right + window.scrollX + 10;
  if (left + tooltipRect.width > window.scrollX + window.innerWidth - 12) {
    left = rect.left + window.scrollX - tooltipRect.width - 10;
  }
  tooltip.style.top = `${top}px`;
  tooltip.style.left = `${Math.max(12, left)}px`;
}

function scheduleHideConceptTooltip() {
  clearConceptTooltipHideTimer();
  state.tooltipHideTimer = window.setTimeout(() => {
    hideConceptTooltip();
  }, 160);
}

function shouldKeepConceptTooltipOnScroll(event) {
  const tooltip = getConceptTooltipElement();
  if (!tooltip || tooltip.hidden) return false;
  const target = event?.target;
  return target === tooltip || (target instanceof Node && tooltip.contains(target));
}

function hideConceptTooltip() {
  clearConceptTooltipHideTimer();
  const tooltip = getConceptTooltipElement();
  if (!tooltip) return;
  tooltip.hidden = true;
  tooltip.innerHTML = "";
  state.tooltipAnchorCode = null;
  state.tooltipAnchorEl = null;
}

async function ensureResultConceptsLoaded() {
  const taskId = state.taskId;
  if (!taskId) return getCurrentConceptPayload();
  if (state.conceptCacheByTask[taskId]) {
    return state.conceptCacheByTask[taskId];
  }
  if (!state.conceptFetchPromiseByTask[taskId]) {
    state.conceptFetchPromiseByTask[taskId] = getJSON(`/api/tasks/${taskId}/result-stock-concepts`)
      .then((data) => {
        state.conceptCacheByTask[taskId] = {
          formula: data.formula || {},
          topConcepts: data.top_concepts || [],
          items: data.items || {},
        };
        if (state.taskId === taskId) {
          renderTopConceptSummary();
        }
        return state.conceptCacheByTask[taskId];
      })
      .finally(() => {
        delete state.conceptFetchPromiseByTask[taskId];
      });
  }
  return state.conceptFetchPromiseByTask[taskId];
}

async function showConceptTooltip(code, anchorEl) {
  if (!code || !anchorEl) return;
  const tooltip = getConceptTooltipElement();
  clearConceptTooltipHideTimer();
  state.tooltipAnchorCode = code;
  state.tooltipAnchorEl = anchorEl;
  tooltip.hidden = false;
  tooltip.innerHTML = '<div class="concept-tooltip-loading">概念数据加载中...</div>';
  positionConceptTooltip(anchorEl);

  try {
    await ensureResultConceptsLoaded();
    if (state.tooltipAnchorCode !== code) return;
    tooltip.innerHTML = buildConceptTooltipHtml(code);
    tooltip.hidden = false;
    positionConceptTooltip(state.tooltipAnchorEl || anchorEl);
  } catch (err) {
    if (state.tooltipAnchorCode !== code) return;
    tooltip.innerHTML = `<div class="concept-tooltip-empty">加载失败: ${escapeHtml(err.message || "未知错误")}</div>`;
    tooltip.hidden = false;
    positionConceptTooltip(state.tooltipAnchorEl || anchorEl);
  }
}

function renderResultStocks(items) {
  state.resultStocks = items.slice();
  const tbody = $("resultStocksTable").querySelector("tbody");
  tbody.innerHTML = "";
  for (const row of items) {
    const tr = document.createElement("tr");
    tr.dataset.stockCode = row.code;
    tr.classList.toggle("active-row", row.code === state.selectedCode);
    tr.innerHTML = `
      <td style="white-space:nowrap">
        <div class="result-stock-name-cell">
          <span>${row.name || row.code}</span>
          <button type="button" class="concept-badge-btn" data-concept-code="${row.code}" title="查看概念与入选理由">概</button>
        </div>
      </td>
      <td>${row.signal_count}</td>
      <td>${formatDate(row.first_signal_dt)}</td>
      <td>${formatDate(row.last_signal_dt)}</td>
    `;
    tr.addEventListener("click", () => {
      selectStock(row.code, row.name || "", { scrollIntoView: false }).catch((err) => alert(err.message));
    });
    const conceptBtn = tr.querySelector(".concept-badge-btn");
    conceptBtn.addEventListener("click", (event) => {
      event.stopPropagation();
    });
    conceptBtn.addEventListener("mouseenter", () => {
      showConceptTooltip(row.code, conceptBtn).catch((err) => console.error("加载概念气泡失败", err));
    });
    conceptBtn.addEventListener("mousemove", () => {
      positionConceptTooltip(conceptBtn);
    });
    conceptBtn.addEventListener("mouseleave", () => {
      scheduleHideConceptTooltip();
    });
    tbody.appendChild(tr);
  }
}

function getChartDisplayName() {
  return state.selectedName || state.chartMeta?.name || state.selectedCode || "";
}

function getZoomLockSnapshot(tf = state.chartTf) {
  return state.zoomLocks[tf] || null;
}

function hasZoomLock(tf = state.chartTf) {
  return Boolean(getZoomLockSnapshot(tf));
}

function updateChartTitles() {
  const displayName = getChartDisplayName();
  $("chartTitle").textContent = displayName ? `${displayName} K线图` : "K线图";
  $("chartFullscreenTitle").textContent = displayName || "K线图";
}

function updateZoomLockButton() {
  const button = $("toggleZoomLockBtn");
  const label = $("toggleZoomLockLabel");
  const locked = hasZoomLock();
  const canLock = state.loadedCandles.length > 0;
  button.classList.toggle("is-locked", locked);
  button.disabled = !canLock;
  button.title = locked ? "解锁当前周期显示根数" : "锁定当前周期显示根数";
  label.textContent = locked ? "解锁视图" : "锁定视图";
}

function updateSelectedStockRow(scrollIntoView = false) {
  const rows = $("resultStocksTable").querySelectorAll("tbody tr");
  rows.forEach((row) => {
    const isSelected = row.dataset.stockCode === state.selectedCode;
    row.classList.toggle("active-row", isSelected);
    if (isSelected && scrollIntoView) {
      row.scrollIntoView({ block: "nearest" });
    }
  });
}

async function selectStock(code, name, options = {}) {
  if (!code) return;
  state.selectedCode = code;
  state.selectedName = name || "";
  updateSelectedStockRow(Boolean(options.scrollIntoView));
  updateChartTitles();
  if (options.loadChart === false) return;

  // 自动切换到信号的主周期，避免周线信号显示日线 K 线图
  const stockItem = state.resultStocks.find((s) => s.code === code);
  if (stockItem && stockItem.clock_tf && stockItem.clock_tf !== state.chartTf) {
    setChartTf(stockItem.clock_tf, false);
  }

  await loadChart();
}

function getSelectedStockIndex() {
  return state.resultStocks.findIndex((item) => item.code === state.selectedCode);
}

async function switchSelectedStock(offset) {
  if (state.isSwitchingStock || !state.resultStocks.length) return;

  const currentIndex = getSelectedStockIndex();
  const baseIndex = currentIndex >= 0 ? currentIndex : (offset > 0 ? -1 : state.resultStocks.length);
  const nextIndex = Math.max(0, Math.min(state.resultStocks.length - 1, baseIndex + offset));
  if (nextIndex === currentIndex) return;

  state.isSwitchingStock = true;
  try {
    const nextRow = state.resultStocks[nextIndex];
    await selectStock(nextRow.code, nextRow.name || "", { scrollIntoView: true });
  } finally {
    state.isSwitchingStock = false;
  }
}

async function exitChartFullscreen() {
  if (document.fullscreenElement === $("chartStage")) {
    await document.exitFullscreen();
  }
}

async function toggleChartFullscreen() {
  const chartStage = $("chartStage");
  if (!document.fullscreenEnabled) {
    alert("当前浏览器不支持全屏模式。");
    return;
  }
  if (document.fullscreenElement === chartStage) {
    await document.exitFullscreen();
    return;
  }
  await chartStage.requestFullscreen();
}

function syncChartFullscreenState() {
  const chartStage = $("chartStage");
  const isFullscreen = document.fullscreenElement === chartStage;
  state.isChartFullscreen = isFullscreen;
  chartStage.classList.toggle("is-chart-fullscreen", isFullscreen);
  $("toggleFullscreenBtn").textContent = isFullscreen ? "退出全屏" : "全屏查看";
  updateChartTitles();
  updateZoomLockButton();
  if (state.chart) {
    window.setTimeout(() => state.chart.resize(), 0);
  }
}

function bindFullscreenEvents() {
  document.addEventListener("fullscreenchange", syncChartFullscreenState);
  document.addEventListener("keydown", (event) => {
    if (event.key === "ArrowUp" && state.resultStocks.length) {
      event.preventDefault();
      switchSelectedStock(-1).catch((err) => alert(err.message));
      return;
    }
    if (event.key === "ArrowDown" && state.resultStocks.length) {
      event.preventDefault();
      switchSelectedStock(1).catch((err) => alert(err.message));
      return;
    }
    if (event.key === "Escape" && state.isChartFullscreen) {
      event.preventDefault();
      exitChartFullscreen().catch((err) => console.error("退出全屏失败", err));
    }
  }, true);
}

function getCurrentZoomRange() {
  if (!state.chart || !state.loadedCandles.length) return null;
  const opt = state.chart.getOption();
  const dz = opt?.dataZoom?.[0];
  if (!dz) return null;
  const maxIndex = state.loadedCandles.length - 1;
  const startRaw = Number.isFinite(Number(dz.startValue)) ? Number(dz.startValue) : 0;
  const endRaw = Number.isFinite(Number(dz.endValue)) ? Number(dz.endValue) : maxIndex;
  const left = Math.max(0, Math.min(Math.round(startRaw), maxIndex));
  const right = Math.max(left, Math.min(Math.round(endRaw), maxIndex));
  return { left, right, count: right - left + 1 };
}

function buildLockedZoomRange(totalCount, lockedCount) {
  if (!Number.isFinite(totalCount) || totalCount <= 0) return null;
  const visibleCount = Math.max(1, Math.min(totalCount, Math.round(Number(lockedCount) || 0) || totalCount));
  return {
    left: Math.max(0, totalCount - visibleCount),
    right: totalCount - 1,
  };
}

function needsLockedZoomBackfill(tf = state.chartTf) {
  const lockSnapshot = getZoomLockSnapshot(tf);
  if (!lockSnapshot) return false;
  return state.loadedCandles.length < lockSnapshot.candleCount && state.hasMoreBefore;
}

function applyLockedZoomRange(tf = state.chartTf) {
  const lockSnapshot = getZoomLockSnapshot(tf);
  if (!lockSnapshot) return false;
  const zoomRange = buildLockedZoomRange(state.loadedCandles.length, lockSnapshot.candleCount);
  if (!zoomRange) return false;
  applyAutoZoomRange(zoomRange);
  return true;
}

function scheduleLockedZoomBackfill(tf = state.chartTf) {
  if (!needsLockedZoomBackfill(tf) || state.isLoadingMore) return;
  window.setTimeout(() => {
    if (!needsLockedZoomBackfill(tf) || state.isLoadingMore || tf !== state.chartTf) {
      return;
    }
    loadMoreCandles("before", { preserveVisible: false, trigger: "zoom-lock" }).catch((err) => {
      console.error("锁定视图补齐K线失败", err);
    });
  }, 0);
}

function getPreferredZoomRange(timeMs, intervals, signals) {
  const lockSnapshot = getZoomLockSnapshot();
  if (lockSnapshot) {
    return buildLockedZoomRange(timeMs.length, lockSnapshot.candleCount);
  }
  return computeAutoZoomRange(timeMs, intervals, signals);
}

function toggleZoomLock() {
  const tf = state.chartTf;
  if (hasZoomLock(tf)) {
    delete state.zoomLocks[tf];
    persistZoomLocks();
    updateZoomLockButton();
    return;
  }

  const zoomRange = getCurrentZoomRange();
  if (!zoomRange) return;
  state.zoomLocks[tf] = {
    timeframe: tf,
    candleCount: zoomRange.count,
  };
  persistZoomLocks();
  updateZoomLockButton();
}

function initChart() {
  state.chart = echarts.init($('chart'), 'dark');
  state.chart.setOption({
    animation: false,
    backgroundColor: 'transparent',
    title: { text: '' },
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(13,17,23,0.92)',
      borderColor: 'rgba(0,180,255,0.2)',
      textStyle: { color: '#e2e8f0' },
      formatter: function (params) {
        if (!params || !params.length) return '';
        const kParam = params.find(p => p.seriesName === 'K线');
        if (!kParam || !kParam.data) return '';
        const dataIndex = kParam.dataIndex;
        const candle = dataIndex >= 0 ? state.loadedCandles[dataIndex] : null;
        if (!candle) return '';
        const open = Number(candle.open);
        const high = Number(candle.high);
        const low = Number(candle.low);
        const close = Number(candle.close);
        if (![open, high, low, close].every(Number.isFinite)) return '';
        let dtLabel = '';
        if (candle.datetime) {
          const d = new Date(candle.datetime);
          const yy = String(d.getFullYear()).slice(2);
          const mm = String(d.getMonth() + 1).padStart(2, '0');
          const dd = String(d.getDate()).padStart(2, '0');
          if (['15', '30', '60'].includes(state.chartTf)) {
            const HH = String(d.getHours()).padStart(2, '0');
            const MM = String(d.getMinutes()).padStart(2, '0');
            dtLabel = `${yy}/${mm}/${dd} ${HH}:${MM}`;
          } else {
            dtLabel = `${yy}/${mm}/${dd}`;
          }
        }
        const prevCandle = dataIndex > 0 ? state.loadedCandles[dataIndex - 1] : null;
        const preClose = prevCandle ? Number(prevCandle.close) : Number.NaN;
        let changePct = '';
        if (Number.isFinite(preClose) && preClose > 0) {
          const pctValue = (close - preClose) / preClose * 100;
          const pct = pctValue.toFixed(2);
          const color = pctValue >= 0 ? '#ef4444' : '#10b981';
          changePct = `<br/>涨幅: <span style="color:${color}">${pct}%</span>`;
        }
        return `<div style="font-size:12px;line-height:1.5;">
          ${dtLabel ? dtLabel + '<br/>' : ''}
          开盘: ${open.toFixed(2)}<br/>
          收盘: ${close.toFixed(2)}<br/>
          最低: ${low.toFixed(2)}<br/>
          最高: ${high.toFixed(2)}${changePct}
        </div>`;
      },
    },
    axisPointer: { link: [{ xAxisIndex: 'all' }] },
    legend: {
      show: true,
      left: 10,
      bottom: '12%',
      orient: 'vertical',
      padding: [4, 8],
      backgroundColor: 'rgba(13,17,23,0.7)',
      borderRadius: 4,
      textStyle: { color: '#ccc', fontSize: 11 },
      itemWidth: 18,
      itemHeight: 2,
      selectedMode: true,
      selected: loadLegendSelected() || { 'MA10': true, 'MA20': true },
      data: [
        { name: 'MA10', icon: 'roundRect', textStyle: { color: '#ffffff' } },
        { name: 'MA20', icon: 'roundRect', textStyle: { color: '#fbbf24' } },
      ],
    },
    grid: [
      { left: '8%', right: '4%', top: 48, height: '56%' },
      { left: '8%', right: '4%', top: '70%', height: '18%' },
    ],
    xAxis: [
      { type: 'category', data: [], scale: true, boundaryGap: false, axisLine: { lineStyle: { color: '#2a3a52' } }, axisLabel: { color: '#64748b' } },
      { type: 'category', data: [], gridIndex: 1, scale: true, boundaryGap: false, axisLine: { lineStyle: { color: '#2a3a52' } }, axisLabel: { show: false } },
    ],
    yAxis: [
      { scale: true, splitLine: { lineStyle: { color: 'rgba(56,82,120,0.2)' } }, axisLabel: { color: '#64748b' } },
      { gridIndex: 1, scale: true, splitLine: { lineStyle: { color: 'rgba(56,82,120,0.2)' } }, axisLabel: { show: false }, axisTick: { show: false } },
    ],
    dataZoom: [
      { id: 'kline-inside-zoom', type: 'inside', xAxisIndex: [0, 1], start: 70, end: 100, moveOnMouseMove: true },
      { id: 'kline-slider-zoom', type: 'slider', xAxisIndex: [0, 1], top: '91%', start: 70, end: 100,
        backgroundColor: 'rgba(13,17,23,0.6)', borderColor: 'transparent',
        fillerColor: 'rgba(0,180,255,0.12)',
        handleStyle: { color: '#00b4ff' },
        textStyle: { color: '#64748b' },
      },
    ],
    series: [
      { name: 'K线', type: 'candlestick', data: [],
        itemStyle: { color: '#ef4444', color0: '#10b981', borderColor: '#ef4444', borderColor0: '#10b981' },
      },
      { name: '成交量', type: 'bar', xAxisIndex: 1, yAxisIndex: 1, data: [] },
      { name: 'MA10', type: 'line', data: [], smooth: false, symbol: 'none',
        lineStyle: { width: 1, color: '#ffffff' },
        itemStyle: { color: '#ffffff' },
      },
      { name: 'MA20', type: 'line', data: [], smooth: false, symbol: 'none',
        lineStyle: { width: 2, color: '#fbbf24' },
        itemStyle: { color: '#fbbf24' },
      },
    ],
  });

  window.addEventListener("resize", () => state.chart.resize());

  // Persist legend visibility on toggle
  state.chart.on('legendselectchanged', function (params) {
    persistLegendSelected(params.selected);
  });

  // Lazy-load trigger on zoom/drag
  state.chart.on("dataZoom", () => {
    clearTimeout(_lazyLoadTimer);
    _lazyLoadTimer = setTimeout(onDataZoomCheck, 300);
  });

  // Shift+拖动显示两点间涨跌幅
  let shiftDragStart = null;
  let shiftDragEnd = null;
  let isShiftPressed = false;
  let isDragging = false;

  const zr = state.chart.getZr();

  // 根据是否进入 Shift 测量模式，切换 inside dataZoom 的拖动行为。
  function setShiftMeasureMode(enabled) {
    if (!state.chart) return;
    state.chart.setOption({
      dataZoom: [{
        id: 'kline-inside-zoom',
        moveOnMouseMove: !enabled,
      }],
    });
  }

  // 按像素位置取最近的 K 线索引，避免 convertFromPixel 在类目轴上返回不稳定值。
  function getNearestCandleIndexByPixel(offsetX) {
    if (!state.chart || !state.loadedCandles.length) return -1;

    const opt = state.chart.getOption();
    const xAxisData = opt?.xAxis?.[0]?.data || [];
    if (!xAxisData.length) return -1;

    const dz = opt?.dataZoom?.[0] || null;
    const rangeStart = Number.isFinite(Number(dz?.startValue)) ? Math.max(0, Math.floor(Number(dz.startValue))) : 0;
    const rangeEnd = Number.isFinite(Number(dz?.endValue))
      ? Math.min(xAxisData.length - 1, Math.ceil(Number(dz.endValue)))
      : xAxisData.length - 1;

    let nearestIndex = -1;
    let nearestDistance = Number.POSITIVE_INFINITY;
    for (let index = rangeStart; index <= rangeEnd; index += 1) {
      const pixelX = state.chart.convertToPixel({ xAxisIndex: 0 }, xAxisData[index]);
      const distance = Math.abs(pixelX - offsetX);
      if (distance < nearestDistance) {
        nearestDistance = distance;
        nearestIndex = index;
      }
    }
    return nearestIndex;
  }

  // 从鼠标事件中提取测量点，价格取当前像素所在的主图 y 轴值。
  function getShiftDragPoint(event) {
    if (!state.chart) return null;
    const pixel = [event.offsetX, event.offsetY];
    if (!state.chart.containPixel({ gridIndex: 0 }, pixel)) return null;

    const gridPoint = state.chart.convertFromPixel({ gridIndex: 0 }, pixel);
    const dataIndex = getNearestCandleIndexByPixel(event.offsetX);
    const price = Array.isArray(gridPoint) ? Number(gridPoint[1]) : Number.NaN;

    if (dataIndex < 0 || !Number.isFinite(price) || price <= 0) {
      return null;
    }

    return {
      x: event.offsetX,
      y: event.offsetY,
      dataIndex,
      price,
    };
  }

  // 阻止 Shift 测量与图表自身拖动手势同时生效。
  function suppressPointerEvent(event) {
    if (!event) return;
    if (typeof event.stop === 'function') {
      event.stop();
    }
    if (typeof event.preventDefault === 'function') {
      event.preventDefault();
    }
    const nativeEvent = event.event;
    if (nativeEvent && typeof nativeEvent.stopPropagation === 'function') {
      nativeEvent.stopPropagation();
    }
    if (nativeEvent && typeof nativeEvent.preventDefault === 'function') {
      nativeEvent.preventDefault();
    }
  }

  // 按线段方向生成箭头三角形，避免箭头固定朝右导致拐弯。
  function buildArrowHeadPoints(startPoint, endPoint, size = 10) {
    const deltaX = endPoint.x - startPoint.x;
    const deltaY = endPoint.y - startPoint.y;
    const length = Math.hypot(deltaX, deltaY);
    if (!Number.isFinite(length) || length < 1) return null;

    const unitX = deltaX / length;
    const unitY = deltaY / length;
    const perpX = -unitY;
    const perpY = unitX;
    const baseX = endPoint.x - unitX * size;
    const baseY = endPoint.y - unitY * size;
    const wingSize = size * 0.55;

    return [
      [endPoint.x, endPoint.y],
      [baseX + perpX * wingSize, baseY + perpY * wingSize],
      [baseX - perpX * wingSize, baseY - perpY * wingSize],
    ];
  }

  function clearShiftDragState() {
    isDragging = false;
    shiftDragStart = null;
    shiftDragEnd = null;
    updateShiftDragGraphic();
  }

  function updateShiftPressedState(pressed) {
    isShiftPressed = pressed;
    setShiftMeasureMode(pressed);
    if (!pressed && isDragging) {
      clearShiftDragState();
    }
  }

  function isShiftGestureActive(event) {
    const nativeEvent = event?.event || event;
    return Boolean(isShiftPressed || nativeEvent?.shiftKey);
  }

  window.addEventListener("keydown", (e) => {
    if (e.key === "Shift") {
      updateShiftPressedState(true);
    }
  }, true);

  window.addEventListener("keyup", (e) => {
    if (e.key === "Shift") {
      updateShiftPressedState(false);
      if (!isDragging) {
        shiftDragStart = null;
        shiftDragEnd = null;
        updateShiftDragGraphic();
      }
    }
  }, true);

  window.addEventListener("blur", () => {
    updateShiftPressedState(false);
  });

  zr.on("mousedown", function (e) {
    if (!isShiftGestureActive(e.event) || e.event.button !== 0) return;
    updateShiftPressedState(true);
    suppressPointerEvent(e.event);
    const point = getShiftDragPoint(e);
    if (point) {
      shiftDragStart = point;
      shiftDragEnd = null;
      isDragging = true;
      updateShiftDragGraphic();
    }
  });

  zr.on("mousemove", function (e) {
    if (!isDragging || !shiftDragStart) return;
    if (!isShiftGestureActive(e.event)) {
      updateShiftPressedState(false);
      return;
    }
    suppressPointerEvent(e.event);
    const point = getShiftDragPoint(e);
    if (point) {
      shiftDragEnd = point;
      updateShiftDragGraphic();
    }
  });

  zr.on("mouseup", function (e) {
    if (isDragging) {
      suppressPointerEvent(e.event);
      clearShiftDragState();
    }
  });

  document.addEventListener("mouseup", function () {
    if (isDragging) {
      clearShiftDragState();
    }
  });

  zr.on("globalout", function (e) {
    if (isDragging) {
      suppressPointerEvent(e.event);
      clearShiftDragState();
    }
  });

  function updateShiftDragGraphic() {
    const graphicElements = [];
    if (shiftDragStart && shiftDragEnd && state.chart) {
      const candles = state.loadedCandles || [];
      const startIdx = shiftDragStart.dataIndex;
      const endIdx = shiftDragEnd.dataIndex;
      if (startIdx >= 0 && startIdx < candles.length && endIdx >= 0 && endIdx < candles.length) {
        const startPrice = shiftDragStart.price;
        const endPrice = shiftDragEnd.price;
        if (!Number.isFinite(startPrice) || !Number.isFinite(endPrice) || startPrice <= 0) {
          state.chart.setOption({ graphic: { elements: [] } }, { notMerge: false, replaceMerge: ['graphic'] });
          return;
        }
        const deltaPriceValue = endPrice - startPrice;
        const pctValue = deltaPriceValue / startPrice * 100;
        const pct = pctValue.toFixed(2);
        const deltaPrice = deltaPriceValue.toFixed(2);
        const color = pctValue >= 0 ? "#ef4444" : "#10b981";
        const sign = pctValue >= 0 ? "+" : "";
        const arrowPoints = buildArrowHeadPoints(shiftDragStart, shiftDragEnd);
        const labelText = `${sign}${pct}%\n${sign}${deltaPrice} (${startPrice.toFixed(2)} → ${endPrice.toFixed(2)})`;

        const midX = (shiftDragStart.x + shiftDragEnd.x) / 2;
        const midY = (shiftDragStart.y + shiftDragEnd.y) / 2;

        graphicElements.push({
          type: "line",
          shape: {
            x1: shiftDragStart.x,
            y1: shiftDragStart.y,
            x2: shiftDragEnd.x,
            y2: shiftDragEnd.y
          },
          style: {
            stroke: "#fbbf24",
            lineWidth: 2,
            lineDash: [4, 4]
          },
          z: 100
        });

        if (arrowPoints) {
          graphicElements.push({
            type: "polygon",
            shape: {
              points: arrowPoints
            },
            style: {
              fill: "#fbbf24"
            },
            z: 100
          });
        }

        graphicElements.push({
          type: "text",
          style: {
            text: labelText,
            x: midX,
            y: midY - 28,
            fill: color,
            fontSize: 13,
            fontWeight: "bold",
            align: "center",
            verticalAlign: "middle",
            lineHeight: 18,
            backgroundColor: "rgba(13,17,23,0.8)",
            padding: [4, 8],
            borderRadius: 4
          },
          z: 101
        });
      }
    }

    if (!state.chart) return;
    state.chart.setOption({ graphic: { elements: graphicElements } }, { notMerge: false, replaceMerge: ['graphic'] });
  }
}

function nearestIndex(times, targetMs) {
  if (!times.length) return -1;
  let bestIdx = 0;
  let bestDiff = Math.abs(times[0] - targetMs);
  for (let i = 1; i < times.length; i += 1) {
    const diff = Math.abs(times[i] - targetMs);
    if (diff < bestDiff) {
      bestDiff = diff;
      bestIdx = i;
    }
  }
  return bestIdx;
}

function toEpochMs(value) {
  if (!value) return NaN;
  const ms = new Date(value).getTime();
  return Number.isFinite(ms) ? ms : NaN;
}

function computeAutoZoomRange(timeMs, intervals, signals) {
  if (!timeMs.length) return null;
  let minIdx = Number.POSITIVE_INFINITY;
  let maxIdx = -1;

  const absorbTs = (tsValue) => {
    const ms = toEpochMs(tsValue);
    if (!Number.isFinite(ms)) return;
    const idx = nearestIndex(timeMs, ms);
    if (idx < 0) return;
    if (idx < minIdx) minIdx = idx;
    if (idx > maxIdx) maxIdx = idx;
  };

  for (const interval of intervals || []) {
    absorbTs(interval.start_dt);
    absorbTs(interval.end_dt);
  }

  for (const sig of signals || []) {
    absorbTs(sig.chart_interval_start_ts);
    absorbTs(sig.chart_interval_end_ts);
    absorbTs(sig.marker_ts);
    absorbTs(sig.anchor_day_ts);
    absorbTs(sig.window_start_ts);
    absorbTs(sig.window_end_ts);
    absorbTs(sig.signal_dt);
  }

  if (!Number.isFinite(minIdx) || maxIdx < 0) {
    return { left: 0, right: timeMs.length - 1 };
  }

  const span = Math.max(1, maxIdx - minIdx + 1);
  const pad = Math.max(1, span * 4);
  let left = Math.max(0, minIdx - pad);
  let right = Math.min(timeMs.length - 1, maxIdx + pad);
  return { left, right };
}

function applyAutoZoomRange(zoomRange) {
  if (!state.chart || !zoomRange) return;
  state.chart.dispatchAction({
    type: "dataZoom",
    dataZoomIndex: 0,
    startValue: zoomRange.left,
    endValue: zoomRange.right,
  });
  state.chart.dispatchAction({
    type: "dataZoom",
    dataZoomIndex: 1,
    startValue: zoomRange.left,
    endValue: zoomRange.right,
  });
}

function formatPercent(value, digits = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return `${(num * 100).toFixed(digits)}%`;
}

function formatNumber(value, digits = 3) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return num.toFixed(digits);
}

function formatAuditLine(title, node, asPercent = true) {
  if (!node || typeof node !== "object") return "";
  const valueText = asPercent ? formatPercent(node.value) : formatNumber(node.value);
  const thresholdText = asPercent ? formatPercent(node.threshold) : formatNumber(node.threshold);
  const passText = node.pass === true ? "通过" : node.pass === false ? "不通过" : "-";
  return `${title}: ${valueText} / 阈值 ${thresholdText} (${passText})`;
}

function buildSignalTooltip(sig) {
  const markerMode = sig.marker_mode === "anchor" ? "锚点" : "窗口中心";
  const lines = [];
  lines.push(`<b>${sig.signal_label || sig.strategy_name || "策略信号"}</b>`);
  lines.push(`触发确认: ${sig.signal_dt ? new Date(sig.signal_dt).toLocaleString() : "-"}`);
  lines.push(`红点语义: ${markerMode}`);
  lines.push(`红点时间: ${sig.marker_ts ? new Date(sig.marker_ts).toLocaleString() : "-"}`);
  lines.push(`该日(锚点): ${sig.anchor_day_ts ? new Date(sig.anchor_day_ts).toLocaleString() : "-"}`);
  const chartStart = sig.chart_interval_start_ts || sig.window_start_ts;
  const chartEnd = sig.chart_interval_end_ts || sig.window_end_ts;
  lines.push(`命中窗口: ${chartStart ? new Date(chartStart).toLocaleString() : "-"} ~ ${chartEnd ? new Date(chartEnd).toLocaleString() : "-"}`);

  const audit = sig.daily_audit && typeof sig.daily_audit === "object" ? sig.daily_audit : {};
  const gainLine = formatAuditLine("该日涨幅(前收基准)", audit.anchor_gain, true);
  const ampLine = formatAuditLine("该日振幅", audit.anchor_amplitude, true);
  const preMaxVolLine = formatAuditLine("前10日最大成交量", audit.pre_max_volume, false);
  const preAvgVolLine = formatAuditLine("前10日平均成交量", audit.pre_mean_volume, false);
  const preMaxAmpLine = formatAuditLine("前10日最大振幅", audit.pre_max_amplitude, true);
  const allPassedText = audit.all_passed === true ? "是" : audit.all_passed === false ? "否" : "-";

  lines.push("日线审计:");
  lines.push(`- 全部通过: ${allPassedText}`);
  if (gainLine) lines.push(`- ${gainLine}`);
  if (ampLine) lines.push(`- ${ampLine}`);
  if (preMaxVolLine) lines.push(`- ${preMaxVolLine}`);
  if (preAvgVolLine) lines.push(`- ${preAvgVolLine}`);
  if (preMaxAmpLine) lines.push(`- ${preMaxAmpLine}`);
  return lines.join("<br/>");
}

function renderChart(chartData, skipAutoZoom = false) {
  const candles = chartData.candles || [];
  const intervals = chartData.intervals || [];
  const signals = chartData.signals || [];

  // Save for lazy loading (only on fresh load, not merge re-render)
  if (!skipAutoZoom) {
    state.loadedCandles = candles;
    state.chartIntervals = intervals;
    state.chartSignals = signals;
    state.chartMeta = chartData;
    state.hasMoreBefore = candles.length > 0;
    state.hasMoreAfter = candles.length > 0;
  }
  if (candles.length > 0) {
    state.loadedStartTs = new Date(candles[0].datetime).getTime();
    state.loadedEndTs = new Date(candles[candles.length - 1].datetime).getTime();
  } else {
    state.loadedStartTs = null;
    state.loadedEndTs = null;
  }

  const xData = candles.map((c) => {
    const d = new Date(c.datetime);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  });
  const timeMs = candles.map((c) => new Date(c.datetime).getTime());
  const kData = candles.map((c) => [c.open, c.close, c.low, c.high]);
  const kDataWithPrevClose = candles.map((c) => ({
    value: [c.open, c.close, c.low, c.high],
  }));
  const vData = candles.map((c, i) => ({
    value: c.volume,
    itemStyle: {
      color: c.close >= c.open ? 'rgba(220, 80, 70, 0.55)' : 'rgba(30, 175, 120, 0.55)',
      borderColor: c.close >= c.open ? 'rgba(220, 80, 70, 0.8)' : 'rgba(30, 175, 120, 0.8)',
      borderWidth: 1,
    },
  }));

  // ── 前端计算 MA 均线 ──
  const closes = candles.map(c => Number(c.close));
  function calcMA(period) {
    const result = [];
    for (let i = 0; i < closes.length; i++) {
      if (i < period - 1) { result.push(null); continue; }
      let sum = 0;
      for (let j = i - period + 1; j <= i; j++) sum += closes[j];
      result.push(+(sum / period).toFixed(2));
    }
    return result;
  }
  const ma10Data = calcMA(10);
  const ma20Data = calcMA(20);

  const markAreaData = [];
  for (const interval of intervals) {
    if (!timeMs.length) continue;
    const startIdx = nearestIndex(timeMs, new Date(interval.start_dt).getTime());
    const endIdx = nearestIndex(timeMs, new Date(interval.end_dt).getTime());
    if (startIdx < 0 || endIdx < 0) continue;
    const left = Math.min(startIdx, endIdx);
    const right = Math.max(startIdx, endIdx);
    const strategyName = interval.strategy_name || chartData.strategy_name || "策略信号";
    const isStrategyWindow = interval.interval_kind === "strategy_window" || interval.interval_kind === "anchor_to_intraday_end";
    const intervalTitle = isStrategyWindow ? "命中窗口" : "信号聚合区间";
    const intervalFillColor = isStrategyWindow ? "rgba(244, 114, 182, 0.18)" : "rgba(0, 180, 255, 0.15)";
    const intervalLabelColor = isStrategyWindow ? "#ec4899" : "#0ea5e9";
    markAreaData.push([
      {
        xAxis: xData[left],
        itemStyle: { color: intervalFillColor },
        label: {
          show: true,
          color: intervalLabelColor,
          fontSize: 11,
          formatter: `${strategyName}\n${intervalTitle}\n${new Date(interval.start_dt).toLocaleString()} ~ ${new Date(interval.end_dt).toLocaleString()}`,
        },
      },
      { xAxis: xData[right] },
    ]);
  }

  const markPointData = [];
  for (const sig of signals) {
    if (!timeMs.length) continue;
    const markerMode = sig.marker_mode === "anchor" ? "anchor" : "interval_center";
    const anchorTsRaw = sig.anchor_day_ts || null;
    const markerTsRaw = sig.marker_ts || anchorTsRaw || sig.signal_dt;
    const markerTs = markerTsRaw ? new Date(markerTsRaw).getTime() : NaN;
    if (!Number.isFinite(markerTs)) continue;
    const idx = nearestIndex(timeMs, markerTs);
    if (idx < 0) continue;
    let markerY = kData[idx][1];
    if (markerMode === "interval_center") {
      const intervalStartRaw = sig.chart_interval_start_ts || sig.window_start_ts;
      const intervalEndRaw = sig.chart_interval_end_ts || sig.window_end_ts;
      if (intervalStartRaw && intervalEndRaw) {
        const sIdx = nearestIndex(timeMs, new Date(intervalStartRaw).getTime());
        const eIdx = nearestIndex(timeMs, new Date(intervalEndRaw).getTime());
        if (sIdx >= 0 && eIdx >= 0) {
          const left = Math.min(sIdx, eIdx);
          const right = Math.max(sIdx, eIdx);
          let rangeHigh = Number.NEGATIVE_INFINITY;
          for (let i = left; i <= right; i += 1) {
            rangeHigh = Math.max(rangeHigh, kData[i][3]);
          }
          if (Number.isFinite(rangeHigh)) {
            markerY = rangeHigh * 1.01;
          }
        }
      }
    }
    const labelText = sig.signal_label || sig.strategy_name || chartData.strategy_name || "策略信号";
    const pointTitle = markerMode === "anchor" ? `${labelText}（该日锚点）` : `${labelText}（窗口中心）`;
    // 根据标记点在图表中的相对位置，自动将标签推向画布中央
    const xRatio = xData.length > 1 ? idx / (xData.length - 1) : 0.5;
    let yMin = Infinity, yMax = -Infinity;
    for (let i = 0; i < kData.length; i++) {
      yMin = Math.min(yMin, kData[i][2]);
      yMax = Math.max(yMax, kData[i][3]);
    }
    const yRatio = yMax > yMin ? (markerY - yMin) / (yMax - yMin) : 0.5;
    const hAlign = xRatio > 0.75 ? "right" : xRatio < 0.25 ? "left" : "center";
    const vPos = yRatio > 0.7 ? "bottom" : "top";
    markPointData.push({
      coord: [xData[idx], markerY],
      value: pointTitle,
      label: {
        show: true,
        position: vPos,
        align: hAlign,
        formatter: `{a|${pointTitle}}\n{b|${new Date(markerTsRaw).toLocaleString()}}`,
        rich: {
          a: { color: "#39cc57", fontSize: 13, fontWeight: 600, align: hAlign },
          b: { color: "#94a3b8", fontSize: 13, align: hAlign },
        },
      },
      itemStyle: { color: "#ef4444" },
      tooltip: {
        formatter: () => buildSignalTooltip(sig),
      },
    });
  }

  // ── overlay_lines: 策略提供的斜线覆盖层（如三角形上下沿） ──
  const markLineData = [];
  for (const sig of signals) {
    const overlayLines = sig.overlay_lines || (sig.payload && sig.payload.overlay_lines) || [];
    for (const line of overlayLines) {
      if (!line.start_ts || !line.end_ts) continue;
      const sIdx = nearestIndex(timeMs, new Date(line.start_ts).getTime());
      const eIdx = nearestIndex(timeMs, new Date(line.end_ts).getTime());
      if (sIdx < 0 || eIdx < 0) continue;
      markLineData.push([
        {
          coord: [xData[sIdx], line.start_price],
          name: line.label || "",
          label: {
            show: true,
            formatter: line.label || "",
            color: line.color || "#fbbf24",
            fontSize: 11,
            fontWeight: 600,
            position: "start",
          },
        },
        {
          coord: [xData[eIdx], line.end_price],
        },
      ]);
    }
  }
  const markLineOpt = markLineData.length
    ? {
        silent: true,
        symbol: "none",
        lineStyle: { type: "dashed", width: 2, color: "#fbbf24" },
        label: { show: false },
        data: markLineData.map((pair) => {
          const lineColor = pair[0].label?.color || "#fbbf24";
          return [
            { ...pair[0], lineStyle: { type: "dashed", width: 2, color: lineColor } },
            pair[1],
          ];
        }),
      }
    : { silent: true, data: [] };

  state.chart.setOption({
    title: { text: "" },
    xAxis: [{ data: xData }, { data: xData }],
    series: [
      {
        name: "K线",
        data: kDataWithPrevClose,
        markArea: { silent: true, data: markAreaData },
        markPoint: {
          symbol: "pin",
          symbolSize: 40,
          data: markPointData,
        },
        markLine: markLineOpt,
      },
      { name: "成交量", data: vData },
      { name: "MA10", data: ma10Data },
      { name: "MA20", data: ma20Data },
    ],
  });
  if (!skipAutoZoom) {
    state.initialZoomRange = getPreferredZoomRange(timeMs, intervals, signals);
    applyAutoZoomRange(state.initialZoomRange);
    scheduleLockedZoomBackfill();
  }
  updateZoomLockButton();

  if (!intervals.length) {
    $("intervalSummary").textContent = "当前股票暂无可标注的触发区间。";
  } else {
    const hasStrategyWindow = intervals.some(
      (x) => x.interval_kind === "strategy_window" || x.interval_kind === "anchor_to_intraday_end",
    );
    const modeText = hasStrategyWindow ? "命中窗口" : "信号聚合区间";
    $("intervalSummary").textContent = `标注区间数: ${intervals.length}（主区间: ${modeText}），触发点数: ${signals.length}`;
  }
}

// ── K线懒加载 ──────────────────────────────────────────
let _lazyLoadTimer = null;
const LAZY_FETCH_LIMIT = 500;

function onDataZoomCheck() {
  if (!state.chart || state.isLoadingMore || !state.loadedCandles.length) return;
  const opt = state.chart.getOption();
  const dz = opt.dataZoom && opt.dataZoom[0];
  if (!dz) return;

  const total = state.loadedCandles.length;
  const startIdx = Math.round(dz.startValue);
  const endIdx = Math.round(dz.endValue);
  const threshold = Math.max(5, Math.ceil(total * 0.03));

  if (startIdx <= threshold && state.hasMoreBefore) {
    loadMoreCandles("before");
  } else if (endIdx >= total - 1 - threshold && state.hasMoreAfter) {
    loadMoreCandles("after");
  }
}

function getVisibleZoomTs() {
  if (!state.chart || !state.loadedCandles.length) return null;
  const opt = state.chart.getOption();
  const dz = opt.dataZoom && opt.dataZoom[0];
  if (!dz) return null;
  const timeMs = state.loadedCandles.map((c) => new Date(c.datetime).getTime());
  const si = Math.max(0, Math.min(Math.round(dz.startValue), timeMs.length - 1));
  const ei = Math.max(0, Math.min(Math.round(dz.endValue), timeMs.length - 1));
  return { start: timeMs[si], end: timeMs[ei] };
}

async function loadMoreCandles(direction, options = {}) {
  if (state.isLoadingMore || !state.taskId || !state.selectedCode) return;
  const preserveVisible = options.preserveVisible !== false;
  state.isLoadingMore = true;
  try {
    const zoomTs = preserveVisible ? getVisibleZoomTs() : null;
    const tf = state.chartTf;
    const taskId = state.taskId;
    const selectedCode = state.selectedCode;
    let url =
      `/api/tasks/${taskId}/candles?code=${encodeURIComponent(selectedCode)}` +
      `&timeframe=${encodeURIComponent(tf)}&limit=${LAZY_FETCH_LIMIT}`;

    if (direction === "before") {
      const beforeMs = state.loadedStartTs - 1;
      url += `&end_ts=${encodeURIComponent(new Date(beforeMs).toISOString())}`;
    } else {
      const afterMs = state.loadedEndTs + 1;
      url += `&start_ts=${encodeURIComponent(new Date(afterMs).toISOString())}`;
    }

    const data = await getJSON(url);
    if (taskId !== state.taskId || selectedCode !== state.selectedCode || tf !== state.chartTf) {
      return;
    }
    const newCandles = data.candles || [];

    if (newCandles.length === 0) {
      if (direction === "before") state.hasMoreBefore = false;
      else state.hasMoreAfter = false;
      return;
    }
    if (newCandles.length < LAZY_FETCH_LIMIT) {
      if (direction === "before") state.hasMoreBefore = false;
      else state.hasMoreAfter = false;
    }

    const existingTs = new Set(state.loadedCandles.map((c) => new Date(c.datetime).getTime()));
    const unique = newCandles.filter((c) => !existingTs.has(new Date(c.datetime).getTime()));
    if (!unique.length) {
      if (direction === "before") state.hasMoreBefore = false;
      else state.hasMoreAfter = false;
      return;
    }

    if (direction === "before") {
      state.loadedCandles = [...unique, ...state.loadedCandles];
    } else {
      state.loadedCandles = [...state.loadedCandles, ...unique];
    }

    // Re-render with merged data, preserving zoom position
    const mergedData = { ...state.chartMeta, candles: state.loadedCandles };
    renderChart(mergedData, true);

    if (zoomTs) {
      const timeMs = state.loadedCandles.map((c) => new Date(c.datetime).getTime());
      const si = nearestIndex(timeMs, zoomTs.start);
      const ei = nearestIndex(timeMs, zoomTs.end);
      if (si >= 0 && ei >= 0) {
        applyAutoZoomRange({ left: si, right: ei });
      }
    } else if (applyLockedZoomRange(tf) && needsLockedZoomBackfill(tf)) {
      scheduleLockedZoomBackfill(tf);
    }
  } catch (err) {
    console.error("懒加载K线失败", err);
  } finally {
    state.isLoadingMore = false;
  }
}

function setChartTf(tf, autoReload = true) {
  state.chartTf = tf;
  const buttons = document.querySelectorAll("[data-chart-tf]");
  buttons.forEach((btn) => {
    if (btn.dataset.chartTf === tf) {
      btn.classList.add("active");
    } else {
      btn.classList.remove("active");
    }
  });
  updateZoomLockButton();
  if (autoReload) {
    loadChart().catch((err) => alert(err.message));
  }
}

function bindChartTfButtons() {
  const buttons = document.querySelectorAll("[data-chart-tf]");
  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      setChartTf(btn.dataset.chartTf || "15", true);
    });
  });
}

async function loadChart() {
  if (!state.taskId || !state.selectedCode) {
    return;
  }
  const tf = state.chartTf;
  const requestSeq = ++state.chartLoadSeq;
  const selectedCode = state.selectedCode;

  const url = `/api/tasks/${state.taskId}/stock-chart?code=${encodeURIComponent(selectedCode)}` +
    `&timeframe=${encodeURIComponent(tf)}`;
  const data = await getJSON(url);
  if (requestSeq !== state.chartLoadSeq || selectedCode !== state.selectedCode || tf !== state.chartTf) {
    return;
  }

  state.selectedName = data.name || state.selectedName || "";
  updateChartTitles();
  renderChart(data);
}

async function loadResultStocksAndMaybeChart() {
  if (!state.taskId) return 0;
  const data = await getJSON(`/api/tasks/${state.taskId}/result-stocks`);
  const items = data.items || [];
  const conceptLoadPromise = ensureResultConceptsLoaded().catch((err) => {
    console.error("加载概念摘要失败", err);
    renderTopConceptSummary();
    return null;
  });

  const stillExists = items.some((x) => x.code === state.selectedCode);
  if (!stillExists) {
    state.selectedCode = items[0]?.code || null;
    state.selectedName = items[0]?.name || "";
  }
  renderResultStocks(items);
  if (state.selectedCode) {
    await loadChart();
  } else {
    state.selectedName = "";
    updateChartTitles();
    $("intervalSummary").textContent = "当前任务暂无命中股票。";
    renderChart({ candles: [], intervals: [], signals: [] });
  }
  await conceptLoadPromise;
  return items.length;
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

  const es = new EventSource(`/api/tasks/${state.taskId}/status-stream`);
  state.eventSource = es;
  setStreamState("SSE: 连接中", "is-connecting");

  es.onopen = () => {
    setStreamState("SSE: 已连接", "is-connected");
  };

  es.onerror = () => {
    setStreamState("SSE: 重连中", "is-connecting");
  };

  es.addEventListener("task-status", (e) => {
    const task = JSON.parse(e.data);
    const needReload = state.lastResultCount !== task.result_count;
    if (needReload) {
      state.lastResultCount = task.result_count;
      loadResultStocksAndMaybeChart()
        .then((totalStocks) => renderMeta(task, totalStocks))
        .catch((err) => console.error("加载结果失败", err));
    } else {
      const tbodyRows = $("resultStocksTable").querySelector("tbody").rows.length;
      renderMeta(task, tbodyRows);
    }
  });

  es.addEventListener("done", () => {
    setStreamState("SSE: 任务结束", "is-idle");
    if (state.eventSource === es) {
      es.close();
      state.eventSource = null;
    }
    startHeartbeatStream();
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

async function syncTask(forceReloadResults = false) {
  if (!state.taskId) return;
  const task = await getJSON(`/api/tasks/${state.taskId}`);
  const needReload = forceReloadResults || state.lastResultCount !== task.result_count;
  let totalStocks = 0;
  if (needReload) {
    totalStocks = await loadResultStocksAndMaybeChart();
    state.lastResultCount = task.result_count;
  } else {
    const tbodyRows = $("resultStocksTable").querySelector("tbody").rows.length;
    totalStocks = tbodyRows;
  }
  renderMeta(task, totalStocks);
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

    const fromUrl = parseTaskIdFromUrl();
    const taskId = selectTaskId || fromUrl || state.taskId || (data.items && data.items[0] ? data.items[0].task_id : null);
    if (!taskId) {
      renderTopConceptSummary();
      startPolling();
      return;
    }

    state.taskId = taskId;
    if (shouldBroadcast) {
      publishTaskSelection(state.taskId);
    }
    writeTaskIdToUrl(taskId);
    select.value = taskId;
    state.lastResultCount = -1;
    renderTopConceptSummary();
    const paramsBtn = $("showParamsBtn");
    if (paramsBtn) paramsBtn.disabled = !taskId;
    await syncTask(true);
    startPolling();
  } finally {
    state.pollInFlight = false;
  }
}

function bindEvents() {
  $("refreshTasksBtn").addEventListener("click", () => {
    refreshTaskList().catch((err) => alert(err.message));
  });
  $("taskSelect").addEventListener("change", async (ev) => {
    if (state.pollInFlight) return;
    state.pollInFlight = true;
    state.taskId = ev.target.value;
    publishTaskSelection(state.taskId);
    state.selectedCode = null;
    state.lastResultCount = -1;
    writeTaskIdToUrl(state.taskId);
    renderTopConceptSummary();
    const paramsBtnChg = $("showParamsBtn");
    if (paramsBtnChg) paramsBtnChg.disabled = !state.taskId;
    try {
      await syncTask(true);
      startPolling();
    } finally {
      state.pollInFlight = false;
    }
  });

  bindChartTfButtons();
  setChartTf(state.chartTf, false);

  const autoReload = () => {
    loadChart().catch((err) => alert(err.message));
  };
  $("toggleZoomLockBtn").addEventListener("click", () => {
    toggleZoomLock();
  });
  $("toggleFullscreenBtn").addEventListener("click", () => {
    toggleChartFullscreen().catch((err) => alert(err.message));
  });
  $("reloadChartBtn").addEventListener("click", () => {
    autoReload();
  });

  const conceptTooltip = getConceptTooltipElement();
  if (conceptTooltip) {
    conceptTooltip.addEventListener("mouseenter", () => {
      clearConceptTooltipHideTimer();
    });
    conceptTooltip.addEventListener("mouseleave", () => {
      scheduleHideConceptTooltip();
    });
  }

  window.addEventListener("scroll", (event) => {
    if (shouldKeepConceptTooltipOnScroll(event)) {
      clearConceptTooltipHideTimer();
      return;
    }
    hideConceptTooltip();
  }, true);
  window.addEventListener("resize", () => hideConceptTooltip());
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

function openParamModal() {
  const taskId = state.taskId;
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
  const overlay = $("paramModalOverlay");
  overlay.hidden = true;
  $("paramModalBody").innerHTML = "";
}

function bindParamModal() {
  const btn = $("showParamsBtn");
  if (btn) btn.addEventListener("click", openParamModal);
  const closeBtn = $("paramModalCloseBtn");
  if (closeBtn) closeBtn.addEventListener("click", closeParamModal);
  const overlay = $("paramModalOverlay");
  if (overlay) {
    overlay.addEventListener("click", (e) => {
      if (e.target === overlay) closeParamModal();
    });
  }
  window.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && overlay && !overlay.hidden) closeParamModal();
  });
}

async function init() {
  await ensureEcharts();
  state.zoomLocks = loadZoomLocksFromStorage();
  startHeartbeatStream();
  initChart();
  bindEvents();
  bindFullscreenEvents();
  syncChartFullscreenState();
  bindCrossTabSync();
  bindParamModal();
  await refreshTaskList();
}

init().catch((err) => {
  console.error("页面初始化失败", err);
  alert(`页面初始化失败：${err.message}`);
});
