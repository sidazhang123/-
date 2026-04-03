/**
 * 回测详情页逻辑。
 *
 * URL 参数: ?job_id=xxx&combo_index=yyy (combo_index 仅 sweep 模式)
 *
 * 功能:
 * 1. 加载命中记录表格 (可排序)
 * 2. 点击行 → 渲染 K线图 + 红色命中区域高亮
 * 3. 周期切换 tabs
 */

/* global echarts */

(function () {
  "use strict";

  const $ = (id) => document.getElementById(id);

  async function getJSON(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(await r.text().catch(() => r.statusText));
    return r.json();
  }

  // ── URL params ──
  const params = new URLSearchParams(window.location.search);
  const JOB_ID = params.get("job_id") || "";
  const COMBO_INDEX = parseInt(params.get("combo_index") ?? "-1", 10);

  // ── state ──
  const state = {
    hits: [],
    chart: null,
    selectedCode: null,
    selectedTf: null,
    sortKey: "profit_z_pct",
    sortAsc: false,
  };

  const TF_ORDER = ["w", "d", "60", "30", "15"];

  // ── init ──
  async function init() {
    if (!JOB_ID) {
      $("bdSummary").textContent = "缺少 job_id 参数";
      return;
    }
    initChart();
    await loadHits();
    renderTable();
    window.addEventListener("resize", () => { if (state.chart) state.chart.resize(); });
  }

  // ── load hits ──
  async function loadHits() {
    const qs = COMBO_INDEX >= 0
      ? `sort_by=profit_z_pct&sort_order=desc&combo_index=${COMBO_INDEX}`
      : "sort_by=profit_z_pct&sort_order=desc";
    try {
      const data = await getJSON(`/api/backtests/${JOB_ID}/hits?${qs}`);
      state.hits = data.items || [];
      $("bdSummary").textContent = `共 ${state.hits.length} 条命中记录` +
        (COMBO_INDEX >= 0 ? ` (参数组合 #${COMBO_INDEX})` : "");
    } catch (err) {
      $("bdSummary").textContent = "加载失败: " + err.message;
    }
  }

  // ── table ──
  const COLUMNS = [
    { key: "name",             label: "股票",        fmt: (v) => v || "—" },
    { key: "code",             label: "代码",        fmt: (v) => v || "—" },
    { key: "tf_key",           label: "周期",        fmt: (v) => v || "—" },
    { key: "pattern_start_ts", label: "命中起点",    fmt: fmtTs },
    { key: "profit_x_pct",    label: "盈利x%",      fmt: fmtPct },
    { key: "drawdown_x_pct",  label: "回撤x%",      fmt: fmtPct },
    { key: "sharpe_x",        label: "夏普x",       fmt: fmtNum },
    { key: "profit_y_pct",    label: "盈利y%",      fmt: fmtPct },
    { key: "drawdown_y_pct",  label: "回撤y%",      fmt: fmtPct },
    { key: "sharpe_y",        label: "夏普y",       fmt: fmtNum },
    { key: "profit_z_pct",    label: "盈利z%",      fmt: fmtPct },
    { key: "drawdown_z_pct",  label: "回撤z%",      fmt: fmtPct },
    { key: "sharpe_z",        label: "夏普z",       fmt: fmtNum },
  ];

  function fmtTs(v) {
    if (!v) return "—";
    const d = new Date(v);
    if (isNaN(d.getTime())) return String(v).slice(0, 16);
    const yy = String(d.getFullYear()).slice(2);
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${yy}/${mm}/${dd}`;
  }
  function fmtPct(v) {
    if (v == null) return "—";
    const n = parseFloat(v);
    if (isNaN(n)) return "—";
    const cls = n >= 0 ? "bd-positive" : "bd-negative";
    return `<span class="${cls}">${n.toFixed(2)}</span>`;
  }
  function fmtNum(v) {
    if (v == null) return "—";
    const n = parseFloat(v);
    return isNaN(n) ? "—" : n.toFixed(3);
  }

  function renderTable() {
    // Header
    const head = $("bdHead");
    head.innerHTML = "<tr>" + COLUMNS.map((c) =>
      `<th data-col="${c.key}">${c.label}</th>`
    ).join("") + "</tr>";

    // Sort
    const sorted = [...state.hits].sort((a, b) => {
      let va = a[state.sortKey], vb = b[state.sortKey];
      if (va == null) va = state.sortAsc ? Infinity : -Infinity;
      if (vb == null) vb = state.sortAsc ? Infinity : -Infinity;
      if (typeof va === "string") return state.sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
      return state.sortAsc ? va - vb : vb - va;
    });

    const body = $("bdBody");
    body.innerHTML = "";
    for (const hit of sorted) {
      const tr = document.createElement("tr");
      tr.innerHTML = COLUMNS.map((c) => `<td>${c.fmt(hit[c.key])}</td>`).join("");
      tr.addEventListener("click", () => {
        body.querySelectorAll("tr").forEach((r) => r.classList.remove("bd-active"));
        tr.classList.add("bd-active");
        selectHit(hit);
      });
      body.appendChild(tr);
    }

    // Header click sort
    head.addEventListener("click", (e) => {
      const th = e.target.closest("th[data-col]");
      if (!th) return;
      const col = th.dataset.col;
      if (state.sortKey === col) {
        state.sortAsc = !state.sortAsc;
      } else {
        state.sortKey = col;
        state.sortAsc = false;
      }
      renderTable();
    });
  }

  // ── select hit → chart ──
  function selectHit(hit) {
    state.selectedCode = hit.code;
    state.selectedTf = hit.tf_key;

    // Determine available TFs from all hits for this code
    const tfSet = new Set(state.hits.filter((h) => h.code === hit.code).map((h) => h.tf_key));
    renderTfTabs([...tfSet], hit.tf_key);
    loadChart(hit.code, hit.tf_key);
  }

  function renderTfTabs(tfs, activeTf) {
    const container = $("bdTfTabs");
    const ordered = TF_ORDER.filter((t) => tfs.includes(t));
    // add any not in TF_ORDER
    for (const t of tfs) { if (!ordered.includes(t)) ordered.push(t); }

    container.innerHTML = "";
    for (const tf of ordered) {
      const btn = document.createElement("button");
      btn.className = "bd-tf-tab" + (tf === activeTf ? " active" : "");
      btn.textContent = tf;
      btn.addEventListener("click", () => {
        state.selectedTf = tf;
        container.querySelectorAll(".bd-tf-tab").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        loadChart(state.selectedCode, tf);
      });
      container.appendChild(btn);
    }
  }

  // ── chart ──
  function initChart() {
    state.chart = echarts.init($("bdChart"), "dark");
    state.chart.setOption({
      animation: false,
      backgroundColor: "transparent",
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(13,17,23,0.92)",
        borderColor: "rgba(0,180,255,0.2)",
        textStyle: { color: "#e2e8f0" },
        formatter: function (params) {
          if (!params || !params.length) return "";
          var dateLabel = params[0].axisValue;
          var parts = ['<b>' + dateLabel + '</b>'];
          for (var i = 0; i < params.length; i++) {
            var p = params[i];
            if (p.seriesName === "K线" && p.data && p.data.length >= 4) {
              var open = p.value[1], close = p.value[2];
              var chgPct = open === 0 ? 0 : ((close - open) / open * 100);
              var color = chgPct >= 0 ? "#ef4444" : "#10b981";
              parts.push(p.marker + ' 涨跌幅: <span style="color:' + color + ';font-weight:bold;">' + chgPct.toFixed(2) + '%</span>');
              parts.push('　收盘: ' + close.toFixed(2));
            } else if (p.seriesName === "成交量") {
              var vol = typeof p.data === "object" ? p.data.value : p.data;
              parts.push(p.marker + ' 成交量: ' + (vol >= 1e8 ? (vol / 1e8).toFixed(2) + '亿' : vol >= 1e4 ? (vol / 1e4).toFixed(0) + '万' : vol));
            }
          }
          return parts.join('<br/>');
        },
      },
      grid: [
        { left: "8%", right: "4%", top: 32, height: "58%" },
        { left: "8%", right: "4%", top: "72%", height: "16%" },
      ],
      xAxis: [
        { type: "category", data: [], boundaryGap: false, axisLine: { lineStyle: { color: "#2a3a52" } }, axisLabel: { color: "#64748b" } },
        { type: "category", data: [], gridIndex: 1, boundaryGap: false, axisLine: { lineStyle: { color: "#2a3a52" } }, axisLabel: { show: false } },
      ],
      yAxis: [
        { scale: true, splitLine: { lineStyle: { color: "rgba(56,82,120,0.2)" } }, axisLabel: { color: "#64748b" } },
        { gridIndex: 1, scale: true, splitLine: { lineStyle: { color: "rgba(56,82,120,0.2)" } }, axisLabel: { show: false } },
      ],
      dataZoom: [
        { type: "inside", xAxisIndex: [0, 1], start: 60, end: 100 },
        { type: "slider", xAxisIndex: [0, 1], top: "92%", start: 60, end: 100,
          backgroundColor: "rgba(13,17,23,0.6)", fillerColor: "rgba(0,180,255,0.12)", handleStyle: { color: "#00b4ff" }, textStyle: { color: "#64748b" } },
      ],
      series: [
        { name: "K线", type: "candlestick", data: [],
          itemStyle: { color: "#ef4444", color0: "#10b981", borderColor: "#ef4444", borderColor0: "#10b981" } },
        { name: "成交量", type: "bar", xAxisIndex: 1, yAxisIndex: 1, data: [] },
      ],
    });
  }

  async function loadChart(code, tfKey) {
    $("bdChartInfo").textContent = `${code} - ${tfKey} K线图`;

    const comboQs = COMBO_INDEX >= 0 ? `&combo_index=${COMBO_INDEX}` : "";
    try {
      const data = await getJSON(
        `/api/backtests/${JOB_ID}/stock-chart?code=${encodeURIComponent(code)}&tf_key=${encodeURIComponent(tfKey)}${comboQs}`
      );
      renderKline(data);
    } catch (err) {
      $("bdChartInfo").textContent = `加载失败: ${err.message}`;
    }
  }

  function renderKline(data) {
    const candles = data.candles || [];
    const intervals = data.hit_intervals || [];

    if (!candles.length) {
      $("bdChartInfo").textContent += " (无K线数据)";
      return;
    }

    const xData = [];
    const kData = [];
    const vData = [];
    const timeMs = [];

    for (const c of candles) {
      const dt = new Date(c.datetime || c.date);
      const label = formatXLabel(dt, data.tf_key);
      xData.push(label);
      timeMs.push(dt.getTime());
      kData.push([c.open, c.close, c.low, c.high]);

      const vol = c.volume || 0;
      const color = c.close >= c.open ? "#ef4444" : "#10b981";
      vData.push({ value: vol, itemStyle: { color } });
    }

    // markArea for hit intervals (red highlight, no text)
    const markAreaData = [];
    // markPoint for hit end positions (triangle markers)
    const markPointData = [];
    for (const iv of intervals) {
      const sIdx = nearestIndex(timeMs, new Date(iv.start_ts).getTime());
      const eIdx = nearestIndex(timeMs, new Date(iv.end_ts).getTime());
      if (sIdx < 0 || eIdx < 0) continue;
      markAreaData.push([
        { xAxis: xData[Math.min(sIdx, eIdx)], itemStyle: { color: "rgba(239,68,68,0.15)" } },
        { xAxis: xData[Math.max(sIdx, eIdx)] },
      ]);
      // 在命中区间结束位置标注三角形
      markPointData.push({
        coord: [xData[eIdx], kData[eIdx][1]],
        symbol: "triangle",
        symbolSize: 12,
        symbolRotate: 180,
        itemStyle: { color: "#f59e0b" },
        label: { show: false },
      });
    }

    state.chart.setOption({
      xAxis: [{ data: xData }, { data: xData }],
      series: [
        {
          name: "K线",
          data: kData,
          markArea: { silent: true, data: markAreaData },
          markPoint: { silent: true, data: markPointData },
        },
        { name: "成交量", data: vData },
      ],
    });

    // Auto zoom to show latest hits
    if (intervals.length && xData.length > 50) {
      const lastHitEnd = intervals.reduce((mx, iv) => {
        const t = new Date(iv.end_ts).getTime();
        return t > mx ? t : mx;
      }, 0);
      const lastIdx = nearestIndex(timeMs, lastHitEnd);
      const startPct = Math.max(0, ((lastIdx - 80) / xData.length) * 100);
      const endPct = Math.min(100, ((lastIdx + 20) / xData.length) * 100);
      state.chart.dispatchAction({ type: "dataZoom", start: startPct, end: endPct });
    }
  }

  function formatXLabel(dt, tf) {
    const yy = String(dt.getFullYear()).slice(2);
    const mm = String(dt.getMonth() + 1).padStart(2, "0");
    const dd = String(dt.getDate()).padStart(2, "0");
    if (["15", "30", "60"].includes(tf)) {
      const HH = String(dt.getHours()).padStart(2, "0");
      const MM = String(dt.getMinutes()).padStart(2, "0");
      return `${yy}/${mm}/${dd} ${HH}:${MM}`;
    }
    return `${yy}/${mm}/${dd}`;
  }

  function nearestIndex(sortedMs, targetMs) {
    if (!sortedMs.length) return -1;
    let lo = 0, hi = sortedMs.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (sortedMs[mid] < targetMs) lo = mid + 1;
      else hi = mid;
    }
    if (lo > 0 && Math.abs(sortedMs[lo - 1] - targetMs) < Math.abs(sortedMs[lo] - targetMs)) {
      return lo - 1;
    }
    return lo;
  }

  // ── go ──
  init().catch((err) => console.error("backtest-detail init failed:", err));
})();
