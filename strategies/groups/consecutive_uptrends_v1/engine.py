"""
`consecutive_uptrends_v1` specialized engine。

职责：
1. 按周线/日线/60分钟三个可选周期，批量加载 K 线数据并在每个启用周期上独立检测连续小阳形态。
2. 连续小阳定义：连续 m 根阳线（close > open），每根涨幅 ≤ p%。
3. 可选急跌段：连阳结束后一定偏移范围内出现连续 z 根急跌线，跌幅达到连阳总涨幅的 y%。
4. 多周期之间为并集关系（OR）：任一启用周期检测到连续小阳即产生信号。
5. 同一股票多周期命中时合并为 1 条信号，clock_tf 取最粗命中周期。

连续小阳识别算法：
  1. 在最新 scan_bars 根 K 线中，从尾部向前滑动搜索连续 m 根阳线窗口。
  2. 阳线约束：close > open，且 (close − open) / open ≤ bar_gain_max%。
  3. 可选急跌段约束（selloff_enabled=true 时生效）：
     a. 急跌第一根 bar 出现在连阳末尾后的 floor(selloff_start_pct% × m) 根线以内。
     b. 急跌连续 z 根 bar，其中最低收盘价相对连阳最高收盘价的回撤
        ≤ selloff_return_pct%（以连阳总涨幅为基准）。
"""

from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from strategies.engine_commons import (
    DetectionResult,
    StockScanResult,
    as_bool,
    as_dict,
    as_float,
    as_int,
    build_signal_dict,
    coarsest_tf,
    connect_source_readonly,
    normalize_execution_params,
    read_universe_filter_params,
)

STRATEGY_LABEL = "K线连续小阳 v1"

# 周期 key → klines 表名
_TF_TABLE: dict[str, str] = {
    "w": "klines_w",
    "d": "klines_d",
    "60": "klines_60",
}

# 周期粗细排序（粗 → 细）
_TF_ORDER: list[str] = ["w", "d", "60"]

# manifest 参数组 key → 周期 key
_PARAM_SECTION_TO_TF: dict[str, str] = {
    "weekly": "w",
    "daily": "d",
    "min60": "60",
}

_TF_TO_PARAM_SECTION: dict[str, str] = {v: k for k, v in _PARAM_SECTION_TO_TF.items()}

# 周期中文名
_TF_LABEL: dict[str, str] = {
    "w": "周线",
    "d": "日线",
    "60": "60min",
}


# ---------------------------------------------------------------------------
# 参数规范化
# ---------------------------------------------------------------------------


def _normalize_tf_params(group_params: dict[str, Any], section_key: str) -> dict[str, Any]:
    """规范化单个周期的连续小阳参数组。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    2. section_key: manifest 参数组 key（weekly / daily / min60）。
    输出：
    1. 返回经类型转换和边界裁剪后的周期参数字典。
    用途：
    1. 统一处理三个周期的相同参数结构。
    边界条件：
    1. bar_gain_max 以百分比存储，内部转为小数。
    2. selloff_start_pct / selloff_return_pct 以百分比存储，内部转为小数。
    """
    raw = as_dict(group_params.get(section_key))
    return {
        "enabled": as_bool(raw.get("enabled"), False),
        "scan_bars": as_int(raw.get("scan_bars"), 20, minimum=5, maximum=500),
        "streak_len": as_int(raw.get("streak_len"), 5, minimum=2, maximum=100),
        "bar_gain_max": as_float(raw.get("bar_gain_max"), 2.0, minimum=0.01, maximum=100.0) / 100.0,
        "selloff_enabled": as_bool(raw.get("selloff_enabled"), False),
        "selloff_start_pct": as_float(raw.get("selloff_start_pct"), 50.0, minimum=0.0, maximum=500.0) / 100.0,
        "selloff_return_pct": as_float(raw.get("selloff_return_pct"), 70.0, minimum=1.0, maximum=500.0) / 100.0,
        "selloff_bars": as_int(raw.get("selloff_bars"), 3, minimum=1, maximum=50),
    }


# ---------------------------------------------------------------------------
# 数据加载
# ---------------------------------------------------------------------------


def _bars_to_days(bars: int, tf_key: str) -> int:
    """将 K 线根数估算为自然日天数（含安全余量）。

    输入：
    1. bars: K 线根数。
    2. tf_key: 周期标识。
    输出：
    1. 返回包含安全余量的自然日天数。
    用途：
    1. 在构造 SQL 日期范围时把 K 线根数转换为日历天数。
    边界条件：
    1. 除以交易日比例后加固定天数安全余量。
    """
    if tf_key == "w":
        return bars * 7 + 60
    if tf_key == "d":
        return int(bars * 1.5) + 30
    # 60min: ~4 bars/day
    return int(bars / 4 * 1.5) + 30


def _load_bars(
    *,
    source_db_path: Path,
    codes: list[str],
    tf_key: str,
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    """批量加载指定周期的 OHLCV 数据。

    输入：
    1. source_db_path: DuckDB 源库路径。
    2. codes: 待扫描股票代码列表。
    3. tf_key: 周期标识（w/d/60）。
    4. start_day/end_day: 日期范围边界。
    输出：
    1. 返回按 `code + ts` 排序的 DataFrame，时间列统一命名为 `ts`。
    边界条件：
    1. `codes` 为空时返回空表。
    2. 表名来自 _TF_TABLE 映射。
    """
    if not codes:
        return pd.DataFrame()

    table = _TF_TABLE[tf_key]
    with connect_source_readonly(source_db_path) as con:
        con.execute("CREATE TEMP TABLE _tmp_codes (code VARCHAR)")
        con.execute("INSERT INTO _tmp_codes(code) SELECT unnest($1)", [codes])
        frame = con.execute(
            f"""
            SELECT
                t.code,
                t.datetime AS ts,
                t.open,
                t.high,
                t.low,
                t.close,
                t.volume
            FROM {table} t
            JOIN _tmp_codes c ON c.code = t.code
            WHERE t.datetime >= ? AND t.datetime <= ?
            ORDER BY t.code, t.datetime
            """,
            [
                datetime.combine(start_day, datetime.min.time()),
                datetime.combine(end_day, datetime.max.time()),
            ],
        ).fetchdf()
    return frame


# ---------------------------------------------------------------------------
# 连续小阳检测核心
# ---------------------------------------------------------------------------


def detect_streak(
    code_frame: pd.DataFrame,
    params: dict[str, Any],
) -> DetectionResult:
    """在单只股票的单个周期数据上检测连续小阳（含可选急跌段）。

    输入：
    1. code_frame: 单只股票在该周期的 OHLCV DataFrame（已按 ts 排序）。
    2. params: 已规范化的单周期参数。
    输出：
    1. DetectionResult，matched 表示是否命中，metrics 含连阳与急跌段明细。
    用途：
    1. 从窗口尾部向前滑动搜索连续 m 根阳线。找到后若启用急跌检查，继续验证急跌段。
    2. 可被筛选编排器和未来回测引擎独立调用。
    边界条件：
    1. 数据不足 streak_len 时返回 matched=False。
    2. scan_bars 内任意位置的连阳均有效。
    3. 急跌偏移上限 = floor(selloff_start_pct × streak_len)。
    4. 急跌回撤基准 = z 根线最低收盘价 vs 连阳最高收盘价 / 连阳总涨幅。
    """
    scan_bars = int(params["scan_bars"])
    streak_len = int(params["streak_len"])
    bar_gain_max = float(params["bar_gain_max"])
    selloff_enabled = bool(params["selloff_enabled"])
    selloff_start_pct = float(params["selloff_start_pct"])
    selloff_return_pct = float(params["selloff_return_pct"])
    selloff_bars = int(params["selloff_bars"])

    if len(code_frame) < streak_len:
        return DetectionResult(matched=False, metrics={"reason": "数据不足"})

    # 截取最新 scan_bars 根线
    window = code_frame.tail(scan_bars).reset_index(drop=True)
    n = len(window)

    opens = window["open"].values
    closes = window["close"].values

    # 预计算每根线是否为合格阳线
    is_bull = (closes > opens) & (opens > 0)
    gains = (closes - opens) / opens
    is_qualified = is_bull & (gains <= bar_gain_max)

    # 从尾部向前滑动搜索连续 streak_len 根合格阳线
    for end_idx in range(n - 1, streak_len - 2, -1):
        start_idx = end_idx - streak_len + 1
        if start_idx < 0:
            continue

        # 检查这段窗口是否全部合格
        if not is_qualified[start_idx: end_idx + 1].all():
            continue

        # 命中连续小阳段
        streak_closes = closes[start_idx: end_idx + 1]
        streak_max_close = float(streak_closes.max())
        streak_start_close = float(closes[start_idx])
        streak_total_gain = streak_max_close - streak_start_close

        streak_start_ts = window.iloc[start_idx]["ts"]
        streak_end_ts = window.iloc[end_idx]["ts"]

        streak_detail = {
            "streak_start_idx": int(start_idx),
            "streak_end_idx": int(end_idx),
            "streak_start_ts": streak_start_ts,
            "streak_end_ts": streak_end_ts,
            "streak_len": streak_len,
            "streak_total_gain": round(streak_total_gain, 4),
            "streak_max_close": round(streak_max_close, 4),
            "streak_start_close": round(streak_start_close, 4),
            "streak_avg_gain": round(float(gains[start_idx: end_idx + 1].mean()), 6),
        }

        # 如果不启用急跌检查，直接命中
        if not selloff_enabled:
            return DetectionResult(
                matched=True,
                pattern_start_idx=int(start_idx),
                pattern_end_idx=int(end_idx),
                pattern_start_ts=streak_start_ts,
                pattern_end_ts=streak_end_ts,
                metrics=streak_detail,
            )

        # 急跌段检查
        max_offset = int(math.floor(selloff_start_pct * streak_len))
        # 急跌第一根 bar 的搜索范围：[end_idx+1, end_idx+1+max_offset]
        search_start = end_idx + 1
        search_end_limit = min(search_start + max_offset, n - selloff_bars)

        if streak_total_gain <= 0:
            # 连阳总涨幅 ≤ 0，无法计算回撤比例
            continue

        for sell_start in range(search_start, search_end_limit + 1):
            sell_end = sell_start + selloff_bars - 1
            if sell_end >= n:
                break

            sell_closes = closes[sell_start: sell_end + 1]
            sell_min_close = float(sell_closes.min())
            drawdown = streak_max_close - sell_min_close
            drawdown_ratio = drawdown / streak_total_gain

            if drawdown_ratio >= selloff_return_pct:
                # 急跌幅度达标
                selloff_detail = {
                    "selloff_start_idx": int(sell_start),
                    "selloff_end_idx": int(sell_end),
                    "selloff_start_ts": window.iloc[sell_start]["ts"],
                    "selloff_end_ts": window.iloc[sell_end]["ts"],
                    "selloff_min_close": round(sell_min_close, 4),
                    "selloff_drawdown": round(drawdown, 4),
                    "selloff_drawdown_ratio": round(drawdown_ratio, 4),
                }
                return DetectionResult(
                    matched=True,
                    pattern_start_idx=int(start_idx),
                    pattern_end_idx=int(sell_end),
                    pattern_start_ts=streak_start_ts,
                    pattern_end_ts=window.iloc[sell_end]["ts"],
                    metrics={**streak_detail, **selloff_detail},
                )

        # 连阳命中但急跌段未找到 → 继续搜索下一个连阳窗口
        continue

    return DetectionResult(
        matched=False,
        metrics={"reason": "未找到符合条件的连续小阳" + ("（含急跌段）" if selloff_enabled else "")},
    )


def detect_streak_vectorized(
    code_frame: pd.DataFrame,
    params: dict[str, Any],
) -> list[DetectionResult]:
    """在单只股票的完整历史上一次性检测所有连续小阳（含可选急跌段）命中点。

    与 detect_streak 的区别：
    - detect_streak 只在 tail(scan_bars) 中从尾部向前搜索第一个命中，
      供筛选模式和回测外层滑窗使用。
    - 本函数遍历全量历史，定位所有不重叠的连续小阳段，
      回测引擎无需再用外层 while 滑窗逐 bar 推进。

    输入：
    1. code_frame: 单只股票完整 OHLCV DataFrame（已按 ts 排序）。
    2. params: 已规范化的单周期参数。
    输出：
    1. DetectionResult 列表，每个元素对应一段命中。
    边界条件：
    1. 数据不足 streak_len 时返回空列表。
    2. 急跌模式下，连阳总涨幅 ≤ 0 的连阳段跳过。
    3. 不重叠：下一段搜索从上一段 pattern_end_idx + 1 开始。
    """
    streak_len = int(params["streak_len"])
    bar_gain_max = float(params["bar_gain_max"])
    selloff_enabled = bool(params["selloff_enabled"])
    selloff_start_pct = float(params["selloff_start_pct"])
    selloff_return_pct = float(params["selloff_return_pct"])
    selloff_bars = int(params["selloff_bars"])

    n = len(code_frame)
    if n < streak_len:
        return []

    opens = code_frame["open"].values.astype(np.float64)
    closes = code_frame["close"].values.astype(np.float64)
    ts_values = code_frame["ts"].values

    # 预计算合格阳线掩码
    safe_opens = np.where(opens > 0, opens, 1.0)
    gains = (closes - opens) / safe_opens
    is_qualified = (closes > opens) & (opens > 0) & (gains <= bar_gain_max)

    # 计算每个位置为止的连续合格阳线长度 (run length)
    run_len = np.zeros(n, dtype=np.int32)
    if is_qualified[0]:
        run_len[0] = 1
    for i in range(1, n):
        run_len[i] = (run_len[i - 1] + 1) if is_qualified[i] else 0

    # run_len[i] >= streak_len 的位置是一段连续小阳的末尾
    results: list[DetectionResult] = []
    last_end = -1  # 去重：跳过与上一个命中重叠的区间

    i = streak_len - 1
    while i < n:
        if run_len[i] < streak_len:
            i += 1
            continue

        # 找到一段连续段：end_idx = i, start_idx = i - streak_len + 1
        end_idx = i
        start_idx = end_idx - streak_len + 1

        if start_idx <= last_end:
            i += 1
            continue

        streak_closes = closes[start_idx: end_idx + 1]
        streak_max_close = float(np.max(streak_closes))
        streak_start_close = float(closes[start_idx])
        streak_total_gain = streak_max_close - streak_start_close

        if not selloff_enabled:
            # 无急跌：直接命中
            results.append(DetectionResult(
                matched=True,
                pattern_start_idx=start_idx,
                pattern_end_idx=end_idx,
                pattern_start_ts=ts_values[start_idx],
                pattern_end_ts=ts_values[end_idx],
                metrics={},
            ))
            last_end = end_idx
            i = end_idx + 1
            continue

        # 急跌段检查
        if streak_total_gain <= 0:
            i += 1
            continue

        max_offset = int(math.floor(selloff_start_pct * streak_len))
        search_start = end_idx + 1
        search_end_limit = min(search_start + max_offset, n - selloff_bars)

        found_selloff = False
        for sell_start in range(search_start, search_end_limit + 1):
            sell_end = sell_start + selloff_bars - 1
            if sell_end >= n:
                break

            sell_min_close = float(np.min(closes[sell_start: sell_end + 1]))
            drawdown = streak_max_close - sell_min_close
            drawdown_ratio = drawdown / streak_total_gain

            if drawdown_ratio >= selloff_return_pct:
                results.append(DetectionResult(
                    matched=True,
                    pattern_start_idx=start_idx,
                    pattern_end_idx=sell_end,
                    pattern_start_ts=ts_values[start_idx],
                    pattern_end_ts=ts_values[sell_end],
                    metrics={},
                ))
                last_end = sell_end
                i = sell_end + 1
                found_selloff = True
                break

        if not found_selloff:
            i += 1

    return results


# ---------------------------------------------------------------------------
# 单股扫描
# ---------------------------------------------------------------------------


def _build_signal_label(hit_tfs: list[str], per_tf_detail: dict[str, dict[str, Any]]) -> str:
    """构建信号标签：拼接各命中周期的连阳摘要。

    输入：
    1. hit_tfs: 命中的周期 key 列表。
    2. per_tf_detail: 各周期检测结果 dict。
    输出：
    1. 如 "日线连阳5根(涨5.2%)" 或含急跌信息，涨幅为相对起始收盘价的百分比。
    边界条件：
    1. 命中周期为空时返回策略默认标签。
    2. streak_start_close 为 0 时跳过百分比，仅显示连阳根数。
    """
    parts: list[str] = []
    for tf in hit_tfs:
        detail = per_tf_detail.get(tf, {})
        streak_len = detail.get("streak_len", 0)
        total_gain = detail.get("streak_total_gain", 0)
        start_close = detail.get("streak_start_close", 0)
        gain_pct = (total_gain / start_close * 100) if start_close > 0 else 0
        label = f"{_TF_LABEL.get(tf, tf)}连阳{streak_len}根(涨{gain_pct:.1f}%)" if start_close > 0 else f"{_TF_LABEL.get(tf, tf)}连阳{streak_len}根"
        if detail.get("selloff_drawdown_ratio") is not None:
            label += f"+急跌{detail.get('selloff_drawdown_ratio', 0)*100:.0f}%"
        parts.append(label)
    return " + ".join(parts) if parts else STRATEGY_LABEL


def build_consecutive_uptrends_payload(
    *,
    hit_tfs: list[str],
    per_tf_detail: dict[str, dict[str, Any]],
    tf_data: dict[str, pd.DataFrame],
    all_params: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """根据各周期检测结果组装前端渲染 payload。

    输入：
    1. hit_tfs: 命中的周期 key 列表（至少有一个）。
    2. per_tf_detail: 各周期检测明细（含未命中周期）。
    3. tf_data: 各周期的 DataFrame。
    4. all_params: 各周期的已规范化参数。
    输出：
    1. 符合图表渲染合同的 payload dict，包含 chart_interval、window、per_tf 等字段。
    边界条件：
    1. hit_tfs 不可为空（调用方应保证至少有一个命中周期）。
    """
    clock_tf = coarsest_tf(hit_tfs)
    clock_detail = per_tf_detail[clock_tf]
    clock_frame = tf_data[clock_tf]

    # 展示窗口：从连阳起始到连阳（或急跌段）结束
    window_start_ts = clock_detail["streak_start_ts"]
    window_end_ts = clock_detail.get("selloff_end_ts", clock_detail["streak_end_ts"])

    # 图表区间：取最粗命中周期最近 scan_bars 根线
    scan_bars = int(all_params[clock_tf]["scan_bars"])
    chart_start_idx = max(len(clock_frame) - scan_bars, 0)
    chart_interval_start_ts = clock_frame.iloc[chart_start_idx]["ts"]
    chart_interval_end_ts = clock_frame.iloc[-1]["ts"]

    signal_label = _build_signal_label(hit_tfs, per_tf_detail)

    return {
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "chart_interval_start_ts": chart_interval_start_ts,
        "chart_interval_end_ts": chart_interval_end_ts,
        "anchor_day_ts": clock_detail.get("streak_end_ts", window_end_ts),
        "hit_timeframes": hit_tfs,
        "per_tf": per_tf_detail,
        "signal_summary": signal_label,
    }


def _scan_one_code(
    *,
    code: str,
    name: str,
    tf_data: dict[str, pd.DataFrame],
    all_params: dict[str, dict[str, Any]],
    enabled_tfs: list[str],
    strategy_group_id: str,
    strategy_name: str,
) -> tuple[StockScanResult, dict[str, int]]:
    """扫描单只股票：在所有已启用周期上执行连续小阳检测（OR 逻辑）。

    输入：
    1. code/name: 股票标识。
    2. tf_data: {tf_key: 该股票的 DataFrame} 仅包含已启用周期。
    3. all_params: {tf_key: 已规范化参数} 仅包含已启用周期。
    4. enabled_tfs: 按粗→细排序的已启用周期列表。
    5. strategy_group_id/strategy_name: 构建信号用。
    输出：
    1. (StockScanResult, 统计计数 dict)。
    边界条件：
    1. OR 逻辑：任一周期命中即产生信号。
    2. 同一股票多周期命中时合并为 1 条信号。
    """
    stats = {"candidate": 0, "kept": 0}
    total_bars = sum(len(df) for df in tf_data.values())

    per_tf_detail: dict[str, dict[str, Any]] = {}
    hit_tfs: list[str] = []

    for tf in enabled_tfs:
        frame = tf_data.get(tf, pd.DataFrame())
        params = all_params[tf]
        if frame.empty:
            per_tf_detail[tf] = {"detected": False, "reason": "无数据"}
            continue
        result = detect_streak(frame, params)
        detail = {"detected": result.matched, "tf": tf, **result.metrics}
        per_tf_detail[tf] = detail
        if result.matched:
            hit_tfs.append(tf)

    stats["candidate"] = 1
    if not hit_tfs:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=total_bars,
            signal_count=0,
            signals=[],
        ), stats

    # 命中 → 生成 1 条合并信号
    stats["kept"] = 1
    payload = build_consecutive_uptrends_payload(
        hit_tfs=hit_tfs,
        per_tf_detail=per_tf_detail,
        tf_data=tf_data,
        all_params=all_params,
    )
    clock_tf = coarsest_tf(hit_tfs)

    signal = build_signal_dict(
        code=code,
        name=name,
        signal_dt=payload["window_end_ts"],
        clock_tf=clock_tf,
        strategy_group_id=strategy_group_id,
        strategy_name=strategy_name,
        signal_label=payload["signal_summary"],
        payload=payload,
    )

    return StockScanResult(
        code=code,
        name=name,
        processed_bars=total_bars,
        signal_count=1,
        signals=[signal],
    ), stats


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------


def run_consecutive_uptrends_v1_specialized(
    *,
    source_db_path: Path,
    start_ts: datetime | None,
    end_ts: datetime | None,
    codes: list[str],
    code_to_name: dict[str, str],
    group_params: dict[str, Any],
    strategy_group_id: str,
    strategy_name: str,
    cache_scope: str,
    cache_dir: Path | None = None,
) -> tuple[dict[str, StockScanResult], dict[str, Any]]:
    """`consecutive_uptrends_v1` specialized 主入口。

    输入：
    1. 所有参数由 TaskManager 以关键字方式传入。
    输出：
    1. 返回 (result_map, metrics)，供 TaskManager 写库和前端展示使用。
    用途：
    1. 组织参数规范化、各周期数据加载、逐股连续小阳检测和汇总统计。
    边界条件：
    1. 所有周期均未启用时返回空结果 + warn 级提示。
    2. `codes` 为空时直接返回空结果。
    3. 当前策略不使用缓存。
    """
    _ = (cache_scope, cache_dir)

    # ── 参数规范化 ──
    all_tf_params: dict[str, dict[str, Any]] = {}
    for section_key, tf_key in _PARAM_SECTION_TO_TF.items():
        all_tf_params[tf_key] = _normalize_tf_params(group_params, section_key)

    execution_params = normalize_execution_params(group_params)
    universe_filter_params = read_universe_filter_params(group_params)

    enabled_tfs = [tf for tf in _TF_ORDER if all_tf_params[tf]["enabled"]]

    base_metrics: dict[str, Any] = {
        "total_codes": len(codes),
        "enabled_timeframes": enabled_tfs,
        "candidate_count": 0,
        "kept_count": 0,
        "codes_with_signal": 0,
        "stock_errors": 0,
        "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
        "concept_filter_enabled": universe_filter_params["enabled"],
        "concept_terms": universe_filter_params["concept_terms"],
        "reason_terms": universe_filter_params["reason_terms"],
    }

    if not enabled_tfs:
        base_metrics["warning"] = "所有周期均未启用，无法筛选"
        return {}, base_metrics

    if not codes:
        return {}, base_metrics

    # ── 数据加载 ──
    end_dt = end_ts or datetime.now()
    end_day = end_dt.date()

    tf_frames: dict[str, pd.DataFrame] = {}
    tf_load_times: dict[str, float] = {}

    for tf in enabled_tfs:
        params = all_tf_params[tf]
        bars_needed = int(params["scan_bars"])
        days_back = _bars_to_days(bars_needed, tf)
        raw_start = start_ts.date() if start_ts is not None else (end_dt - timedelta(days=days_back)).date()
        desired_start = (end_dt - timedelta(days=days_back)).date()
        tf_start = min(raw_start, desired_start)

        t0 = time.perf_counter()
        frame = _load_bars(
            source_db_path=Path(source_db_path),
            codes=codes,
            tf_key=tf,
            start_day=tf_start,
            end_day=end_day,
        )
        tf_load_times[tf] = round(time.perf_counter() - t0, 4)
        tf_frames[tf] = frame

    base_metrics["load_times"] = tf_load_times
    for tf in enabled_tfs:
        base_metrics[f"total_{tf}_rows"] = len(tf_frames.get(tf, pd.DataFrame()))

    # ── 按股票分组 ──
    per_code_data: dict[str, dict[str, pd.DataFrame]] = {code: {} for code in codes}
    for tf in enabled_tfs:
        frame = tf_frames[tf]
        if frame.empty:
            continue
        for code_val, code_frame in frame.groupby("code", sort=False):
            if code_val in per_code_data:
                per_code_data[code_val][tf] = code_frame.sort_values("ts").reset_index(drop=True)

    # ── 逐股扫描 ──
    scan_start = time.perf_counter()
    result_map: dict[str, StockScanResult] = {}
    total_candidates = 0
    total_kept = 0

    for code in codes:
        code_tf_data = per_code_data.get(code, {})
        for tf in enabled_tfs:
            if tf not in code_tf_data:
                code_tf_data[tf] = pd.DataFrame()

        try:
            code_result, stats = _scan_one_code(
                code=code,
                name=code_to_name.get(code, ""),
                tf_data=code_tf_data,
                all_params={tf: all_tf_params[tf] for tf in enabled_tfs},
                enabled_tfs=enabled_tfs,
                strategy_group_id=strategy_group_id,
                strategy_name=strategy_name,
            )
        except Exception as exc:
            code_result = StockScanResult(
                code=code,
                name=code_to_name.get(code, ""),
                processed_bars=0,
                signal_count=0,
                signals=[],
                error_message=str(exc),
            )
            stats = {"candidate": 0, "kept": 0}
            base_metrics["stock_errors"] += 1

        total_candidates += stats.get("candidate", 0)
        total_kept += stats.get("kept", 0)

        if code_result.signal_count > 0 or code_result.error_message:
            result_map[code] = code_result

    scan_sec = round(time.perf_counter() - scan_start, 4)
    base_metrics["scan_phase_sec"] = scan_sec
    base_metrics["candidate_count"] = total_candidates
    base_metrics["kept_count"] = total_kept
    base_metrics["codes_with_signal"] = sum(1 for r in result_map.values() if r.signal_count > 0)

    return result_map, base_metrics


# ---------------------------------------------------------------------------
# 回测钩子
# ---------------------------------------------------------------------------

def _normalize_for_backtest(group_params: dict[str, Any], section_key: str) -> dict[str, Any]:
    return {"params": _normalize_tf_params(group_params, section_key)}


BACKTEST_HOOKS = {
    "detect": detect_streak,
    "detect_vectorized": detect_streak_vectorized,
    "prepare": None,
    "normalize_params": _normalize_for_backtest,
    "tf_sections": {
        "weekly": {"tf_key": "w", "table": "klines_w"},
        "daily": {"tf_key": "d", "table": "klines_d"},
        "min60": {"tf_key": "60", "table": "klines_60"},
    },
    "tf_logic": "or",
}
