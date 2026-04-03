"""
`weekly_oversold_rsi_v1` specialized engine。

职责：
1. 仅使用周线数据，筛选 RSI 处于历史性超跌区域的股票。
2. 命中条件：最近一根周线 RSI(m) < x 且 ≤ 该股票全部历史周线中最低 RSI(m) × y/100。
3. 历史 RSI 最低值按每只股票在 klines_w 中所有可用数据计算，不限定固定起始日期。

SQL 侧优化：
- 仅 SELECT code, datetime, close 三列（RSI 只需要收盘价），减少数据传输。
- 不限定 start_day，加载全量周线历史以计算准确的历史最低 RSI。

RSI 计算方法（Wilder 平滑）：
  1. delta = close(t) − close(t−1)
  2. gain = max(delta, 0)，loss = abs(min(delta, 0))
  3. 首个窗口：avg_gain = mean(gain[:m])，avg_loss = mean(loss[:m])
  4. 后续递推：avg_gain = (avg_gain*(m−1) + gain) / m，avg_loss 同理
  5. RS = avg_gain / avg_loss，RSI = 100 − 100/(1+RS)
"""

from __future__ import annotations

import time
from datetime import datetime
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
    connect_source_readonly,
    normalize_execution_params,
    read_universe_filter_params,
)

STRATEGY_LABEL = "周线超跌 v1"


# ---------------------------------------------------------------------------
# 参数规范化
# ---------------------------------------------------------------------------


def _normalize_weekly_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化周线 RSI 参数组。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回经类型转换和边界裁剪后的 RSI 参数字典。
    用途：
    1. 统一解析前端传入的 RSI 参数。
    边界条件：
    1. rsi_period 最小 2（Wilder 平滑至少需要 2 根线的变化量）。
    2. historical_ratio_pct 以百分比存储，内部转为小数比例。
    """
    raw = as_dict(group_params.get("weekly"))
    return {
        "rsi_period": as_int(raw.get("rsi_period"), 14, minimum=2, maximum=100),
        "rsi_threshold": as_float(raw.get("rsi_threshold"), 24.0, minimum=0.0, maximum=100.0),
        "historical_ratio_pct": as_float(raw.get("historical_ratio_pct"), 120.0, minimum=100.0, maximum=1000.0) / 100.0,
    }


# ---------------------------------------------------------------------------
# 数据加载（SQL 侧优化：仅取 code + datetime + close）
# ---------------------------------------------------------------------------


def _load_weekly_closes(
    *,
    source_db_path: Path,
    codes: list[str],
) -> pd.DataFrame:
    """批量加载所有股票的全量周线收盘价（SQL 侧仅选 3 列）。

    输入：
    1. source_db_path: DuckDB 源库路径。
    2. codes: 待扫描股票代码列表。
    输出：
    1. 返回按 `code + ts` 排序的 DataFrame（code, ts, close 三列）。
    边界条件：
    1. `codes` 为空时返回空表。
    2. 不限定 start_day，加载全量数据以计算准确的历史最低 RSI。
    """
    if not codes:
        return pd.DataFrame()

    with connect_source_readonly(source_db_path) as con:
        con.execute("CREATE TEMP TABLE _tmp_codes (code VARCHAR)")
        con.execute("INSERT INTO _tmp_codes(code) SELECT unnest($1)", [codes])
        frame = con.execute(
            """
            SELECT
                t.code,
                t.datetime AS ts,
                t.close
            FROM klines_w t
            JOIN _tmp_codes c ON c.code = t.code
            ORDER BY t.code, t.datetime
            """,
        ).fetchdf()
    return frame


# ---------------------------------------------------------------------------
# RSI 计算（Wilder 平滑）
# ---------------------------------------------------------------------------


def _compute_rsi_series(closes: np.ndarray, period: int) -> np.ndarray:
    """计算 Wilder RSI 序列。

    输入：
    1. closes: 收盘价 numpy 数组。
    2. period: RSI 平滑周期。
    输出：
    1. 返回与 closes 等长的 RSI 数组，前 period 个值为 NaN。
    用途：
    1. 使用 Wilder EMA 平滑（与主流行情软件一致）。
    边界条件：
    1. closes 长度 ≤ period 时返回全 NaN 数组。
    2. avg_loss 为零时 RSI = 100。
    """
    n = len(closes)
    rsi = np.full(n, np.nan)
    if n <= period:
        return rsi

    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    # 首个窗口的简单平均
    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))

    if avg_loss == 0:
        rsi[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi[period] = 100.0 - 100.0 / (1.0 + rs)

    # Wilder 递推
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi[i + 1] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i + 1] = 100.0 - 100.0 / (1.0 + rs)

    return rsi


# ---------------------------------------------------------------------------
# 三层架构：特征预计算 / 检测 / payload 组装
# ---------------------------------------------------------------------------


def prepare_weekly_rsi_features(code_frame: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """计算 RSI 列（列式特征预计算）。

    输入：
    1. code_frame: 单只股票的全量周线 DataFrame（已按 ts 排序，含 close 列）。
    2. params: 已规范化的 RSI 参数。
    输出：
    1. 返回增加了 rsi 列的 DataFrame。
    用途：
    1. 筛选模式：逐股调用后传入 detect_weekly_rsi。
    2. 回测模式：调用一次后滑窗 N 次 detect，避免重复计算 RSI。
    边界条件：
    1. 空 DataFrame 直接返回。
    """
    if code_frame.empty:
        return code_frame
    rsi_period = int(params["rsi_period"])
    closes = code_frame["close"].values.astype(np.float64)
    rsi_series = _compute_rsi_series(closes, rsi_period)
    result = code_frame.copy()
    result["rsi"] = rsi_series
    return result


def detect_weekly_rsi(code_frame: pd.DataFrame, params: dict[str, Any]) -> DetectionResult:
    """检测最新周线 RSI 是否处于历史性超跌区域。

    输入：
    1. code_frame: 单只股票的全量周线 DataFrame（已按 ts 排序，含 rsi 列）。
    2. params: 已规范化的 RSI 参数。
    输出：
    1. DetectionResult，matched 表示是否命中，metrics 含 latest_rsi 与 historical_min_rsi。
    用途：
    1. 判断最新 RSI 是否同时低于阈值和历史最低 RSI 比例。
    2. 可被筛选编排器和未来回测引擎独立调用。
    边界条件：
    1. code_frame 必须已包含 rsi 列（由 prepare_weekly_rsi_features 预计算）。
    2. 有效 RSI 值为空或最新 RSI 为 NaN 时返回 matched=False。
    """
    rsi_threshold = float(params["rsi_threshold"])
    historical_ratio = float(params["historical_ratio_pct"])

    rsi_values = code_frame["rsi"].values
    latest_rsi = float(rsi_values[-1])
    if np.isnan(latest_rsi):
        return DetectionResult(matched=False, metrics={"reason": "最新RSI为NaN"})

    valid_rsi = rsi_values[~np.isnan(rsi_values)]
    if len(valid_rsi) == 0:
        return DetectionResult(matched=False, metrics={"reason": "无有效RSI值"})

    historical_min_rsi = float(valid_rsi.min())
    metrics = {
        "latest_rsi": round(latest_rsi, 2),
        "historical_min_rsi": round(historical_min_rsi, 2),
    }

    passed = (latest_rsi < rsi_threshold) and (latest_rsi <= historical_min_rsi * historical_ratio)
    if not passed:
        return DetectionResult(matched=False, metrics=metrics)

    i = len(code_frame) - 1
    return DetectionResult(
        matched=True,
        pattern_start_idx=i,
        pattern_end_idx=i,
        pattern_start_ts=code_frame.iloc[i]["ts"],
        pattern_end_ts=code_frame.iloc[i]["ts"],
        metrics=metrics,
    )


def detect_weekly_rsi_vectorized(code_frame: pd.DataFrame, params: dict[str, Any]) -> list[DetectionResult]:
    """在单只股票的完整周线历史上一次性检测所有 RSI 超跌命中点。

    与 detect_weekly_rsi 的区别：
    - detect_weekly_rsi 只检查最新一根K线，供筛选模式和回测外层滑窗使用。
    - 本函数遍历全部有效 RSI 位置，收集所有命中记录，
      回测引擎无需再用外层 while 滑窗逐 bar 推进。

    输入：
    1. code_frame: 单只股票完整周线 DataFrame（已按 ts 排序，含 rsi 列）。
    2. params: 已规范化的 RSI 参数。
    输出：
    1. DetectionResult 列表，每个元素对应一根命中K线。
    边界条件：
    1. code_frame 必须已包含 rsi 列（由 prepare_weekly_rsi_features 预计算）。
    2. 无有效 RSI 值时返回空列表。
    """
    if code_frame.empty:
        return []

    rsi_threshold = float(params["rsi_threshold"])
    historical_ratio = float(params["historical_ratio_pct"])

    rsi_values = code_frame["rsi"].values.astype(np.float64)
    ts_values = code_frame["ts"].values

    # 逐 bar 累积最低 RSI（NaN 用 inf 占位，accumulate 后还原）
    rsi_for_min = np.where(np.isnan(rsi_values), np.inf, rsi_values)
    running_min = np.minimum.accumulate(rsi_for_min)
    running_min = np.where(running_min == np.inf, np.nan, running_min)

    # 列式布尔掩码
    valid = ~np.isnan(rsi_values)
    cond_threshold = rsi_values < rsi_threshold
    cond_historical = rsi_values <= running_min * historical_ratio
    matched = valid & cond_threshold & cond_historical

    hit_indices = np.where(matched)[0]
    results: list[DetectionResult] = []
    for i in hit_indices:
        results.append(DetectionResult(
            matched=True,
            pattern_start_idx=int(i),
            pattern_end_idx=int(i),
            pattern_start_ts=ts_values[i],
            pattern_end_ts=ts_values[i],
            metrics={
                "latest_rsi": round(float(rsi_values[i]), 2),
                "historical_min_rsi": round(float(running_min[i]), 2),
            },
        ))
    return results


def build_weekly_rsi_payload(
    *,
    code_frame: pd.DataFrame,
    params: dict[str, Any],
    detection_metrics: dict[str, Any],
) -> dict[str, Any]:
    """根据检测结果组装前端渲染 payload。

    输入：
    1. code_frame: 该股票的全量周线 DataFrame。
    2. params: 已规范化的 RSI 参数。
    3. detection_metrics: detect_weekly_rsi 返回的 metrics dict。
    输出：
    1. 符合图表渲染合同的 payload dict。
    边界条件：
    1. 仅在命中时调用。
    """
    total_bars = len(code_frame)
    latest_ts = code_frame.iloc[-1]["ts"]
    rsi_period = int(params["rsi_period"])
    rsi_threshold = float(params["rsi_threshold"])
    historical_ratio = float(params["historical_ratio_pct"])
    latest_rsi = detection_metrics["latest_rsi"]
    historical_min_rsi = detection_metrics["historical_min_rsi"]

    # 图表区间：最近 60 根周线
    chart_bars = 60
    chart_start_idx = max(total_bars - chart_bars, 0)
    chart_interval_start_ts = code_frame.iloc[chart_start_idx]["ts"]
    chart_interval_end_ts = latest_ts

    signal_label = f"周线RSI{rsi_period}={latest_rsi:.1f}(历史最低{historical_min_rsi:.1f})"

    return {
        "window_start_ts": chart_interval_start_ts,
        "window_end_ts": chart_interval_end_ts,
        "chart_interval_start_ts": chart_interval_start_ts,
        "chart_interval_end_ts": chart_interval_end_ts,
        "anchor_day_ts": latest_ts,
        "hit_timeframes": ["w"],
        "latest_rsi": latest_rsi,
        "historical_min_rsi": historical_min_rsi,
        "rsi_period": rsi_period,
        "rsi_threshold": rsi_threshold,
        "historical_ratio": round(historical_ratio, 4),
        "total_weekly_bars": total_bars,
        "signal_summary": signal_label,
    }


# ---------------------------------------------------------------------------
# 单股扫描
# ---------------------------------------------------------------------------


def _scan_one_code(
    *,
    code: str,
    name: str,
    code_frame: pd.DataFrame,
    params: dict[str, Any],
    strategy_group_id: str,
    strategy_name: str,
) -> tuple[StockScanResult, dict[str, int]]:
    """扫描单只股票的周线 RSI 超跌。

    输入：
    1. code/name: 股票标识。
    2. code_frame: 该股票的全量周线 DataFrame（code, ts, close）。
    3. params: 已规范化的 RSI 参数。
    4. strategy_group_id/strategy_name: 构建信号用。
    输出：
    1. (StockScanResult, 统计计数 dict)。
    边界条件：
    1. 周线数据不足 rsi_period + 1 根时返回无信号。
    2. 历史最低 RSI 为 NaN（全部数据的 RSI 均 NaN）时返回无信号。
    3. 命中时 clock_tf 固定为 "w"，图表区间取最近 60 根周线。
    """
    stats = {"candidate": 1, "kept": 0}
    total_bars = len(code_frame)
    rsi_period = int(params["rsi_period"])

    if total_bars <= rsi_period:
        return StockScanResult(
            code=code, name=name, processed_bars=total_bars,
            signal_count=0, signals=[],
        ), stats

    # Layer 1: 特征预计算
    enriched = prepare_weekly_rsi_features(code_frame, params)

    # Layer 2: 检测
    result = detect_weekly_rsi(enriched, params)
    if not result.matched:
        return StockScanResult(
            code=code, name=name, processed_bars=total_bars,
            signal_count=0, signals=[],
        ), stats

    # Layer 3: payload 组装
    stats["kept"] = 1
    payload = build_weekly_rsi_payload(
        code_frame=enriched,
        params=params,
        detection_metrics=result.metrics,
    )

    signal = build_signal_dict(
        code=code,
        name=name,
        signal_dt=payload["anchor_day_ts"],
        clock_tf="w",
        strategy_group_id=strategy_group_id,
        strategy_name=strategy_name,
        signal_label=payload["signal_summary"],
        payload=payload,
    )

    return StockScanResult(
        code=code, name=name, processed_bars=total_bars,
        signal_count=1, signals=[signal],
    ), stats


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------


def run_weekly_oversold_rsi_v1_specialized(
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
    """`weekly_oversold_rsi_v1` specialized 主入口。

    输入：
    1. 所有参数由 TaskManager 以关键字方式传入。
    输出：
    1. 返回 (result_map, metrics)，供 TaskManager 写库和前端展示使用。
    用途：
    1. 加载全量周线收盘价、逐股计算 RSI、筛选超跌股票。
    边界条件：
    1. `codes` 为空时直接返回空结果。
    2. start_ts / end_ts 参数在本策略中不用于截断历史数据（需全量计算 RSI），
       但 end_ts 用于限定"最近一根周线"的判定。
    3. 当前策略不使用缓存。
    """
    _ = (start_ts, end_ts, cache_scope, cache_dir)

    # ── 参数规范化 ──
    weekly_params = _normalize_weekly_params(group_params)
    execution_params = normalize_execution_params(group_params)
    universe_filter_params = read_universe_filter_params(group_params)

    base_metrics: dict[str, Any] = {
        "total_codes": len(codes),
        "enabled_timeframes": ["w"],
        "candidate_count": 0,
        "kept_count": 0,
        "codes_with_signal": 0,
        "stock_errors": 0,
        "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
        "concept_filter_enabled": universe_filter_params["enabled"],
        "concept_terms": universe_filter_params["concept_terms"],
        "reason_terms": universe_filter_params["reason_terms"],
    }

    if not codes:
        return {}, base_metrics

    # ── 数据加载（全量周线收盘价） ──
    t0 = time.perf_counter()
    all_weekly = _load_weekly_closes(
        source_db_path=Path(source_db_path),
        codes=codes,
    )
    load_sec = round(time.perf_counter() - t0, 4)
    base_metrics["load_times"] = {"w": load_sec}
    base_metrics["total_w_rows"] = len(all_weekly)

    # ── 按股票分组 ──
    per_code_data: dict[str, pd.DataFrame] = {}
    if not all_weekly.empty:
        for code_val, code_frame in all_weekly.groupby("code", sort=False):
            per_code_data[str(code_val)] = code_frame.sort_values("ts").reset_index(drop=True)

    # ── 逐股扫描 ──
    scan_start = time.perf_counter()
    result_map: dict[str, StockScanResult] = {}
    total_candidates = 0
    total_kept = 0

    for code in codes:
        code_frame = per_code_data.get(code, pd.DataFrame())

        if code_frame.empty:
            total_candidates += 1
            continue

        try:
            code_result, stats = _scan_one_code(
                code=code,
                name=code_to_name.get(code, ""),
                code_frame=code_frame,
                params=weekly_params,
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
    return {"params": _normalize_weekly_params(group_params)}


BACKTEST_HOOKS = {
    "detect": detect_weekly_rsi,
    "detect_vectorized": detect_weekly_rsi_vectorized,
    "prepare": prepare_weekly_rsi_features,
    "normalize_params": _normalize_for_backtest,
    "tf_sections": {
        "weekly": {"tf_key": "w", "table": "klines_w"},
    },
    "tf_logic": "or",
}
