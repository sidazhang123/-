"""
`weekly_oversold_rsi_v1` specialized engine。

职责：
1. 仅使用周线数据，筛选 RSI 处于历史性超跌区域的股票。
2. 命中条件：最近一根周线 RSI(m) < x 且 ≤ 该股票全部历史周线中最低 RSI(m) × y/100。
3. 历史 RSI 最低值按每只股票在 klines_w 中所有可用数据计算，不限定固定起始日期。
4. 可选启用末端暴跌检查：最新周线收盘价低于 Keltner 通道下轨（SMA(n) − K × ATR(n)），
   体现下跌段末期出现加速暴跌。ATR 使用 Wilder EMA 平滑，与 RSI 风格统一。

SQL 侧优化：
- SELECT code, datetime, high, low, close 五列（ATR 需要 high/low，RSI 仅需 close）。
- 不限定 start_day，加载全量周线历史以计算准确的历史最低 RSI。

RSI 计算方法（Wilder 平滑）：
  1. delta = close(t) − close(t−1)
  2. gain = max(delta, 0)，loss = abs(min(delta, 0))
  3. 首个窗口：avg_gain = mean(gain[:m])，avg_loss = mean(loss[:m])
  4. 后续递推：avg_gain = (avg_gain*(m−1) + gain) / m，avg_loss 同理
  5. RS = avg_gain / avg_loss，RSI = 100 − 100/(1+RS)

ATR 计算方法（Wilder 平滑）：
  1. TR(t) = max(high(t)−low(t), |high(t)−close(t−1)|, |low(t)−close(t−1)|)
  2. 首个窗口：ATR = mean(TR[:n])
  3. 后续递推：ATR = (ATR*(n−1) + TR) / n

Keltner 通道下轨：
  keltner_lower = SMA(close, n) − K × ATR(n)
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
    """规范化周线 RSI + ATR 偏离参数组。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回经类型转换和边界裁剪后的 RSI + ATR 参数字典。
    用途：
    1. 统一解析前端传入的 RSI 及 ATR 偏离参数。
    边界条件：
    1. rsi_period 最小 2（Wilder 平滑至少需要 2 根线的变化量）。
    2. historical_ratio_pct 以百分比存储，内部转为小数比例。
    3. atr_deviation_enabled 默认关闭，不影响原有筛选行为。
    4. atr_period 最小 2（至少 2 根线计算 True Range）。
    5. atr_multiplier 控制 Keltner 通道宽度，范围 0.5-10.0。
    """
    raw = as_dict(group_params.get("weekly"))
    return {
        "rsi_period": as_int(raw.get("rsi_period"), 14, minimum=2, maximum=100),
        "rsi_threshold": as_float(raw.get("rsi_threshold"), 24.0, minimum=0.0, maximum=100.0),
        "historical_ratio_pct": as_float(raw.get("historical_ratio_pct"), 120.0, minimum=100.0, maximum=1000.0) / 100.0,
        "atr_deviation_enabled": as_bool(raw.get("atr_deviation_enabled"), False),
        "atr_period": as_int(raw.get("atr_period"), 14, minimum=2, maximum=100),
        "atr_multiplier": as_float(raw.get("atr_multiplier"), 2.0, minimum=0.5, maximum=10.0),
    }


# ---------------------------------------------------------------------------
# 数据加载（SQL 侧优化：仅取 code + datetime + close）
# ---------------------------------------------------------------------------


def _load_weekly_ohlc(
    *,
    source_db_path: Path,
    codes: list[str],
) -> pd.DataFrame:
    """批量加载所有股票的全量周线 OHLC 数据（high/low/close）。

    输入：
    1. source_db_path: DuckDB 源库路径。
    2. codes: 待扫描股票代码列表。
    输出：
    1. 返回按 `code + ts` 排序的 DataFrame（code, ts, high, low, close 五列）。
    边界条件：
    1. `codes` 为空时返回空表。
    2. 不限定 start_day，加载全量数据以计算准确的历史最低 RSI。
    3. 加载 high/low 列以支持 ATR 偏离条件（即使未启用也统一加载，字段开销极小）。
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
                t.high,
                t.low,
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
# ATR / SMA 计算
# ---------------------------------------------------------------------------


def _compute_atr_series(
    high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int
) -> np.ndarray:
    """计算 Wilder 平滑 ATR 序列。

    输入：
    1. high/low/close: 等长 numpy 数组（float64）。
    2. period: ATR 平滑周期。
    输出：
    1. 与输入等长的 ATR 数组，前 period 个值为 NaN（第 0 根无前一收盘价，
       前 period-1 根不足以计算首窗口均值，故有效值从索引 period 开始）。
    边界条件：
    1. 数组长度 ≤ period 时返回全 NaN 数组。
    """
    n = len(close)
    atr = np.full(n, np.nan)
    if n <= period:
        return atr

    # True Range: 第 0 根无前收盘价，从索引 1 开始
    prev_close = close[:-1]
    tr = np.maximum(high[1:] - low[1:],
                    np.maximum(np.abs(high[1:] - prev_close),
                               np.abs(low[1:] - prev_close)))

    # 首窗口简单平均（tr 索引 0..period-1 对应 K 线索引 1..period）
    avg_tr = float(np.mean(tr[:period]))
    atr[period] = avg_tr

    # Wilder 递推
    for i in range(period, len(tr)):
        avg_tr = (avg_tr * (period - 1) + tr[i]) / period
        atr[i + 1] = avg_tr

    return atr


def _compute_sma_series(values: np.ndarray, period: int) -> np.ndarray:
    """计算简单移动平均序列。

    输入：
    1. values: numpy 数组（float64）。
    2. period: 移动平均周期。
    输出：
    1. 与输入等长的 SMA 数组，前 period-1 个值为 NaN。
    边界条件：
    1. 数组长度 < period 时返回全 NaN 数组。
    """
    n = len(values)
    sma = np.full(n, np.nan)
    if n < period:
        return sma
    cumsum = np.cumsum(values)
    sma[period - 1:] = (cumsum[period - 1:] - np.concatenate([[0.0], cumsum[:-period]])) / period
    return sma


# ---------------------------------------------------------------------------
# 三层架构：特征预计算 / 检测 / payload 组装
# ---------------------------------------------------------------------------


def prepare_weekly_rsi_features(code_frame: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """计算 RSI 列及可选的 ATR/SMA/Keltner 列（列式特征预计算）。

    输入：
    1. code_frame: 单只股票的全量周线 DataFrame（已按 ts 排序，含 high/low/close 列）。
    2. params: 已规范化的 RSI + ATR 参数。
    输出：
    1. 返回增加了 rsi 列的 DataFrame。若 atr_deviation_enabled=True，
       额外包含 atr、sma、keltner_lower 三列。
    用途：
    1. 筛选模式：逐股调用后传入 detect_weekly_rsi。
    2. 回测模式：调用一次后滑窗 N 次 detect，避免重复计算 RSI/ATR。
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

    if params.get("atr_deviation_enabled", False):
        atr_period = int(params["atr_period"])
        atr_multiplier = float(params["atr_multiplier"])
        high = code_frame["high"].values.astype(np.float64)
        low = code_frame["low"].values.astype(np.float64)
        atr_series = _compute_atr_series(high, low, closes, atr_period)
        sma_series = _compute_sma_series(closes, atr_period)
        result["atr"] = atr_series
        result["sma"] = sma_series
        result["keltner_lower"] = sma_series - atr_multiplier * atr_series

    return result


def detect_weekly_rsi(code_frame: pd.DataFrame, params: dict[str, Any]) -> DetectionResult:
    """检测最新周线 RSI 是否处于历史性超跌区域（可选 ATR 偏离条件）。

    输入：
    1. code_frame: 单只股票的全量周线 DataFrame（已按 ts 排序，含 rsi 列；
       若 atr_deviation_enabled=True 还需含 keltner_lower 列）。
    2. params: 已规范化的 RSI + ATR 参数。
    输出：
    1. DetectionResult，matched 表示是否命中，metrics 含 latest_rsi 与 historical_min_rsi，
       若启用 ATR 偏离则额外含 latest_close、keltner_lower。
    用途：
    1. 判断最新 RSI 是否同时低于阈值和历史最低 RSI 比例。
    2. 若启用 ATR 偏离，追加判断最新收盘价 < Keltner 通道下轨。
    3. 可被筛选编排器和回测引擎独立调用。
    边界条件：
    1. code_frame 必须已包含 rsi 列（由 prepare_weekly_rsi_features 预计算）。
    2. 有效 RSI 值为空或最新 RSI 为 NaN 时返回 matched=False。
    3. ATR 偏离启用但 keltner_lower 最新值为 NaN 时返回 matched=False。
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
    metrics: dict[str, Any] = {
        "latest_rsi": round(latest_rsi, 2),
        "historical_min_rsi": round(historical_min_rsi, 2),
    }

    passed = (latest_rsi < rsi_threshold) and (latest_rsi <= historical_min_rsi * historical_ratio)

    # 可选 ATR 偏离条件
    if passed and params.get("atr_deviation_enabled", False):
        keltner_lower_values = code_frame["keltner_lower"].values
        latest_keltner = float(keltner_lower_values[-1])
        latest_close = float(code_frame["close"].values[-1])
        metrics["latest_close"] = round(latest_close, 4)
        metrics["keltner_lower"] = round(latest_keltner, 4)
        if np.isnan(latest_keltner):
            passed = False
        else:
            passed = latest_close < latest_keltner

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
    """在单只股票的完整周线历史上一次性检测所有 RSI 超跌命中点（可选 ATR 偏离条件）。

    与 detect_weekly_rsi 的区别：
    - detect_weekly_rsi 只检查最新一根K线，供筛选模式和回测外层滑窗使用。
    - 本函数遍历全部有效 RSI 位置，收集所有命中记录，
      回测引擎无需再用外层 while 滑窗逐 bar 推进。

    输入：
    1. code_frame: 单只股票完整周线 DataFrame（已按 ts 排序，含 rsi 列；
       若 atr_deviation_enabled=True 还需含 keltner_lower 列）。
    2. params: 已规范化的 RSI + ATR 参数。
    输出：
    1. DetectionResult 列表，每个元素对应一根命中K线。
    边界条件：
    1. code_frame 必须已包含 rsi 列（由 prepare_weekly_rsi_features 预计算）。
    2. 无有效 RSI 值时返回空列表。
    3. ATR 偏离启用时，keltner_lower 为 NaN 的位置不会命中。
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

    # 可选 ATR 偏离条件
    atr_enabled = params.get("atr_deviation_enabled", False)
    if atr_enabled and "keltner_lower" in code_frame.columns:
        close_values = code_frame["close"].values.astype(np.float64)
        keltner_lower_values = code_frame["keltner_lower"].values.astype(np.float64)
        cond_atr = close_values < keltner_lower_values
        cond_atr_valid = ~np.isnan(keltner_lower_values)
        matched = matched & cond_atr & cond_atr_valid

    hit_indices = np.where(matched)[0]
    results: list[DetectionResult] = []
    for i in hit_indices:
        ii = int(i)
        metrics: dict[str, Any] = {
            "latest_rsi": round(float(rsi_values[ii]), 2),
            "historical_min_rsi": round(float(running_min[ii]), 2),
        }
        if atr_enabled and "keltner_lower" in code_frame.columns:
            metrics["latest_close"] = round(float(code_frame["close"].values[ii]), 4)
            metrics["keltner_lower"] = round(float(code_frame["keltner_lower"].values[ii]), 4)
        results.append(DetectionResult(
            matched=True,
            pattern_start_idx=ii,
            pattern_end_idx=ii,
            pattern_start_ts=ts_values[ii],
            pattern_end_ts=ts_values[ii],
            metrics=metrics,
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
    2. params: 已规范化的 RSI + ATR 参数。
    3. detection_metrics: detect_weekly_rsi 返回的 metrics dict。
    输出：
    1. 符合图表渲染合同的 payload dict。
    边界条件：
    1. 仅在命中时调用。
    2. 若 ATR 偏离启用，payload 额外包含 atr_period、atr_multiplier、
       keltner_lower、latest_close 字段，signal_summary 追加 ATR 描述。
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

    payload: dict[str, Any] = {
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

    # ATR 偏离追加信息
    if params.get("atr_deviation_enabled", False):
        keltner_lower = detection_metrics.get("keltner_lower")
        latest_close = detection_metrics.get("latest_close")
        atr_period = int(params["atr_period"])
        atr_multiplier = float(params["atr_multiplier"])
        payload["atr_period"] = atr_period
        payload["atr_multiplier"] = atr_multiplier
        payload["keltner_lower"] = keltner_lower
        payload["latest_close"] = latest_close
        signal_label += f",收盘{latest_close:.2f}<Keltner下轨{keltner_lower:.2f}"
        payload["signal_summary"] = signal_label

    return payload


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

    # ── 数据加载（全量周线 OHLC） ──
    t0 = time.perf_counter()
    all_weekly = _load_weekly_ohlc(
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

    # ── 向量化批量检测 ──
    scan_start = time.perf_counter()

    # 批量计算 RSI 并向量化检测，替代逐股 _scan_one_code 循环
    _batch_hits: set[str] = set()
    if not all_weekly.empty:
        _rsi_period = int(weekly_params["rsi_period"])
        _rsi_thr = float(weekly_params["rsi_threshold"])
        _hist_ratio = float(weekly_params["historical_ratio_pct"])

        # 批量 RSI：groupby.transform 内部逐组递推（Wilder EMA 本质串行）
        _g = all_weekly.groupby("code", sort=False)
        _rsi_all = _g["close"].transform(
            lambda s: _compute_rsi_series(s.values.astype(np.float64), _rsi_period)
        )
        _latest_rsi = _rsi_all.groupby(all_weekly["code"], sort=False).last()
        _min_rsi = _rsi_all.groupby(all_weekly["code"], sort=False).min()

        _ok = (
            _latest_rsi.notna()
            & (_latest_rsi < _rsi_thr)
            & _min_rsi.notna()
            & (_latest_rsi <= _min_rsi * _hist_ratio)
        )

        # ATR 偏离前置过滤（仅在启用时追加条件）
        _atr_enabled = weekly_params.get("atr_deviation_enabled", False)
        if _atr_enabled:
            _atr_period = int(weekly_params["atr_period"])
            _atr_mult = float(weekly_params["atr_multiplier"])
            _atr_all = _g.apply(
                lambda df: pd.Series(
                    _compute_atr_series(
                        df["high"].values.astype(np.float64),
                        df["low"].values.astype(np.float64),
                        df["close"].values.astype(np.float64),
                        _atr_period,
                    ),
                    index=df.index,
                ),
            )
            # apply 后可能产生 MultiIndex，需 droplevel 回到原始索引
            if isinstance(_atr_all.index, pd.MultiIndex):
                _atr_all = _atr_all.droplevel(0)
            _sma_all = _g["close"].transform(
                lambda s: _compute_sma_series(s.values.astype(np.float64), _atr_period)
            )
            _keltner_lower = _sma_all - _atr_mult * _atr_all
            _latest_close = all_weekly.groupby("code", sort=False)["close"].last()
            _latest_keltner = _keltner_lower.groupby(all_weekly["code"], sort=False).last()
            _ok = _ok & _latest_keltner.notna() & (_latest_close < _latest_keltner)

        _batch_hits = set(_latest_rsi.index[_ok])

    result_map: dict[str, StockScanResult] = {}
    total_candidates = len(codes)
    total_kept = 0

    for code in codes:
        if code not in _batch_hits:
            continue

        code_frame = per_code_data.get(code, pd.DataFrame())

        if code_frame.empty:
            continue

        try:
            # 向量化已确认命中，直接计算 RSI metrics 构建 payload
            rsi_period = int(weekly_params["rsi_period"])
            total_bars = len(code_frame)

            if total_bars <= rsi_period:
                continue

            enriched = prepare_weekly_rsi_features(code_frame, weekly_params)
            rsi_values = enriched["rsi"].values
            latest_rsi = float(rsi_values[-1])
            valid_rsi = rsi_values[~np.isnan(rsi_values)]
            historical_min_rsi = float(valid_rsi.min()) if len(valid_rsi) > 0 else float("nan")

            detection_metrics: dict[str, Any] = {
                "latest_rsi": round(latest_rsi, 2),
                "historical_min_rsi": round(historical_min_rsi, 2),
            }

            # ATR 偏离 metrics（前置批量已确认命中，此处补充字段供 payload 使用）
            if weekly_params.get("atr_deviation_enabled", False):
                detection_metrics["latest_close"] = round(float(enriched["close"].values[-1]), 4)
                detection_metrics["keltner_lower"] = round(float(enriched["keltner_lower"].values[-1]), 4)

            payload = build_weekly_rsi_payload(
                code_frame=enriched,
                params=weekly_params,
                detection_metrics=detection_metrics,
            )

            signal = build_signal_dict(
                code=code,
                name=code_to_name.get(code, ""),
                signal_dt=payload["anchor_day_ts"],
                clock_tf="w",
                strategy_group_id=strategy_group_id,
                strategy_name=strategy_name,
                signal_label=payload["signal_summary"],
                payload=payload,
            )

            code_result = StockScanResult(
                code=code,
                name=code_to_name.get(code, ""),
                processed_bars=total_bars,
                signal_count=1,
                signals=[signal],
            )
            total_kept += 1
        except Exception as exc:
            code_result = StockScanResult(
                code=code,
                name=code_to_name.get(code, ""),
                processed_bars=0,
                signal_count=0,
                signals=[],
                error_message=str(exc),
            )
            base_metrics["stock_errors"] += 1

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


# ---------------------------------------------------------------------------
# GPU 批量检测 (detect_batch hook)
# ---------------------------------------------------------------------------


def detect_weekly_rsi_batch(
    columns: dict,
    boundaries: np.ndarray,
    det_kwargs: dict[str, Any],
):
    """GPU 批量检测：对拼接后的多股票 RSI 数据一次性执行超跌判定（可选 ATR 偏离）。

    columns: {col_name: cupy.ndarray} — flat GPU 数组（必含 "rsi"、"close"；
             若 atr_deviation_enabled=True 还需含 "keltner_lower"）
    boundaries: numpy int64 array, shape=(n_stocks+1,)
    det_kwargs: {"params": {...}}
    返回: cupy boolean mask, shape=(total_rows,)
    """
    from strategies.engine_commons import gpu_segmented_minimum_accumulate
    import cupy as cp

    params = det_kwargs["params"]
    rsi_threshold = float(params["rsi_threshold"])
    historical_ratio = float(params["historical_ratio_pct"])

    rsi_values = columns["rsi"]

    # 逐 segment 累积最低 RSI
    rsi_for_min = cp.where(cp.isnan(rsi_values), cp.inf, rsi_values)
    running_min = gpu_segmented_minimum_accumulate(rsi_for_min, boundaries)
    running_min = cp.where(running_min == cp.inf, cp.nan, running_min)

    # 列式布尔掩码
    valid = ~cp.isnan(rsi_values)
    cond_threshold = rsi_values < rsi_threshold
    cond_historical = rsi_values <= running_min * historical_ratio

    matched = valid & cond_threshold & cond_historical

    # 可选 ATR 偏离条件
    if params.get("atr_deviation_enabled", False) and "keltner_lower" in columns:
        close_values = columns["close"]
        keltner_lower = columns["keltner_lower"]
        cond_atr = close_values < keltner_lower
        cond_atr_valid = ~cp.isnan(keltner_lower)
        matched = matched & cond_atr & cond_atr_valid

    return matched


def _get_batch_columns(params: dict[str, Any]) -> list[str]:
    """根据参数动态返回 GPU 批量检测所需的列名列表。

    输入：
    1. params: 已规范化的 RSI + ATR 参数。
    输出：
    1. 列名列表。
    边界条件：
    1. ATR 偏离关闭时仅需 close + rsi。
    2. ATR 偏离启用时追加 keltner_lower。
    """
    cols = ["close", "rsi"]
    if params.get("atr_deviation_enabled", False):
        cols.append("keltner_lower")
    return cols


detect_weekly_rsi_batch._batch_columns = ["close", "rsi", "keltner_lower"]


# ---------------------------------------------------------------------------
# GPU 批量 RSI 特征预计算 (prepare_batch hook)
# ---------------------------------------------------------------------------

_RSI_KERNEL_CODE = r'''
extern "C" __global__
void compute_rsi_kernel(
    const double* close,
    const long long* boundaries,
    const int n_segments,
    const int period,
    double* rsi,
    const int n
) {
    int seg = blockIdx.x * blockDim.x + threadIdx.x;
    if (seg >= n_segments) return;

    long long s = boundaries[seg];
    long long e = boundaries[seg + 1];
    long long seg_len = e - s;

    // Initialize to NaN
    const double nan_val = __longlong_as_double(0x7FF8000000000000LL);
    for (long long i = s; i < e; i++) {
        rsi[i] = nan_val;
    }

    if (seg_len <= period) return;

    // First window: simple average of gains/losses
    double sum_gain = 0.0;
    double sum_loss = 0.0;
    for (int j = 0; j < period; j++) {
        double d = close[s + j + 1] - close[s + j];
        if (d > 0.0) sum_gain += d;
        else sum_loss -= d;
    }

    double avg_gain = sum_gain / period;
    double avg_loss = sum_loss / period;

    if (avg_loss == 0.0) {
        rsi[s + period] = 100.0;
    } else {
        double rs = avg_gain / avg_loss;
        rsi[s + period] = 100.0 - 100.0 / (1.0 + rs);
    }

    // Wilder recursion
    for (long long j = period; j < seg_len - 1; j++) {
        double d = close[s + j + 1] - close[s + j];
        double g = d > 0.0 ? d : 0.0;
        double l = d < 0.0 ? -d : 0.0;
        avg_gain = (avg_gain * (period - 1) + g) / period;
        avg_loss = (avg_loss * (period - 1) + l) / period;
        if (avg_loss == 0.0) {
            rsi[s + j + 1] = 100.0;
        } else {
            double rs = avg_gain / avg_loss;
            rsi[s + j + 1] = 100.0 - 100.0 / (1.0 + rs);
        }
    }
}
'''

_RSI_KERNEL = None


def _get_rsi_kernel():
    global _RSI_KERNEL
    if _RSI_KERNEL is not None:
        return _RSI_KERNEL
    import cupy as cp
    _RSI_KERNEL = cp.RawKernel(_RSI_KERNEL_CODE, 'compute_rsi_kernel')
    return _RSI_KERNEL


# ---------------------------------------------------------------------------
# GPU ATR + SMA CUDA kernel
# ---------------------------------------------------------------------------

_ATR_SMA_KERNEL_CODE = r'''
extern "C" __global__
void compute_atr_sma_kernel(
    const double* high,
    const double* low,
    const double* close,
    const long long* boundaries,
    const int n_segments,
    const int period,
    const double multiplier,
    double* atr,
    double* sma,
    const int n
) {
    /*
     * 每个线程处理一个 segment（一只股票），逐 bar 串行计算：
     * 1. True Range: max(H-L, |H-prevC|, |L-prevC|)
     * 2. ATR: Wilder EMA(TR, period)
     * 3. SMA: 简单移动平均(close, period)
     *
     * ATR[0..period] = NaN（第 0 根无前收盘价，前 period-1 根不足首窗口）
     * SMA[0..period-2] = NaN
     */
    int seg = blockIdx.x * blockDim.x + threadIdx.x;
    if (seg >= n_segments) return;

    long long s = boundaries[seg];
    long long e = boundaries[seg + 1];
    long long seg_len = e - s;

    const double nan_val = __longlong_as_double(0x7FF8000000000000LL);

    // Initialize ATR and SMA to NaN
    for (long long i = s; i < e; i++) {
        atr[i] = nan_val;
        sma[i] = nan_val;
    }

    if (seg_len <= period) return;

    // --- SMA(close, period) ---
    double sum_close = 0.0;
    for (int j = 0; j < period; j++) {
        sum_close += close[s + j];
    }
    sma[s + period - 1] = sum_close / period;
    for (long long j = period; j < seg_len; j++) {
        sum_close += close[s + j] - close[s + j - period];
        sma[s + j] = sum_close / period;
    }

    // --- TR + ATR (Wilder EMA) ---
    // TR starts from index 1 (need prev close), so first valid ATR at index=period
    double sum_tr = 0.0;
    for (int j = 0; j < period; j++) {
        long long idx = s + j + 1;
        double h = high[idx];
        double l = low[idx];
        double pc = close[idx - 1];
        double hl = h - l;
        double hpc = h - pc; if (hpc < 0.0) hpc = -hpc;
        double lpc = l - pc; if (lpc < 0.0) lpc = -lpc;
        double tr = hl;
        if (hpc > tr) tr = hpc;
        if (lpc > tr) tr = lpc;
        sum_tr += tr;
    }
    double avg_tr = sum_tr / period;
    atr[s + period] = avg_tr;

    for (long long j = period; j < seg_len - 1; j++) {
        long long idx = s + j + 1;
        double h = high[idx];
        double l = low[idx];
        double pc = close[idx - 1];
        double hl = h - l;
        double hpc = h - pc; if (hpc < 0.0) hpc = -hpc;
        double lpc = l - pc; if (lpc < 0.0) lpc = -lpc;
        double tr = hl;
        if (hpc > tr) tr = hpc;
        if (lpc > tr) tr = lpc;
        avg_tr = (avg_tr * (period - 1) + tr) / period;
        atr[idx] = avg_tr;
    }
}
'''

_ATR_SMA_KERNEL = None


def _get_atr_sma_kernel():
    """延迟编译并缓存 ATR+SMA CUDA kernel。

    输入：无。
    输出：已编译的 CuPy RawKernel 对象。
    边界条件：首次调用时编译，后续直接返回缓存。
    """
    global _ATR_SMA_KERNEL
    if _ATR_SMA_KERNEL is not None:
        return _ATR_SMA_KERNEL
    import cupy as cp
    _ATR_SMA_KERNEL = cp.RawKernel(_ATR_SMA_KERNEL_CODE, 'compute_atr_sma_kernel')
    return _ATR_SMA_KERNEL


def prepare_weekly_rsi_features_batch(gpu_raw, boundaries, params):
    """GPU 批量计算 Wilder RSI（及可选 ATR/SMA/Keltner）特征列。

    每个 CUDA 线程处理一个 segment（一只股票），在 segment 内顺序计算：
    1. 首窗口简单平均 gains/losses
    2. Wilder 递推平滑

    gpu_raw: {"open", "high", "low", "close", "volume"} CuPy arrays
    boundaries: segment boundaries numpy array
    params: {"rsi_period": int, "atr_deviation_enabled": bool, "atr_period": int, "atr_multiplier": float, ...}

    Returns: {"close": cupy, "rsi": cupy} ;若 atr_deviation_enabled=True 额外含 "keltner_lower"
    """
    import cupy as cp

    rsi_period = int(params["rsi_period"])
    close = gpu_raw["close"]
    n = len(close)

    rsi = cp.empty(n, dtype=cp.float64)

    n_segments = len(boundaries) - 1
    boundaries_gpu = cp.asarray(boundaries.astype(np.int64))

    kernel = _get_rsi_kernel()
    threads_per_block = 256
    blocks = (n_segments + threads_per_block - 1) // threads_per_block

    kernel(
        (blocks,), (threads_per_block,),
        (close, boundaries_gpu, np.int32(n_segments),
         np.int32(rsi_period), rsi, np.int32(n)),
    )

    result = {"close": close, "rsi": rsi}

    # 可选 ATR/SMA/Keltner 特征
    if params.get("atr_deviation_enabled", False):
        atr_period = int(params["atr_period"])
        atr_multiplier = float(params["atr_multiplier"])
        high = gpu_raw["high"]
        low = gpu_raw["low"]

        atr = cp.empty(n, dtype=cp.float64)
        sma = cp.empty(n, dtype=cp.float64)

        atr_sma_kernel = _get_atr_sma_kernel()
        atr_sma_kernel(
            (blocks,), (threads_per_block,),
            (high, low, close, boundaries_gpu, np.int32(n_segments),
             np.int32(atr_period), np.float64(atr_multiplier),
             atr, sma, np.int32(n)),
        )

        keltner_lower = sma - atr_multiplier * atr
        result["keltner_lower"] = keltner_lower

    return result


BACKTEST_HOOKS = {
    "detect": detect_weekly_rsi,
    "detect_vectorized": detect_weekly_rsi_vectorized,
    "detect_batch": detect_weekly_rsi_batch,
    "prepare": prepare_weekly_rsi_features,
    "prepare_batch": prepare_weekly_rsi_features_batch,
    "prepare_key": lambda p: {
        "rsi_period": p["rsi_period"],
        "atr_deviation_enabled": p.get("atr_deviation_enabled", False),
        "atr_period": p.get("atr_period", 14),
    },
    "normalize_params": _normalize_for_backtest,
    "tf_sections": {
        "weekly": {"tf_key": "w", "table": "klines_w"},
    },
    "tf_logic": "or",
}
