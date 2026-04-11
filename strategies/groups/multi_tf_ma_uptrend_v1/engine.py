"""
`multi_tf_ma_uptrend_v1` specialized engine。

职责：
1. 按周线/日线/60分钟/15分钟四个可选周期，批量加载 K 线数据并计算 MA 均线及其百分比斜率。
2. 对每只股票在所有已启用周期上分别检查：
   a. 【必选】最新 n 根线中 MA 均线斜率百分比落在 [slope_min, slope_max] 区间；
   b. 【可选】连续筛选——所有不重叠窗口斜率均满足区间；
   c. 【可选】收盘偏离——最后一根线收盘价距指定 MA 的偏离幅度在阈值内；
   d. 【可选】收敛震荡——最后 p 根线的最高/最低振幅小于阈值；
   e. 【可选】收盘偏离上限——扫描区间内每根线收盘价距主 MA 的偏离幅度不超过阈值（仅筛选模式）。
3. 仅当所有已启用周期全部通过时产生信号。

斜率公式（百分比）：
  slope_pct = 100 × (MA(t) − MA(t−m)) / (MA(t−m) × m)
  其中 m = slope_gap。
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

STRATEGY_LABEL = "多周期均线向上 v1"

# 周期 key → klines 表名 的映射
_TF_TABLE: dict[str, str] = {
    "w": "klines_w",
    "d": "klines_d",
    "60": "klines_60",
    "15": "klines_15",
}

# 周期粗细排序（用于选取信号展示周期）
_TF_ORDER: list[str] = ["w", "d", "60", "15"]

# 周期 key → manifest 中 default_params 参数组的 key（manifest 中用 min60/min15 避免纯数字 key）
_PARAM_SECTION_TO_TF: dict[str, str] = {
    "weekly": "w",
    "daily": "d",
    "min60": "60",
    "min15": "15",
}

_TF_TO_PARAM_SECTION: dict[str, str] = {v: k for k, v in _PARAM_SECTION_TO_TF.items()}

# 周期中文名（用于信号标签）
_TF_LABEL: dict[str, str] = {
    "w": "周线",
    "d": "日线",
    "60": "60min",
    "15": "15min",
}


# ---------------------------------------------------------------------------
# 参数规范化
# ---------------------------------------------------------------------------


def _normalize_tf_params(group_params: dict[str, Any], section_key: str) -> dict[str, Any]:
    """规范化单个周期参数组。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    2. section_key: manifest 参数组 key（weekly / daily / min60 / min15）。
    输出：
    1. 返回经类型转换和边界裁剪后的周期参数字典。
    用途：
    1. 统一处理四个周期的相同参数结构。
    边界条件：
    1. slope_max 强制 ≥ slope_min。
    2. close_deviation_pct 裁剪到 [0, 100]。
    3. consolidation_max_amp_pct 裁剪到 [0.1, 100]。
    """
    raw = as_dict(group_params.get(section_key))
    slope_min = as_float(raw.get("slope_min"), 10.0)
    return {
        "enabled": as_bool(raw.get("enabled"), False),
        "n": as_int(raw.get("n"), 20, minimum=2, maximum=500),
        "ma_period": as_int(raw.get("ma_period"), 10, minimum=2, maximum=250),
        "slope_gap": as_int(raw.get("slope_gap"), 4, minimum=1, maximum=200),
        "slope_min": slope_min,
        "slope_max": as_float(raw.get("slope_max"), 20.0, minimum=slope_min),
        "continuous_check": as_bool(raw.get("continuous_check"), False),
        "close_deviation_enabled": as_bool(raw.get("close_deviation_enabled"), False),
        "close_deviation_ma_period": as_int(raw.get("close_deviation_ma_period"), 10, minimum=2, maximum=250),
        "close_deviation_pct": as_float(raw.get("close_deviation_pct"), 5.0, minimum=0.0, maximum=100.0),
        "consolidation_enabled": as_bool(raw.get("consolidation_enabled"), False),
        "consolidation_bars": as_int(raw.get("consolidation_bars"), 3, minimum=2, maximum=100),
        "consolidation_max_amp_pct": as_float(raw.get("consolidation_max_amp_pct"), 3.0, minimum=0.1, maximum=100.0),
        "max_close_deviation_enabled": as_bool(raw.get("max_close_deviation_enabled"), False),
        "max_close_deviation_pct": as_float(raw.get("max_close_deviation_pct"), 20.0, minimum=0.1, maximum=200.0),
    }


# ---------------------------------------------------------------------------
# 数据加载
# ---------------------------------------------------------------------------


def _required_history_bars(params: dict[str, Any]) -> int:
    """计算单个周期命中规则所需的最少 K 线根数（含 MA 计算前缀）。

    输入：
    1. params: 已规范化的单周期参数。
    输出：
    1. 在连续筛选模式下需 ma_period + slope_gap × floor(n / slope_gap) 根；
       非连续模式仅需 ma_period + slope_gap 根。
    用途：
    1. 用于计算数据加载的起始日期偏移。
    边界条件：
    1. 多出一定余量（+slope_gap）以确保 MA shift 不缺值。
    """
    ma_period = int(params["ma_period"])
    slope_gap = int(params["slope_gap"])
    n = int(params["n"])
    if params["continuous_check"]:
        windows = max(n // slope_gap, 1)
        return ma_period + slope_gap * windows
    return ma_period + slope_gap


def _bars_to_days(bars: int, tf_key: str) -> int:
    """将 K 线根数估算为自然日天数。

    输入：
    1. bars: K 线根数。
    2. tf_key: 周期标识。
    输出：
    1. 返回包含安全余量的自然日天数。
    用途：
    1. 在构造 SQL 日期范围时把 K 线根数转换为日历天数。
    边界条件：
    1. 除以交易日比例后加 30 天安全余量。
    """
    if tf_key == "w":
        return bars * 7 + 60
    if tf_key == "d":
        return int(bars * 1.5) + 30
    if tf_key == "60":
        return int(bars / 4 * 1.5) + 30
    # 15min: ~16 bars/day
    return int(bars / 16 * 1.5) + 30


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
    3. tf_key: 周期标识（w/d/60/15）。
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
# 特征计算
# ---------------------------------------------------------------------------


def prepare_multi_tf_ma_features(frame: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """计算主 MA、偏离 MA 及百分比斜率列（列式特征预计算）。

    输入：
    1. frame: 按 code+ts 排序的 OHLCV DataFrame。
    2. params: 已规范化的单周期参数。
    输出：
    1. 返回新增 `ma`、`ma_dev`（偏离 MA）、`slope_pct` 列后的 DataFrame。
    用途：
    1. 筛选模式：数据加载后调用一次，后续 detect 直接使用已计算的特征列。
    2. 回测模式：调用一次后滑窗 N 次 detect，避免重复计算 MA/斜率。
    边界条件：
    1. 空表直接原样返回。
    2. 偏离 MA 仅在 close_deviation_enabled 为 True 时计算（否则与 ma 相同）。
    """
    if frame.empty:
        return frame

    ma_period = int(params["ma_period"])
    slope_gap = int(params["slope_gap"])
    dev_ma_period = int(params["close_deviation_ma_period"]) if params["close_deviation_enabled"] else ma_period

    prepared: list[pd.DataFrame] = []
    for _, code_frame in frame.groupby("code", sort=False):
        part = code_frame.sort_values("ts").copy()
        part["ma"] = part["close"].rolling(ma_period, min_periods=ma_period).mean()
        ma_shifted = part["ma"].shift(slope_gap)
        # 百分比斜率：100 × (MA(t) − MA(t−m)) / (MA(t−m) × m)
        part["slope_pct"] = 100.0 * (part["ma"] - ma_shifted) / (ma_shifted * slope_gap)
        # 偏离 MA（可能与主 MA 周期不同）
        if dev_ma_period != ma_period:
            part["ma_dev"] = part["close"].rolling(dev_ma_period, min_periods=dev_ma_period).mean()
        else:
            part["ma_dev"] = part["ma"]
        prepared.append(part)
    return pd.concat(prepared, axis=0, ignore_index=True)


prepare_multi_tf_ma_features._multi_stock = True


# ---------------------------------------------------------------------------
# 条件检查
# ---------------------------------------------------------------------------


def _check_slope(code_frame: pd.DataFrame, params: dict[str, Any]) -> tuple[bool, list[float], str]:
    """检查最新 n 根线中 MA 斜率是否满足区间条件。

    输入：
    1. code_frame: 单只股票的有特征列的 DataFrame。
    2. params: 已规范化的单周期参数。
    输出：
    1. (是否通过, 各窗口斜率列表, 失败原因描述)。
    用途：
    1. 必选斜率检查 + 可选连续窗口。
    边界条件：
    1. 非连续模式只检查最新一个窗口（即 t 处的斜率）。
    2. 连续模式按 slope_gap 步长向前采样 floor(n/slope_gap) 个窗口，全部需满足。
    3. 数据不足直接返回 False。
    """
    n = int(params["n"])
    slope_gap = int(params["slope_gap"])
    slope_min = float(params["slope_min"])
    slope_max = float(params["slope_max"])
    continuous = params["continuous_check"]

    # 数据窗口由调用方截取，此处直接使用
    slopes = code_frame["slope_pct"].dropna()

    if slopes.empty:
        return False, [], "斜率数据不足"

    if not continuous:
        # 非连续：只检查最新一个值
        latest_slope = float(slopes.iloc[-1])
        passed = slope_min <= latest_slope <= slope_max
        reason = "" if passed else f"斜率 {latest_slope:.2f}% 不在 [{slope_min}, {slope_max}]"
        return passed, [latest_slope], reason

    # 连续模式：从末尾向前步进 slope_gap，采样 floor(n/slope_gap) 个窗口端点
    window_count = max(n // slope_gap, 1)
    sampled: list[float] = []
    required_points = 1 + (window_count - 1) * slope_gap
    if len(slopes) < required_points:
        return False, [], f"连续检查需要 {required_points} 个有效斜率值，仅有 {len(slopes)} 个"

    for offset in range(window_count):
        idx = len(slopes) - 1 - offset * slope_gap
        sampled.append(float(slopes.iloc[idx]))
    sampled.reverse()

    passed = all(slope_min <= v <= slope_max for v in sampled)
    reason = "" if passed else f"连续窗口中存在超出 [{slope_min}, {slope_max}] 的斜率"
    return passed, sampled, reason


def _check_close_deviation(code_frame: pd.DataFrame, params: dict[str, Any]) -> tuple[bool, float, str]:
    """检查最后一根线的收盘价距离指定 MA 均线的偏离幅度。

    输入：
    1. code_frame: 单只股票的有特征列的 DataFrame。
    2. params: 已规范化的单周期参数。
    输出：
    1. (是否通过, 实际偏离百分比, 失败原因描述)。
    用途：
    1. 可选条件：收盘偏离筛选。
    边界条件：
    1. MA 值为 NaN 或 ≤0 时返回 False。
    """
    if not params["close_deviation_enabled"]:
        return True, 0.0, ""

    latest = code_frame.iloc[-1]
    ma_val = latest["ma_dev"]
    if pd.isna(ma_val) or float(ma_val) <= 0:
        return False, 0.0, "偏离 MA 值无效"

    close_val = float(latest["close"])
    deviation_pct = abs(close_val - float(ma_val)) / float(ma_val) * 100.0
    limit = float(params["close_deviation_pct"])
    passed = deviation_pct <= limit
    reason = "" if passed else f"偏离 {deviation_pct:.2f}% 超过阈值 {limit}%"
    return passed, deviation_pct, reason


def _check_consolidation(code_frame: pd.DataFrame, params: dict[str, Any]) -> tuple[bool, float, str]:
    """检查最后 p 根线的振幅是否在收敛区间内。

    输入：
    1. code_frame: 单只股票有 OHLC 列的 DataFrame。
    2. params: 已规范化的单周期参数。
    输出：
    1. (是否通过, 实际振幅百分比, 失败原因描述)。
    用途：
    1. 可选条件：收敛震荡筛选。
    边界条件：
    1. min_low ≤ 0 时返回 False（防止除零）。
    2. 数据不足 p 根线时返回 False。
    """
    if not params["consolidation_enabled"]:
        return True, 0.0, ""

    p = int(params["consolidation_bars"])
    if len(code_frame) < p:
        return False, 0.0, f"收敛检查需 {p} 根线，仅有 {len(code_frame)} 根"

    tail = code_frame.tail(p)
    max_high = float(tail["high"].max())
    min_low = float(tail["low"].min())
    if min_low <= 0:
        return False, 0.0, "最低价为零，无法计算振幅"

    amp_pct = (max_high - min_low) / min_low * 100.0
    limit = float(params["consolidation_max_amp_pct"])
    passed = amp_pct < limit
    reason = "" if passed else f"振幅 {amp_pct:.2f}% ≥ 阈值 {limit}%"
    return passed, amp_pct, reason


def _check_max_close_deviation(code_frame: pd.DataFrame, params: dict[str, Any]) -> tuple[bool, float, str]:
    """检查扫描区间内每根线的收盘价是否偏离主 MA 超过阈值。

    输入：
    1. code_frame: 单只股票的有特征列的 DataFrame（含 close、ma 列）。
    2. params: 已规范化的单周期参数。
    输出：
    1. (是否通过, 最大偏离百分比, 失败原因描述)。
    用途：
    1. 可选条件：排除扫描区间内曾大幅偏离均线的股票（仅筛选模式使用）。
    边界条件：
    1. max_close_deviation_enabled 为 False 时直接返回通过。
    2. MA 值存在 NaN 或 ≤ 0 的行会被跳过，不影响其余行的判断。
    3. 全部 MA 值无效时返回 False。
    """
    if not params.get("max_close_deviation_enabled", False):
        return True, 0.0, ""

    closes = code_frame["close"].values
    ma_values = code_frame["ma"].values
    valid = (~np.isnan(ma_values)) & (ma_values > 0)
    if not valid.any():
        return False, 0.0, "MA 全部无效，无法检查收盘价偏离"

    dev_pct = np.abs(closes[valid] - ma_values[valid]) / ma_values[valid] * 100.0
    max_dev = float(np.nanmax(dev_pct))
    limit = float(params.get("max_close_deviation_pct", 20.0))
    passed = max_dev <= limit
    reason = "" if passed else f"收盘价最大偏离 {max_dev:.2f}% 超过阈值 {limit}%"
    return passed, max_dev, reason


# ---------------------------------------------------------------------------
# 单股扫描
# ---------------------------------------------------------------------------


def detect_multi_tf_ma_uptrend(
    code_frame: pd.DataFrame,
    params: dict[str, Any],
    tf_key: str,
    *,
    metrics_only: bool = False,
) -> DetectionResult:
    """对单只股票的单个周期执行全部条件检查。

    输入：
    1. code_frame: 单只股票在该周期的特征 DataFrame（含 ma/slope_pct/ma_dev 列）。
    2. params: 已规范化的单周期参数。
    3. tf_key: 周期标识。
    4. metrics_only: 为 True 时跳过条件门控、只计算 metrics（供向量化命中后
       收集前端 payload 数据使用）。
    输出：
    1. DetectionResult，matched 表示是否全部通过，metrics 含斜率/偏离/收敛明细。
    用途：
    1. 汇总必选 + 可选条件结果。
    2. 可被筛选编排器和未来回测引擎独立调用。
    边界条件：
    1. code_frame 必须已包含特征列（由 prepare_multi_tf_ma_features 预计算）。
    2. 数据为空直接返回 matched=False。
    """
    metrics: dict[str, Any] = {"tf": tf_key, "enabled": True}

    if code_frame.empty:
        metrics["reason"] = "无数据"
        return DetectionResult(matched=False, metrics=metrics)

    # 必选：斜率检查
    slope_ok, slope_values, slope_reason = _check_slope(code_frame, params)
    metrics["slope_passed"] = slope_ok
    metrics["slope_values"] = [round(v, 4) for v in slope_values]
    if not metrics_only and not slope_ok:
        metrics["reason"] = slope_reason
        return DetectionResult(matched=False, metrics=metrics)

    # 可选：收盘偏离
    dev_ok, dev_pct, dev_reason = _check_close_deviation(code_frame, params)
    metrics["close_deviation_passed"] = dev_ok
    metrics["close_deviation_pct"] = round(dev_pct, 4)
    if not metrics_only and not dev_ok:
        metrics["reason"] = dev_reason
        return DetectionResult(matched=False, metrics=metrics)

    # 可选：收敛震荡
    cons_ok, cons_pct, cons_reason = _check_consolidation(code_frame, params)
    metrics["consolidation_passed"] = cons_ok
    metrics["consolidation_amp_pct"] = round(cons_pct, 4)
    if not metrics_only and not cons_ok:
        metrics["reason"] = cons_reason
        return DetectionResult(matched=False, metrics=metrics)

    # 可选：收盘价偏离上限（仅筛选模式）
    hd_ok, hd_max_pct, hd_reason = _check_max_close_deviation(code_frame, params)
    metrics["max_close_deviation_passed"] = hd_ok
    metrics["max_close_deviation_max_pct"] = round(hd_max_pct, 4)
    if not metrics_only and not hd_ok:
        metrics["reason"] = hd_reason
        return DetectionResult(matched=False, metrics=metrics)

    n = int(params["n"])
    i = len(code_frame) - 1
    return DetectionResult(
        matched=True,
        pattern_start_idx=max(i - n + 1, 0),
        pattern_end_idx=i,
        pattern_start_ts=code_frame.iloc[max(i - n + 1, 0)]["ts"],
        pattern_end_ts=code_frame.iloc[i]["ts"],
        metrics=metrics,
    )


def detect_multi_tf_ma_uptrend_vectorized(
    code_frame: pd.DataFrame,
    params: dict[str, Any],
    tf_key: str,
) -> list[DetectionResult]:
    """在单只股票的完整历史上一次性检测所有均线向上命中点。

    与 detect_multi_tf_ma_uptrend 的区别：
    - detect_multi_tf_ma_uptrend 只检查最新 n 根线的条件，
      供筛选模式和回测外层滑窗使用。
    - 本函数对全部K线做列式运算，收集所有命中记录，
      回测引擎无需再用外层 while 滑窗逐 bar 推进。

    输入：
    1. code_frame: 单只股票完整特征 DataFrame（已按 ts 排序，含 ma/slope_pct/ma_dev 列）。
    2. params: 已规范化的单周期参数。
    3. tf_key: 周期标识。
    输出：
    1. DetectionResult 列表，每个元素含 pattern_start/end idx/ts。
    边界条件：
    1. code_frame 必须已包含 ma/slope_pct/ma_dev 列（由 prepare_multi_tf_ma_features 预计算）。
    2. 空数据或禁用时返回空列表。
    """
    if code_frame.empty or not params.get("enabled", True):
        return []

    n_param = int(params["n"])
    slope_gap = int(params["slope_gap"])
    slope_min = float(params["slope_min"])
    slope_max = float(params["slope_max"])
    continuous = params["continuous_check"]

    xp = np
    ts_values = code_frame["ts"].values
    slope_values = code_frame["slope_pct"].values.astype(np.float64)
    n_total = len(code_frame)

    # ── 斜率条件 ──
    slope_in_range = (~np.isnan(slope_values)
                      & (slope_values >= slope_min)
                      & (slope_values <= slope_max))

    if continuous:
        window_count = max(n_param // slope_gap, 1)
        cond_slope = slope_in_range.copy()
        for k in range(1, window_count):
            offset = k * slope_gap
            shifted = np.zeros(n_total, dtype=bool)
            shifted[offset:] = slope_in_range[:-offset]
            cond_slope &= shifted
    else:
        cond_slope = slope_in_range

    # ── 收盘偏离条件 ──
    if params["close_deviation_enabled"]:
        closes = code_frame["close"].values.astype(np.float64)
        ma_dev_values = code_frame["ma_dev"].values.astype(np.float64)
        limit_pct = float(params["close_deviation_pct"])
        with np.errstate(divide="ignore", invalid="ignore"):
            dev_pct = np.abs(closes - ma_dev_values) / ma_dev_values * 100.0
        cond_dev = (~np.isnan(ma_dev_values)
                    & (ma_dev_values > 0)
                    & (dev_pct <= limit_pct))
    else:
        cond_dev = np.ones(n_total, dtype=bool)

    # ── 收敛震荡条件 ──
    if params["consolidation_enabled"]:
        p = int(params["consolidation_bars"])
        amp_limit = float(params["consolidation_max_amp_pct"])
        highs = code_frame["high"].values.astype(np.float64)
        lows = code_frame["low"].values.astype(np.float64)
        if n_total >= p:
            rolling_max_h = pd.Series(highs).rolling(p).max().values
            rolling_min_l = pd.Series(lows).rolling(p).min().values
        else:
            rolling_max_h = np.full(n_total, np.nan)
            rolling_min_l = np.full(n_total, np.nan)
        safe_min_l = np.where(rolling_min_l > 0, rolling_min_l, np.nan)
        with np.errstate(divide="ignore", invalid="ignore"):
            amp_pct = (rolling_max_h - rolling_min_l) / safe_min_l * 100.0
        cond_cons = (~np.isnan(amp_pct)) & (amp_pct < amp_limit)
    else:
        cond_cons = np.ones(n_total, dtype=bool)

    matched = cond_slope & cond_dev & cond_cons

    hit_indices = np.where(matched)[0]
    results: list[DetectionResult] = []
    for i in hit_indices:
        ii = int(i)
        start_idx = max(ii - n_param + 1, 0)
        results.append(DetectionResult(
            matched=True,
            pattern_start_idx=start_idx,
            pattern_end_idx=ii,
            pattern_start_ts=ts_values[start_idx],
            pattern_end_ts=ts_values[ii],
            metrics={"tf": tf_key},
        ))
    return results


def _build_signal_label(enabled_tfs: list[str], per_tf_detail: dict[str, dict[str, Any]], all_params: dict[str, dict[str, Any]]) -> str:
    """构建信号标签：拼接各启用周期的摘要。

    输入：
    1. enabled_tfs: 已启用的周期 key 列表。
    2. per_tf_detail: 各周期检查结果 dict。
    3. all_params: 各周期参数 dict。
    输出：
    1. 如 "周线MA10斜率12.3% + 日线MA20斜率18.5%"。
    边界条件：
    1. 斜率值取最新一个窗口（列表最后一个元素）。
    """
    parts: list[str] = []
    for tf in enabled_tfs:
        detail = per_tf_detail.get(tf, {})
        params = all_params.get(tf, {})
        ma_period = params.get("ma_period", "?")
        slopes = detail.get("slope_values", [])
        latest_slope = f"{slopes[-1]:.1f}%" if slopes else "n/a"
        parts.append(f"{_TF_LABEL.get(tf, tf)}MA{ma_period}斜率{latest_slope}")
    return " + ".join(parts) if parts else STRATEGY_LABEL


def build_multi_tf_ma_uptrend_payload(
    *,
    enabled_tfs: list[str],
    per_tf_detail: dict[str, dict[str, Any]],
    tf_data: dict[str, pd.DataFrame],
    all_params: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """根据各周期检测结果组装前端渲染 payload。

    输入：
    1. enabled_tfs: 已启用的周期 key 列表（全部通过）。
    2. per_tf_detail: 各周期检查结果 dict。
    3. tf_data: 各周期的 DataFrame。
    4. all_params: 各周期的已规范化参数。
    输出：
    1. 符合图表渲染合同的 payload dict。
    边界条件：
    1. 仅在全部周期通过时调用。
    """
    clock_tf = coarsest_tf(enabled_tfs)
    clock_frame = tf_data[clock_tf]
    latest_ts = clock_frame.iloc[-1]["ts"]

    # 展示窗口 = 最粗周期的最近 n 根线
    n_coarsest = int(all_params[clock_tf]["n"])
    window_start_idx = max(len(clock_frame) - n_coarsest, 0)
    window_start_ts = clock_frame.iloc[window_start_idx]["ts"]
    window_end_ts = latest_ts

    signal_label = _build_signal_label(enabled_tfs, per_tf_detail, all_params)

    return {
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "chart_interval_start_ts": window_start_ts,
        "chart_interval_end_ts": window_end_ts,
        "anchor_day_ts": latest_ts,
        "enabled_timeframes": enabled_tfs,
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
    """扫描单只股票：在所有已启用周期上执行条件检查。

    输入：
    1. code/name: 股票标识。
    2. tf_data: {tf_key: 该股票的特征 DataFrame} 仅包含已启用周期。
    3. all_params: {tf_key: 已规范化参数} 仅包含已启用周期。
    4. enabled_tfs: 按粗→细排序的已启用周期列表。
    5. strategy_group_id/strategy_name: 构建信号用。
    输出：
    1. (StockScanResult, 统计计数 dict)。
    边界条件：
    1. 任一启用周期不通过则整体无信号。
    """
    stats = {"candidate": 0, "kept": 0}
    total_bars = sum(len(df) for df in tf_data.values())

    per_tf_detail: dict[str, dict[str, Any]] = {}
    all_passed = True
    for tf in enabled_tfs:
        frame = tf_data.get(tf, pd.DataFrame())
        params = all_params[tf]
        # 筛选模式：按 scope 参数截取数据窗口
        n = int(params.get("n", len(frame)))
        frame_window = frame.tail(n)
        result = detect_multi_tf_ma_uptrend(frame_window, params, tf)
        detail = {"passed": result.matched, **result.metrics}
        per_tf_detail[tf] = detail
        if not result.matched:
            all_passed = False
            break

    stats["candidate"] = 1
    if not all_passed:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=total_bars,
            signal_count=0,
            signals=[],
        ), stats

    # 全部通过 → 生成信号
    stats["kept"] = 1
    payload = build_multi_tf_ma_uptrend_payload(
        enabled_tfs=enabled_tfs,
        per_tf_detail=per_tf_detail,
        tf_data=tf_data,
        all_params=all_params,
    )
    clock_tf = coarsest_tf(enabled_tfs)

    signal = build_signal_dict(
        code=code,
        name=name,
        signal_dt=payload["anchor_day_ts"],
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


def run_multi_tf_ma_uptrend_v1_specialized(
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
    """`multi_tf_ma_uptrend_v1` specialized 主入口。

    输入：
    1. 所有参数由 TaskManager 以关键字方式传入。
    输出：
    1. 返回 (result_map, metrics)，供 TaskManager 写库和前端展示使用。
    用途：
    1. 组织参数规范化、各周期数据加载与特征计算、逐股扫描和汇总统计。
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

    # 针对每个已启用周期计算起始日期并批量加载
    tf_frames: dict[str, pd.DataFrame] = {}
    tf_load_times: dict[str, float] = {}

    for tf in enabled_tfs:
        params = all_tf_params[tf]
        bars_needed = _required_history_bars(params) + int(params["n"])
        days_back = _bars_to_days(bars_needed, tf)
        raw_start = start_ts.date() if start_ts is not None else (end_dt - timedelta(days=days_back)).date()
        # 确保回看足够
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

        if not frame.empty:
            frame = prepare_multi_tf_ma_features(frame, params)
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
                per_code_data[code_val][tf] = code_frame.copy()

    # ── 向量化批量检测 ──
    scan_start = time.perf_counter()

    # 完全复刻 detect_multi_tf_ma_uptrend() 全部条件，pandas 向量化替代逐股循环 (AND)
    _batch_hits: set[str] = set(codes)
    for tf in enabled_tfs:
        frame = tf_frames[tf]
        if frame.empty:
            _batch_hits = set()
            break
        _p = all_tf_params[tf]
        _slope_min = float(_p["slope_min"])
        _slope_max = float(_p["slope_max"])
        _continuous = _p["continuous_check"]
        _n = int(_p["n"])
        _slope_gap = int(_p["slope_gap"])

        _g = frame.groupby("code", sort=False)

        if not _continuous:
            _last = _g.last()
            _slope_ok = (
                _last["slope_pct"].notna()
                & (_last["slope_pct"] >= _slope_min)
                & (_last["slope_pct"] <= _slope_max)
            )
        else:
            _wc = max(_n // _slope_gap, 1)
            _slope_ok = pd.Series(True, index=_g.last().index)
            for _offset in range(_wc):
                _sv = _g["slope_pct"].shift(_offset * _slope_gap)
                _sv_last = _sv.groupby(frame["code"], sort=False).last()
                _slope_ok &= _sv_last.notna() & (_sv_last >= _slope_min) & (_sv_last <= _slope_max)
            _last = _g.last()

        _ok = _slope_ok

        if _p["close_deviation_enabled"]:
            _ma_dev = _last["ma_dev"]
            _safe_ma = _ma_dev.where(_ma_dev > 0, np.nan)
            _dev_pct = (_last["close"] - _safe_ma).abs() / _safe_ma * 100
            _ok &= _dev_pct.notna() & (_dev_pct <= float(_p["close_deviation_pct"]))

        if _p["consolidation_enabled"]:
            _cp = int(_p["consolidation_bars"])
            _cl = float(_p["consolidation_max_amp_pct"])
            _tail_p = frame.groupby("code", sort=False).tail(_cp)
            _rmax = _tail_p.groupby("code", sort=False)["high"].max()
            _rmin = _tail_p.groupby("code", sort=False)["low"].min()
            _safe_rmin = _rmin.where(_rmin > 0, np.nan)
            _amp = (_rmax - _rmin) / _safe_rmin * 100
            _ok &= _amp.notna() & (_amp < _cl)

        _batch_hits &= set(_last.index[_ok])

        # 可选：收盘价偏离上限（仅筛选模式）
        if _p["max_close_deviation_enabled"] and _batch_hits:
            _hd_limit = float(_p["max_close_deviation_pct"])
            _tail_n = frame.groupby("code", sort=False).tail(_n)
            _safe_ma_hd = _tail_n["ma"].where(_tail_n["ma"] > 0, np.nan)
            _hd_pct = (_tail_n["close"] - _safe_ma_hd).abs() / _safe_ma_hd * 100
            _tail_n_with_dev = _tail_n.assign(_hd_pct=_hd_pct)
            _hd_max = _tail_n_with_dev.groupby("code", sort=False)["_hd_pct"].max()
            _hd_pass = _hd_max.notna() & (_hd_max <= _hd_limit)
            _batch_hits &= set(_hd_pass.index[_hd_pass])
        if not _batch_hits:
            break

    result_map: dict[str, StockScanResult] = {}
    total_candidates = len(codes)
    total_kept = 0

    for code in codes:
        if code not in _batch_hits:
            continue

        code_tf_data = per_code_data.get(code, {})
        # 补齐缺失周期为空 DataFrame
        for tf in enabled_tfs:
            if tf not in code_tf_data:
                code_tf_data[tf] = pd.DataFrame()

        try:
            # 向量化已确认全部周期通过，仅收集 metrics 构建 payload
            total_bars = sum(len(df) for df in code_tf_data.values())
            per_tf_detail: dict[str, dict[str, Any]] = {}
            for tf in enabled_tfs:
                frame = code_tf_data.get(tf, pd.DataFrame())
                params = all_tf_params[tf]
                n = int(params.get("n", len(frame)))
                frame_window = frame.tail(n)
                result = detect_multi_tf_ma_uptrend(
                    frame_window, params, tf, metrics_only=True,
                )
                per_tf_detail[tf] = {"passed": True, **result.metrics}

            payload = build_multi_tf_ma_uptrend_payload(
                enabled_tfs=enabled_tfs,
                per_tf_detail=per_tf_detail,
                tf_data=code_tf_data,
                all_params={tf: all_tf_params[tf] for tf in enabled_tfs},
            )
            clock_tf = coarsest_tf(enabled_tfs)

            signal = build_signal_dict(
                code=code,
                name=code_to_name.get(code, ""),
                signal_dt=payload["anchor_day_ts"],
                clock_tf=clock_tf,
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
    return {
        "params": _normalize_tf_params(group_params, section_key),
        "tf_key": _PARAM_SECTION_TO_TF[section_key],
    }


# ---------------------------------------------------------------------------
# GPU 批量检测 (detect_batch hook)
# ---------------------------------------------------------------------------


def detect_multi_tf_ma_uptrend_batch(
    columns: dict,
    boundaries: np.ndarray,
    det_kwargs: dict[str, Any],
):
    """GPU 批量检测：对拼接后的多股票数据一次性执行均线向上布尔运算。

    columns: {col_name: cupy.ndarray} — flat GPU 数组
    boundaries: numpy int64 array, shape=(n_stocks+1,)
    det_kwargs: {"params": {...}, "tf_key": ...}
    返回: cupy boolean mask, shape=(total_rows,)
    """
    from strategies.engine_commons import (
        gpu_segmented_rolling_max,
        gpu_segmented_rolling_min,
        gpu_segmented_shift,
    )
    import cupy as cp

    params = det_kwargs["params"]
    if not params.get("enabled", True):
        return cp.zeros(len(columns["close"]), dtype=bool)

    n_param = int(params["n"])
    slope_gap = int(params["slope_gap"])
    slope_min = float(params["slope_min"])
    slope_max = float(params["slope_max"])
    continuous = params["continuous_check"]

    slope_values = columns["slope_pct"]
    n_total = len(slope_values)

    # ── 斜率条件 ──
    slope_in_range = (~cp.isnan(slope_values)
                      & (slope_values >= slope_min)
                      & (slope_values <= slope_max))

    if continuous:
        window_count = max(n_param // slope_gap, 1)
        cond_slope = slope_in_range.copy()
        for k in range(1, window_count):
            offset = k * slope_gap
            shifted = cp.zeros(n_total, dtype=bool)
            if offset < n_total:
                shifted[offset:] = slope_in_range[:-offset]
            # 修正跨段：per-segment 前 offset 个位置置 False
            for seg_i in range(len(boundaries) - 1):
                seg_s = int(boundaries[seg_i])
                seg_reset_end = min(seg_s + offset, int(boundaries[seg_i + 1]))
                shifted[seg_s:seg_reset_end] = False
            cond_slope &= shifted
    else:
        cond_slope = slope_in_range

    # ── 收盘偏离条件 ──
    if params["close_deviation_enabled"]:
        closes = columns["close"]
        ma_dev_values = columns["ma_dev"]
        limit_pct = float(params["close_deviation_pct"])
        dev_pct = cp.abs(closes - ma_dev_values) / cp.where(ma_dev_values > 0, ma_dev_values, 1.0) * 100.0
        cond_dev = (~cp.isnan(ma_dev_values)
                    & (ma_dev_values > 0)
                    & (dev_pct <= limit_pct))
    else:
        cond_dev = cp.ones(n_total, dtype=bool)

    # ── 收敛震荡条件 ──
    if params["consolidation_enabled"]:
        p = int(params["consolidation_bars"])
        amp_limit = float(params["consolidation_max_amp_pct"])
        highs = columns["high"]
        lows = columns["low"]
        if n_total >= p:
            rolling_max_h = gpu_segmented_rolling_max(highs, boundaries, p)
            rolling_min_l = gpu_segmented_rolling_min(lows, boundaries, p)
        else:
            rolling_max_h = cp.full(n_total, cp.nan)
            rolling_min_l = cp.full(n_total, cp.nan)
        safe_min_l = cp.where(rolling_min_l > 0, rolling_min_l, cp.nan)
        amp_pct = (rolling_max_h - rolling_min_l) / safe_min_l * 100.0
        cond_cons = (~cp.isnan(amp_pct)) & (amp_pct < amp_limit)
    else:
        cond_cons = cp.ones(n_total, dtype=bool)

    return cond_slope & cond_dev & cond_cons


def _get_multi_tf_batch_columns(det_kwargs: dict[str, Any]) -> list[str]:
    """根据参数动态决定 batch 需要的列。"""
    cols = ["close", "slope_pct"]
    params = det_kwargs.get("params", {})
    if params.get("close_deviation_enabled"):
        cols.append("ma_dev")
    if params.get("consolidation_enabled"):
        cols.extend(["high", "low"])
    return cols


detect_multi_tf_ma_uptrend_batch._batch_columns = ["open", "high", "low", "close", "volume", "ma", "slope_pct", "ma_dev"]


def prepare_multi_tf_ma_features_batch(
    gpu_raw: dict,
    boundaries: np.ndarray,
    params: dict[str, Any],
) -> dict:
    """GPU 原生特征计算：直接在 GPU 上计算 MA / 斜率 / 偏离 MA。

    gpu_raw:  {"open": cp, "high": cp, "low": cp, "close": cp, "volume": cp}
    boundaries: numpy int64 array, shape=(n_stocks+1,)
    params:   已规范化的单周期参数
    返回: 包含原始列 + 特征列的新 dict（浅拷贝原始列）
    """
    from strategies.engine_commons import (
        gpu_segmented_rolling_mean,
        gpu_segmented_shift,
    )
    import cupy as cp

    ma_period = int(params["ma_period"])
    slope_gap = int(params["slope_gap"])
    dev_enabled = params.get("close_deviation_enabled", False)
    dev_ma_period = int(params.get("close_deviation_ma_period", ma_period)) if dev_enabled else ma_period

    close = gpu_raw["close"]

    # MA
    ma = gpu_segmented_rolling_mean(close, boundaries, ma_period)

    # 百分比斜率: 100 × (MA(t) − MA(t−m)) / (MA(t−m) × m)
    ma_shifted = gpu_segmented_shift(ma, boundaries, slope_gap)
    safe_shifted = cp.where(ma_shifted > 0, ma_shifted, 1.0)
    slope_pct = 100.0 * (ma - ma_shifted) / (safe_shifted * slope_gap)
    # MA(t−m) 无效时 slope_pct 也应为 NaN
    slope_pct = cp.where(cp.isnan(ma_shifted), cp.nan, slope_pct)

    # 偏离 MA
    if dev_enabled and dev_ma_period != ma_period:
        ma_dev = gpu_segmented_rolling_mean(close, boundaries, dev_ma_period)
    else:
        ma_dev = ma

    result = dict(gpu_raw)  # 浅拷贝，共享原始数组
    result["ma"] = ma
    result["slope_pct"] = slope_pct
    result["ma_dev"] = ma_dev
    return result


BACKTEST_HOOKS = {
    "detect": detect_multi_tf_ma_uptrend,
    "detect_vectorized": detect_multi_tf_ma_uptrend_vectorized,
    "detect_batch": detect_multi_tf_ma_uptrend_batch,
    "prepare": prepare_multi_tf_ma_features,
    "prepare_batch": prepare_multi_tf_ma_features_batch,
    "prepare_key": lambda p: {
        "ma_period": p["ma_period"],
        "slope_gap": p["slope_gap"],
        "close_deviation_enabled": p["close_deviation_enabled"],
        "close_deviation_ma_period": p.get("close_deviation_ma_period", p["ma_period"]),
    },
    "normalize_params": _normalize_for_backtest,
    "tf_sections": {
        "weekly": {"tf_key": "w", "table": "klines_w"},
        "daily": {"tf_key": "d", "table": "klines_d"},
        "min60": {"tf_key": "60", "table": "klines_60"},
        "min15": {"tf_key": "15", "table": "klines_15"},
    },
    "tf_logic": "and",
}
