"""
`multi_tf_ma_uptrend_v1` specialized engine。

职责：
1. 按周线/日线/60分钟/15分钟四个可选周期，批量加载 K 线数据并计算 MA 均线及其百分比斜率。
2. 对每只股票在所有已启用周期上分别检查：
   a. 【必选】最新 n 根线中 MA 均线斜率百分比落在 [slope_min, slope_max] 区间；
   b. 【可选】连续筛选——所有不重叠窗口斜率均满足区间；
   c. 【可选】收盘偏离——最后一根线收盘价距指定 MA 的偏离幅度在阈值内；
   d. 【可选】收敛震荡——最后 p 根线的最高/最低振幅小于阈值。
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

import pandas as pd

from strategies.engine_commons import (
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


def _prepare_features(frame: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """计算主 MA、偏离 MA 及百分比斜率列。

    输入：
    1. frame: 按 code+ts 排序的 OHLCV DataFrame。
    2. params: 已规范化的单周期参数。
    输出：
    1. 返回新增 `ma`、`ma_dev`（偏离 MA）、`slope_pct` 列后的 DataFrame。
    用途：
    1. 为后续逐股扫描提供统一特征列。
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

    # 截取最新 n 根线的斜率
    recent = code_frame.tail(n)
    slopes = recent["slope_pct"].dropna()

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


# ---------------------------------------------------------------------------
# 单股扫描
# ---------------------------------------------------------------------------


def _check_tf_conditions(
    code_frame: pd.DataFrame,
    params: dict[str, Any],
    tf_key: str,
) -> tuple[bool, dict[str, Any]]:
    """对单只股票的单个周期执行全部条件检查。

    输入：
    1. code_frame: 单只股票在该周期的特征 DataFrame。
    2. params: 已规范化的单周期参数。
    3. tf_key: 周期标识。
    输出：
    1. (是否全部通过, 检查结果明细 dict)。
    用途：
    1. 汇总必选 + 可选条件结果。
    边界条件：
    1. 数据为空直接返回 False。
    """
    detail: dict[str, Any] = {"tf": tf_key, "enabled": True, "passed": False}

    if code_frame.empty:
        detail["reason"] = "无数据"
        return False, detail

    # 必选：斜率检查
    slope_ok, slope_values, slope_reason = _check_slope(code_frame, params)
    detail["slope_passed"] = slope_ok
    detail["slope_values"] = [round(v, 4) for v in slope_values]
    if not slope_ok:
        detail["reason"] = slope_reason
        return False, detail

    # 可选：收盘偏离
    dev_ok, dev_pct, dev_reason = _check_close_deviation(code_frame, params)
    detail["close_deviation_passed"] = dev_ok
    detail["close_deviation_pct"] = round(dev_pct, 4)
    if not dev_ok:
        detail["reason"] = dev_reason
        return False, detail

    # 可选：收敛震荡
    cons_ok, cons_pct, cons_reason = _check_consolidation(code_frame, params)
    detail["consolidation_passed"] = cons_ok
    detail["consolidation_amp_pct"] = round(cons_pct, 4)
    if not cons_ok:
        detail["reason"] = cons_reason
        return False, detail

    detail["passed"] = True
    return True, detail





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
        passed, detail = _check_tf_conditions(frame, params, tf)
        per_tf_detail[tf] = detail
        if not passed:
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
    clock_tf = coarsest_tf(enabled_tfs)
    clock_frame = tf_data[clock_tf]
    latest_ts = clock_frame.iloc[-1]["ts"]

    # 展示窗口 = 最粗周期的最近 n 根线
    n_coarsest = int(all_params[clock_tf]["n"])
    window_start_idx = max(len(clock_frame) - n_coarsest, 0)
    window_start_ts = clock_frame.iloc[window_start_idx]["ts"]
    window_end_ts = latest_ts

    signal_label = _build_signal_label(enabled_tfs, per_tf_detail, all_params)

    payload: dict[str, Any] = {
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "chart_interval_start_ts": window_start_ts,
        "chart_interval_end_ts": window_end_ts,
        "anchor_day_ts": latest_ts,
        "enabled_timeframes": enabled_tfs,
        "per_tf": per_tf_detail,
        "signal_summary": signal_label,
    }

    signal = build_signal_dict(
        code=code,
        name=name,
        signal_dt=latest_ts,
        clock_tf=clock_tf,
        strategy_group_id=strategy_group_id,
        strategy_name=strategy_name,
        signal_label=signal_label,
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
            frame = _prepare_features(frame, params)
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

    # ── 逐股扫描 ──
    scan_start = time.perf_counter()
    result_map: dict[str, StockScanResult] = {}
    total_candidates = 0
    total_kept = 0

    for code in codes:
        code_tf_data = per_code_data.get(code, {})
        # 补齐缺失周期为空 DataFrame
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
