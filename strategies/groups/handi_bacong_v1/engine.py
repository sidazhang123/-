"""
`handi_bacong_v1` specialized engine。

职责：
1. 在日线/周线两个可选周期上，搜索长期水平震荡箱体并检测箱体末端向上突破放量信号。
2. 箱体上下沿通过分段分位数边界 + 线性回归拟合确定：
   - 将窗口均分为 segment_count 段。
   - 每段取 high 的 90% 分位数为上沿代表值、low 的 10% 分位数为下沿代表值。
   - 对代表值做线性回归，验证斜率接近零以确认水平性。
   - 水平线价格取各段代表值的均值。
3. 突破检测：
   - 箱体末端保留 1-3 根 K 线作为突破窗口（动态尝试，取最长有效箱体）。
   - 价格突破：近 1-3 根 K 线中最高收盘价 ≥ 上沿 + breakout_range_pct% × 箱体振幅。
   - 量能突破：箱体成交量去掉最高/最低各 10% 后求均值，
     近 1-3 根 K 线平均成交量 ≥ 修剪均量 × (1 + breakout_volume_pct%)。
4. 多周期之间为交集关系（AND）：所有启用周期均需独立检测到箱体突破才产生信号。
5. 同一股票多周期命中时合并为 1 条信号，clock_tf 取最粗命中周期。
6. 命中结果在 K 线图上画两条水平实线（上沿红色、下沿绿色）。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
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

STRATEGY_LABEL = "旱地拔葱 v1"

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

# 周期 key → klines 表名
_TF_TABLE: dict[str, str] = {
    "w": "klines_w",
    "d": "klines_d",
}

# 周期粗细排序（粗 → 细）
_TF_ORDER: list[str] = ["w", "d"]

# manifest 参数组 key → 周期 key
_PARAM_SECTION_TO_TF: dict[str, str] = {
    "weekly": "w",
    "daily": "d",
}

# 周期中文名
_TF_LABEL: dict[str, str] = {
    "w": "周线",
    "d": "日线",
}

# 各周期每月约含的 bar 数（用于斜率标准化）
_TF_BARS_PER_MONTH: dict[str, int] = {
    "w": 4,
    "d": 22,
}

# 每周自然日数（时长换算常量）
_DAYS_PER_WEEK = 7.0

# 突破窗口最大 bar 数
_MAX_BREAKOUT_BARS = 3


# ---------------------------------------------------------------------------
# 检测结果数据结构
# ---------------------------------------------------------------------------


@dataclass
class ChannelResult:
    """水平通道检测结果。

    输入：
    1. 由 _evaluate_channel() 在所有条件通过后构建。
    输出：
    1. 供突破检测和信号构建使用。
    """

    upper_level: float
    lower_level: float
    range_pct: float
    upper_slope_unit: float
    lower_slope_unit: float
    fit_error: float


@dataclass
class BreakoutResult:
    """突破检测结果。

    输入：
    1. 由 _check_breakout() 在价格和量能条件均通过后构建。
    输出：
    1. 供单股扫描使用，包含突破明细指标。
    """

    breakout_bars: int
    max_close: float
    breakout_threshold: float
    avg_breakout_volume: float
    trimmed_avg_box_volume: float
    volume_ratio: float


# ---------------------------------------------------------------------------
# 参数规范化
# ---------------------------------------------------------------------------


def _normalize_global_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化全局参数组。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回经类型转换和边界裁剪后的全局参数字典。
    边界条件：
    1. min_duration_weeks 范围 [2, 260]（约 5 年）。
    """
    raw = as_dict(group_params.get("global"))
    return {
        "min_duration_weeks": as_int(raw.get("min_duration_weeks"), 6, minimum=2, maximum=260),
    }


def _normalize_tf_params(group_params: dict[str, Any], section_key: str) -> dict[str, Any]:
    """规范化单个周期的箱体突破参数组。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    2. section_key: 参数组键名（"weekly" / "daily"）。
    输出：
    1. 返回经类型转换和边界裁剪后的单周期参数字典。
    边界条件：
    1. range_pct_max 强制 >= range_pct_min。
    """
    raw = as_dict(group_params.get(section_key))

    range_pct_min = as_float(raw.get("range_pct_min"), 20.0, minimum=5.0, maximum=100.0)

    return {
        "enabled": as_bool(raw.get("enabled"), False),
        "range_pct_min": range_pct_min,
        "range_pct_max": as_float(
            raw.get("range_pct_max"), 40.0,
            minimum=range_pct_min, maximum=200.0,
        ),
        "touch_tolerance_pct": as_float(raw.get("touch_tolerance_pct"), 10.0, minimum=1.0, maximum=30.0),
        "segment_count": as_int(raw.get("segment_count"), 4, minimum=2, maximum=20),
        "max_slope_pct": as_float(raw.get("max_slope_pct"), 3.0, minimum=0.5, maximum=10.0),
        "breakout_range_pct": as_float(raw.get("breakout_range_pct"), 100.0, minimum=0.0, maximum=500.0),
        "breakout_volume_pct": as_float(raw.get("breakout_volume_pct"), 100.0, minimum=0.0, maximum=1000.0),
    }


# ---------------------------------------------------------------------------
# 数据加载
# ---------------------------------------------------------------------------


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
    2. codes: 待查询股票代码列表。
    3. tf_key: 周期 key（"w" / "d"）。
    4. start_day/end_day: 日期边界。
    输出：
    1. 返回按 code + ts 排序的 DataFrame；codes 为空返回空表。
    边界条件：
    1. 使用临时表 + JOIN 模式批量读取。
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
# 上下沿检测核心
# ---------------------------------------------------------------------------


def _evaluate_channel(
    highs: np.ndarray,
    lows: np.ndarray,
    segment_count: int,
    max_slope_pct: float,
    range_pct_min: float,
    range_pct_max: float,
    bars_per_month: int,
) -> ChannelResult | None:
    """对单个候选窗口执行水平通道检测。

    输入：
    1. highs/lows: 窗口内 K 线的 high/low numpy 数组。
    2. segment_count: 分段数量。
    3. max_slope_pct: 上下沿每月最大允许斜率百分比。
    4. range_pct_min/range_pct_max: 区间幅度百分比范围。
    5. bars_per_month: 该周期每月约含的 bar 数（用于斜率标准化）。
    输出：
    1. 通过则返回 ChannelResult，否则返回 None。
    边界条件：
    1. 窗口 bar 数不足 segment_count 时返回 None。
    2. 上下沿斜率超出水平容差时返回 None。
    3. 区间幅度不在 [range_pct_min, range_pct_max] 范围时返回 None。
    """
    n = len(highs)
    if n < segment_count:
        return None

    # 分段分位数边界构造
    seg_size = n / segment_count
    upper_vals: list[float] = []
    lower_vals: list[float] = []
    center_xs: list[float] = []

    for i in range(segment_count):
        seg_start = int(round(i * seg_size))
        seg_end = int(round((i + 1) * seg_size))
        seg_end = min(seg_end, n)
        if seg_start >= seg_end:
            return None

        seg_highs = highs[seg_start:seg_end]
        seg_lows = lows[seg_start:seg_end]

        upper_vals.append(float(np.quantile(seg_highs, 0.9)))
        lower_vals.append(float(np.quantile(seg_lows, 0.1)))
        center_xs.append((seg_start + seg_end - 1) / 2.0)

    xs = np.array(center_xs, dtype=np.float64)
    us = np.array(upper_vals, dtype=np.float64)
    ls = np.array(lower_vals, dtype=np.float64)

    # 线性回归拟合
    b_u, a_u = np.polyfit(xs, us, 1)
    b_l, a_l = np.polyfit(xs, ls, 1)

    # 参考价格（用于斜率标准化）
    p_ref_u = float(np.mean(us))
    p_ref_l = float(np.mean(ls))
    if p_ref_u <= 0 or p_ref_l <= 0:
        return None

    # 单位斜率：每月涨跌幅百分比
    upper_slope_unit = (b_u * bars_per_month) / p_ref_u * 100.0
    lower_slope_unit = (b_l * bars_per_month) / p_ref_l * 100.0

    # 水平性检查：上下沿的月斜率都不能超过 max_slope_pct
    if abs(upper_slope_unit) > max_slope_pct:
        return None
    if abs(lower_slope_unit) > max_slope_pct:
        return None

    # 水平线价格：取各段代表值的均值
    upper_level = float(np.mean(us))
    lower_level = float(np.mean(ls))

    if lower_level <= 0:
        return None

    # 区间幅度检查
    range_pct = (upper_level - lower_level) / lower_level * 100.0
    if range_pct < range_pct_min or range_pct > range_pct_max:
        return None

    # 拟合误差（MARE）
    fitted_upper = a_u + b_u * xs
    fitted_lower = a_l + b_l * xs
    upper_errors = np.abs(us - fitted_upper) / np.where(fitted_upper > 0, fitted_upper, 1.0)
    lower_errors = np.abs(ls - fitted_lower) / np.where(fitted_lower > 0, fitted_lower, 1.0)
    fit_error = float(np.mean(np.concatenate([upper_errors, lower_errors])))

    return ChannelResult(
        upper_level=round(upper_level, 4),
        lower_level=round(lower_level, 4),
        range_pct=round(range_pct, 2),
        upper_slope_unit=round(upper_slope_unit, 4),
        lower_slope_unit=round(lower_slope_unit, 4),
        fit_error=round(fit_error, 6),
    )


# ---------------------------------------------------------------------------
# 突破检测
# ---------------------------------------------------------------------------


def _check_breakout(
    box_volumes: np.ndarray,
    breakout_frame: pd.DataFrame,
    channel: ChannelResult,
    breakout_range_pct: float,
    breakout_volume_pct: float,
) -> BreakoutResult | None:
    """检测箱体末端的价格突破和量能突破。

    输入：
    1. box_volumes: 箱体窗口内所有 K 线的成交量 numpy 数组。
    2. breakout_frame: 突破窗口的 DataFrame（1-3 根 K 线，含 close/volume 列）。
    3. channel: 已确认的水平通道检测结果。
    4. breakout_range_pct: 突破价格阈值（箱体振幅的百分比）。
    5. breakout_volume_pct: 突破量能阈值（修剪均量的百分比）。
    输出：
    1. 通过则返回 BreakoutResult，否则返回 None。
    边界条件：
    1. breakout_frame 为空时返回 None。
    2. 箱体 bar 数不足以做 10% 修剪时退化为全量均值。
    """
    if breakout_frame.empty:
        return None

    amplitude = channel.upper_level - channel.lower_level
    if amplitude <= 0:
        return None

    # ── 价格突破检查 ──
    breakout_closes = breakout_frame["close"].values.astype(np.float64)
    max_close = float(np.max(breakout_closes))
    breakout_threshold = channel.upper_level + (breakout_range_pct / 100.0) * amplitude

    if max_close < breakout_threshold:
        return None

    # ── 量能突破检查 ──
    # 箱体成交量修剪均值：排序后去掉 top/bottom 各 10%
    sorted_vols = np.sort(box_volumes.astype(np.float64))
    n_box = len(sorted_vols)
    trim_count = max(int(n_box * 0.1), 0)

    if n_box - 2 * trim_count > 0:
        trimmed_vols = sorted_vols[trim_count:n_box - trim_count]
    else:
        # bar 太少，不做修剪
        trimmed_vols = sorted_vols

    trimmed_avg = float(np.mean(trimmed_vols)) if len(trimmed_vols) > 0 else 0.0

    if trimmed_avg <= 0:
        return None

    breakout_volumes = breakout_frame["volume"].values.astype(np.float64)
    avg_breakout_vol = float(np.mean(breakout_volumes))

    volume_threshold = trimmed_avg * (1.0 + breakout_volume_pct / 100.0)
    if avg_breakout_vol < volume_threshold:
        return None

    volume_ratio = avg_breakout_vol / trimmed_avg

    return BreakoutResult(
        breakout_bars=len(breakout_frame),
        max_close=round(max_close, 4),
        breakout_threshold=round(breakout_threshold, 4),
        avg_breakout_volume=round(avg_breakout_vol, 2),
        trimmed_avg_box_volume=round(trimmed_avg, 2),
        volume_ratio=round(volume_ratio, 2),
    )


# ---------------------------------------------------------------------------
# 窗口搜索（含动态突破 bar 数探测）
# ---------------------------------------------------------------------------


def _find_channel_for_box(
    *,
    highs_all: np.ndarray,
    lows_all: np.ndarray,
    ts_arr: np.ndarray,
    e_box_idx: int,
    min_duration_weeks: int,
    segment_count: int,
    max_slope_pct: float,
    range_pct_min: float,
    range_pct_max: float,
    bars_per_month: int,
) -> tuple[ChannelResult, int, int] | None:
    """在固定 e_box_idx 下搜索最长有效水平通道。

    输入：
    1. highs_all/lows_all/ts_arr: 全量 K 线数据的 numpy 数组。
    2. e_box_idx: 箱体右边界索引（含）。
    3. min_duration_weeks: 最短持续周数。
    4. segment_count/max_slope_pct/range_pct_min/range_pct_max: 通道检测参数。
    5. bars_per_month: 该周期每月约含的 bar 数。
    输出：
    1. 返回 (ChannelResult, s_idx, e_box_idx) 或 None。
    边界条件：
    1. 从最远向最近枚举 s_idx，第一个通过即为最长有效窗口。
    2. 窗口时长不够 min_duration_weeks 时终止枚举。
    """
    latest_ts = pd.Timestamp(ts_arr[e_box_idx])

    for s_idx in range(0, e_box_idx):
        ts_start = pd.Timestamp(ts_arr[s_idx])
        window_days = (latest_ts - ts_start).days
        window_weeks = window_days / _DAYS_PER_WEEK

        # 窗口不够最短时长，后续更短，终止
        if window_weeks < min_duration_weeks:
            break

        window_len = e_box_idx - s_idx + 1
        if window_len < segment_count:
            continue

        # 通道检测
        w_highs = highs_all[s_idx:e_box_idx + 1]
        w_lows = lows_all[s_idx:e_box_idx + 1]

        channel = _evaluate_channel(
            highs=w_highs,
            lows=w_lows,
            segment_count=segment_count,
            max_slope_pct=max_slope_pct,
            range_pct_min=range_pct_min,
            range_pct_max=range_pct_max,
            bars_per_month=bars_per_month,
        )
        if channel is not None:
            return channel, s_idx, e_box_idx

    return None


def _scan_for_code(
    *,
    code_frame: pd.DataFrame,
    tf_params: dict[str, Any],
    min_duration_weeks: int,
    bars_per_month: int,
) -> tuple[ChannelResult, BreakoutResult, int, int, int] | None:
    """在单只股票的 K 线数据上搜索最长有效箱体并验证突破。

    输入：
    1. code_frame: 单只股票的 OHLCV DataFrame（已按 ts 排序）。
    2. tf_params: 已规范化的单周期参数。
    3. min_duration_weeks: 最短箱体持续周数。
    4. bars_per_month: 该周期每月约含的 bar 数。
    输出：
    1. 返回 (ChannelResult, BreakoutResult, s_idx, e_box_idx, n_breakout) 或 None。
    边界条件：
    1. 动态尝试末尾排除 1、2、3 根 bar，取最长有效箱体。
    2. 数据不足时返回 None。
    """
    n_total = len(code_frame)
    if n_total < 4:
        return None

    ts_arr = code_frame["ts"].values
    highs_all = code_frame["high"].values.astype(np.float64)
    lows_all = code_frame["low"].values.astype(np.float64)
    volumes_all = code_frame["volume"].values.astype(np.float64)

    segment_count = tf_params["segment_count"]
    max_slope_pct = tf_params["max_slope_pct"]
    range_pct_min = tf_params["range_pct_min"]
    range_pct_max = tf_params["range_pct_max"]
    breakout_range_pct = tf_params["breakout_range_pct"]
    breakout_volume_pct = tf_params["breakout_volume_pct"]

    best: tuple[ChannelResult, BreakoutResult, int, int, int] | None = None

    # 动态尝试突破 bar 数：1, 2, 3
    for n_breakout in range(1, _MAX_BREAKOUT_BARS + 1):
        e_box_idx = n_total - 1 - n_breakout
        if e_box_idx < segment_count:
            continue

        found = _find_channel_for_box(
            highs_all=highs_all,
            lows_all=lows_all,
            ts_arr=ts_arr,
            e_box_idx=e_box_idx,
            min_duration_weeks=min_duration_weeks,
            segment_count=segment_count,
            max_slope_pct=max_slope_pct,
            range_pct_min=range_pct_min,
            range_pct_max=range_pct_max,
            bars_per_month=bars_per_month,
        )
        if found is None:
            continue

        channel, s_idx, _ = found

        # 验证突破
        breakout_frame = code_frame.iloc[e_box_idx + 1:]
        box_volumes = volumes_all[s_idx:e_box_idx + 1]

        breakout = _check_breakout(
            box_volumes=box_volumes,
            breakout_frame=breakout_frame,
            channel=channel,
            breakout_range_pct=breakout_range_pct,
            breakout_volume_pct=breakout_volume_pct,
        )
        if breakout is None:
            continue

        # 本次有效：记录为候选（最长箱体）
        box_len = e_box_idx - s_idx + 1
        if best is None or (e_box_idx - s_idx + 1) > (best[3] - best[2] + 1):
            best = (channel, breakout, s_idx, e_box_idx, n_breakout)

    return best


# ---------------------------------------------------------------------------
# 检测入口
# ---------------------------------------------------------------------------


def detect_handi_bacong(
    code_frame: pd.DataFrame,
    tf_params: dict[str, Any],
    global_params: dict[str, Any],
    tf_key: str,
) -> DetectionResult:
    """在单只股票的指定周期上检测旱地拔葱形态。

    输入：
    1. code_frame: 单只股票在该周期的 OHLCV DataFrame（已按 ts 排序）。
    2. tf_params: 已规范化的单周期参数。
    3. global_params: 已规范化的全局参数。
    4. tf_key: 周期 key（"w" / "d"）。
    输出：
    1. DetectionResult，matched 表示是否命中，metrics 含通道与突破明细。
    边界条件：
    1. 数据不足时返回 matched=False。
    """
    bars_per_month = _TF_BARS_PER_MONTH.get(tf_key, 22)
    min_duration_weeks = global_params["min_duration_weeks"]

    scan = _scan_for_code(
        code_frame=code_frame,
        tf_params=tf_params,
        min_duration_weeks=min_duration_weeks,
        bars_per_month=bars_per_month,
    )
    if scan is None:
        return DetectionResult(matched=False)

    channel, breakout, s_idx, e_box_idx, n_breakout = scan
    # 整体模式范围：从箱体起始到最后一根突破 bar
    e_idx = len(code_frame) - 1

    window_start_ts = code_frame.iloc[s_idx]["ts"]
    window_end_ts = code_frame.iloc[e_idx]["ts"]
    duration_weeks = round((pd.Timestamp(window_end_ts) - pd.Timestamp(window_start_ts)).days / _DAYS_PER_WEEK, 1)

    return DetectionResult(
        matched=True,
        pattern_start_idx=s_idx,
        pattern_end_idx=e_idx,
        pattern_start_ts=window_start_ts,
        pattern_end_ts=window_end_ts,
        metrics={
            "tf_key": tf_key,
            "tf_label": _TF_LABEL.get(tf_key, tf_key),
            "duration_weeks": duration_weeks,
            "box_bars": e_box_idx - s_idx + 1,
            "breakout_bars": n_breakout,
            "channel": {
                "upper_level": channel.upper_level,
                "lower_level": channel.lower_level,
                "range_pct": channel.range_pct,
                "upper_slope_unit": channel.upper_slope_unit,
                "lower_slope_unit": channel.lower_slope_unit,
                "fit_error": channel.fit_error,
            },
            "breakout": {
                "breakout_bars": breakout.breakout_bars,
                "max_close": breakout.max_close,
                "breakout_threshold": breakout.breakout_threshold,
                "avg_breakout_volume": breakout.avg_breakout_volume,
                "trimmed_avg_box_volume": breakout.trimmed_avg_box_volume,
                "volume_ratio": breakout.volume_ratio,
            },
        },
    )


# ---------------------------------------------------------------------------
# 信号 payload 构建
# ---------------------------------------------------------------------------


def build_handi_bacong_payload(
    *,
    hit_tfs: list[str],
    per_tf_detail: dict[str, dict[str, Any]],
    tf_data: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    """根据各周期检测结果组装前端渲染 payload（含 overlay_lines）。

    输入：
    1. hit_tfs: 命中的周期 key 列表（至少有一个）。
    2. per_tf_detail: 各周期检测明细（含未命中周期）。
    3. tf_data: 各周期的 DataFrame。
    输出：
    1. 符合图表渲染合同的 payload dict，包含 overlay_lines 和通道/突破明细。
    边界条件：
    1. 仅在所有启用周期均命中后调用。
    """
    clock_tf = coarsest_tf(hit_tfs)
    clock_detail = per_tf_detail[clock_tf]
    clock_frame = tf_data[clock_tf]

    s_idx = clock_detail["pattern_start_idx"]
    e_idx = clock_detail["pattern_end_idx"]

    window_start_ts = clock_frame.iloc[s_idx]["ts"]
    window_end_ts = clock_frame.iloc[e_idx]["ts"]

    # 图表区间：整个窗口 + 向前延伸 10% 以提供上下文
    window_bars = e_idx - s_idx + 1
    extra_bars = max(int(window_bars * 0.1), 3)
    chart_start_idx = max(s_idx - extra_bars, 0)
    chart_interval_start_ts = clock_frame.iloc[chart_start_idx]["ts"]
    chart_interval_end_ts = clock_frame.iloc[e_idx]["ts"]

    # 构建 overlay_lines：使用最粗周期的通道
    channel = clock_detail["channel"]
    # 箱体结束 ts（不含突破 bar）
    box_end_idx = e_idx - clock_detail["breakout_bars"]
    box_end_ts = clock_frame.iloc[box_end_idx]["ts"]

    overlay_lines = [
        {
            "label": "上沿",
            "start_ts": window_start_ts,
            "start_price": channel["upper_level"],
            "end_ts": box_end_ts,
            "end_price": channel["upper_level"],
            "color": "#ef4444",
            "dash": False,
        },
        {
            "label": "下沿",
            "start_ts": window_start_ts,
            "start_price": channel["lower_level"],
            "end_ts": box_end_ts,
            "end_price": channel["lower_level"],
            "color": "#22c55e",
            "dash": False,
        },
    ]

    # 信号摘要
    duration_weeks = clock_detail.get("duration_weeks", 0)
    signal_summary = f"{_TF_LABEL.get(clock_tf, clock_tf)}箱体突破 {duration_weeks}周"

    return {
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "chart_interval_start_ts": chart_interval_start_ts,
        "chart_interval_end_ts": chart_interval_end_ts,
        "anchor_day_ts": window_end_ts,
        "overlay_lines": overlay_lines,
        "signal_summary": signal_summary,
        "clock_tf": clock_tf,
        "hit_tfs": hit_tfs,
        "per_tf": per_tf_detail,
    }


# ---------------------------------------------------------------------------
# 单股扫描（多周期 AND）
# ---------------------------------------------------------------------------


def _scan_one_code(
    *,
    code: str,
    name: str,
    tf_data: dict[str, pd.DataFrame],
    all_params: dict[str, dict[str, Any]],
    global_params: dict[str, Any],
    enabled_tfs: list[str],
    strategy_group_id: str,
    strategy_name: str,
) -> tuple[StockScanResult, dict[str, int]]:
    """扫描单只股票：在所有已启用周期上执行箱体突破检测（AND 逻辑）。

    输入：
    1. code/name: 股票标识。
    2. tf_data: {tf_key: DataFrame} 该股票各周期的 K 线数据。
    3. all_params: {tf_key: params} 各周期参数。
    4. global_params: 全局参数。
    5. enabled_tfs: 已启用的周期 key 列表。
    6. strategy_group_id/strategy_name: 构建信号用。
    输出：
    1. (StockScanResult, 统计计数 dict)。
    边界条件：
    1. 任一启用周期不通过则整体无信号（AND 短路）。
    """
    stats = {"candidate": 0, "kept": 0}
    total_bars = sum(len(df) for df in tf_data.values())

    per_tf_detail: dict[str, dict[str, Any]] = {}
    hit_tfs: list[str] = []
    all_passed = True

    for tf in enabled_tfs:
        frame = tf_data.get(tf, pd.DataFrame())
        params = all_params[tf]
        result = detect_handi_bacong(frame, params, global_params, tf)

        detail: dict[str, Any] = {"passed": result.matched}
        if result.matched:
            detail.update(result.metrics)
            detail["pattern_start_idx"] = result.pattern_start_idx
            detail["pattern_end_idx"] = result.pattern_end_idx
            hit_tfs.append(tf)
        per_tf_detail[tf] = detail

        if not result.matched:
            all_passed = False
            break  # AND 短路

    stats["candidate"] = 1

    if not all_passed:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=total_bars,
            signal_count=0,
            signals=[],
        ), stats

    # 构建 payload
    payload = build_handi_bacong_payload(
        hit_tfs=hit_tfs,
        per_tf_detail=per_tf_detail,
        tf_data=tf_data,
    )

    clock_tf = payload["clock_tf"]
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

    stats["kept"] = 1
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


def run_handi_bacong_v1_specialized(
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
    """`handi_bacong_v1` specialized 主入口。

    输入：
    1. 所有参数由 TaskManager 以关键字方式传入。
    输出：
    1. 返回 (result_map, metrics)，供 TaskManager 写库和前端展示使用。
    边界条件：
    1. codes 为空时直接返回空结果。
    2. 无启用周期时直接返回空结果。
    3. 当前策略不使用缓存。
    """
    _ = (cache_scope, cache_dir)

    # ── 参数规范化 ──
    global_params = _normalize_global_params(group_params)

    all_tf_params: dict[str, dict[str, Any]] = {}
    for section_key, tf_key in _PARAM_SECTION_TO_TF.items():
        all_tf_params[tf_key] = _normalize_tf_params(group_params, section_key)

    execution_params = normalize_execution_params(group_params)
    universe_filter_params = read_universe_filter_params(group_params)

    enabled_tfs = [tf for tf in _TF_ORDER if all_tf_params[tf]["enabled"]]

    base_metrics: dict[str, Any] = {
        "total_codes": len(codes),
        "enabled_timeframes": enabled_tfs,
        "min_duration_weeks": global_params["min_duration_weeks"],
        "candidate_count": 0,
        "kept_count": 0,
        "codes_with_signal": 0,
        "stock_errors": 0,
        "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
        "concept_filter_enabled": universe_filter_params["enabled"],
        "concept_terms": universe_filter_params["concept_terms"],
        "reason_terms": universe_filter_params["reason_terms"],
    }

    if not codes or not enabled_tfs:
        return {}, base_metrics

    # ── 数据加载 ──
    end_dt = end_ts or datetime.now()
    end_day = end_dt.date()

    # 回溯天数：min_duration_weeks 转天数 + 安全余量（含突破 bar）
    days_back = int(global_params["min_duration_weeks"] * _DAYS_PER_WEEK) + 90
    raw_start = start_ts.date() if start_ts is not None else (end_dt - timedelta(days=days_back)).date()
    desired_start = (end_dt - timedelta(days=days_back)).date()
    load_start = min(raw_start, desired_start)

    load_t0 = time.perf_counter()
    tf_raw_frames: dict[str, pd.DataFrame] = {}
    for tf in enabled_tfs:
        tf_raw_frames[tf] = _load_bars(
            source_db_path=Path(source_db_path),
            codes=codes,
            tf_key=tf,
            start_day=load_start,
            end_day=end_day,
        )
    load_sec = round(time.perf_counter() - load_t0, 4)
    base_metrics["load_phase_sec"] = load_sec

    for tf, raw in tf_raw_frames.items():
        base_metrics[f"total_{tf}_rows"] = len(raw)

    # ── 按股票分组 ──
    per_code_tf: dict[str, dict[str, pd.DataFrame]] = {}
    for tf, raw in tf_raw_frames.items():
        if raw.empty:
            continue
        for code_val, code_frame in raw.groupby("code", sort=False):
            if code_val not in per_code_tf:
                per_code_tf[code_val] = {}
            per_code_tf[code_val][tf] = code_frame.sort_values("ts").reset_index(drop=True)

    # ── 逐股扫描 ──
    scan_start = time.perf_counter()
    result_map: dict[str, StockScanResult] = {}
    total_candidates = 0
    total_kept = 0

    for code in codes:
        code_tf_data = per_code_tf.get(code, {})
        if not code_tf_data:
            continue

        # 确保每个启用周期都有数据（即使是空 DataFrame）
        tf_data_for_code: dict[str, pd.DataFrame] = {}
        for tf in enabled_tfs:
            tf_data_for_code[tf] = code_tf_data.get(tf, pd.DataFrame())

        try:
            code_result, stats = _scan_one_code(
                code=code,
                name=code_to_name.get(code, ""),
                tf_data=tf_data_for_code,
                all_params=all_tf_params,
                global_params=global_params,
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
