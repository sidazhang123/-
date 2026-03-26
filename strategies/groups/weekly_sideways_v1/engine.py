"""
`weekly_sideways_v1` specialized engine。

职责：
1. 在周线级别上搜索长期水平震荡区间（箱体结构）。
2. 上下沿通过分段分位数边界 + 线性回归拟合确定：
   - 将窗口均分为 segment_count 段。
   - 每段取 high 的 90% 分位数为上沿代表值、low 的 10% 分位数为下沿代表值。
   - 对代表值做线性回归，验证斜率接近零以确认水平性。
   - 水平线价格取各段代表值的均值。
3. 在确定上下沿后，检测价格在区间内的往返轮动次数和节奏。
   - 触摸定义：high 进入上沿容差带 = 触摸上沿，low 进入下沿容差带 = 触碰下沿。
   - 一次完整轮动（round trip）= 两次半轮动（swing），即上→下→上或下→上→下。
   - 每次半轮动时长必须在 [rotation_months_min, rotation_months_max] 内。
4. 可选启用参考月份最低收盘价比较。
5. 窗口搜索策略：保留最长的有效窗口。
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
    connect_source_readonly,
    normalize_execution_params,
    read_universe_filter_params,
)

STRATEGY_LABEL = "周线长期震荡 v1"

# 周线月 bar 数（用于斜率标准化：每月约 4 根周线）
_W_BARS_PER_MONTH = 4

# 每月自然日数（时长换算常量）
_DAYS_PER_MONTH = 30.0


# ---------------------------------------------------------------------------
# 检测结果数据结构
# ---------------------------------------------------------------------------


@dataclass
class ChannelResult:
    """水平通道检测结果。

    输入：
    1. 由 _evaluate_channel() 在所有条件通过后构建。
    输出：
    1. 供轮动检测和信号构建使用。
    """

    upper_level: float
    lower_level: float
    range_pct: float
    upper_slope_unit: float
    lower_slope_unit: float
    fit_error: float


@dataclass
class RotationResult:
    """轮动检测结果。

    输入：
    1. 由 _detect_rotations() 在每次半轮动时长合法且次数达标后构建。
    输出：
    1. 供单股扫描使用，包含完整轮动次数和各半轮动明细。
    """

    rotation_count: int
    swing_count: int
    swing_details: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# 参数规范化
# ---------------------------------------------------------------------------


def _normalize_weekly_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化周线震荡检测参数组。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回经类型转换和边界裁剪后的周线参数字典。
    边界条件：
    1. rotation_months_max 强制 >= rotation_months_min。
    2. range_pct_max 强制 >= range_pct_min。
    """
    raw = as_dict(group_params.get("weekly"))

    rotation_months_min = as_float(raw.get("rotation_months_min"), 1.0, minimum=0.5, maximum=12.0)
    range_pct_min = as_float(raw.get("range_pct_min"), 20.0, minimum=5.0, maximum=100.0)

    return {
        "min_duration_months": as_int(raw.get("min_duration_months"), 12, minimum=3, maximum=60),
        "rotation_months_min": rotation_months_min,
        "rotation_months_max": as_float(
            raw.get("rotation_months_max"), 4.0,
            minimum=rotation_months_min, maximum=24.0,
        ),
        "range_pct_min": range_pct_min,
        "range_pct_max": as_float(
            raw.get("range_pct_max"), 40.0,
            minimum=range_pct_min, maximum=200.0,
        ),
        "min_rotations": as_int(raw.get("min_rotations"), 2, minimum=1, maximum=20),
        "touch_tolerance_pct": as_float(raw.get("touch_tolerance_pct"), 10.0, minimum=1.0, maximum=30.0),
        "segment_count": as_int(raw.get("segment_count"), 4, minimum=2, maximum=20),
        "max_slope_pct": as_float(raw.get("max_slope_pct"), 3.0, minimum=0.5, maximum=10.0),
    }


def _normalize_reference_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化参考月比较参数组。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回参考月参数字典，含 enabled 开关。
    边界条件：
    1. ref_gain_max_pct 强制 >= ref_gain_min_pct。
    """
    raw = as_dict(group_params.get("reference"))

    ref_gain_min_pct = as_float(raw.get("ref_gain_min_pct"), -5.0, minimum=-50.0, maximum=100.0)

    return {
        "enabled": as_bool(raw.get("enabled"), False),
        "ref_year": as_int(raw.get("ref_year"), 2025, minimum=2000, maximum=2030),
        "ref_month": as_int(raw.get("ref_month"), 4, minimum=1, maximum=12),
        "ref_gain_min_pct": ref_gain_min_pct,
        "ref_gain_max_pct": as_float(
            raw.get("ref_gain_max_pct"), 30.0,
            minimum=ref_gain_min_pct, maximum=200.0,
        ),
    }


# ---------------------------------------------------------------------------
# 数据加载
# ---------------------------------------------------------------------------


def _load_weekly_bars(
    *,
    source_db_path: Path,
    codes: list[str],
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    """批量加载周线 OHLCV 数据。

    输入：
    1. source_db_path: DuckDB 源库路径。
    2. codes: 待查询股票代码列表。
    3. start_day/end_day: 日期边界。
    输出：
    1. 返回按 code + ts 排序的 DataFrame；codes 为空返回空表。
    边界条件：
    1. 使用临时表 + JOIN 模式批量读取。
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
                t.open,
                t.high,
                t.low,
                t.close,
                t.volume
            FROM klines_w t
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


def _load_reference_low_close(
    *,
    source_db_path: Path,
    codes: list[str],
    ref_year: int,
    ref_month: int,
) -> dict[str, float]:
    """批量加载参考月份各股票的周线最低收盘价。

    输入：
    1. source_db_path: DuckDB 源库路径。
    2. codes: 待查询股票代码列表。
    3. ref_year/ref_month: 参考年月。
    输出：
    1. 返回 {code: min_close} 字典。数据不存在的股票不在字典中。
    边界条件：
    1. 指定月份无数据的股票不会出现在返回结果中，调用方应视为跳过。
    """
    if not codes:
        return {}

    # 计算该月的日期范围
    month_start = date(ref_year, ref_month, 1)
    if ref_month == 12:
        month_end = date(ref_year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(ref_year, ref_month + 1, 1) - timedelta(days=1)

    with connect_source_readonly(source_db_path) as con:
        con.execute("CREATE TEMP TABLE _tmp_codes (code VARCHAR)")
        con.execute("INSERT INTO _tmp_codes(code) SELECT unnest($1)", [codes])
        rows = con.execute(
            """
            SELECT t.code, MIN(t.close) AS min_close
            FROM klines_w t
            JOIN _tmp_codes c ON c.code = t.code
            WHERE t.datetime >= ? AND t.datetime <= ?
            GROUP BY t.code
            """,
            [
                datetime.combine(month_start, datetime.min.time()),
                datetime.combine(month_end, datetime.max.time()),
            ],
        ).fetchall()

    return {row[0]: float(row[1]) for row in rows if row[1] is not None}


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
) -> ChannelResult | None:
    """对单个候选窗口执行水平通道检测。

    输入：
    1. highs/lows: 窗口内 K 线的 high/low numpy 数组。
    2. segment_count: 分段数量。
    3. max_slope_pct: 上下沿每月最大允许斜率百分比。
    4. range_pct_min/range_pct_max: 区间幅度百分比范围。
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

    # 单位斜率：每月涨跌幅百分比（每月约 4 根周线）
    upper_slope_unit = (b_u * _W_BARS_PER_MONTH) / p_ref_u * 100.0
    lower_slope_unit = (b_l * _W_BARS_PER_MONTH) / p_ref_l * 100.0

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
# 轮动检测
# ---------------------------------------------------------------------------


# 触摸类型常量
_TOUCH_UPPER = 1
_TOUCH_LOWER = 2
_TOUCH_NONE = 0


def _detect_rotations(
    highs: np.ndarray,
    lows: np.ndarray,
    ts_arr: np.ndarray,
    upper_level: float,
    lower_level: float,
    touch_tolerance_pct: float,
    rotation_months_min: float,
    rotation_months_max: float,
    min_rotations: int,
) -> RotationResult | None:
    """在已确定上下沿的窗口内检测往返轮动。

    输入：
    1. highs/lows: 窗口内 K 线数据的 numpy 数组。
    2. ts_arr: 时间戳数组（numpy datetime64）。
    3. upper_level/lower_level: 上下沿价格。
    4. touch_tolerance_pct: 触摸容差（区间幅度的百分比）。
    5. rotation_months_min/rotation_months_max: 每次半轮动时长范围（月）。
    6. min_rotations: 最少完整轮动次数（往返）。
    输出：
    1. 通过则返回 RotationResult，否则返回 None。
    边界条件：
    1. 触摸事件不足以构成 min_rotations 次完整轮动时返回 None。
    2. 任何一次半轮动时长超出 [p, q] 范围时，该半轮动被标记为无效，
       但不立即中断，仅在最终合格轮动数不足时返回 None。
    """
    n = len(highs)
    if n < 2:
        return None

    band_width = upper_level - lower_level
    if band_width <= 0:
        return None

    touch_band = band_width * touch_tolerance_pct / 100.0

    # 标记每根 bar 的触摸类型
    touch_types = np.zeros(n, dtype=np.int8)
    for i in range(n):
        if highs[i] >= upper_level - touch_band:
            touch_types[i] = _TOUCH_UPPER
        if lows[i] <= lower_level + touch_band:
            # 如果同时触摸上下沿（极端情况），优先标记为上沿触摸
            if touch_types[i] == _TOUCH_NONE:
                touch_types[i] = _TOUCH_LOWER

    # 合并连续同类型触摸为事件，提取交替事件序列
    events: list[tuple[int, int, int]] = []  # (type, first_idx, last_idx)
    i = 0
    while i < n:
        if touch_types[i] == _TOUCH_NONE:
            i += 1
            continue
        t_type = touch_types[i]
        first_idx = i
        last_idx = i
        i += 1
        while i < n and touch_types[i] == t_type:
            last_idx = i
            i += 1
        events.append((t_type, first_idx, last_idx))

    if len(events) < 2:
        return None

    # 建立交替序列（去除连续同类事件，保留最后一个）
    alternating: list[tuple[int, int, int]] = [events[0]]
    for ev in events[1:]:
        if ev[0] != alternating[-1][0]:
            alternating.append(ev)
        else:
            # 连续同类型，替换为最新的
            alternating[-1] = ev

    if len(alternating) < 2:
        return None

    # 计算半轮动（swing）
    swing_details: list[dict[str, Any]] = []
    valid_swing_count = 0

    for j in range(len(alternating) - 1):
        ev_from = alternating[j]
        ev_to = alternating[j + 1]

        # swing 时长：从 from 事件中点到 to 事件中点
        from_mid_idx = (ev_from[1] + ev_from[2]) // 2
        to_mid_idx = (ev_to[1] + ev_to[2]) // 2

        from_ts = pd.Timestamp(ts_arr[from_mid_idx])
        to_ts = pd.Timestamp(ts_arr[to_mid_idx])
        swing_days = (to_ts - from_ts).days
        swing_months = swing_days / _DAYS_PER_MONTH

        in_range = rotation_months_min <= swing_months <= rotation_months_max

        detail = {
            "from_type": "upper" if ev_from[0] == _TOUCH_UPPER else "lower",
            "to_type": "upper" if ev_to[0] == _TOUCH_UPPER else "lower",
            "swing_months": round(swing_months, 2),
            "valid": in_range,
        }
        swing_details.append(detail)

        if in_range:
            valid_swing_count += 1

    # 完整轮动数 = 有效半轮动数 // 2
    rotation_count = valid_swing_count // 2

    if rotation_count < min_rotations:
        return None

    return RotationResult(
        rotation_count=rotation_count,
        swing_count=valid_swing_count,
        swing_details=swing_details,
    )


# ---------------------------------------------------------------------------
# 窗口搜索
# ---------------------------------------------------------------------------


def _scan_for_code(
    *,
    code_frame: pd.DataFrame,
    params: dict[str, Any],
) -> tuple[ChannelResult, RotationResult, int, int] | None:
    """在单只股票的周线数据上搜索最长有效水平震荡窗口。

    输入：
    1. code_frame: 单只股票的周线 OHLCV DataFrame（已按 ts 排序）。
    2. params: 已规范化的周线参数。
    输出：
    1. 返回 (ChannelResult, RotationResult, start_idx, end_idx) 或 None。
    边界条件：
    1. 数据不足 min_duration_months 时返回 None。
    2. 搜索策略：t_e 固定为最新 bar，t_s 从最远向最近枚举，
       第一个通过所有检查的窗口即为最长有效窗口。
    """
    if code_frame.empty:
        return None

    ts_arr = code_frame["ts"].values
    highs_all = code_frame["high"].values.astype(np.float64)
    lows_all = code_frame["low"].values.astype(np.float64)
    closes_all = code_frame["close"].values.astype(np.float64)
    n_total = len(code_frame)

    min_duration_months = params["min_duration_months"]
    segment_count = params["segment_count"]
    max_slope_pct = params["max_slope_pct"]
    range_pct_min = params["range_pct_min"]
    range_pct_max = params["range_pct_max"]
    touch_tolerance_pct = params["touch_tolerance_pct"]
    rotation_months_min = params["rotation_months_min"]
    rotation_months_max = params["rotation_months_max"]
    min_rotations = params["min_rotations"]

    # t_e 固定为最新 bar
    e_idx = n_total - 1
    latest_ts = pd.Timestamp(ts_arr[e_idx])

    # t_s 从最远向最近枚举（step = 1 bar）
    for s_idx in range(0, e_idx):
        ts_start = pd.Timestamp(ts_arr[s_idx])
        window_days = (latest_ts - ts_start).days
        window_months = window_days / _DAYS_PER_MONTH

        # 窗口不够最短时长要求，后续更短，可以终止
        if window_months < min_duration_months:
            break

        window_len = e_idx - s_idx + 1
        if window_len < segment_count:
            continue

        # 通道检测
        w_highs = highs_all[s_idx:e_idx + 1]
        w_lows = lows_all[s_idx:e_idx + 1]

        channel = _evaluate_channel(
            highs=w_highs,
            lows=w_lows,
            segment_count=segment_count,
            max_slope_pct=max_slope_pct,
            range_pct_min=range_pct_min,
            range_pct_max=range_pct_max,
        )
        if channel is None:
            continue

        # 当前价位检查
        c_now = float(closes_all[e_idx])
        band_width = channel.upper_level - channel.lower_level
        touch_band = band_width * touch_tolerance_pct / 100.0
        if c_now > channel.upper_level + touch_band or c_now < channel.lower_level - touch_band:
            continue

        # 轮动检测
        w_ts = ts_arr[s_idx:e_idx + 1]

        rotation = _detect_rotations(
            highs=w_highs,
            lows=w_lows,
            ts_arr=w_ts,
            upper_level=channel.upper_level,
            lower_level=channel.lower_level,
            touch_tolerance_pct=touch_tolerance_pct,
            rotation_months_min=rotation_months_min,
            rotation_months_max=rotation_months_max,
            min_rotations=min_rotations,
        )
        if rotation is None:
            continue

        # 第一个通过的即为最长有效窗口
        return channel, rotation, s_idx, e_idx

    return None


def detect_weekly_sideways(
    code_frame: pd.DataFrame,
    params: dict[str, Any],
) -> DetectionResult:
    """在单只股票的周线数据上检测长期水平震荡形态。

    输入：
    1. code_frame: 单只股票的周线 OHLCV DataFrame（已按 ts 排序）。
    2. params: 已规范化的周线参数。
    输出：
    1. DetectionResult，matched 表示是否命中，metrics 含通道与轮动明细。
    用途：
    1. 搜索最长有效窗口，返回通道参数和轮动统计。
    2. 可被筛选编排器和未来回测引擎独立调用。
    边界条件：
    1. 数据不足时返回 matched=False。
    2. 参考月过滤不在此层处理（由编排器负责）。
    """
    scan = _scan_for_code(code_frame=code_frame, params=params)
    if scan is None:
        return DetectionResult(matched=False)

    channel, rotation, s_idx, e_idx = scan
    return DetectionResult(
        matched=True,
        pattern_start_idx=s_idx,
        pattern_end_idx=e_idx,
        pattern_start_ts=code_frame.iloc[s_idx]["ts"],
        pattern_end_ts=code_frame.iloc[e_idx]["ts"],
        metrics={
            "channel": {
                "upper_level": channel.upper_level,
                "lower_level": channel.lower_level,
                "range_pct": channel.range_pct,
                "upper_slope_unit": channel.upper_slope_unit,
                "lower_slope_unit": channel.lower_slope_unit,
                "fit_error": channel.fit_error,
            },
            "rotation": {
                "rotation_count": rotation.rotation_count,
                "swing_count": rotation.swing_count,
                "swing_details": rotation.swing_details,
            },
        },
    )


# ---------------------------------------------------------------------------
# 参考月比较
# ---------------------------------------------------------------------------


def _check_reference_low(
    lower_level: float,
    ref_low_close: float | None,
    ref_params: dict[str, Any],
) -> bool:
    """检查下沿相对参考月最低收盘价的涨幅是否在指定范围内。

    输入：
    1. lower_level: 检测到的下沿价格。
    2. ref_low_close: 参考月的最低收盘价，None 表示数据不存在。
    3. ref_params: 已规范化的参考月参数。
    输出：
    1. True 表示通过（含数据不存在跳过的情况），False 表示不通过。
    边界条件：
    1. 数据不存在（ref_low_close is None）时返回 True（跳过过滤）。
    2. ref_low_close <= 0 时返回 True（避免除零，跳过过滤）。
    """
    if ref_low_close is None or ref_low_close <= 0:
        return True

    gain_pct = (lower_level - ref_low_close) / ref_low_close * 100.0
    return ref_params["ref_gain_min_pct"] <= gain_pct <= ref_params["ref_gain_max_pct"]


# ---------------------------------------------------------------------------
# 单股扫描
# ---------------------------------------------------------------------------


def build_weekly_sideways_payload(
    *,
    code_frame: pd.DataFrame,
    detection_result: DetectionResult,
) -> dict[str, Any]:
    """根据检测结果组装前端渲染 payload（含 overlay_lines）。

    输入：
    1. code_frame: 该股票的周线 DataFrame。
    2. detection_result: detect_weekly_sideways 返回的 DetectionResult（matched=True）。
    输出：
    1. 符合图表渲染合同的 payload dict，包含 overlay_lines 和通道/轮动明细。
    边界条件：
    1. 仅在命中时调用。
    """
    m = detection_result.metrics
    s_idx = detection_result.pattern_start_idx
    e_idx = detection_result.pattern_end_idx
    channel = m["channel"]
    rotation = m["rotation"]

    window_start_ts = code_frame.iloc[s_idx]["ts"]
    window_end_ts = code_frame.iloc[e_idx]["ts"]

    # 图表区间：整个窗口 + 向前延伸 10% 以提供上下文
    window_bars = e_idx - s_idx + 1
    extra_bars = max(int(window_bars * 0.1), 3)
    chart_start_idx = max(s_idx - extra_bars, 0)
    chart_interval_start_ts = code_frame.iloc[chart_start_idx]["ts"]
    chart_interval_end_ts = code_frame.iloc[e_idx]["ts"]

    # 构建 overlay_lines（两条水平实线）
    overlay_lines = [
        {
            "label": "上沿",
            "start_ts": window_start_ts,
            "start_price": channel["upper_level"],
            "end_ts": window_end_ts,
            "end_price": channel["upper_level"],
            "color": "#ef4444",
            "dash": False,
        },
        {
            "label": "下沿",
            "start_ts": window_start_ts,
            "start_price": channel["lower_level"],
            "end_ts": window_end_ts,
            "end_price": channel["lower_level"],
            "color": "#22c55e",
            "dash": False,
        },
    ]

    # 构建诊断信息
    window_start_date = pd.Timestamp(window_start_ts)
    window_end_date = pd.Timestamp(window_end_ts)
    duration_months = round((window_end_date - window_start_date).days / _DAYS_PER_MONTH, 1)

    signal_label = f"周线水平震荡 {duration_months}月"

    return {
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "chart_interval_start_ts": chart_interval_start_ts,
        "chart_interval_end_ts": chart_interval_end_ts,
        "anchor_day_ts": window_end_ts,
        "overlay_lines": overlay_lines,
        "signal_summary": signal_label,
        "channel": channel,
        "rotation": rotation,
        "duration_months": duration_months,
        "window_bars": window_bars,
    }


def _scan_one_code(
    *,
    code: str,
    name: str,
    code_frame: pd.DataFrame,
    weekly_params: dict[str, Any],
    ref_params: dict[str, Any],
    ref_low_map: dict[str, float],
    strategy_group_id: str,
    strategy_name: str,
) -> tuple[StockScanResult, dict[str, int]]:
    """扫描单只股票：水平通道检测 + 轮动检测 + 参考月过滤。

    输入：
    1. code/name: 股票标识。
    2. code_frame: 该股票的周线 DataFrame。
    3. weekly_params: 已规范化的周线参数。
    4. ref_params: 已规范化的参考月参数。
    5. ref_low_map: {code: ref_low_close} 批量预加载的参考月数据。
    6. strategy_group_id/strategy_name: 构建信号用。
    输出：
    1. (StockScanResult, 统计计数 dict)。
    边界条件：
    1. 无命中返回空信号结果。
    """
    stats = {"candidate": 0, "kept": 0}
    total_bars = len(code_frame)

    result = detect_weekly_sideways(code_frame, weekly_params)

    stats["candidate"] = 1

    if not result.matched:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=total_bars,
            signal_count=0,
            signals=[],
        ), stats

    # 参考月过滤
    if ref_params["enabled"]:
        channel = result.metrics["channel"]
        ref_low = ref_low_map.get(code)
        if not _check_reference_low(channel["lower_level"], ref_low, ref_params):
            return StockScanResult(
                code=code,
                name=name,
                processed_bars=total_bars,
                signal_count=0,
                signals=[],
            ), stats

    # 构建 payload
    payload = build_weekly_sideways_payload(
        code_frame=code_frame,
        detection_result=result,
    )

    signal = build_signal_dict(
        code=code,
        name=name,
        signal_dt=payload["window_end_ts"],
        clock_tf="w",
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


def run_weekly_sideways_v1_specialized(
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
    """`weekly_sideways_v1` specialized 主入口。

    输入：
    1. 所有参数由 TaskManager 以关键字方式传入。
    输出：
    1. 返回 (result_map, metrics)，供 TaskManager 写库和前端展示使用。
    边界条件：
    1. codes 为空时直接返回空结果。
    2. 当前策略不使用缓存。
    """
    _ = (cache_scope, cache_dir)

    # ── 参数规范化 ──
    weekly_params = _normalize_weekly_params(group_params)
    ref_params = _normalize_reference_params(group_params)
    execution_params = normalize_execution_params(group_params)
    universe_filter_params = read_universe_filter_params(group_params)

    base_metrics: dict[str, Any] = {
        "total_codes": len(codes),
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

    # ── 数据加载 ──
    end_dt = end_ts or datetime.now()
    end_day = end_dt.date()

    # 回溯天数：需覆盖 min_duration_months 的完整范围，加安全余量
    days_back = int(weekly_params["min_duration_months"] * _DAYS_PER_MONTH) + 90
    raw_start = start_ts.date() if start_ts is not None else (end_dt - timedelta(days=days_back)).date()
    desired_start = (end_dt - timedelta(days=days_back)).date()
    load_start = min(raw_start, desired_start)

    load_t0 = time.perf_counter()
    weekly_raw = _load_weekly_bars(
        source_db_path=Path(source_db_path),
        codes=codes,
        start_day=load_start,
        end_day=end_day,
    )
    load_sec = round(time.perf_counter() - load_t0, 4)

    base_metrics["load_phase_sec"] = load_sec
    base_metrics["total_w_rows"] = len(weekly_raw)

    if weekly_raw.empty:
        return {}, base_metrics

    # 参考月数据预加载
    ref_low_map: dict[str, float] = {}
    if ref_params["enabled"]:
        ref_t0 = time.perf_counter()
        ref_low_map = _load_reference_low_close(
            source_db_path=Path(source_db_path),
            codes=codes,
            ref_year=ref_params["ref_year"],
            ref_month=ref_params["ref_month"],
        )
        base_metrics["ref_load_sec"] = round(time.perf_counter() - ref_t0, 4)
        base_metrics["ref_codes_with_data"] = len(ref_low_map)

    # ── 按股票分组 ──
    per_code: dict[str, pd.DataFrame] = {}
    for code_val, code_frame in weekly_raw.groupby("code", sort=False):
        per_code[code_val] = code_frame.sort_values("ts").reset_index(drop=True)

    # ── 逐股扫描 ──
    scan_start = time.perf_counter()
    result_map: dict[str, StockScanResult] = {}
    total_candidates = 0
    total_kept = 0

    for code in codes:
        code_frame = per_code.get(code)
        if code_frame is None or code_frame.empty:
            continue

        try:
            code_result, stats = _scan_one_code(
                code=code,
                name=code_to_name.get(code, ""),
                code_frame=code_frame,
                weekly_params=weekly_params,
                ref_params=ref_params,
                ref_low_map=ref_low_map,
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
