"""
`converging_triangle_v1` specialized engine。

职责：
1. 按周线/日线两个可选周期，批量加载 K 线数据并在每个启用周期上独立检测大三角收敛末端形态。
2. 三角形定义：上沿整体下降、下沿整体上升，区间宽度持续收缩，
   当前价格位于三角结构内部且接近末端（apex）。
3. 上下沿通过分段分位数边界 + 线性回归拟合得到：
   - 上沿 = 各段 high 的 90% 分位数
   - 下沿 = 各段 low 的 10% 分位数
4. 两个周期独立检测，同一标的若同时命中 D 和 W，仅保留 W 结果。
5. 同一标的、同一周期若有多个窗口通过条件，仅保留评分最高的一个。

三角检测算法：
  1. 在候选窗口 [t_s, t_e] 内，将 bar 均分为 segment_count 段。
  2. 计算各段上沿代表值 Q90(high) 和下沿代表值 Q10(low)。
  3. 对上下沿点做线性回归，得到 U(x) = a_u + b_u*x，L(x) = a_l + b_l*x。
  4. 早期淘汰：先检查上沿斜率 < 0、下沿斜率 > 0，不满足立即跳过。
  5. 依次检查收敛速度、对称性、收缩比例、apex 进度、当前价格位置。
  6. 全部通过后计算综合评分用于同周期最优窗口选择。
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

STRATEGY_LABEL = "大收敛三角 v1"

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

# 单位周期 bar 数（用于斜率标准化）
_TF_UNIT_BARS: dict[str, int] = {
    "d": 30,
    "w": 4,
}

# 窗口枚举步长（t_e 和 t_s 统一步长）
_TF_STEP: dict[str, int] = {
    "d": 5,
    "w": 1,
}

# 评分权重
_SCORE_W_CONTRACTION = 0.35
_SCORE_W_APEX = 0.30
_SCORE_W_SYMMETRY = 0.20
_SCORE_W_FIT = 0.15


# ---------------------------------------------------------------------------
# 三角检测结果
# ---------------------------------------------------------------------------


@dataclass
class TriangleResult:
    """单个候选窗口的三角检测结果。

    输入：
    1. 由 _evaluate_triangle() 在所有条件通过后构建。
    输出：
    1. 供 _scan_one_code() 用于同周期最优窗口选择和信号构建。
    """

    score: float
    start_idx: int
    end_idx: int
    window_bars: int
    # 上下沿拟合参数
    a_u: float
    b_u: float
    a_l: float
    b_l: float
    # 各项指标
    upper_slope_unit: float
    lower_slope_unit: float
    converging_speed: float
    slope_symmetry_ratio: float
    contraction_ratio: float
    apex_progress: float
    fit_error: float
    # 上下沿边界点（用于前端展示）
    upper_start: float
    upper_end: float
    lower_start: float
    lower_end: float
    # 上下沿触碰次数
    upper_touch_count: int
    lower_touch_count: int


# ---------------------------------------------------------------------------
# 参数规范化
# ---------------------------------------------------------------------------


def _normalize_tf_params(group_params: dict[str, Any], section_key: str) -> dict[str, Any]:
    """规范化单个周期的三角检测参数组。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    2. section_key: manifest 参数组 key（weekly / daily）。
    输出：
    1. 返回经类型转换和边界裁剪后的周期参数字典。百分数参数已转为比例。
    用途：
    1. 统一处理两个周期的相同参数结构。
    边界条件：
    1. pattern_months_max 强制 ≥ pattern_months_min。
    2. converging_speed_min / contraction_ratio_max / apex_progress_min 为百分数，内部转比例。
    """
    raw = as_dict(group_params.get(section_key))

    pattern_months_min = as_int(raw.get("pattern_months_min"), 3, minimum=1, maximum=24)

    return {
        "enabled": as_bool(raw.get("enabled"), False),
        "search_recent_weeks": as_int(raw.get("search_recent_weeks"), 12, minimum=1, maximum=52),
        "pattern_months_min": pattern_months_min,
        "pattern_months_max": as_int(
            raw.get("pattern_months_max"), 10,
            minimum=pattern_months_min, maximum=36,
        ),
        "converging_speed_min": as_float(raw.get("converging_speed_min"), 8.0, minimum=0.1, maximum=100.0) / 100.0,
        "slope_symmetry_tolerance": as_float(raw.get("slope_symmetry_tolerance"), 0.35, minimum=0.0, maximum=1.0),
        "contraction_ratio_max": as_float(raw.get("contraction_ratio_max"), 45.0, minimum=1.0, maximum=99.0) / 100.0,
        "apex_progress_min": as_float(raw.get("apex_progress_min"), 75.0, minimum=10.0, maximum=99.0) / 100.0,
        "segment_count": as_int(raw.get("segment_count"), 4, minimum=2, maximum=20),
        "upper_edge_zone_ratio": as_float(raw.get("upper_edge_zone_ratio"), 10.0, minimum=1.0, maximum=50.0) / 100.0,
        "lower_edge_zone_ratio": as_float(raw.get("lower_edge_zone_ratio"), 10.0, minimum=1.0, maximum=50.0) / 100.0,
        "min_upper_touch_count": as_int(raw.get("min_upper_touch_count"), 3, minimum=0, maximum=20),
        "min_lower_touch_count": as_int(raw.get("min_lower_touch_count"), 2, minimum=0, maximum=20),
        "lower_break_pct": as_float(raw.get("lower_break_pct"), 10.0, minimum=0.0, maximum=50.0) / 100.0,
    }


# ---------------------------------------------------------------------------
# 数据加载
# ---------------------------------------------------------------------------


def _compute_days_back(params: dict[str, Any], tf_key: str) -> int:
    """根据参数计算需要回溯的自然日天数。

    输入：
    1. params: 已规范化的周期参数。
    2. tf_key: 周期标识。
    输出：
    1. 包含安全余量的自然日天数。
    用途：
    1. 构造 SQL 日期范围时将参数需求转换为日历天数。
    边界条件：
    1. 需要覆盖 search_recent_weeks + pattern_months_max 的完整范围。
    """
    search_days = params["search_recent_weeks"] * 7
    pattern_days = int(params["pattern_months_max"] * 30)
    base = search_days + pattern_days
    if tf_key == "w":
        return base + 60
    return int(base * 1.5) + 30


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
    3. tf_key: 周期标识（w/d）。
    4. start_day/end_day: 日期范围边界。
    输出：
    1. 返回按 `code + ts` 排序的 DataFrame，时间列统一命名为 `ts`。
    边界条件：
    1. `codes` 为空时返回空表。
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
# 三角检测核心
# ---------------------------------------------------------------------------


def _evaluate_triangle(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    segment_count: int,
    converging_speed_min: float,
    slope_symmetry_tolerance: float,
    contraction_ratio_max: float,
    apex_progress_min: float,
    tf_key: str,
    upper_edge_zone_ratio: float = 0.10,
    lower_edge_zone_ratio: float = 0.10,
    min_upper_touch_count: int = 3,
    min_lower_touch_count: int = 2,
) -> TriangleResult | None:
    """对单个候选窗口执行三角形检测。

    输入：
    1. highs/lows/closes: 窗口内 K 线数据的 numpy 数组。
    2. segment_count: 分段数量。
    3. converging_speed_min: 最小收敛速度（比例）。
    4. slope_symmetry_tolerance: 对称性最大允许偏差。
    5. contraction_ratio_max: 最大收缩比例。
    6. apex_progress_min: 最小 apex 进度（比例）。
    7. tf_key: 周期标识，用于确定单位斜率换算的 B_unit。
    8. upper_edge_zone_ratio: 上沿触碰区域比例（相对三角宽度）。
    9. lower_edge_zone_ratio: 下沿触碰区域比例（相对三角宽度）。
    10. min_upper_touch_count: 上沿最少触碰次数。
    11. min_lower_touch_count: 下沿最少触碰次数。
    输出：
    1. 通过则返回 TriangleResult，否则返回 None。
    用途：
    1. 按 spec §9-§17 逐条检查三角形条件，含早期淘汰机制。
    2. 通过几何条件后，统计上下沿触碰次数作为额外过滤条件。
    边界条件：
    1. 窗口 bar 数不足 segment_count 时返回 None。
    2. 上下沿斜率方向不满足时立即短路返回。
    3. 触碰次数不足时返回 None。
    """
    n = len(highs)
    if n < segment_count:
        return None

    # §9 分段分位数边界构造
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

    # §10 线性回归
    b_u, a_u = np.polyfit(xs, us, 1)
    b_l, a_l = np.polyfit(xs, ls, 1)

    # §11 参考价格与单位斜率
    p_ref = float(np.median(closes))
    if p_ref <= 0:
        return None

    b_unit = _TF_UNIT_BARS.get(tf_key, 30)
    upper_slope_unit = (b_u * b_unit) / p_ref
    lower_slope_unit = (b_l * b_unit) / p_ref

    # §4.2 [早期淘汰] 上沿必须下降
    if upper_slope_unit >= 0:
        return None

    # §4.2 [早期淘汰] 下沿必须上升
    if lower_slope_unit <= 0:
        return None

    # §12 收敛速度
    converging_speed = lower_slope_unit - upper_slope_unit
    if converging_speed < converging_speed_min:
        return None

    # §13 对称性
    abs_upper = abs(upper_slope_unit)
    abs_lower = abs(lower_slope_unit)
    denom = max(abs_upper, abs_lower)
    if denom == 0:
        return None
    slope_symmetry_ratio = abs(abs_upper - abs_lower) / denom
    if slope_symmetry_ratio > slope_symmetry_tolerance:
        return None

    # §14 宽度与收缩比例
    upper_start = a_u  # U(0)
    lower_start = a_l  # L(0)
    width_start = upper_start - lower_start
    if width_start <= 0:
        return None

    x_end = float(n - 1)
    upper_end = a_u + b_u * x_end  # U(L-1)
    lower_end = a_l + b_l * x_end  # L(L-1)
    width_now = upper_end - lower_end
    if width_now <= 0:
        return None

    contraction_ratio = width_now / width_start
    if contraction_ratio > contraction_ratio_max:
        return None

    # §15 Apex 与末端进度
    denom_apex = b_u - b_l
    if denom_apex == 0:
        return None
    x_apex = (a_l - a_u) / denom_apex
    if x_apex <= 0:
        return None

    apex_progress = x_end / x_apex
    if apex_progress < apex_progress_min:
        return None
    if apex_progress >= 1.0:
        return None

    # §16 当前价格位置
    c_now = float(closes[-1])
    if c_now < lower_end or c_now > upper_end:
        return None

    # §16b 上下沿触碰次数统计
    # 对窗口内每根 bar 计算归一化位置，判断是否进入触碰区域。
    # 连续满足条件的 bar 合并为一次独立触碰事件。
    x_range = np.arange(n, dtype=np.float64)
    u_line = a_u + b_u * x_range  # 上沿拟合值
    l_line = a_l + b_l * x_range  # 下沿拟合值
    w_line = u_line - l_line       # 各 bar 处的三角宽度

    # 防止宽度为零导致除零
    w_safe = np.where(w_line > 0, w_line, 1.0)
    pos_u = (highs - l_line) / w_safe   # 上沿归一化位置
    pos_l = (lows - l_line) / w_safe    # 下沿归一化位置

    upper_touching = pos_u >= (1.0 - upper_edge_zone_ratio)
    lower_touching = pos_l <= lower_edge_zone_ratio

    # 统计独立触碰事件：连续 True 合并为一次
    upper_touch_count = int(np.sum(np.diff(upper_touching.astype(np.int8)) == 1))
    if upper_touching[0]:
        upper_touch_count += 1
    lower_touch_count = int(np.sum(np.diff(lower_touching.astype(np.int8)) == 1))
    if lower_touching[0]:
        lower_touch_count += 1

    if upper_touch_count < min_upper_touch_count:
        return None
    if lower_touch_count < min_lower_touch_count:
        return None

    # 全部通过，计算评分 §18
    score_contraction = 1.0 - contraction_ratio
    score_apex = apex_progress
    score_symmetry = 1.0 - slope_symmetry_ratio

    # fit_error: MARE
    fitted_upper = a_u + b_u * xs
    fitted_lower = a_l + b_l * xs
    upper_errors = np.abs(us - fitted_upper) / np.where(fitted_upper > 0, fitted_upper, 1.0)
    lower_errors = np.abs(ls - fitted_lower) / np.where(fitted_lower > 0, fitted_lower, 1.0)
    fit_error = float(np.mean(np.concatenate([upper_errors, lower_errors])))
    score_fit = max(0.0, 1.0 - fit_error)

    score = (
        _SCORE_W_CONTRACTION * score_contraction
        + _SCORE_W_APEX * score_apex
        + _SCORE_W_SYMMETRY * score_symmetry
        + _SCORE_W_FIT * score_fit
    )

    return TriangleResult(
        score=round(score, 6),
        start_idx=0,
        end_idx=n - 1,
        window_bars=n,
        a_u=round(a_u, 6),
        b_u=round(b_u, 6),
        a_l=round(a_l, 6),
        b_l=round(b_l, 6),
        upper_slope_unit=round(upper_slope_unit, 6),
        lower_slope_unit=round(lower_slope_unit, 6),
        converging_speed=round(converging_speed, 6),
        slope_symmetry_ratio=round(slope_symmetry_ratio, 6),
        contraction_ratio=round(contraction_ratio, 6),
        apex_progress=round(apex_progress, 6),
        fit_error=round(fit_error, 6),
        upper_start=round(float(upper_start), 4),
        upper_end=round(float(upper_end), 4),
        lower_start=round(float(lower_start), 4),
        lower_end=round(float(lower_end), 4),
        upper_touch_count=upper_touch_count,
        lower_touch_count=lower_touch_count,
    )


def _scan_tf_for_code(
    *,
    code_frame: pd.DataFrame,
    params: dict[str, Any],
    tf_key: str,
) -> TriangleResult | None:
    """在单只股票的单个周期数据上搜索最优三角形窗口。

    输入：
    1. code_frame: 单只股票在该周期的 OHLCV DataFrame（已按 ts 排序）。
    2. params: 已规范化的单周期参数。
    3. tf_key: 周期标识。
    输出：
    1. 返回该周期下评分最高的 TriangleResult，或无命中时返回 None。
       结果中 start_idx / end_idx 为窗口在 code_frame 中的绝对索引。
    用途：
    1. 枚举所有合法候选窗口，逐窗口评估后保留同周期最优。
    边界条件：
    1. 数据不足时返回 None。
    2. t_e 和 t_s 统一步长枚举，通过时间戳校验月数合法性。
    """
    if code_frame.empty:
        return None

    ts_arr = code_frame["ts"].values  # numpy datetime64 array
    highs_all = code_frame["high"].values.astype(np.float64)
    lows_all = code_frame["low"].values.astype(np.float64)
    closes_all = code_frame["close"].values.astype(np.float64)
    n_total = len(code_frame)

    search_recent_weeks = params["search_recent_weeks"]
    pattern_months_min = params["pattern_months_min"]
    pattern_months_max = params["pattern_months_max"]
    segment_count = params["segment_count"]
    converging_speed_min = params["converging_speed_min"]
    slope_symmetry_tolerance = params["slope_symmetry_tolerance"]
    contraction_ratio_max = params["contraction_ratio_max"]
    apex_progress_min = params["apex_progress_min"]

    step = _TF_STEP[tf_key]

    # 搜索范围：t_e 的时间必须在 [T_latest - search_recent_weeks, T_latest]
    latest_ts = pd.Timestamp(ts_arr[-1])
    search_start_ts = latest_ts - pd.Timedelta(weeks=search_recent_weeks)

    # 月数换算常量
    days_per_month = 30.0

    best: TriangleResult | None = None

    # t_e 从最新向前枚举
    for e_idx in range(n_total - 1, -1, -step):
        te_ts = pd.Timestamp(ts_arr[e_idx])
        if te_ts < search_start_ts:
            break

        # t_s 从最远到最近枚举
        earliest_start_ts = te_ts - pd.Timedelta(days=pattern_months_max * days_per_month)
        latest_start_ts = te_ts - pd.Timedelta(days=pattern_months_min * days_per_month)

        for s_idx in range(0, e_idx, step):
            ts_ts = pd.Timestamp(ts_arr[s_idx])

            # 时间合法性检查
            if ts_ts < earliest_start_ts:
                continue
            if ts_ts > latest_start_ts:
                break

            # 窗口 bar 数必须足够分段
            window_len = e_idx - s_idx + 1
            if window_len < segment_count:
                continue

            # 半区粗筛：前半 high 最大值应 > 后半（上沿下降趋势），
            # 后半 low 最小值应 > 前半（下沿上升趋势）。
            # 比完整 _evaluate_triangle 便宜一个数量级，可淘汰 70-85% 的窗口。
            mid = (s_idx + e_idx) // 2
            if highs_all[mid:e_idx + 1].max() >= highs_all[s_idx:mid].max():
                continue
            if lows_all[mid:e_idx + 1].min() < lows_all[s_idx:mid].min():
                continue

            # 提取窗口数据切片
            w_highs = highs_all[s_idx:e_idx + 1]
            w_lows = lows_all[s_idx:e_idx + 1]
            w_closes = closes_all[s_idx:e_idx + 1]

            result = _evaluate_triangle(
                highs=w_highs,
                lows=w_lows,
                closes=w_closes,
                segment_count=segment_count,
                converging_speed_min=converging_speed_min,
                slope_symmetry_tolerance=slope_symmetry_tolerance,
                contraction_ratio_max=contraction_ratio_max,
                apex_progress_min=apex_progress_min,
                tf_key=tf_key,
                upper_edge_zone_ratio=params["upper_edge_zone_ratio"],
                lower_edge_zone_ratio=params["lower_edge_zone_ratio"],
                min_upper_touch_count=params["min_upper_touch_count"],
                min_lower_touch_count=params["min_lower_touch_count"],
            )

            if result is not None:
                # 记录绝对索引
                result.start_idx = s_idx
                result.end_idx = e_idx
                if best is None or result.score > best.score:
                    best = result

    # 最新 K 线破位检查：若最新收盘价低于三角下沿的容忍度则放弃
    if best is not None:
        latest_close = float(closes_all[-1])
        lower_threshold = best.lower_end * (1.0 - params["lower_break_pct"])
        if latest_close < lower_threshold:
            best = None

    return best


# ---------------------------------------------------------------------------
# 单股扫描
# ---------------------------------------------------------------------------


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
    """扫描单只股票：在所有已启用周期上执行三角检测。

    输入：
    1. code/name: 股票标识。
    2. tf_data: {tf_key: 该股票的 DataFrame} 仅包含已启用周期。
    3. all_params: {tf_key: 已规范化参数} 仅包含已启用周期。
    4. enabled_tfs: 按粗→细排序的已启用周期列表。
    5. strategy_group_id/strategy_name: 构建信号用。
    输出：
    1. (StockScanResult, 统计计数 dict)。
    边界条件：
    1. 两个周期独立检测，同一标的若同时命中 D 和 W，仅保留 W。
    2. 每个周期仅保留评分最高的一个窗口。
    """
    stats = {"candidate": 0, "kept": 0}
    total_bars = sum(len(df) for df in tf_data.values())

    per_tf_result: dict[str, TriangleResult] = {}

    for tf in enabled_tfs:
        frame = tf_data.get(tf, pd.DataFrame())
        params = all_params[tf]
        if frame.empty:
            continue
        tri = _scan_tf_for_code(
            code_frame=frame,
            params=params,
            tf_key=tf,
        )
        if tri is not None:
            per_tf_result[tf] = tri

    stats["candidate"] = 1
    if not per_tf_result:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=total_bars,
            signal_count=0,
            signals=[],
        ), stats

    # §19 跨周期去重：W 优先
    if "w" in per_tf_result:
        chosen_tf = "w"
    elif "d" in per_tf_result:
        chosen_tf = "d"
    else:
        chosen_tf = list(per_tf_result.keys())[0]

    tri = per_tf_result[chosen_tf]
    frame = tf_data[chosen_tf]

    # 构建展示数据
    window_start_ts = frame.iloc[tri.start_idx]["ts"]
    window_end_ts = frame.iloc[tri.end_idx]["ts"]

    # 图表区间：整个窗口即为展示区间，额外向前延伸 10% 以提供上下文
    extra_bars = max(int(tri.window_bars * 0.1), 5)
    chart_start_idx = max(tri.start_idx - extra_bars, 0)
    chart_interval_start_ts = frame.iloc[chart_start_idx]["ts"]
    chart_interval_end_ts = frame.iloc[tri.end_idx]["ts"]

    tf_label = _TF_LABEL.get(chosen_tf, chosen_tf)
    signal_label = f"{tf_label}大收敛三角"

    # 构建 overlay_lines（上下沿拟合线坐标）
    overlay_lines = [
        {
            "label": "上沿",
            "start_ts": window_start_ts,
            "start_price": tri.upper_start,
            "end_ts": window_end_ts,
            "end_price": tri.upper_end,
            "color": "#ef4444",
            "dash": True,
        },
        {
            "label": "下沿",
            "start_ts": window_start_ts,
            "start_price": tri.lower_start,
            "end_ts": window_end_ts,
            "end_price": tri.lower_end,
            "color": "#22c55e",
            "dash": True,
        },
    ]

    # 构建诊断信息
    hit_tfs = list(per_tf_result.keys())
    per_tf_detail: dict[str, dict[str, Any]] = {}
    for tf, result in per_tf_result.items():
        per_tf_detail[tf] = {
            "detected": True,
            "score": result.score,
            "upper_slope_unit": result.upper_slope_unit,
            "lower_slope_unit": result.lower_slope_unit,
            "converging_speed": result.converging_speed,
            "slope_symmetry_ratio": result.slope_symmetry_ratio,
            "contraction_ratio": result.contraction_ratio,
            "apex_progress": result.apex_progress,
            "fit_error": result.fit_error,
            "window_bars": result.window_bars,
            "upper_touch_count": result.upper_touch_count,
            "lower_touch_count": result.lower_touch_count,
        }

    payload: dict[str, Any] = {
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "chart_interval_start_ts": chart_interval_start_ts,
        "chart_interval_end_ts": chart_interval_end_ts,
        "anchor_day_ts": window_end_ts,
        "hit_timeframes": hit_tfs,
        "chosen_tf": chosen_tf,
        "per_tf": per_tf_detail,
        "overlay_lines": overlay_lines,
        "signal_summary": signal_label,
    }

    signal = build_signal_dict(
        code=code,
        name=name,
        signal_dt=window_end_ts,
        clock_tf=chosen_tf,
        strategy_group_id=strategy_group_id,
        strategy_name=strategy_name,
        signal_label=signal_label,
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


def run_converging_triangle_v1_specialized(
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
    """`converging_triangle_v1` specialized 主入口。

    输入：
    1. 所有参数由 TaskManager 以关键字方式传入。
    输出：
    1. 返回 (result_map, metrics)，供 TaskManager 写库和前端展示使用。
    用途：
    1. 组织参数规范化、各周期数据加载、逐股三角检测和汇总统计。
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
        days_back = _compute_days_back(params, tf)
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
