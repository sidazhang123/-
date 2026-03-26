"""
`flag_pattern_v1` specialized engine。

职责：
1. 按周线/日线/15分钟三个可选周期，批量加载 K 线数据并在每个启用周期上独立检测旗形形态。
2. 旗形定义：旗杆（连续若干根线的真实冲高）+ 旗面（紧接旗杆之后的收敛区段）。
3. 旗面收敛通过历史波动率（HV）的线性回归标准化斜率衡量。
4. 多周期之间为并集关系（OR）：任一启用周期检测到旗形即产生信号。
5. 同一股票多周期命中时合并为 1 条信号，clock_tf 取最粗命中周期。

旗形识别算法：
  1. 在最新 scan_bars 根 K 线中，从尾部向前搜索有效 旗杆+旗面 组合。
  2. 旗杆约束：pole_len ∈ [min, max]，首根 bar 的 low 为旗杆最低点 P0，
     区间内 max(high) 为旗杆顶端 P1（P1 bar 必须在 P0 bar 之后），
     pole_return = (P1−P0)/P0 ∈ [min, max]。
  3. 旗面宽度：flag_len ∈ [pole_len×ratio_min, pole_len×ratio_max]。
  4. 旗面空间：(flag_high−P1)/pole_gain ≤ high_above_max，
               (P1−flag_low)/pole_gain ≤ low_below_max。
  5. 旗面收敛：从旗面 converge_start_frac 位置开始计算 HV 线性回归标准化斜率 ≤ 阈值。
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

STRATEGY_LABEL = "多周期旗形上涨 v1"

# 周期 key → klines 表名
_TF_TABLE: dict[str, str] = {
    "w": "klines_w",
    "d": "klines_d",
    "15": "klines_15",
}

# 周期粗细排序（粗 → 细）
_TF_ORDER: list[str] = ["w", "d", "15"]

# manifest 参数组 key → 周期 key
_PARAM_SECTION_TO_TF: dict[str, str] = {
    "weekly": "w",
    "daily": "d",
    "min15": "15",
}

_TF_TO_PARAM_SECTION: dict[str, str] = {v: k for k, v in _PARAM_SECTION_TO_TF.items()}

# 周期中文名
_TF_LABEL: dict[str, str] = {
    "w": "周线",
    "d": "日线",
    "15": "15min",
}


# ---------------------------------------------------------------------------
# 参数规范化
# ---------------------------------------------------------------------------


def _normalize_tf_params(group_params: dict[str, Any], section_key: str) -> dict[str, Any]:
    """规范化单个周期的旗形参数组。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    2. section_key: manifest 参数组 key（weekly / daily / min15）。
    输出：
    1. 返回经类型转换和边界裁剪后的周期参数字典。
    用途：
    1. 统一处理三个周期的相同参数结构。
    边界条件：
    1. pole_len_max 强制 ≥ pole_len_min。
    2. pole_return_max 强制 ≥ pole_return_min。
    3. flag_width_ratio_max 强制 ≥ flag_width_ratio_min。
    """
    raw = as_dict(group_params.get(section_key))
    pole_len_min = as_int(raw.get("pole_len_min"), 2, minimum=1, maximum=50)
    # 前端以百分数展示，引擎内部统一转为小数
    pole_return_min_pct = as_float(raw.get("pole_return_min"), 5.0, minimum=0.1, maximum=1000.0)
    pole_return_min = pole_return_min_pct / 100.0
    flag_width_ratio_min_pct = as_float(raw.get("flag_width_ratio_min"), 30.0, minimum=1.0, maximum=1000.0)
    flag_width_ratio_min = flag_width_ratio_min_pct / 100.0
    return {
        "enabled": as_bool(raw.get("enabled"), False),
        "scan_bars": as_int(raw.get("scan_bars"), 30, minimum=5, maximum=500),
        "pole_len_min": pole_len_min,
        "pole_len_max": as_int(raw.get("pole_len_max"), 5, minimum=pole_len_min, maximum=100),
        "pole_return_min": pole_return_min,
        "pole_return_max": as_float(raw.get("pole_return_max"), 15.0, minimum=pole_return_min_pct, maximum=1000.0) / 100.0,
        "flag_width_ratio_min": flag_width_ratio_min,
        "flag_width_ratio_max": as_float(raw.get("flag_width_ratio_max"), 150.0, minimum=flag_width_ratio_min_pct, maximum=2000.0) / 100.0,
        "flag_high_above_max": as_float(raw.get("flag_high_above_max"), 5.0, minimum=0.0, maximum=500.0) / 100.0,
        "flag_low_below_max": as_float(raw.get("flag_low_below_max"), 30.0, minimum=0.0, maximum=500.0) / 100.0,
        "converge_start_frac": as_float(raw.get("converge_start_frac"), 30.0, minimum=0.0, maximum=90.0) / 100.0,
        "hv_window": as_int(raw.get("hv_window"), 3, minimum=2, maximum=50),
        "hv_slope_norm_max": as_float(raw.get("hv_slope_norm_max"), -0.2, maximum=10.0),
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
    3. tf_key: 周期标识（w/d/15）。
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
# 旗形检测核心
# ---------------------------------------------------------------------------


def _check_hv_convergence(
    closes: np.ndarray,
    params: dict[str, Any],
    flag_len: int,
) -> tuple[bool, float, str]:
    """检查旗面区段的 HV 线性回归标准化斜率是否满足收敛条件。

    输入：
    1. closes: 旗面区段的收盘价序列（numpy 数组）。
    2. params: 已规范化的单周期参数。
    3. flag_len: 旗面总长度（根数）。
    输出：
    1. (是否通过, 标准化斜率值, 失败原因描述)。
    用途：
    1. 判断旗面后段波动率是否呈下降趋势。
    边界条件：
    1. 有效 HV 值不足 2 个时返回 False（无法做线性回归）。
    2. mean(HV) 为零时返回 False（防止除零）。
    """
    hv_window = int(params["hv_window"])
    converge_start_frac = float(params["converge_start_frac"])
    hv_slope_norm_max = float(params["hv_slope_norm_max"])

    if len(closes) < 2:
        return False, 0.0, "旗面收盘价不足 2 根，无法计算收益率"

    # log return
    log_returns = np.log(closes[1:] / closes[:-1])

    if len(log_returns) < hv_window:
        return False, 0.0, f"收益率序列长度 {len(log_returns)} 不足 hv_window={hv_window}"

    # rolling HV (标准差)
    hv_series = pd.Series(log_returns).rolling(hv_window, min_periods=hv_window).std().values

    # 截取收敛检查段
    start_idx = int(math.floor(converge_start_frac * flag_len))
    # hv_series 的长度 = len(closes) - 1，索引对齐到旗面第 1 根起
    # 从 start_idx 开始截取（确保 start_idx 在 hv_series 范围内）
    start_idx = min(start_idx, len(hv_series) - 1)
    hv_segment = hv_series[start_idx:]

    # 剔除 NaN
    valid_mask = ~np.isnan(hv_segment)
    hv_valid = hv_segment[valid_mask]

    if len(hv_valid) < 2:
        return False, 0.0, f"收敛段有效 HV 值仅 {len(hv_valid)} 个，不足 2 个做线性回归"

    mean_hv = float(np.mean(hv_valid))
    if mean_hv <= 0:
        return False, 0.0, "收敛段 HV 均值为零，无法标准化"

    # 线性回归 HV = a + b*t
    t = np.arange(len(hv_valid), dtype=np.float64)
    coeffs = np.polyfit(t, hv_valid, 1)  # coeffs[0] = b (斜率), coeffs[1] = a (截距)
    b = float(coeffs[0])
    segment_len = len(hv_valid)

    # 标准化斜率
    hv_slope_norm = (b * segment_len) / mean_hv

    passed = hv_slope_norm <= hv_slope_norm_max
    reason = "" if passed else f"HV标准化斜率 {hv_slope_norm:.4f} > 阈值 {hv_slope_norm_max}"
    return passed, round(hv_slope_norm, 4), reason


def detect_flag(
    code_frame: pd.DataFrame,
    params: dict[str, Any],
) -> DetectionResult:
    """在单只股票的单个周期数据上检测旗形。

    输入：
    1. code_frame: 单只股票在该周期的 OHLCV DataFrame（已按 ts 排序）。
    2. params: 已规范化的单周期参数。
    输出：
    1. DetectionResult，matched 表示是否命中，metrics 含旗杆+旗面明细。
    用途：
    1. 从窗口尾部向前搜索最新的一个有效 旗杆+旗面 组合。
    2. 可被筛选编排器和未来回测引擎独立调用。
    边界条件：
    1. 数据不足 scan_bars 时使用全部可用数据。
    2. 旗杆首根 bar 的 low 必须是旗杆段的最小 low（P0 约束）。
    3. P1 所在 bar 必须在 P0 所在 bar 之后（上涨旗杆方向约束）。
    4. 找到第一个有效组合即返回，不继续搜索。
    """
    scan_bars = int(params["scan_bars"])
    pole_len_min = int(params["pole_len_min"])
    pole_len_max = int(params["pole_len_max"])
    pole_return_min = float(params["pole_return_min"])
    pole_return_max = float(params["pole_return_max"])
    flag_width_ratio_min = float(params["flag_width_ratio_min"])
    flag_width_ratio_max = float(params["flag_width_ratio_max"])
    flag_high_above_max = float(params["flag_high_above_max"])
    flag_low_below_max = float(params["flag_low_below_max"])

    if len(code_frame) < pole_len_min + 1:
        return DetectionResult(matched=False, metrics={"reason": "数据不足"})

    # 截取最新 scan_bars 根线
    window = code_frame.tail(scan_bars).reset_index(drop=True)
    n = len(window)

    highs = window["high"].values
    lows = window["low"].values
    closes = window["close"].values

    # 从窗口尾部向前搜索：pole_end 是旗杆最后一根 bar 的索引
    # 旗面紧接 pole_end 之后，需要至少 1 根旗面 bar
    # 搜索时 pole_end 从大到小遍历，找到第一个有效组合即返回
    for pole_end in range(n - 2, pole_len_min - 2, -1):
        for pole_len in range(pole_len_max, pole_len_min - 1, -1):
            pole_start = pole_end - pole_len + 1
            if pole_start < 0:
                continue

            # 旗杆区段
            pole_highs = highs[pole_start: pole_end + 1]
            pole_lows = lows[pole_start: pole_end + 1]

            # P0 约束：旗杆首根 bar 的 low 必须是旗杆段的最小 low
            p0 = float(pole_lows[0])
            if p0 > float(pole_lows.min()):
                continue

            # P1 = max(high)
            p1_local_idx = int(np.argmax(pole_highs))
            p1 = float(pole_highs[p1_local_idx])

            # P1 必须在 P0 之后（P0 在索引 0，P1 索引必须 > 0）
            if p1_local_idx == 0:
                continue

            # 旗杆涨幅
            if p0 <= 0:
                continue
            pole_gain = p1 - p0
            pole_return = pole_gain / p0
            if pole_return < pole_return_min or pole_return > pole_return_max:
                continue

            # 旗面宽度范围
            flag_len_min_calc = max(1, int(math.floor(pole_len * flag_width_ratio_min)))
            flag_len_max_calc = int(math.ceil(pole_len * flag_width_ratio_max))

            # 旗面起始位置
            flag_start = pole_end + 1
            available_bars = n - flag_start

            if available_bars < flag_len_min_calc:
                continue

            # 在允许范围内尝试最长旗面（更充分的收敛检查）
            actual_flag_len_max = min(flag_len_max_calc, available_bars)

            # 从最长旗面开始尝试（更多数据做 HV 检查）
            for flag_len in range(actual_flag_len_max, flag_len_min_calc - 1, -1):
                flag_end = flag_start + flag_len - 1  # inclusive

                flag_highs = highs[flag_start: flag_end + 1]
                flag_lows = lows[flag_start: flag_end + 1]
                flag_closes = closes[flag_start: flag_end + 1]

                flag_high = float(flag_highs.max())
                flag_low = float(flag_lows.min())

                # 旗面空间约束
                if pole_gain <= 0:
                    continue
                high_above = (flag_high - p1) / pole_gain
                low_below = (p1 - flag_low) / pole_gain

                if high_above > flag_high_above_max:
                    continue
                if low_below > flag_low_below_max:
                    continue

                # 旗面收敛检查（包含旗杆最后一根的 close 作为首个收益率基准）
                # 把旗杆最后一根 close 拼在旗面 close 前面，用于计算第一个 log return
                convergence_closes = np.concatenate([[closes[pole_end]], flag_closes])
                hv_ok, hv_slope_norm, hv_reason = _check_hv_convergence(
                    convergence_closes, params, flag_len,
                )

                if not hv_ok:
                    continue

                # 命中！
                pole_start_ts = window.iloc[pole_start]["ts"]
                pole_end_ts = window.iloc[pole_end]["ts"]
                flag_start_ts = window.iloc[flag_start]["ts"]
                flag_end_ts = window.iloc[flag_end]["ts"]

                return DetectionResult(
                    matched=True,
                    pattern_start_idx=int(pole_start),
                    pattern_end_idx=int(flag_end),
                    pattern_start_ts=pole_start_ts,
                    pattern_end_ts=flag_end_ts,
                    metrics={
                        "pole_start_ts": pole_start_ts,
                        "pole_end_ts": pole_end_ts,
                        "flag_start_ts": flag_start_ts,
                        "flag_end_ts": flag_end_ts,
                        "pole_len": pole_len,
                        "flag_len": flag_len,
                        "p0": round(p0, 4),
                        "p1": round(p1, 4),
                        "pole_return": round(pole_return, 4),
                        "pole_gain": round(pole_gain, 4),
                        "flag_high": round(flag_high, 4),
                        "flag_low": round(flag_low, 4),
                        "high_above": round(high_above, 4),
                        "low_below": round(low_below, 4),
                        "hv_slope_norm": hv_slope_norm,
                    },
                )

    return DetectionResult(
        matched=False,
        metrics={"reason": "未找到符合条件的旗形"},
    )


# ---------------------------------------------------------------------------
# 单股扫描
# ---------------------------------------------------------------------------





def _build_signal_label(hit_tfs: list[str], per_tf_detail: dict[str, dict[str, Any]]) -> str:
    """构建信号标签：拼接各命中周期的旗形摘要。

    输入：
    1. hit_tfs: 命中的周期 key 列表。
    2. per_tf_detail: 各周期检测结果 dict。
    输出：
    1. 如 "日线旗形(杆涨6.2%,面5根) + 15min旗形(杆涨3.8%,面4根)"。
    边界条件：
    1. 命中周期为空时返回策略默认标签。
    """
    parts: list[str] = []
    for tf in hit_tfs:
        detail = per_tf_detail.get(tf, {})
        pole_return = detail.get("pole_return", 0)
        flag_len = detail.get("flag_len", 0)
        parts.append(f"{_TF_LABEL.get(tf, tf)}旗形(杆涨{pole_return*100:.1f}%,面{flag_len}根)")
    return " + ".join(parts) if parts else STRATEGY_LABEL


def build_flag_pattern_payload(
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

    # 展示窗口：从旗杆起始到旗面结束
    window_start_ts = clock_detail["pole_start_ts"]
    window_end_ts = clock_detail["flag_end_ts"]

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
        "anchor_day_ts": clock_detail.get("pole_end_ts", window_end_ts),
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
    """扫描单只股票：在所有已启用周期上执行旗形检测（OR 逻辑）。

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
        result = detect_flag(frame, params)
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
    payload = build_flag_pattern_payload(
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


def run_flag_pattern_v1_specialized(
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
    """`flag_pattern_v1` specialized 主入口。

    输入：
    1. 所有参数由 TaskManager 以关键字方式传入。
    输出：
    1. 返回 (result_map, metrics)，供 TaskManager 写库和前端展示使用。
    用途：
    1. 组织参数规范化、各周期数据加载、逐股旗形检测和汇总统计。
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
        # 需要的最少 bar 数 = scan_bars（已包含旗杆+旗面的窗口范围）
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
