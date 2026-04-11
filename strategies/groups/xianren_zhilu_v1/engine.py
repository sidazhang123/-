"""
`xianren_zhilu_v1` specialized engine。

职责：
1. 按周线/日线两个可选周期，批量加载 K 线数据并在每个启用周期上独立检测仙人指路形态。
2. 仙人指路定义：
   a. 最新K线收上影线：close ≥ open 且 (high − close) / (high − open) × 100 ≥ shadow_ratio%。
   b. 下影线 < 上影线 × lower_shadow_pct%（过滤下影线过长的K线）。
   c. 当日最大涨幅 (high − open) / open × 100 ≥ max_gain_pct%。
   d. 当K线放量：成交量 ≥ 之前3根K线平均成交量 × volume_multiplier。
   e. 前期均线下降：lookback_bars 根前的 MA 值 ≥ 当前 MA 值 × price_decline_ratio。
3. 多周期之间为并集关系（OR）：任一启用周期检测到仙人指路即产生信号。
4. 同一股票多周期命中时合并为 1 条信号，clock_tf 取最粗命中周期。
"""

from __future__ import annotations

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

STRATEGY_LABEL = "仙人指路 v1"

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


# ---------------------------------------------------------------------------
# 参数规范化
# ---------------------------------------------------------------------------


def _normalize_tf_params(group_params: dict[str, Any], section_key: str) -> dict[str, Any]:
    """规范化单个周期的仙人指路参数组。"""
    raw = as_dict(group_params.get(section_key))
    return {
        "enabled": as_bool(raw.get("enabled"), False),
        "ma_period": as_int(raw.get("ma_period"), 5, minimum=2, maximum=60),
        "shadow_ratio": as_float(raw.get("shadow_ratio"), 40, minimum=0.0, maximum=100.0),
        "lower_shadow_pct": as_float(raw.get("lower_shadow_pct"), 30.0, minimum=0.0, maximum=100.0),
        "max_gain_pct": as_float(raw.get("max_gain_pct"), 7.0, minimum=0.0, maximum=30.0),
        "volume_multiplier": as_float(raw.get("volume_multiplier"), 1.3, minimum=1.0),
        "lookback_bars": as_int(raw.get("lookback_bars"), 5, minimum=1, maximum=60),
        "price_decline_ratio": as_float(raw.get("price_decline_ratio"), 1.2, minimum=1.0),
    }


# ---------------------------------------------------------------------------
# 数据加载
# ---------------------------------------------------------------------------


def _bars_to_days(bars: int, tf_key: str) -> int:
    """将 K 线根数估算为自然日天数（含安全余量）。"""
    if tf_key == "w":
        return bars * 7 + 60
    return int(bars * 1.5) + 30


def _load_bars(
    *,
    source_db_path: Path,
    codes: list[str],
    tf_key: str,
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    """批量加载指定周期的 OHLCV 数据。"""
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


def prepare_xianren_zhilu_features(frame: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """计算 MA 均线列（列式特征预计算）。

    输入：
    1. frame: 含多只股票的 OHLCV DataFrame（已按 code+ts 排序）。
    2. params: 已规范化的单周期参数。
    输出：
    1. 返回增加了 ma 列的 DataFrame。
    用途：
    1. 筛选模式：数据加载后调用一次，后续 detect_xianren_zhilu 直接使用已计算的 ma 列。
    2. 回测模式：调用一次后滑窗 N 次 detect，避免重复计算 MA。
    边界条件：
    1. 空 DataFrame 直接返回。
    """
    if frame.empty:
        return frame

    ma_period = int(params["ma_period"])
    prepared: list[pd.DataFrame] = []
    for _, code_frame in frame.groupby("code", sort=False):
        part = code_frame.sort_values("ts").copy()
        part["ma"] = part["close"].rolling(ma_period, min_periods=ma_period).mean()
        prepared.append(part)
    return pd.concat(prepared, axis=0, ignore_index=True)


prepare_xianren_zhilu_features._multi_stock = True


# ---------------------------------------------------------------------------
# 单股单周期检测
# ---------------------------------------------------------------------------


def detect_xianren_zhilu(
    code_frame: pd.DataFrame,
    params: dict[str, Any],
) -> DetectionResult:
    """检测最新一根K线是否满足仙人指路形态。

    输入：
    1. code_frame: 单只股票在该周期的 OHLCV DataFrame（已按 ts 排序，含 ma 列）。
    2. params: 已规范化的单周期参数。
    输出：
    1. DetectionResult，matched 表示是否命中，metrics 含上影线比值、量比、均线衰减比等明细。
    用途：
    1. 检测最新一根K线是否满足仙人指路三条件。
    2. 可被筛选编排器和未来回测引擎独立调用。
    边界条件：
    1. code_frame 必须已包含 ma 列（由 prepare_xianren_zhilu_features 预计算）。
    2. 数据不足时返回 matched=False。
    """
    metrics: dict[str, Any] = {}

    shadow_ratio = float(params["shadow_ratio"])
    lower_shadow_pct = float(params["lower_shadow_pct"])
    max_gain_pct = float(params["max_gain_pct"])
    volume_multiplier = float(params["volume_multiplier"])
    lookback_bars = int(params["lookback_bars"])
    ma_period = int(params["ma_period"])

    # 数据量检查：至少需要 lookback_bars + ma_period 根线
    min_bars = lookback_bars + ma_period
    if len(code_frame) < min_bars:
        metrics["reason"] = f"数据不足，需 {min_bars} 根，仅有 {len(code_frame)} 根"
        return DetectionResult(matched=False, metrics=metrics)

    i = len(code_frame) - 1
    current = code_frame.iloc[i]
    current_ma = current["ma"]

    if pd.isna(current_ma):
        metrics["reason"] = "当前MA值无效"
        return DetectionResult(matched=False, metrics=metrics)

    open_price = float(current["open"])
    close_price = float(current["close"])
    high_price = float(current["high"])
    current_volume = float(current["volume"])

    # 条件1：close >= open（强制）且 high > open（有最大涨幅）
    if close_price < open_price:
        metrics["reason"] = "非阳线"
        return DetectionResult(matched=False, metrics=metrics)

    max_gain = high_price - open_price
    if max_gain <= 0:
        metrics["reason"] = "无最大涨幅"
        return DetectionResult(matched=False, metrics=metrics)

    shadow = high_price - close_price
    if shadow <= 0:
        metrics["reason"] = "无上影线"
        return DetectionResult(matched=False, metrics=metrics)

    # 上影线占最大涨庅百分比
    shadow_pct = shadow / max_gain * 100
    metrics["shadow_ratio"] = round(shadow_pct, 2)
    if shadow_pct < shadow_ratio:
        metrics["reason"] = f"上影线占比 {shadow_pct:.2f}% < 阈值 {shadow_ratio}%"
        return DetectionResult(matched=False, metrics=metrics)

    # 条件1a：下影线 < 上影线 × lower_shadow_pct%
    low_price = float(current["low"])
    lower_shadow = open_price - low_price
    if lower_shadow < 0:
        lower_shadow = 0.0
    lower_shadow_ratio = (lower_shadow / shadow * 100) if shadow > 0 else 0.0
    metrics["lower_shadow_pct"] = round(lower_shadow_ratio, 2)
    if lower_shadow >= shadow * (lower_shadow_pct / 100):
        metrics["reason"] = f"下影线占上影线 {lower_shadow_ratio:.2f}% ≥ 阈值 {lower_shadow_pct}%"
        return DetectionResult(matched=False, metrics=metrics)

    # 条件1b：当日最大涨幅 >= max_gain_pct%
    gain_pct = max_gain / open_price * 100 if open_price > 0 else 0.0
    metrics["max_gain_pct"] = round(gain_pct, 2)
    if gain_pct < max_gain_pct:
        metrics["reason"] = f"最大涨幅 {gain_pct:.2f}% < 阈值 {max_gain_pct}%"
        return DetectionResult(matched=False, metrics=metrics)

    # 条件2：放量（当前成交量 ≥ 前3根均量 × 倍数）
    if i < 3:
        metrics["reason"] = "数据不足3根计算均量"
        return DetectionResult(matched=False, metrics=metrics)

    avg_volume_3 = code_frame.iloc[i - 3:i]["volume"].mean()
    if pd.isna(avg_volume_3) or float(avg_volume_3) == 0:
        metrics["reason"] = "前3根均量无效"
        return DetectionResult(matched=False, metrics=metrics)

    volume_ratio = current_volume / float(avg_volume_3)
    metrics["volume_ratio"] = round(volume_ratio, 2)
    if current_volume < float(avg_volume_3) * volume_multiplier:
        metrics["reason"] = f"量比 {volume_ratio:.2f} < 阈值 {volume_multiplier}"
        return DetectionResult(matched=False, metrics=metrics)

    # 条件3：前期均线下降（lookback_bars 根前的 MA ≥ 当前 MA × price_decline_ratio）
    lookback_idx = i - lookback_bars
    if lookback_idx < 0:
        metrics["reason"] = "回看索引越界"
        return DetectionResult(matched=False, metrics=metrics)

    ma_lookback = code_frame.iloc[lookback_idx]["ma"]
    if pd.isna(ma_lookback):
        metrics["reason"] = "回看MA值无效"
        return DetectionResult(matched=False, metrics=metrics)

    current_ma_val = float(current_ma)
    ma_lookback_val = float(ma_lookback)
    price_decline_ratio = float(params["price_decline_ratio"])

    if current_ma_val > 0:
        ma_decline_ratio = ma_lookback_val / current_ma_val
    else:
        metrics["reason"] = "当前MA为零"
        return DetectionResult(matched=False, metrics=metrics)

    metrics["ma_current"] = round(current_ma_val, 2)
    metrics["ma_lookback"] = round(ma_lookback_val, 2)
    metrics["ma_decline_ratio"] = round(ma_decline_ratio, 2)

    if ma_lookback_val < current_ma_val * price_decline_ratio:
        metrics["reason"] = f"MA下降比 {ma_decline_ratio:.2f} < 阈值 {price_decline_ratio}"
        return DetectionResult(matched=False, metrics=metrics)

    return DetectionResult(
        matched=True,
        pattern_start_idx=i,
        pattern_end_idx=i,
        pattern_start_ts=current["ts"],
        pattern_end_ts=current["ts"],
        metrics=metrics,
    )


def detect_xianren_zhilu_vectorized(
    code_frame: pd.DataFrame,
    params: dict[str, Any],
) -> list[DetectionResult]:
    """在单只股票的完整历史上一次性检测所有仙人指路命中点。

    与 detect_xianren_zhilu 的区别：
    - detect_xianren_zhilu 只检查最新一根K线，供筛选模式和回测外层滑窗使用。
    - 本函数对全部K线做列式布尔运算，收集所有命中记录，
      回测引擎无需再用外层 while 滑窗逐 bar 推进。

    输入：
    1. code_frame: 单只股票完整 OHLCV DataFrame（已按 ts 排序，含 ma 列）。
    2. params: 已规范化的单周期参数。
    输出：
    1. DetectionResult 列表，每个元素对应一根命中K线。
    边界条件：
    1. code_frame 必须已包含 ma 列（由 prepare_xianren_zhilu_features 预计算）。
    2. 数据不足 lookback_bars + ma_period 根时返回空列表。
    """
    if code_frame.empty:
        return []

    shadow_ratio_threshold = float(params["shadow_ratio"])
    lower_shadow_pct = float(params["lower_shadow_pct"])
    max_gain_pct = float(params["max_gain_pct"])
    volume_multiplier = float(params["volume_multiplier"])
    lookback_bars = int(params["lookback_bars"])
    ma_period = int(params["ma_period"])
    price_decline_ratio = float(params["price_decline_ratio"])
    min_bars = lookback_bars + ma_period

    n = len(code_frame)
    if n < min_bars:
        return []

    opens = code_frame["open"].values.astype(np.float64)
    closes = code_frame["close"].values.astype(np.float64)
    highs = code_frame["high"].values.astype(np.float64)
    lows = code_frame["low"].values.astype(np.float64)
    volumes = code_frame["volume"].values.astype(np.float64)
    mas = code_frame["ma"].values.astype(np.float64)
    ts_values = code_frame["ts"].values

    # 条件1：close >= open 且有上影线
    body = closes - opens
    shadow = highs - closes
    max_gain = highs - opens
    is_bull = closes >= opens
    has_shadow = shadow > 0
    has_gain = max_gain > 0

    # 条件2：上影线占最大涨幅百分比 >= shadow_ratio
    safe_gain = np.where(max_gain > 0, max_gain, 1.0)
    cond_shadow = (shadow / safe_gain * 100) >= shadow_ratio_threshold

    # 条件2a：下影线 < 上影线 × lower_shadow_pct%
    lower_shadow = np.maximum(opens - lows, 0.0)
    safe_shadow = np.where(shadow > 0, shadow, 1.0)
    cond_lower = lower_shadow < safe_shadow * (lower_shadow_pct / 100)

    # 条件2b：当日最大涨幅 >= max_gain_pct%
    safe_opens = np.where(opens > 0, opens, 1.0)
    cond_gain_pct = (max_gain / safe_opens * 100) >= max_gain_pct

    # 条件3：放量（volume >= 前3根均量 × multiplier）
    avg_vol_3 = pd.Series(volumes).rolling(3).mean().shift(1).values
    cond_volume = volumes >= avg_vol_3 * volume_multiplier

    # 条件4：前期均线下降（ma[i - lookback_bars] >= ma[i] * price_decline_ratio）
    ma_lookback = np.empty(n, dtype=np.float64)
    ma_lookback[:] = np.nan
    if lookback_bars < n:
        ma_lookback[lookback_bars:] = mas[:-lookback_bars]
    cond_ma = (~np.isnan(mas) & ~np.isnan(ma_lookback)
               & (mas > 0)
               & (ma_lookback >= mas * price_decline_ratio))

    # 数据充足性
    indices = np.arange(n)
    cond_data = indices >= max(min_bars - 1, 3)

    matched = is_bull & has_shadow & has_gain & cond_shadow & cond_lower & cond_gain_pct & cond_volume & cond_ma & cond_data

    hit_indices = np.where(matched)[0]
    results: list[DetectionResult] = []
    for i in hit_indices:
        ii = int(i)
        results.append(DetectionResult(
            matched=True,
            pattern_start_idx=ii,
            pattern_end_idx=ii,
            pattern_start_ts=ts_values[ii],
            pattern_end_ts=ts_values[ii],
            metrics={
                "shadow_ratio": round(float(shadow[ii] / max_gain[ii] * 100), 2),
                "volume_ratio": round(float(volumes[ii] / avg_vol_3[ii]), 2),
                "ma_current": round(float(mas[ii]), 2),
                "ma_lookback": round(float(ma_lookback[ii]), 2),
                "ma_decline_ratio": round(float(ma_lookback[ii] / mas[ii]), 2),
            },
        ))
    return results


# ---------------------------------------------------------------------------
# 单股扫描（多周期 OR）
# ---------------------------------------------------------------------------


def build_xianren_zhilu_payload(
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
    clock_frame = tf_data[clock_tf]
    latest_ts = clock_frame.iloc[-1]["ts"]

    # 展示窗口 = 最粗命中周期的回看区间
    lookback_bars = int(all_params[clock_tf]["lookback_bars"])
    window_start_idx = max(len(clock_frame) - lookback_bars - int(all_params[clock_tf]["ma_period"]), 0)
    window_start_ts = clock_frame.iloc[window_start_idx]["ts"]
    window_end_ts = latest_ts

    # 构建信号标签
    label_parts = [f"{_TF_LABEL.get(tf, tf)}{STRATEGY_LABEL}" for tf in hit_tfs]
    signal_label = " + ".join(label_parts)

    return {
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "chart_interval_start_ts": window_start_ts,
        "chart_interval_end_ts": window_end_ts,
        "anchor_day_ts": latest_ts,
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
    """扫描单只股票：任一启用周期命中即产生信号（OR 逻辑）。"""
    stats = {"candidate": 0, "kept": 0}
    total_bars = sum(len(df) for df in tf_data.values())

    hit_tfs: list[str] = []
    per_tf_detail: dict[str, dict[str, Any]] = {}

    for tf in enabled_tfs:
        frame = tf_data.get(tf, pd.DataFrame())
        if frame.empty:
            per_tf_detail[tf] = {"passed": False, "reason": "无数据"}
            continue
        params = all_params[tf]
        result = detect_xianren_zhilu(frame, params)
        detail = {"passed": result.matched, **result.metrics}
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

    # 命中 → 生成信号（clock_tf 取最粗命中周期）
    stats["kept"] = 1
    payload = build_xianren_zhilu_payload(
        hit_tfs=hit_tfs,
        per_tf_detail=per_tf_detail,
        tf_data=tf_data,
        all_params=all_params,
    )
    clock_tf = coarsest_tf(hit_tfs)

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


def run_xianren_zhilu_v1_specialized(
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
    """仙人指路 specialized 主入口。"""
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
        # 需要 lookback_bars + ma_period 根线的数据
        bars_needed = int(params["lookback_bars"]) + int(params["ma_period"]) + 10
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

        if not frame.empty:
            frame = prepare_xianren_zhilu_features(frame, params)
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

    # 完全复刻 detect_xianren_zhilu() 全部条件，pandas 向量化替代逐股循环 (OR)
    _batch_hits: set[str] = set()
    _batch_hits_per_tf: dict[str, set[str]] = {}
    for tf in enabled_tfs:
        frame = tf_frames[tf]
        if frame.empty:
            _batch_hits_per_tf[tf] = set()
            continue
        _p = all_tf_params[tf]
        _sr = float(_p["shadow_ratio"])
        _lsp = float(_p["lower_shadow_pct"])
        _mgp = float(_p["max_gain_pct"])
        _vm = float(_p["volume_multiplier"])
        _lb = int(_p["lookback_bars"])
        _mp = int(_p["ma_period"])
        _pdr = float(_p["price_decline_ratio"])
        _min_bars = _lb + _mp

        _g = frame.groupby("code", sort=False)
        # groupby.shift 内部 Cython 实现，真正向量化
        _sv1 = _g["volume"].shift(1)
        _sv2 = _g["volume"].shift(2)
        _sv3 = _g["volume"].shift(3)
        _f = frame.copy()
        _f["_avg3"] = (_sv1 + _sv2 + _sv3) / 3.0
        _f["_ma_lb"] = _g["ma"].shift(_lb)
        _f["_n"] = _g.cumcount() + 1

        _last = _f.groupby("code", sort=False).last()
        _o, _c, _h = _last["open"], _last["close"], _last["high"]
        _l, _v, _ma = _last["low"], _last["volume"], _last["ma"]
        _shadow = _h - _c
        _max_gain = _h - _o
        _lower_sh = (_o - _l).clip(lower=0)
        _safe_mg = _max_gain.where(_max_gain > 0, np.nan)
        _safe_o = _o.where(_o > 0, np.nan)

        _ok = (
            (_last["_n"] >= _min_bars)
            & (_c >= _o) & (_h > _o) & (_shadow > 0) & (_max_gain > 0)
            & _ma.notna()
            & (_shadow / _safe_mg * 100 >= _sr)
            & (_lower_sh < _shadow * (_lsp / 100))
            & (_max_gain / _safe_o * 100 >= _mgp)
            & _last["_avg3"].notna() & (_last["_avg3"] > 0)
            & (_v >= _last["_avg3"] * _vm)
            & (_ma > 0) & _last["_ma_lb"].notna()
            & (_last["_ma_lb"] >= _ma * _pdr)
        )
        _tf_hits = set(_last.index[_ok])
        _batch_hits_per_tf[tf] = _tf_hits
        _batch_hits |= _tf_hits

    result_map: dict[str, StockScanResult] = {}
    total_candidates = len(codes)
    total_kept = 0

    for code in codes:
        if code not in _batch_hits:
            continue

        code_tf_data = per_code_data.get(code, {})
        for tf in enabled_tfs:
            if tf not in code_tf_data:
                code_tf_data[tf] = pd.DataFrame()

        try:
            # 向量化已确认命中，直接从数据收集 metrics 构建 payload
            total_bars = sum(len(df) for df in code_tf_data.values())
            hit_tfs: list[str] = [
                tf for tf in enabled_tfs
                if code in _batch_hits_per_tf.get(tf, set())
            ]
            per_tf_detail: dict[str, dict[str, Any]] = {}

            for tf in enabled_tfs:
                frame = code_tf_data.get(tf, pd.DataFrame())
                if frame.empty or tf not in hit_tfs:
                    per_tf_detail[tf] = {"passed": tf in hit_tfs, "reason": "无数据" if frame.empty else ""}
                    continue
                params = all_tf_params[tf]
                i = len(frame) - 1
                cur = frame.iloc[i]
                o, c, h, l_p = float(cur["open"]), float(cur["close"]), float(cur["high"]), float(cur["low"])
                vol = float(cur["volume"])
                max_gain = h - o
                shadow = h - c
                lower_sh = max(o - l_p, 0.0)
                avg3 = float(frame.iloc[i - 3:i]["volume"].mean()) if i >= 3 else 1.0
                lb_idx = i - int(params["lookback_bars"])
                ma_cur = float(cur["ma"])
                ma_lb = float(frame.iloc[lb_idx]["ma"]) if 0 <= lb_idx < len(frame) else ma_cur

                per_tf_detail[tf] = {
                    "passed": True,
                    "shadow_ratio": round(shadow / max_gain * 100, 2) if max_gain > 0 else 0.0,
                    "lower_shadow_pct": round(lower_sh / shadow * 100, 2) if shadow > 0 else 0.0,
                    "max_gain_pct": round(max_gain / o * 100, 2) if o > 0 else 0.0,
                    "volume_ratio": round(vol / avg3, 2) if avg3 > 0 else 0.0,
                    "ma_current": round(ma_cur, 2),
                    "ma_lookback": round(ma_lb, 2),
                    "ma_decline_ratio": round(ma_lb / ma_cur, 2) if ma_cur > 0 else 0.0,
                }

            payload = build_xianren_zhilu_payload(
                hit_tfs=hit_tfs,
                per_tf_detail=per_tf_detail,
                tf_data=code_tf_data,
                all_params={tf: all_tf_params[tf] for tf in enabled_tfs},
            )
            clock_tf = coarsest_tf(hit_tfs)

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
    return {"params": _normalize_tf_params(group_params, section_key)}


# ---------------------------------------------------------------------------
# GPU 批量检测 (detect_batch hook)
# ---------------------------------------------------------------------------


def detect_xianren_zhilu_batch(
    columns: dict,
    boundaries: np.ndarray,
    det_kwargs: dict[str, Any],
):
    """GPU 批量检测：对拼接后的多股票数据一次性执行仙人指路布尔运算。

    columns: {col_name: cupy.ndarray} — flat GPU 数组
    boundaries: numpy int64 array, shape=(n_stocks+1,)
    det_kwargs: {"params": {...}}
    返回: cupy boolean mask, shape=(total_rows,)
    """
    from strategies.engine_commons import (
        gpu_segmented_rolling_mean,
        gpu_segmented_shift,
    )
    import cupy as cp

    params = det_kwargs["params"]
    shadow_ratio_threshold = float(params["shadow_ratio"])
    lower_shadow_pct = float(params["lower_shadow_pct"])
    max_gain_pct = float(params["max_gain_pct"])
    volume_multiplier = float(params["volume_multiplier"])
    lookback_bars = int(params["lookback_bars"])
    ma_period = int(params["ma_period"])
    price_decline_ratio = float(params["price_decline_ratio"])
    min_bars = lookback_bars + ma_period

    opens = columns["open"]
    closes = columns["close"]
    highs = columns["high"]
    lows = columns["low"]
    volumes = columns["volume"]
    mas = columns["ma"]
    n = len(opens)

    # 条件1：close >= open 且有上影线
    shadow = highs - closes
    max_gain = highs - opens
    is_bull = closes >= opens
    has_shadow = shadow > 0
    has_gain = max_gain > 0

    # 条件2：上影线占最大涨幅百分比
    safe_gain = cp.where(max_gain > 0, max_gain, 1.0)
    cond_shadow = (shadow / safe_gain * 100) >= shadow_ratio_threshold

    # 条件2a：下影线 < 上影线 × lower_shadow_pct%
    lower_shadow = cp.maximum(opens - lows, 0.0)
    safe_shadow = cp.where(shadow > 0, shadow, 1.0)
    cond_lower = lower_shadow < safe_shadow * (lower_shadow_pct / 100)

    # 条件2b：当日最大涨幅 >= max_gain_pct%
    safe_opens = cp.where(opens > 0, opens, 1.0)
    cond_gain_pct = (max_gain / safe_opens * 100) >= max_gain_pct

    # 条件3：放量（segmented rolling mean + shift）
    avg_vol_3 = gpu_segmented_shift(
        gpu_segmented_rolling_mean(volumes, boundaries, 3),
        boundaries, 1,
    )
    cond_volume = volumes >= avg_vol_3 * volume_multiplier

    # 条件4：前期均线下降（segmented shift）
    ma_lookback = gpu_segmented_shift(mas, boundaries, lookback_bars)
    cond_ma = (~cp.isnan(mas) & ~cp.isnan(ma_lookback)
               & (mas > 0)
               & (ma_lookback >= mas * price_decline_ratio))

    # 数据充足性：per-segment 前 min_bars-1 个位置 mask 掉
    cond_data = cp.ones(n, dtype=bool)
    for i in range(len(boundaries) - 1):
        s = int(boundaries[i])
        e = min(s + max(min_bars - 1, 3), int(boundaries[i + 1]))
        if s < e:
            cond_data[s:e] = False

    return (is_bull & has_shadow & has_gain & cond_shadow & cond_lower
            & cond_gain_pct & cond_volume & cond_ma & cond_data)


detect_xianren_zhilu_batch._batch_columns = ["open", "high", "low", "close", "volume", "ma"]


def prepare_xianren_zhilu_features_batch(
    gpu_raw: dict,
    boundaries: np.ndarray,
    params: dict[str, Any],
) -> dict:
    """GPU 原生特征计算：直接在 GPU 上计算 MA。"""
    from strategies.engine_commons import gpu_segmented_rolling_mean

    ma_period = int(params["ma_period"])
    ma = gpu_segmented_rolling_mean(gpu_raw["close"], boundaries, ma_period)

    result = dict(gpu_raw)
    result["ma"] = ma
    return result


BACKTEST_HOOKS = {
    "detect": detect_xianren_zhilu,
    "detect_vectorized": detect_xianren_zhilu_vectorized,
    "detect_batch": detect_xianren_zhilu_batch,
    "prepare": prepare_xianren_zhilu_features,
    "prepare_batch": prepare_xianren_zhilu_features_batch,
    "prepare_key": lambda p: {"ma_period": p["ma_period"]},
    "normalize_params": _normalize_for_backtest,
    "tf_sections": {
        "weekly": {"tf_key": "w", "table": "klines_w"},
        "daily": {"tf_key": "d", "table": "klines_d"},
    },
    "tf_logic": "or",
}
