"""
`xianren_zhilu_v1` specialized engine。

职责：
1. 按周线/日线两个可选周期，批量加载 K 线数据并在每个启用周期上独立检测仙人指路形态。
2. 仙人指路定义：
   a. 最新K线收上影线：close > open 且 (high − close) / (close − open) ≥ shadow_ratio。
   b. 当K线放量：成交量 ≥ 之前3根K线平均成交量 × volume_multiplier。
   c. 前期均线下降：lookback_bars 根前的 MA 值 ≥ 当前 MA 值 × price_decline_ratio。
3. 多周期之间为并集关系（OR）：任一启用周期检测到仙人指路即产生信号。
4. 同一股票多周期命中时合并为 1 条信号，clock_tf 取最粗命中周期。
"""

from __future__ import annotations

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

STRATEGY_LABEL = "仙人指路"

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
        "shadow_ratio": as_float(raw.get("shadow_ratio"), 0.6, minimum=0.0, maximum=10.0),
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


def _prepare_features(frame: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """计算 MA 均线列。"""
    if frame.empty:
        return frame

    ma_period = int(params["ma_period"])
    prepared: list[pd.DataFrame] = []
    for _, code_frame in frame.groupby("code", sort=False):
        part = code_frame.sort_values("ts").copy()
        part["ma"] = part["close"].rolling(ma_period, min_periods=ma_period).mean()
        prepared.append(part)
    return pd.concat(prepared, axis=0, ignore_index=True)


# ---------------------------------------------------------------------------
# 单股单周期检测
# ---------------------------------------------------------------------------


def _check_xianren_zhilu(
    code_frame: pd.DataFrame,
    params: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    """检测最新一根K线是否满足仙人指路形态。

    返回 (是否命中, 检测明细 dict)。
    """
    detail: dict[str, Any] = {"passed": False}

    shadow_ratio = float(params["shadow_ratio"])
    volume_multiplier = float(params["volume_multiplier"])
    lookback_bars = int(params["lookback_bars"])
    ma_period = int(params["ma_period"])

    # 数据量检查：至少需要 lookback_bars + ma_period 根线
    min_bars = lookback_bars + ma_period
    if len(code_frame) < min_bars:
        detail["reason"] = f"数据不足，需 {min_bars} 根，仅有 {len(code_frame)} 根"
        return False, detail

    i = len(code_frame) - 1
    current = code_frame.iloc[i]
    current_ma = current["ma"]

    if pd.isna(current_ma):
        detail["reason"] = "当前MA值无效"
        return False, detail

    open_price = float(current["open"])
    close_price = float(current["close"])
    high_price = float(current["high"])
    current_volume = float(current["volume"])

    # 条件1：阳线（close > open）且 high > close
    body = close_price - open_price
    if body <= 0:
        detail["reason"] = "非阳线"
        return False, detail

    shadow = high_price - close_price
    if shadow <= 0:
        detail["reason"] = "无上影线"
        return False, detail

    # 上影线与实体比值
    shadow_ratio_actual = shadow / body
    detail["shadow_ratio"] = round(shadow_ratio_actual, 3)
    if shadow_ratio_actual < shadow_ratio:
        detail["reason"] = f"上影线比值 {shadow_ratio_actual:.3f} < 阈值 {shadow_ratio}"
        return False, detail

    # 条件2：放量（当前成交量 ≥ 前3根均量 × 倍数）
    if i < 3:
        detail["reason"] = "数据不足3根计算均量"
        return False, detail

    avg_volume_3 = code_frame.iloc[i - 3:i]["volume"].mean()
    if pd.isna(avg_volume_3) or float(avg_volume_3) == 0:
        detail["reason"] = "前3根均量无效"
        return False, detail

    volume_ratio = current_volume / float(avg_volume_3)
    detail["volume_ratio"] = round(volume_ratio, 2)
    if current_volume < float(avg_volume_3) * volume_multiplier:
        detail["reason"] = f"量比 {volume_ratio:.2f} < 阈值 {volume_multiplier}"
        return False, detail

    # 条件3：前期均线下降（lookback_bars 根前的 MA ≥ 当前 MA × price_decline_ratio）
    lookback_idx = i - lookback_bars
    if lookback_idx < 0:
        detail["reason"] = "回看索引越界"
        return False, detail

    ma_lookback = code_frame.iloc[lookback_idx]["ma"]
    if pd.isna(ma_lookback):
        detail["reason"] = "回看MA值无效"
        return False, detail

    current_ma_val = float(current_ma)
    ma_lookback_val = float(ma_lookback)
    price_decline_ratio = float(params["price_decline_ratio"])

    if current_ma_val > 0:
        ma_decline_ratio = ma_lookback_val / current_ma_val
    else:
        detail["reason"] = "当前MA为零"
        return False, detail

    detail["ma_current"] = round(current_ma_val, 2)
    detail["ma_lookback"] = round(ma_lookback_val, 2)
    detail["ma_decline_ratio"] = round(ma_decline_ratio, 2)

    if ma_lookback_val < current_ma_val * price_decline_ratio:
        detail["reason"] = f"MA下降比 {ma_decline_ratio:.2f} < 阈值 {price_decline_ratio}"
        return False, detail

    detail["passed"] = True
    return True, detail


# ---------------------------------------------------------------------------
# 单股扫描（多周期 OR）
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
        passed, detail = _check_xianren_zhilu(frame, params)
        per_tf_detail[tf] = detail
        if passed:
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

    payload: dict[str, Any] = {
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "chart_interval_start_ts": window_start_ts,
        "chart_interval_end_ts": window_end_ts,
        "anchor_ts": latest_ts,
        "hit_timeframes": hit_tfs,
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
