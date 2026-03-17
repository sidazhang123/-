"""
`xianren_zhilu_v1` specialized engine.

仙人指路形态策略：
1. 最新日线收上影线：close > open 且 (high - close) / (high - open) > 阈值
2. 指路当日放量：该日成交量 > 之前3天平均成交量 * 倍数
3. 前N天股价下降：N天前的MA3 > 当日MA3 * 比例
"""

from __future__ import annotations

import time
from datetime import date, datetime
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
    connect_source_readonly,
)

STRATEGY_LABEL = "仙人指路"

DEFAULT_DAILY_PARAMS: dict[str, Any] = {
    "shadow_ratio": 0.6,
    "volume_multiplier": 1.3,
    "lookback_days": 5,
    "price_decline_ratio": 1.2,
}

DEFAULT_EXECUTION_PARAMS: dict[str, Any] = {
    "fallback_to_backtrader": False,
}


def _normalize_daily_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """从 group_params 中提取并校验日线参数。"""
    raw = as_dict(group_params.get("daily"))
    return {
        "shadow_ratio": as_float(
            raw.get("shadow_ratio"),
            DEFAULT_DAILY_PARAMS["shadow_ratio"],
            minimum=0.0,
            maximum=1.0,
        ),
        "volume_multiplier": as_float(
            raw.get("volume_multiplier"),
            DEFAULT_DAILY_PARAMS["volume_multiplier"],
            minimum=1.0,
        ),
        "lookback_days": as_int(
            raw.get("lookback_days"),
            DEFAULT_DAILY_PARAMS["lookback_days"],
            minimum=1,
            maximum=30,
        ),
        "price_decline_ratio": as_float(
            raw.get("price_decline_ratio"),
            DEFAULT_DAILY_PARAMS["price_decline_ratio"],
            minimum=1.0,
        ),
    }


def _normalize_execution_params(group_params: dict[str, Any]) -> dict[str, Any]:
    raw = as_dict(group_params.get("execution"))
    return {
        "fallback_to_backtrader": as_bool(
            raw.get("fallback_to_backtrader"),
            DEFAULT_EXECUTION_PARAMS["fallback_to_backtrader"],
        ),
    }


def _load_daily_bars(
    *,
    source_db_path: Path,
    codes: list[str],
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    """加载日线 OHLCV 数据。"""
    if not codes:
        return pd.DataFrame()

    with connect_source_readonly(source_db_path) as con:
        con.execute("create temp table _tmp_codes (code varchar)")
        con.execute("insert into _tmp_codes(code) select unnest($1)", [codes])
        frame = con.execute(
            """
            select
                t.code,
                t.datetime as day_ts,
                t.open,
                t.high,
                t.low,
                t.close,
                t.volume
            from klines_d t
            join _tmp_codes c on c.code = t.code
            where t.datetime >= ? and t.datetime <= ?
            order by t.code, t.datetime
            """,
            [datetime.combine(start_day, datetime.min.time()),
             datetime.combine(end_day, datetime.max.time())],
        ).fetchdf()
    return frame


def _calculate_ma5(close_series: pd.Series) -> pd.Series:
    """计算 5 日简单移动平均。"""
    return close_series.rolling(window=5, min_periods=5).mean()


def _scan_one_code(
    *,
    code: str,
    name: str,
    daily_frame: pd.DataFrame,
    daily_params: dict[str, Any],
    strategy_group_id: str,
    strategy_name: str,
) -> tuple[StockScanResult, dict[str, int]]:
    """扫描单只股票，返回 (扫描结果, 统计信息)。

    只检查最新一天的K线（时间窗口最后一天）是否满足仙人指路形态。
    """
    stats = {
        "processed_bars": len(daily_frame),
        "candidate_count": 0,
        "kept_count": 0,
    }

    signals: list[dict[str, Any]] = []

    shadow_ratio = daily_params["shadow_ratio"]
    volume_multiplier = daily_params["volume_multiplier"]
    lookback_days = daily_params["lookback_days"]
    price_decline_ratio = daily_params["price_decline_ratio"]

    if len(daily_frame) < lookback_days + 5:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=stats["processed_bars"],
            signal_count=len(signals),
            signals=signals,
        ), stats

    df = daily_frame.reset_index(drop=True).sort_values("day_ts")
    df["ma5"] = _calculate_ma5(df["close"])

    i = len(df) - 1
    current = df.iloc[i]
    current_dt = current["day_ts"]
    current_ma5 = current["ma5"]

    if pd.isna(current_ma5):
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=stats["processed_bars"],
            signal_count=len(signals),
            signals=signals,
        ), stats

    open_price = current["open"]
    close_price = current["close"]
    high_price = current["high"]
    current_volume = current["volume"]

    if high_price == open_price:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=stats["processed_bars"],
            signal_count=len(signals),
            signals=signals,
        ), stats

    shadow = high_price - close_price
    body = close_price - open_price

    if body <= 0:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=stats["processed_bars"],
            signal_count=len(signals),
            signals=signals,
        ), stats

    shadow_ratio_current = shadow / body
    if shadow_ratio_current < shadow_ratio:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=stats["processed_bars"],
            signal_count=len(signals),
            signals=signals,
        ), stats

    avg_volume_3d = df.iloc[i-3:i]["volume"].mean()
    if pd.isna(avg_volume_3d) or avg_volume_3d == 0:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=stats["processed_bars"],
            signal_count=len(signals),
            signals=signals,
        ), stats

    if current_volume < avg_volume_3d * volume_multiplier:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=stats["processed_bars"],
            signal_count=len(signals),
            signals=signals,
        ), stats

    ma5_lookback_idx = i - lookback_days
    if ma5_lookback_idx < 5:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=stats["processed_bars"],
            signal_count=len(signals),
            signals=signals,
        ), stats

    ma5_lookback = df.iloc[ma5_lookback_idx]["ma5"]
    if pd.isna(ma5_lookback):
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=stats["processed_bars"],
            signal_count=len(signals),
            signals=signals,
        ), stats

    if ma5_lookback < current_ma5 * price_decline_ratio:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=stats["processed_bars"],
            signal_count=len(signals),
            signals=signals,
        ), stats

    stats["candidate_count"] += 1

    window_start = df.iloc[ma5_lookback_idx]["day_ts"]
    window_end = current_dt

    signal = build_signal_dict(
        code=code,
        name=name,
        signal_dt=current_dt,
        clock_tf="d",
        strategy_group_id=strategy_group_id,
        strategy_name=strategy_name,
        signal_label=STRATEGY_LABEL,
        payload={
            "window_start_ts": window_start,
            "window_end_ts": window_end,
            "chart_interval_start_ts": window_start,
            "chart_interval_end_ts": window_end,
            "anchor_day_ts": current_dt,
            "shadow_ratio": round(shadow_ratio_current, 3),
            "volume_ratio": round(current_volume / avg_volume_3d, 2) if avg_volume_3d > 0 else None,
            "ma5_current": round(current_ma5, 2),
            "ma5_lookback": round(ma5_lookback, 2),
            "ma5_decline_ratio": round(ma5_lookback / current_ma5, 2) if current_ma5 > 0 else None,
        },
    )
    signals.append(signal)
    stats["kept_count"] += 1

    return StockScanResult(
        code=code,
        name=name,
        processed_bars=stats["processed_bars"],
        signal_count=len(signals),
        signals=signals,
    ), stats


def run_xianren_zhilu_v1_specialized(
    *,
    source_db_path: Path,
    start_ts: datetime,
    end_ts: datetime,
    codes: list[str],
    code_to_name: dict[str, str],
    group_params: dict[str, Any],
    strategy_group_id: str,
    strategy_name: str,
    cache_scope: str,
    cache_dir: Path | None = None,
) -> tuple[dict[str, StockScanResult], dict[str, Any]]:
    """仙人指路 specialized 入口。"""
    _ = (cache_scope, cache_dir)

    daily_params = _normalize_daily_params(group_params)
    execution_params = _normalize_execution_params(group_params)

    empty_metrics: dict[str, Any] = {
        "daily_phase_sec": 0.0,
        "scan_phase_sec": 0.0,
        "total_daily_rows": 0,
        "total_codes": len(codes),
        "candidate_count": 0,
        "kept_count": 0,
        "codes_with_signal": 0,
        "stock_errors": 0,
        "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
    }

    if not codes:
        return {}, empty_metrics

    start_day = start_ts.date()
    end_day = end_ts.date()

    daily_phase_start = time.perf_counter()
    daily_raw = _load_daily_bars(
        source_db_path=Path(source_db_path),
        codes=codes,
        start_day=start_day,
        end_day=end_day,
    )
    daily_phase_sec = time.perf_counter() - daily_phase_start

    if daily_raw.empty:
        empty_metrics["daily_phase_sec"] = round(daily_phase_sec, 4)
        return {}, empty_metrics

    per_code = {
        code: frame.copy()
        for code, frame in daily_raw.groupby("code", sort=False)
    }

    result_map: dict[str, StockScanResult] = {}
    total_daily_rows = 0
    candidate_count = 0
    kept_count = 0
    codes_with_signal = 0
    stock_errors = 0

    scan_phase_start = time.perf_counter()
    for code in codes:
        name = code_to_name.get(code, "")
        code_frame = per_code.get(code)
        if code_frame is None or code_frame.empty:
            continue

        try:
            scan_result, stats = _scan_one_code(
                code=code,
                name=name,
                daily_frame=code_frame,
                daily_params=daily_params,
                strategy_group_id=strategy_group_id,
                strategy_name=strategy_name,
            )
        except Exception as exc:
            stock_errors += 1
            result_map[code] = StockScanResult(
                code=code,
                name=name,
                processed_bars=0,
                signal_count=0,
                signals=[],
                error_message=f"{type(exc).__name__}: {exc}",
            )
            continue

        total_daily_rows += stats["processed_bars"]
        candidate_count += stats["candidate_count"]
        kept_count += stats["kept_count"]

        if scan_result.signal_count > 0:
            codes_with_signal += 1
            result_map[code] = scan_result

    scan_phase_sec = time.perf_counter() - scan_phase_start

    metrics: dict[str, Any] = {
        "daily_phase_sec": round(daily_phase_sec, 4),
        "scan_phase_sec": round(scan_phase_sec, 4),
        "total_daily_rows": total_daily_rows,
        "total_codes": len(codes),
        "candidate_count": candidate_count,
        "kept_count": kept_count,
        "codes_with_signal": codes_with_signal,
        "stock_errors": stock_errors,
        "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
    }

    return result_map, metrics
