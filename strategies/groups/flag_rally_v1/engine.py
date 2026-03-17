"""
`flag_rally_v1` specialized engine.

旗形上涨策略：
1. 日线：ma10上涨 + RSI14 < 50
2. 15分钟：最近15个交易日内出现2次及以上旗形形态（区间不重叠）
   - 连续2-5根阳线，合计涨幅3-8%
   - 后续3+根连续阴线，首根阴线open与最后一根阳线close涨跌幅差<2%
   - 之后4根K线合计振幅<2%
"""

from __future__ import annotations

import time
from datetime import date, datetime
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
    connect_source_readonly,
)

STRATEGY_LABEL = "旗形上涨 v1"

DEFAULT_DAILY_PARAMS = {
    "lookback_days": 15,
    "ma_period": 10,
    "rsi_period": 14,
    "rsi_threshold": 50,
}

DEFAULT_INTRADAY_PARAMS = {
    "min_bull_count": 2,
    "max_bull_count": 5,
    "min_bull_gain_pct": 0.03,
    "max_bull_gain_pct": 0.08,
    "min_bear_count": 3,
    "max_bear_open_close_diff_pct": 0.02,
    "consolidation_bars": 4,
    "consolidation_amplitude_pct": 0.02,
}

DEFAULT_EXECUTION_PARAMS = {
    "fallback_to_backtrader": False,
}


def _normalize_daily_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化日线参数"""
    raw = as_dict(group_params.get("daily"))
    lookback_days = as_int(raw.get("lookback_days"), DEFAULT_DAILY_PARAMS["lookback_days"], minimum=5, maximum=60)
    ma_period = as_int(raw.get("ma_period"), DEFAULT_DAILY_PARAMS["ma_period"], minimum=5, maximum=120)
    rsi_period = as_int(raw.get("rsi_period"), DEFAULT_DAILY_PARAMS["rsi_period"], minimum=5, maximum=60)
    return {
        "lookback_days": lookback_days,
        "ma_period": ma_period,
        "rsi_period": rsi_period,
        "rsi_threshold": as_int(raw.get("rsi_threshold"), DEFAULT_DAILY_PARAMS["rsi_threshold"], minimum=0, maximum=100),
    }


def _normalize_intraday_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化15分钟参数"""
    raw = as_dict(group_params.get("intraday"))
    min_bull = as_int(raw.get("min_bull_count"), DEFAULT_INTRADAY_PARAMS["min_bull_count"], minimum=2, maximum=5)
    max_bull = as_int(raw.get("max_bull_count"), DEFAULT_INTRADAY_PARAMS["max_bull_count"], minimum=min_bull, maximum=10)
    consolidation_bars = as_int(raw.get("consolidation_bars"), DEFAULT_INTRADAY_PARAMS["consolidation_bars"], minimum=2, maximum=20)
    return {
        "min_bull_count": min_bull,
        "max_bull_count": max_bull,
        "min_bull_gain_pct": as_float(raw.get("min_bull_gain_pct"), DEFAULT_INTRADAY_PARAMS["min_bull_gain_pct"], minimum=0.0),
        "max_bull_gain_pct": as_float(raw.get("max_bull_gain_pct"), DEFAULT_INTRADAY_PARAMS["max_bull_gain_pct"], minimum=0.0),
        "min_bear_count": as_int(raw.get("min_bear_count"), DEFAULT_INTRADAY_PARAMS["min_bear_count"], minimum=1, maximum=20),
        "max_bear_open_close_diff_pct": as_float(raw.get("max_bear_open_close_diff_pct"), DEFAULT_INTRADAY_PARAMS["max_bear_open_close_diff_pct"], minimum=0.0),
        "consolidation_bars": consolidation_bars,
        "consolidation_amplitude_pct": as_float(raw.get("consolidation_amplitude_pct"), DEFAULT_INTRADAY_PARAMS["consolidation_amplitude_pct"], minimum=0.0),
    }


def _normalize_execution_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化执行参数"""
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
    """加载日线数据"""
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


def _load_15m_bars(
    *,
    source_db_path: Path,
    codes: list[str],
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    """加载15分钟K线数据"""
    if not codes:
        return pd.DataFrame()

    with connect_source_readonly(source_db_path) as con:
        con.execute("create temp table _tmp_codes (code varchar)")
        con.execute("insert into _tmp_codes(code) select unnest($1)", [codes])
        frame = con.execute(
            """
            select
                t.code,
                t.datetime as ts,
                t.open,
                t.high,
                t.low,
                t.close,
                t.volume
            from klines_15 t
            join _tmp_codes c on c.code = t.code
            where t.datetime >= ? and t.datetime <= ?
            order by t.code, t.datetime
            """,
            [datetime.combine(start_day, datetime.min.time()),
             datetime.combine(end_day, datetime.max.time())],
        ).fetchdf()
    return frame


def _prepare_daily_features(frame: pd.DataFrame, ma_period: int, rsi_period: int) -> pd.DataFrame:
    """计算日线MA和RSI指标"""
    if frame.empty:
        return frame

    prepared: list[pd.DataFrame] = []
    for _, code_frame in frame.groupby("code", sort=False):
        part = code_frame.sort_values("day_ts").copy()
        part["prev_close"] = part["close"].shift(1)
        part["ma"] = part["close"].rolling(ma_period, min_periods=ma_period).mean()
        delta = part["close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.rolling(rsi_period, min_periods=rsi_period).mean()
        avg_loss = loss.rolling(rsi_period, min_periods=rsi_period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        part["rsi"] = 100 - (100 / (1 + rs))
        prepared.append(part)
    return pd.concat(prepared, axis=0, ignore_index=True)


def _check_daily_conditions(daily_frame: pd.DataFrame, daily_params: dict[str, Any]) -> bool:
    """检查日线条件：ma10上涨 + RSI14 < threshold"""
    if daily_frame.empty:
        return False

    ma_period = int(daily_params["ma_period"])
    rsi_period = int(daily_params["rsi_period"])
    rsi_threshold = int(daily_params["rsi_threshold"])

    valid_days = daily_frame[daily_frame["ma"].notna() & daily_frame["rsi"].notna()]
    if len(valid_days) < ma_period + rsi_period:
        return False

    valid_days = valid_days.sort_values("day_ts")
    latest = valid_days.iloc[-1]
    old = valid_days.iloc[-(ma_period + 1)] if len(valid_days) > ma_period else valid_days.iloc[0]

    ma_up = latest["ma"] > old["ma"]
    rsi_low = latest["rsi"] < rsi_threshold

    return ma_up and rsi_low


def _is_finite_number(value: Any) -> bool:
    """检查值是否为有限数字"""
    try:
        return bool(np.isfinite(float(value)))
    except Exception:
        return False


def _scan_one_code(
    *,
    code: str,
    name: str,
    daily_frame: pd.DataFrame,
    m15_frame: pd.DataFrame,
    daily_params: dict[str, Any],
    intraday_params: dict[str, Any],
    strategy_group_id: str,
    strategy_name: str,
) -> tuple[StockScanResult, dict[str, int]]:
    """扫描单只股票"""
    stats = {
        "processed_daily_bars": 0,
        "processed_m15_bars": 0,
        "flag_pattern_count": 0,
        "signal_count": 0,
    }

    if daily_frame.empty or m15_frame.empty:
        return StockScanResult(
            code=code, name=name,
            processed_bars=0,
            signal_count=0, signals=[],
        ), stats

    stats["processed_daily_bars"] = len(daily_frame)
    stats["processed_m15_bars"] = len(m15_frame)

    if not _check_daily_conditions(daily_frame, daily_params):
        return StockScanResult(
            code=code, name=name,
            processed_bars=stats["processed_daily_bars"],
            signal_count=0, signals=[],
        ), stats

    lookback_days = int(daily_params["lookback_days"])
    daily_dates = sorted(daily_frame["day_ts"].dt.date.unique())
    if len(daily_dates) < lookback_days:
        return StockScanResult(
            code=code, name=name,
            processed_bars=stats["processed_daily_bars"],
            signal_count=0, signals=[],
        ), stats

    target_dates = daily_dates[-lookback_days:]

    m15_frame = m15_frame.copy()
    m15_frame["trade_date"] = m15_frame["ts"].dt.date
    m15_frame = m15_frame[m15_frame["trade_date"].isin(target_dates)]
    if m15_frame.empty:
        return StockScanResult(
            code=code, name=name,
            processed_bars=stats["processed_daily_bars"],
            signal_count=0, signals=[],
        ), stats

    m15_frame = m15_frame.sort_values("ts").reset_index(drop=True)
    m15_frame["prev_close"] = m15_frame["close"].shift(1)

    m15_bars = []
    for idx, row in m15_frame.iterrows():
        if not _is_finite_number(row.get("prev_close")) or row["prev_close"] <= 0:
            continue
        if not _is_finite_number(row.get("close")) or row["close"] <= 0:
            continue
        m15_bars.append({
            "idx": idx,
            "ts": row["ts"],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "prev_close": float(row["prev_close"]),
            "is_bull": bool(row["close"] > row["open"]),
            "trade_date": row["trade_date"],
        })

    if len(m15_bars) < 15:
        return StockScanResult(
            code=code, name=name,
            processed_bars=stats["processed_daily_bars"],
            signal_count=0, signals=[],
        ), stats

    min_bull = intraday_params["min_bull_count"]
    max_bull = intraday_params["max_bull_count"]
    min_bull_gain = float(intraday_params["min_bull_gain_pct"])
    max_bull_gain = float(intraday_params["max_bull_gain_pct"])
    min_bear = intraday_params["min_bear_count"]
    max_bear_diff = float(intraday_params["max_bear_open_close_diff_pct"])
    consolidation_bars = int(intraday_params["consolidation_bars"])
    consolidation_amp = float(intraday_params["consolidation_amplitude_pct"])

    flag_patterns: list[dict[str, Any]] = []

    for i in range(len(m15_bars)):
        bar_i = m15_bars[i]
        if not bar_i["is_bull"]:
            continue

        pre_close = bar_i["prev_close"]
        if not _is_finite_number(pre_close) or pre_close <= 0:
            continue

        bull_count = 0
        for j in range(i, min(i + max_bull, len(m15_bars))):
            if m15_bars[j]["is_bull"]:
                bull_count += 1
            else:
                break

        if bull_count < min_bull:
            continue

        first_bull = m15_bars[i]
        last_bull = m15_bars[i + bull_count - 1]
        bull_total_gain = (last_bull["close"] - pre_close) / pre_close

        if bull_total_gain < min_bull_gain or bull_total_gain > max_bull_gain:
            continue

        bear_start = i + bull_count
        if bear_start >= len(m15_bars):
            continue

        bear_count = 0
        for j in range(bear_start, len(m15_bars)):
            if not m15_bars[j]["is_bull"]:
                bear_count += 1
            else:
                break

        if bear_count < min_bear:
            continue

        first_bear = m15_bars[bear_start]
        last_bull_close = last_bull["close"]
        bear_open = first_bear["open"]
        open_diff_pct = abs((bear_open / last_bull_close) - 1)

        if open_diff_pct >= max_bear_diff:
            continue

        consolidation_start = bear_start + bear_count
        if consolidation_start + consolidation_bars > len(m15_bars):
            continue

        consolidation_window = m15_bars[consolidation_start:consolidation_start + consolidation_bars]
        cons_high = max(b["high"] for b in consolidation_window)
        cons_low = min(b["low"] for b in consolidation_window)
        cons_amplitude = (cons_high - cons_low) / cons_low if cons_low > 0 else float("inf")

        if cons_amplitude >= consolidation_amp:
            continue

        flag_patterns.append({
            "pattern_start_idx": i,
            "pattern_end_idx": consolidation_start + consolidation_bars - 1,
            "bull_start_ts": first_bull["ts"],
            "bull_end_ts": last_bull["ts"],
            "consolidation_start_ts": consolidation_window[0]["ts"],
            "consolidation_end_ts": consolidation_window[-1]["ts"],
            "bull_count": bull_count,
            "bull_total_gain": bull_total_gain,
            "bear_count": bear_count,
            "consolidation_amplitude": cons_amplitude,
        })

        i = consolidation_start + consolidation_bars - 1

    if len(flag_patterns) < 2:
        return StockScanResult(
            code=code, name=name,
            processed_bars=stats["processed_daily_bars"],
            signal_count=0, signals=[],
        ), stats

    flag_patterns.sort(key=lambda x: x["pattern_start_idx"])

    non_overlapping_patterns: list[dict[str, Any]] = []
    last_end = -1
    for pattern in flag_patterns:
        if pattern["pattern_start_idx"] > last_end:
            non_overlapping_patterns.append(pattern)
            last_end = pattern["pattern_end_idx"]

    if len(non_overlapping_patterns) < 2:
        return StockScanResult(
            code=code, name=name,
            processed_bars=stats["processed_daily_bars"],
            signal_count=0, signals=[],
        ), stats

    stats["flag_pattern_count"] = len(non_overlapping_patterns)

    signals: list[dict[str, Any]] = []
    latest_pattern = non_overlapping_patterns[-1]
    latest_date = latest_pattern["consolidation_end_ts"]

    signal = build_signal_dict(
        code=code,
        name=name,
        signal_dt=latest_date,
        clock_tf="15",
        strategy_group_id=strategy_group_id,
        strategy_name=strategy_name,
        signal_label=STRATEGY_LABEL,
        payload={
            "anchor_day_ts": latest_pattern["consolidation_end_ts"],
            "window_start_ts": latest_pattern["bull_start_ts"],
            "window_end_ts": latest_pattern["consolidation_end_ts"],
            "chart_interval_start_ts": latest_pattern["bull_start_ts"],
            "chart_interval_end_ts": latest_pattern["consolidation_end_ts"],
            "pattern_count": len(non_overlapping_patterns),
            "patterns": [
                {
                    "bull_start_ts": p["bull_start_ts"],
                    "bull_end_ts": p["bull_end_ts"],
                    "bull_count": p["bull_count"],
                    "bull_total_gain": p["bull_total_gain"],
                    "bear_count": p["bear_count"],
                    "consolidation_start_ts": p["consolidation_start_ts"],
                    "consolidation_end_ts": p["consolidation_end_ts"],
                    "consolidation_amplitude": p["consolidation_amplitude"],
                }
                for p in non_overlapping_patterns
            ],
            "daily_conditions": {
                "ma_up": True,
                "rsi_below_threshold": True,
            },
        },
    )
    signals.append(signal)
    stats["signal_count"] = len(signals)

    return StockScanResult(
        code=code, name=name,
        processed_bars=stats["processed_daily_bars"],
        signal_count=len(signals), signals=signals,
    ), stats


def run_flag_rally_v1_specialized(
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
    """旗形上涨策略 specialized 入口"""
    _ = (cache_scope, cache_dir)

    daily_params = _normalize_daily_params(group_params)
    intraday_params = _normalize_intraday_params(group_params)
    execution_params = _normalize_execution_params(group_params)

    empty_metrics = {
        "daily_phase_sec": 0.0,
        "m15_phase_sec": 0.0,
        "scan_phase_sec": 0.0,
        "total_daily_rows": 0,
        "total_m15_rows": 0,
        "total_codes": len(codes),
        "codes_passed_daily": 0,
        "flag_pattern_count": 0,
        "signal_count": 0,
        "stock_errors": 0,
        "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
    }

    if not codes:
        return {}, empty_metrics

    start_day = start_ts.date()
    end_day = end_ts.date()
    lookback_days = int(daily_params["lookback_days"])
    fetch_padding = max(lookback_days + 30, 60)
    fetch_start_day = start_day - pd.Timedelta(days=fetch_padding)

    daily_phase_start = time.perf_counter()
    daily_raw = _load_daily_bars(
        source_db_path=Path(source_db_path),
        codes=codes,
        start_day=fetch_start_day,
        end_day=end_day,
    )
    daily_prepared = _prepare_daily_features(
        daily_raw,
        int(daily_params["ma_period"]),
        int(daily_params["rsi_period"]),
    )
    daily_phase_sec = time.perf_counter() - daily_phase_start

    m15_phase_start = time.perf_counter()
    m15_raw = _load_15m_bars(
        source_db_path=Path(source_db_path),
        codes=codes,
        start_day=start_day,
        end_day=end_day,
    )
    m15_phase_sec = time.perf_counter() - m15_phase_start

    if daily_prepared.empty:
        return {}, {
            **empty_metrics,
            "daily_phase_sec": round(daily_phase_sec, 4),
            "m15_phase_sec": round(m15_phase_sec, 4),
        }

    per_code_daily = {code: frame.copy() for code, frame in daily_prepared.groupby("code", sort=False)}
    per_code_m15 = {code: frame.copy() for code, frame in m15_raw.groupby("code", sort=False)} if not m15_raw.empty else {}

    result_map: dict[str, StockScanResult] = {}
    codes_passed_daily = 0
    total_flag_patterns = 0
    total_signals = 0
    stock_errors = 0

    scan_phase_start = time.perf_counter()
    for code in codes:
        name = code_to_name.get(code, "")
        code_daily = per_code_daily.get(code)
        code_m15 = per_code_m15.get(code)

        if code_daily is None or code_daily.empty:
            continue

        if not _check_daily_conditions(code_daily, daily_params):
            continue

        codes_passed_daily += 1

        if code_m15 is None or code_m15.empty:
            continue

        try:
            scan_result, stats = _scan_one_code(
                code=code,
                name=name,
                daily_frame=code_daily,
                m15_frame=code_m15,
                daily_params=daily_params,
                intraday_params=intraday_params,
                strategy_group_id=strategy_group_id,
                strategy_name=strategy_name,
            )
        except Exception as exc:
            stock_errors += 1
            result_map[code] = StockScanResult(
                code=code, name=name,
                processed_bars=0,
                signal_count=0, signals=[],
                error_message=f"{type(exc).__name__}: {exc}",
            )
            continue

        total_flag_patterns += stats["flag_pattern_count"]
        total_signals += stats["signal_count"]

        if scan_result.signal_count > 0 or scan_result.error_message:
            result_map[code] = scan_result

    scan_phase_sec = time.perf_counter() - scan_phase_start

    return result_map, {
        "daily_phase_sec": round(daily_phase_sec, 4),
        "m15_phase_sec": round(m15_phase_sec, 4),
        "scan_phase_sec": round(scan_phase_sec, 4),
        "total_daily_rows": len(daily_raw),
        "total_m15_rows": len(m15_raw),
        "total_codes": len(codes),
        "codes_passed_daily": codes_passed_daily,
        "flag_pattern_count": total_flag_patterns,
        "signal_count": total_signals,
        "stock_errors": stock_errors,
        "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
    }
