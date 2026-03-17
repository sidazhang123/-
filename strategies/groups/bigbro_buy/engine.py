"""
`bigbro_buy` specialized engine.

Goal:
1. Batch-scan daily bars in DuckDB with vectorized IO.
2. Keep rule semantics aligned with the existing backtrader implementation.
3. Return task-manager compatible result_map + metrics.
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
    connect_source_readonly,
)
from . import strategy as backtrader_strategy

# 向后兼容别名（外部若有按原名引用）
BigbroBuyStockScanResult = StockScanResult

WINDOW_FIELD_KEYS = (
    "anchor_day_ts",
    "window_start_ts",
    "window_end_ts",
    "chart_interval_start_ts",
    "chart_interval_end_ts",
)

DEFAULT_EXECUTION_PARAMS = {
    "fallback_to_backtrader": False,
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
    if not codes:
        return pd.DataFrame()

    with connect_source_readonly(source_db_path) as con:
        con.execute("drop table if exists _tmp_candidate_codes")
        con.execute("create temp table _tmp_candidate_codes (code varchar)")
        con.execute("insert into _tmp_candidate_codes(code) select unnest($1)", [codes])
        frame = con.execute(
            """
            select
                t.code,
                t.datetime as day_ts,
                t.open,
                t.close
            from klines_d t
            join _tmp_candidate_codes c on c.code = t.code
            where cast(t.datetime as date) >= ?
              and cast(t.datetime as date) <= ?
            order by t.code asc, t.datetime asc
            """,
            [start_day, end_day],
        ).fetchdf()

    if frame.empty:
        return frame

    frame["day_ts"] = pd.to_datetime(frame["day_ts"], errors="coerce")
    frame["open"] = pd.to_numeric(frame["open"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame


def _build_rows(daily_frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in daily_frame.itertuples(index=False):
        day_ts_raw = getattr(item, "day_ts", None)
        day_ts: datetime | None = None
        if day_ts_raw is not None and not pd.isna(day_ts_raw):
            day_ts = pd.Timestamp(day_ts_raw).to_pydatetime().replace(tzinfo=None)

        open_price_raw = getattr(item, "open", None)
        close_price_raw = getattr(item, "close", None)
        open_price = float(open_price_raw) if open_price_raw is not None and not pd.isna(open_price_raw) else None
        close_price = float(close_price_raw) if close_price_raw is not None and not pd.isna(close_price_raw) else None

        valid = (
            day_ts is not None
            and open_price is not None
            and close_price is not None
            and open_price > 0
        )

        body_return = None
        is_bull = False
        if valid:
            body_return = float((close_price - open_price) / open_price)
            is_bull = bool(close_price > open_price)

        rows.append(
            {
                "day_ts": day_ts,
                "open": open_price,
                "close": close_price,
                "body_return": body_return,
                "is_bull": is_bull,
                "valid": valid,
            }
        )
    return rows


def _build_disabled_per_rule_results() -> dict[str, dict[str, Any]]:
    return {
        "w": backtrader_strategy._disabled_result("w"),
        "60": backtrader_strategy._disabled_result("60"),
        "30": backtrader_strategy._disabled_result("30"),
        "15": backtrader_strategy._disabled_result("15"),
    }


def _extract_window_fields(d_values: dict[str, Any], combo_values: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for key in WINDOW_FIELD_KEYS:
        if combo_values.get(key) is not None:
            merged[key] = combo_values.get(key)
            continue
        if d_values.get(key) is not None:
            merged[key] = d_values.get(key)
    return merged


def _build_signal(
    *,
    code: str,
    name: str,
    signal_dt: datetime,
    strategy_group_id: str,
    strategy_name: str,
    group_params: dict[str, Any],
    d_values: dict[str, Any],
) -> dict[str, Any] | None:
    per_rule = _build_disabled_per_rule_results()
    per_rule["d"] = {
        "passed": True,
        "enabled": True,
        "ready": True,
        "message": "d 规则通过",
        "values": dict(d_values),
    }

    combo_result = backtrader_strategy.eval_combo(None, per_rule, group_params)
    if not combo_result.get("ready") or not combo_result.get("passed"):
        return None

    passed_timeframes = [
        tf
        for tf, result in per_rule.items()
        if result.get("enabled", True) and result.get("ready", False) and result.get("passed", False)
    ]

    combo_values = combo_result.get("values") if isinstance(combo_result.get("values"), dict) else {}
    payload = {
        "per_rule": per_rule,
        "combo": combo_result,
        "debug": {
            "passed_timeframes": passed_timeframes,
        },
    }
    payload.update(_extract_window_fields(d_values=d_values, combo_values=combo_values))

    signal_label = backtrader_strategy.build_signal_label(None, per_rule, combo_result, group_params)
    return {
        "code": code,
        "name": name,
        "signal_dt": signal_dt,
        "clock_tf": "d",
        "strategy_group_id": strategy_group_id,
        "strategy_name": strategy_name,
        "signal_label": str(signal_label or strategy_name),
        "payload": payload,
    }


def _scan_one_code(
    *,
    code: str,
    name: str,
    rows: list[dict[str, Any]],
    d_params: dict[str, Any],
    group_params: dict[str, Any],
    strategy_group_id: str,
    strategy_name: str,
) -> tuple[BigbroBuyStockScanResult, dict[str, int]]:
    processed_bars = len(rows)
    lookback_days = int(d_params["lookback_days"])
    candidate_anchor_count = 0
    kept_signal_count = 0

    if not d_params["enabled"]:
        return (
            BigbroBuyStockScanResult(
                code=code,
                name=name,
                processed_bars=processed_bars,
                signal_count=0,
                signals=[],
                error_message=None,
            ),
            {
                "candidate_anchor_count": candidate_anchor_count,
                "kept_signal_count": kept_signal_count,
            },
        )

    if processed_bars < lookback_days:
        return (
            BigbroBuyStockScanResult(
                code=code,
                name=name,
                processed_bars=processed_bars,
                signal_count=0,
                signals=[],
                error_message=None,
            ),
            {
                "candidate_anchor_count": candidate_anchor_count,
                "kept_signal_count": kept_signal_count,
            },
        )

    if d_params["last_bar_only"]:
        anchor_indexes = [processed_bars - 1]
    else:
        anchor_indexes = list(range(lookback_days - 1, processed_bars))

    signals: list[dict[str, Any]] = []
    for anchor_idx in anchor_indexes:
        candidate_anchor_count += 1
        window_start_idx = anchor_idx - lookback_days + 1
        lookback_window = rows[window_start_idx : anchor_idx + 1]
        if len(lookback_window) < lookback_days:
            continue
        if any(not item["valid"] for item in lookback_window):
            continue

        eval_rows = [
            {
                "day_ts": item["day_ts"],
                "open": float(item["open"]),
                "close": float(item["close"]),
                "body_return": float(item["body_return"]),
                "is_bull": bool(item["is_bull"]),
            }
            for item in lookback_window
        ]

        selected = backtrader_strategy._pick_latest_qualified_window(
            eval_rows,
            min_streak_days=int(d_params["min_streak_days"]),
            bull_ratio=float(d_params["bull_ratio"]),
            max_body_drop_pct=float(d_params["max_body_drop_pct"]),
            max_body_rise_pct=float(d_params["max_body_rise_pct"]),
        )
        if selected is None:
            continue

        d_values = {
            "lookback_days": lookback_days,
            "window_size": int(selected["window_size"]),
            "required_bulls": int(selected["required_bulls"]),
            "bull_count": int(selected["bull_count"]),
            "max_body_drop": float(selected["max_body_drop"]),
            "max_body_rise": float(selected["max_body_rise"]),
            "window_returns": list(selected["window_returns"]),
            "anchor_day_ts": selected["window_end_ts"],
            "window_start_ts": selected["window_start_ts"],
            "window_end_ts": selected["window_end_ts"],
            "chart_interval_start_ts": selected["window_start_ts"],
            "chart_interval_end_ts": selected["window_end_ts"],
        }

        signal_dt = rows[anchor_idx]["day_ts"]
        if signal_dt is None:
            continue

        signal = _build_signal(
            code=code,
            name=name,
            signal_dt=signal_dt,
            strategy_group_id=strategy_group_id,
            strategy_name=strategy_name,
            group_params=group_params,
            d_values=d_values,
        )
        if signal is None:
            continue

        signals.append(signal)
        kept_signal_count += 1

        if d_params["last_bar_only"]:
            break

    return (
        BigbroBuyStockScanResult(
            code=code,
            name=name,
            processed_bars=processed_bars,
            signal_count=len(signals),
            signals=signals,
            error_message=None,
        ),
        {
            "candidate_anchor_count": candidate_anchor_count,
            "kept_signal_count": kept_signal_count,
        },
    )


def run_bigbro_buy_specialized(
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
) -> tuple[dict[str, BigbroBuyStockScanResult], dict[str, Any]]:
    _ = cache_scope

    d_params = backtrader_strategy._normalize_d_params(group_params)
    execution_params = _normalize_execution_params(group_params)

    if not codes:
        return {}, {
            "daily_phase_sec": 0.0,
            "scan_phase_sec": 0.0,
            "daily_cache_hit": False,
            "anchor_count": 0,
            "codes_with_anchor": 0,
            "total_daily_rows": 0,
            "total_codes": 0,
            "worker_count": 1,
            "candidate_windows": 0,
            "kept_windows": 0,
            "stock_errors": 0,
            "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
        }

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

    result_map: dict[str, BigbroBuyStockScanResult] = {}
    total_daily_rows = 0
    candidate_windows = 0
    kept_windows = 0
    codes_with_anchor = 0
    stock_errors = 0

    if daily_raw.empty:
        return {}, {
            "daily_phase_sec": round(daily_phase_sec, 4),
            "scan_phase_sec": 0.0,
            "daily_cache_hit": False,
            "anchor_count": 0,
            "codes_with_anchor": 0,
            "total_daily_rows": 0,
            "total_codes": int(len(codes)),
            "worker_count": 1,
            "candidate_windows": 0,
            "kept_windows": 0,
            "stock_errors": 0,
            "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
        }

    grouped = {code: frame.copy() for code, frame in daily_raw.groupby("code", sort=False)}

    scan_phase_start = time.perf_counter()
    for code in codes:
        name = code_to_name.get(code, "")
        code_frame = grouped.get(code)
        if code_frame is None or code_frame.empty:
            continue

        code_rows = _build_rows(code_frame)
        total_daily_rows += len(code_rows)

        try:
            code_result, stats = _scan_one_code(
                code=code,
                name=name,
                rows=code_rows,
                d_params=d_params,
                group_params=group_params,
                strategy_group_id=strategy_group_id,
                strategy_name=strategy_name,
            )
        except Exception as exc:
            stock_errors += 1
            result_map[code] = BigbroBuyStockScanResult(
                code=code,
                name=name,
                processed_bars=len(code_rows),
                signal_count=0,
                signals=[],
                error_message=f"{type(exc).__name__}: {exc}",
            )
            continue

        candidate_windows += int(stats["candidate_anchor_count"])
        kept_windows += int(stats["kept_signal_count"])
        if code_result.signal_count > 0:
            codes_with_anchor += 1

        if code_result.signal_count > 0 or code_result.error_message:
            result_map[code] = code_result

    scan_phase_sec = time.perf_counter() - scan_phase_start

    metrics = {
        "daily_phase_sec": round(daily_phase_sec, 4),
        "scan_phase_sec": round(scan_phase_sec, 4),
        "daily_cache_hit": False,
        "anchor_count": int(candidate_windows),
        "codes_with_anchor": int(codes_with_anchor),
        "total_daily_rows": int(total_daily_rows),
        "total_codes": int(len(codes)),
        "worker_count": 1,
        "candidate_windows": int(candidate_windows),
        "kept_windows": int(kept_windows),
        "stock_errors": int(stock_errors),
        "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
    }
    return result_map, metrics
