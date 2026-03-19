"""
`bu_zhi_dao_v1` 专用引擎。

策略摘要：
1. 以 15 交易日滑动窗口为基础，先定位锚点日（大涨 + 放量）。
2. 再校验锚点后第 2~3 日价格与量能约束。
3. 最后校验窗口末日的日线振幅、日线量能与 15m 均价约束。

输出约定：
1. 每个有效窗口最多输出一条信号。
2. payload 同时写入窗口区间、锚点信息、阈值快照与审计标记。
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from datetime import time as dt_time
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
    connect_source_readonly,
)

# 向后兼容别名
BuZhiDaoStockScanResult = StockScanResult

# 日线规则默认参数（均为可配置项）。
DEFAULT_DAILY_PARAMS = {
    "window_days": 15,
    "anchor_gain_min_pct": 0.08,
    "anchor_pre_volume_lookback_days": 10,
    "anchor_pre_volume_mean_ratio_min": 3.0,
    "post_check_start_day": 2,
    "post_check_end_day": 3,
    "post_price_cap_gain_factor": 0.5,
    "post_volume_ratio_max": 0.6,
    "last_day_amplitude_max": 0.02,
    "last_day_mean_price_ratio_max": 1.2,
    "last_day_volume_ratio_max": 0.35,
}

DEFAULT_EXECUTION_PARAMS = {
    "fallback_to_backtrader": False,
}


def _normalize_daily_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化日线参数，确保所有阈值可计算。"""

    raw = as_dict(group_params.get("daily"))

    lookback_days = as_int(
        raw.get("anchor_pre_volume_lookback_days"),
        DEFAULT_DAILY_PARAMS["anchor_pre_volume_lookback_days"],
        minimum=2,
        maximum=120,
    )
    window_days = as_int(
        raw.get("window_days"),
        DEFAULT_DAILY_PARAMS["window_days"],
        minimum=8,
        maximum=90,
    )
    post_start = as_int(
        raw.get("post_check_start_day"),
        DEFAULT_DAILY_PARAMS["post_check_start_day"],
        minimum=1,
        maximum=20,
    )
    post_end = as_int(
        raw.get("post_check_end_day"),
        DEFAULT_DAILY_PARAMS["post_check_end_day"],
        minimum=post_start,
        maximum=30,
    )
    return {
        "window_days": window_days,
        "anchor_gain_min_pct": as_float(
            raw.get("anchor_gain_min_pct"),
            DEFAULT_DAILY_PARAMS["anchor_gain_min_pct"],
            minimum=0.0,
        ),
        "anchor_pre_volume_lookback_days": lookback_days,
        "anchor_pre_volume_mean_ratio_min": as_float(
            raw.get("anchor_pre_volume_mean_ratio_min"),
            DEFAULT_DAILY_PARAMS["anchor_pre_volume_mean_ratio_min"],
            minimum=0.0,
        ),
        "post_check_start_day": post_start,
        "post_check_end_day": post_end,
        "post_price_cap_gain_factor": as_float(
            raw.get("post_price_cap_gain_factor"),
            DEFAULT_DAILY_PARAMS["post_price_cap_gain_factor"],
            minimum=0.0,
        ),
        "post_volume_ratio_max": as_float(
            raw.get("post_volume_ratio_max"),
            DEFAULT_DAILY_PARAMS["post_volume_ratio_max"],
            minimum=0.0,
        ),
        "last_day_amplitude_max": as_float(
            raw.get("last_day_amplitude_max"),
            DEFAULT_DAILY_PARAMS["last_day_amplitude_max"],
            minimum=0.0,
        ),
        "last_day_mean_price_ratio_max": as_float(
            raw.get("last_day_mean_price_ratio_max"),
            DEFAULT_DAILY_PARAMS["last_day_mean_price_ratio_max"],
            minimum=0.0,
        ),
        "last_day_volume_ratio_max": as_float(
            raw.get("last_day_volume_ratio_max"),
            DEFAULT_DAILY_PARAMS["last_day_volume_ratio_max"],
            minimum=0.0,
        ),
    }


def _normalize_execution_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """
    输入：
    1. group_params: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `_normalize_execution_params` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    raw = as_dict(group_params.get("execution"))
    return {
        "fallback_to_backtrader": as_bool(
            raw.get("fallback_to_backtrader"),
            DEFAULT_EXECUTION_PARAMS["fallback_to_backtrader"],
        )
    }


def _load_daily_bars(
    *,
    source_db_path: Path,
    codes: list[str],
    fetch_start_day: date,
    end_day: date,
) -> pd.DataFrame:
    """读取日线行情，包含回看天数。"""

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
                t.high,
                t.low,
                t.close,
                t.volume
            from klines_d t
            join _tmp_candidate_codes c on c.code = t.code
            where cast(t.datetime as date) >= ?
              and cast(t.datetime as date) <= ?
            order by t.code asc, t.datetime asc
            """,
            [fetch_start_day, end_day],
        ).fetchdf()

    if frame.empty:
        return frame

    frame["day_ts"] = pd.to_datetime(frame["day_ts"])
    for key in ["open", "high", "low", "close", "volume"]:
        frame[key] = pd.to_numeric(frame[key], errors="coerce")
    frame["trade_day"] = frame["day_ts"].dt.date
    return frame


def _load_intraday_day_ohlc_mean(
    *,
    source_db_path: Path,
    codes: list[str],
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    """读取最后一天约束所需的 15m 日聚合均值。"""

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
                cast(t.datetime as date) as trade_day,
                (sum(t.open) + sum(t.high) + sum(t.low) + sum(t.close)) / (4.0 * count(*)) as ohlc_mean
            from klines_15 t
            join _tmp_candidate_codes c on c.code = t.code
            where cast(t.datetime as date) >= ?
              and cast(t.datetime as date) <= ?
            group by t.code, cast(t.datetime as date)
            order by t.code asc, trade_day asc
            """,
            [start_day, end_day],
        ).fetchdf()

    if frame.empty:
        return frame

    frame["trade_day"] = pd.to_datetime(frame["trade_day"]).dt.date
    frame["ohlc_mean"] = pd.to_numeric(frame["ohlc_mean"], errors="coerce")
    return frame


def _prepare_daily_features(frame: pd.DataFrame, lookback_days: int) -> pd.DataFrame:
    """计算该策略需要的日线衍生字段。"""

    if frame.empty:
        return frame

    prepared: list[pd.DataFrame] = []
    for _, code_frame in frame.groupby("code", sort=False):
        part = code_frame.sort_values("day_ts").copy()
        part["prev_close"] = part["close"].shift(1)
        part["day_gain"] = (part["close"] - part["prev_close"]) / part["prev_close"]
        part["day_amplitude"] = (part["high"] - part["low"]) / part["prev_close"]
        part["pre_volume_mean"] = part["volume"].shift(1).rolling(lookback_days, min_periods=lookback_days).mean()
        prepared.append(part)
    return pd.concat(prepared, axis=0, ignore_index=True)


def _is_finite_number(value: Any) -> bool:
    """
    输入：
    1. value: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `_is_finite_number` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    try:
        return bool(np.isfinite(float(value)))
    except Exception:
        return False


def _scan_one_code(
    *,
    code: str,
    name: str,
    daily_frame: pd.DataFrame,
    intraday_day_mean_map: dict[date, float],
    start_day: date,
    end_day: date,
    daily_params: dict[str, Any],
    strategy_group_id: str,
    strategy_name: str,
) -> tuple[BuZhiDaoStockScanResult, dict[str, int]]:
    """扫描单股票，返回信号与统计信息。"""

    stats = {
        "processed_bars": 0,
        "anchor_hit_count": 0,
        "candidate_window_count": 0,
        "kept_window_count": 0,
        "dedup_dropped_count": 0,
    }

    if daily_frame.empty:
        return BuZhiDaoStockScanResult(code=code, name=name, processed_bars=0, signal_count=0, signals=[]), stats

    scoped = daily_frame[
        (daily_frame["trade_day"] >= start_day) & (daily_frame["trade_day"] <= end_day)
    ].sort_values("day_ts")
    scoped = scoped.reset_index(drop=True)

    stats["processed_bars"] = int(len(scoped))
    if scoped.empty or len(scoped) < int(daily_params["window_days"]):
        return (
            BuZhiDaoStockScanResult(
                code=code,
                name=name,
                processed_bars=stats["processed_bars"],
                signal_count=0,
                signals=[],
            ),
            stats,
        )

    window_days = int(daily_params["window_days"])
    anchor_gain_min_pct = float(daily_params["anchor_gain_min_pct"])
    anchor_pre_volume_mean_ratio_min = float(daily_params["anchor_pre_volume_mean_ratio_min"])
    post_check_start_day = int(daily_params["post_check_start_day"])
    post_check_end_day = int(daily_params["post_check_end_day"])
    post_price_cap_gain_factor = float(daily_params["post_price_cap_gain_factor"])
    post_volume_ratio_max = float(daily_params["post_volume_ratio_max"])
    last_day_amplitude_max = float(daily_params["last_day_amplitude_max"])
    last_day_mean_price_ratio_max = float(daily_params["last_day_mean_price_ratio_max"])
    last_day_volume_ratio_max = float(daily_params["last_day_volume_ratio_max"])

    window_candidates: list[dict[str, Any]] = []
    max_anchor_index_offset = post_check_end_day
    last_anchor_abs_index = len(scoped) - 1

    for window_start in range(0, len(scoped) - window_days + 1):
        window_end = window_start + window_days - 1

        # 锚点必须预留 post_check_end_day 的后置天数。
        anchor_search_end = min(window_end - max_anchor_index_offset, last_anchor_abs_index)
        if anchor_search_end < window_start:
            continue

        picked_anchor: int | None = None
        for anchor_abs in range(window_start, anchor_search_end + 1):
            anchor = scoped.iloc[anchor_abs]
            prev_close = anchor.get("prev_close")
            anchor_gain = anchor.get("day_gain")
            pre_volume_mean = anchor.get("pre_volume_mean")
            anchor_volume = anchor.get("volume")

            if not (_is_finite_number(prev_close) and float(prev_close) > 0):
                continue
            if not (_is_finite_number(anchor_gain) and float(anchor_gain) > anchor_gain_min_pct):
                continue
            if not (_is_finite_number(pre_volume_mean) and float(pre_volume_mean) > 0):
                continue
            if not (_is_finite_number(anchor_volume) and float(anchor_volume) > 0):
                continue
            if not (float(anchor_volume) > float(pre_volume_mean) * anchor_pre_volume_mean_ratio_min):
                continue

            # 锚点后第2~3天约束：逐日 AND。
            price_cap = float(prev_close) * (1.0 + float(anchor_gain) * post_price_cap_gain_factor)
            post_ok = True
            for offset in range(post_check_start_day, post_check_end_day + 1):
                check_abs = anchor_abs + offset
                if check_abs > window_end:
                    post_ok = False
                    break
                day_row = scoped.iloc[check_abs]
                open_close_max = max(float(day_row["open"]), float(day_row["close"]))
                if not (open_close_max < price_cap):
                    post_ok = False
                    break
                if not (float(day_row["volume"]) < float(anchor_volume) * post_volume_ratio_max):
                    post_ok = False
                    break
            if not post_ok:
                continue

            picked_anchor = anchor_abs
            stats["anchor_hit_count"] += 1
            break

        if picked_anchor is None:
            continue

        anchor_row = scoped.iloc[picked_anchor]
        last_row = scoped.iloc[window_end]
        last_prev_close = last_row.get("prev_close")
        if not (_is_finite_number(last_prev_close) and float(last_prev_close) > 0):
            continue

        last_day_amplitude = (float(last_row["high"]) - float(last_row["low"])) / float(last_prev_close)
        if not (last_day_amplitude < last_day_amplitude_max):
            continue

        last_trade_day = last_row["trade_day"]
        last_day_ohlc_mean = intraday_day_mean_map.get(last_trade_day)
        if not _is_finite_number(last_day_ohlc_mean):
            continue

        anchor_open = float(anchor_row["open"])
        anchor_volume = float(anchor_row["volume"])
        if not (float(last_day_ohlc_mean) < anchor_open * last_day_mean_price_ratio_max):
            continue
        if not (float(last_row["volume"]) < anchor_volume * last_day_volume_ratio_max):
            continue

        stats["candidate_window_count"] += 1
        signal_dt = datetime.combine(last_trade_day, dt_time(hour=15, minute=0))
        window_start_ts = scoped.iloc[window_start]["day_ts"].to_pydatetime()
        window_end_ts = last_row["day_ts"].to_pydatetime()
        anchor_day_ts = anchor_row["day_ts"].to_pydatetime()

        # 为前端与回归提供可核对的审计字段。
        audit_flags = {
            "anchor_gain_pass": True,
            "anchor_volume_pass": True,
            "post_days_price_pass": True,
            "post_days_volume_pass": True,
            "last_day_amplitude_pass": True,
            "last_day_mean_price_pass": True,
            "last_day_volume_pass": True,
        }
        audit_flags["all_passed"] = True

        daily_metrics = {
            "anchor_prev_close": float(anchor_row["prev_close"]),
            "anchor_gain": float(anchor_row["day_gain"]),
            "anchor_volume": anchor_volume,
            "anchor_pre_volume_mean": float(anchor_row["pre_volume_mean"]),
            "anchor_open": anchor_open,
            "day2_open_or_close_max": max(
                float(scoped.iloc[picked_anchor + post_check_start_day]["open"]),
                float(scoped.iloc[picked_anchor + post_check_start_day]["close"]),
            ),
            "day3_open_or_close_max": max(
                float(scoped.iloc[picked_anchor + post_check_end_day]["open"]),
                float(scoped.iloc[picked_anchor + post_check_end_day]["close"]),
            ),
            "day2_volume": float(scoped.iloc[picked_anchor + post_check_start_day]["volume"]),
            "day3_volume": float(scoped.iloc[picked_anchor + post_check_end_day]["volume"]),
        }

        thresholds = {
            "window_days": window_days,
            "anchor_gain_min_pct": anchor_gain_min_pct,
            "anchor_pre_volume_mean_ratio_min": anchor_pre_volume_mean_ratio_min,
            "post_check_start_day": post_check_start_day,
            "post_check_end_day": post_check_end_day,
            "post_price_cap_gain_factor": post_price_cap_gain_factor,
            "post_volume_ratio_max": post_volume_ratio_max,
            "last_day_amplitude_max": last_day_amplitude_max,
            "last_day_mean_price_ratio_max": last_day_mean_price_ratio_max,
            "last_day_volume_ratio_max": last_day_volume_ratio_max,
            "amplitude_formula": "(high-low)/prev_close",
        }

        payload = {
            "anchor_day_ts": anchor_day_ts,
            "window_start_ts": window_start_ts,
            "window_end_ts": window_end_ts,
            "chart_interval_start_ts": anchor_day_ts,
            "chart_interval_end_ts": window_end_ts,
            "daily_metrics": daily_metrics,
            "last_day_metrics": {
                "last_day_amplitude": float(last_day_amplitude),
                "last_day_15m_ohlc_mean": float(last_day_ohlc_mean),
                "last_day_volume": float(last_row["volume"]),
            },
            "thresholds": thresholds,
            "audit_pass_flags": audit_flags,
        }

        window_candidates.append(
            {
                "window_start_day": scoped.iloc[window_start]["trade_day"],
                "window_end_day": last_trade_day,
                "signal": {
                    "code": code,
                    "name": name,
                    "signal_dt": signal_dt,
                    "clock_tf": "d",
                    "strategy_group_id": strategy_group_id,
                    "strategy_name": strategy_name,
                    "signal_label": strategy_name,
                    "payload": payload,
                },
            }
        )

    # 同股票窗口去重：按时间顺序保留第一个，后续有重叠则丢弃。
    kept_windows: list[dict[str, Any]] = []
    for item in sorted(window_candidates, key=lambda x: x["window_start_day"]):
        if not kept_windows:
            kept_windows.append(item)
            continue
        last_kept = kept_windows[-1]
        if item["window_start_day"] <= last_kept["window_end_day"]:
            stats["dedup_dropped_count"] += 1
            continue
        kept_windows.append(item)

    stats["kept_window_count"] = len(kept_windows)
    signals = [item["signal"] for item in kept_windows]
    return (
        BuZhiDaoStockScanResult(
            code=code,
            name=name,
            processed_bars=stats["processed_bars"],
            signal_count=len(signals),
            signals=signals,
            error_message=None,
        ),
        stats,
    )


def run_bu_zhi_dao_v1_specialized(
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
) -> tuple[dict[str, BuZhiDaoStockScanResult], dict[str, Any]]:
    """
    `bu_zhi_dao_v1` 专用入口。

    说明：
    1. `cache_scope` / `cache_dir` 为统一接口保留参数，当前策略首版不启用缓存；
    2. 返回值结构与现有 specialized 引擎保持一致，供 TaskManager 直接消费。
    """

    _ = (cache_scope, cache_dir)
    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        raise ValueError("start_ts 不能晚于 end_ts")

    daily_params = _normalize_daily_params(group_params)
    execution_params = _normalize_execution_params(group_params)

    if not codes:
        return {}, {
            "daily_phase_sec": 0.0,
            "intraday_phase_sec": 0.0,
            "daily_cache_hit": False,
            "anchor_count": 0,
            "codes_with_anchor": 0,
            "total_daily_rows": 0,
            "total_codes": 0,
            "worker_count": 1,
            "candidate_windows": 0,
            "kept_windows": 0,
            "dedup_dropped_windows": 0,
            "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
        }

    end_dt = end_ts or datetime.now()
    screen_span_days = max(
        int(daily_params["window_days"]) + int(daily_params["post_check_end_day"]) + 20,
        60,
    )
    start_day = start_ts.date() if start_ts is not None else (end_dt - timedelta(days=screen_span_days)).date()
    end_day = end_dt.date()
    lookback_days = int(daily_params["anchor_pre_volume_lookback_days"])
    fetch_padding = max(lookback_days + int(daily_params["window_days"]) + 10, 45)
    fetch_start_day = start_day - timedelta(days=fetch_padding)

    daily_phase_start = time.perf_counter()
    daily_raw = _load_daily_bars(
        source_db_path=source_db_path,
        codes=codes,
        fetch_start_day=fetch_start_day,
        end_day=end_day,
    )
    daily_prepared = _prepare_daily_features(daily_raw, lookback_days)
    daily_phase_sec = time.perf_counter() - daily_phase_start

    intraday_phase_start = time.perf_counter()
    intraday_day_mean = _load_intraday_day_ohlc_mean(
        source_db_path=source_db_path,
        codes=codes,
        start_day=start_day,
        end_day=end_day,
    )
    intraday_phase_load_sec = time.perf_counter() - intraday_phase_start

    intraday_map_by_code: dict[str, dict[date, float]] = {}
    if not intraday_day_mean.empty:
        for _, row in intraday_day_mean.iterrows():
            code = str(row["code"])
            trade_day = row["trade_day"]
            mean_value = float(row["ohlc_mean"])
            intraday_map_by_code.setdefault(code, {})[trade_day] = mean_value

    result_map: dict[str, BuZhiDaoStockScanResult] = {}
    total_daily_rows = 0
    anchor_count = 0
    candidate_windows = 0
    kept_windows = 0
    dedup_dropped_windows = 0
    codes_with_anchor = 0
    stock_errors = 0

    if daily_prepared.empty:
        metrics = {
            "daily_phase_sec": round(daily_phase_sec, 4),
            "intraday_phase_sec": round(intraday_phase_load_sec, 4),
            "daily_cache_hit": False,
            "anchor_count": 0,
            "codes_with_anchor": 0,
            "total_daily_rows": 0,
            "total_codes": len(codes),
            "worker_count": 1,
            "candidate_windows": 0,
            "kept_windows": 0,
            "dedup_dropped_windows": 0,
            "stock_errors": 0,
            "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
        }
        return {}, metrics

    per_code_daily = {code: frame.copy() for code, frame in daily_prepared.groupby("code", sort=False)}

    scan_phase_start = time.perf_counter()
    for code in codes:
        name = code_to_name.get(code, "")
        code_daily_frame = per_code_daily.get(code)
        if code_daily_frame is None or code_daily_frame.empty:
            continue

        try:
            scan_result, stats = _scan_one_code(
                code=code,
                name=name,
                daily_frame=code_daily_frame,
                intraday_day_mean_map=intraday_map_by_code.get(code, {}),
                start_day=start_day,
                end_day=end_day,
                daily_params=daily_params,
                strategy_group_id=strategy_group_id,
                strategy_name=strategy_name,
            )
        except Exception as exc:
            stock_errors += 1
            result_map[code] = BuZhiDaoStockScanResult(
                code=code,
                name=name,
                processed_bars=0,
                signal_count=0,
                signals=[],
                error_message=f"{type(exc).__name__}: {exc}",
            )
            continue

        total_daily_rows += int(stats["processed_bars"])
        anchor_count += int(stats["anchor_hit_count"])
        candidate_windows += int(stats["candidate_window_count"])
        kept_windows += int(stats["kept_window_count"])
        dedup_dropped_windows += int(stats["dedup_dropped_count"])
        if stats["anchor_hit_count"] > 0:
            codes_with_anchor += 1

        # 与既有 specialized 链路保持一致：仅命中或异常才进入 result_map。
        if scan_result.error_message or scan_result.signal_count > 0:
            result_map[code] = scan_result

    scan_phase_sec = time.perf_counter() - scan_phase_start
    intraday_phase_sec = intraday_phase_load_sec + scan_phase_sec

    metrics = {
        "daily_phase_sec": round(daily_phase_sec, 4),
        "intraday_phase_sec": round(intraday_phase_sec, 4),
        "daily_cache_hit": False,
        "anchor_count": int(anchor_count),
        "codes_with_anchor": int(codes_with_anchor),
        "total_daily_rows": int(total_daily_rows),
        "total_codes": int(len(codes)),
        "worker_count": 1,
        "candidate_windows": int(candidate_windows),
        "kept_windows": int(kept_windows),
        "dedup_dropped_windows": int(dedup_dropped_windows),
        "stock_errors": int(stock_errors),
        "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
    }
    return result_map, metrics
