"""
`bigbro_buy` backtrader 策略实现。

规则摘要：
1. 仅使用日线数据。
2. 在最近 N 日（默认10）中寻找任意连续窗口（默认最少4日）。
3. 连续窗口满足：
   - 阳线数 >= int(窗口长度 * 0.8)
   - 最大实体日跌幅 > -1%
   - 最大实体日涨幅 < 2.5%
4. 默认仅在最后一根日线判定。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

STRATEGY_LABEL = "大哥买点 v1"

# 图表区间标准合同：
# 1. payload.chart_interval_start_ts 必填；
# 2. payload.chart_interval_end_ts 必填；
# 3. payload.anchor_day_ts 可选。
CHART_INTERVAL_PAYLOAD_CONTRACT = {
    "chart_interval_start_ts": "required",
    "chart_interval_end_ts": "required",
    "anchor_day_ts": "optional",
}

DEFAULT_D_PARAMS = {
    "enabled": True,
    "lookback_days": 10,
    "min_streak_days": 4,
    "bull_ratio": 0.8,
    "max_body_drop_pct": 0.01,
    "max_body_rise_pct": 0.025,
    "last_bar_only": True,
}


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"1", "true", "yes", "y", "on"}:
            return True
        if token in {"0", "false", "no", "n", "off"}:
            return False
    return bool(value)


def _as_int(value: Any, default: int, *, minimum: int = 0, maximum: int | None = None) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def _as_float(value: Any, default: float, *, minimum: float | None = None, maximum: float | None = None) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = default
    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def _disabled_result(tf_label: str) -> dict[str, Any]:
    return {
        "passed": True,
        "enabled": False,
        "ready": True,
        "message": f"{tf_label} 规则禁用：bigbro_buy 仅使用 d 周期",
        "values": {},
    }


def _normalize_d_params(group_params: dict[str, Any]) -> dict[str, Any]:
    raw = _as_dict(group_params.get("d"))
    lookback_days = _as_int(raw.get("lookback_days"), DEFAULT_D_PARAMS["lookback_days"], minimum=4, maximum=120)
    min_streak_days = _as_int(
        raw.get("min_streak_days"),
        DEFAULT_D_PARAMS["min_streak_days"],
        minimum=4,
        maximum=lookback_days,
    )
    return {
        "enabled": _as_bool(raw.get("enabled"), DEFAULT_D_PARAMS["enabled"]),
        "lookback_days": lookback_days,
        "min_streak_days": min_streak_days,
        "bull_ratio": _as_float(raw.get("bull_ratio"), DEFAULT_D_PARAMS["bull_ratio"], minimum=0.0, maximum=1.0),
        "max_body_drop_pct": _as_float(raw.get("max_body_drop_pct"), DEFAULT_D_PARAMS["max_body_drop_pct"], minimum=0.0),
        "max_body_rise_pct": _as_float(raw.get("max_body_rise_pct"), DEFAULT_D_PARAMS["max_body_rise_pct"], minimum=0.0),
        "last_bar_only": _as_bool(raw.get("last_bar_only"), DEFAULT_D_PARAMS["last_bar_only"]),
    }


def _is_last_daily_bar(strategy: Any) -> bool:
    data_by_tf = getattr(strategy, "data_by_tf", {})
    if not isinstance(data_by_tf, dict):
        return False
    data = data_by_tf.get("d")
    if data is None:
        return False
    try:
        current_len = int(len(data))
        total_len = int(data.buflen())
    except Exception:
        return False
    return total_len > 0 and current_len == total_len


def _collect_recent_daily_bars(strategy: Any, lookback_days: int) -> list[dict[str, Any]] | None:
    rows: list[dict[str, Any]] = []
    for ago in range(lookback_days - 1, -1, -1):
        open_price = strategy.tf_line("d", "open", ago)
        close_price = strategy.tf_line("d", "close", ago)
        day_ts = strategy.tf_datetime("d", ago)
        if (
            open_price is None
            or close_price is None
            or day_ts is None
            or open_price <= 0
            or not isinstance(day_ts, datetime)
        ):
            return None
        body_return = (close_price - open_price) / open_price
        rows.append(
            {
                "day_ts": day_ts,
                "open": float(open_price),
                "close": float(close_price),
                "body_return": float(body_return),
                "is_bull": bool(close_price > open_price),
            }
        )
    return rows


def _pick_latest_qualified_window(
    rows: list[dict[str, Any]],
    *,
    min_streak_days: int,
    bull_ratio: float,
    max_body_drop_pct: float,
    max_body_rise_pct: float,
) -> dict[str, Any] | None:
    total = len(rows)
    best: dict[str, Any] | None = None

    for window_size in range(min_streak_days, total + 1):
        for start_idx in range(0, total - window_size + 1):
            end_idx = start_idx + window_size - 1
            window = rows[start_idx : end_idx + 1]
            bull_count = sum(1 for row in window if row["is_bull"])
            required_bulls = int(window_size * bull_ratio)
            max_rise = max(float(row["body_return"]) for row in window)
            max_drop = min(float(row["body_return"]) for row in window)

            passed = (
                bull_count >= required_bulls
                and max_drop > -max_body_drop_pct
                and max_rise < max_body_rise_pct
            )
            if not passed:
                continue

            candidate = {
                "start_idx": start_idx,
                "end_idx": end_idx,
                "window_size": window_size,
                "required_bulls": required_bulls,
                "bull_count": bull_count,
                "max_body_drop": max_drop,
                "max_body_rise": max_rise,
                "window_start_ts": window[0]["day_ts"],
                "window_end_ts": window[-1]["day_ts"],
                "window_returns": [float(row["body_return"]) for row in window],
            }
            if best is None:
                best = candidate
                continue
            if candidate["end_idx"] > best["end_idx"]:
                best = candidate
                continue
            if candidate["end_idx"] == best["end_idx"] and candidate["window_size"] > best["window_size"]:
                best = candidate

    return best


def eval_w(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    _ = (strategy, group_params)
    return _disabled_result("w")


def eval_60(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    _ = (strategy, group_params)
    return _disabled_result("60")


def eval_30(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    _ = (strategy, group_params)
    return _disabled_result("30")


def eval_15(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    _ = (strategy, group_params)
    return _disabled_result("15")


def eval_d(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    params = _normalize_d_params(group_params)
    if not params["enabled"]:
        return {
            "passed": True,
            "enabled": False,
            "ready": True,
            "message": "d 规则未启用",
            "values": {},
        }

    if params["last_bar_only"] and not _is_last_daily_bar(strategy):
        return {
            "passed": False,
            "enabled": True,
            "ready": True,
            "message": "d 规则仅在最后一根日线判定（last_bar_only=true）",
            "values": {
                "last_bar_only": True,
                "is_last_daily_bar": False,
            },
        }

    lookback_days = params["lookback_days"]
    if not strategy.tf_has_bars("d", lookback_days):
        return {
            "passed": False,
            "enabled": True,
            "ready": False,
            "message": f"d 数据不足，至少需要 {lookback_days} 根日线",
            "values": {
                "required_bars": lookback_days,
            },
        }

    rows = _collect_recent_daily_bars(strategy, lookback_days)
    if rows is None:
        return {
            "passed": False,
            "enabled": True,
            "ready": False,
            "message": "d 数据存在空值或异常，无法完成判定",
            "values": {},
        }

    selected = _pick_latest_qualified_window(
        rows,
        min_streak_days=params["min_streak_days"],
        bull_ratio=params["bull_ratio"],
        max_body_drop_pct=params["max_body_drop_pct"],
        max_body_rise_pct=params["max_body_rise_pct"],
    )

    if selected is None:
        return {
            "passed": False,
            "enabled": True,
            "ready": True,
            "message": "最近窗口内未找到满足条件的连续区间",
            "values": {
                "lookback_days": lookback_days,
                "min_streak_days": params["min_streak_days"],
                "bull_ratio": params["bull_ratio"],
                "max_body_drop_pct": params["max_body_drop_pct"],
                "max_body_rise_pct": params["max_body_rise_pct"],
            },
        }

    window_start_ts = selected["window_start_ts"]
    window_end_ts = selected["window_end_ts"]
    return {
        "passed": True,
        "enabled": True,
        "ready": True,
        "message": "d 规则通过",
        "values": {
            "lookback_days": lookback_days,
            "window_size": selected["window_size"],
            "required_bulls": selected["required_bulls"],
            "bull_count": selected["bull_count"],
            "max_body_drop": selected["max_body_drop"],
            "max_body_rise": selected["max_body_rise"],
            "window_returns": selected["window_returns"],
            "anchor_day_ts": window_end_ts,
            "window_start_ts": window_start_ts,
            "window_end_ts": window_end_ts,
            "chart_interval_start_ts": window_start_ts,
            "chart_interval_end_ts": window_end_ts,
        },
    }


def eval_combo(
    strategy: Any,
    per_rule_results: dict[str, dict[str, Any]],
    group_params: dict[str, Any],
) -> dict[str, Any]:
    _ = strategy
    emit_signal = _as_bool(_as_dict(group_params.get("combo")).get("emit_signal"), True)
    if not emit_signal:
        return {
            "passed": False,
            "enabled": True,
            "ready": True,
            "message": "组合层 emit_signal=false，不输出信号",
            "values": {
                "emit_signal": False,
            },
        }

    d_result = per_rule_results.get("d")
    if not isinstance(d_result, dict):
        return {
            "passed": False,
            "enabled": True,
            "ready": False,
            "message": "d 规则结果缺失",
            "values": {},
        }

    d_values = d_result.get("values") if isinstance(d_result.get("values"), dict) else {}
    if not d_result.get("enabled", True):
        return {
            "passed": False,
            "enabled": True,
            "ready": True,
            "message": "d 规则未启用，组合层不输出信号",
            "values": {
                "emit_signal": True,
            },
        }

    if not d_result.get("ready", False):
        return {
            "passed": False,
            "enabled": True,
            "ready": False,
            "message": "d 规则尚未就绪",
            "values": {
                "emit_signal": True,
            },
        }

    if not d_result.get("passed", False):
        return {
            "passed": False,
            "enabled": True,
            "ready": True,
            "message": "d 规则未通过",
            "values": {
                "emit_signal": True,
                **d_values,
            },
        }

    return {
        "passed": True,
        "enabled": True,
        "ready": True,
        "message": "bigbro_buy 组合规则通过",
        "values": {
            "emit_signal": True,
            **d_values,
        },
    }


def build_signal_label(
    strategy: Any,
    per_rule_results: dict[str, dict[str, Any]],
    combo_result: dict[str, Any],
    group_params: dict[str, Any],
) -> str:
    _ = (strategy, per_rule_results, combo_result, group_params)
    return STRATEGY_LABEL


GROUP_HANDLERS = {
    "eval_w": eval_w,
    "eval_d": eval_d,
    "eval_60": eval_60,
    "eval_30": eval_30,
    "eval_15": eval_15,
    "eval_combo": eval_combo,
    "build_signal_label": build_signal_label,
}
