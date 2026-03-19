"""
`weekly_slow_bull_v1` specialized engine。

职责：
1. 批量读取周线与日线数据，并计算所需均线与四周斜率。
2. 识别最近若干根周线中斜率持续落在目标区间、且最新日线贴近日线 MA20 的慢牛股。
3. 生成符合 TaskManager 落库合同的标准化信号结构。

规则口径：
1. MA10 四周斜率 = (当前 MA10 - 4 周前 MA10) / 4。
2. MA20 四周斜率 = (当前 MA20 - 4 周前 MA20) / 4。
3. 默认要求最近 8 个不重叠周线窗口的 MA10 四周斜率全部在 10 到 20 之间。
4. 默认要求最近 1 个不重叠周线窗口的 MA20 四周斜率在 12 到 20 之间。
5. 默认要求最新日线收盘价处于日线 MA20 的正负 5% 范围内。
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
    connect_source_readonly,
)

STRATEGY_LABEL = "周线慢牛 v1"

DEFAULT_WEEKLY_PARAMS: dict[str, Any] = {
    "ma10_period": 10,
    "ma20_period": 20,
    "slope_gap_weeks": 4,
    "ma10_recent_windows": 8,
    "ma10_slope_min": 10.0,
    "ma10_slope_max": 20.0,
    "ma20_recent_windows": 1,
    "ma20_slope_min": 12.0,
    "ma20_slope_max": 20.0,
    "signal_window_bars": 36,
}

DEFAULT_DAILY_PARAMS: dict[str, Any] = {
    "ma20_period": 20,
    "close_to_ma20_pct": 0.05,
}

DEFAULT_EXECUTION_PARAMS: dict[str, Any] = {
    "fallback_to_backtrader": False,
}


def _normalize_weekly_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化周线参数。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回已完成类型转换、边界裁剪后的周线参数字典。
    用途：
    1. 保证 manifest 默认值和运行时覆盖值都能安全进入 engine 逻辑。
    边界条件：
    1. `ma20_period` 会被强制不小于 `ma10_period`。
    2. 斜率区间上界会被强制不小于下界。
    """
    raw = as_dict(group_params.get("weekly"))
    ma10_period = as_int(raw.get("ma10_period"), DEFAULT_WEEKLY_PARAMS["ma10_period"], minimum=2, maximum=120)
    ma20_period = as_int(raw.get("ma20_period"), DEFAULT_WEEKLY_PARAMS["ma20_period"], minimum=ma10_period, maximum=240)
    slope_gap_weeks = as_int(raw.get("slope_gap_weeks"), DEFAULT_WEEKLY_PARAMS["slope_gap_weeks"], minimum=1, maximum=26)
    ma10_recent_windows = as_int(raw.get("ma10_recent_windows"), DEFAULT_WEEKLY_PARAMS["ma10_recent_windows"], minimum=1, maximum=52)
    ma20_recent_windows = as_int(raw.get("ma20_recent_windows"), DEFAULT_WEEKLY_PARAMS["ma20_recent_windows"], minimum=1, maximum=52)
    ma10_slope_min = as_float(raw.get("ma10_slope_min"), DEFAULT_WEEKLY_PARAMS["ma10_slope_min"])
    ma10_slope_max = as_float(raw.get("ma10_slope_max"), DEFAULT_WEEKLY_PARAMS["ma10_slope_max"], minimum=ma10_slope_min)
    ma20_slope_min = as_float(raw.get("ma20_slope_min"), DEFAULT_WEEKLY_PARAMS["ma20_slope_min"])
    ma20_slope_max = as_float(raw.get("ma20_slope_max"), DEFAULT_WEEKLY_PARAMS["ma20_slope_max"], minimum=ma20_slope_min)
    signal_window_bars = as_int(raw.get("signal_window_bars"), DEFAULT_WEEKLY_PARAMS["signal_window_bars"], minimum=8, maximum=200)
    return {
        "ma10_period": ma10_period,
        "ma20_period": ma20_period,
        "slope_gap_weeks": slope_gap_weeks,
        "ma10_recent_windows": ma10_recent_windows,
        "ma10_slope_min": ma10_slope_min,
        "ma10_slope_max": ma10_slope_max,
        "ma20_recent_windows": ma20_recent_windows,
        "ma20_slope_min": ma20_slope_min,
        "ma20_slope_max": ma20_slope_max,
        "signal_window_bars": signal_window_bars,
    }


def _normalize_daily_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化日线参数。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回已完成类型转换、边界裁剪后的日线参数字典。
    用途：
    1. 统一最新日线收盘价与日线 MA20 偏离范围的判断口径。
    边界条件：
    1. 偏离百分比按绝对值处理，并裁剪到 0 到 0.5 之间。
    """
    raw = as_dict(group_params.get("daily"))
    return {
        "ma20_period": as_int(
            raw.get("ma20_period"),
            DEFAULT_DAILY_PARAMS["ma20_period"],
            minimum=2,
            maximum=120,
        ),
        "close_to_ma20_pct": as_float(
            raw.get("close_to_ma20_pct"),
            DEFAULT_DAILY_PARAMS["close_to_ma20_pct"],
            minimum=0.0,
            maximum=0.5,
        ),
    }


def _normalize_execution_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化执行参数。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回执行层参数字典。
    用途：
    1. 保留 specialized 与 backtrader 回退路径的统一参数接口。
    边界条件：
    1. 当前策略默认不启用回退。
    """
    raw = as_dict(group_params.get("execution"))
    return {
        "fallback_to_backtrader": as_bool(
            raw.get("fallback_to_backtrader"),
            DEFAULT_EXECUTION_PARAMS["fallback_to_backtrader"],
        ),
    }


def _read_universe_filter_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """读取框架保留的股票池预筛选配置。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回概念过滤参数，仅用于 metrics 和调试展示。
    用途：
    1. 记录 TaskManager 预筛选后的上下文，不在 engine 内二次裁剪股票池。
    边界条件：
    1. 引擎不重复执行同一轮概念过滤。
    """
    raw_universe = as_dict(group_params.get("universe_filters"))
    raw_concepts = as_dict(raw_universe.get("concepts"))
    concept_terms = raw_concepts.get("concept_terms")
    reason_terms = raw_concepts.get("reason_terms")
    return {
        "enabled": as_bool(raw_concepts.get("enabled"), False),
        "concept_terms": [str(item).strip() for item in concept_terms] if isinstance(concept_terms, list) else [],
        "reason_terms": [str(item).strip() for item in reason_terms] if isinstance(reason_terms, list) else [],
    }


def _required_history_bars(weekly_params: dict[str, Any]) -> int:
    """计算命中规则所需的最少周线数量。

    输入：
    1. weekly_params: 已规范化的周线参数。
    输出：
    1. 返回满足 MA10 与 MA20 斜率检查所需的最少原始周线根数。
    用途：
    1. 提前判断单股是否具备足够历史，避免在扫描阶段反复做隐式推导。
    边界条件：
    1. 同时覆盖 MA10 与 MA20 两组条件，取两者所需历史的较大值。
    """
    ma10_need = (
        int(weekly_params["ma10_period"])
        + int(weekly_params["slope_gap_weeks"]) * int(weekly_params["ma10_recent_windows"])
    )
    ma20_need = (
        int(weekly_params["ma20_period"])
        + int(weekly_params["slope_gap_weeks"]) * int(weekly_params["ma20_recent_windows"])
    )
    return max(ma10_need, ma20_need)


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
    2. codes: 待扫描股票代码列表。
    3. start_day/end_day: 日期范围边界。
    输出：
    1. 返回按 `code + week_ts` 排序的周线 DataFrame。
    用途：
    1. 以临时表 + JOIN 的方式批量读取周线，避免逐股查询。
    边界条件：
    1. `codes` 为空时返回空表。
    """
    if not codes:
        return pd.DataFrame()

    with connect_source_readonly(source_db_path) as con:
        con.execute("create temp table _tmp_codes (code varchar)")
        con.execute("insert into _tmp_codes(code) select unnest($1)", [codes])
        frame = con.execute(
            """
            select
                t.code,
                t.datetime as week_ts,
                t.open,
                t.high,
                t.low,
                t.close,
                t.volume
            from klines_w t
            join _tmp_codes c on c.code = t.code
            where t.datetime >= ? and t.datetime <= ?
            order by t.code, t.datetime
            """,
            [
                datetime.combine(start_day, datetime.min.time()),
                datetime.combine(end_day, datetime.max.time()),
            ],
        ).fetchdf()
    return frame


def _load_daily_bars(
    *,
    source_db_path: Path,
    codes: list[str],
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    """批量加载日线 OHLCV 数据。

    输入：
    1. source_db_path: DuckDB 源库路径。
    2. codes: 待扫描股票代码列表。
    3. start_day/end_day: 日期范围边界。
    输出：
    1. 返回按 `code + day_ts` 排序的日线 DataFrame。
    用途：
    1. 为最新日线贴近 MA20 的过滤条件提供原始数据。
    边界条件：
    1. `codes` 为空时返回空表。
    """
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
            [
                datetime.combine(start_day, datetime.min.time()),
                datetime.combine(end_day, datetime.max.time()),
            ],
        ).fetchdf()
    return frame


def _prepare_weekly_features(frame: pd.DataFrame, weekly_params: dict[str, Any]) -> pd.DataFrame:
    """计算周线 MA 和四周斜率特征。

    输入：
    1. frame: 全部股票的周线原始 DataFrame。
    2. weekly_params: 已规范化的周线参数。
    输出：
    1. 返回新增 `ma10`、`ma20`、`ma10_slope`、`ma20_slope` 列后的 DataFrame。
    用途：
    1. 为后续逐股扫描提供统一特征列。
    边界条件：
    1. 空表直接原样返回。
    2. 斜率列需要同时满足当前 MA 和 N 周前 MA 均已就绪才会有值。
    """
    if frame.empty:
        return frame

    ma10_period = int(weekly_params["ma10_period"])
    ma20_period = int(weekly_params["ma20_period"])
    slope_gap = int(weekly_params["slope_gap_weeks"])

    prepared: list[pd.DataFrame] = []
    for _, code_frame in frame.groupby("code", sort=False):
        part = code_frame.sort_values("week_ts").copy()
        part["ma10"] = part["close"].rolling(ma10_period, min_periods=ma10_period).mean()
        part["ma20"] = part["close"].rolling(ma20_period, min_periods=ma20_period).mean()
        part["ma10_slope"] = (part["ma10"] - part["ma10"].shift(slope_gap)) / float(slope_gap)
        part["ma20_slope"] = (part["ma20"] - part["ma20"].shift(slope_gap)) / float(slope_gap)
        prepared.append(part)
    return pd.concat(prepared, axis=0, ignore_index=True)


def _prepare_daily_features(frame: pd.DataFrame, daily_params: dict[str, Any]) -> pd.DataFrame:
    """计算日线 MA20 特征。

    输入：
    1. frame: 全部股票的日线原始 DataFrame。
    2. daily_params: 已规范化的日线参数。
    输出：
    1. 返回新增 `ma20` 列后的日线 DataFrame。
    用途：
    1. 为最新日线贴近 MA20 的过滤条件提供特征列。
    边界条件：
    1. 空表直接原样返回。
    """
    if frame.empty:
        return frame

    ma20_period = int(daily_params["ma20_period"])
    prepared: list[pd.DataFrame] = []
    for _, code_frame in frame.groupby("code", sort=False):
        part = code_frame.sort_values("day_ts").copy()
        part["ma20"] = part["close"].rolling(ma20_period, min_periods=ma20_period).mean()
        prepared.append(part)
    return pd.concat(prepared, axis=0, ignore_index=True)


def _window_slopes_in_range(
    series: pd.Series,
    recent_windows: int,
    window_size: int,
    lower: float,
    upper: float,
) -> tuple[bool, list[float]]:
    """检查最近若干个不重叠窗口的斜率是否全部落在目标区间。

    输入：
    1. series: 斜率序列。
    2. recent_windows: 需要检查的最近窗口数。
    3. window_size: 相邻窗口端点之间的间隔周数。
    4. lower/upper: 允许区间边界。
    输出：
    1. 返回 `(是否全部命中, 最近窗口斜率列表)`。
    用途：
    1. 统一处理 MA10 与 MA20 最近 N 个不重叠窗口斜率的区间检查。
    边界条件：
    1. 斜率有效值不足以支撑 `recent_windows` 个窗口时直接返回 False。
    """
    valid = series.dropna()
    required_points = 1 + (recent_windows - 1) * window_size
    if len(valid) < required_points:
        return False, []
    sampled: list[float] = []
    for offset in range(recent_windows):
        sampled.append(float(valid.iloc[-1 - offset * window_size]))
    sampled.reverse()
    passed = all(lower <= value <= upper for value in sampled)
    return passed, sampled


def _format_pct(value: float) -> str:
    """把比例值格式化为百分比字符串。

    输入：
    1. value: 0 到 1 之间的比例值。
    输出：
    1. 返回保留两位小数的百分比字符串。
    用途：
    1. 统一信号标签和 payload 中的百分比展示格式。
    边界条件：
    1. 超出常规范围的值也按普通数字格式化，不做异常抛出。
    """
    return f"{value * 100:.2f}%"


def _build_signal_label(*, daily_deviation: float, ma10_recent: list[float], ma20_recent: list[float]) -> str:
    """构建更直观的信号标签。

    输入：
    1. daily_deviation: 最新日线收盘价相对日线 MA20 的偏离比例。
    2. ma10_recent: 最近命中检查使用的 MA10 窗口斜率列表。
    3. ma20_recent: 最近命中检查使用的 MA20 窗口斜率列表。
    输出：
    1. 返回用于前端结果列表展示的信号标签。
    用途：
    1. 在不修改框架的前提下，把关键判据直接展示到前端列表。
    边界条件：
    1. 斜率列表理论上不为空；若为空则显示为 `n/a`。
    """
    ma10_latest = f"{ma10_recent[-1]:.2f}" if ma10_recent else "n/a"
    ma20_latest = f"{ma20_recent[-1]:.2f}" if ma20_recent else "n/a"
    return f"慢牛 | d偏离{_format_pct(daily_deviation)} | 10斜率{ma10_latest} | 20斜率{ma20_latest}"


def _build_signal_payload(
    code_frame: pd.DataFrame,
    daily_frame: pd.DataFrame,
    weekly_params: dict[str, Any],
    daily_params: dict[str, Any],
    ma10_recent: list[float],
    ma20_recent: list[float],
    daily_deviation: float,
) -> dict[str, Any]:
    """构建标准化信号 payload。

    输入：
    1. code_frame: 当前股票的周线特征数据。
    2. daily_frame: 当前股票的日线特征数据。
    3. weekly_params: 已规范化的周线参数。
    4. daily_params: 已规范化的日线参数。
    5. ma10_recent: 最近命中检查使用的 MA10 窗口斜率列表。
    6. ma20_recent: 最近命中检查使用的 MA20 窗口斜率列表。
    7. daily_deviation: 最新日线收盘价相对日线 MA20 的偏离比例。
    输出：
    1. 返回可直接写入结果表的 payload 字典。
    用途：
    1. 记录这次信号使用的窗口、阈值和最近斜率明细，便于前端与排查。
    边界条件：
    1. 展示窗口长度受 `signal_window_bars` 控制，但不会早于真实可用历史。
    """
    latest = code_frame.iloc[-1]
    latest_daily = daily_frame.iloc[-1]
    signal_window_bars = int(weekly_params["signal_window_bars"])
    required_bars = _required_history_bars(weekly_params)
    window_bars = max(signal_window_bars, required_bars)
    window_start_index = max(len(code_frame) - window_bars, 0)
    window_start_ts = code_frame.iloc[window_start_index]["week_ts"]
    window_end_ts = latest["week_ts"]
    signal_summary = _build_signal_label(
        daily_deviation=daily_deviation,
        ma10_recent=ma10_recent,
        ma20_recent=ma20_recent,
    )
    return {
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "chart_interval_start_ts": window_start_ts,
        "chart_interval_end_ts": window_end_ts,
        "anchor_day_ts": latest["week_ts"],
        "ma10": round(float(latest["ma10"]), 6),
        "ma20": round(float(latest["ma20"]), 6),
        "ma10_recent_window_slopes": [round(v, 6) for v in ma10_recent],
        "ma20_recent_window_slopes": [round(v, 6) for v in ma20_recent],
        "slope_gap_weeks": int(weekly_params["slope_gap_weeks"]),
        "ma10_recent_windows": int(weekly_params["ma10_recent_windows"]),
        "ma10_slope_min": float(weekly_params["ma10_slope_min"]),
        "ma10_slope_max": float(weekly_params["ma10_slope_max"]),
        "ma20_recent_windows": int(weekly_params["ma20_recent_windows"]),
        "ma20_slope_min": float(weekly_params["ma20_slope_min"]),
        "ma20_slope_max": float(weekly_params["ma20_slope_max"]),
        "latest_close": round(float(latest["close"]), 6),
        "latest_daily_close": round(float(latest_daily["close"]), 6),
        "latest_daily_ma20": round(float(latest_daily["ma20"]), 6),
        "daily_close_to_ma20_pct": round(daily_deviation, 6),
        "daily_close_to_ma20_pct_text": _format_pct(daily_deviation),
        "daily_ma20_period": int(daily_params["ma20_period"]),
        "daily_close_to_ma20_limit_pct": float(daily_params["close_to_ma20_pct"]),
        "daily_close_to_ma20_limit_pct_text": _format_pct(float(daily_params["close_to_ma20_pct"])),
        "signal_summary": signal_summary,
    }


def _scan_one_code(
    *,
    code: str,
    name: str,
    weekly_frame: pd.DataFrame,
    daily_frame: pd.DataFrame,
    weekly_params: dict[str, Any],
    daily_params: dict[str, Any],
    strategy_group_id: str,
    strategy_name: str,
) -> tuple[StockScanResult, dict[str, int]]:
    """扫描单只股票的周线慢牛条件。

    输入：
    1. code/name: 当前股票标识。
    2. weekly_frame: 当前股票的周线特征切片。
    3. daily_frame: 当前股票的日线特征切片。
    4. weekly_params: 已规范化的周线参数。
    5. daily_params: 已规范化的日线参数。
    6. strategy_group_id/strategy_name: 构建标准信号时使用。
    输出：
    1. 返回 `(扫描结果, 统计信息)`。
    用途：
    1. 在最新一根周线处，按不重叠窗口检查 MA10 与 MA20 斜率条件，并同时检查最新日线收盘是否贴近日线 MA20。
    边界条件：
    1. 历史不足、最近斜率缺值或日线 MA20 尚未形成时直接返回无信号。
    """
    stats = {
        "processed_bars": len(weekly_frame),
        "processed_daily_bars": len(daily_frame),
        "candidate_count": 0,
        "kept_count": 0,
    }

    if weekly_frame.empty or daily_frame.empty:
        return StockScanResult(code=code, name=name, processed_bars=0, signal_count=0, signals=[]), stats

    required_bars = _required_history_bars(weekly_params)
    if len(weekly_frame) < required_bars:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=len(weekly_frame),
            signal_count=0,
            signals=[],
        ), stats

    ma10_passed, ma10_recent = _window_slopes_in_range(
        weekly_frame["ma10_slope"],
        int(weekly_params["ma10_recent_windows"]),
        int(weekly_params["slope_gap_weeks"]),
        float(weekly_params["ma10_slope_min"]),
        float(weekly_params["ma10_slope_max"]),
    )
    ma20_passed, ma20_recent = _window_slopes_in_range(
        weekly_frame["ma20_slope"],
        int(weekly_params["ma20_recent_windows"]),
        int(weekly_params["slope_gap_weeks"]),
        float(weekly_params["ma20_slope_min"]),
        float(weekly_params["ma20_slope_max"]),
    )

    latest_daily = daily_frame.sort_values("day_ts").iloc[-1]
    if pd.isna(latest_daily["ma20"]) or float(latest_daily["ma20"]) <= 0:
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=len(weekly_frame),
            signal_count=0,
            signals=[],
        ), stats
    daily_deviation = abs(float(latest_daily["close"]) - float(latest_daily["ma20"])) / float(latest_daily["ma20"])
    daily_passed = daily_deviation <= float(daily_params["close_to_ma20_pct"])

    stats["candidate_count"] = 1
    if not (ma10_passed and ma20_passed and daily_passed):
        return StockScanResult(
            code=code,
            name=name,
            processed_bars=len(weekly_frame),
            signal_count=0,
            signals=[],
        ), stats

    payload = _build_signal_payload(
        code_frame=weekly_frame,
        daily_frame=daily_frame,
        weekly_params=weekly_params,
        daily_params=daily_params,
        ma10_recent=ma10_recent,
        ma20_recent=ma20_recent,
        daily_deviation=daily_deviation,
    )
    signal_label = _build_signal_label(
        daily_deviation=daily_deviation,
        ma10_recent=ma10_recent,
        ma20_recent=ma20_recent,
    )
    latest = weekly_frame.iloc[-1]
    signal = build_signal_dict(
        code=code,
        name=name,
        signal_dt=latest["week_ts"],
        clock_tf="w",
        strategy_group_id=strategy_group_id,
        strategy_name=strategy_name,
        signal_label=signal_label,
        payload=payload,
    )
    stats["kept_count"] = 1
    return StockScanResult(
        code=code,
        name=name,
        processed_bars=len(weekly_frame),
        signal_count=1,
        signals=[signal],
    ), stats


def run_weekly_slow_bull_v1_specialized(
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
    """`weekly_slow_bull_v1` specialized 主入口。

    输入：
    1. 所有参数由 TaskManager 以关键字方式传入。
    输出：
    1. 返回 `(result_map, metrics)`，供 TaskManager 写库和前端展示使用。
    用途：
    1. 组织参数规范化、周线批量加载、逐股扫描和汇总统计。
    边界条件：
    1. `codes` 可能为空，需返回空结果。
    2. 当前策略不使用缓存，但保留 `cache_scope/cache_dir` 参数以兼容框架签名。
    """
    _ = (cache_scope, cache_dir)

    weekly_params = _normalize_weekly_params(group_params)
    daily_params = _normalize_daily_params(group_params)
    execution_params = _normalize_execution_params(group_params)
    universe_filter_params = _read_universe_filter_params(group_params)

    empty_metrics: dict[str, Any] = {
        "weekly_phase_sec": 0.0,
        "daily_phase_sec": 0.0,
        "scan_phase_sec": 0.0,
        "total_weekly_rows": 0,
        "total_daily_rows": 0,
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
        return {}, empty_metrics

    end_dt = end_ts or datetime.now()
    required_bars = _required_history_bars(weekly_params)
    screen_span_days = max(
        int(weekly_params["signal_window_bars"]) * 7 + 60,
        required_bars * 7 + 60,
        int(daily_params["ma20_period"]) + 120,
        360,
    )
    start_day = start_ts.date() if start_ts is not None else (end_dt - timedelta(days=screen_span_days)).date()
    end_day = end_dt.date()

    weekly_phase_start = time.perf_counter()
    weekly_raw = _load_weekly_bars(
        source_db_path=Path(source_db_path),
        codes=codes,
        start_day=start_day,
        end_day=end_day,
    )
    weekly_phase_sec = time.perf_counter() - weekly_phase_start

    daily_lookback_days = max(int(daily_params["ma20_period"]) + 30, 60)
    daily_min_start_day = end_day - timedelta(days=daily_lookback_days)
    daily_start_day = start_day if start_day <= daily_min_start_day else daily_min_start_day
    daily_phase_start = time.perf_counter()
    daily_raw = _load_daily_bars(
        source_db_path=Path(source_db_path),
        codes=codes,
        start_day=daily_start_day,
        end_day=end_day,
    )
    daily_phase_sec = time.perf_counter() - daily_phase_start

    if weekly_raw.empty or daily_raw.empty:
        empty_metrics["weekly_phase_sec"] = round(weekly_phase_sec, 4)
        empty_metrics["daily_phase_sec"] = round(daily_phase_sec, 4)
        return {}, empty_metrics

    weekly_ready = _prepare_weekly_features(weekly_raw, weekly_params)
    daily_ready = _prepare_daily_features(daily_raw, daily_params)
    per_code = {
        code: frame.copy()
        for code, frame in weekly_ready.groupby("code", sort=False)
    }
    per_code_daily = {
        code: frame.copy()
        for code, frame in daily_ready.groupby("code", sort=False)
    }

    result_map: dict[str, StockScanResult] = {}
    total_weekly_rows = 0
    total_daily_rows = 0
    candidate_count = 0
    kept_count = 0
    codes_with_signal = 0
    stock_errors = 0

    scan_phase_start = time.perf_counter()
    for code in codes:
        name = code_to_name.get(code, "")
        code_frame = per_code.get(code)
        code_daily_frame = per_code_daily.get(code)
        if code_frame is None or code_frame.empty or code_daily_frame is None or code_daily_frame.empty:
            continue

        try:
            scan_result, stats = _scan_one_code(
                code=code,
                name=name,
                weekly_frame=code_frame,
                daily_frame=code_daily_frame,
                weekly_params=weekly_params,
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

        total_weekly_rows += stats["processed_bars"]
        total_daily_rows += stats["processed_daily_bars"]
        candidate_count += stats["candidate_count"]
        kept_count += stats["kept_count"]

        if scan_result.signal_count > 0:
            codes_with_signal += 1

        if scan_result.signal_count > 0 or scan_result.error_message:
            result_map[code] = scan_result

    scan_phase_sec = time.perf_counter() - scan_phase_start

    metrics: dict[str, Any] = {
        "weekly_phase_sec": round(weekly_phase_sec, 4),
        "daily_phase_sec": round(daily_phase_sec, 4),
        "scan_phase_sec": round(scan_phase_sec, 4),
        "total_weekly_rows": total_weekly_rows,
        "total_daily_rows": total_daily_rows,
        "total_codes": len(codes),
        "candidate_count": candidate_count,
        "kept_count": kept_count,
        "codes_with_signal": codes_with_signal,
        "stock_errors": stock_errors,
        "execution_fallback_to_backtrader": execution_params["fallback_to_backtrader"],
        "slope_gap_weeks": int(weekly_params["slope_gap_weeks"]),
        "ma10_recent_windows": int(weekly_params["ma10_recent_windows"]),
        "ma10_slope_min": float(weekly_params["ma10_slope_min"]),
        "ma10_slope_max": float(weekly_params["ma10_slope_max"]),
        "ma20_recent_windows": int(weekly_params["ma20_recent_windows"]),
        "ma20_slope_min": float(weekly_params["ma20_slope_min"]),
        "ma20_slope_max": float(weekly_params["ma20_slope_max"]),
        "daily_ma20_period": int(daily_params["ma20_period"]),
        "daily_close_to_ma20_pct": float(daily_params["close_to_ma20_pct"]),
    }
    return result_map, metrics
