"""
`strategy_2` specialized engine 模板。

这是后续 AI 创建新策略时最应该阅读和修改的文件。它的目标不是实现某个业务策略，
而是明确 specialized 策略的标准结构、输入输出契约和常见禁止事项。

复制模板后的推荐顺序：
1. 先改 `manifest.json` 的 `id/name/description/module/specialized_entry`。
2. 再改本文件中的 `STRATEGY_LABEL`、默认参数、参数规范化函数。
3. 实现三层函数：`detect_xxx` → `build_xxx_payload`，并在 `_scan_one_code` 中调用。
4. 只在确实需要 backtrader 回退时才去改 `strategy.py`。

三层架构（所有 specialized 策略必须遵循）：
  prepare_xxx_features(bars, params) → enriched DataFrame（可选，仅当需要预计算指标时）
  detect_xxx(bars, params)            → DetectionResult（纯检测，不含 I/O）
  build_xxx_payload(result, ...)      → dict（组装前端信号 payload）
  _scan_one_code()                    → 编排层，调用上述三层函数
此设计让 detect 层可独立用于回测/单元测试，不依赖数据库或前端结构。

强约束：
1. 主入口签名保持 keyword-only，不要改成位置参数风格。
2. 返回值始终保持 `(result_map, metrics)`。
3. `result_map` 只包含命中或异常股票，不要把所有未命中股票都塞进去。
4. 如需概念预筛选，只读取 `group_params["universe_filters"]["concepts"]` 用于记录参数；
   实际过滤由 TaskManager 在进入 engine 前执行，engine 不要重复裁剪 `codes`。
5. ST 股票过滤由 `group_params["filter_st"]["enabled"]` 控制，默认开启；
   过滤同样由 TaskManager 在进入 engine 前执行，engine 不需要处理。
6. 每个信号 payload 都必须正确填写 `chart_interval_start_ts` 和 `chart_interval_end_ts`，
   且这两个字段必须表示"单次信号"的实际展示窗口，而不是全任务跨度或多段历史总跨度。
7. 如果策略检测到可视化辅助线（趋势线、边界线、支撑/阻力线等），应通过 payload
   的 `overlay_lines` 字段传递给前端，前端会自动在 K 线图上用 ECharts markLine 渲染。
   每条线是一个 dict，必须包含 start_ts/end_ts/start_price/end_price，
   可选 color（默认 #fbbf24）、dash（默认 True）、label（默认空）。
   此机制已在 routes.py 和 results.js 中内置支持，无需额外前端改动。"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from strategies.engine_commons import (
    DetectionResult,
    StockScanResult,
    as_bool,
    as_dict,
    as_float,
    as_int,
    build_signal_dict,
    connect_source_readonly,
    normalize_execution_params,
    read_universe_filter_params,
)

# ---------------------------------------------------------------------------
# TODO: 复制模板后先改这里。这个标签会直接显示在前端结果列表里。
# 建议写成“策略名称 + 版本”风格，避免多个模板衍生策略在界面里难以区分。
# ---------------------------------------------------------------------------
STRATEGY_LABEL = "策略2（模板）"

# ---------------------------------------------------------------------------
# TODO: 把这组默认参数改成你的真实策略参数。
# 这里的结构必须和 manifest.json -> default_params 保持一致，
# 否则前端展示、参数校验和 engine 读取会出现分叉。
#
# 【多周期策略补充】若需要多个周期，添加以下结构：
# _TF_TABLE: dict[str, str] = {"w": "klines_w", "d": "klines_d", "60": "klines_60"}
# _TF_ORDER: list[str] = ["w", "d", "60"]  # 粗 → 细排序
# 并在 manifest.json 中为每个周期添加 enabled 开关与 param_help 的
# _render: "inline_template" + _label + _tf_key + _templates 模式。
# 参考示例：multi_tf_ma_uptrend_v1、consecutive_uptrends_v1。
# ---------------------------------------------------------------------------
DEFAULT_DAILY_PARAMS: dict[str, Any] = {
    "example_param": 10,
}

DEFAULT_EXECUTION_PARAMS: dict[str, Any] = {
    "fallback_to_backtrader": False,
}


# ---------------------------------------------------------------------------
# 参数规范化
# ---------------------------------------------------------------------------

def _normalize_daily_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """从 `group_params` 中提取并校验日线参数。

    输入：
    1. group_params: TaskManager 合并后的策略参数，通常来自 manifest 默认值和前端覆盖值。
    输出：
    1. 返回已经完成类型转换、默认值填充和边界裁剪的参数字典。
    用途：
    1. 把原始 JSON 风格参数转换成 engine 内部可以直接使用的安全配置。
    边界条件：
    1. 缺失、空值或类型不符的字段应通过 `as_int/as_float/as_bool/as_dict` 等工具回落到安全默认值。

    TODO: 复制模板后，把这里替换成你的真实参数规范化逻辑。
    """
    raw = as_dict(group_params.get("daily"))
    return {
        "example_param": as_int(
            raw.get("example_param"),
            DEFAULT_DAILY_PARAMS["example_param"],
            minimum=1,
            maximum=120,
        ),
    }


# ---------------------------------------------------------------------------
# 执行参数与概念过滤参数规范化已提取到 engine_commons.py：
#   normalize_execution_params() —— 标准版，仅提取 fallback_to_backtrader
#   read_universe_filter_params() —— 读取概念预筛选配置
# 如果新策略需要额外执行参数（如 worker_count），可以保留本地版本。
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 数据加载
# ---------------------------------------------------------------------------

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
    2. codes: 待查询股票代码列表。
    3. start_day/end_day: 日期边界。
    输出：
    1. 返回按 `code + datetime` 排序的 DataFrame；若 `codes` 为空则返回空表。
    用途：
    1. 演示 specialized 策略推荐的“临时表 + JOIN”批量读数模式。
    边界条件：
    1. 这里只演示日线读取；多周期策略应按同样模式补充 15/30/60/w 的加载函数。

    TODO: 根据策略需要调整 SQL 查询字段，例如添加 amount、turnover 或自定义衍生列。
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
                t.datetime as ts,
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


# ---------------------------------------------------------------------------
# 检测层：纯检测逻辑，不含 I/O，可独立用于回测和单元测试
# ---------------------------------------------------------------------------

def detect_strategy_2(
    daily_frame: pd.DataFrame,
    params: dict[str, Any],
) -> DetectionResult:
    """对单只股票的日线数据执行核心检测。

    输入：
    1. daily_frame: 单只股票的日线 DataFrame（已按时间排序）。
    2. params: 已规范化的策略参数。
    输出：
    1. DetectionResult：matched=True 表示命中；metrics 中存放诊断指标。
    边界条件：
    1. 数据不足时直接返回 matched=False。

    TODO: 复制模板后，在此实现你的核心筛选规则。
    """
    if daily_frame.empty:
        return DetectionResult(matched=False)

    # ---- 示例框架（替换为你的实际规则）----
    # example_param = params["example_param"]
    # ... 计算指标 ...
    # if condition_met:
    #     return DetectionResult(
    #         matched=True,
    #         pattern_start_idx=start_idx,
    #         pattern_end_idx=end_idx,
    #         metrics={"your_metric": value},
    #     )
    return DetectionResult(matched=False)


def detect_strategy_2_vectorized(
    daily_frame: pd.DataFrame,
    params: dict[str, Any],
) -> list[DetectionResult]:
    """向量化检测：处理完整历史，返回所有命中的 DetectionResult 列表。

    输入：
    1. daily_frame: 单只股票的完整日线 DataFrame（已按时间排序）。
    2. params: 已规范化的策略参数。
    输出：
    1. 命中的 DetectionResult 列表；无命中时返回空列表。
    用途：
    1. 回测引擎的首选执行路径，一次性扫描整段历史，避免滑窗逐 bar 调用。
    2. 缺少此函数时，回测将回退到已弃用的 detect 滑窗路径。

    TODO: 复制模板后，实现向量化检测逻辑。
    """
    return []


# ---------------------------------------------------------------------------
# 信号组装层：将检测结果转换为前端 payload
# ---------------------------------------------------------------------------

def build_strategy_2_payload(
    *,
    code: str,
    name: str,
    result: DetectionResult,
    daily_frame: pd.DataFrame,
    strategy_group_id: str,
    strategy_name: str,
) -> dict[str, Any]:
    """将 DetectionResult 组装为标准信号 payload。

    输入：
    1. result: detect_strategy_2 返回的 DetectionResult。
    2. daily_frame: 用于提取时间戳窗口。
    输出：
    1. 标准 signal dict，可直接写入 StockScanResult.signals。

    TODO: 复制模板后，根据策略需要填充 payload 字段。
    """
    # 示例：从 pattern 索引提取时间窗口
    # s_idx = result.pattern_start_idx or 0
    # e_idx = result.pattern_end_idx or (len(daily_frame) - 1)
    # window_start_ts = daily_frame.iloc[s_idx]["ts"]
    # window_end_ts = daily_frame.iloc[e_idx]["ts"]

    last_row = daily_frame.iloc[-1]
    signal_dt = last_row["ts"]

    return build_signal_dict(
        code=code,
        name=name,
        signal_dt=signal_dt,
        clock_tf="d",
        strategy_group_id=strategy_group_id,
        strategy_name=strategy_name,
        signal_label=STRATEGY_LABEL,
        payload={
            # TODO: 替换为你的实际 payload 字段
            # "window_start_ts": window_start_ts,
            # "window_end_ts": window_end_ts,
            # "chart_interval_start_ts": ...,
            # "chart_interval_end_ts": ...,
            # "overlay_lines": [],
            **result.metrics,
        },
    )


# ---------------------------------------------------------------------------
# 单股扫描（编排层）
# ---------------------------------------------------------------------------

def _scan_one_code(
    *,
    code: str,
    name: str,
    daily_frame: pd.DataFrame,
    daily_params: dict[str, Any],
    strategy_group_id: str,
    strategy_name: str,
) -> tuple[StockScanResult, dict[str, int]]:
    """编排层：调用 detect → build_payload，返回 `(扫描结果, 统计信息)`。

    输入：
    1. code/name: 当前股票标识。
    2. daily_frame: 当前股票的单独数据切片。
    3. daily_params: 已规范化的参数。
    4. strategy_group_id/strategy_name: 用于构建标准信号结构。
    输出：
    1. `StockScanResult`：当前股票的命中结果或错误结果。
    2. 统计字典：至少返回 `processed_bars`、`candidate_count`、`kept_count`。
    边界条件：
    1. 单股逻辑应允许返回 0 个信号；这不代表异常。
    2. 如果你在这里抛异常，主入口会把它转成单股错误，而不是终止整个任务。
    3. 核心检测逻辑应放在 `detect_strategy_2` 中，此处只做编排。
    """
    stats = {
        "processed_bars": len(daily_frame),
        "candidate_count": 0,
        "kept_count": 0,
    }

    signals: list[dict[str, Any]] = []

    # ---- 三层调用：detect → build_payload ----
    result = detect_strategy_2(daily_frame, daily_params)
    if result.matched:
        stats["candidate_count"] += 1
        signal = build_strategy_2_payload(
            code=code,
            name=name,
            result=result,
            daily_frame=daily_frame,
            strategy_group_id=strategy_group_id,
            strategy_name=strategy_name,
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


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run_strategy_2_specialized(
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
    """`strategy_2` specialized 主入口。

    输入：
    1. 参数全部由 TaskManager 以关键字方式传入。
    输出：
    1. 返回 `(result_map, metrics)`，供 TaskManager 写库和前端展示使用。
    用途：
    1. 组织参数规范化、批量加载、逐股扫描和汇总统计四个阶段。
    边界条件：
    1. `codes` 可能为空，也可能已被概念预筛选裁剪。
    2. 当前模板默认不使用缓存，但必须保留 `cache_scope/cache_dir` 参数以兼容框架签名。

    该函数由 TaskManager 通过 `StrategyRegistry.load_specialized_runner()` 动态加载并调用。
    """
    _ = (cache_scope, cache_dir)  # 当前模板不使用缓存，保留参数以兼容框架签名

    daily_params = _normalize_daily_params(group_params)
    execution_params = normalize_execution_params(group_params)
    universe_filter_params = read_universe_filter_params(group_params)

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
        "concept_filter_enabled": universe_filter_params["enabled"],
        "concept_terms": universe_filter_params["concept_terms"],
        "reason_terms": universe_filter_params["reason_terms"],
    }

    if not codes:
        return {}, empty_metrics

    # 注意：走到这里时，codes 可能已经被概念预筛选裁剪。
    # 不要在 engine 内再次按相同概念规则过滤，否则会造成双重筛选和统计失真。

    end_dt = end_ts or datetime.now()
    start_day = start_ts.date() if start_ts is not None else (end_dt - timedelta(days=90)).date()
    end_day = end_dt.date()

    # ---- Phase 1: 数据加载 ----
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

    # ---- Phase 2: 逐股扫描 ----
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

        # 与框架约定保持一致：仅命中或异常才进入 result_map
        if scan_result.signal_count > 0 or scan_result.error_message:
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


# ---------------------------------------------------------------------------
# 回测钩子
# ---------------------------------------------------------------------------

def _normalize_for_backtest(group_params: dict[str, Any], section_key: str) -> dict[str, Any]:
    """将前端传入的 group_params 规范化为 detect 函数所需的参数格式。

    TODO: 复制模板后，按实际检测函数签名调整返回值。
    """
    return {
        "params": _normalize_daily_params(group_params),
        "tf_key": "d",
    }


# ---------------------------------------------------------------------------
# BACKTEST_HOOKS 告诉回测引擎如何调用本策略的检测逻辑。
#
# 结构说明：
#   detect            — 核心检测函数，签名: (df, *, params, tf_key, ...) → DetectionResult
#   detect_vectorized — 向量化检测入口（推荐实现，回测的首选 CPU 路径）
#   detect_batch      — GPU 批量检测入口（可选，回测 sweep 模式的首选路径）
#   prepare           — CPU 预计算指标函数（可选，签名: (df, params) → df）
#   prepare_batch     — GPU 批量预计算函数（可选，签名: (gpu_raw, boundaries, params) → dict）
#   prepare_key       — sweep 分组键（可选，有 prepare_batch 时推荐，用于合并相同预计算的 combo）
#   normalize_params  — 将前端 group_params + section_key 转换为 detect 函数的 kwargs
#   tf_sections       — 回测覆盖的周期分段及其对应K线表名
#   tf_logic          — 多周期命中逻辑: "and"（全部命中）/ "or"（任一命中）
#
# 回测引擎调度优先级： detect_batch (GPU) > detect_vectorized (CPU) > detect (滑窗，已弃用)
#
# TODO: 复制模板后，替换 detect / prepare / tf_sections 为实际函数和周期配置。
# ---------------------------------------------------------------------------
BACKTEST_HOOKS = {
    "detect": detect_strategy_2,
    "detect_vectorized": detect_strategy_2_vectorized,
    "detect_batch": None,          # TODO: GPU 批量检测（可选，参考 weekly_oversold_rsi_v1 / consecutive_uptrends_v1）
    "prepare": None,
    "prepare_batch": None,         # TODO: GPU 批量预计算（可选，参考 weekly_oversold_rsi_v1 / multi_tf_ma_uptrend_v1）
    "prepare_key": None,           # TODO: sweep 分组键（有 prepare_batch 时推荐，参考 weekly_oversold_rsi_v1）
    "normalize_params": _normalize_for_backtest,
    "tf_sections": {
        "daily": {"tf_key": "d", "table": "klines_d"},
    },
    "tf_logic": "or",
}
