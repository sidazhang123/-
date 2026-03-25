"""
Specialized engine 公共基础设施。

职责：
1. 提供所有 specialized 引擎共用的数据类型、参数转换工具与信号构建工厂。
2. 保证新策略在 `strategy_2/engine.py` 模板基础上即可快速接入，无需重复编写样板代码。
3. 统一 TaskManager 消费侧所需的结果结构（StockScanResult）与信号字段合同。

向后兼容约定：
- 各策略可保留原有类型别名（如 `BigbroBuyStockScanResult = StockScanResult`）。
- 工具函数签名使用 keyword-only 参数以防调用侧的位置传参歧义。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

# ---------------------------------------------------------------------------
# 结果数据类型
# ---------------------------------------------------------------------------

@dataclass
class StockScanResult:
    """单股票扫描结果（供 TaskManager 统一落库）。

    所有 specialized 引擎的 run_xxx_specialized() 返回值中
    result_map 的 value 必须符合此结构。
    """

    code: str
    name: str
    processed_bars: int
    signal_count: int
    signals: list[dict[str, Any]]
    error_message: str | None = None


# ---------------------------------------------------------------------------
# 图表区间标准合同
# ---------------------------------------------------------------------------

CHART_INTERVAL_PAYLOAD_CONTRACT = {
    "chart_interval_start_ts": "required",
    "chart_interval_end_ts": "required",
    "anchor_day_ts": "optional",
}


# ---------------------------------------------------------------------------
# 参数安全转换工具
# ---------------------------------------------------------------------------

def as_dict(value: Any) -> dict[str, Any]:
    """把任意值转为 dict；非 dict 时返回空对象。"""
    return value if isinstance(value, dict) else {}


def as_bool(value: Any, default: bool) -> bool:
    """安全解析布尔参数，兼容字符串开关。"""
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


def as_int(
    value: Any,
    default: int,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    """安全解析整数参数并做边界裁剪。"""
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def as_float(
    value: Any,
    default: float,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    """安全解析浮点参数并做下界/上界裁剪。"""
    try:
        parsed = float(value)
    except Exception:
        parsed = default
    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


# ---------------------------------------------------------------------------
# DuckDB 只读连接
# ---------------------------------------------------------------------------

def connect_source_readonly(source_db_path: Path) -> duckdb.DuckDBPyConnection:
    """以只读模式连接 DuckDB 数据源。"""
    return duckdb.connect(str(source_db_path), read_only=True)


# ---------------------------------------------------------------------------
# 标准信号构建工厂
# ---------------------------------------------------------------------------

def build_signal_dict(
    *,
    code: str,
    name: str,
    signal_dt: datetime,
    clock_tf: str,
    strategy_group_id: str,
    strategy_name: str,
    signal_label: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """构建符合 TaskManager 落库要求的标准信号 dict。

    必填字段由 TaskManager._run_specialized_engine 中 add_result 消费：
    code, name, signal_dt, clock_tf, strategy_group_id, strategy_name,
    signal_label, payload.
    """
    return {
        "code": code,
        "name": name,
        "signal_dt": signal_dt,
        "clock_tf": clock_tf,
        "strategy_group_id": strategy_group_id,
        "strategy_name": strategy_name,
        "signal_label": signal_label,
        "payload": payload,
    }


# ---------------------------------------------------------------------------
# 通用参数规范化工具（各引擎共用，避免重复定义）
# ---------------------------------------------------------------------------

STANDARD_TF_ORDER: list[str] = ["w", "d", "60", "30", "15"]
"""标准周期粗细排序（粗 → 细），供多周期策略选取最粗命中周期。"""


def normalize_execution_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化执行参数（标准版：仅提取 backtrader 回退开关）。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回执行层参数字典，包含 fallback_to_backtrader 布尔值。
    用途：
    1. 供无额外执行参数的 specialized 策略复用，避免各引擎重复定义。
    边界条件：
    1. 含有自定义执行参数（如 worker_count）的策略应保留本地版本。
    """
    raw = as_dict(group_params.get("execution"))
    return {
        "fallback_to_backtrader": as_bool(raw.get("fallback_to_backtrader"), False),
    }


def read_universe_filter_params(group_params: dict[str, Any]) -> dict[str, Any]:
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


def coarsest_tf(tfs: list[str]) -> str:
    """返回给定周期列表中最粗粒度的周期。

    输入：
    1. tfs: 周期 key 列表。
    输出：
    1. 返回 STANDARD_TF_ORDER 中排序最靠前的匹配周期。
    边界条件：
    1. 列表为空时返回 "d"（日线兜底）。
    """
    for tf in STANDARD_TF_ORDER:
        if tf in tfs:
            return tf
    return "d"
