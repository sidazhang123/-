"""
`burst_pullback_box_v1` 策略协议壳文件。

职责：
1. 暴露统一 `GROUP_HANDLERS` 供注册中心加载。
2. 标识 specialized 为主路径，backtrader 仅作回退语义。
3. 在回退时保证返回结构稳定，便于任务链路统一处理。
"""

from __future__ import annotations

"""
burst_pullback_box_v1 的策略组壳文件。

说明：
1. 本策略组默认由 specialized 引擎执行，核心逻辑在
   `strategies/groups/burst_pullback_box_v1/engine.py`。
2. 这里仍按统一协议暴露 GROUP_HANDLERS，便于框架注册与降级兜底。
3. 当 specialized 引擎不可用且允许 fallback 时，系统可能走 backtrader 路径；
   本文件在 backtrader 下只提供“不可判定”语义，不承诺与 specialized 完全一致。
"""

from typing import Any


def _disabled_rule(message: str) -> dict[str, Any]:
    """返回“规则禁用”结果，统一结构。"""

    return {
        "passed": True,
        "enabled": False,
        "ready": True,
        "message": message,
        "values": {},
    }


def eval_w(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """周线在该策略中不参与判定。"""

    _ = (strategy, group_params)
    return _disabled_rule("该策略组仅使用 d/15m，周线规则禁用")


def eval_60(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """60 分钟在该策略中不参与判定。"""

    _ = (strategy, group_params)
    return _disabled_rule("该策略组仅使用 d/15m，60m 规则禁用")


def eval_30(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """30 分钟在该策略中不参与判定。"""

    _ = (strategy, group_params)
    return _disabled_rule("该策略组仅使用 d/15m，30m 规则禁用")


def eval_d(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """backtrader 降级路径下，日线规则不在此实现。"""

    _ = (strategy, group_params)
    return {
        "passed": False,
        "enabled": True,
        "ready": False,
        "message": "burst_pullback_box_v1 默认走 specialized 引擎，backtrader 仅作降级兜底",
        "values": {},
    }


def eval_15(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """backtrader 降级路径下，15m 规则不在此实现。"""

    _ = (strategy, group_params)
    return {
        "passed": False,
        "enabled": True,
        "ready": False,
        "message": "burst_pullback_box_v1 默认走 specialized 引擎，backtrader 仅作降级兜底",
        "values": {},
    }


def eval_combo(
    strategy: Any,
    per_rule_results: dict[str, dict[str, Any]],
    group_params: dict[str, Any],
) -> dict[str, Any]:
    """降级路径的组合规则说明。"""

    _ = (strategy, per_rule_results, group_params)
    return {
        "passed": False,
        "enabled": True,
        "ready": False,
        "message": "specialized 引擎不可用时可降级 backtrader，但该路径不保证与 specialized 完全一致",
        "values": {},
    }


def build_signal_label(
    strategy: Any,
    per_rule_results: dict[str, dict[str, Any]],
    combo_result: dict[str, Any],
    group_params: dict[str, Any],
) -> str:
    """信号标签统一返回策略组名称，前端只展示组合策略名。"""

    _ = (strategy, per_rule_results, combo_result, group_params)
    return getattr(strategy, "strategy_name", "放量冲高回落箱体 v1")


GROUP_HANDLERS = {
    "eval_w": eval_w,
    "eval_d": eval_d,
    "eval_60": eval_60,
    "eval_30": eval_30,
    "eval_15": eval_15,
    "eval_combo": eval_combo,
    "build_signal_label": build_signal_label,
}
