"""
`handi_bacong_v1` backtrader handlers（specialized 策略的协议占位文件）。

该策略的主执行路径在 `engine.py`，本文件仅满足框架的 backtrader 协议要求。
"""

from __future__ import annotations

from typing import Any


def _disabled_result(tf_label: str) -> dict[str, Any]:
    """返回统一的"当前周期规则未启用"结构。

    输入：
    1. tf_label: 周期标识字符串。
    输出：
    1. 返回 disabled 结果字典。
    """
    return {
        "passed": True,
        "enabled": False,
        "ready": True,
        "message": f"{tf_label} 规则禁用：该策略组使用 specialized 引擎",
        "values": {},
    }


def eval_w(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """周线占位规则。

    输入：
    1. strategy: backtrader 策略实例，占位实现不使用。
    2. group_params: 运行参数，占位实现不使用。
    输出：
    1. 返回 disabled 结果。
    """
    _ = (strategy, group_params)
    return _disabled_result("w")


def eval_d(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """日线占位规则。

    输入：
    1. strategy: backtrader 策略实例，占位实现不使用。
    2. group_params: 运行参数，占位实现不使用。
    输出：
    1. 返回 disabled 结果。
    """
    _ = (strategy, group_params)
    return _disabled_result("d")


def eval_60(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """60 分钟占位规则。

    输入：
    1. strategy: backtrader 策略实例，占位实现不使用。
    2. group_params: 运行参数，占位实现不使用。
    输出：
    1. 返回 disabled 结果。
    """
    _ = (strategy, group_params)
    return _disabled_result("60")


def eval_30(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """30 分钟占位规则。

    输入：
    1. strategy: backtrader 策略实例，占位实现不使用。
    2. group_params: 运行参数，占位实现不使用。
    输出：
    1. 返回 disabled 结果。
    """
    _ = (strategy, group_params)
    return _disabled_result("30")


def eval_15(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """15 分钟占位规则。

    输入：
    1. strategy: backtrader 策略实例，占位实现不使用。
    2. group_params: 运行参数，占位实现不使用。
    输出：
    1. 返回 disabled 结果。
    """
    _ = (strategy, group_params)
    return _disabled_result("15")


def eval_combo(
    strategy: Any,
    per_rule_results: dict[str, dict[str, Any]],
    group_params: dict[str, Any],
) -> dict[str, Any]:
    """组合规则占位实现。

    输入：
    1. strategy: backtrader 策略实例。
    2. per_rule_results: 各周期规则结果。
    3. group_params: 运行参数。
    输出：
    1. 返回不产出信号的 combo 结果。
    """
    _ = (strategy, per_rule_results, group_params)
    return {
        "passed": False,
        "enabled": True,
        "ready": True,
        "message": "该策略组使用 specialized 引擎，backtrader combo 不产出信号",
        "values": {},
    }


def build_signal_label(
    strategy: Any,
    per_rule_results: dict[str, dict[str, Any]],
    combo_result: dict[str, Any],
    group_params: dict[str, Any],
) -> str:
    """返回默认信号标签。

    输入：
    1. strategy/per_rule_results/combo_result/group_params: 框架标准参数，占位不使用。
    输出：
    1. 返回策略标签字符串。
    """
    _ = (strategy, per_rule_results, combo_result, group_params)
    return "旱地拔葱 v1"


GROUP_HANDLERS = {
    "eval_w": eval_w,
    "eval_d": eval_d,
    "eval_60": eval_60,
    "eval_30": eval_30,
    "eval_15": eval_15,
    "eval_combo": eval_combo,
    "build_signal_label": build_signal_label,
}
