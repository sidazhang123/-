"""
`flag_pattern_v1` backtrader handlers（specialized 引擎占位文件）。

说明：
1. 该策略默认走 `engine.py` 中的 specialized 主逻辑，而不是 backtrader 路径。
2. 当前文件仅用于满足框架对 `GROUP_HANDLERS` 的协议要求。
3. 只有在明确启用 `fallback_to_backtrader=true` 且真实实现了回退逻辑时，才应修改这里。
"""

from __future__ import annotations

from typing import Any


def _disabled_result(tf_label: str) -> dict[str, Any]:
    """返回统一的禁用结果结构。

    输入：
    1. tf_label: 周期标签，仅用于提示消息。
    输出：
    1. 返回 specialized 策略未启用 backtrader 周期规则的占位结果。
    用途：
    1. 维持框架协议完整，避免 specialized 策略缺少 backtrader handler。
    边界条件：
    1. 该函数不参与真实选股逻辑。
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
    1. 返回禁用结果。
    用途：
    1. 维持 specialized 策略的 backtrader 协议兼容性。
    边界条件：
    1. 不产生真实信号。
    """
    _ = (strategy, group_params)
    return _disabled_result("w")


def eval_d(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """日线占位规则。

    输入：
    1. strategy: backtrader 策略实例，占位实现不使用。
    2. group_params: 运行参数，占位实现不使用。
    输出：
    1. 返回禁用结果。
    边界条件：
    1. 不产生真实信号。
    """
    _ = (strategy, group_params)
    return _disabled_result("d")


def eval_60(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """60分钟占位规则。

    输入：
    1. strategy: backtrader 策略实例，占位实现不使用。
    2. group_params: 运行参数，占位实现不使用。
    输出：
    1. 返回禁用结果。
    边界条件：
    1. 不产生真实信号。
    """
    _ = (strategy, group_params)
    return _disabled_result("60")


def eval_30(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """30分钟占位规则。

    输入：
    1. strategy: backtrader 策略实例，占位实现不使用。
    2. group_params: 运行参数，占位实现不使用。
    输出：
    1. 返回禁用结果。
    边界条件：
    1. 不产生真实信号。
    """
    _ = (strategy, group_params)
    return _disabled_result("30")


def eval_15(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """15分钟占位规则。

    输入：
    1. strategy: backtrader 策略实例，占位实现不使用。
    2. group_params: 运行参数，占位实现不使用。
    输出：
    1. 返回禁用结果。
    边界条件：
    1. 不产生真实信号。
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
    1. strategy: backtrader 策略实例，占位实现不使用。
    2. per_rule_results: 各周期规则结果，占位实现不使用。
    3. group_params: 运行参数，占位实现不使用。
    输出：
    1. 返回不产出 backtrader 信号的占位结果。
    用途：
    1. 明确该策略应走 specialized 主路径。
    边界条件：
    1. 不产生真实信号。
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
    """返回回退路径使用的默认信号标签。

    输入：
    1. strategy: backtrader 策略实例，占位实现不使用。
    2. per_rule_results: 各周期规则结果，占位实现不使用。
    3. combo_result: 组合规则结果，占位实现不使用。
    4. group_params: 运行参数，占位实现不使用。
    输出：
    1. 返回固定标签字符串。
    用途：
    1. 保持协议完整。
    边界条件：
    1. 仅在回退路径真的启用时才会被框架使用。
    """
    _ = (strategy, per_rule_results, combo_result, group_params)
    return "多周期旗形上涨 v1"


GROUP_HANDLERS = {
    "eval_w": eval_w,
    "eval_d": eval_d,
    "eval_60": eval_60,
    "eval_30": eval_30,
    "eval_15": eval_15,
    "eval_combo": eval_combo,
    "build_signal_label": build_signal_label,
}
