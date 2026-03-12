"""
`bu_zhi_dao_v1` 策略协议壳文件。

职责：
1. 提供 `GROUP_HANDLERS` 协议占位，满足统一注册接口。
2. 明确该策略默认主路径是 specialized 引擎。
3. 在 fallback 场景下返回可解释的占位语义。
"""

from __future__ import annotations

from typing import Any


def _placeholder(tf_label: str) -> dict[str, Any]:
    """返回占位规则结果：明确提示该策略主路径是 specialized。"""

    return {
        "passed": False,
        "enabled": False,
        "ready": False,
        "message": f"{tf_label} 规则未启用；`bu_zhi_dao_v1` 默认走 specialized 引擎",
        "values": {},
    }


def eval_w(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """
    输入：
    1. strategy: 输入参数，具体约束以调用方和实现为准。
    2. group_params: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `eval_w` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    _ = (strategy, group_params)
    return _placeholder("w")


def eval_d(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """
    输入：
    1. strategy: 输入参数，具体约束以调用方和实现为准。
    2. group_params: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `eval_d` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    _ = (strategy, group_params)
    return _placeholder("d")


def eval_60(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """
    输入：
    1. strategy: 输入参数，具体约束以调用方和实现为准。
    2. group_params: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `eval_60` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    _ = (strategy, group_params)
    return _placeholder("60")


def eval_30(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """
    输入：
    1. strategy: 输入参数，具体约束以调用方和实现为准。
    2. group_params: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `eval_30` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    _ = (strategy, group_params)
    return _placeholder("30")


def eval_15(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """
    输入：
    1. strategy: 输入参数，具体约束以调用方和实现为准。
    2. group_params: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `eval_15` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    _ = (strategy, group_params)
    return _placeholder("15")


def eval_combo(
    strategy: Any,
    per_rule_results: dict[str, dict[str, Any]],
    group_params: dict[str, Any],
) -> dict[str, Any]:
    """
    输入：
    1. strategy: 输入参数，具体约束以调用方和实现为准。
    2. per_rule_results: 输入参数，具体约束以调用方和实现为准。
    3. group_params: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `eval_combo` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    _ = (strategy, per_rule_results, group_params)
    return {
        "passed": False,
        "enabled": False,
        "ready": False,
        "message": "该策略主路径为 specialized；backtrader 组合层不产出信号",
        "values": {},
    }


def build_signal_label(
    strategy: Any,
    per_rule_results: dict[str, dict[str, Any]],
    combo_result: dict[str, Any],
    group_params: dict[str, Any],
) -> str:
    """
    输入：
    1. strategy: 输入参数，具体约束以调用方和实现为准。
    2. per_rule_results: 输入参数，具体约束以调用方和实现为准。
    3. combo_result: 输入参数，具体约束以调用方和实现为准。
    4. group_params: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `build_signal_label` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    _ = (strategy, per_rule_results, combo_result, group_params)
    return "不知道"


GROUP_HANDLERS = {
    "eval_w": eval_w,
    "eval_d": eval_d,
    "eval_60": eval_60,
    "eval_30": eval_30,
    "eval_15": eval_15,
    "eval_combo": eval_combo,
    "build_signal_label": build_signal_label,
}

