"""
`strategy_2` backtrader handlers（specialized 模板的协议占位文件）。

这份文件的核心目的不是承载策略逻辑，而是向后续 AI 明确三件事：
1. `strategy_2` 的主执行路径在 `engine.py`，不是这里。
2. specialized 策略仍然需要导出 `GROUP_HANDLERS`，以满足框架的 backtrader 协议要求。
3. 只有在明确启用 `fallback_to_backtrader=true` 且你真的实现了回退逻辑时，才应该把这里从占位实现改成真实规则。

默认情况下，复制模板创建新策略后，这个文件通常不需要改动。
"""

from __future__ import annotations

from typing import Any


def _disabled_result(tf_label: str) -> dict[str, Any]:
    """返回一个统一的“当前周期规则未启用”结构。

    这个结构用于告诉 backtrader 路径：当前策略实际走 specialized 主逻辑，
    因此这里的各周期规则函数只是协议占位，而不是业务判断入口。
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
    1. 返回 disabled 结果，表示 specialized 策略未启用该 backtrader 周期规则。
    """
    _ = (strategy, group_params)
    return _disabled_result("w")


def eval_d(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """日线占位规则，语义同 `eval_w()`。"""
    _ = (strategy, group_params)
    return _disabled_result("d")


def eval_60(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """60 分钟占位规则，语义同 `eval_w()`。"""
    _ = (strategy, group_params)
    return _disabled_result("60")


def eval_30(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """30 分钟占位规则，语义同 `eval_w()`。"""
    _ = (strategy, group_params)
    return _disabled_result("30")


def eval_15(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    """15 分钟占位规则，语义同 `eval_w()`。"""
    _ = (strategy, group_params)
    return _disabled_result("15")


def eval_combo(
    strategy: Any,
    per_rule_results: dict[str, dict[str, Any]],
    group_params: dict[str, Any],
) -> dict[str, Any]:
    """组合规则占位实现。

    对 specialized 模板来说，这里明确返回“不产出 backtrader 信号”。
    如果后续 AI 真的要支持 backtrader 回退，应把真实组合规则写在这里。
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
    """返回 specialized 模板的默认信号标签。

    仅当 backtrader 回退路径真的被启用时，这个标签才会被使用。
    默认模板下，它的主要作用是维持协议完整。
    """
    _ = (strategy, per_rule_results, combo_result, group_params)
    return "策略2（specialized 模板）"


GROUP_HANDLERS = {
    "eval_w": eval_w,
    "eval_d": eval_d,
    "eval_60": eval_60,
    "eval_30": eval_30,
    "eval_15": eval_15,
    "eval_combo": eval_combo,
    "build_signal_label": build_signal_label,
}
