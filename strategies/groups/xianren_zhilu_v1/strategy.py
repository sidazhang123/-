"""
`xianren_zhilu_v1` backtrader handlers（specialized 引擎占位）。

说明：
1. 该策略默认走 specialized 引擎（见 engine.py），backtrader handlers 仅作协议占位。
2. 如 specialized 引擎执行失败且 manifest 配置了 fallback_to_backtrader=true，
   框架会降级到 backtrader 路径调用这些 handlers。
3. 新建策略时直接复制此文件即可，通常无需修改。
"""

from __future__ import annotations

from typing import Any


def _disabled_result(tf_label: str) -> dict[str, Any]:
    return {
        "passed": True,
        "enabled": False,
        "ready": True,
        "message": f"{tf_label} 规则禁用：该策略组使用 specialized 引擎",
        "values": {},
    }


def eval_w(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    _ = (strategy, group_params)
    return _disabled_result("w")


def eval_d(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    _ = (strategy, group_params)
    return _disabled_result("d")


def eval_60(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    _ = (strategy, group_params)
    return _disabled_result("60")


def eval_30(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    _ = (strategy, group_params)
    return _disabled_result("30")


def eval_15(strategy: Any, group_params: dict[str, Any]) -> dict[str, Any]:
    _ = (strategy, group_params)
    return _disabled_result("15")


def eval_combo(
    strategy: Any,
    per_rule_results: dict[str, dict[str, Any]],
    group_params: dict[str, Any],
) -> dict[str, Any]:
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
    _ = (strategy, per_rule_results, combo_result, group_params)
    return "仙人指路"


GROUP_HANDLERS = {
    "eval_w": eval_w,
    "eval_d": eval_d,
    "eval_60": eval_60,
    "eval_30": eval_30,
    "eval_15": eval_15,
    "eval_combo": eval_combo,
    "build_signal_label": build_signal_label,
}
