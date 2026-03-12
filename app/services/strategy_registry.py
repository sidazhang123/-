"""
策略注册中心。

职责：
1. 扫描 `strategies/groups/*/manifest.json`，发现可用策略组。
2. 解析并校验策略元信息（引擎类型、执行参数、默认参数与说明）。
3. 动态加载策略处理器与专用引擎入口，向任务系统提供统一运行时对象。

设计目标：
1. 把“策略定义”与“任务调度框架”解耦。
2. 新增策略时尽量只新增目录文件，避免改动框架代码。
"""

from __future__ import annotations

import copy
import importlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

from app.settings import STRATEGY_GROUPS_DIR, TIMEFRAME_ORDER


class StrategyRegistryError(RuntimeError):
    """策略定义、参数或动态加载失败时抛出的统一异常。"""


SpecializedRunner = Callable[..., tuple[dict[str, Any], dict[str, Any]]]


@dataclass(frozen=True)
class StrategyExecutionMeta:
    """策略执行侧元信息。"""

    requires_time_window: bool = False
    supports_intra_task_parallel: bool = False
    cache_scope: str = "none"
    specialized_entry: str | None = None
    required_timeframes: tuple[str, ...] = ()


@dataclass(frozen=True)
class StrategyGroupMeta:
    """单个策略组的 manifest 元信息。"""

    group_id: str
    name: str
    description: str
    module_path: str
    default_params: dict[str, Any]
    param_help: dict[str, Any] | None = None
    engine: str = "backtrader"
    execution: StrategyExecutionMeta = field(default_factory=StrategyExecutionMeta)


@dataclass(frozen=True)
class StrategyGroupRuntime:
    """backtrader 执行链使用的 handlers 运行时对象。"""

    meta: StrategyGroupMeta
    per_tf_handlers: dict[str, Callable[[Any, dict[str, Any]], dict[str, Any]]]
    combo_handler: Callable[[Any, dict[str, dict[str, Any]], dict[str, Any]], dict[str, Any]]
    signal_label_builder: Callable[[Any, dict[str, dict[str, Any]], dict[str, Any], dict[str, Any]], str]


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """
    输入：
    1. base: 输入参数，具体约束以调用方和实现为准。
    2. override: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `_deep_merge` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


class StrategyRegistry:
    REQUIRED_KEYS = ["eval_w", "eval_d", "eval_60", "eval_30", "eval_15", "eval_combo"]
    VALID_ENGINES = {"backtrader", "specialized"}
    VALID_CACHE_SCOPE = {"none", "daily_candidates", "full_pipeline"}
    VALID_TIMEFRAMES = set(TIMEFRAME_ORDER)

    def __init__(self, groups_dir: Path = STRATEGY_GROUPS_DIR):
        """
        输入：
        1. groups_dir: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `__init__` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        self.groups_dir = groups_dir

    def _manifest_paths(self) -> list[Path]:
        """支持两种布局：
        1) 新布局：strategies/groups/<strategy_id>/manifest.json
        2) 旧布局：strategies/groups/*.manifest.json（兼容读取）
        """

        if not self.groups_dir.exists():
            return []
        paths = list(self.groups_dir.glob("*/manifest.json"))
        paths.extend(self.groups_dir.glob("*.manifest.json"))
        # 去重并按路径稳定排序，保证输出顺序稳定。
        return sorted({path.resolve(): path for path in paths}.values(), key=lambda p: str(p))

    @staticmethod
    def _parse_specialized_entry(raw_entry: Any, path: Path) -> str | None:
        """
        输入：
        1. raw_entry: 输入参数，具体约束以调用方和实现为准。
        2. path: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_parse_specialized_entry` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        if raw_entry is None:
            return None
        if not isinstance(raw_entry, str):
            raise StrategyRegistryError(f"策略组清单 execution.specialized_entry 必须是字符串: {path}")
        entry = raw_entry.strip()
        if not entry:
            return None
        if ":" not in entry:
            raise StrategyRegistryError(
                f"策略组清单 execution.specialized_entry 必须是 module:function 形式: {path}"
            )
        module_path, fn_name = entry.split(":", 1)
        if not module_path.strip() or not fn_name.strip():
            raise StrategyRegistryError(
                f"策略组清单 execution.specialized_entry 不能为空模块或函数名: {path}"
            )
        return entry

    def _parse_required_timeframes(
        self,
        *,
        raw_value: Any,
        engine: str,
        path: Path,
    ) -> tuple[str, ...]:
        """
        输入：
        1. raw_value: 输入参数，具体约束以调用方和实现为准。
        2. engine: 输入参数，具体约束以调用方和实现为准。
        3. path: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_parse_required_timeframes` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        if raw_value is None:
            # specialized 默认按 d + 15m 做时间覆盖过滤；backtrader 默认在 TaskManager 使用全周期。
            return ("d", "15") if engine == "specialized" else ()
        if not isinstance(raw_value, list):
            raise StrategyRegistryError(f"策略组清单 execution.required_timeframes 必须是数组: {path}")
        result: list[str] = []
        for item in raw_value:
            if not isinstance(item, str):
                raise StrategyRegistryError(f"策略组清单 execution.required_timeframes 只能包含字符串: {path}")
            tf = item.strip()
            if tf not in self.VALID_TIMEFRAMES:
                raise StrategyRegistryError(
                    f"策略组清单 execution.required_timeframes 含无效周期 {tf}: {path}"
                )
            result.append(tf)
        return tuple(result)

    def _load_manifest(self, path: Path) -> StrategyGroupMeta:
        """
        输入：
        1. path: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_load_manifest` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        try:
            raw = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            raise StrategyRegistryError(f"策略组清单读取失败 {path}: {exc}") from exc

        group_id = str(raw.get("id") or "").strip()
        name = str(raw.get("name") or "").strip()
        description = str(raw.get("description") or "").strip()
        module_path = str(raw.get("module") or "").strip()
        engine = str(raw.get("engine") or "backtrader").strip().lower()
        execution_raw = raw.get("execution") if raw.get("execution") is not None else {}
        default_params = raw.get("default_params")
        param_help = raw.get("param_help")

        if not group_id:
            raise StrategyRegistryError(f"策略组清单缺少 id: {path}")
        if not name:
            raise StrategyRegistryError(f"策略组清单缺少 name: {path}")
        if not description:
            raise StrategyRegistryError(f"策略组清单缺少 description: {path}")
        if not module_path:
            raise StrategyRegistryError(f"策略组清单缺少 module: {path}")
        if engine not in self.VALID_ENGINES:
            raise StrategyRegistryError(f"策略组清单 engine 不合法({engine}): {path}")
        if not isinstance(execution_raw, dict):
            raise StrategyRegistryError(f"策略组清单 execution 必须是对象: {path}")
        if not isinstance(default_params, dict):
            raise StrategyRegistryError(f"策略组清单 default_params 必须是对象: {path}")
        if param_help is not None and not isinstance(param_help, dict):
            raise StrategyRegistryError(f"策略组清单 param_help 必须是对象: {path}")

        cache_scope = str(execution_raw.get("cache_scope") or "none").strip().lower()
        if cache_scope not in self.VALID_CACHE_SCOPE:
            raise StrategyRegistryError(f"策略组清单 execution.cache_scope 不合法({cache_scope}): {path}")

        specialized_entry = self._parse_specialized_entry(execution_raw.get("specialized_entry"), path)
        if engine == "specialized" and not specialized_entry:
            raise StrategyRegistryError(f"策略组清单 specialized 策略必须声明 execution.specialized_entry: {path}")

        required_timeframes = self._parse_required_timeframes(
            raw_value=execution_raw.get("required_timeframes"),
            engine=engine,
            path=path,
        )

        return StrategyGroupMeta(
            group_id=group_id,
            name=name,
            description=description,
            module_path=module_path,
            engine=engine,
            execution=StrategyExecutionMeta(
                requires_time_window=bool(execution_raw.get("requires_time_window", False)),
                supports_intra_task_parallel=bool(execution_raw.get("supports_intra_task_parallel", False)),
                cache_scope=cache_scope,
                specialized_entry=specialized_entry,
                required_timeframes=required_timeframes,
            ),
            default_params=default_params,
            param_help=param_help,
        )

    def _all_meta(self) -> dict[str, StrategyGroupMeta]:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_all_meta` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        result: dict[str, StrategyGroupMeta] = {}
        for path in self._manifest_paths():
            meta = self._load_manifest(path)
            if meta.group_id in result:
                raise StrategyRegistryError(f"策略组 id 重复: {meta.group_id}")
            result[meta.group_id] = meta
        return result

    def list_groups(self) -> list[dict[str, Any]]:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `list_groups` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        groups = self._all_meta()
        return [self.serialize_group(meta) for _, meta in sorted(groups.items(), key=lambda item: item[0])]

    def get_group_meta(self, group_id: str) -> StrategyGroupMeta:
        """
        输入：
        1. group_id: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_group_meta` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        groups = self._all_meta()
        meta = groups.get(group_id)
        if not meta:
            raise KeyError(f"策略组不存在: {group_id}")
        return meta

    def get_group(self, group_id: str) -> dict[str, Any]:
        """
        输入：
        1. group_id: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_group` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        return self.serialize_group(self.get_group_meta(group_id))

    def merge_group_params(self, group_id: str, custom: dict[str, Any] | None) -> dict[str, Any]:
        """
        输入：
        1. group_id: 输入参数，具体约束以调用方和实现为准。
        2. custom: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `merge_group_params` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        if custom is None:
            custom = {}
        if not isinstance(custom, dict):
            raise StrategyRegistryError("group_params 必须是 JSON 对象")
        meta = self.get_group_meta(group_id)
        return _deep_merge(meta.default_params, custom)

    @staticmethod
    def _reload_module(module_path: str) -> ModuleType:
        """
        输入：
        1. module_path: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_reload_module` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        module = importlib.import_module(module_path)
        return importlib.reload(module)

    def load_runtime(self, group_id: str) -> StrategyGroupRuntime:
        """
        输入：
        1. group_id: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `load_runtime` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        meta = self.get_group_meta(group_id)
        module = self._reload_module(meta.module_path)
        handlers = getattr(module, "GROUP_HANDLERS", None)
        if not isinstance(handlers, dict):
            raise StrategyRegistryError(f"策略组模块缺少 GROUP_HANDLERS: {meta.module_path}")

        for key in self.REQUIRED_KEYS:
            fn = handlers.get(key)
            if not callable(fn):
                raise StrategyRegistryError(f"策略组模块处理函数缺失或不可调用: {meta.module_path}.{key}")

        label_builder = handlers.get("build_signal_label")
        if callable(label_builder):
            signal_label_builder = label_builder
        else:
            signal_label_builder = lambda strategy, per_rule, combo, params: meta.name

        return StrategyGroupRuntime(
            meta=meta,
            per_tf_handlers={
                "w": handlers["eval_w"],
                "d": handlers["eval_d"],
                "60": handlers["eval_60"],
                "30": handlers["eval_30"],
                "15": handlers["eval_15"],
            },
            combo_handler=handlers["eval_combo"],
            signal_label_builder=signal_label_builder,
        )

    def load_specialized_runner(self, group_id: str) -> SpecializedRunner:
        """按 manifest 的 specialized_entry 动态加载专用引擎入口函数。"""

        meta = self.get_group_meta(group_id)
        if meta.engine != "specialized":
            raise StrategyRegistryError(f"策略组 {group_id} 不是 specialized 引擎")
        entry = meta.execution.specialized_entry
        if not entry:
            raise StrategyRegistryError(f"策略组 {group_id} 未配置 execution.specialized_entry")
        module_path, fn_name = entry.split(":", 1)
        module = self._reload_module(module_path.strip())
        fn = getattr(module, fn_name.strip(), None)
        if not callable(fn):
            raise StrategyRegistryError(f"specialized 入口不可调用: {entry}")
        return fn

    @staticmethod
    def serialize_group(meta: StrategyGroupMeta) -> dict[str, Any]:
        """
        输入：
        1. meta: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `serialize_group` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        return {
            "id": meta.group_id,
            "name": meta.name,
            "description": meta.description,
            "engine": meta.engine,
            "execution": {
                "requires_time_window": meta.execution.requires_time_window,
                "supports_intra_task_parallel": meta.execution.supports_intra_task_parallel,
                "cache_scope": meta.execution.cache_scope,
                "required_timeframes": list(meta.execution.required_timeframes),
                "specialized_entry": meta.execution.specialized_entry,
            },
            "default_params": copy.deepcopy(meta.default_params),
            "param_help": copy.deepcopy(meta.param_help) if meta.param_help is not None else None,
        }

