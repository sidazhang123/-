"""
API 路由层。

职责：
1. 对外暴露任务创建、任务控制、日志查询、结果查询、图表数据与前端设置保存接口。
2. 负责请求参数校验、错误语义转换与响应结构拼装。
3. 约束图表标注合同：窗口区间、标记时间与标记模式统一从后端返回。

说明：
1. 业务执行细节由 `TaskManager` 与策略引擎负责，路由层不承载策略计算。
2. 本文件是前端页面与后端任务系统之间的协议边界。
"""

from __future__ import annotations

import asyncio
import copy
import json
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.db.market_data import MarketDataDB
from app.models.api_models import (
    ConceptControlResponse,
    ConceptCreateResponse,
    ConceptLogsResponse,
    ConceptStatusResponse,
    CreateTaskRequest,
    CreateTaskResponse,
    LogsResponse,
    MaintenanceControlResponse,
    MaintenanceCreateRequest,
    MaintenanceCreateResponse,
    MaintenanceFormSettingsPayload,
    MaintenanceLogsResponse,
    MaintenanceStatusResponse,
    MonitorFormSettingsPayload,
    ResultStockConceptEntry,
    ResultStockConceptsResponse,
    ResultsResponse,
    StockStatesResponse,
    TaskControlResponse,
    TaskStatusResponse,
)
from app.services.strategy_registry import StrategyRegistryError
from app.settings import TIMEFRAME_TO_TABLE

router = APIRouter(prefix="/api")

TF_MINUTES = {
    "15": 15,
    "30": 30,
    "60": 60,
    "d": 60 * 24,
    "w": 60 * 24 * 7,
}


def _merge_signal_intervals(
    signals: list[dict[str, Any]],
    *,
    timeframe: str,
    interval_bars: int,
) -> list[dict[str, Any]]:
    """
    按时间窗口合并相邻信号。

    输入：
    1. signals: 已标准化的信号列表，每项至少包含 `signal_dt`。
    2. timeframe: 信号所属周期，用于推导单根 bar 的分钟跨度。
    3. interval_bars: 每个信号默认展开成多少根 bar 的展示区间。
    输出：
    1. 返回合并后的区间列表，每项包含 `start_dt`、`end_dt` 和归属该区间的 `signals`。
    用途：
    1. 为结果页生成更稳定的高亮区间，避免时间上紧邻的信号被拆成过多碎片窗口。
    边界条件：
    1. 空输入返回空列表。
    2. 只要新区间与前一区间重叠或相邻一个 bar，就会被并入同一组。
    """
    if not signals:
        return []

    span = timedelta(minutes=TF_MINUTES[timeframe] * max(1, interval_bars))
    half_span = span / 2
    adjacency_gap = timedelta(minutes=TF_MINUTES[timeframe])

    normalized = sorted(signals, key=lambda x: x["signal_dt"])
    merged: list[dict[str, Any]] = []

    for signal in normalized:
        dt = signal["signal_dt"]
        start = dt - half_span
        end = dt + half_span

        if not merged:
            merged.append(
                {
                    "start_dt": start,
                    "end_dt": end,
                    "signals": [signal],
                }
            )
            continue

        last = merged[-1]
        if start <= last["end_dt"] + adjacency_gap:
            if end > last["end_dt"]:
                last["end_dt"] = end
            last["signals"].append(signal)
            continue

        merged.append(
            {
                "start_dt": start,
                "end_dt": end,
                "signals": [signal],
            }
        )

    return merged


def _coerce_datetime(value: Any) -> datetime | None:
    """
    把多种时间值宽松转换成 `datetime`。

    输入：
    1. value: 可能是 `datetime`、ISO 时间字符串、空字符串或 `None`。
    输出：
    1. 成功时返回 `datetime`，否则返回 `None`。
    用途：
    1. 兼容策略 payload 与状态库中可能出现的不同时间表示形式。
    边界条件：
    1. 空字符串和无法解析的字符串统一返回 `None`。
    2. 末尾为 `Z` 的字符串会先转换成 `+00:00` 再解析。
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        if token.endswith("Z"):
            token = token[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(token)
        except ValueError:
            return None
    return None


def _datetime_midpoint(start: datetime, end: datetime) -> datetime:
    """
    输入：
    1. start: 输入参数，具体约束以调用方和实现为准。
    2. end: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `_datetime_midpoint` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    return start + (end - start) / 2


def _extract_latest_pattern_window(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Extract the latest pattern window from strategy payload when available."""
    patterns = payload.get("patterns") if isinstance(payload.get("patterns"), list) else None
    if not patterns:
        return None

    for pattern in reversed(patterns):
        if not isinstance(pattern, dict):
            continue
        window_start_raw = pattern.get("bull_start_ts") or pattern.get("consolidation_start_ts")
        window_end_raw = pattern.get("consolidation_end_ts") or pattern.get("bull_end_ts")
        anchor_day_raw = pattern.get("consolidation_end_ts") or pattern.get("bull_end_ts") or window_end_raw

        window_start_dt = _coerce_datetime(window_start_raw)
        window_end_dt = _coerce_datetime(window_end_raw)
        anchor_day_dt = _coerce_datetime(anchor_day_raw)
        if window_start_dt is None or window_end_dt is None:
            continue
        if window_end_dt < window_start_dt:
            window_start_dt, window_end_dt = window_end_dt, window_start_dt

        return {
            "anchor_day_raw": anchor_day_raw,
            "anchor_day_dt": anchor_day_dt or window_end_dt,
            "window_start_raw": window_start_raw,
            "window_start_dt": window_start_dt,
            "window_end_raw": window_end_raw,
            "window_end_dt": window_end_dt,
        }

    return None


def _extract_signal_window(payload: dict[str, Any]) -> dict[str, Any]:
    """从策略 payload 中提取标准化的锚点、命中窗口和图表展示窗口。

    读取优先级：
    1. 优先读取 payload 顶层字段，如 `window_start_ts`、`chart_interval_start_ts`。
    2. 顶层缺失时回退到 `payload["window"]` 嵌套对象。
    3. 仍缺失时，再尝试从最新 pattern 窗口推导展示区间。

    返回值中的 `*_raw` 保留原始文本，`*_dt` 则是宽松解析后的 `datetime`。
    """
    window = payload.get("window") if isinstance(payload.get("window"), dict) else {}
    latest_pattern_window = _extract_latest_pattern_window(payload)

    anchor_day_raw = payload.get("anchor_day_ts")
    window_start_raw = payload.get("window_start_ts")
    if window_start_raw is None:
        window_start_raw = window.get("start_ts")
    if window_start_raw is None and latest_pattern_window is not None:
        window_start_raw = latest_pattern_window["window_start_raw"]
    window_end_raw = payload.get("window_end_ts")
    if window_end_raw is None:
        window_end_raw = window.get("end_ts")
    if window_end_raw is None and latest_pattern_window is not None:
        window_end_raw = latest_pattern_window["window_end_raw"]
    if anchor_day_raw is None and latest_pattern_window is not None:
        anchor_day_raw = latest_pattern_window["anchor_day_raw"]
    chart_start_raw = payload.get("chart_interval_start_ts")
    chart_end_raw = payload.get("chart_interval_end_ts")
    if latest_pattern_window is not None and payload.get("window_start_ts") is None and payload.get("window_end_ts") is None:
        chart_start_raw = latest_pattern_window["window_start_raw"]
        chart_end_raw = latest_pattern_window["window_end_raw"]

    anchor_day_dt = _coerce_datetime(anchor_day_raw)
    window_start_dt = _coerce_datetime(window_start_raw)
    window_end_dt = _coerce_datetime(window_end_raw)
    chart_start_dt = _coerce_datetime(chart_start_raw) or anchor_day_dt or window_start_dt
    chart_end_dt = _coerce_datetime(chart_end_raw) or window_end_dt

    if chart_start_dt and chart_end_dt and chart_end_dt < chart_start_dt:
        chart_start_dt, chart_end_dt = chart_end_dt, chart_start_dt

    return {
        "anchor_day_raw": anchor_day_raw,
        "anchor_day_dt": anchor_day_dt,
        "window_start_raw": window_start_raw,
        "window_start_dt": window_start_dt,
        "window_end_raw": window_end_raw,
        "window_end_dt": window_end_dt,
        "chart_start_raw": chart_start_raw,
        "chart_start_dt": chart_start_dt,
        "chart_end_raw": chart_end_raw,
        "chart_end_dt": chart_end_dt,
    }


def _help_text_from_node(node: Any) -> str:
    """
    把 `manifest.param_help` 中的节点转换成可展示的帮助文本。

    支持三类输入：
    1. 字符串：直接返回。
    2. 列表：按换行拼接，常用于 `_overview`。
    3. 字典：读取 `_comment` 和 `_overview` 组合成块文本。
    """
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "\n".join(str(x) for x in node)
    if isinstance(node, dict):
        blocks: list[str] = []
        text = node.get("_comment")
        if isinstance(text, str) and text.strip():
            blocks.append(text.strip())
        overview = node.get("_overview")
        if isinstance(overview, list) and overview:
            blocks.append("\n".join(str(x) for x in overview))
        return "\n\n".join(blocks)
    return ""


def _build_annotated_params(params: Any, help_node: Any) -> Any:
    """
    递归给参数结构附加注释字段。

    规则：
    1. 根节点注释写入 `__comment__`。
    2. 子字段注释写入 `__comment_<field>`。
    3. 这些注释字段会随参数一起发给前端，用于渲染表单帮助信息，并在用户提交时做完整性校验。
    """
    if not isinstance(params, dict):
        return params
    result: dict[str, Any] = {}
    root_help = _help_text_from_node(help_node)
    if root_help:
        result["__comment__"] = root_help

    for key, value in params.items():
        child_help_node = help_node.get(key) if isinstance(help_node, dict) else None
        child_help_text = _help_text_from_node(child_help_node)
        if child_help_text:
            result[f"__comment_{key}"] = child_help_text
        result[key] = _build_annotated_params(value, child_help_node)
    return result


def _strip_comment_keys(value: Any) -> Any:
    """
    递归删除参数结构中的 `__comment*` 注释字段。

    用于把前端回传的“带注释参数树”还原成真正参与策略执行的参数字典。
    """
    if isinstance(value, list):
        return [_strip_comment_keys(x) for x in value]
    if not isinstance(value, dict):
        return value
    result: dict[str, Any] = {}
    for key, child in value.items():
        if key.startswith("__comment"):
            continue
        result[key] = _strip_comment_keys(child)
    return result


def _validate_comment_integrity(
    submitted: Any,
    expected: Any,
    *,
    path: str = "group_params",
) -> list[str]:
    """
    校验前端提交的注释字段没有丢失、篡改或新增。

    这些 `__comment*` 字段来自 manifest 的 `param_help`，属于框架协议的一部分，
    前端可以展示，但不允许用户修改其内容。
    """
    errors: list[str] = []
    if not isinstance(submitted, dict) or not isinstance(expected, dict):
        return errors

    for key, expected_value in expected.items():
        if key.startswith("__comment"):
            if key not in submitted:
                errors.append(f"{path} 缺少注释字段: {key}")
                continue
            if submitted.get(key) != expected_value:
                errors.append(f"{path} 注释字段不可修改: {key}")

    for key in submitted.keys():
        if key.startswith("__comment") and key not in expected:
            errors.append(f"{path} 存在未知注释字段: {key}")

    for key, expected_value in expected.items():
        if key.startswith("__comment"):
            continue
        if key in submitted and isinstance(submitted.get(key), dict) and isinstance(expected_value, dict):
            child_path = f"{path}.{key}"
            errors.extend(
                _validate_comment_integrity(
                    submitted.get(key),
                    expected_value,
                    path=child_path,
                )
            )
    return errors


def _validate_params_shape(
    submitted: Any,
    template: Any,
    *,
    path: str = "group_params",
) -> list[str]:
    """
    输入：
    1. submitted: 输入参数，具体约束以调用方和实现为准。
    2. template: 输入参数，具体约束以调用方和实现为准。
    3. path: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `_validate_params_shape` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    errors: list[str] = []

    if isinstance(template, dict):
        if not isinstance(submitted, dict):
            return [f"{path} 必须是对象"]
        for key in template.keys():
            if key not in submitted:
                errors.append(f"{path} 缺少字段: {key}")
        for key in submitted.keys():
            if key not in template:
                errors.append(f"{path} 存在未知字段: {key}")
        for key, expected_value in template.items():
            if key not in submitted:
                continue
            errors.extend(
                _validate_params_shape(
                    submitted.get(key),
                    expected_value,
                    path=f"{path}.{key}",
                )
            )
        return errors

    if isinstance(template, bool):
        if not isinstance(submitted, bool):
            errors.append(f"{path} 必须是布尔值")
        return errors

    if isinstance(template, int) and not isinstance(template, bool):
        if not isinstance(submitted, int) or isinstance(submitted, bool):
            errors.append(f"{path} 必须是整数")
        return errors

    if isinstance(template, float):
        if not isinstance(submitted, (int, float)) or isinstance(submitted, bool):
            errors.append(f"{path} 必须是数值")
        return errors

    if isinstance(template, str):
        if not isinstance(submitted, str):
            errors.append(f"{path} 必须是字符串")
        return errors

    if template is None:
        return errors

    if not isinstance(submitted, type(template)):
        errors.append(f"{path} 类型不合法，应为 {type(template).__name__}")
    return errors


@router.get("/health")
def health() -> dict[str, str]:
    """
    输入：
    1. 无显式输入参数。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `health` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    return {"status": "ok"}


@router.get("/maintenance/runtime-metadata")
def get_maintenance_runtime_metadata() -> dict[str, Any]:
    """
    输入：
    1. 无显式输入参数。
    输出：
    1. zsdtdx 运行时元数据快照，含活跃主机与连接池统计。
    用途：
    1. 供维护页面展示 TDX 服务器可用状态指示灯。
    边界条件：
    1. zsdtdx 未安装或连接异常时返回 error 字段。
    """
    try:
        from zsdtdx.simple_api import get_client, get_runtime_metadata
        with get_client():
            meta = get_runtime_metadata()
        return {"ok": True, "metadata": meta}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "metadata": {}}


@router.get("/strategy-groups")
def list_strategy_groups(request: Request) -> dict[str, Any]:
    """
    输入：
    1. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `list_strategy_groups` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    registry = request.app.state.strategy_registry
    return {"items": registry.list_groups()}


@router.get("/strategy-groups/{group_id}")
def get_strategy_group(group_id: str, request: Request) -> dict[str, Any]:
    """
    输入：
    1. group_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_strategy_group` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    registry = request.app.state.strategy_registry
    try:
        return registry.get_group(group_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/ui-settings/monitor")
def get_monitor_ui_settings(request: Request) -> dict[str, Any]:
    """
    输入：
    1. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_monitor_ui_settings` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    return {"settings": state_db.get_monitor_form_settings()}


@router.post("/ui-settings/monitor")
def save_monitor_ui_settings(payload: MonitorFormSettingsPayload, request: Request) -> dict[str, Any]:
    """
    输入：
    1. payload: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `save_monitor_ui_settings` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    registry = request.app.state.strategy_registry
    settings = payload.model_dump()

    start_ts_raw = (settings.get("start_ts") or "").strip()
    end_ts_raw = (settings.get("end_ts") or "").strip()
    start_dt = _coerce_datetime(start_ts_raw) if start_ts_raw else None
    end_dt = _coerce_datetime(end_ts_raw) if end_ts_raw else None
    if start_ts_raw and start_dt is None:
        raise HTTPException(status_code=400, detail="start_ts 格式不合法")
    if end_ts_raw and end_dt is None:
        raise HTTPException(status_code=400, detail="end_ts 格式不合法")
    if start_dt and end_dt and start_dt > end_dt:
        raise HTTPException(status_code=400, detail="start_ts 不能晚于 end_ts")

    group_id = str(settings.get("strategy_group_id") or "").strip()
    if not group_id:
        raise HTTPException(status_code=400, detail="strategy_group_id 不能为空")

    try:
        meta = registry.get_group_meta(group_id)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    group_params_text = settings.get("group_params_text")
    if not isinstance(group_params_text, str):
        raise HTTPException(status_code=400, detail="group_params_text 必须是字符串")
    group_params_text = group_params_text.strip() or "{}"

    try:
        parsed_params = json.loads(group_params_text)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"group_params JSON 格式错误: line {exc.lineno}, col {exc.colno}",
        ) from exc
    if not isinstance(parsed_params, dict):
        raise HTTPException(status_code=400, detail="group_params JSON 顶层必须是对象")

    expected_annotated = _build_annotated_params(copy.deepcopy(meta.default_params), meta.param_help or {})
    comment_errors = _validate_comment_integrity(parsed_params, expected_annotated, path="group_params")
    if comment_errors:
        raise HTTPException(
            status_code=400,
            detail="注释字段不可修改或删除: " + "；".join(comment_errors[:8]),
        )

    cleaned_params = _strip_comment_keys(parsed_params)
    data_errors = _validate_params_shape(cleaned_params, meta.default_params, path="group_params")
    if data_errors:
        raise HTTPException(
            status_code=400,
            detail="参数数据不合法: " + "；".join(data_errors[:8]),
        )

    try:
        registry.merge_group_params(group_id, cleaned_params)
    except StrategyRegistryError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    settings["start_ts"] = start_ts_raw
    settings["end_ts"] = end_ts_raw
    settings["strategy_group_id"] = group_id
    settings["group_params_text"] = group_params_text
    state_db.set_monitor_form_settings(settings)
    return {"settings": settings}


@router.get("/ui-settings/maintenance")
def get_maintenance_ui_settings(request: Request) -> dict[str, Any]:
    """
    输入：
    1. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_maintenance_ui_settings` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    return {"settings": state_db.get_maintenance_form_settings()}


@router.post("/ui-settings/maintenance")
def save_maintenance_ui_settings(payload: MaintenanceFormSettingsPayload, request: Request) -> dict[str, Any]:
    """
    输入：
    1. payload: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `save_maintenance_ui_settings` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    settings = payload.model_dump()
    state_db.set_maintenance_form_settings(settings)
    return {"settings": settings}


@router.post("/tasks", response_model=CreateTaskResponse)
def create_task(payload: CreateTaskRequest, request: Request) -> CreateTaskResponse:
    """
    输入：
    1. payload: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `create_task` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    task_manager = request.app.state.task_manager
    maintenance_manager = request.app.state.maintenance_manager
    concept_manager = request.app.state.concept_manager
    if maintenance_manager.has_running_job():
        raise HTTPException(status_code=409, detail="数据库维护任务运行中，当前禁止创建筛选任务")
    if concept_manager.has_running_job():
        raise HTTPException(status_code=409, detail="概念更新任务运行中，当前禁止创建筛选任务")
    try:
        task_id = task_manager.create_task(
            stocks=payload.stocks,
            start_ts=payload.start_ts,
            end_ts=payload.end_ts,
            group_params=payload.group_params,
            strategy_group_id=payload.strategy_group_id,
            source_db=payload.source_db,
            run_mode=payload.run_mode,
            sample_size=payload.sample_size,
            skip_coverage_filter=payload.skip_coverage_filter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return CreateTaskResponse(task_id=task_id)


@router.post("/tasks/{task_id}/pause", response_model=TaskControlResponse)
@router.get("/tasks/{task_id}/result-stock-concepts", response_model=ResultStockConceptsResponse)
def get_result_stock_concepts(task_id: str, request: Request) -> ResultStockConceptsResponse:
    """
    输入：
    1. task_id: 任务 ID。
    2. request: 请求上下文。
    输出：
    1. 返回结果股票的概念明细与任务概念公式配置。
    用途：
    1. 供结果页批量懒加载概念气泡内容。
    边界条件：
    1. 任务不存在返回 404；无结果股票时返回空 items。
    """
    state_db = request.app.state.state_db
    task = state_db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")

    result_stocks = state_db.get_result_stock_summaries(task_id)
    codes = [str(item.get("code") or "") for item in result_stocks if item.get("code")]
    summary = task.get("summary") if isinstance(task.get("summary"), dict) else {}
    formula = summary.get("concept_formula") if isinstance(summary.get("concept_formula"), dict) else {}

    if not codes:
        return ResultStockConceptsResponse(task_id=task_id, formula=formula, items={})

    market_db = MarketDataDB(task["source_db"])
    raw_map = market_db.get_stock_concepts_by_codes(codes)
    items = {
        code: [ResultStockConceptEntry(**entry) for entry in entries]
        for code, entries in raw_map.items()
    }
    return ResultStockConceptsResponse(task_id=task_id, formula=formula, items=items)


def pause_task(task_id: str, request: Request) -> TaskControlResponse:
    """
    输入：
    1. task_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `pause_task` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    task_manager = request.app.state.task_manager
    try:
        result = task_manager.pause_task(task_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TaskControlResponse(**result)


@router.post("/tasks/{task_id}/resume", response_model=TaskControlResponse)
def resume_task(task_id: str, request: Request) -> TaskControlResponse:
    """
    输入：
    1. task_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `resume_task` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    task_manager = request.app.state.task_manager
    try:
        result = task_manager.resume_task(task_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TaskControlResponse(**result)


@router.post("/tasks/{task_id}/stop", response_model=TaskControlResponse)
def stop_task(task_id: str, request: Request) -> TaskControlResponse:
    """
    输入：
    1. task_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `stop_task` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    task_manager = request.app.state.task_manager
    try:
        result = task_manager.stop_task(task_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TaskControlResponse(**result)


@router.get("/tasks")
def list_tasks(
    request: Request,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    """
    输入：
    1. request: 输入参数，具体约束以调用方和实现为准。
    2. offset: 输入参数，具体约束以调用方和实现为准。
    3. limit: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `list_tasks` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    return {"items": state_db.list_tasks(offset=offset, limit=limit)}


@router.get("/tasks/{task_id}", response_model=TaskStatusResponse)
def get_task(task_id: str, request: Request) -> TaskStatusResponse:
    """
    输入：
    1. task_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_task` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    task = state_db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")

    summary = task.get("summary") or {}
    run_mode = (task.get("params") or {}).get("run_mode") or "full"
    return TaskStatusResponse(
        task_id=task["task_id"],
        status=task["status"],
        progress=float(task.get("progress") or 0.0),
        total_stocks=int(task.get("total_stocks") or 0),
        processed_stocks=int(task.get("processed_stocks") or 0),
        result_count=int(task.get("result_count") or 0),
        info_log_count=int(task.get("info_log_count") or 0),
        error_log_count=int(task.get("error_log_count") or 0),
        current_code=task.get("current_code"),
        started_at=task.get("started_at"),
        finished_at=task.get("finished_at"),
        error_message=task.get("error_message"),
        unresolved_inputs=summary.get("unresolved_inputs") or [],
        run_mode=run_mode,
        strategy_group_id=task.get("strategy_group_id") or "",
        strategy_name=task.get("strategy_name") or "",
        strategy_description=task.get("strategy_description") or "",
    )


@router.get("/tasks/{task_id}/logs", response_model=LogsResponse)
def get_logs(
    task_id: str,
    request: Request,
    level: str = Query(default="all", pattern="^(all|info|error)$"),
    after_log_id: int | None = Query(default=None, ge=0),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=200, ge=1, le=5000),
) -> LogsResponse:
    """
    输入：
    1. task_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    3. level: 输入参数，具体约束以调用方和实现为准。
    4. after_log_id: 输入参数，具体约束以调用方和实现为准。
    5. offset: 输入参数，具体约束以调用方和实现为准。
    6. limit: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_logs` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    if not state_db.get_task(task_id):
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
    items = state_db.get_logs(
        task_id=task_id,
        level=level,
        offset=offset,
        limit=limit,
        after_log_id=after_log_id,
    )
    next_after_log_id = int(items[-1]["log_id"]) if items else (int(after_log_id) if after_log_id is not None else None)
    return LogsResponse(items=items, next_after_log_id=next_after_log_id)


@router.get("/tasks/{task_id}/results", response_model=ResultsResponse)
def get_results(
    task_id: str,
    request: Request,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=5000, ge=1, le=100000),
) -> ResultsResponse:
    """
    输入：
    1. task_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    3. offset: 输入参数，具体约束以调用方和实现为准。
    4. limit: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_results` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    task = state_db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
    items = state_db.get_results(task_id=task_id, offset=offset, limit=limit)
    total_count = int(task.get("result_count") or 0)
    return ResultsResponse(
        total_count=total_count,
        offset=offset,
        limit=limit,
        has_more=(offset + len(items)) < total_count,
        items=items,
    )


@router.get("/tasks/{task_id}/result-stocks")
def get_result_stocks(task_id: str, request: Request) -> dict[str, Any]:
    """
    输入：
    1. task_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_result_stocks` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    if not state_db.get_task(task_id):
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")

    items = state_db.get_result_stock_summaries(task_id)
    return {
        "task_id": task_id,
        "total_stocks": len(items),
        "items": items,
    }


@router.get("/tasks/{task_id}/candles")
def get_candles(
    task_id: str,
    request: Request,
    code: str,
    timeframe: str = Query(default="d"),
    end_ts: str | None = Query(default=None),
    start_ts: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict[str, Any]:
    """轻量级蜡烛图数据端点，用于懒加载追加数据。"""
    if timeframe not in TIMEFRAME_TO_TABLE:
        raise HTTPException(status_code=400, detail=f"不支持的周期: {timeframe}")
    state_db = request.app.state.state_db
    task = state_db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
    parsed_start = _coerce_datetime(start_ts)
    parsed_end = _coerce_datetime(end_ts)
    market_db = MarketDataDB(task["source_db"])
    if parsed_end and not parsed_start:
        candles = market_db.fetch_candles_between(
            code=code,
            timeframe=timeframe,
            start_ts=None,
            end_ts=parsed_end,
            limit=limit,
            order="desc",
        )
        candles.reverse()
    else:
        candles = market_db.fetch_candles_between(
            code=code,
            timeframe=timeframe,
            start_ts=parsed_start,
            end_ts=parsed_end,
            limit=limit,
        )
    return {"candles": candles}


@router.get("/tasks/{task_id}/stock-chart")
def get_stock_chart(
    task_id: str,
    request: Request,
    code: str,
    timeframe: str = Query(default="15"),
    padding_bars: int = Query(default=300, ge=1, le=99999),
    interval_bars: int = Query(default=3, ge=1, le=200),
) -> dict[str, Any]:
    """
    输入：
    1. task_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    3. code: 输入参数，具体约束以调用方和实现为准。
    4. timeframe: 输入参数，具体约束以调用方和实现为准。
    5. padding_bars: 输入参数，具体约束以调用方和实现为准。
    6. interval_bars: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_stock_chart` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    if timeframe not in TIMEFRAME_TO_TABLE:
        raise HTTPException(status_code=400, detail=f"不支持的周期: {timeframe}")

    state_db = request.app.state.state_db
    task = state_db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")

    strategy_group_id = task.get("strategy_group_id") or ""
    strategy_name = task.get("strategy_name") or ""
    strategy_description = task.get("strategy_description") or ""

    signals = state_db.get_results_by_code(task_id=task_id, code=code)
    stock_name = signals[0]["name"] if signals else ""
    if not signals:
        return {
            "task_id": task_id,
            "code": code,
            "name": stock_name,
            "timeframe": timeframe,
            "strategy_group_id": strategy_group_id,
            "strategy_name": strategy_name,
            "strategy_description": strategy_description,
            "signals": [],
            "intervals": [],
            "candles": [],
        }

    tf_minutes = TF_MINUTES[timeframe]
    padding = timedelta(minutes=tf_minutes * max(1, padding_bars))
    query_start: datetime | None = None
    query_end: datetime | None = None

    signal_items: list[dict[str, Any]] = []
    interval_items: list[dict[str, Any]] = []
    min_interval_start: datetime | None = None
    max_interval_end: datetime | None = None
    max_signal_dt: datetime | None = None

    for item in signals:
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        window_info = _extract_signal_window(payload)

        signal_dt_raw = item.get("signal_dt")
        signal_dt_dt = _coerce_datetime(signal_dt_raw)
        signal_dt = signal_dt_dt or signal_dt_raw

        anchor_day_dt = window_info["anchor_day_dt"]
        chart_start_dt = window_info["chart_start_dt"]
        chart_end_dt = window_info["chart_end_dt"]

        marker_mode = "interval_center"
        marker_ts_dt: datetime | None = None
        marker_ts_raw: Any = None
        if anchor_day_dt:
            marker_mode = "anchor"
            marker_ts_dt = anchor_day_dt
        elif chart_start_dt and chart_end_dt:
            marker_ts_dt = _datetime_midpoint(chart_start_dt, chart_end_dt)
        else:
            marker_ts_dt = signal_dt_dt
            marker_ts_raw = signal_dt_raw

        daily_audit = payload.get("daily_audit") if isinstance(payload.get("daily_audit"), dict) else {}
        if not daily_audit:
            fallback_audit = payload.get("audit_pass_flags")
            if isinstance(fallback_audit, dict):
                daily_audit = fallback_audit

        signal_items.append(
            {
                "signal_dt": signal_dt,
                "clock_tf": item["clock_tf"],
                "strategy_group_id": item.get("strategy_group_id") or strategy_group_id,
                "strategy_name": item.get("strategy_name") or strategy_name,
                "signal_label": item.get("signal_label") or strategy_name,
                "anchor_day_ts": anchor_day_dt or window_info["anchor_day_raw"],
                "window_start_ts": window_info["window_start_dt"] or window_info["window_start_raw"],
                "window_end_ts": window_info["window_end_dt"] or window_info["window_end_raw"],
                "chart_interval_start_ts": chart_start_dt or window_info["chart_start_raw"],
                "chart_interval_end_ts": chart_end_dt or window_info["chart_end_raw"],
                "marker_mode": marker_mode,
                "marker_ts": marker_ts_dt or marker_ts_raw,
                "daily_metrics": payload.get("daily_metrics") if isinstance(payload.get("daily_metrics"), dict) else {},
                "daily_audit": daily_audit,
            }
        )

        if chart_start_dt and chart_end_dt:
            interval_items.append(
                {
                    "start_dt": chart_start_dt,
                    "end_dt": chart_end_dt,
                    "signal_count": 1,
                    "strategy_name": item.get("strategy_name") or strategy_name,
                    "interval_kind": "strategy_window",
                    "anchor_day_ts": anchor_day_dt or window_info["anchor_day_raw"],
                    "window_start_ts": window_info["window_start_dt"] or window_info["window_start_raw"],
                    "window_end_ts": window_info["window_end_dt"] or window_info["window_end_raw"],
                    "chart_interval_start_ts": chart_start_dt,
                    "chart_interval_end_ts": chart_end_dt,
                }
            )
            if min_interval_start is None or chart_start_dt < min_interval_start:
                min_interval_start = chart_start_dt
            if max_interval_end is None or chart_end_dt > max_interval_end:
                max_interval_end = chart_end_dt

        if signal_dt_dt and (max_signal_dt is None or signal_dt_dt > max_signal_dt):
            max_signal_dt = signal_dt_dt

    if interval_items:
        interval_items.sort(key=lambda item: (item["start_dt"], item["end_dt"]))
        query_start = min_interval_start - padding if min_interval_start else None
        end_ref = max_interval_end
        if max_signal_dt and (end_ref is None or max_signal_dt > end_ref):
            end_ref = max_signal_dt
        query_end = end_ref + padding if end_ref else None

    if query_start is None or query_end is None:
        merged = _merge_signal_intervals(signals, timeframe=timeframe, interval_bars=interval_bars)
        query_start = merged[0]["start_dt"] - padding
        query_end = merged[-1]["end_dt"] + padding

        interval_items = [
            {
                "start_dt": item["start_dt"],
                "end_dt": item["end_dt"],
                "signal_count": len(item["signals"]),
                "strategy_name": strategy_name,
                "interval_kind": "signal_cluster",
            }
            for item in merged
        ]

    task_start = task.get("start_ts")
    task_end = task.get("end_ts")
    if task_start and query_start and query_start < task_start:
        query_start = task_start
    if task_end and query_end and query_end > task_end:
        query_end = task_end

    market_db = MarketDataDB(task["source_db"])
    candles = market_db.fetch_candles_between(
        code=code,
        timeframe=timeframe,
        start_ts=query_start,
        end_ts=query_end,
        limit=60000,
    )

    return {
        "task_id": task_id,
        "code": code,
        "name": stock_name,
        "timeframe": timeframe,
        "strategy_group_id": strategy_group_id,
        "strategy_name": strategy_name,
        "strategy_description": strategy_description,
        "signals": signal_items,
        "intervals": interval_items,
        "candles": candles,
    }


@router.get("/tasks/{task_id}/stock-states", response_model=StockStatesResponse)
def get_stock_states(
    task_id: str,
    request: Request,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=200, ge=1, le=5000),
) -> StockStatesResponse:
    """
    输入：
    1. task_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    3. offset: 输入参数，具体约束以调用方和实现为准。
    4. limit: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_stock_states` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    if not state_db.get_task(task_id):
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
    items = state_db.get_stock_states(task_id=task_id, offset=offset, limit=limit)
    return StockStatesResponse(items=items)


@router.post("/maintenance/jobs", response_model=MaintenanceCreateResponse)
def create_maintenance_job(payload: MaintenanceCreateRequest, request: Request) -> MaintenanceCreateResponse:
    """
    输入：
    1. payload: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `create_maintenance_job` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    manager = request.app.state.maintenance_manager
    try:
        job_id = manager.create_job(mode=payload.mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return MaintenanceCreateResponse(job_id=job_id)


@router.post("/concept/jobs", response_model=ConceptCreateResponse)
def create_concept_job(request: Request) -> ConceptCreateResponse:
    """
    输入：
    1. request: 请求上下文。
    输出：
    1. 新建概念更新任务 ID。
    用途：
    1. 供维护页触发概念全量更新。
    边界条件：
    1. 若已有维护/概念/筛选任务运行中则返回冲突。
    """
    manager = request.app.state.concept_manager
    try:
        job_id = manager.create_job()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return ConceptCreateResponse(job_id=job_id)


@router.get("/concept/jobs")
def list_concept_jobs(
    request: Request,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    """分页读取概念任务列表。"""
    state_db = request.app.state.state_db
    raw_items = state_db.list_concept_jobs(offset=offset, limit=limit)
    items = [
        {
            "job_id": item.get("job_id"),
            "created_at": item.get("created_at"),
            "started_at": item.get("started_at"),
            "finished_at": item.get("finished_at"),
            "status": item.get("status"),
            "phase": item.get("phase"),
            "progress": item.get("progress"),
            "error_message": item.get("error_message"),
        }
        for item in raw_items
    ]
    return {"items": items}


@router.get("/concept/jobs/{job_id}", response_model=ConceptStatusResponse)
def get_concept_job(job_id: str, request: Request) -> ConceptStatusResponse:
    """读取概念任务状态。"""
    state_db = request.app.state.state_db
    job = state_db.get_concept_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"概念任务不存在: {job_id}")
    return ConceptStatusResponse(**_concept_status_dict(job))


@router.post("/concept/jobs/{job_id}/stop", response_model=ConceptControlResponse)
def stop_concept_job(job_id: str, request: Request) -> ConceptControlResponse:
    """停止概念任务。"""
    manager = request.app.state.concept_manager
    try:
        result = manager.stop_job(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ConceptControlResponse(**result)


@router.get("/concept/jobs/{job_id}/logs", response_model=ConceptLogsResponse)
def get_concept_logs(
    job_id: str,
    request: Request,
    level: str = Query(default="all", pattern="^(all|info|error)$"),
    after_log_id: int | None = Query(default=None, ge=0),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=300, ge=1, le=5000),
) -> ConceptLogsResponse:
    """读取概念任务日志。"""
    state_db = request.app.state.state_db
    if not state_db.get_concept_job(job_id):
        raise HTTPException(status_code=404, detail=f"概念任务不存在: {job_id}")
    items = state_db.get_concept_logs(
        job_id=job_id,
        level=level,
        offset=offset,
        limit=limit,
        after_log_id=after_log_id,
    )
    next_after_log_id = int(items[-1]["log_id"]) if items else (int(after_log_id) if after_log_id is not None else None)
    return ConceptLogsResponse(items=items, next_after_log_id=next_after_log_id)


@router.get("/maintenance/jobs")
def list_maintenance_jobs(
    request: Request,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    """
    输入：
    1. request: 输入参数，具体约束以调用方和实现为准。
    2. offset: 输入参数，具体约束以调用方和实现为准。
    3. limit: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `list_maintenance_jobs` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    raw_items = state_db.list_maintenance_jobs(offset=offset, limit=limit)
    items = [
        {
            "job_id": item.get("job_id"),
            "created_at": item.get("created_at"),
            "started_at": item.get("started_at"),
            "finished_at": item.get("finished_at"),
            "status": item.get("status"),
            "phase": item.get("phase"),
            "progress": item.get("progress"),
            "mode": item.get("mode"),
            "error_message": item.get("error_message"),
        }
        for item in raw_items
    ]
    return {"items": items}


@router.get("/maintenance/jobs/{job_id}", response_model=MaintenanceStatusResponse)
def get_maintenance_job(job_id: str, request: Request) -> MaintenanceStatusResponse:
    """
    输入：
    1. job_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_maintenance_job` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    job = state_db.get_maintenance_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"维护任务不存在: {job_id}")
    summary_raw = job.get("summary") if isinstance(job.get("summary"), dict) else {}
    rows_written = summary_raw.get("rows_written")
    if not isinstance(rows_written, dict):
        rows_written = {}
    summary = {
        "steps_total": int(summary_raw.get("steps_total") or 0),
        "steps_completed": int(summary_raw.get("steps_completed") or 0),
        "total_tasks": int(summary_raw.get("total_tasks") or 0),
        "success_tasks": int(summary_raw.get("success_tasks") or 0),
        "failed_tasks": int(summary_raw.get("failed_tasks") or 0),
        "retry_rounds_used": int(summary_raw.get("retry_rounds_used") or 0),
        "rows_written": rows_written,
        "retry_skipped_tasks": int(summary_raw.get("retry_skipped_tasks") or 0),
        "removed_corrupted_rows": int(summary_raw.get("removed_corrupted_rows") or 0),
        "duration_seconds": float(summary_raw.get("duration_seconds") or 0.0),
    }
    fetch_progress_raw = summary_raw.get("fetch_progress")
    if isinstance(fetch_progress_raw, dict):
        summary["fetch_progress"] = fetch_progress_raw
    return MaintenanceStatusResponse(
        job_id=job["job_id"],
        status=job["status"],
        phase=job.get("phase"),
        progress=float(job.get("progress") or 0.0),
        mode=str(job.get("mode") or "latest_update"),
        started_at=job.get("started_at"),
        finished_at=job.get("finished_at"),
        error_message=job.get("error_message"),
        summary=summary,
    )


@router.post("/maintenance/jobs/{job_id}/stop", response_model=MaintenanceControlResponse)
def stop_maintenance_job(job_id: str, request: Request) -> MaintenanceControlResponse:
    """
    输入：
    1. job_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `stop_maintenance_job` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    manager = request.app.state.maintenance_manager
    try:
        result = manager.stop_job(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return MaintenanceControlResponse(**result)


@router.get("/maintenance/jobs/{job_id}/logs", response_model=MaintenanceLogsResponse)
def get_maintenance_logs(
    job_id: str,
    request: Request,
    level: str = Query(default="all", pattern="^(all|info|error)$"),
    after_log_id: int | None = Query(default=None, ge=0),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=300, ge=1, le=5000),
) -> MaintenanceLogsResponse:
    """
    输入：
    1. job_id: 输入参数，具体约束以调用方和实现为准。
    2. request: 输入参数，具体约束以调用方和实现为准。
    3. level: 输入参数，具体约束以调用方和实现为准。
    4. after_log_id: 输入参数，具体约束以调用方和实现为准。
    5. offset: 输入参数，具体约束以调用方和实现为准。
    6. limit: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_maintenance_logs` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    state_db = request.app.state.state_db
    if not state_db.get_maintenance_job(job_id):
        raise HTTPException(status_code=404, detail=f"维护任务不存在: {job_id}")
    items = state_db.get_maintenance_logs(
        job_id=job_id,
        level=level,
        offset=offset,
        limit=limit,
        after_log_id=after_log_id,
    )
    next_after_log_id = int(items[-1]["log_id"]) if items else (int(after_log_id) if after_log_id is not None else None)
    return MaintenanceLogsResponse(items=items, next_after_log_id=next_after_log_id)


# ---------------------------------------------------------------------------
# SSE (Server-Sent Events) 推送端点
# ---------------------------------------------------------------------------


def _sse_event(event: str, data: Any) -> str:
    """格式化一条 SSE 事件。"""
    payload = json.dumps(data, ensure_ascii=False, default=str)
    return f"event: {event}\ndata: {payload}\n\n"


@router.get("/stream/heartbeat")
async def heartbeat_event_stream(request: Request):
    """页面级 SSE 心跳流，用于在未选择具体任务时保持连接状态。"""

    async def generate():
        yield _sse_event("connected", {"scope": "page"})
        while True:
            if await request.is_disconnected():
                return
            yield _sse_event("heartbeat", {"ts": datetime.now()})
            await asyncio.sleep(15.0)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _task_status_dict(task: dict[str, Any]) -> dict[str, Any]:
    summary = task.get("summary") or {}
    run_mode = (task.get("params") or {}).get("run_mode") or "full"
    return {
        "task_id": task["task_id"],
        "status": task["status"],
        "progress": float(task.get("progress") or 0.0),
        "total_stocks": int(task.get("total_stocks") or 0),
        "processed_stocks": int(task.get("processed_stocks") or 0),
        "result_count": int(task.get("result_count") or 0),
        "info_log_count": int(task.get("info_log_count") or 0),
        "error_log_count": int(task.get("error_log_count") or 0),
        "current_code": task.get("current_code"),
        "started_at": task.get("started_at"),
        "finished_at": task.get("finished_at"),
        "error_message": task.get("error_message"),
        "unresolved_inputs": summary.get("unresolved_inputs") or [],
        "run_mode": run_mode,
        "strategy_group_id": task.get("strategy_group_id") or "",
        "strategy_name": task.get("strategy_name") or "",
        "strategy_description": task.get("strategy_description") or "",
    }


def _maintenance_status_dict(job: dict[str, Any]) -> dict[str, Any]:
    summary_raw = job.get("summary") if isinstance(job.get("summary"), dict) else {}
    rows_written = summary_raw.get("rows_written")
    if not isinstance(rows_written, dict):
        rows_written = {}
    summary = {
        "steps_total": int(summary_raw.get("steps_total") or 0),
        "steps_completed": int(summary_raw.get("steps_completed") or 0),
        "total_tasks": int(summary_raw.get("total_tasks") or 0),
        "success_tasks": int(summary_raw.get("success_tasks") or 0),
        "failed_tasks": int(summary_raw.get("failed_tasks") or 0),
        "retry_rounds_used": int(summary_raw.get("retry_rounds_used") or 0),
        "rows_written": rows_written,
        "retry_skipped_tasks": int(summary_raw.get("retry_skipped_tasks") or 0),
        "removed_corrupted_rows": int(summary_raw.get("removed_corrupted_rows") or 0),
        "duration_seconds": float(summary_raw.get("duration_seconds") or 0.0),
    }
    fetch_progress = summary_raw.get("fetch_progress")
    if isinstance(fetch_progress, dict):
        summary["fetch_progress"] = fetch_progress
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "phase": job.get("phase"),
        "progress": float(job.get("progress") or 0.0),
        "mode": str(job.get("mode") or "latest_update"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "error_message": job.get("error_message"),
        "summary": summary,
    }


def _concept_status_dict(job: dict[str, Any]) -> dict[str, Any]:
    summary_raw = job.get("summary") if isinstance(job.get("summary"), dict) else {}
    summary = {
        "steps_total": int(summary_raw.get("steps_total") or 0),
        "steps_completed": int(summary_raw.get("steps_completed") or 0),
        "total_tasks": int(summary_raw.get("total_tasks") or 0),
        "success_tasks": int(summary_raw.get("success_tasks") or 0),
        "failed_tasks": int(summary_raw.get("failed_tasks") or 0),
        "records_written": int(summary_raw.get("records_written") or 0),
        "filtered_records": int(summary_raw.get("filtered_records") or 0),
        "duration_seconds": float(summary_raw.get("duration_seconds") or 0.0),
    }
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "phase": job.get("phase"),
        "progress": float(job.get("progress") or 0.0),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "error_message": job.get("error_message"),
        "summary": summary,
    }


@router.get("/tasks/{task_id}/stream")
async def task_event_stream(task_id: str, request: Request):
    """SSE 推送：筛选任务状态 + 增量日志 + 逐股进度（监控页）。"""
    state_db = request.app.state.state_db

    async def generate():
        info_cursor: int | None = None
        error_cursor: int | None = None
        last_status_key: tuple | None = None
        last_stock_trigger: tuple | None = None

        yield _sse_event("connected", {"task_id": task_id})

        while True:
            if await request.is_disconnected():
                return

            try:
                task = await asyncio.to_thread(state_db.get_task, task_id)
            except Exception:
                yield _sse_event("stream-error", {"detail": "读取任务状态失败"})
                return
            if not task:
                yield _sse_event("stream-error", {"detail": f"任务不存在: {task_id}"})
                return

            # -- 任务状态（仅变化时推送） --
            status_data = _task_status_dict(task)
            status_key = (
                status_data["status"],
                status_data["progress"],
                status_data["processed_stocks"],
                status_data["result_count"],
                status_data["current_code"],
                status_data["info_log_count"],
                status_data["error_log_count"],
                status_data["error_message"],
            )
            if status_key != last_status_key:
                last_status_key = status_key
                yield _sse_event("task-status", status_data)

            # -- 日志（首次 bootstrap，后续增量） --
            try:
                if info_cursor is None:
                    info_total = int(task.get("info_log_count") or 0)
                    info_offset = max(0, info_total - 200)
                    info_items = await asyncio.to_thread(
                        state_db.get_logs,
                        task_id=task_id, level="info",
                        offset=info_offset, limit=200, after_log_id=None,
                    )
                else:
                    info_items = await asyncio.to_thread(
                        state_db.get_logs,
                        task_id=task_id, level="info",
                        offset=0, limit=200, after_log_id=info_cursor,
                    )
                if info_items:
                    info_cursor = int(info_items[-1]["log_id"])
                    yield _sse_event("logs-info", info_items)
                elif info_cursor is None:
                    info_cursor = 0

                if error_cursor is None:
                    error_total = int(task.get("error_log_count") or 0)
                    error_offset = max(0, error_total - 200)
                    error_items = await asyncio.to_thread(
                        state_db.get_logs,
                        task_id=task_id, level="error",
                        offset=error_offset, limit=200, after_log_id=None,
                    )
                else:
                    error_items = await asyncio.to_thread(
                        state_db.get_logs,
                        task_id=task_id, level="error",
                        offset=0, limit=200, after_log_id=error_cursor,
                    )
                if error_items:
                    error_cursor = int(error_items[-1]["log_id"])
                    yield _sse_event("logs-error", error_items)
                elif error_cursor is None:
                    error_cursor = 0
            except Exception:
                pass

            # -- 逐股进度（处理数或命中数变化时推送） --
            try:
                stock_trigger = (status_data["processed_stocks"], status_data["result_count"])
                if stock_trigger != last_stock_trigger:
                    last_stock_trigger = stock_trigger
                    stock_items = await asyncio.to_thread(
                        state_db.get_stock_states,
                        task_id=task_id, offset=0, limit=3000,
                    )
                    yield _sse_event("stock-states", stock_items)
            except Exception:
                pass

            # -- 终态检测 --
            if task["status"] in ("completed", "failed", "stopped"):
                yield _sse_event("done", {"status": task["status"]})
                return

            await asyncio.sleep(2.0)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/maintenance/jobs/{job_id}/stream")
async def maintenance_event_stream(job_id: str, request: Request):
    """SSE 推送：维护任务状态 + 增量日志（维护页）。"""
    state_db = request.app.state.state_db

    async def generate():
        info_cursor: int | None = None
        error_cursor: int | None = None
        last_status_key: tuple | None = None

        yield _sse_event("connected", {"job_id": job_id})

        while True:
            if await request.is_disconnected():
                return

            try:
                job = await asyncio.to_thread(state_db.get_maintenance_job, job_id)
            except Exception:
                yield _sse_event("stream-error", {"detail": "读取维护任务状态失败"})
                return
            if not job:
                yield _sse_event("stream-error", {"detail": f"维护任务不存在: {job_id}"})
                return

            # -- 任务状态 --
            status_data = _maintenance_status_dict(job)
            status_key = (
                status_data["status"],
                status_data["phase"],
                status_data["progress"],
                status_data.get("error_message"),
                json.dumps(status_data["summary"], default=str, sort_keys=True),
            )
            if status_key != last_status_key:
                last_status_key = status_key
                yield _sse_event("job-status", status_data)

            # -- 日志 --
            try:
                if info_cursor is None:
                    info_items = await asyncio.to_thread(
                        state_db.get_maintenance_logs,
                        job_id=job_id, level="info",
                        offset=0, limit=300, after_log_id=None,
                    )
                else:
                    info_items = await asyncio.to_thread(
                        state_db.get_maintenance_logs,
                        job_id=job_id, level="info",
                        offset=0, limit=300, after_log_id=info_cursor,
                    )
                if info_items:
                    info_cursor = int(info_items[-1]["log_id"])
                    yield _sse_event("logs-info", info_items)
                elif info_cursor is None:
                    info_cursor = 0

                if error_cursor is None:
                    error_items = await asyncio.to_thread(
                        state_db.get_maintenance_logs,
                        job_id=job_id, level="error",
                        offset=0, limit=300, after_log_id=None,
                    )
                else:
                    error_items = await asyncio.to_thread(
                        state_db.get_maintenance_logs,
                        job_id=job_id, level="error",
                        offset=0, limit=300, after_log_id=error_cursor,
                    )
                if error_items:
                    error_cursor = int(error_items[-1]["log_id"])
                    yield _sse_event("logs-error", error_items)
                elif error_cursor is None:
                    error_cursor = 0
            except Exception:
                pass

            # -- 终态检测 --
            if job["status"] in ("completed", "failed", "stopped"):
                yield _sse_event("done", {"status": job["status"]})
                return

            await asyncio.sleep(1.0)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/concept/jobs/{job_id}/stream")
async def concept_event_stream(job_id: str, request: Request):
    """SSE 推送：概念任务状态 + 增量日志。"""
    state_db = request.app.state.state_db

    async def generate():
        info_cursor: int | None = None
        error_cursor: int | None = None
        last_status_key: tuple | None = None

        yield _sse_event("connected", {"job_id": job_id})

        while True:
            if await request.is_disconnected():
                return

            try:
                job = await asyncio.to_thread(state_db.get_concept_job, job_id)
            except Exception:
                yield _sse_event("stream-error", {"detail": "读取概念任务状态失败"})
                return
            if not job:
                yield _sse_event("stream-error", {"detail": f"概念任务不存在: {job_id}"})
                return

            status_data = _concept_status_dict(job)
            status_key = (
                status_data["status"],
                status_data["phase"],
                status_data["progress"],
                status_data.get("error_message"),
                json.dumps(status_data["summary"], default=str, sort_keys=True),
            )
            if status_key != last_status_key:
                last_status_key = status_key
                yield _sse_event("job-status", status_data)

            try:
                if info_cursor is None:
                    info_items = await asyncio.to_thread(
                        state_db.get_concept_logs,
                        job_id=job_id, level="info",
                        offset=0, limit=300, after_log_id=None,
                    )
                else:
                    info_items = await asyncio.to_thread(
                        state_db.get_concept_logs,
                        job_id=job_id, level="info",
                        offset=0, limit=300, after_log_id=info_cursor,
                    )
                if info_items:
                    info_cursor = int(info_items[-1]["log_id"])
                    yield _sse_event("logs-info", info_items)
                elif info_cursor is None:
                    info_cursor = 0

                if error_cursor is None:
                    error_items = await asyncio.to_thread(
                        state_db.get_concept_logs,
                        job_id=job_id, level="error",
                        offset=0, limit=300, after_log_id=None,
                    )
                else:
                    error_items = await asyncio.to_thread(
                        state_db.get_concept_logs,
                        job_id=job_id, level="error",
                        offset=0, limit=300, after_log_id=error_cursor,
                    )
                if error_items:
                    error_cursor = int(error_items[-1]["log_id"])
                    yield _sse_event("logs-error", error_items)
                elif error_cursor is None:
                    error_cursor = 0
            except Exception:
                pass

            if job["status"] in ("completed", "failed", "stopped"):
                yield _sse_event("done", {"status": job["status"]})
                return

            await asyncio.sleep(1.0)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/tasks/{task_id}/status-stream")
async def task_status_stream(task_id: str, request: Request):
    """轻量 SSE 推送：仅任务状态（结果页用）。"""
    state_db = request.app.state.state_db

    async def generate():
        last_status_key: tuple | None = None

        yield _sse_event("connected", {"task_id": task_id})

        while True:
            if await request.is_disconnected():
                return

            try:
                task = await asyncio.to_thread(state_db.get_task, task_id)
            except Exception:
                yield _sse_event("stream-error", {"detail": "读取任务状态失败"})
                return
            if not task:
                yield _sse_event("stream-error", {"detail": f"任务不存在: {task_id}"})
                return

            status_data = _task_status_dict(task)
            status_key = (
                status_data["status"],
                status_data["progress"],
                status_data["processed_stocks"],
                status_data["result_count"],
            )
            if status_key != last_status_key:
                last_status_key = status_key
                yield _sse_event("task-status", status_data)

            if task["status"] in ("completed", "failed", "stopped"):
                yield _sse_event("done", {"status": task["status"]})
                return

            await asyncio.sleep(2.0)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
