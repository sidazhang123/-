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
import json
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.db.market_data import MarketDataDB
from app.models.api_models import (
    BacktestControlResponse,
    BacktestCreateRequest,
    BacktestCreateResponse,
    BacktestFormSettingsPayload,
    BacktestHitRecord,
    BacktestHitsResponse,
    BacktestLogsResponse,
    BacktestStatusResponse,
    BacktestSweepResponse,
    BacktestSweepRow,
    ConceptControlResponse,
    ConceptCreateResponse,
    ConceptLogsResponse,
    ConceptStatusResponse,
    CreateTaskRequest,
    CreateTaskResponse,
    DeleteTasksRequest,
    LogsResponse,
    MaintenanceControlResponse,
    MaintenanceCreateRequest,
    MaintenanceCreateResponse,
    MaintenanceFormSettingsPayload,
    MaintenanceLogsResponse,
    MaintenanceStatusResponse,
    MonitorFormSettingsPayload,
    ResultStockConceptEntry,
    ResultTopConceptSummary,
    ResultStockConceptsResponse,
    ResultsResponse,
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
    计算两个时间点的中点。

    输入：
    1. start: 起始时间。
    2. end: 结束时间。
    输出：
    1. start 与 end 的时间中点。
    用途：
    1. 为无明确锚点的信号标记推算展示位置。
    边界条件：
    1. start 与 end 相同时返回其本身。
    """
    return start + (end - start) / 2


def _extract_latest_pattern_window(payload: dict[str, Any]) -> dict[str, Any] | None:
    """从策略 payload 的 patterns 列表中提取最新一条形态窗口。

    输入：
    1. payload: 策略结果的原始负载字典。
    输出：
    1. 返回窗口信息字典，包含 anchor_day、window_start、window_end 的原始值与解析后 datetime。
    2. 无有效 pattern 时返回 None。
    用途：
    1. 为未在顶层明确声明窗口字段的策略提供图表展示区间的回退推导。
    边界条件：
    1. 仅读取列表末尾的 pattern；无效或时间解析失败时跳过。
    """
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


def _validate_params_shape(
    submitted: Any,
    template: Any,
    *,
    path: str = "group_params",
) -> list[str]:
    """
    递归校验提交参数的结构是否与模板一致。

    输入：
    1. submitted: 前端提交的参数值。
    2. template: 策略 manifest 中的 default_params 模板。
    3. path: 当前递归路径，用于报错定位。
    输出：
    1. 返回错误消息列表；空列表表示校验通过。
    用途：
    1. 保存监控页设置时校验 group_params 字段类型、嵌套结构和未知字段。
    边界条件：
    1. template 为 None 时跳过校验；模板为空列表时仅校验 submitted 是否为列表。
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

    if isinstance(template, list):
        if not isinstance(submitted, list):
            return [f"{path} 必须是数组"]
        if not template:
            return errors
        for index, item in enumerate(submitted):
            errors.extend(
                _validate_params_shape(
                    item,
                    template[0],
                    path=f"{path}[{index}]",
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


def _parse_legacy_group_params(raw_value: Any) -> dict[str, Any]:
    if not isinstance(raw_value, str):
        return {}
    text = raw_value.strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return _strip_comment_keys(parsed)


def _normalize_monitor_settings_payload(settings: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(settings) if isinstance(settings, dict) else {}
    group_params = normalized.get("group_params")
    if not isinstance(group_params, dict):
        group_params = _parse_legacy_group_params(normalized.get("group_params_text"))
    normalized["group_params"] = group_params if isinstance(group_params, dict) else {}
    normalized.pop("group_params_text", None)
    # 确保 per_strategy_params 始终返回
    psp = normalized.get("per_strategy_params")
    if not isinstance(psp, dict):
        psp = {}
    # 兼容: 如果 per_strategy_params 为空但 group_params 有值，回填当前策略
    gid = normalized.get("strategy_group_id") or ""
    if gid and group_params and gid not in psp:
        psp[gid] = group_params
    normalized["per_strategy_params"] = psp
    return normalized


def _build_top_concept_summaries(
    concept_map: dict[str, list[dict[str, Any]]],
    *,
    total_hit_stocks: int,
) -> list[ResultTopConceptSummary]:
    concept_counter: dict[str, set[str]] = {}
    for code, entries in concept_map.items():
        code_token = str(code or "").strip()
        if not code_token:
            continue
        seen_boards: set[str] = set()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            board_name = str(entry.get("board_name") or "").strip()
            if not board_name or board_name in seen_boards:
                continue
            seen_boards.add(board_name)
            concept_counter.setdefault(board_name, set()).add(code_token)

    sorted_items = sorted(
        concept_counter.items(),
        key=lambda item: (-len(item[1]), item[0]),
    )
    return [
        ResultTopConceptSummary(
            board_name=board_name,
            hit_stock_count=len(code_set),
            total_hit_stocks=total_hit_stocks,
        )
        for board_name, code_set in sorted_items[:3]
    ]


@router.get("/health")
def health() -> dict[str, str]:
    """
    输入：
    1. 无显式输入参数。
    输出：
    1. 固定返回 `{"status": "ok"}`。
    用途：
    1. 服务健康检查。
    边界条件：
    1. 无。
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
        from zsdtdx.simple_api import get_runtime_metadata
        meta = get_runtime_metadata()
        return {"ok": True, "metadata": meta}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "metadata": {}}


@router.get("/strategy-groups")
def list_strategy_groups(request: Request) -> dict[str, Any]:
    """
    输入：
    1. request: 请求上下文。
    输出：
    1. 包含 items 列表的字典，每项为策略组摘要。
    用途：
    1. 供前端下拉菜单加载全部策略组。
    边界条件：
    1. 无策略组时返回空列表。
    """
    registry = request.app.state.strategy_registry
    return {"items": registry.list_groups()}


@router.get("/strategy-groups/{group_id}")
def get_strategy_group(group_id: str, request: Request) -> dict[str, Any]:
    """
    输入：
    1. group_id: 策略组 ID。
    2. request: 请求上下文。
    输出：
    1. 策略组完整 manifest 信息。
    用途：
    1. 读取单个策略组的参数模板、帮助文本等详情。
    边界条件：
    1. 策略组不存在时返回 404。
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
    1. request: 请求上下文。
    输出：
    1. 监控页表单配置字典，含 group_params 与 per_strategy_params。
    用途：
    1. 前端加载监控页已保存的表单状态。
    边界条件：
    1. 未保存过时返回空字典。
    """
    state_db = request.app.state.state_db
    return {"settings": _normalize_monitor_settings_payload(state_db.get_monitor_form_settings())}


@router.post("/ui-settings/monitor")
def save_monitor_ui_settings(payload: MonitorFormSettingsPayload, request: Request) -> dict[str, Any]:
    """
    输入：
    1. payload: 监控页表单配置。
    2. request: 请求上下文。
    输出：
    1. 保存后的完整设置字典。
    用途：
    1. 持久化监控页表单配置，并同步更新 per_strategy_params。
    边界条件：
    1. 策略组不存在或参数校验失败时返回 400。
    """
    state_db = request.app.state.state_db
    registry = request.app.state.strategy_registry
    settings = payload.model_dump()

    group_id = str(settings.get("strategy_group_id") or "").strip()
    if not group_id:
        raise HTTPException(status_code=400, detail="strategy_group_id 不能为空")

    try:
        meta = registry.get_group_meta(group_id)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    group_params = settings.get("group_params")
    if not isinstance(group_params, dict):
        raise HTTPException(status_code=400, detail="group_params 必须是对象")

    data_errors = _validate_params_shape(group_params, meta.default_params, path="group_params")
    if data_errors:
        raise HTTPException(
            status_code=400,
            detail="参数数据不合法: " + "；".join(data_errors[:8]),
        )

    try:
        registry.merge_group_params(group_id, group_params)
    except StrategyRegistryError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    settings["strategy_group_id"] = group_id
    settings["group_params"] = group_params

    # ── 同步维护 per_strategy_params ──
    psp = settings.get("per_strategy_params")
    if not isinstance(psp, dict):
        existing = state_db.get_monitor_form_settings()
        psp = existing.get("per_strategy_params") if isinstance(existing, dict) else {}
        if not isinstance(psp, dict):
            psp = {}
    psp[group_id] = group_params
    settings["per_strategy_params"] = psp

    state_db.set_monitor_form_settings(settings)
    return {"settings": settings}


@router.get("/ui-settings/maintenance")
def get_maintenance_ui_settings(request: Request) -> dict[str, Any]:
    """
    输入：
    1. request: 请求上下文。
    输出：
    1. 维护页表单配置字典。
    用途：
    1. 前端加载维护页已保存的表单状态。
    边界条件：
    1. 未保存过时返回空字典。
    """
    state_db = request.app.state.state_db
    return {"settings": state_db.get_maintenance_form_settings()}


@router.post("/ui-settings/maintenance")
def save_maintenance_ui_settings(payload: MaintenanceFormSettingsPayload, request: Request) -> dict[str, Any]:
    """
    输入：
    1. payload: 维护页表单配置。
    2. request: 请求上下文。
    输出：
    1. 保存后的完整设置字典。
    用途：
    1. 持久化维护页表单配置。
    边界条件：
    1. 无。
    """
    state_db = request.app.state.state_db
    settings = payload.model_dump()
    state_db.set_maintenance_form_settings(settings)
    return {"settings": settings}


@router.post("/tasks", response_model=CreateTaskResponse)
def create_task(payload: CreateTaskRequest, request: Request) -> CreateTaskResponse:
    """
    输入：
    1. payload: 任务创建参数（策略、时间窗口等）。
    2. request: 请求上下文。
    输出：
    1. 新任务 ID。
    用途：
    1. 创建筛选任务并异步执行，默认全市场扫描。
    边界条件：
    1. 维护/概念任务运行中时返回 409；参数不合法返回 400。
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
            stocks=[],
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
        return ResultStockConceptsResponse(task_id=task_id, formula=formula, top_concepts=[], items={})

    market_db = MarketDataDB(task["source_db"])
    raw_map = market_db.get_stock_concepts_by_codes(codes)
    top_concepts = _build_top_concept_summaries(raw_map, total_hit_stocks=len(codes))
    items = {
        code: [ResultStockConceptEntry(**entry) for entry in entries]
        for code, entries in raw_map.items()
    }
    return ResultStockConceptsResponse(task_id=task_id, formula=formula, top_concepts=top_concepts, items=items)


@router.post("/tasks/{task_id}/pause", response_model=TaskControlResponse)
def pause_task(task_id: str, request: Request) -> TaskControlResponse:
    """
    输入：
    1. task_id: 筛选任务 ID。
    2. request: 请求上下文。
    输出：
    1. 任务控制响应（含新状态）。
    用途：
    1. 暂停运行中的筛选任务。
    边界条件：
    1. 任务不存在时返回 404。
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
    1. task_id: 筛选任务 ID。
    2. request: 请求上下文。
    输出：
    1. 任务控制响应（含新状态）。
    用途：
    1. 恢复已暂停的筛选任务。
    边界条件：
    1. 任务不存在时返回 404。
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
    1. task_id: 筛选任务 ID。
    2. request: 请求上下文。
    输出：
    1. 任务控制响应（含新状态）。
    用途：
    1. 停止运行中或已暂停的筛选任务。
    边界条件：
    1. 任务不存在时返回 404。
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
    1. request: 请求上下文。
    2. offset: 分页起始偏移。
    3. limit: 每页最大条数。
    输出：
    1. 包含 items 列表的任务列表字典。
    用途：
    1. 分页读取筛选任务列表。
    边界条件：
    1. 无任务时返回空列表。
    """
    state_db = request.app.state.state_db
    return {"items": state_db.list_tasks(offset=offset, limit=limit)}


@router.delete("/tasks")
def delete_tasks(body: DeleteTasksRequest, request: Request) -> dict[str, Any]:
    """批量删除终态筛选任务及其关联日志和结果。"""
    state_db = request.app.state.state_db
    deleted = state_db.delete_tasks(body.task_ids)
    return {"deleted": deleted}


@router.get("/tasks/{task_id}/params")
def get_task_params(task_id: str, request: Request) -> dict[str, Any]:
    """
    输入：
    1. task_id: 筛选任务 ID。
    2. request: FastAPI 请求对象。
    输出：
    1. 包含 group_params、param_help、run_mode 等字段的字典。
    用途：
    1. 读取任务参数快照；若数据库中 param_help 为空，则从当前策略清单兜底。
    边界条件：
    1. 策略组已被删除时 param_help 仍会为 None，前端需容忍。
    """
    state_db = request.app.state.state_db
    task = state_db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
    params = task.get("params") or {}
    param_help = params.get("param_help")
    strategy_group_id = task.get("strategy_group_id") or ""
    strategy_name = task.get("strategy_name") or ""
    # 兜底：数据库中未存储 param_help 时，尝试从当前策略清单获取
    if param_help is None and strategy_group_id:
        registry = request.app.state.strategy_registry
        try:
            meta = registry.get_group_meta(strategy_group_id)
            param_help = meta.param_help
        except (KeyError, Exception):
            pass
    return {
        "group_params": params.get("group_params") or {},
        "param_help": param_help,
        "strategy_group_id": strategy_group_id,
        "strategy_name": strategy_name,
        "run_mode": params.get("run_mode") or "full",
        "sample_size": params.get("sample_size"),
        "skip_coverage_filter": params.get("skip_coverage_filter"),
    }


@router.get("/tasks/{task_id}", response_model=TaskStatusResponse)
def get_task(task_id: str, request: Request) -> TaskStatusResponse:
    """
    输入：
    1. task_id: 筛选任务 ID。
    2. request: 请求上下文。
    输出：
    1. 筛选任务状态响应。
    用途：
    1. 读取单个筛选任务的当前状态与进度。
    边界条件：
    1. 任务不存在时返回 404。
    """
    state_db = request.app.state.state_db
    task = state_db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
    return TaskStatusResponse(**_task_status_dict(task))


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
    1. task_id: 筛选任务 ID。
    2. level: 日志级别过滤（all/info/error）。
    3. after_log_id: 增量游标，仅返回该 ID 之后的日志。
    4. offset: 分页偏移。
    5. limit: 每页最大条数。
    输出：
    1. 日志列表与下次轮询游标。
    用途：
    1. 前端分页或增量拉取筛选任务日志。
    边界条件：
    1. 任务不存在时返回 404。
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
    1. task_id: 筛选任务 ID。
    2. offset: 分页偏移。
    3. limit: 每页最大条数。
    输出：
    1. 结果列表与分页信息。
    用途：
    1. 分页读取筛选任务命中结果。
    边界条件：
    1. 任务不存在时返回 404。
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
    1. task_id: 筛选任务 ID。
    2. request: 请求上下文。
    输出：
    1. 包含命中股票汇总列表。
    用途：
    1. 结果页左侧股票列表加载。
    边界条件：
    1. 任务不存在时返回 404。
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
    1. task_id: 筛选任务 ID。
    2. code: 股票代码。
    3. timeframe: K 线周期。
    4. padding_bars: 窗口前后补充 bar 数。
    5. interval_bars: 信号默认展开 bar 数。
    输出：
    1. K 线数据、信号标注、高亮区间及策略元信息。
    用途：
    1. 结果页单股图表展示。
    边界条件：
    1. 任务不存在返回 404；不支持的周期返回 400；无信号时返回空列表。
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

        # overlay_lines: 策略可选的斜线覆盖层（如三角形上下沿拟合线）
        overlay_lines_raw = payload.get("overlay_lines") if isinstance(payload.get("overlay_lines"), list) else []
        overlay_lines_out: list[dict[str, Any]] = []
        for ol in overlay_lines_raw:
            if not isinstance(ol, dict):
                continue
            ol_start = _coerce_datetime(ol.get("start_ts"))
            ol_end = _coerce_datetime(ol.get("end_ts"))
            if ol_start and ol_end:
                overlay_lines_out.append({
                    "start_ts": ol_start,
                    "end_ts": ol_end,
                    "start_price": ol.get("start_price"),
                    "end_price": ol.get("end_price"),
                    "color": ol.get("color", "#fbbf24"),
                    "dash": ol.get("dash", True),
                    "label": ol.get("label", ""),
                })

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
                "overlay_lines": overlay_lines_out,
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

@router.post("/maintenance/jobs", response_model=MaintenanceCreateResponse)
def create_maintenance_job(payload: MaintenanceCreateRequest, request: Request) -> MaintenanceCreateResponse:
    """
    输入：
    1. payload: 维护任务创建参数（mode）。
    2. request: 请求上下文。
    输出：
    1. 新维护任务 ID。
    用途：
    1. 创建 K 线维护任务并异步执行。
    边界条件：
    1. 参数不合法时返回 400。
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
            "mode": item.get("mode"),
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
    1. request: 请求上下文。
    2. offset: 分页偏移。
    3. limit: 每页最大条数。
    输出：
    1. 维护任务列表。
    用途：
    1. 分页读取维护任务历史。
    边界条件：
    1. 无任务时返回空列表。
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
    1. job_id: 维护任务 ID。
    2. request: 请求上下文。
    输出：
    1. 维护任务状态响应。
    用途：
    1. 读取单个维护任务的当前状态与摘要。
    边界条件：
    1. 任务不存在时返回 404。
    """
    state_db = request.app.state.state_db
    job = state_db.get_maintenance_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"维护任务不存在: {job_id}")
    return MaintenanceStatusResponse(**_maintenance_status_dict(job))


@router.post("/maintenance/jobs/{job_id}/stop", response_model=MaintenanceControlResponse)
def stop_maintenance_job(job_id: str, request: Request) -> MaintenanceControlResponse:
    """
    输入：
    1. job_id: 维护任务 ID。
    2. request: 请求上下文。
    输出：
    1. 任务控制响应（含新状态）。
    用途：
    1. 停止运行中的维护任务。
    边界条件：
    1. 任务不存在时返回 404。
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
    1. job_id: 维护任务 ID。
    2. level: 日志级别过滤（all/info/error）。
    3. after_log_id: 增量游标。
    4. offset: 分页偏移。
    5. limit: 每页最大条数。
    输出：
    1. 日志列表与下次轮询游标。
    用途：
    1. 前端分页或增量拉取维护任务日志。
    边界条件：
    1. 任务不存在时返回 404。
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
        "mode": str(job.get("mode") or "concept_update"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "error_message": job.get("error_message"),
        "summary": summary,
    }


@router.get("/tasks/{task_id}/stream")
async def task_event_stream(task_id: str, request: Request):
    """SSE 推送：筛选任务状态 + 增量日志（监控页）。"""
    state_db = request.app.state.state_db

    async def generate():
        info_cursor: int | None = None
        error_cursor: int | None = None
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


# ---------------------------------------------------------------------------
# 回测 (Backtest)
# ---------------------------------------------------------------------------


@router.post("/backtests", response_model=BacktestCreateResponse)
def create_backtest_job(
    payload: BacktestCreateRequest, request: Request
) -> BacktestCreateResponse:
    # ── 校验策略是否支持回测 ──
    registry = request.app.state.strategy_registry
    try:
        meta = registry.get_group_meta(payload.strategy_group_id)
    except (KeyError, Exception):
        raise HTTPException(status_code=400, detail="策略组不存在")
    if "backtest" not in meta.usage:
        raise HTTPException(status_code=400, detail="该策略不支持回测")

    # ── 校验 param_ranges 不含 scope 路径 ──
    if payload.param_ranges and payload.group_params:
        scope_paths: set[str] = set()
        for section_key, section_val in payload.group_params.items():
            if isinstance(section_val, dict):
                for sp in section_val.get("scope_params", []):
                    scope_paths.add(f"{section_key}.{sp}")
        bad = [p for p in payload.param_ranges if p in scope_paths]
        if bad:
            raise HTTPException(
                status_code=400,
                detail=f"param_ranges 不得包含 scope 参数: {', '.join(bad)}",
            )
    manager = request.app.state.backtest_manager
    try:
        job_id = manager.create_job(
            strategy_group_id=payload.strategy_group_id,
            mode=payload.mode,
            forward_bars=tuple(payload.forward_bars),
            slide_step=payload.slide_step,
            group_params=payload.group_params,
            param_ranges={
                k: v.model_dump() for k, v in payload.param_ranges.items()
            } if payload.param_ranges else {},
            lock_fwd_cache=payload.lock_fwd_cache,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return BacktestCreateResponse(job_id=job_id)


@router.get("/backtests")
def list_backtest_jobs(request: Request) -> dict[str, Any]:
    """返回回测任务列表 (最近30条) + 当前运行中的任务。"""
    state_db = request.app.state.state_db
    registry = request.app.state.strategy_registry
    jobs = state_db.list_backtest_jobs(limit=30)
    running = state_db.get_running_backtest_job()
    # 增补 strategy_name 并格式化时间
    for j in jobs:
        gid = j.get("strategy_group_id") or ""
        try:
            j["strategy_name"] = registry.get_group_meta(gid).name if gid else ""
        except (KeyError, Exception):
            j["strategy_name"] = ""
        for k in ("created_at", "finished_at"):
            if isinstance(j.get(k), datetime):
                j[k] = j[k].isoformat()
    return {
        "jobs": jobs,
        "running_job_id": running["job_id"] if running else None,
    }


@router.get("/backtests/{job_id}/logs")
def get_backtest_logs(
    job_id: str,
    request: Request,
    level: str = Query(default="info", description="info/error"),
    after_log_id: int = Query(default=0, description="增量游标"),
) -> dict[str, Any]:
    """非 SSE 日志查询 (用于刷新后恢复)。"""
    manager = request.app.state.backtest_manager
    logs = manager.get_logs(job_id, level, after_log_id)
    return {"items": logs}


@router.post("/backtests/{job_id}/stop", response_model=BacktestControlResponse)
def stop_backtest_job(job_id: str, request: Request) -> BacktestControlResponse:
    manager = request.app.state.backtest_manager
    ok = manager.stop_job(job_id)
    if not ok:
        raise HTTPException(status_code=404, detail="回测任务不存在或不可停止")
    return BacktestControlResponse(job_id=job_id, status="failed", message="回测任务已强制终止")


@router.get("/backtests/{job_id}/params")
def get_backtest_job_params(job_id: str, request: Request) -> dict[str, Any]:
    """返回回测任务的参数快照（group_params + param_help + 回测专有参数）。

    输入：
    1. job_id: 回测任务 ID。
    2. request: FastAPI 请求对象。
    输出：
    1. 包含 group_params、param_help、forward_bars、slide_step 等字段的字典。
    用途：
    1. "参" 按钮点击后展示回测任务参数快照。
    边界条件：
    1. param_help 从策略注册表兜底获取；策略组被删除时 param_help 为 None。
    """
    state_db = request.app.state.state_db
    job = state_db.get_backtest_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"回测任务不存在: {job_id}")

    strategy_group_id = job.get("strategy_group_id") or ""
    param_help = None
    strategy_name = ""
    if strategy_group_id:
        registry = request.app.state.strategy_registry
        try:
            meta = registry.get_group_meta(strategy_group_id)
            param_help = meta.param_help
            strategy_name = meta.name
        except (KeyError, Exception):
            pass

    return {
        "group_params": job.get("group_params") or {},
        "param_help": param_help,
        "strategy_group_id": strategy_group_id,
        "strategy_name": strategy_name,
        "mode": job.get("mode") or "fixed",
        "forward_bars": job.get("forward_bars") or [2, 5, 7],
        "slide_step": job.get("slide_step") or 1,
        "param_ranges": job.get("param_ranges") or {},
    }


@router.delete("/backtests")
def delete_backtest_jobs(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    """批量删除回测任务及其日志和缓存目录。

    输入：
    1. payload: { "job_ids": ["id1", "id2", ...] }。
    输出：
    1. { "deleted": 实际删除数 }。
    用途：
    1. 任务管理页批量删除回测任务。
    边界条件：
    1. 跳过正在运行的任务（queued / running / stopping）。
    """
    import shutil

    job_ids = payload.get("job_ids") or []
    if not job_ids:
        return {"deleted": 0}

    state_db = request.app.state.state_db
    cache_dir = request.app.state.backtest_cache_dir
    deleted = 0
    for jid in job_ids:
        job = state_db.get_backtest_job(jid)
        if not job:
            continue
        if job["status"] in ("queued", "running", "stopping"):
            continue
        state_db.delete_backtest_job(jid)
        # 清理缓存目录
        job_cache = cache_dir / jid
        if job_cache.exists():
            shutil.rmtree(job_cache, ignore_errors=True)
        deleted += 1
    return {"deleted": deleted}


@router.get("/backtests/{job_id}/status", response_model=BacktestStatusResponse)
def get_backtest_status(job_id: str, request: Request) -> BacktestStatusResponse:
    manager = request.app.state.backtest_manager
    status = manager.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="回测任务不存在")
    return BacktestStatusResponse(**status)


@router.get("/backtests/{job_id}/stream")
async def backtest_event_stream(job_id: str, request: Request):
    """SSE 推送: 回测任务状态 + 增量日志。"""
    manager = request.app.state.backtest_manager
    info_after_log_id = 0
    error_after_log_id = 0
    try:
        info_after_log_id = max(0, int(request.query_params.get("info_after_log_id", "0")))
    except (TypeError, ValueError):
        info_after_log_id = 0
    try:
        error_after_log_id = max(0, int(request.query_params.get("error_after_log_id", "0")))
    except (TypeError, ValueError):
        error_after_log_id = 0

    async def generate():
        info_cursor = info_after_log_id
        error_cursor = error_after_log_id
        last_status_key: tuple | None = None

        yield _sse_event("connected", {"job_id": job_id})

        while True:
            if await request.is_disconnected():
                return

            status = await asyncio.to_thread(manager.get_job_status, job_id)
            if not status:
                yield _sse_event("stream-error", {"message": "任务不存在"})
                return

            status_key = (
                status["status"], status["progress"],
                status["processed_stocks"], status["total_stocks"], status["combo_index"],
                status.get("phase"), status.get("phase_label"),
                status.get("phase_index"), status.get("phase_total"),
            )
            if status_key != last_status_key:
                last_status_key = status_key
                yield _sse_event("job-status", status)

            info_items = await asyncio.to_thread(
                manager.get_logs, job_id, "info", info_cursor
            )
            if info_items:
                info_cursor = info_items[-1]["log_id"]
                yield _sse_event("logs-info", info_items)

            error_items = await asyncio.to_thread(
                manager.get_logs, job_id, "error", error_cursor
            )
            if error_items:
                error_cursor = error_items[-1]["log_id"]
                yield _sse_event("logs-error", error_items)

            if status["status"] in ("completed", "failed", "stopped"):
                yield _sse_event("done", {"status": status["status"]})
                return

            await asyncio.sleep(1.0)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/backtests/{job_id}/hits")
def get_backtest_hits(
    job_id: str,
    request: Request,
    sort_by: str = Query(default="profit_z_pct", description="排序字段"),
    sort_order: str = Query(default="desc", description="asc/desc"),
    combo_index: int = Query(default=-1, description="sweep 组合索引 (-1=非sweep)"),
) -> BacktestHitsResponse:
    """返回命中记录 (从 CSV 读取)。"""
    import duckdb

    cache_dir = request.app.state.backtest_cache_dir
    if combo_index >= 0:
        csv_path = cache_dir / job_id / f"combo_{combo_index}" / "hits.csv"
    else:
        csv_path = cache_dir / job_id / "hits.csv"

    if not csv_path.exists():
        return BacktestHitsResponse(total_count=0, items=[])

    con = duckdb.connect()
    try:
        order = "DESC" if sort_order.lower() == "desc" else "ASC"
        allowed_cols = {
            "code", "name", "tf_key", "pattern_start_ts", "pattern_end_ts",
            "buy_price", "profit_x_pct", "drawdown_x_pct", "sharpe_x",
            "profit_y_pct", "drawdown_y_pct", "sharpe_y",
            "profit_z_pct", "drawdown_z_pct", "sharpe_z",
        }
        if sort_by not in allowed_cols:
            sort_by = "profit_z_pct"

        df = con.execute(
            f"SELECT * FROM read_csv_auto('{csv_path.as_posix()}') "
            f"ORDER BY \"{sort_by}\" {order} NULLS LAST"
        ).fetchdf()
    finally:
        con.close()

    # 兼容旧缓存: 盈利/回撤钳位 ≥ 0
    for col in df.columns:
        if col.startswith(("profit_", "drawdown_")) and col.endswith("_pct"):
            df[col] = df[col].clip(lower=0)

    items = []
    for _, row in df.iterrows():
        items.append(BacktestHitRecord(
            code=row.get("code") or "",
            name=row.get("name") or "",
            tf_key=row.get("tf_key") or "",
            pattern_start_ts=row.get("pattern_start_ts"),
            pattern_end_ts=row.get("pattern_end_ts"),
            buy_price=row.get("buy_price", 0),
            profit_x_pct=_bt_safe_val(row.get("profit_x_pct")),
            drawdown_x_pct=_bt_safe_val(row.get("drawdown_x_pct")),
            sharpe_x=_bt_safe_val(row.get("sharpe_x")),
            profit_y_pct=_bt_safe_val(row.get("profit_y_pct")),
            drawdown_y_pct=_bt_safe_val(row.get("drawdown_y_pct")),
            sharpe_y=_bt_safe_val(row.get("sharpe_y")),
            profit_z_pct=_bt_safe_val(row.get("profit_z_pct")),
            drawdown_z_pct=_bt_safe_val(row.get("drawdown_z_pct")),
            sharpe_z=_bt_safe_val(row.get("sharpe_z")),
        ))

    return BacktestHitsResponse(total_count=len(items), items=items)


@router.get("/backtests/{job_id}/stats")
def get_backtest_stats(job_id: str, request: Request) -> dict[str, Any]:
    """返回统计分析结果。"""
    cache_dir = request.app.state.backtest_cache_dir
    json_path = cache_dir / job_id / "stats.json"

    if not json_path.exists():
        raise HTTPException(status_code=404, detail="统计结果不存在")

    data = json.loads(json_path.read_text(encoding="utf-8"))
    # 兼容旧缓存: 盈利/回撤统计钳位 ≥ 0
    _clamp_stats_per_forward(data)
    return data


@router.get("/backtests/{job_id}/sweep")
def get_backtest_sweep(job_id: str, request: Request) -> BacktestSweepResponse:
    """返回 sweep 参数组合结果表。"""
    cache_dir = request.app.state.backtest_cache_dir
    json_path = cache_dir / job_id / "sweep_results.json"

    if not json_path.exists():
        raise HTTPException(status_code=404, detail="sweep 结果不存在")

    data = json.loads(json_path.read_text(encoding="utf-8"))
    items = [BacktestSweepRow(**row) for row in data]
    return BacktestSweepResponse(combo_total=len(items), items=items)


@router.get("/backtests/{job_id}/sweep-stats")
def get_backtest_sweep_stats(job_id: str, request: Request) -> dict[str, Any]:
    """返回 sweep 所有参数组合的详细统计（per-forward profit/drawdown/sharpe）。

    读取每个 combo_*/stats.json，提取 mean/median/max 汇总指标，
    供前端渲染参数组合对比表格和散点图。
    """
    cache_dir = request.app.state.backtest_cache_dir
    job_dir = cache_dir / job_id

    sweep_path = job_dir / "sweep_results.json"
    if not sweep_path.exists():
        raise HTTPException(status_code=404, detail="sweep 结果不存在")

    sweep_rows = json.loads(sweep_path.read_text(encoding="utf-8"))

    # 从任务记录获取 forward_bars 映射
    state_db = request.app.state.state_db
    job = state_db.get_backtest_job(job_id)
    forward_bars = job.get("forward_bars", [2, 5, 7]) if job else [2, 5, 7]
    labels = ["x", "y", "z"]
    forward_labels = {str(fb): labels[i] for i, fb in enumerate(forward_bars) if i < 3}

    combos: list[dict[str, Any]] = []
    for row in sweep_rows:
        combo_idx = row.get("combo_index", 0)
        stats_path = job_dir / f"combo_{combo_idx}" / "stats.json"

        entry: dict[str, Any] = {
            "combo_index": combo_idx,
            "param_combo": row.get("param_combo", {}),
            "total_hits": row.get("total_hits", 0),
        }

        if stats_path.exists():
            stats = json.loads(stats_path.read_text(encoding="utf-8"))
            entry["unique_stocks"] = stats.get("unique_stocks", 0)
            per_fwd: dict[str, Any] = {}
            for fwd_key, fwd_data in (stats.get("per_forward") or {}).items():
                profit = fwd_data.get("profit", {})
                drawdown = fwd_data.get("drawdown", {})
                sharpe = fwd_data.get("sharpe", {})
                per_fwd[fwd_key] = {
                    "profit_mean": max(profit.get("mean") or 0, 0),
                    "profit_median": max(profit.get("median") or 0, 0),
                    "profit_max": max(profit.get("max") or 0, 0),
                    "drawdown_mean": max(drawdown.get("mean") or 0, 0),
                    "drawdown_median": max(drawdown.get("median") or 0, 0),
                    "drawdown_max": max(drawdown.get("max") or 0, 0),
                    "sharpe_mean": sharpe.get("mean"),
                    "sharpe_median": sharpe.get("median"),
                    "win_rate": profit.get("win_rate"),
                }
            entry["per_forward"] = per_fwd
        else:
            entry["unique_stocks"] = 0
            entry["per_forward"] = {}

        combos.append(entry)

    return {
        "forward_bars": forward_bars,
        "forward_labels": forward_labels,
        "combos": combos,
    }


@router.get("/backtests/{job_id}/stock-chart")
def get_backtest_stock_chart(
    job_id: str,
    request: Request,
    code: str = Query(..., description="股票代码"),
    tf_key: str = Query(default="d", description="周期"),
    combo_index: int = Query(default=-1, description="sweep 组合索引 (-1=非sweep)"),
) -> dict[str, Any]:
    """返回 K线 + 命中高亮区间。"""
    cache_dir = request.app.state.backtest_cache_dir

    if combo_index >= 0:
        csv_path = cache_dir / job_id / f"combo_{combo_index}" / "hits.csv"
    else:
        csv_path = cache_dir / job_id / "hits.csv"

    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="命中数据不存在")

    import duckdb

    con = duckdb.connect()
    try:
        hits_df = con.execute(
            f"SELECT pattern_start_ts, pattern_end_ts "
            f"FROM read_csv_auto('{csv_path.as_posix()}') "
            f"WHERE code = ? AND tf_key = ? "
            f"ORDER BY pattern_start_ts",
            [code, tf_key],
        ).fetchdf()
    finally:
        con.close()

    table_name = TIMEFRAME_TO_TABLE.get(tf_key, f"klines_{tf_key}")
    market_db = MarketDataDB(request.app.state.source_db_path)
    candles = market_db.fetch_candles_for_chart(
        code=code,
        timeframe=tf_key,
        start_ts=None,
        end_ts=None,
        center_ts=None,
        window=800,
    )

    intervals = []
    for _, row in hits_df.iterrows():
        intervals.append({
            "start_ts": str(row["pattern_start_ts"]),
            "end_ts": str(row["pattern_end_ts"]),
        })

    return {
        "code": code,
        "tf_key": tf_key,
        "candles": candles,
        "hit_intervals": intervals,
    }


@router.get("/ui-settings/backtest")
def get_backtest_ui_settings(request: Request) -> dict[str, Any]:
    state_db = request.app.state.state_db
    raw = state_db.get_backtest_form_settings()
    # 兼容旧格式: strategy_settings → per_strategy_sweep_ranges
    if isinstance(raw, dict) and "strategy_settings" in raw and "per_strategy_sweep_ranges" not in raw:
        old_ss = raw.pop("strategy_settings", {})
        pssr: dict[str, Any] = {}
        if isinstance(old_ss, dict):
            for gid, val in old_ss.items():
                if isinstance(val, dict) and "sweep_ranges" in val:
                    pssr[gid] = val["sweep_ranges"]
        raw["per_strategy_sweep_ranges"] = pssr
        raw.pop("sweep_ranges", None)
    return {"settings": raw}


@router.post("/ui-settings/backtest")
def save_backtest_ui_settings(
    payload: BacktestFormSettingsPayload, request: Request
) -> dict[str, Any]:
    state_db = request.app.state.state_db
    settings_data = payload.model_dump()

    # ── 将 group_params 交叉同步到 monitor 的 per_strategy_params ──
    group_id = (settings_data.get("strategy_group_id") or "").strip()
    group_params = settings_data.get("group_params")
    if group_id and isinstance(group_params, dict) and group_params:
        monitor_raw = state_db.get_monitor_form_settings()
        if not isinstance(monitor_raw, dict):
            monitor_raw = {}
        psp = monitor_raw.get("per_strategy_params")
        if not isinstance(psp, dict):
            psp = {}
        psp[group_id] = group_params
        monitor_raw["per_strategy_params"] = psp
        # 如果 monitor 当前选中的也是同一策略，同步 group_params
        if monitor_raw.get("strategy_group_id") == group_id:
            monitor_raw["group_params"] = group_params
        state_db.set_monitor_form_settings(monitor_raw)

    # 不存储 group_params 到回测设置中（单一来源在 monitor）
    settings_data.pop("group_params", None)
    state_db.set_backtest_form_settings(settings_data)
    return {"settings": settings_data}


def _clamp_stats_per_forward(data: dict[str, Any]) -> None:
    """兼容旧缓存: 将 per_forward 中 profit/drawdown 统计钳位至 ≥ 0。"""
    _CLAMP_KEYS = {"mean", "median", "max", "min", "q25", "q75", "std"}
    for _fwd_key, fwd_data in (data.get("per_forward") or {}).items():
        for metric_name in ("profit", "drawdown"):
            bucket = fwd_data.get(metric_name)
            if not isinstance(bucket, dict):
                continue
            for k in _CLAMP_KEYS:
                v = bucket.get(k)
                if isinstance(v, (int, float)) and v < 0:
                    bucket[k] = 0.0
            # 直方图: 钳位 bin 下界及计数不受影响 (只是移除了负区间的意义)
            hist = bucket.get("histogram")
            if isinstance(hist, list):
                bucket["histogram"] = [
                    {**b, "bin_start": max(b.get("bin_start", 0), 0),
                     "bin_end": max(b.get("bin_end", 0), 0)}
                    if b.get("bin_start", 0) < 0 or b.get("bin_end", 0) < 0
                    else b
                    for b in hist
                ]


def _bt_safe_val(v: Any) -> float | None:
    """NaN → None 转换 (回测专用)。"""
    if v is None:
        return None
    try:
        import math
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return None
