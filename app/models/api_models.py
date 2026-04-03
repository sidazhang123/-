"""
API 数据模型定义（Pydantic）。

职责：
1. 定义请求体与响应体字段结构，保证接口契约稳定。
2. 在边界层执行基础校验（字段类型、取值范围、额外字段禁止）。
3. 为前端与自动化脚本提供一致的数据格式说明。

说明：
1. 模型字段命名与路由返回 JSON 保持一一对应。
2. 模型变更属于接口变更，需同步更新文档与前端调用。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class CreateTaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stocks: list[str] = Field(default_factory=list, description="股票代码或中文名称列表")
    start_ts: datetime | None = None
    end_ts: datetime | None = None
    source_db: str | None = None
    run_mode: Literal["full", "sample20"] = "full"
    sample_size: int = Field(default=20, ge=1, le=5000)
    strategy_group_id: str = Field(min_length=1)
    group_params: dict[str, Any] = Field(default_factory=dict)
    skip_coverage_filter: bool = Field(default=True, description="跳过数据完整性过滤（默认跳过）")


class CreateTaskResponse(BaseModel):
    task_id: str


class DeleteTasksRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_ids: list[str] = Field(min_length=1, description="要删除的任务 ID 列表")


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    progress: float
    total_stocks: int
    processed_stocks: int
    result_count: int
    info_log_count: int
    error_log_count: int
    current_code: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    unresolved_inputs: list[str] = Field(default_factory=list)
    run_mode: Literal["full", "sample20"] = "full"
    strategy_group_id: str
    strategy_name: str
    strategy_description: str


class LogItem(BaseModel):
    log_id: int | None = None
    ts: datetime | None = None
    level: str = ""
    message: str = ""
    detail: dict[str, Any] | None = None


class LogsResponse(BaseModel):
    items: list[LogItem]
    next_after_log_id: int | None = None


class ResultsResponse(BaseModel):
    total_count: int
    offset: int
    limit: int
    has_more: bool
    items: list[dict[str, Any]]

class TaskControlResponse(BaseModel):
    task_id: str
    status: str
    message: str


class MonitorFormSettingsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_db: str = ""
    stocks_input: str = ""
    sample_size: int = Field(default=20, ge=1, le=5000)
    strategy_group_id: str = ""
    group_params: dict[str, Any] = Field(default_factory=dict)


class MaintenanceCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["latest_update", "historical_backfill"] = "latest_update"


class MaintenanceCreateResponse(BaseModel):
    job_id: str


class MaintenanceStatusResponse(BaseModel):
    class MaintenanceSummaryPayload(BaseModel):
        model_config = ConfigDict(extra="allow")

        steps_total: int = 0
        steps_completed: int = 0
        total_tasks: int = 0
        success_tasks: int = 0
        failed_tasks: int = 0
        retry_rounds_used: int = 0
        rows_written: dict[str, int] = Field(default_factory=dict)
        retry_skipped_tasks: int = 0
        removed_corrupted_rows: int = 0
        duration_seconds: float = 0.0

    job_id: str
    status: str
    phase: str | None = None
    progress: float = 0.0
    mode: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    summary: MaintenanceSummaryPayload = Field(default_factory=MaintenanceSummaryPayload)


class MaintenanceControlResponse(BaseModel):
    job_id: str
    status: str
    message: str


class MaintenanceLogsResponse(BaseModel):
    items: list[LogItem]
    next_after_log_id: int | None = None


class MaintenanceFormSettingsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["latest_update", "historical_backfill"] = "latest_update"
    info_log_autoscroll: bool = True
    error_log_autoscroll: bool = True


class ConceptCreateResponse(BaseModel):
    job_id: str


class ConceptStatusResponse(BaseModel):
    class ConceptSummaryPayload(BaseModel):
        model_config = ConfigDict(extra="allow")

        steps_total: int = 0
        steps_completed: int = 0
        total_tasks: int = 0
        success_tasks: int = 0
        failed_tasks: int = 0
        records_written: int = 0
        filtered_records: int = 0
        duration_seconds: float = 0.0

    job_id: str
    status: str
    phase: str | None = None
    progress: float = 0.0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    summary: ConceptSummaryPayload = Field(default_factory=ConceptSummaryPayload)


class ConceptControlResponse(BaseModel):
    job_id: str
    status: str
    message: str


class ConceptLogsResponse(BaseModel):
    items: list[LogItem]
    next_after_log_id: int | None = None


class ResultStockConceptEntry(BaseModel):
    board_name: str = ""
    selected_reason: str = ""
    updated_at: datetime | None = None


class ResultTopConceptSummary(BaseModel):
    board_name: str
    hit_stock_count: int
    total_hit_stocks: int


class ResultStockConceptsResponse(BaseModel):
    task_id: str
    formula: dict[str, Any] = Field(default_factory=dict)
    top_concepts: list[ResultTopConceptSummary] = Field(default_factory=list)
    items: dict[str, list[ResultStockConceptEntry]] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# 回测 (Backtest)
# ---------------------------------------------------------------------------


class ParamRange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min: float
    max: float
    step: float = Field(gt=0)


class BacktestCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy_group_id: str = Field(min_length=1)
    mode: Literal["fixed", "sweep"] = "fixed"
    forward_bars: list[int] = Field(min_length=3, max_length=3, description="前瞻K线数 [x, y, z]")
    slide_step: int = Field(default=1, ge=1, description="滑动步长")
    group_params: dict[str, Any] = Field(default_factory=dict, description="策略参数（fixed模式可选，默认取监控页保存参数）")
    param_ranges: dict[str, ParamRange] = Field(
        default_factory=dict,
        description="sweep模式：参数路径 → 扫描范围。路径格式: 'section.param_name'",
    )


class BacktestCreateResponse(BaseModel):
    job_id: str


class BacktestStatusResponse(BaseModel):
    class BacktestSummaryPayload(BaseModel):
        model_config = ConfigDict(extra="allow")

        total_hits: int = 0
        total_errors: int = 0
        duration_seconds: float = 0.0

    job_id: str
    status: str
    progress: float = 0.0
    total_stocks: int = 0
    processed_stocks: int = 0
    combo_index: int = 0
    combo_total: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    summary: BacktestSummaryPayload = Field(default_factory=BacktestSummaryPayload)


class BacktestControlResponse(BaseModel):
    job_id: str
    status: str
    message: str


class BacktestLogsResponse(BaseModel):
    items: list[LogItem]
    next_after_log_id: int | None = None


class BacktestHitRecord(BaseModel):
    code: str
    name: str
    tf_key: str
    pattern_start_ts: datetime
    pattern_end_ts: datetime
    buy_price: float
    profit_x_pct: float | None = None
    drawdown_x_pct: float | None = None
    sharpe_x: float | None = None
    profit_y_pct: float | None = None
    drawdown_y_pct: float | None = None
    sharpe_y: float | None = None
    profit_z_pct: float | None = None
    drawdown_z_pct: float | None = None
    sharpe_z: float | None = None


class BacktestHitsResponse(BaseModel):
    total_count: int
    items: list[BacktestHitRecord]


class BacktestSweepRow(BaseModel):
    combo_index: int
    param_combo: dict[str, Any]
    total_hits: int = 0
    win_rate_x: float | None = None
    avg_profit_x: float | None = None
    avg_drawdown_x: float | None = None
    win_rate_z: float | None = None
    avg_profit_z: float | None = None
    avg_drawdown_z: float | None = None


class BacktestSweepResponse(BaseModel):
    combo_total: int
    items: list[BacktestSweepRow]


class BacktestFormSettingsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    forward_bars: list[int] = Field(default_factory=lambda: [2, 5, 7], min_length=3, max_length=3)
    slide_step: int = Field(default=1, ge=1)
    strategy_group_id: str = ""
    group_params: dict[str, Any] = Field(default_factory=dict)
    sweep_ranges: dict[str, Any] = Field(default_factory=dict)
    strategy_settings: dict[str, Any] = Field(default_factory=dict)
    info_log_autoscroll: bool = True
    error_log_autoscroll: bool = True
