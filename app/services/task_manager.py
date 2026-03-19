"""
任务管理核心（TaskManager）。

职责：
1. 管理任务生命周期：创建、排队、执行、暂停、恢复、停止、收尾。
2. 根据策略元信息选择执行轨道（backtrader / specialized）。
3. 协调数据读取、策略执行、结果入库与实时日志输出。
4. 控制并发与降级策略，保证任务系统稳定运行。

说明：
1. 该模块是平台业务主编排层，策略细节由 `strategies/groups/*` 实现。
2. 对专用引擎相关逻辑必须保持清晰注释，便于后续新增策略复用。
"""

from __future__ import annotations

"""
任务调度器（TaskManager）。

关键设计：
1. 任务级并发：通过 ThreadPoolExecutor 控制同时运行的任务数量。
2. 单任务执行引擎：支持 backtrader 与 specialized 双轨。
3. 对专用引擎增加并发槽位控制，避免多任务同时重计算导致资源争抢。
4. 保留暂停/恢复/停止语义，保证状态可恢复与可追踪。
"""

import json
import logging
import random
import threading
import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from app.db.market_data import MarketDataDB
from app.db.state_db import StateDB
from app.services.concept_formula import parse_concept_formula
from app.services.screener import run_single_stock_backtrader
from app.services.strategy_registry import StrategyRegistry, StrategyRegistryError
from app.services.task_logger import TaskLogger
from app.settings import (
    SOURCE_DB_PATH,
    SPECIALIZED_ENGINE_FALLBACK_TO_BACKTRADER,
    SPECIALIZED_ENGINE_MUTEX,
)


class TaskStopRequested(Exception):
    """用于在任务执行中断点快速跳出控制流的内部异常。"""

    pass


class TaskManager:
    def __init__(self, state_db: StateDB, app_logger: logging.Logger, strategy_registry: StrategyRegistry):
        """
        输入：
        1. state_db: 状态库存取对象。
        2. app_logger: 应用主日志器。
        3. strategy_registry: 策略注册中心。
        输出：
        1. 无返回值。
        用途：
        1. 初始化任务线程池、Future 跟踪表、并发槽位和中断任务恢复逻辑。
        边界条件：
        1. specialized 任务额外受 `_specialized_slots` 限流，避免多任务同时占满重计算资源。
        """
        self.state_db = state_db
        self.app_logger = app_logger
        self.strategy_registry = strategy_registry
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="screen-task")
        self._futures: dict[str, Future] = {}
        self._lock = threading.Lock()
        # 专用引擎并发槽位：限制 specialized 重计算任务的同时运行数，避免资源争抢。
        self._specialized_slots = threading.Semaphore(max(1, SPECIALIZED_ENGINE_MUTEX))
        self._recover_interrupted_tasks()

    def create_task(
        self,
        *,
        stocks: list[str],
        start_ts: datetime | None,
        end_ts: datetime | None,
        group_params: dict[str, Any] | None,
        strategy_group_id: str,
        source_db: str | None,
        run_mode: str,
        sample_size: int,
        skip_coverage_filter: bool = True,
    ) -> str:
        """
        输入：
        1. stocks: 原始股票输入列表，可为代码或名称。
        2. start_ts/end_ts: 可选筛选时间窗口。
        3. group_params: 前端覆盖的策略参数。
        4. strategy_group_id: 目标策略组 ID。
        5. source_db: 可选源库路径；为空时使用全局默认值。
        6. run_mode/sample_size: 运行模式和采样规模参数。
        输出：
        1. 新建任务 ID。
        用途：
        1. 校验运行前置条件、合并策略参数、写入任务主表并提交后台执行。
        边界条件：
        1. 维护任务运行中或概念任务运行中时，禁止创建筛选任务。
        2. 若调用方显式传入时间窗口，则必须满足 `start_ts <= end_ts`。
        """
        source_db_path = Path(source_db) if source_db else SOURCE_DB_PATH

        if self.state_db.get_running_maintenance_job() is not None:
            raise ValueError("数据库维护任务运行中，当前禁止创建筛选任务")
        if self.state_db.get_running_concept_job() is not None:
            raise ValueError("概念更新任务运行中，当前禁止创建筛选任务")

        try:
            group_meta = self.strategy_registry.get_group_meta(strategy_group_id)
            merged_group_params = self.strategy_registry.merge_group_params(strategy_group_id, group_params)
        except KeyError as exc:
            raise ValueError(str(exc)) from exc
        except StrategyRegistryError as exc:
            raise ValueError(str(exc)) from exc

        if start_ts and end_ts and start_ts > end_ts:
            raise ValueError("start_ts 不能晚于 end_ts")

        task_id = str(uuid4())
        self.state_db.create_task(
            task_id=task_id,
            source_db=str(source_db_path),
            start_ts=start_ts,
            end_ts=end_ts,
            strategy_group_id=group_meta.group_id,
            strategy_name=group_meta.name,
            strategy_description=group_meta.description,
            params={
                "stocks": stocks,
                "group_params": merged_group_params,
                "run_mode": run_mode,
                "sample_size": sample_size,
                "skip_coverage_filter": skip_coverage_filter,
            },
        )
        self._submit_task(task_id)
        return task_id

    def pause_task(self, task_id: str) -> dict[str, str]:
        """
        将任务状态切换为 `paused`。

        终态任务不会被重复暂停；若任务已处于 `paused`，则返回幂等提示而不是报错。
        """
        task = self.state_db.get_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        status = task.get("status")

        if status in {"completed", "failed", "stopped"}:
            return {"task_id": task_id, "status": status, "message": "任务已结束，无法暂停"}
        if status == "paused":
            return {"task_id": task_id, "status": status, "message": "任务已经处于暂停状态"}

        self.state_db.update_task_fields(task_id, status="paused")
        self.state_db.append_log(task_id=task_id, level="info", message="收到暂停请求")
        return {"task_id": task_id, "status": "paused", "message": "暂停请求已记录"}

    def resume_task(self, task_id: str) -> dict[str, str]:
        """
        恢复一个已暂停或被中断的任务。

        若任务当前没有后台 Future，会重新调度；若已在执行，则仅返回幂等提示。
        """
        task = self.state_db.get_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        status = task.get("status")

        if status in {"completed", "failed", "stopped"}:
            return {"task_id": task_id, "status": status, "message": "任务已结束，无法恢复"}

        self.state_db.update_task_fields(
            task_id,
            status="running",
            finished_at=None,
            error_message=None,
        )
        submitted = self._submit_task(task_id)
        if submitted:
            self.state_db.append_log(task_id=task_id, level="info", message="收到恢复请求，任务已进入运行")
            return {"task_id": task_id, "status": "running", "message": "任务已恢复并重新调度"}
        return {"task_id": task_id, "status": "running", "message": "任务已恢复，正在执行中"}

    def stop_task(self, task_id: str) -> dict[str, str]:
        """
        请求停止任务。

        该操作会把任务状态推进到停止语义，由执行线程在协作取消检查点感知并收尾。
        """
        task = self.state_db.get_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        status = task.get("status")

        if status in {"completed", "failed", "stopped"}:
            return {"task_id": task_id, "status": status, "message": "任务已结束"}

        if status in {"queued", "paused"}:
            self.state_db.update_task_fields(
                task_id,
                status="stopped",
                finished_at=datetime.now(),
                current_code=None,
            )
            self.state_db.append_log(task_id=task_id, level="info", message="任务已停止")
            return {"task_id": task_id, "status": "stopped", "message": "任务已停止"}

        self.state_db.update_task_fields(task_id, status="stopping")
        self.state_db.append_log(task_id=task_id, level="info", message="收到停止请求，当前股票处理结束后停止")
        return {"task_id": task_id, "status": "stopping", "message": "停止请求已记录"}

    def _submit_task(self, task_id: str) -> bool:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_submit_task` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        with self._lock:
            existing = self._futures.get(task_id)
            if existing and not existing.done():
                return False
            future = self.executor.submit(self._run_task, task_id)
            self._futures[task_id] = future
            return True

    def _recover_interrupted_tasks(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_recover_interrupted_tasks` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        interrupted = self.state_db.list_tasks_by_status(["running", "queued", "stopping"])
        for task in interrupted:
            task_id = task["task_id"]
            self.state_db.update_task_fields(
                task_id,
                status="paused",
                current_code=None,
                error_message="服务重启后自动暂停，可点击恢复任务继续执行",
            )
            self.state_db.append_log(
                task_id=task_id,
                level="info",
                message="检测到服务重启，任务自动切换为暂停状态",
            )

    def _wait_if_paused_or_stopping(self, task_id: str, task_logger: TaskLogger) -> None:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        2. task_logger: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_wait_if_paused_or_stopping` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        announced = False
        while True:
            task = self.state_db.get_task(task_id)
            if not task:
                raise TaskStopRequested("任务不存在")
            status = task.get("status")

            if status == "paused":
                if not announced:
                    task_logger.info("任务已暂停，等待恢复")
                    announced = True
                time.sleep(1.0)
                continue
            if status == "stopping":
                raise TaskStopRequested("收到停止请求")
            if status == "stopped":
                raise TaskStopRequested("任务已停止")
            if status in {"running", "queued"}:
                if announced:
                    task_logger.info("任务恢复执行")
                return
            if status in {"completed", "failed"}:
                raise TaskStopRequested(f"任务状态变更为 {status}")
            time.sleep(0.5)

    @staticmethod
    def _parse_bool(value: Any, default: bool) -> bool:
        """
        输入：
        1. value: 输入参数，具体约束以调用方和实现为准。
        2. default: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_parse_bool` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        if isinstance(value, str):
            token = value.strip().lower()
            if token in {"1", "true", "yes", "y", "on"}:
                return True
            if token in {"0", "false", "no", "n", "off"}:
                return False
        return bool(value)
    def _required_timeframes_for_filtering(
        self,
        strategy_engine: str,
        configured: list[str] | tuple[str, ...] | None = None,
    ) -> list[str]:
        """
        给“时间区间完整性过滤”选择需要检查的周期。

        - specialized: 仅依赖 d + 15m
        - backtrader: 默认检查 w/d/60/30/15
        """
        if configured:
            return list(configured)
        if strategy_engine == "specialized":
            return ["d", "15"]
        return ["w", "d", "60", "30", "15"]

    def _specialized_fallback_enabled(self, group_params: dict[str, Any]) -> bool:
        """
        读取 specialized -> backtrader 自动降级开关。

        优先级：
        1. group_params.execution.fallback_to_backtrader
        2. 全局默认 SPECIALIZED_ENGINE_FALLBACK_TO_BACKTRADER
        """
        execution = group_params.get("execution") if isinstance(group_params, dict) else None
        if isinstance(execution, dict) and "fallback_to_backtrader" in execution:
            return self._parse_bool(execution.get("fallback_to_backtrader"), SPECIALIZED_ENGINE_FALLBACK_TO_BACKTRADER)
        return SPECIALIZED_ENGINE_FALLBACK_TO_BACKTRADER

    def _acquire_specialized_slot(self, task_id: str, task_logger: TaskLogger) -> None:
        """
        获取 specialized 槽位。

        采用短超时循环，在等待期间持续检查任务是否被暂停/停止，
        防止阻塞导致控制指令无响应。
        """
        announced = False
        while True:
            if self._specialized_slots.acquire(timeout=1.0):
                return
            if not announced:
                task_logger.info("等待专用引擎并发槽位")
                announced = True
            self._wait_if_paused_or_stopping(task_id, task_logger)

    def _run_specialized_engine(
        self,
        *,
        task_id: str,
        task_logger: TaskLogger,
        source_db_path: Path,
        start_ts: datetime | None,
        end_ts: datetime | None,
        codes: list[str],
        code_to_name: dict[str, str],
        strategy_group_id: str,
        strategy_name: str,
        group_params: dict[str, Any],
        cache_scope: str,
        specialized_runner: Callable[..., tuple[dict[str, Any], dict[str, Any]]],
        summary: dict[str, Any],
        signal_code_set: set[str],
        processed_codes: set[str],
        processed_count: int,
    ) -> tuple[int, set[str]]:
        """
        specialized 分支的单任务执行主体。

        执行步骤：
        1. 过滤未处理股票；
        2. 获取槽位并调用专用引擎；
        3. 汇总阶段指标并写入 summary；
        4. 按股票逐个落库结果/状态并推进进度。
        """
        remaining_codes = [code for code in codes if code not in processed_codes]
        if not remaining_codes:
            return processed_count, signal_code_set

        self._acquire_specialized_slot(task_id, task_logger)
        try:
            task_logger.info(
                "专用引擎阶段开始",
                {
                    "strategy_group_id": strategy_group_id,
                    "strategy_name": strategy_name,
                    "codes": len(remaining_codes),
                    "cache_scope": cache_scope,
                },
            )
            result_map, metrics = specialized_runner(
                source_db_path=source_db_path,
                start_ts=start_ts,
                end_ts=end_ts,
                codes=remaining_codes,
                code_to_name=code_to_name,
                group_params=group_params,
                strategy_group_id=strategy_group_id,
                strategy_name=strategy_name,
                cache_scope=cache_scope,
            )
        finally:
            self._specialized_slots.release()

        total_daily_rows = int(metrics.get("total_daily_rows") or 0)
        anchor_count = int(metrics.get("anchor_count") or 0)
        codes_with_anchor = int(metrics.get("codes_with_anchor") or 0)
        anchor_compression = round(anchor_count / total_daily_rows, 8) if total_daily_rows > 0 else None
        code_compression = round(codes_with_anchor / len(remaining_codes), 8) if remaining_codes else None

        summary["specialized_metrics"] = {
            **metrics,
            "anchors_over_daily_rows": anchor_compression,
            "codes_with_anchor_over_codes": code_compression,
        }
        task_logger.info("专用引擎阶段完成", summary["specialized_metrics"])

        for code in remaining_codes:
            self._wait_if_paused_or_stopping(task_id, task_logger)

            stock_name = code_to_name.get(code, "")
            self.state_db.update_task_fields(task_id, current_code=code)

            code_result = result_map.get(code)
            if code_result is None:
                # 无候选或无触发也写入 completed，保证任务可恢复进度的闭环。
                self.state_db.upsert_stock_state(
                    task_id=task_id,
                    code=code,
                    name=stock_name,
                    status="completed",
                    processed_bars=0,
                    signal_count=0,
                    last_dt=None,
                    last_rules={
                        "engine": "specialized",
                        "anchor_candidate": False,
                    },
                    error_message=None,
                )
            else:
                if code_result.error_message:
                    summary["stock_errors"] += 1
                    task_logger.error(
                        "专用引擎处理失败",
                        {
                            "code": code,
                            "name": stock_name,
                            "error": code_result.error_message,
                        },
                    )

                for signal in code_result.signals:
                    # specialized 引擎已产出统一信号结构，这里直接落库。
                    self.state_db.add_result(
                        task_id=task_id,
                        code=signal["code"],
                        name=signal["name"],
                        signal_dt=signal["signal_dt"],
                        clock_tf=signal["clock_tf"],
                        strategy_group_id=signal["strategy_group_id"],
                        strategy_name=signal["strategy_name"],
                        signal_label=signal["signal_label"],
                        payload=signal.get("payload"),
                    )

                if code_result.signal_count > 0:
                    signal_code_set.add(code)

                last_dt = max((signal["signal_dt"] for signal in code_result.signals), default=None)
                self.state_db.upsert_stock_state(
                    task_id=task_id,
                    code=code,
                    name=stock_name,
                    status="completed" if not code_result.error_message else "failed",
                    processed_bars=code_result.processed_bars,
                    signal_count=code_result.signal_count,
                    last_dt=last_dt,
                    last_rules={
                        "engine": "specialized",
                        "anchor_candidate": True,
                    },
                    error_message=code_result.error_message,
                )

            processed_count += 1
            summary["processed_codes"] = processed_count
            summary["signal_codes"] = len(signal_code_set)
            # 进度口径统一按“已处理股票数 / 总股票数”计算。
            self.state_db.update_task_fields(
                task_id,
                processed_stocks=processed_count,
                progress=round(processed_count * 100.0 / len(codes), 4),
                summary_json=json.dumps(summary, ensure_ascii=False, default=str),
            )

        return processed_count, signal_code_set

    def _run_task(self, task_id: str) -> None:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_run_task` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        task = self.state_db.get_task(task_id)
        if not task:
            return
        current_status = str(task.get("status") or "")
        if current_status in {"completed", "failed", "stopped"}:
            return
        if current_status == "stopping":
            self.state_db.update_task_fields(
                task_id,
                status="stopped",
                finished_at=datetime.now(),
                current_code=None,
            )
            self.state_db.append_log(task_id=task_id, level="info", message="任务在启动前被停止")
            return

        params = task.get("params") or {}
        stocks: list[str] = params.get("stocks") or []
        group_params: dict[str, Any] = params.get("group_params") or {}
        run_mode: str = params.get("run_mode") or "full"
        sample_size: int = int(params.get("sample_size") or 20)
        skip_coverage_filter: bool = bool(params.get("skip_coverage_filter", True))

        source_db = task.get("source_db") or str(SOURCE_DB_PATH)
        start_ts = task.get("start_ts")
        end_ts = task.get("end_ts")
        strategy_group_id = task.get("strategy_group_id")
        strategy_name = task.get("strategy_name") or strategy_group_id
        strategy_description = task.get("strategy_description") or ""

        market_db = MarketDataDB(Path(source_db))
        task_logger = TaskLogger(task_id, self.state_db, self.app_logger)

        summary: dict[str, Any] = task.get("summary") or {
            "unresolved_inputs": [],
            "processed_codes": 0,
            "signal_codes": 0,
            "stock_errors": 0,
            "started_at": datetime.now().isoformat(),
            "strategy_group_id": strategy_group_id,
            "strategy_name": strategy_name,
        }

        try:
            market_db.ensure_available()
            strategy_meta = self.strategy_registry.get_group_meta(strategy_group_id)
            strategy_engine = strategy_meta.engine
            cache_scope = strategy_meta.execution.cache_scope
            supports_intra_task_parallel = strategy_meta.execution.supports_intra_task_parallel
            configured_timeframes = list(strategy_meta.execution.required_timeframes)
            specialized_runner: Callable[..., tuple[dict[str, Any], dict[str, Any]]] | None = None
            if strategy_engine == "specialized":
                specialized_runner = self.strategy_registry.load_specialized_runner(strategy_group_id)
            summary["strategy_engine"] = strategy_engine

            update_fields: dict[str, Any] = {
                "status": "running",
                "error_message": None,
            }
            if not task.get("started_at"):
                update_fields["started_at"] = datetime.now()
            self.state_db.update_task_fields(task_id, **update_fields)
            task_logger.info(
                "任务已开始",
                {
                    "source_db": source_db,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "run_mode": run_mode,
                    "sample_size": sample_size,
                    "strategy_group_id": strategy_group_id,
                    "strategy_name": strategy_name,
                    "strategy_engine": strategy_engine,
                },
            )

            task_logger.info(
                "策略组加载完成",
                {
                    "strategy_group_id": strategy_group_id,
                    "strategy_name": strategy_name,
                    "strategy_description": strategy_description,
                    "strategy_engine": strategy_engine,
                    "cache_scope": cache_scope,
                    "supports_intra_task_parallel": supports_intra_task_parallel,
                },
            )

            with market_db.open_read_session() as market_session:
                stock_rows = market_session.get_all_stocks()
                code_to_name = {code: name for code, name in stock_rows}

                codes = summary.get("resolved_codes") or []
                unresolved_inputs = summary.get("unresolved_inputs") or []
                if not codes:
                    resolution = market_session.resolve_stock_inputs(stocks, stock_rows=stock_rows)
                    codes = list(resolution.codes)
                    unresolved_inputs = resolution.unresolved
                    code_to_name.update(resolution.code_to_name)

                    if start_ts is not None and end_ts is not None and codes and not skip_coverage_filter:
                        required_timeframes = self._required_timeframes_for_filtering(
                            strategy_engine,
                            configured=configured_timeframes,
                        )
                        coverage = market_session.filter_codes_with_complete_range(
                            codes=codes,
                            start_ts=start_ts,
                            end_ts=end_ts,
                            required_timeframes=required_timeframes,
                        )
                        raw_count = len(codes)
                        codes = coverage.eligible_codes
                        excluded_codes = coverage.excluded_codes
                        summary["coverage_filter"] = {
                            "required_timeframes": required_timeframes,
                            "expected_bars": coverage.expected_bars_by_timeframe,
                            "raw_code_count": raw_count,
                            "eligible_code_count": len(codes),
                            "excluded_code_count": len(excluded_codes),
                        }

                        if excluded_codes:
                            task_logger.info(
                                "已排除时间区间内数据不完整股票",
                                {
                                    "raw_code_count": raw_count,
                                    "eligible_code_count": len(codes),
                                    "excluded_code_count": len(excluded_codes),
                                    "excluded_code_preview": excluded_codes[:30],
                                    "required_timeframes": required_timeframes,
                                    "expected_bars": coverage.expected_bars_by_timeframe,
                                },
                            )

                    if run_mode == "sample20" and codes:
                        selected = random.sample(codes, k=min(sample_size, len(codes)))
                        codes = selected

                    concept_formula = parse_concept_formula(group_params)
                    summary["concept_formula"] = concept_formula.to_dict()
                    if concept_formula.is_active() and codes:
                        raw_count = len(codes)
                        codes, concept_filter_summary = market_session.filter_codes_by_concept_formula(
                            codes=codes,
                            formula=concept_formula,
                        )
                        summary["concept_filter"] = concept_filter_summary
                        if raw_count != len(codes):
                            task_logger.info(
                                "已应用概念公式预筛选",
                                {
                                    "raw_code_count": raw_count,
                                    "eligible_code_count": len(codes),
                                    "excluded_code_count": raw_count - len(codes),
                                    "concept_terms": list(concept_formula.concept_terms),
                                    "reason_terms": list(concept_formula.reason_terms),
                                    "excluded_code_preview": concept_filter_summary.get("excluded_code_preview") or [],
                                },
                            )

                    summary["unresolved_inputs"] = unresolved_inputs
                    summary["resolved_codes"] = codes
                    summary["run_mode"] = run_mode
                    summary["sample_size"] = sample_size
                    summary["total_codes"] = len(codes)
                    self.state_db.update_task_fields(
                        task_id,
                        total_stocks=len(codes),
                        summary_json=json.dumps(summary, ensure_ascii=False, default=str),
                    )

                    if unresolved_inputs:
                        task_logger.error("部分输入股票未能解析", {"unresolved": unresolved_inputs})

                if not codes:
                    task_logger.info("输入列表未匹配到任何股票，任务结束")
                    summary["finished_at"] = datetime.now().isoformat()
                    self.state_db.update_task_fields(
                        task_id,
                        status="completed",
                        finished_at=datetime.now(),
                        progress=100.0,
                        current_code=None,
                        summary_json=json.dumps(summary, ensure_ascii=False, default=str),
                    )
                    return

                processed_codes, signal_code_set = self.state_db.get_task_recovery_snapshot(task_id)
                processed_count = len(processed_codes)
                summary["processed_codes"] = processed_count
                summary["signal_codes"] = len(signal_code_set)

                self.state_db.update_task_fields(
                    task_id,
                    total_stocks=len(codes),
                    processed_stocks=processed_count,
                    progress=round(processed_count * 100.0 / len(codes), 4),
                    summary_json=json.dumps(summary, ensure_ascii=False, default=str),
                )

                fallback_to_backtrader = self._specialized_fallback_enabled(group_params)
                if strategy_engine == "specialized":
                    if specialized_runner is None:
                        raise RuntimeError(f"策略组 {strategy_group_id} 未能加载 specialized 入口")
                    # 主路径：优先 specialized。
                    try:
                        processed_count, signal_code_set = self._run_specialized_engine(
                            task_id=task_id,
                            task_logger=task_logger,
                            source_db_path=Path(source_db),
                            start_ts=start_ts,
                            end_ts=end_ts,
                            codes=codes,
                            code_to_name=code_to_name,
                            strategy_group_id=strategy_group_id,
                            strategy_name=strategy_name,
                            group_params=group_params,
                            cache_scope=cache_scope,
                            specialized_runner=specialized_runner,
                            summary=summary,
                            signal_code_set=signal_code_set,
                            processed_codes=processed_codes,
                            processed_count=processed_count,
                        )
                        processed_codes, signal_code_set = self.state_db.get_task_recovery_snapshot(task_id)
                    except Exception as specialized_exc:
                        # specialized 失败后可按开关降级到 backtrader，保证任务不中断。
                        err_text = f"{type(specialized_exc).__name__}: {specialized_exc}"
                        task_logger.error(
                            "专用引擎执行失败",
                            {
                                "error": err_text,
                                "fallback_to_backtrader": fallback_to_backtrader,
                                "traceback": traceback.format_exc(limit=6),
                            },
                        )
                        if not fallback_to_backtrader:
                            raise
                        task_logger.info("开始降级到 backtrader 执行")
                        strategy_engine = "backtrader"
                        processed_codes, signal_code_set = self.state_db.get_task_recovery_snapshot(task_id)
                        processed_count = len(processed_codes)
                        summary["processed_codes"] = processed_count
                        summary["signal_codes"] = len(signal_code_set)
                        self.state_db.update_task_fields(
                            task_id,
                            processed_stocks=processed_count,
                            progress=round(processed_count * 100.0 / len(codes), 4),
                            summary_json=json.dumps(summary, ensure_ascii=False, default=str),
                        )

                if strategy_engine == "backtrader":
                    strategy_runtime = self.strategy_registry.load_runtime(strategy_group_id)
                    fetch_timeframes = list(configured_timeframes) if configured_timeframes else ["w", "d", "60", "30", "15"]

                    for code in codes:
                        if code in processed_codes:
                            continue

                        self._wait_if_paused_or_stopping(task_id, task_logger)

                        stock_name = code_to_name.get(code, "")
                        self.state_db.update_task_fields(task_id, current_code=code)
                        task_logger.info(
                            "开始处理股票",
                            {
                                "index": processed_count + 1,
                                "total": len(codes),
                                "code": code,
                                "name": stock_name,
                            },
                        )

                        try:

                            def on_signal(signal: dict[str, Any]) -> None:
                                """
                                输入：
                                1. signal: 输入参数，具体约束以调用方和实现为准。
                                输出：
                                1. 返回值语义由函数实现定义；无返回时为 `None`。
                                用途：
                                1. 执行 `on_signal` 对应的业务或工具逻辑。
                                边界条件：
                                1. 关键边界与异常分支按函数体内判断与调用约定处理。
                                """
                                self.state_db.add_result(
                                    task_id=task_id,
                                    code=signal["code"],
                                    name=signal["name"],
                                    signal_dt=signal["signal_dt"],
                                    clock_tf=signal["clock_tf"],
                                    strategy_group_id=signal["strategy_group_id"],
                                    strategy_name=signal["strategy_name"],
                                    signal_label=signal["signal_label"],
                                    payload=signal.get("payload"),
                                )

                            timeframe_dfs: dict[str, Any] = {}
                            for tf in fetch_timeframes:
                                timeframe_dfs[tf] = market_session.fetch_ohlcv(
                                    code=code,
                                    timeframe=tf,
                                    start_ts=start_ts,
                                    end_ts=end_ts,
                                )

                            run_summary, run_errors = run_single_stock_backtrader(
                                code=code,
                                name=stock_name,
                                timeframe_dfs=timeframe_dfs,
                                strategy_group_id=strategy_group_id,
                                strategy_name=strategy_name,
                                strategy_group_runtime=strategy_runtime,
                                group_params=group_params,
                                on_signal=on_signal,
                            )

                            if run_summary.signal_count > 0:
                                signal_code_set.add(code)

                            if run_errors:
                                summary["stock_errors"] += 1
                                for err in run_errors:
                                    task_logger.error("策略执行出现问题", {"code": code, "name": stock_name, "error": err})

                            self.state_db.upsert_stock_state(
                                task_id=task_id,
                                code=code,
                                name=stock_name,
                                status="completed",
                                processed_bars=run_summary.processed_bars,
                                signal_count=run_summary.signal_count,
                                last_dt=run_summary.last_dt,
                                last_rules=run_summary.last_rules,
                                error_message="; ".join(run_errors) if run_errors else None,
                            )

                            task_logger.info(
                                "股票处理完成",
                                {
                                    "code": code,
                                    "name": stock_name,
                                    "processed_bars": run_summary.processed_bars,
                                    "signal_count": run_summary.signal_count,
                                    "last_dt": run_summary.last_dt.isoformat() if run_summary.last_dt else None,
                                },
                            )
                        except Exception as stock_exc:
                            summary["stock_errors"] += 1
                            err_text = f"{type(stock_exc).__name__}: {stock_exc}"
                            self.state_db.upsert_stock_state(
                                task_id=task_id,
                                code=code,
                                name=stock_name,
                                status="failed",
                                processed_bars=0,
                                signal_count=0,
                                last_dt=None,
                                last_rules=None,
                                error_message=err_text,
                            )
                            task_logger.error(
                                "股票处理失败",
                                {
                                    "code": code,
                                    "name": stock_name,
                                    "error": err_text,
                                    "traceback": traceback.format_exc(limit=4),
                                },
                            )

                        processed_codes.add(code)
                        processed_count += 1
                        summary["processed_codes"] = processed_count
                        summary["signal_codes"] = len(signal_code_set)
                        self.state_db.update_task_fields(
                            task_id,
                            processed_stocks=processed_count,
                            progress=round(processed_count * 100.0 / len(codes), 4),
                            summary_json=json.dumps(summary, ensure_ascii=False, default=str),
                        )

            final_status = (self.state_db.get_task(task_id) or {}).get("status")
            if final_status in {"stopping", "stopped"}:
                raise TaskStopRequested("任务在收尾阶段收到停止请求")

            summary["finished_at"] = datetime.now().isoformat()
            task_logger.info("任务已完成", summary)
            self.state_db.update_task_fields(
                task_id,
                status="completed",
                finished_at=datetime.now(),
                progress=100.0,
                current_code=None,
                summary_json=json.dumps(summary, ensure_ascii=False, default=str),
            )
        except TaskStopRequested as stop_exc:
            current = self.state_db.get_task(task_id) or {}
            current_status = current.get("status")
            if current_status == "stopping":
                self.state_db.update_task_fields(
                    task_id,
                    status="stopped",
                    finished_at=datetime.now(),
                    current_code=None,
                )
                task_logger.info("任务已停止")
            elif current_status == "paused":
                self.state_db.update_task_fields(task_id, current_code=None)
                task_logger.info("任务保持暂停状态")
            else:
                self.state_db.update_task_fields(
                    task_id,
                    status="stopped",
                    finished_at=datetime.now(),
                    current_code=None,
                )
                task_logger.info("任务停止", {"reason": str(stop_exc)})
        except Exception as exc:
            err_text = f"{type(exc).__name__}: {exc}"
            summary["finished_at"] = datetime.now().isoformat()
            summary["fatal_error"] = err_text
            task_logger.error(
                "任务失败",
                {
                    "error": err_text,
                    "traceback": traceback.format_exc(limit=8),
                },
            )
            self.state_db.update_task_fields(
                task_id,
                status="failed",
                finished_at=datetime.now(),
                current_code=None,
                error_message=err_text,
                summary_json=json.dumps(summary, ensure_ascii=False, default=str),
            )

    def shutdown(self, grace_seconds: int, forced_status: str = "failed") -> None:
        """
        优雅关闭筛选任务执行器。
        输入：
        1. grace_seconds: 最大等待秒数。
        2. forced_status: 超时后写入的任务状态，默认 failed。
        输出：
        1. 无返回值。
        用途：
        1. 在应用停机时触发“停止请求 + 有界等待 + 超时强制落库”。
        边界条件：
        1. 不强杀线程，仅依赖任务内 stop 检查点收尾。
        """

        safe_grace = max(0, int(grace_seconds))
        active_tasks = self.state_db.list_tasks_by_status(["queued", "running", "stopping", "paused"], limit=2000)
        active_ids = [str(item.get("task_id")) for item in active_tasks if item.get("task_id")]

        for task_id in active_ids:
            try:
                self.stop_task(task_id)
            except Exception as exc:
                self.app_logger.error("筛选任务停机请求失败 | %s | %s", task_id, exc)

        deadline = time.monotonic() + safe_grace
        while True:
            pending_ids: list[str] = []
            for task_id in active_ids:
                task = self.state_db.get_task(task_id)
                if not task:
                    continue
                status = str(task.get("status") or "")
                if status in {"completed", "failed", "stopped"}:
                    continue
                pending_ids.append(task_id)

            if not pending_ids:
                break
            if time.monotonic() >= deadline:
                for task_id in pending_ids:
                    self.state_db.update_task_fields(
                        task_id,
                        status=forced_status,
                        finished_at=datetime.now(),
                        current_code=None,
                        error_message="forced_shutdown",
                    )
                    self.state_db.append_log(
                        task_id=task_id,
                        level="error",
                        message="优雅停机超时，已强制结束",
                        detail={"forced_status": forced_status, "reason": "forced_shutdown"},
                    )
                break
            time.sleep(0.2)

        self.executor.shutdown(wait=False, cancel_futures=True)

    def get_task_future(self, task_id: str) -> Future | None:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_task_future` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        with self._lock:
            return self._futures.get(task_id)

