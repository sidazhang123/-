"""
数据维护任务管理器。

职责：
1. 管理维护任务生命周期（创建、执行、停止、恢复中断状态）。
2. 调用维护引擎执行 latest_update / historical_backfill。
3. 维护任务单并发执行，避免源库写入冲突。
"""

from __future__ import annotations

import dataclasses
import json
import logging
import threading
import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from uuid import uuid4

from app.db.state_db import StateDB
from app.services.kline_maintenance import (
    MaintenanceStopRequested,
    MarketDataMaintenanceService,
)
from app.services.maintenance_logger import MaintenanceLogger
from app.settings import (
    MAINTENANCE_DEBUG_HISTORICAL_DATA,
    MAINTENANCE_DEBUG_LATEST_UPDATE,
    MAINTENANCE_STOP_FORCE_REWARM_TIMEOUT_SECONDS,
    MAINTENANCE_STOP_SOFT_TIMEOUT_SECONDS,
    SOURCE_DB_PATH,
)


class MaintenanceManager:
    """
    维护任务调度器（单并发）。
    """

    def __init__(self, state_db: StateDB, app_logger: logging.Logger):
        """
        输入：
        1. state_db: 状态库实例。
        2. app_logger: 应用日志器。
        输出：
        1. 无返回值。
        用途：
        1. 初始化维护任务执行线程池与停止 watchdog。
        边界条件：
        1. 维护任务固定单并发执行。
        """

        self.state_db = state_db
        self.app_logger = app_logger
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="maintenance-task")
        self._futures: dict[str, Future] = {}
        self._stop_watchdog_threads: dict[str, threading.Thread] = {}
        self._stop_watchdog_done_events: dict[str, threading.Event] = {}
        self._stop_watchdog_force_events: dict[str, threading.Event] = {}
        self._lock = threading.Lock()
        self._recover_interrupted_jobs()

    @staticmethod
    def _is_terminal_status(status: str) -> bool:
        """
        输入：
        1. status: 任务状态字符串。
        输出：
        1. 是否为终态。
        用途：
        1. 统一维护任务终态判断。
        边界条件：
        1. 未知状态按非终态处理。
        """

        token = str(status or "").strip().lower()
        return token in {"completed", "failed", "stopped"}

    def _normalize_mode(self, mode: str | None) -> str:
        """
        输入：
        1. mode: 请求模式字符串。
        输出：
        1. 标准模式值（latest_update/historical_backfill）。
        用途：
        1. 对维护模式做严格校验。
        边界条件：
        1. 非法值抛 ValueError。
        """

        token = str(mode or "").strip().lower()
        if token not in {"latest_update", "historical_backfill"}:
            raise ValueError(f"非法维护模式: {mode}")
        return token

    def _debug_enabled_for_mode(self, mode: str) -> bool:
        """
        输入：
        1. mode: 标准维护模式。
        输出：
        1. 对应模式的 debug 开关。
        用途：
        1. 将配置注入维护日志器。
        边界条件：
        1. 未知模式按 False 处理。
        """

        if mode == "latest_update":
            return bool(MAINTENANCE_DEBUG_LATEST_UPDATE)
        if mode == "historical_backfill":
            return bool(MAINTENANCE_DEBUG_HISTORICAL_DATA)
        return False

    def has_running_job(self) -> bool:
        """
        输入：
        1. 无。
        输出：
        1. 是否存在运行中的维护任务。
        用途：
        1. 作为创建新维护任务的并发保护。
        边界条件：
        1. queued/running/stopping 都视为运行中。
        """

        return self.state_db.get_running_maintenance_job() is not None

    def create_job(self, *, mode: str) -> str:
        """
        输入：
        1. mode: 维护模式。
        输出：
        1. 新建任务 ID。
        用途：
        1. 创建并提交维护任务。
        边界条件：
        1. 若已有运行中任务会抛 ValueError。
        """

        if self.has_running_job():
            raise ValueError("已有维护任务正在运行，请等待完成后再创建")
        if self.state_db.get_running_concept_job() is not None:
            raise ValueError("已有概念更新任务正在运行，请等待完成后再创建维护任务")

        active_tasks = self.state_db.list_tasks_by_status(["running", "queued", "stopping"], limit=1)
        if active_tasks:
            raise ValueError("存在运行中的筛选任务，请先结束筛选任务后再执行维护")

        normalized_mode = self._normalize_mode(mode)
        job_id = str(uuid4())
        self.state_db.create_maintenance_job(
            job_id=job_id,
            source_db=str(SOURCE_DB_PATH),
            mode=normalized_mode,
            params={},
        )
        self._submit_job(job_id)
        return job_id

    def stop_job(self, job_id: str) -> dict[str, str]:
        """
        输入：
        1. job_id: 维护任务 ID。
        输出：
        1. 停止操作结果。
        用途：
        1. 将任务置为 stopping 并触发超时 watchdog。
        边界条件：
        1. 终态任务不会重复停止。
        """

        job = self.state_db.get_maintenance_job(job_id)
        if not job:
            raise ValueError(f"维护任务不存在: {job_id}")

        status = str(job.get("status") or "")
        if status in {"completed", "failed", "stopped"}:
            return {"job_id": job_id, "status": status, "message": "维护任务已结束"}
        if status == "stopping":
            self._ensure_stop_watchdog(job_id)
            return {"job_id": job_id, "status": status, "message": "维护任务正在停止"}

        self.state_db.update_maintenance_job_fields(job_id, status="stopping")
        maint_logger = MaintenanceLogger(
            job_id=job_id,
            state_db=self.state_db,
            logger=self.app_logger,
            debug_enabled=self._debug_enabled_for_mode(str(job.get("mode") or "")),
        )
        maint_logger.info("收到停止请求（协作取消阶段）")
        self._ensure_stop_watchdog(job_id)
        return {"job_id": job_id, "status": "stopping", "message": "停止请求已记录"}

    def _is_stop_requested(self, job_id: str) -> bool:
        """
        输入：
        1. job_id: 维护任务 ID。
        输出：
        1. 是否已请求停止。
        用途：
        1. 提供维护引擎协作取消检查。
        边界条件：
        1. 任务记录不存在时按已停止处理。
        """

        current = self.state_db.get_maintenance_job(job_id)
        if not current:
            return True
        return str(current.get("status") or "") in {"stopping", "stopped", "failed"}

    def _submit_job(self, job_id: str) -> bool:
        """
        输入：
        1. job_id: 维护任务 ID。
        输出：
        1. 是否提交成功。
        用途：
        1. 提交后台执行任务。
        边界条件：
        1. 同任务已有未完成 Future 时不重复提交。
        """

        with self._lock:
            existing = self._futures.get(job_id)
            if existing and not existing.done():
                return False
            future = self.executor.submit(self._run_job, job_id)
            self._futures[job_id] = future
            return True

    def _ensure_stop_watchdog(self, job_id: str) -> None:
        """
        输入：
        1. job_id: 维护任务 ID。
        输出：
        1. 无返回值。
        用途：
        1. 启动停止超时 watchdog（软超时后强制重建 zsdtdx 并行池）。
        边界条件：
        1. 同一任务 watchdog 只会存在一个活跃线程。
        """

        with self._lock:
            existing = self._stop_watchdog_threads.get(job_id)
            if existing is not None and existing.is_alive():
                return

            done_event = self._stop_watchdog_done_events.get(job_id)
            if done_event is None:
                done_event = threading.Event()
                self._stop_watchdog_done_events[job_id] = done_event
            done_event.clear()

            force_event = self._stop_watchdog_force_events.get(job_id)
            if force_event is None:
                force_event = threading.Event()
                self._stop_watchdog_force_events[job_id] = force_event
            force_event.clear()

            worker = threading.Thread(
                target=self._stop_watchdog_worker,
                name=f"maintenance-stop-watchdog-{job_id[:8]}",
                args=(job_id, done_event, force_event),
                daemon=True,
            )
            self._stop_watchdog_threads[job_id] = worker
            worker.start()

    def _wait_stop_watchdog_done(self, job_id: str, *, force_only: bool = False) -> None:
        """
        输入：
        1. job_id: 维护任务 ID。
        2. force_only: 是否仅在已触发强制重建时等待。
        输出：
        1. 无返回值。
        用途：
        1. 在任务收尾前等待 watchdog 清理完成。
        边界条件：
        1. watchdog 未启动或未触发强制重建时可直接返回。
        """

        with self._lock:
            done_event = self._stop_watchdog_done_events.get(job_id)
            force_event = self._stop_watchdog_force_events.get(job_id)

        if force_only and (force_event is None or not force_event.is_set()):
            return
        if done_event is not None:
            done_event.wait()

        with self._lock:
            worker = self._stop_watchdog_threads.get(job_id)
            if worker is not None and not worker.is_alive():
                self._stop_watchdog_threads.pop(job_id, None)
            done_evt = self._stop_watchdog_done_events.get(job_id)
            if done_evt is not None and done_evt.is_set():
                self._stop_watchdog_done_events.pop(job_id, None)
                self._stop_watchdog_force_events.pop(job_id, None)

    def _stop_watchdog_worker(self, job_id: str, done_event: threading.Event, force_event: threading.Event) -> None:
        """
        输入：
        1. job_id: 维护任务 ID。
        2. done_event: watchdog 完成事件。
        3. force_event: 强制重建触发事件。
        输出：
        1. 无返回值。
        用途：
        1. 停止超时后尝试重建 zsdtdx 并行资源。
        边界条件：
        1. 无论成功失败都确保 done_event 最终置位。
        """

        maint_logger = MaintenanceLogger(
            job_id=job_id,
            state_db=self.state_db,
            logger=self.app_logger,
            debug_enabled=True,
        )
        soft_timeout = max(0.0, float(MAINTENANCE_STOP_SOFT_TIMEOUT_SECONDS))
        started_at = time.monotonic()
        try:
            while True:
                current = self.state_db.get_maintenance_job(job_id)
                if not current:
                    return
                status = str(current.get("status") or "")
                if self._is_terminal_status(status):
                    return
                if (time.monotonic() - started_at) >= soft_timeout:
                    break
                time.sleep(0.2)

            current = self.state_db.get_maintenance_job(job_id)
            if not current:
                return
            if self._is_terminal_status(str(current.get("status") or "")):
                return

            force_event.set()
            maint_logger.warning(
                "停止超时，开始强制终止并行进程",
                {
                    "soft_timeout_seconds": soft_timeout,
                    "elapsed_seconds": round(time.monotonic() - started_at, 3),
                },
            )
            try:
                from zsdtdx.parallel_fetcher import force_restart_parallel_fetcher

                summary = force_restart_parallel_fetcher(
                    prewarm=True,
                    prewarm_timeout_seconds=float(MAINTENANCE_STOP_FORCE_REWARM_TIMEOUT_SECONDS),
                    max_rounds=3,
                )
                maint_logger.info("并行进程及连接重建完成", summary if isinstance(summary, dict) else None)
            except Exception as exc:  # pragma: no cover
                maint_logger.error(
                    "并行进程及连接重建失败",
                    {
                        "error": f"{type(exc).__name__}: {exc}",
                        "traceback": traceback.format_exc(limit=8),
                    },
                )
        finally:
            done_event.set()

    def _recover_interrupted_jobs(self) -> None:
        """
        输入：
        1. 无。
        输出：
        1. 无返回值。
        用途：
        1. 服务重启时把中断中的维护任务标记为失败。
        边界条件：
        1. 仅处理 queued/running/stopping。
        """

        interrupted = self.state_db.list_maintenance_jobs_by_status(["queued", "running", "stopping"], limit=2000)
        for item in interrupted:
            job_id = str(item.get("job_id") or "")
            if not job_id:
                continue
            self.state_db.update_maintenance_job_fields(
                job_id,
                status="failed",
                finished_at=datetime.now(),
                error_message="服务重启导致维护任务中断，请重新执行",
            )
            self.state_db.append_maintenance_log(
                job_id=job_id,
                level="error",
                message="检测到服务重启，维护任务已标记为失败",
                detail=None,
            )

    def _run_job(self, job_id: str) -> None:
        """
        输入：
        1. job_id: 维护任务 ID。
        输出：
        1. 无返回值。
        用途：
        1. 后台执行单个维护任务。
        边界条件：
        1. 异常会转为 failed；停止请求会转为 stopped。
        """

        job = self.state_db.get_maintenance_job(job_id)
        if not job:
            return
        if self._is_terminal_status(str(job.get("status") or "")):
            return

        try:
            mode = self._normalize_mode(str(job.get("mode") or ""))
        except ValueError as exc:
            self.state_db.update_maintenance_job_fields(
                job_id,
                status="failed",
                phase="failed",
                progress=100.0,
                finished_at=datetime.now(),
                error_message=str(exc),
            )
            return

        maint_logger = MaintenanceLogger(
            job_id=job_id,
            state_db=self.state_db,
            logger=self.app_logger,
            debug_enabled=self._debug_enabled_for_mode(mode),
        )
        progress_state: dict[str, float] = {"value": 0.0, "ts": 0.0}

        def on_progress(progress: float, phase: str | None = None, detail: dict[str, object] | None = None) -> None:
            """
            输入：
            1. progress: 百分比进度（0-100）。
            2. phase: 阶段标识。
            3. detail: 可选明细。
            输出：
            1. 无返回值。
            用途：
            1. 维护引擎回调更新任务进度。
            边界条件：
            1. 节流写入避免高频更新状态库。
            """

            now_ts = time.monotonic()
            safe_progress = min(99.9, max(0.0, float(progress)))
            prev_value = float(progress_state["value"])
            prev_ts = float(progress_state["ts"])
            if safe_progress < prev_value:
                return
            if (safe_progress - prev_value) < 0.1 and (now_ts - prev_ts) < 0.6:
                return

            fields: dict[str, object] = {"progress": round(safe_progress, 3)}
            if phase:
                fields["phase"] = str(phase)
            if isinstance(detail, dict) and "round" in detail and "processed" in detail:
                fp = {k: v for k, v in detail.items() if k != "rows"}
                summary_obj: dict[str, object] = {"fetch_progress": fp}
                if "rows_written" in detail:
                    summary_obj["rows_written"] = detail["rows_written"]
                fields["summary_json"] = json.dumps(
                    summary_obj,
                    ensure_ascii=False,
                    default=str,
                )
            self.state_db.update_maintenance_job_fields(job_id, **fields)
            progress_state["value"] = safe_progress
            progress_state["ts"] = now_ts
            if isinstance(detail, dict):
                maint_logger.debug_lazy("进度更新", lambda d=detail: {k: v for k, v in d.items() if k != "rows"})

        self.state_db.update_maintenance_job_fields(
            job_id,
            status="running",
            phase="running",
            progress=0.0,
            error_message=None,
            started_at=job.get("started_at") or datetime.now(),
        )
        maint_logger.info("维护任务已开始", {"mode": mode})

        try:
            if self._is_stop_requested(job_id):
                self.state_db.update_maintenance_job_fields(
                    job_id,
                    status="stopped",
                    phase="stopped",
                    progress=100.0,
                    finished_at=datetime.now(),
                )
                maint_logger.info("维护任务在启动前被停止")
                return

            service = MarketDataMaintenanceService(
                source_db_path=SOURCE_DB_PATH,
                logger=maint_logger,
                stop_checker=lambda: self._is_stop_requested(job_id),
                progress_reporter=on_progress,
                state_db=self.state_db,
            )
            summary = service.run_update(mode=mode)

            if self._is_stop_requested(job_id):
                self._wait_stop_watchdog_done(job_id, force_only=True)
                self.state_db.update_maintenance_job_fields(
                    job_id,
                    status="stopped",
                    phase="stopped",
                    progress=100.0,
                    finished_at=datetime.now(),
                    summary_json=json.dumps(dataclasses.asdict(summary), ensure_ascii=False, default=str),
                )
                maint_logger.info("维护任务停止完成（重建已完成）")
                return

            finished_ts = datetime.now()
            self.state_db.update_maintenance_job_fields(
                job_id,
                status="completed",
                phase="done",
                progress=100.0,
                finished_at=finished_ts,
                summary_json=json.dumps(dataclasses.asdict(summary), ensure_ascii=False, default=str),
            )
            # 写版本戳供回测前瞻缓存校验
            try:
                self.state_db.set_meta_value(
                    "backtest.source_data_version",
                    f"{job_id}_{finished_ts.isoformat()}",
                )
            except Exception:
                pass
            maint_logger.info("维护任务完成")
        except MaintenanceStopRequested:
            self._wait_stop_watchdog_done(job_id, force_only=True)
            self.state_db.update_maintenance_job_fields(
                job_id,
                status="stopped",
                phase="stopped",
                progress=100.0,
                finished_at=datetime.now(),
            )
            maint_logger.info("维护任务停止完成（重建已完成）")
        except Exception as exc:
            if self._is_stop_requested(job_id):
                self._wait_stop_watchdog_done(job_id, force_only=True)
                self.state_db.update_maintenance_job_fields(
                    job_id,
                    status="stopped",
                    phase="stopped",
                    progress=100.0,
                    finished_at=datetime.now(),
                    error_message=None,
                )
                maint_logger.info("维护任务停止完成（重建已完成）", {"reason": f"{type(exc).__name__}: {exc}"})
                return
            err_text = f"{type(exc).__name__}: {exc}"
            maint_logger.error("维护任务失败", {"error": err_text, "traceback": traceback.format_exc(limit=8)})
            self.state_db.update_maintenance_job_fields(
                job_id,
                status="failed",
                phase="failed",
                progress=100.0,
                finished_at=datetime.now(),
                error_message=err_text,
            )
        finally:
            maint_logger.flush_debug()

    def shutdown(self, grace_seconds: int, forced_status: str = "failed") -> None:
        """
        输入：
        1. grace_seconds: 优雅停机等待秒数。
        2. forced_status: 超时后写入状态。
        输出：
        1. 无返回值。
        用途：
        1. 应用停机时中断维护任务并有界等待。
        边界条件：
        1. 不强杀线程，仅更新状态并关闭执行池。
        """

        safe_grace = max(0, int(grace_seconds))
        active_jobs = self.state_db.list_maintenance_jobs_by_status(["queued", "running", "stopping"], limit=2000)
        active_ids = [str(item.get("job_id")) for item in active_jobs if item.get("job_id")]

        for job_id in active_ids:
            try:
                self.stop_job(job_id)
            except Exception as exc:
                self.app_logger.error("维护任务停机请求失败 | %s | %s", job_id, exc)

        deadline = time.monotonic() + safe_grace
        while True:
            pending_ids: list[str] = []
            for job_id in active_ids:
                job = self.state_db.get_maintenance_job(job_id)
                if not job:
                    continue
                if self._is_terminal_status(str(job.get("status") or "")):
                    continue
                pending_ids.append(job_id)
            if not pending_ids:
                break
            if time.monotonic() >= deadline:
                for job_id in pending_ids:
                    self.state_db.update_maintenance_job_fields(
                        job_id,
                        status=forced_status,
                        phase="failed" if forced_status == "failed" else forced_status,
                        progress=100.0,
                        finished_at=datetime.now(),
                        error_message="forced_shutdown",
                    )
                    self.state_db.append_maintenance_log(
                        job_id=job_id,
                        level="error",
                        message="优雅停机超时，已强制结束",
                        detail={"forced_status": forced_status, "reason": "forced_shutdown"},
                    )
                break
            time.sleep(0.2)

        self.executor.shutdown(wait=False, cancel_futures=True)

    def get_job_future(self, job_id: str) -> Future | None:
        """
        输入：
        1. job_id: 维护任务 ID。
        输出：
        1. 对应 Future（不存在则 None）。
        用途：
        1. 调试维护任务执行状态。
        边界条件：
        1. 仅返回当前进程内提交过的任务。
        """

        with self._lock:
            return self._futures.get(job_id)
