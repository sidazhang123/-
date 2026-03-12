"""
概念更新任务管理器。

职责：
1. 管理概念更新任务生命周期（创建、执行、停止、恢复中断状态）。
2. 调用概念更新引擎执行东财概念全量刷新。
3. 保持概念任务与 K 线维护任务相互隔离，但共享全局并发约束。
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
from app.services.concept_logger import ConceptLogger
from app.services.concept_maintenance import ConceptMaintenanceService, ConceptStopRequested
from app.settings import SOURCE_DB_PATH


class ConceptManager:
    """
    概念更新任务调度器（单并发）。

    设计约束：
    1. 同一时刻只允许一个概念任务运行。
    2. 概念任务和筛选任务、维护任务之间存在运行期互斥约束。
    """

    def __init__(self, state_db: StateDB, app_logger: logging.Logger):
        """
        输入：
        1. state_db: 状态库实例。
        2. app_logger: 应用日志器。
        输出：
        1. 无返回值。
        用途：
        1. 初始化概念任务执行线程池。
        边界条件：
        1. 概念任务固定单并发执行。
        """

        self.state_db = state_db
        self.app_logger = app_logger
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="concept-task")
        self._futures: dict[str, Future] = {}
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
        1. 统一概念任务终态判断。
        边界条件：
        1. 未知状态按非终态处理。
        """

        return str(status or "").strip().lower() in {"completed", "failed", "stopped"}

    def has_running_job(self) -> bool:
        """
        输入：
        1. 无。
        输出：
        1. 是否存在运行中的概念任务。
        用途：
        1. 作为创建新概念任务的并发保护。
        边界条件：
        1. queued/running/stopping 都视为运行中。
        """

        return self.state_db.get_running_concept_job() is not None

    def create_job(self) -> str:
        """
        输入：
        1. 无。
        输出：
        1. 新建任务 ID。
        用途：
        1. 创建并提交概念更新任务。
        边界条件：
        1. 若已有维护/概念/筛选运行中任务会抛 ValueError。
        """

        if self.has_running_job():
            raise ValueError("已有概念更新任务正在运行，请等待完成后再创建")
        if self.state_db.get_running_maintenance_job() is not None:
            raise ValueError("已有数据库维护任务正在运行，请等待完成后再执行概念更新")
        active_tasks = self.state_db.list_tasks_by_status(["running", "queued", "stopping"], limit=1)
        if active_tasks:
            raise ValueError("存在运行中的筛选任务，请先结束筛选任务后再执行概念更新")

        job_id = str(uuid4())
        self.state_db.create_concept_job(job_id=job_id, source_db=str(SOURCE_DB_PATH), params={})
        self._submit_job(job_id)
        return job_id

    def stop_job(self, job_id: str) -> dict[str, str]:
        """
        输入：
        1. job_id: 概念任务 ID。
        输出：
        1. 停止操作结果。
        用途：
        1. 将任务置为 stopping。
        边界条件：
        1. 终态任务不会重复停止。
        """

        job = self.state_db.get_concept_job(job_id)
        if not job:
            raise ValueError(f"概念任务不存在: {job_id}")
        status = str(job.get("status") or "")
        if status in {"completed", "failed", "stopped"}:
            return {"job_id": job_id, "status": status, "message": "概念任务已结束"}
        if status == "stopping":
            return {"job_id": job_id, "status": status, "message": "概念任务正在停止"}

        self.state_db.update_concept_job_fields(job_id, status="stopping")
        ConceptLogger(job_id=job_id, state_db=self.state_db, logger=self.app_logger).info("收到停止请求（协作取消阶段）")
        return {"job_id": job_id, "status": "stopping", "message": "停止请求已记录"}

    def _is_stop_requested(self, job_id: str) -> bool:
        """
        输入：
        1. job_id: 概念任务 ID。
        输出：
        1. 是否已请求停止。
        用途：
        1. 提供概念引擎协作取消检查。
        边界条件：
        1. 任务记录不存在时按已停止处理。
        """

        current = self.state_db.get_concept_job(job_id)
        if not current:
            return True
        return str(current.get("status") or "") in {"stopping", "stopped", "failed"}

    def _submit_job(self, job_id: str) -> bool:
        """
        输入：
        1. job_id: 概念任务 ID。
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

    def _recover_interrupted_jobs(self) -> None:
        """
        输入：
        1. 无。
        输出：
        1. 无返回值。
        用途：
        1. 服务重启时把中断中的概念任务标记为失败。
        边界条件：
        1. 仅处理 queued/running/stopping。
        """

        interrupted = self.state_db.list_concept_jobs_by_status(["queued", "running", "stopping"], limit=2000)
        for item in interrupted:
            job_id = str(item.get("job_id") or "")
            if not job_id:
                continue
            self.state_db.update_concept_job_fields(
                job_id,
                status="failed",
                finished_at=datetime.now(),
                error_message="服务重启导致概念任务中断，请重新执行",
            )
            self.state_db.append_concept_log(
                job_id=job_id,
                level="error",
                message="检测到服务重启，概念任务已标记为失败",
                detail=None,
            )

    def _run_job(self, job_id: str) -> None:
        """
        输入：
        1. job_id: 概念任务 ID。
        输出：
        1. 无返回值。
        用途：
        1. 后台执行单个概念任务。
        边界条件：
        1. 异常会转为 failed；停止请求会转为 stopped。
        """

        job = self.state_db.get_concept_job(job_id)
        if not job or self._is_terminal_status(str(job.get("status") or "")):
            return

        concept_logger = ConceptLogger(job_id=job_id, state_db=self.state_db, logger=self.app_logger, debug_enabled=True)
        progress_state: dict[str, float] = {"value": 0.0, "ts": 0.0}

        def on_progress(progress: float, phase: str | None = None, detail: dict[str, object] | None = None) -> None:
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
            self.state_db.update_concept_job_fields(job_id, **fields)
            progress_state["value"] = safe_progress
            progress_state["ts"] = now_ts
            if isinstance(detail, dict):
                concept_logger.debug_lazy("进度更新", lambda d=detail: d)

        self.state_db.update_concept_job_fields(
            job_id,
            status="running",
            phase="running",
            progress=0.0,
            error_message=None,
            started_at=job.get("started_at") or datetime.now(),
        )
        concept_logger.info("概念更新任务已开始")

        try:
            if self._is_stop_requested(job_id):
                self.state_db.update_concept_job_fields(
                    job_id,
                    status="stopped",
                    phase="stopped",
                    progress=100.0,
                    finished_at=datetime.now(),
                )
                concept_logger.info("概念任务在启动前被停止")
                return

            service = ConceptMaintenanceService(
                source_db_path=SOURCE_DB_PATH,
                logger=concept_logger,
                stop_checker=lambda: self._is_stop_requested(job_id),
                progress_reporter=on_progress,
            )
            summary = service.run_update()
            if self._is_stop_requested(job_id):
                self.state_db.update_concept_job_fields(
                    job_id,
                    status="stopped",
                    phase="stopped",
                    progress=100.0,
                    finished_at=datetime.now(),
                    summary_json=json.dumps(dataclasses.asdict(summary), ensure_ascii=False, default=str),
                )
                concept_logger.info("概念任务停止完成")
                return

            self.state_db.update_concept_job_fields(
                job_id,
                status="completed",
                phase="done",
                progress=100.0,
                finished_at=datetime.now(),
                summary_json=json.dumps(dataclasses.asdict(summary), ensure_ascii=False, default=str),
            )
            concept_logger.info("概念任务完成")
        except ConceptStopRequested:
            self.state_db.update_concept_job_fields(
                job_id,
                status="stopped",
                phase="stopped",
                progress=100.0,
                finished_at=datetime.now(),
            )
            concept_logger.info("概念任务停止完成")
        except Exception as exc:
            if self._is_stop_requested(job_id):
                self.state_db.update_concept_job_fields(
                    job_id,
                    status="stopped",
                    phase="stopped",
                    progress=100.0,
                    finished_at=datetime.now(),
                    error_message=None,
                )
                concept_logger.info("概念任务停止完成", {"reason": f"{type(exc).__name__}: {exc}"})
                return
            err_text = f"{type(exc).__name__}: {exc}"
            concept_logger.error("概念任务失败", {"error": err_text, "traceback": traceback.format_exc(limit=8)})
            self.state_db.update_concept_job_fields(
                job_id,
                status="failed",
                phase="failed",
                progress=100.0,
                finished_at=datetime.now(),
                error_message=err_text,
            )
        finally:
            concept_logger.flush_debug()

    def shutdown(self) -> None:
        """
        输入：
        1. 无。
        输出：
        1. 无返回值。
        用途：
        1. 应用停机时关闭概念任务执行池。
        边界条件：
        1. 不强杀线程，仅关闭线程池调度。
        """

        self.executor.shutdown(wait=False, cancel_futures=True)