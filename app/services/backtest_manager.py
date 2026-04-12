"""
回测管理器: 回测 job 生命周期管理。

职责:
1. 互斥控制 (同时仅一个回测 job)
2. 创建/启动/停止 job，状态落库 (state_db)
3. 进度更新 + 日志落库
4. 结果写入 cache/backtest/{job_id}/ (CSV)
5. sweep 模式: 笛卡尔积参数遍历
"""

from __future__ import annotations

import bisect
import itertools
import json
import logging
import multiprocessing
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from app import settings
from app.services import backtest_engine, backtest_stats

import pandas as pd

logger = logging.getLogger(__name__)


class BacktestManager:
    """回测管理器单例。"""

    def __init__(self, state_db, app_logger: logging.Logger) -> None:
        from app.db.state_db import StateDB
        self.state_db: StateDB = state_db
        self.app_logger = app_logger
        self._lock = threading.Lock()
        self._current_job_id: str | None = None
        self._current_future = None
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="bt")
        self._active_detection_pools: set[Any] = set()
        self._force_failed_jobs: set[str] = set()
        self._job_options: dict[str, dict[str, Any]] = {}
        self._job_runtime_status: dict[str, dict[str, Any]] = {}
        self._stop_event = threading.Event()
        self._recover_interrupted_jobs()

    def _recover_interrupted_jobs(self) -> None:
        job = self.state_db.get_running_backtest_job()
        if job:
            job_id = job["job_id"]
            self.state_db.update_backtest_job_fields(
                job_id,
                status="failed",
                finished_at=datetime.now(),
                error_message="服务重启导致回测任务中断",
            )
            self.state_db.append_backtest_log(
                job_id=job_id, level="error",
                message="检测到服务重启，回测任务已标记为失败",
            )

    # ── 外部 API ──

    def create_job(
        self,
        *,
        strategy_group_id: str,
        mode: str,
        forward_bars: tuple[int, int, int],
        slide_step: int,
        group_params: dict[str, Any],
        param_ranges: dict[str, Any],
        lock_fwd_cache: bool = True,
    ) -> str:
        with self._lock:
            if self._current_job_id:
                existing = self.state_db.get_backtest_job(self._current_job_id)
                if existing and existing["status"] in ("queued", "running"):
                    raise RuntimeError("已有回测任务正在运行，请等待完成或停止后再创建")

            job_id = uuid.uuid4().hex[:12]
            self.state_db.create_backtest_job(
                job_id=job_id,
                strategy_group_id=strategy_group_id,
                mode=mode,
                forward_bars=forward_bars,
                slide_step=slide_step,
                group_params=group_params,
                param_ranges=param_ranges,
            )
            self._current_job_id = job_id
            self._stop_event.clear()
            self._force_failed_jobs.discard(job_id)
            self._job_options[job_id] = {"lock_fwd_cache": lock_fwd_cache}

        self._current_future = self._executor.submit(self._run_job, job_id)
        return job_id

    def stop_job(self, job_id: str) -> bool:
        """
        输入：
        1. job_id: 回测任务 ID。
        输出：
        1. 是否成功受理停止。
        用途：
        1. 用户点击停止按钮后立即把任务标记为失败，并强制终止阶段 B 的子进程。
        边界条件：
        1. 若任务不存在、不是当前任务，或已处于终态，则返回 False。
        """
        with self._lock:
            if self._current_job_id != job_id:
                return False
            job = self.state_db.get_backtest_job(job_id)
            if not job or job["status"] not in ("queued", "running", "stopping"):
                return False
        self._force_stop_job(job_id, error_message="stopped_by_user", log_message="收到停止请求，正在强制终止回测子进程")
        return True

    def get_job_status(self, job_id: str) -> dict[str, Any] | None:
        job = self.state_db.get_backtest_job(job_id)
        if not job:
            return None
        runtime_status = self._get_job_runtime_status(job_id)
        if runtime_status and job.get("status") in ("queued", "running", "stopping"):
            job.update(runtime_status)
        for k in ("created_at", "updated_at", "started_at", "finished_at"):
            if isinstance(job.get(k), datetime):
                job[k] = job[k].isoformat()
        return job

    def get_logs(self, job_id: str, level: str, after_log_id: int = 0) -> list[dict[str, Any]]:
        return self.state_db.get_backtest_logs(
            job_id, level, after_log_id=after_log_id if after_log_id else None,
        )

    def _set_job_runtime_status(self, job_id: str, **fields: Any) -> None:
        with self._lock:
            status = self._job_runtime_status.setdefault(job_id, {})
            status.update(fields)

    def _get_job_runtime_status(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            return dict(self._job_runtime_status.get(job_id, {}))

    def _clear_job_runtime_status(self, job_id: str) -> None:
        with self._lock:
            self._job_runtime_status.pop(job_id, None)

    def shutdown(self) -> None:
        """
        输入：
        1. 无。
        输出：
        1. 无返回值。
        用途：
        1. 应用停机时强制结束当前回测，并同步回收阶段 B 子进程与后台执行线程。
        边界条件：
        1. 若当前无活跃回测任务，则仅关闭执行器。
        """
        current_job_id: str | None = None
        with self._lock:
            current_job_id = self._current_job_id

        if current_job_id:
            job = self.state_db.get_backtest_job(current_job_id)
            if job and job["status"] in ("queued", "running", "stopping"):
                self._force_stop_job(
                    current_job_id,
                    error_message="forced_shutdown",
                    log_message="应用停机，正在强制终止回测子进程",
                )
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _force_stop_job(self, job_id: str, *, error_message: str, log_message: str) -> None:
        """
        输入：
        1. job_id: 回测任务 ID。
        2. error_message: 写入任务的错误码。
        3. log_message: 写入回测日志的说明文本。
        输出：
        1. 无返回值。
        用途：
        1. 统一处理手动停止与应用停机两种强制结束路径。
        边界条件：
        1. 若 Future 尚未运行，则仅取消提交，不等待线程返回。
        """
        with self._lock:
            self._stop_event.set()
            self._force_failed_jobs.add(job_id)
            pools = list(self._active_detection_pools)
            current_future = self._current_future
            self.state_db.update_backtest_job_fields(
                job_id,
                status="failed",
                finished_at=datetime.now(),
                error_message=error_message,
            )
            self.state_db.append_backtest_log(
                job_id=job_id,
                level="error",
                message=log_message,
            )

        if current_future is not None and not current_future.running():
            current_future.cancel()

        for pool in pools:
            self._terminate_detection_pool(job_id, pool)

    def _register_detection_pool(self, pool: Any) -> None:
        """
        输入：
        1. pool: 当前阶段 B 使用的进程池对象。
        输出：
        1. 无返回值。
        用途：
        1. 让 stop_job 能访问并强制终止当前回测子进程。
        边界条件：
        1. 仅注册当前进程内活跃的检测池。
        """
        with self._lock:
            self._active_detection_pools.add(pool)

    def _unregister_detection_pool(self, pool: Any) -> None:
        """
        输入：
        1. pool: 当前阶段 B 使用的进程池对象。
        输出：
        1. 无返回值。
        用途：
        1. 检测阶段结束后移除进程池引用，避免重复终止。
        边界条件：
        1. pool 不存在时静默忽略。
        """
        with self._lock:
            self._active_detection_pools.discard(pool)

    def _is_force_failed(self, job_id: str) -> bool:
        """
        输入：
        1. job_id: 回测任务 ID。
        输出：
        1. 当前任务是否已被标记为强制失败。
        用途：
        1. 防止后台执行线程在用户停止后把状态写回 completed 或 stopped。
        边界条件：
        1. 仅检查当前进程内存态标记。
        """
        with self._lock:
            return job_id in self._force_failed_jobs

    def _terminate_detection_pool(self, job_id: str, pool: Any) -> None:
        """
        输入：
        1. job_id: 回测任务 ID。
        2. pool: 检测阶段使用的进程池。
        输出：
        1. 无返回值。
        用途：
        1. 直接终止进程池中的 worker，避免 stop 请求卡在 future.result()。
        边界条件：
        1. 终止失败时只记录日志，不抛出异常阻断 stop 接口。
        """
        processes = list(getattr(pool, "_processes", {}).values())
        terminated = 0
        for process in processes:
            try:
                if process is None or not process.is_alive():
                    continue
                process.terminate()
                process.join(timeout=1)
                if process.is_alive() and hasattr(process, "kill"):
                    process.kill()
                    process.join(timeout=1)
                terminated += 1
            except Exception as exc:
                logger.warning("强制终止回测子进程失败 | %s | %s", job_id, exc)
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception as exc:
            logger.warning("关闭回测进程池失败 | %s | %s", job_id, exc)
        if terminated > 0:
            self.state_db.append_backtest_log(
                job_id=job_id,
                level="error",
                message=f"已强制终止 {terminated} 个回测子进程",
            )

    # ── 内部执行 ──

    def _run_job(self, job_id: str) -> None:
        """
        输入：
        1. job_id: 回测任务 ID。
        输出：
        1. 无返回值。
        用途：
        1. 在后台线程中执行固定参数或 sweep 回测，并维护任务状态与摘要。
        边界条件：
        1. 用户强制停止后不得把任务状态回写为 completed 或 stopped。
        """
        self.state_db.update_backtest_job_fields(
            job_id, status="running", started_at=datetime.now(),
        )

        job = self.state_db.get_backtest_job(job_id)
        self.state_db.append_backtest_log(
            job_id=job_id, level="info",
            message=f"回测开始: 策略={job['strategy_group_id']}, 模式={job['mode']}",
        )

        try:
            hooks, module_path = backtest_engine.load_backtest_hooks(job["strategy_group_id"])
            source_db_path = settings.SOURCE_DB_PATH
            fwd_cache_dir = settings.BACKTEST_FWD_CACHE_DIR
            cache_dir = settings.BACKTEST_CACHE_DIR
            max_workers = settings.BACKTEST_MAX_WORKERS

            source_version = self.state_db.get_meta_value("backtest.source_data_version")
            code_to_name = backtest_engine.load_code_to_name(source_db_path)
            opts = self._job_options.get(job_id, {})
            skip_ver = opts.get("lock_fwd_cache", True)

            def log_fn(level: str, msg: str) -> None:
                self.state_db.append_backtest_log(job_id=job_id, level=level, message=msg)

            if job["mode"] == "fixed":
                self._run_fixed(job_id, job, hooks, module_path, source_db_path,
                                fwd_cache_dir, cache_dir, max_workers,
                                source_version, code_to_name, log_fn,
                                skip_version_check=skip_ver)
            else:
                self._run_sweep(job_id, job, hooks, module_path, source_db_path,
                                fwd_cache_dir, cache_dir, max_workers,
                                source_version, code_to_name, log_fn,
                                skip_version_check=skip_ver)

            if self._is_force_failed(job_id):
                pass
            elif self._stop_event.is_set():
                self.state_db.update_backtest_job_fields(
                    job_id,
                    status="failed",
                    error_message="stopped_by_user",
                )
                log_fn("error", "回测已因停止请求中断")
            else:
                self.state_db.update_backtest_job_fields(
                    job_id, status="completed", progress=1.0,
                )
                log_fn("info", "回测完成")

        except Exception as exc:
            if self._is_force_failed(job_id):
                logger.info("回测任务已按停止请求强制终止: %s", job_id)
            else:
                self.state_db.update_backtest_job_fields(
                    job_id, status="failed", error_message=str(exc),
                )
                self.state_db.append_backtest_log(
                    job_id=job_id, level="error", message=f"回测失败: {exc}",
                )
                logger.exception("回测任务执行异常: %s", job_id)

        finished_at = datetime.now()
        job_data = self.state_db.get_backtest_job(job_id)
        started_at = job_data.get("started_at") if job_data else None
        duration = (finished_at - started_at).total_seconds() if started_at else 0

        summary = job_data.get("summary", {}) if job_data else {}
        summary["duration_seconds"] = round(duration, 2)
        self.state_db.update_backtest_job_fields(
            job_id,
            finished_at=finished_at,
            summary_json=json.dumps(summary, ensure_ascii=False, default=str),
        )
        self._clear_job_runtime_status(job_id)
        with self._lock:
            if self._current_job_id == job_id:
                self._current_job_id = None
            self._current_future = None
            self._force_failed_jobs.discard(job_id)
            self._job_options.pop(job_id, None)

    def _run_fixed(
        self,
        job_id: str,
        job: dict[str, Any],
        hooks: dict[str, Any],
        module_path: str,
        source_db_path: Path,
        fwd_cache_dir: Path,
        cache_dir: Path,
        max_workers: int,
        source_version: str | None,
        code_to_name: dict[str, str],
        log_fn: Callable[[str, str], None],
        skip_version_check: bool = False,
    ) -> None:
        """固定参数回测。"""
        tf_sections: dict[str, dict] = hooks["tf_sections"]
        forward_bars = tuple(job["forward_bars"])
        x, y, z = forward_bars
        group_params = job["group_params"]
        detect_weight = 0.95
        stats_weight = 0.05

        # ── 阶段 A: 预计算 ──
        parquet_paths: dict[str, Path] = {}
        for section_key, section_info in tf_sections.items():
            if self._stop_event.is_set():
                return

            # 检查参数中是否启用了该周期
            det_kwargs = hooks["normalize_params"](group_params, section_key)
            params = det_kwargs.get("params") or det_kwargs.get("tf_params")
            if params and not params.get("enabled", True):
                continue

            tf_key = section_info["tf_key"]
            table_name = section_info["table"]

            pq_path = backtest_engine.precompute_forward_metrics(
                source_db_path=source_db_path,
                tf_key=tf_key,
                table_name=table_name,
                forward_bars=(x, y, z),
                fwd_cache_dir=fwd_cache_dir,
                source_data_version=source_version,
                skip_version_check=skip_version_check,
                log_fn=log_fn,
            )
            parquet_paths[tf_key] = pq_path

        if not parquet_paths:
            log_fn("info", "无启用的周期，回测结束")
            return

        # ── 阶段 B: 检测 ──
        section_hits_map: dict[str, list[dict[str, Any]]] = {}
        max_total_stocks = 0
        active_sections = [sk for sk, si in tf_sections.items() if si["tf_key"] in parquet_paths]

        # 复用单一 Manager 避免 Windows 反复创建/销毁进程导致 OSError
        mp_manager = multiprocessing.Manager()
        try:
            for sec_idx, section_key in enumerate(active_sections):
                if self._stop_event.is_set():
                    return

                section_info = tf_sections[section_key]
                tf_key = section_info["tf_key"]
                table_name = section_info["table"]

                all_codes = backtest_engine.load_all_codes(source_db_path, table_name)
                max_total_stocks = max(max_total_stocks, len(all_codes))

                # 创建进度队列和消费线程
                progress_queue = mp_manager.Queue()
                processed_count = 0
                progress_done = threading.Event()

                def _consume_progress(
                    q=progress_queue, done_evt=progress_done,
                    jid=job_id, total=len(all_codes), si=sec_idx,
                    n_sec=len(active_sections),
                ):
                    nonlocal processed_count
                    try:
                        while not done_evt.is_set():
                            try:
                                while True:
                                    q.get_nowait()
                                    processed_count += 1
                            except Exception:
                                pass
                            sec_frac = processed_count / total if total > 0 else 0.0
                            prog = detect_weight * ((si + sec_frac) / n_sec) if n_sec > 0 else 0.0
                            self.state_db.update_backtest_job_fields(
                                jid,
                                progress=round(min(prog, detect_weight), 4),
                                processed_stocks=processed_count,
                                total_stocks=total,
                            )
                            done_evt.wait(timeout=0.5)
                    except Exception:
                        logger.exception("[_consume_progress] consumer thread crashed")

                progress_thread = threading.Thread(target=_consume_progress, daemon=True)
                progress_thread.start()

                hits = backtest_engine.run_detection_parallel(
                    all_codes=all_codes,
                    parquet_path=parquet_paths[tf_key],
                    tf_key=tf_key,
                    table_name=table_name,
                    hooks_module_path=module_path,
                    group_params=group_params,
                    section_key=section_key,
                    slide_step=job["slide_step"],
                    max_forward_bar=max(forward_bars),
                    max_workers=max_workers,
                    progress_queue=progress_queue,
                    on_pool_started=self._register_detection_pool,
                    on_pool_finished=self._unregister_detection_pool,
                    log_fn=log_fn,
                )

                progress_done.set()
                progress_thread.join(timeout=5)

                # 排干队列残余项，确保 processed_stocks 达到 total
                try:
                    while True:
                        progress_queue.get_nowait()
                        processed_count += 1
                except Exception:
                    pass
                self.state_db.update_backtest_job_fields(
                    job_id,
                    processed_stocks=len(all_codes),
                    total_stocks=len(all_codes),
                )

                section_hits_map[section_key] = hits
                progress = detect_weight * ((sec_idx + 1) / len(active_sections))
                self.state_db.update_backtest_job_fields(
                    job_id, progress=round(progress, 4), total_stocks=max_total_stocks,
                )
        finally:
            mp_manager.shutdown()

        # ── tf_logic 合并 ──
        tf_logic = hooks.get("tf_logic", "or")
        if tf_logic == "and" and len(section_hits_map) > 1:
            all_hits = _intersect_hits_and(section_hits_map, tf_sections)
            log_fn("info", f"tf_logic=and 交集后命中: {len(all_hits)}")
        else:
            all_hits = [h for hs in section_hits_map.values() for h in hs]

        log_fn("info", f"总命中: {len(all_hits)}")

        self.state_db.update_backtest_job_fields(
            job_id, progress=round(detect_weight, 4),
        )

        # ── 阶段 C: 指标解析 ──
        hits_df = backtest_engine.resolve_hit_metrics(
            hits=all_hits,
            parquet_paths=parquet_paths,
            forward_bars=(x, y, z),
            code_to_name=code_to_name,
            log_fn=log_fn,
        )

        # ── 阶段 D: 输出 ──
        output_dir = cache_dir / job_id
        backtest_engine.write_hits_csv(hits_df, output_dir)

        # 统计
        summary: dict[str, Any] = {"total_hits": len(all_hits)}
        if not hits_df.empty:
            stats = backtest_stats.compute_full_stats(hits_df, (x, y, z))
            backtest_engine.write_stats_json(stats, output_dir)
            summary["stats"] = stats
        else:
            backtest_engine.write_stats_json(
                {"total_hits": 0, "unique_stocks": 0, "per_forward": {}},
                output_dir,
            )

        self.state_db.update_backtest_job_fields(
            job_id,
            progress=round(detect_weight + stats_weight, 4),
            summary_json=json.dumps(summary, ensure_ascii=False, default=str),
        )
        log_fn("info", "结果已写入")

    def _run_sweep(
        self,
        job_id: str,
        job: dict[str, Any],
        hooks: dict[str, Any],
        module_path: str,
        source_db_path: Path,
        fwd_cache_dir: Path,
        cache_dir: Path,
        max_workers: int,
        source_version: str | None,
        code_to_name: dict[str, str],
        log_fn: Callable[[str, str], None],
        skip_version_check: bool = False,
    ) -> None:
        """动态调参回测 (sweep 模式)。

        逐参数组合检测，每个 section 共享进程池以节省进程创建开销。
        """
        tf_sections = hooks["tf_sections"]
        forward_bars = tuple(job["forward_bars"])
        x, y, z = forward_bars
        group_params = job["group_params"]
        param_ranges = job["param_ranges"]
        normalize_params = hooks["normalize_params"]
        detect_weight = 0.95
        stats_weight = 0.05

        # 生成参数组合
        param_combos = self._generate_param_combos(group_params, param_ranges)
        combo_total = len(param_combos)
        self.state_db.update_backtest_job_fields(job_id, combo_total=combo_total)
        log_fn("info", f"参数组合总数: {combo_total}")
        active_sections: list[dict[str, Any]] = []
        for section_key, section_info in tf_sections.items():
            enabled_combo_params: list[tuple[int, dict[str, Any]]] = []
            for combo_idx, combo_params in enumerate(param_combos):
                det_kwargs = normalize_params(combo_params, section_key)
                params = det_kwargs.get("params") or det_kwargs.get("tf_params")
                if params and not params.get("enabled", True):
                    continue
                enabled_combo_params.append((combo_idx, combo_params))
            if not enabled_combo_params:
                continue
            active_sections.append(
                {
                    "section_key": section_key,
                    "section_info": section_info,
                    "enabled_combo_params": enabled_combo_params,
                }
            )

        # ── 阶段 A: 预计算 (前瞻指标与参数无关，只算一次) ──
        parquet_paths: dict[str, Path] = {}
        for section_idx, section_plan in enumerate(active_sections):
            if self._stop_event.is_set():
                return

            section_key = section_plan["section_key"]
            section_info = section_plan["section_info"]
            tf_key = section_info["tf_key"]
            table_name = section_info["table"]
            self._set_job_runtime_status(
                job_id,
                phase="detect",
                phase_label=f"检测 {section_key}",
                phase_index=section_idx + 1,
                phase_total=len(active_sections),
            )

            pq_path = backtest_engine.precompute_forward_metrics(
                source_db_path=source_db_path,
                tf_key=tf_key,
                table_name=table_name,
                forward_bars=(x, y, z),
                fwd_cache_dir=fwd_cache_dir,
                source_data_version=source_version,
                skip_version_check=skip_version_check,
                log_fn=log_fn,
            )
            parquet_paths[tf_key] = pq_path

        if not active_sections or not parquet_paths:
            log_fn("info", "无可用周期，回测结束")
            return

        output_dir = cache_dir / job_id
        output_dir.mkdir(parents=True, exist_ok=True)

        # 预加载启用 section 的股票列表，避免批量检测时重复查询
        for section_plan in active_sections:
            section_plan["all_codes"] = backtest_engine.load_all_codes(
                source_db_path,
                section_plan["section_info"]["table"],
            )
        n_active = len(active_sections)

        # ── 阶段 B+C: 先按 section 批量检测，再按组合顺序统计/落盘 ──
        sweep_results: list[dict[str, Any]] = []
        spill_dir = cache_dir / "_spill"
        backtest_engine.cleanup_spill_dir(spill_dir)
        section_accumulators: dict[str, backtest_engine.SweepHitsAccumulator] = {}
        merged_accumulator: backtest_engine.SweepHitsAccumulator | None = None

        mp_manager = multiprocessing.Manager()
        try:
            for sec_idx, section_plan in enumerate(active_sections):
                if self._stop_event.is_set():
                    return

                section_key = section_plan["section_key"]
                section_info = section_plan["section_info"]
                all_codes = section_plan["all_codes"]
                enabled_combo_params = section_plan["enabled_combo_params"]
                tf_key = section_info["tf_key"]

                progress_queue = mp_manager.Queue()
                processed_count = 0.0
                worker_partials: dict[int, tuple[int, int]] = {}
                progress_done = threading.Event()
                self._set_job_runtime_status(
                    job_id,
                    phase="detect",
                    phase_label=f"检测 {section_key}",
                    phase_index=sec_idx + 1,
                    phase_total=n_active,
                )

                def _drain_progress_queue(q=progress_queue) -> None:
                    nonlocal processed_count
                    while True:
                        try:
                            item = q.get_nowait()
                        except Exception:
                            break
                        if isinstance(item, dict):
                            worker_id = int(item.get("worker") or 0)
                            item_type = item.get("type")
                            if item_type == "combo_progress":
                                combo_done = max(0, int(item.get("done") or 0))
                                combo_total = max(1, int(item.get("total") or 1))
                                if worker_id > 0:
                                    worker_partials[worker_id] = (combo_done, combo_total)
                            elif item_type == "stock_done":
                                processed_count += 1.0
                                if worker_id > 0:
                                    worker_partials.pop(worker_id, None)
                            else:
                                processed_count += float(item.get("count") or 0.0)
                        else:
                            processed_count += float(item)

                def _update_detect_status(
                    jid=job_id,
                    total=len(all_codes),
                    current_index=sec_idx,
                    total_sections=n_active,
                ) -> None:
                    processed_display = min(int(processed_count), total)
                    partial_stock_units = sum(done / combo_total for done, combo_total in worker_partials.values())
                    estimated_done = min(processed_count + partial_stock_units, float(total))
                    section_frac = estimated_done / total if total > 0 else 0.0
                    progress = detect_weight * ((current_index + section_frac) / total_sections)
                    phase_label = f"检测 {section_key} | 股票 {processed_display}/{total}"
                    partial_count = len(worker_partials)
                    if partial_count > 0:
                        avg_combo_total = max(combo_total for _, combo_total in worker_partials.values())
                        avg_combo_done = int(
                            sum(done for done, _ in worker_partials.values()) / partial_count
                        )
                        phase_label += f" | 平均组合进度 {avg_combo_done}/{avg_combo_total}"
                    self._set_job_runtime_status(
                        jid,
                        phase="detect",
                        phase_label=phase_label,
                        phase_index=current_index + 1,
                        phase_total=total_sections,
                    )
                    self.state_db.update_backtest_job_fields(
                        jid,
                        processed_stocks=processed_display,
                        total_stocks=total,
                        progress=round(min(progress, detect_weight), 4),
                    )

                def _consume_progress(
                    q=progress_queue,
                    done_evt=progress_done,
                    jid=job_id,
                    total=len(all_codes),
                    current_index=sec_idx,
                    total_sections=n_active,
                ) -> None:
                    try:
                        while not done_evt.is_set():
                            _ = (jid, total, current_index, total_sections)
                            _drain_progress_queue(q)
                            _update_detect_status()
                            done_evt.wait(timeout=0.5)
                    except Exception:
                        logger.exception("[_consume_progress] consumer thread crashed")

                _update_detect_status()
                progress_thread = threading.Thread(target=_consume_progress, daemon=True)
                progress_thread.start()

                section_hits_map = backtest_engine.run_detection_parallel_batch(
                    all_codes=all_codes,
                    parquet_path=parquet_paths[tf_key],
                    tf_key=tf_key,
                    table_name=section_info["table"],
                    hooks_module_path=module_path,
                    combo_params_list=enabled_combo_params,
                    section_key=section_key,
                    slide_step=job["slide_step"],
                    max_forward_bar=max(forward_bars),
                    max_workers=max_workers,
                    progress_queue=progress_queue,
                    on_pool_started=self._register_detection_pool,
                    on_pool_finished=self._unregister_detection_pool,
                    log_fn=log_fn,
                    spill_dir=spill_dir,
                )

                progress_done.set()
                progress_thread.join(timeout=5)

                _drain_progress_queue(progress_queue)
                _update_detect_status()
                self.state_db.update_backtest_job_fields(
                    job_id,
                    processed_stocks=len(all_codes),
                    total_stocks=len(all_codes),
                    progress=round(detect_weight * ((sec_idx + 1) / n_active), 4),
                )

                section_accumulators[section_key] = section_hits_map

            # ── tf_logic 合并 ──
            tf_logic = hooks.get("tf_logic", "or")
            if len(section_accumulators) == 1:
                # 只有一个 section，直接复用（零拷贝）
                merged_accumulator = next(iter(section_accumulators.values()))
            elif tf_logic == "and" and len(section_accumulators) > 1:
                any_spilled = any(a.is_spilled for a in section_accumulators.values())
                if any_spilled:
                    merged_accumulator = _intersect_hits_and_from_db(
                        section_accumulators, tf_sections, spill_dir, log_fn,
                    )
                else:
                    # 内存模式 AND — 用现有函数，结果写入新 accumulator
                    merged_accumulator = backtest_engine.SweepHitsAccumulator(spill_dir=spill_dir)
                    for combo_idx in range(combo_total):
                        merged_accumulator.ensure_combo(combo_idx)
                    for combo_idx in range(combo_total):
                        sh: dict[str, list[dict]] = {}
                        for sk, acc in section_accumulators.items():
                            hits_list = acc._mem.get(combo_idx, [])
                            if hits_list:
                                sh[sk] = hits_list
                        if not sh:
                            continue
                        if len(sh) > 1:
                            merged_hits = _intersect_hits_and(sh, tf_sections)
                        else:
                            merged_hits = list(next(iter(sh.values())))
                        if merged_hits:
                            merged_accumulator.extend(combo_idx, merged_hits)
                    total_raw = sum(a.total_hits for a in section_accumulators.values())
                    log_fn("info", f"tf_logic=and 交集: {total_raw} → {merged_accumulator.total_hits}")
                # 关闭旧 section accumulators（merged_accumulator 是新的）
                for sk, acc in section_accumulators.items():
                    acc.close()
                section_accumulators.clear()
            else:
                # tf_logic=or，多个 section 合并
                merged_accumulator = backtest_engine.SweepHitsAccumulator(spill_dir=spill_dir)
                for combo_idx in range(combo_total):
                    merged_accumulator.ensure_combo(combo_idx)
                for sk, acc in section_accumulators.items():
                    if acc.is_spilled:
                        # 从 DB 批量读取并追加
                        all_cis = list(acc.hit_counts.keys())
                        for batch_start in range(0, len(all_cis), 500):
                            batch_cis = all_cis[batch_start:batch_start + 500]
                            batch_df = acc.query_combo_batch(batch_cis)
                            for ci_val, grp in batch_df.groupby("_combo_idx", sort=False):
                                rows = grp.to_dict("records")
                                merged_accumulator.extend(int(ci_val), [
                                    {"code": r["code"], "tf_key": r["tf_key"],
                                     "pattern_start_ts": r["pattern_start_ts"],
                                     "pattern_end_ts": r["pattern_end_ts"]}
                                    for r in rows
                                ])
                    else:
                        for ci, hits in acc.items():
                            if hits:
                                merged_accumulator.extend(ci, hits)
                    acc.close()
                section_accumulators.clear()

            # ── 阶段 C: 批量指标解析（一次 JOIN 所有组合） ──
            # 确保所有 combo_idx 存在（含未启用的 section 中被跳过的 combo）
            for ci in range(combo_total):
                merged_accumulator.ensure_combo(ci)
            self._set_job_runtime_status(
                job_id,
                phase="stats",
                phase_label="批量指标解析",
                phase_index=0,
                phase_total=combo_total,
            )
            combo_metrics_map = backtest_engine.resolve_hit_metrics_sweep(
                combo_hits_map=None,
                hit_accumulator=merged_accumulator,
                parquet_paths=parquet_paths,
                forward_bars=(x, y, z),
                code_to_name=code_to_name,
                log_fn=log_fn,
            )

            for combo_idx, combo_params in enumerate(param_combos):
                if self._stop_event.is_set():
                    return

                combo_label = {p: _get_nested(combo_params, p) for p in param_ranges}
                combo_hit_count = merged_accumulator.hit_counts.get(combo_idx, 0)
                hits_df = combo_metrics_map.pop(combo_idx, pd.DataFrame())
                self._set_job_runtime_status(
                    job_id,
                    phase="stats",
                    phase_label="统计参数组合",
                    phase_index=combo_idx + 1,
                    phase_total=combo_total,
                )
                combo_progress = detect_weight
                if combo_total > 0:
                    combo_progress += stats_weight * (combo_idx / combo_total)
                self.state_db.update_backtest_job_fields(
                    job_id,
                    progress=round(min(combo_progress, 0.9999), 4),
                )

                combo_dir = output_dir / f"combo_{combo_idx}"
                backtest_engine.write_hits_csv(hits_df, combo_dir)

                combo_stats: dict[str, Any] = {"total_hits": combo_hit_count}
                if not hits_df.empty:
                    stats = backtest_stats.compute_full_stats(hits_df, (x, y, z))
                    backtest_engine.write_stats_json(stats, combo_dir)
                    combo_stats.update(stats)

                sweep_row = {
                    "combo_index": combo_idx,
                    "param_combo": combo_label,
                    "total_hits": combo_hit_count,
                }
                per_fwd = combo_stats.get("per_forward", {})
                for fwd_n, fwd_label in [(x, "x"), (y, "y"), (z, "z")]:
                    fwd_stats = per_fwd.get(str(fwd_n), {})
                    profit_stats = fwd_stats.get("profit", {})
                    drawdown_stats = fwd_stats.get("drawdown", {})
                    sweep_row[f"win_rate_{fwd_label}"] = profit_stats.get("win_rate")
                    sweep_row[f"avg_profit_{fwd_label}"] = profit_stats.get("mean")
                    sweep_row[f"avg_drawdown_{fwd_label}"] = drawdown_stats.get("mean")

                sweep_results.append(sweep_row)
                final_combo_progress = 1.0
                if combo_total > 0:
                    final_combo_progress = detect_weight + stats_weight * ((combo_idx + 1) / combo_total)
                self.state_db.update_backtest_job_fields(
                    job_id,
                    progress=round(final_combo_progress, 4),
                    combo_index=combo_idx + 1,
                )

        finally:
            mp_manager.shutdown()
            # 清理所有 accumulator（确保异常/取消时也释放）
            for acc in section_accumulators.values():
                if acc is not merged_accumulator:
                    acc.close()
            section_accumulators.clear()
            if merged_accumulator is not None:
                merged_accumulator.close()
                merged_accumulator = None

        # 写 sweep 汇总
        sweep_path = output_dir / "sweep_results.json"
        sweep_path.write_text(
            json.dumps(sweep_results, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        summary = {"sweep_results": sweep_results}
        self.state_db.update_backtest_job_fields(
            job_id,
            summary_json=json.dumps(summary, ensure_ascii=False, default=str),
        )
        log_fn("info", f"sweep 完成: {len(sweep_results)} 组合")

    @staticmethod
    def _generate_param_combos(
        base_params: dict[str, Any],
        param_ranges: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """生成笛卡尔积参数组合。

        自动跳过 scope 参数路径（由 manifest scope_params 声明）。
        """
        import copy

        if not param_ranges:
            return [copy.deepcopy(base_params)]

        # 构建 scope 路径集合：如 {"weekly.scan_bars", "daily.scan_bars"}
        scope_paths: set[str] = set()
        for section_key, section_val in base_params.items():
            if isinstance(section_val, dict):
                for sp in section_val.get("scope_params", []):
                    scope_paths.add(f"{section_key}.{sp}")

        # 解析各参数的候选值（跳过 scope 路径）
        param_paths: list[str] = []
        param_values: list[list[float]] = []

        for path, range_spec in param_ranges.items():
            if path in scope_paths:
                continue
            r_min = range_spec.get("min", range_spec.get("min_val", 0))
            r_max = range_spec.get("max", range_spec.get("max_val", 0))
            r_step = range_spec.get("step", 1)

            if r_step <= 0:
                r_step = 1

            vals: list[float] = []
            v = r_min
            while v <= r_max + 1e-9:
                vals.append(round(v, 6))
                v += r_step

            # 始终包含 max 端点
            if not vals:
                vals = [r_min]
            if abs(vals[-1] - r_max) > 1e-9:
                vals.append(round(r_max, 6))

            param_paths.append(path)
            param_values.append(vals)

        # 笛卡尔积
        combos: list[dict[str, Any]] = []
        for combo_vals in itertools.product(*param_values):
            params = copy.deepcopy(base_params)
            for path, val in zip(param_paths, combo_vals):
                _set_nested(params, path, val)
            combos.append(params)

        return combos


# ─────────────────────────────────────────────────────────────────────────────
# tf_logic 多周期交集 (AND)
# ─────────────────────────────────────────────────────────────────────────────

_TF_ORDER: list[str] = ["w", "d", "60", "30", "15"]

# 以最粗周期为锚点，在此窗口内查找更细周期的命中
_TF_ALIGN_WINDOW: dict[str, timedelta] = {
    "w": timedelta(days=7),
    "d": timedelta(days=1),
    "60": timedelta(days=1),
    "30": timedelta(days=1),
    "15": timedelta(hours=4),
}


def _intersect_hits_and(
    section_hits: dict[str, list[dict[str, Any]]],
    tf_sections: dict[str, dict],
) -> list[dict[str, Any]]:
    """AND 交集：只保留所有已启用周期在时间窗口内同时命中的记录。

    以最粗周期（coarsest TF）的命中为锚点，逐条检查每个更细周期是否存在
    同一代码在 ``[anchor - window, anchor + 1day)`` 范围内的命中。
    仅输出通过全部检查的最粗周期命中，保持前瞻指标一致。
    """
    if len(section_hits) <= 1:
        return list(next(iter(section_hits.values()), []))

    # section_key -> tf_key（仅保留有命中数据的 section）
    section_tf: dict[str, str] = {}
    for sk in section_hits:
        if sk in tf_sections:
            section_tf[sk] = tf_sections[sk]["tf_key"]
    if not section_tf:
        return []

    def _tf_rank(sk: str) -> int:
        tfk = section_tf.get(sk, "d")
        return _TF_ORDER.index(tfk) if tfk in _TF_ORDER else 999

    sorted_sks = sorted(section_tf.keys(), key=_tf_rank)
    coarsest_sk = sorted_sks[0]
    finer_sks = sorted_sks[1:]

    if not finer_sks:
        return section_hits[coarsest_sk]

    coarsest_tf_key = section_tf[coarsest_sk]
    window = _TF_ALIGN_WINDOW.get(coarsest_tf_key, timedelta(days=7))

    # 为更细周期构建 {code: sorted([ts, ...])} 索引
    finer_lookups: dict[str, dict[str, list[pd.Timestamp]]] = {}
    for sk in finer_sks:
        code_dates: dict[str, list[pd.Timestamp]] = defaultdict(list)
        for hit in section_hits[sk]:
            code_dates[hit["code"]].append(pd.Timestamp(hit["pattern_end_ts"]))
        for code in code_dates:
            code_dates[code].sort()
        finer_lookups[sk] = dict(code_dates)

    # 逐条过滤 coarsest TF 的命中
    result: list[dict[str, Any]] = []
    for hit in section_hits[coarsest_sk]:
        code = hit["code"]
        anchor_ts = pd.Timestamp(hit["pattern_end_ts"])
        win_lo = anchor_ts - window
        win_hi = anchor_ts + timedelta(days=1)  # 包含同日日内命中

        aligned = True
        for sk in finer_sks:
            dates = finer_lookups[sk].get(code)
            if not dates:
                aligned = False
                break
            # 在已排序列表中二分查找 win_lo
            lo = bisect.bisect_left(dates, win_lo)
            if lo < len(dates) and dates[lo] < win_hi:
                continue  # 窗口内有命中
            aligned = False
            break

        if aligned:
            result.append(hit)

    return result


def _intersect_hits_and_from_db(
    section_accumulators: dict[str, backtest_engine.SweepHitsAccumulator],
    tf_sections: dict[str, dict],
    spill_dir: Path,
    log_fn: Callable[[str, str], None],
) -> backtest_engine.SweepHitsAccumulator:
    """AND 交集：将各 section 数据加载为 DataFrame，用内存 DuckDB 做 JOIN。"""
    import duckdb as _ddb

    # section_key -> tf_key
    section_tf: dict[str, str] = {}
    for sk in section_accumulators:
        if sk in tf_sections:
            section_tf[sk] = tf_sections[sk]["tf_key"]

    def _tf_rank(sk: str) -> int:
        tfk = section_tf.get(sk, "d")
        return _TF_ORDER.index(tfk) if tfk in _TF_ORDER else 999

    sorted_sks = sorted(section_tf.keys(), key=_tf_rank)
    coarsest_sk = sorted_sks[0]
    finer_sks = sorted_sks[1:]

    if not finer_sks:
        return section_accumulators[coarsest_sk]

    coarsest_tf_key = section_tf[coarsest_sk]
    window = _TF_ALIGN_WINDOW.get(coarsest_tf_key, timedelta(days=7))

    # 确保所有 section 的 pending 缓冲已写盘
    for acc in section_accumulators.values():
        acc._flush_pending()

    # 将各 section 转为 DataFrame（load_all_to_mem 处理内存/pickle两种模式）
    section_dfs: dict[str, pd.DataFrame] = {}
    _cols = ["combo_idx", "code", "tf_key", "pattern_start_ts", "pattern_end_ts"]
    for sk, acc in section_accumulators.items():
        all_data = acc.load_all_to_mem()
        rows = []
        for ci, hits in all_data.items():
            for h in hits:
                rows.append((ci, h["code"], h["tf_key"],
                             h["pattern_start_ts"], h["pattern_end_ts"]))
        section_dfs[sk] = pd.DataFrame(rows, columns=_cols) if rows else pd.DataFrame(columns=_cols)

    # 用纯内存 DuckDB 做 JOIN（无文件 IO）
    merged_acc = backtest_engine.SweepHitsAccumulator(spill_dir=spill_dir)
    merge_db = _ddb.connect()
    try:
        tbl_names: dict[str, str] = {}
        for i, sk in enumerate(section_accumulators):
            tbl = f"sec_{i}"
            merge_db.register(tbl, section_dfs[sk])
            tbl_names[sk] = tbl

        coarsest_tbl = tbl_names[coarsest_sk]
        window_interval = f"{int(window.total_seconds())} seconds"

        exists_clauses = []
        for sk in finer_sks:
            finer_tbl = tbl_names[sk]
            exists_clauses.append(
                f"EXISTS (SELECT 1 FROM {finer_tbl} f "
                f"WHERE f.combo_idx = c.combo_idx AND f.code = c.code "
                f"AND f.pattern_end_ts >= c.pattern_end_ts - INTERVAL '{window_interval}' "
                f"AND f.pattern_end_ts < c.pattern_end_ts + INTERVAL '1 day')"
            )

        where_clause = " AND ".join(exists_clauses)
        sql = (
            f"SELECT c.combo_idx, c.code, c.tf_key, "
            f"c.pattern_start_ts, c.pattern_end_ts "
            f"FROM {coarsest_tbl} c "
            f"WHERE {where_clause}"
        )

        result_df = merge_db.execute(sql).fetchdf()
    finally:
        merge_db.close()

    log_fn("info", f"tf_logic=and: 交集结果 {len(result_df)} 行")

    if not result_df.empty:
        # 直接写入 merged_acc 内存（AND 结果通常远小于原始数据）
        for ci_val, grp in result_df.groupby("combo_idx", sort=False):
            ci = int(ci_val)
            hits = grp[["code", "tf_key", "pattern_start_ts", "pattern_end_ts"]].to_dict("records")
            merged_acc._mem[ci] = hits
            merged_acc.hit_counts[ci] = len(hits)
        merged_acc.total_hits = len(result_df)
        uk = result_df[["code", "tf_key", "pattern_end_ts"]].drop_duplicates()
        merged_acc.unique_keys = set(uk.itertuples(index=False, name=None))

    return merged_acc


def _set_nested(d: dict, path: str, value: Any) -> None:
    """在嵌套字典中按 'a.b.c' 路径设置值。"""
    keys = path.split(".")
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    d[keys[-1]] = value


def _get_nested(d: dict, path: str, default: Any = None) -> Any:
    """从嵌套字典中按 'a.b.c' 路径读取值。"""
    keys = path.split(".")
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k, default)
    return d
