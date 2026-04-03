"""
回测管理器强制停止测试。

覆盖范围：
1. stop_job 会立即把任务标记为 failed。
2. stop_job 会直接终止阶段 B 注册的子进程池。
3. _run_job 在收到强制停止后不会把状态回写为 completed。
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from app.services.backtest_manager import BacktestManager


class _DummyStateDB:
    """最小状态库桩。"""

    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, Any]] = {}
        self.logs: list[dict[str, Any]] = []
        self.meta: dict[str, Any] = {}

    def get_running_backtest_job(self) -> dict[str, Any] | None:
        """
        输入：
        1. 无。
        输出：
        1. 当前运行中的回测任务或 None。
        用途：
        1. 满足 BacktestManager 初始化恢复逻辑。
        边界条件：
        1. 测试桩始终返回 None。
        """
        return None

    def get_backtest_job(self, job_id: str) -> dict[str, Any] | None:
        """
        输入：
        1. job_id: 回测任务 ID。
        输出：
        1. 任务字典或 None。
        用途：
        1. 供管理器读取当前任务状态。
        边界条件：
        1. 未创建任务时返回 None。
        """
        return self.jobs.get(job_id)

    def update_backtest_job_fields(self, job_id: str, **fields: Any) -> None:
        """
        输入：
        1. job_id: 回测任务 ID。
        2. fields: 待更新字段。
        输出：
        1. 无返回值。
        用途：
        1. 模拟状态库字段更新。
        边界条件：
        1. 未创建任务时会自动创建最小记录。
        """
        job = self.jobs.setdefault(job_id, {"job_id": job_id, "summary": {}})
        job.update(fields)

    def append_backtest_log(self, *, job_id: str, level: str, message: str) -> None:
        """
        输入：
        1. job_id: 回测任务 ID。
        2. level: 日志级别。
        3. message: 日志内容。
        输出：
        1. 无返回值。
        用途：
        1. 记录管理器写出的回测日志。
        边界条件：
        1. 仅做内存追加。
        """
        self.logs.append({"job_id": job_id, "level": level, "message": message})

    def get_meta_value(self, key: str) -> Any:
        """
        输入：
        1. key: 元数据键。
        输出：
        1. 元数据值或 None。
        用途：
        1. 满足 _run_job 对 source_data_version 的读取。
        边界条件：
        1. 未设置时返回 None。
        """
        return self.meta.get(key)


class _FakeFuture:
    """Future 测试桩。"""

    def __init__(self, *, running: bool) -> None:
        self._running = running
        self.cancel_called = False

    def running(self) -> bool:
        """
        输入：
        1. 无。
        输出：
        1. Future 是否处于运行态。
        用途：
        1. 模拟 ThreadPoolExecutor Future 的运行态查询。
        边界条件：
        1. 返回构造时传入的固定值。
        """
        return self._running

    def cancel(self) -> bool:
        """
        输入：
        1. 无。
        输出：
        1. 是否取消成功。
        用途：
        1. 验证 stop_job 会尝试取消尚未运行的任务。
        边界条件：
        1. 仅在非运行态时返回 True。
        """
        self.cancel_called = True
        return not self._running


class _FakeProcess:
    """子进程测试桩。"""

    def __init__(self) -> None:
        self.alive = True
        self.terminated = False
        self.killed = False

    def is_alive(self) -> bool:
        """
        输入：
        1. 无。
        输出：
        1. 子进程是否存活。
        用途：
        1. 模拟 multiprocessing.Process 存活检查。
        边界条件：
        1. 由测试手动改变状态。
        """
        return self.alive

    def terminate(self) -> None:
        """
        输入：
        1. 无。
        输出：
        1. 无返回值。
        用途：
        1. 模拟强制终止子进程。
        边界条件：
        1. 终止后进程视为不再存活。
        """
        self.terminated = True
        self.alive = False

    def kill(self) -> None:
        """
        输入：
        1. 无。
        输出：
        1. 无返回值。
        用途：
        1. 模拟二次强杀。
        边界条件：
        1. 调用后进程视为不再存活。
        """
        self.killed = True
        self.alive = False

    def join(self, timeout: float | None = None) -> None:
        """
        输入：
        1. timeout: 等待秒数。
        输出：
        1. 无返回值。
        用途：
        1. 模拟进程 join。
        边界条件：
        1. 测试桩不执行真实等待。
        """
        _ = timeout


class _FakePool:
    """进程池测试桩。"""

    def __init__(self, processes: list[_FakeProcess]) -> None:
        self._processes = {idx: proc for idx, proc in enumerate(processes)}
        self.shutdown_called = False

    def shutdown(self, wait: bool = False, cancel_futures: bool = False) -> None:
        """
        输入：
        1. wait: 是否等待。
        2. cancel_futures: 是否取消 Future。
        输出：
        1. 无返回值。
        用途：
        1. 模拟进程池关闭。
        边界条件：
        1. 仅记录是否被调用。
        """
        _ = (wait, cancel_futures)
        self.shutdown_called = True


def _make_manager() -> tuple[BacktestManager, _DummyStateDB]:
    """
    输入：
    1. 无。
    输出：
    1. BacktestManager 与其测试状态库。
    用途：
    1. 为测试构造最小可运行的回测管理器。
    边界条件：
    1. 不会提交真实后台任务。
    """
    state_db = _DummyStateDB()
    manager = BacktestManager(state_db, logging.getLogger("test.backtest_manager"))
    return manager, state_db


def test_stop_job_marks_failed_and_terminates_process_pool() -> None:
    """
    输入：
    1. 无。
    输出：
    1. 无返回值。
    用途：
    1. 验证用户停止时会立即失败落库并强制终止检测子进程。
    边界条件：
    1. 任务处于 running 态且存在活跃检测池。
    """
    manager, state_db = _make_manager()
    job_id = "job-force-stop"
    state_db.jobs[job_id] = {"job_id": job_id, "status": "running", "summary": {}}
    manager._current_job_id = job_id
    manager._current_future = _FakeFuture(running=True)

    proc1 = _FakeProcess()
    proc2 = _FakeProcess()
    pool = _FakePool([proc1, proc2])
    manager._register_detection_pool(pool)

    ok = manager.stop_job(job_id)

    assert ok is True
    assert state_db.jobs[job_id]["status"] == "failed"
    assert state_db.jobs[job_id]["error_message"] == "stopped_by_user"
    assert isinstance(state_db.jobs[job_id]["finished_at"], datetime)
    assert proc1.terminated is True
    assert proc2.terminated is True
    assert pool.shutdown_called is True
    assert any("强制终止" in item["message"] for item in state_db.logs)
    manager.shutdown()


def test_run_job_does_not_overwrite_force_failed_status(monkeypatch) -> None:
    """
    输入：
    1. monkeypatch: pytest monkeypatch。
    输出：
    1. 无返回值。
    用途：
    1. 验证后台线程在收到强制停止后不会把状态改回 completed。
    边界条件：
    1. 固定参数回测在执行中途收到 stop_job。
    """
    manager, state_db = _make_manager()
    job_id = "job-run-stop"
    state_db.jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "strategy_group_id": "demo",
        "mode": "fixed",
        "forward_bars": [2, 5, 7],
        "slide_step": 3,
        "group_params": {},
        "param_ranges": {},
        "summary": {},
    }
    manager._current_job_id = job_id
    manager._current_future = _FakeFuture(running=True)

    monkeypatch.setattr("app.services.backtest_manager.settings.SOURCE_DB_PATH", "source")
    monkeypatch.setattr("app.services.backtest_manager.settings.BACKTEST_FWD_CACHE_DIR", "fwd")
    monkeypatch.setattr("app.services.backtest_manager.settings.BACKTEST_CACHE_DIR", "cache")
    monkeypatch.setattr("app.services.backtest_manager.settings.BACKTEST_MAX_WORKERS", 1)
    monkeypatch.setattr(
        "app.services.backtest_manager.backtest_engine.load_backtest_hooks",
        lambda strategy_group_id: ({
            "tf_sections": {},
            "normalize_params": lambda params, section_key: {},
        }, "demo.module"),
    )
    monkeypatch.setattr(
        "app.services.backtest_manager.backtest_engine.load_code_to_name",
        lambda source_db_path: {},
    )

    def _fake_run_fixed(*args, **kwargs) -> None:
        manager.stop_job(job_id)

    monkeypatch.setattr(manager, "_run_fixed", _fake_run_fixed)

    manager._run_job(job_id)

    assert state_db.jobs[job_id]["status"] == "failed"
    assert state_db.jobs[job_id]["error_message"] == "stopped_by_user"
    assert manager._current_job_id is None
    manager.shutdown()


def test_shutdown_forces_current_job_failed_and_terminates_process_pool() -> None:
    """
    输入：
    1. 无。
    输出：
    1. 无返回值。
    用途：
    1. 验证应用停机时会沿用强制失败逻辑并回收回测子进程。
    边界条件：
    1. 当前任务处于 running 态且存在活跃检测池。
    """
    manager, state_db = _make_manager()
    job_id = "job-shutdown-stop"
    state_db.jobs[job_id] = {"job_id": job_id, "status": "running", "summary": {}}
    manager._current_job_id = job_id
    manager._current_future = _FakeFuture(running=True)

    proc = _FakeProcess()
    pool = _FakePool([proc])
    manager._register_detection_pool(pool)

    manager.shutdown()

    assert state_db.jobs[job_id]["status"] == "failed"
    assert state_db.jobs[job_id]["error_message"] == "forced_shutdown"
    assert proc.terminated is True
    assert pool.shutdown_called is True
    assert any("应用停机" in item["message"] for item in state_db.logs)