"""
StateDB 并发读写测试。

职责：
1. 验证状态库读写分离后，写事务不会长期阻塞只读查询。
2. 验证维护日志连续写入期间，列表/日志读取接口可稳定返回。
3. 验证连接关闭后可重建并继续读写。
边界：
1. 仅验证 StateDB 行为，不覆盖 API 路由层。
2. 不依赖外部源库，仅使用临时 DuckDB 文件。
"""

from __future__ import annotations

import tempfile
import threading
import time
import unittest
from datetime import datetime
from pathlib import Path

from app.db.state_db import StateDB


class TestStateDBConcurrency(unittest.TestCase):
    """StateDB 并发与重连测试。"""

    def setUp(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 初始化临时状态库并创建基础 schema。
        边界条件：
        1. 每个用例独立临时目录，避免文件句柄互相污染。
        """

        self._tmp = tempfile.TemporaryDirectory(prefix="state-db-concurrency-")
        self.db_path = Path(self._tmp.name) / "screening_state.duckdb"
        self.state_db = StateDB(self.db_path)
        self.state_db.init_schema()

    def tearDown(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 关闭连接并释放临时目录资源。
        边界条件：
        1. 关闭阶段吞掉清理异常，避免影响用例结论。
        """

        try:
            self.state_db.close()
        finally:
            self._tmp.cleanup()

    def test_read_query_not_blocked_by_long_write_transaction(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证写连接长事务进行时，读连接查询可快速返回快照。
        边界条件：
        1. 写事务内显式 sleep，模拟维护阶段长耗时写入窗口。
        """

        self.state_db._with_write_connection(
            lambda con: con.execute(
                """
                create table if not exists _test_rw_lock (
                    id integer
                );
                delete from _test_rw_lock;
                insert into _test_rw_lock values (1);
                """
            )
        )

        started = threading.Event()
        finished = threading.Event()

        def _writer() -> None:
            """
            输入：
            1. 无显式输入参数。
            输出：
            1. 无返回值。
            用途：
            1. 在写连接内开启事务并长时间占用写路径。
            边界条件：
            1. 异常时回滚事务，避免污染后续断言。
            """

            def _op(con) -> None:
                con.execute("begin transaction")
                try:
                    con.execute(
                        """
                        insert into _test_rw_lock
                        select i
                        from range(2, 50000) as t(i)
                        """
                    )
                    started.set()
                    time.sleep(1.0)
                    con.execute("commit")
                except Exception:
                    con.execute("rollback")
                    raise

            self.state_db._with_write_connection(_op)
            finished.set()

        thread = threading.Thread(target=_writer, daemon=True)
        thread.start()
        self.assertTrue(started.wait(timeout=2.0), "写事务未按预期启动")

        begin = time.monotonic()
        count_before_commit = self.state_db._with_read_connection(
            lambda con: int(con.execute("select count(*) from _test_rw_lock").fetchone()[0] or 0)
        )
        elapsed = time.monotonic() - begin

        self.assertLess(elapsed, 0.6, f"读查询耗时过长：{elapsed:.3f}s")
        self.assertEqual(count_before_commit, 1, "读查询应看到提交前快照")

        self.assertTrue(finished.wait(timeout=3.0), "写事务未正常结束")
        thread.join(timeout=1.0)

        count_after_commit = self.state_db._with_read_connection(
            lambda con: int(con.execute("select count(*) from _test_rw_lock").fetchone()[0] or 0)
        )
        self.assertGreater(count_after_commit, 1)

    def test_read_stable_while_maintenance_logs_keep_writing(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证维护日志持续写入时，维护任务/日志读取接口仍可用。
        边界条件：
        1. 使用后台线程高频写入，主线程持续读取并统计最大耗时。
        """

        job_id = "job-concurrency-case"
        self.state_db.create_maintenance_job(
            job_id=job_id,
            source_db="test-source.duckdb",
            mode="latest_update",
            params={"case": "read_while_writing"},
        )

        stop_event = threading.Event()
        write_errors: list[Exception] = []

        def _writer() -> None:
            """
            输入：
            1. 无显式输入参数。
            输出：
            1. 无返回值。
            用途：
            1. 连续写入维护日志与进度，模拟运行中维护任务。
            边界条件：
            1. 任一异常记录后立即退出。
            """

            try:
                for idx in range(120):
                    if stop_event.is_set():
                        return
                    self.state_db.append_maintenance_log(
                        job_id=job_id,
                        level="info",
                        message=f"log-{idx}",
                        detail={"idx": idx},
                    )
                    if idx % 10 == 0:
                        self.state_db.update_maintenance_job_fields(job_id, progress=float(idx))
                    time.sleep(0.01)
            except Exception as exc:  # pragma: no cover - 用于诊断并发异常
                write_errors.append(exc)

        thread = threading.Thread(target=_writer, daemon=True)
        thread.start()

        max_elapsed = 0.0
        reads = 0
        deadline = time.monotonic() + 1.5
        try:
            while time.monotonic() < deadline:
                begin = time.monotonic()
                jobs = self.state_db.list_maintenance_jobs(offset=0, limit=10)
                logs = self.state_db.get_maintenance_logs(job_id=job_id, level="info", offset=0, limit=60)
                elapsed = time.monotonic() - begin
                max_elapsed = max(max_elapsed, elapsed)
                reads += 1
                self.assertTrue(any(item.get("job_id") == job_id for item in jobs))
                self.assertIsInstance(logs, list)
                time.sleep(0.02)
        finally:
            stop_event.set()
            thread.join(timeout=2.0)

        self.assertFalse(write_errors, f"写线程异常：{write_errors!r}")
        self.assertGreater(reads, 5, "读取次数不足，测试不充分")
        self.assertLess(max_elapsed, 0.8, f"读取耗时异常：{max_elapsed:.3f}s")

    def test_close_and_reopen_connection_still_works(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证双连接关闭后可重建并继续读取已写入数据。
        边界条件：
        1. close 可重复调用，不应抛出异常。
        """

        task_id = "task-reopen-case"
        self.state_db.create_task(
            task_id=task_id,
            source_db="test-source.duckdb",
            start_ts=datetime(2025, 1, 1, 9, 30, 0),
            end_ts=datetime(2025, 1, 1, 15, 0, 0),
            strategy_group_id="strategy_2",
            strategy_name="重连测试策略",
            strategy_description="验证 close/reopen",
            params={"foo": 1},
        )
        self.state_db.append_log(task_id=task_id, level="info", message="created", detail={"ok": True})

        self.state_db.close()
        self.state_db.close()

        reopened = StateDB(self.db_path)
        reopened.init_schema()
        try:
            task = reopened.get_task(task_id)
            logs = reopened.get_logs(task_id=task_id, level="info", offset=0, limit=10)
            self.assertIsNotNone(task)
            self.assertEqual(task["task_id"], task_id)
            self.assertGreaterEqual(len(logs), 1)
        finally:
            reopened.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
