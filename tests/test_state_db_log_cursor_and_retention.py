"""
StateDB 日志游标与保留策略测试。

职责：
1. 验证 task/maintenance 日志 after_log_id 游标查询正确且排序稳定。
2. 验证单 task/job 日志保留条数不超过配置上限。
3. 验证大样本下 list_tasks/list_maintenance_jobs 查询耗时可控。
边界：
1. 全部测试仅依赖临时 DuckDB，不依赖外部网络与真实行情源。
2. 仅覆盖状态库层行为，不覆盖 API 路由层序列化逻辑。
"""

from __future__ import annotations

import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from app.db.state_db import StateDB


class TestStateDBLogCursorAndRetention(unittest.TestCase):
    """StateDB 日志游标与保留策略测试集合。"""

    def setUp(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 初始化临时状态库并创建 schema。
        边界条件：
        1. 每个测试独立目录，避免文件句柄冲突。
        """

        self._tmp = tempfile.TemporaryDirectory(prefix="state-db-cursor-retention-")
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
        1. 关闭连接并清理临时目录。
        边界条件：
        1. close 异常不应阻断目录回收。
        """

        try:
            self.state_db.close()
        finally:
            self._tmp.cleanup()

    def _create_task(self, task_id: str) -> None:
        """
        输入：
        1. task_id: 任务 ID。
        输出：
        1. 无返回值。
        用途：
        1. 创建用于日志测试的最小 task 记录。
        边界条件：
        1. 起止时间固定，避免无关变量干扰断言。
        """

        self.state_db.create_task(
            task_id=task_id,
            source_db="test-source.duckdb",
            start_ts=datetime(2026, 2, 17, 9, 30, 0),
            end_ts=datetime(2026, 2, 17, 15, 0, 0),
            strategy_group_id="strategy_group_test",
            strategy_name="cursor-test",
            strategy_description="cursor-test",
            params={"case": "cursor"},
        )

    def _create_job(self, job_id: str) -> None:
        """
        输入：
        1. job_id: 维护任务 ID。
        输出：
        1. 无返回值。
        用途：
        1. 创建用于维护日志测试的最小 job 记录。
        边界条件：
        1. 模式固定 latest_update，避免模式分支影响本测试。
        """

        self.state_db.create_maintenance_job(
            job_id=job_id,
            source_db="test-source.duckdb",
            mode="latest_update",
            params={"case": "cursor"},
        )

    def test_task_logs_after_log_id_cursor_query_stable(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证 task 日志游标查询按 log_id 升序且 after_log_id 优先于 offset。
        边界条件：
        1. after_log_id 为空时走兼容 offset 查询；非空时必须走游标查询。
        """

        task_id = "task-cursor-case"
        self._create_task(task_id)
        for idx in range(12):
            self.state_db.append_log(
                task_id=task_id,
                level="info",
                message=f"log-{idx}",
                detail={"idx": idx},
            )

        first_page = self.state_db.get_logs(task_id=task_id, level="info", offset=0, limit=5)
        self.assertEqual(len(first_page), 5)
        first_ids = [int(item["log_id"]) for item in first_page]
        self.assertEqual(first_ids, sorted(first_ids))

        cursor = int(first_page[-1]["log_id"])
        second_page = self.state_db.get_logs(task_id=task_id, level="info", offset=999, limit=5, after_log_id=cursor)
        self.assertEqual(len(second_page), 5)
        second_ids = [int(item["log_id"]) for item in second_page]
        self.assertEqual(second_ids, sorted(second_ids))
        self.assertTrue(all(log_id > cursor for log_id in second_ids))
        self.assertTrue(set(first_ids).isdisjoint(set(second_ids)))

    def test_maintenance_logs_after_log_id_cursor_query_stable(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证维护日志游标查询按 log_id 升序且游标无重复。
        边界条件：
        1. offset 参数在 after_log_id 非空时应被忽略。
        """

        job_id = "job-cursor-case"
        self._create_job(job_id)
        for idx in range(9):
            self.state_db.append_maintenance_log(
                job_id=job_id,
                level="info",
                message=f"m-log-{idx}",
                detail={"idx": idx},
            )

        first_page = self.state_db.get_maintenance_logs(job_id=job_id, level="info", offset=0, limit=4)
        self.assertEqual(len(first_page), 4)
        first_ids = [int(item["log_id"]) for item in first_page]
        self.assertEqual(first_ids, sorted(first_ids))

        cursor = int(first_page[-1]["log_id"])
        second_page = self.state_db.get_maintenance_logs(
            job_id=job_id,
            level="info",
            offset=999,
            limit=4,
            after_log_id=cursor,
        )
        self.assertEqual(len(second_page), 4)
        second_ids = [int(item["log_id"]) for item in second_page]
        self.assertEqual(second_ids, sorted(second_ids))
        self.assertTrue(all(log_id > cursor for log_id in second_ids))
        self.assertTrue(set(first_ids).isdisjoint(set(second_ids)))

    def test_maintenance_logs_support_debug_level_filter(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证 maintenance 日志支持按 debug 级别过滤查询。
        边界条件：
        1. level=all 时应包含 info/debug 混合日志。
        """

        job_id = "job-debug-level-case"
        self._create_job(job_id)
        self.state_db.append_maintenance_log(
            job_id=job_id,
            level="info",
            message="info-log",
            detail={"kind": "summary"},
        )
        self.state_db.append_maintenance_log(
            job_id=job_id,
            level="debug",
            message="debug-log",
            detail={"kind": "full"},
        )

        debug_logs = self.state_db.get_maintenance_logs(job_id=job_id, level="debug", offset=0, limit=20)
        all_logs = self.state_db.get_maintenance_logs(job_id=job_id, level="all", offset=0, limit=20)

        self.assertEqual(len(debug_logs), 1)
        self.assertEqual(debug_logs[0]["level"], "debug")
        self.assertEqual(debug_logs[0]["message"], "debug-log")
        self.assertEqual(len(all_logs), 2)
        self.assertEqual([item["message"] for item in all_logs], ["info-log", "debug-log"])

    def test_task_log_retention_respects_per_task_limit(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证 task 日志超限后自动删除最旧记录，并同步 info/error 计数。
        边界条件：
        1. 保留上限通过 patch 缩小，避免测试写入过大样本。
        """

        task_id = "task-retention-case"
        self._create_task(task_id)
        with mock.patch("app.db.state_db.LOG_KEEP_TASK_PER_TASK", 5):
            for idx in range(11):
                level = "error" if idx % 3 == 0 else "info"
                self.state_db.append_log(
                    task_id=task_id,
                    level=level,
                    message=f"log-{idx}",
                    detail={"idx": idx},
                )

        logs = self.state_db.get_logs(task_id=task_id, level="all", offset=0, limit=50)
        self.assertEqual(len(logs), 5)
        self.assertEqual([item["message"] for item in logs], [f"log-{idx}" for idx in range(6, 11)])

        task = self.state_db.get_task(task_id)
        self.assertIsNotNone(task)
        info_count = sum(1 for item in logs if str(item.get("level")) == "info")
        error_count = sum(1 for item in logs if str(item.get("level")) == "error")
        self.assertEqual(int(task["info_log_count"]), info_count)
        self.assertEqual(int(task["error_log_count"]), error_count)

    def test_maintenance_log_retention_keeps_recent_jobs_full_logs(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证 maintenance 日志仅保留最近 N 个任务，且每个保留任务不做条数截断。
        边界条件：
        1. 使用小任务数验证“整任务删除”行为。
        """

        old_job_id = "job-retention-001"
        keep_job_id = "job-retention-002"
        latest_job_id = "job-retention-003"

        with mock.patch("app.db.state_db.LOG_KEEP_MAINTENANCE_JOBS", 2):
            self._create_job(old_job_id)
            for idx in range(6):
                self.state_db.append_maintenance_log(
                    job_id=old_job_id,
                    level="info",
                    message=f"old-{idx}",
                    detail={"idx": idx},
                )
            self._create_job(keep_job_id)
            for idx in range(7):
                self.state_db.append_maintenance_log(
                    job_id=keep_job_id,
                    level="info",
                    message=f"keep-{idx}",
                    detail={"idx": idx},
                )
            self._create_job(latest_job_id)
            for idx in range(8):
                self.state_db.append_maintenance_log(
                    job_id=latest_job_id,
                    level="info",
                    message=f"latest-{idx}",
                    detail={"idx": idx},
                )

        old_logs = self.state_db.get_maintenance_logs(job_id=old_job_id, level="all", offset=0, limit=50)
        keep_logs = self.state_db.get_maintenance_logs(job_id=keep_job_id, level="all", offset=0, limit=50)
        latest_logs = self.state_db.get_maintenance_logs(job_id=latest_job_id, level="all", offset=0, limit=50)

        self.assertEqual(len(old_logs), 0)
        self.assertEqual(len(keep_logs), 7)
        self.assertEqual(len(latest_logs), 8)
        self.assertEqual([item["message"] for item in keep_logs], [f"keep-{idx}" for idx in range(7)])
        self.assertEqual([item["message"] for item in latest_logs], [f"latest-{idx}" for idx in range(8)])

    def test_init_schema_preserves_existing_history_on_version_upgrade(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证 schema_version 升级时不会清空既有 task 历史数据。
        边界条件：
        1. 通过手工写回旧版本号模拟升级场景，避免依赖历史库文件。
        """

        task_id = "task-schema-upgrade-case"
        self._create_task(task_id)
        self.state_db.append_log(
            task_id=task_id,
            level="info",
            message="history-should-stay",
            detail={"case": "schema-upgrade"},
        )

        self.state_db._with_write_connection(
            lambda con: con.execute(
                """
                insert into app_meta(meta_key, meta_value)
                values ('schema_version', '3')
                on conflict (meta_key) do update set meta_value = excluded.meta_value
                """
            )
        )

        self.state_db.init_schema()

        task = self.state_db.get_task(task_id)
        logs = self.state_db.get_logs(task_id=task_id, level="all", offset=0, limit=10)

        self.assertIsNotNone(task)
        self.assertEqual(task["task_id"], task_id)
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["message"], "history-should-stay")

        schema_version_row = self.state_db._with_read_connection(
            lambda con: con.execute(
                "select meta_value from app_meta where meta_key = 'schema_version'"
            ).fetchone()
        )
        self.assertEqual(schema_version_row[0], StateDB.SCHEMA_VERSION)

    def test_list_queries_remain_fast_with_large_samples(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证较大样本下 list_tasks/list_maintenance_jobs 查询保持秒级内完成。
        边界条件：
        1. 该测试关注请求级耗时门槛，不比较绝对性能排名。
        """

        for idx in range(320):
            self._create_task(f"task-list-{idx:04d}")
            self._create_job(f"job-list-{idx:04d}")

        task_begin = time.monotonic()
        tasks = self.state_db.list_tasks(offset=0, limit=150)
        task_elapsed = time.monotonic() - task_begin

        job_begin = time.monotonic()
        jobs = self.state_db.list_maintenance_jobs(offset=0, limit=150)
        job_elapsed = time.monotonic() - job_begin

        self.assertEqual(len(tasks), 150)
        self.assertEqual(len(jobs), 150)
        self.assertLess(task_elapsed, 2.0, f"list_tasks 查询耗时过高: {task_elapsed:.3f}s")
        self.assertLess(job_elapsed, 2.0, f"list_maintenance_jobs 查询耗时过高: {job_elapsed:.3f}s")


if __name__ == "__main__":
    unittest.main(verbosity=2)
