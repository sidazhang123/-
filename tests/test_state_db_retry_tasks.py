"""
状态库 retry 任务表测试。

职责：
1. 验证 maintenance_retry_tasks schema 初始化。
2. 验证插入、读取、更新、删除闭环。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.db.state_db import StateDB


class TestStateDBRetryTasks(unittest.TestCase):
    """retry 任务表测试集合。"""

    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory(prefix="state-db-retry-")
        self.db_path = Path(self._temp_dir.name) / "state.duckdb"
        self.state_db = StateDB(self.db_path)
        self.state_db.init_schema()

    def tearDown(self) -> None:
        self.state_db.close()
        self._temp_dir.cleanup()

    def test_schema_contains_retry_table_and_no_legacy_table(self) -> None:
        """
        输入：
        1. 新初始化状态库。
        输出：
        1. 包含 maintenance_retry_tasks，不包含 maintenance_no_source_days。
        用途：
        1. 验证重构后 schema 目标态。
        边界条件：
        1. 表名匹配忽略大小写。
        """

        def _query_table_count(table_name: str) -> int:
            """
            输入：
            1. table_name: 待查询的目标表名。
            输出：
            1. 返回 information_schema.tables 中匹配数量。
            用途：
            1. 复用 StateDB 读连接执行 schema 断言，避免额外连接配置冲突。
            边界条件：
            1. 表名统一转小写进行匹配。
            """

            return int(
                self.state_db._with_read_connection(
                    lambda con: con.execute(
                        """
                        select count(*)
                        from information_schema.tables
                        where lower(table_name) = ?
                        """,
                        [table_name.lower()],
                    ).fetchone()[0]
                )
            )

        def _has_primary_key(table_name: str, column_name: str) -> bool:
            return bool(
                self.state_db._with_read_connection(
                    lambda con: con.execute(
                        """
                        select 1
                        from information_schema.table_constraints tc
                        join information_schema.key_column_usage kcu
                          on tc.constraint_name = kcu.constraint_name
                         and tc.table_name = kcu.table_name
                        where lower(tc.table_name) = ?
                          and tc.constraint_type = 'PRIMARY KEY'
                          and lower(kcu.column_name) = ?
                        limit 1
                        """,
                        [table_name.lower(), column_name.lower()],
                    ).fetchone()
                )
            )

        retry_count = _query_table_count("maintenance_retry_tasks")
        no_source_count = _query_table_count("maintenance_no_source_days")
        maintenance_logs_pk = _has_primary_key("maintenance_logs", "log_id")
        self.assertEqual(retry_count, 1)
        self.assertEqual(no_source_count, 0)
        self.assertTrue(maintenance_logs_pk)

    def test_retry_task_crud_flow(self) -> None:
        """
        输入：
        1. 两条 retry 任务样本。
        输出：
        1. 插入、更新、删除行为符合预期。
        用途：
        1. 验证历史维护步骤4所需状态接口。
        边界条件：
        1. 删除空列表应返回 0。
        """

        inserted = self.state_db.insert_maintenance_retry_tasks(
            [
                {
                    "task_key": "k1",
                    "mode": "historical_backfill",
                    "code": "sh.600000",
                    "freq": "d",
                    "start_date": "2026-02-10",
                    "end_date": "2026-02-10",
                    "attempt_count": 0,
                    "last_status": "pending",
                    "last_error": None,
                },
                {
                    "task_key": "k2",
                    "mode": "historical_backfill",
                    "code": "sz.000001",
                    "freq": "60",
                    "start_date": "2026-02-11",
                    "end_date": "2026-02-11",
                    "attempt_count": 1,
                    "last_status": "failed",
                    "last_error": "timeout",
                },
            ]
        )
        self.assertEqual(inserted, 2)

        rows = self.state_db.get_maintenance_retry_tasks(["k1", "k2"])
        self.assertEqual(set(rows.keys()), {"k1", "k2"})
        self.assertEqual(rows["k1"]["attempt_count"], 0)
        self.assertEqual(rows["k2"]["attempt_count"], 1)
        self.assertEqual(rows["k2"]["last_error"], "timeout")

        self.state_db.update_maintenance_retry_task(
            task_key="k1",
            attempt_count=2,
            last_status="failed",
            last_error="network",
        )
        updated = self.state_db.get_maintenance_retry_tasks(["k1"])["k1"]
        self.assertEqual(updated["attempt_count"], 2)
        self.assertEqual(updated["last_status"], "failed")
        self.assertEqual(updated["last_error"], "network")

        removed = self.state_db.delete_maintenance_retry_tasks(["k1", "k2"])
        self.assertEqual(removed, 2)
        self.assertEqual(self.state_db.get_maintenance_retry_tasks(["k1", "k2"]), {})
        self.assertEqual(self.state_db.delete_maintenance_retry_tasks([]), 0)

    def test_retry_task_chunked_read_and_bulk_update(self) -> None:
        """
        输入：
        1. 多条 retry 任务并启用小批次查询。
        输出：
        1. 分批读取完整命中，批量更新后字段生效。
        用途：
        1. 覆盖步骤4性能优化路径。
        边界条件：
        1. query_batch_size=3 强制触发分批查询。
        """

        rows = []
        for idx in range(8):
            rows.append(
                {
                    "task_key": f"k{idx}",
                    "mode": "historical_backfill",
                    "code": f"sh.600{idx:03d}",
                    "freq": "d",
                    "start_date": "2026-02-10",
                    "end_date": "2026-02-10",
                    "attempt_count": 0,
                    "last_status": "pending",
                    "last_error": None,
                }
            )
        inserted = self.state_db.insert_maintenance_retry_tasks(rows)
        self.assertEqual(inserted, 8)

        fetched = self.state_db.get_maintenance_retry_tasks([f"k{i}" for i in range(8)], query_batch_size=3)
        self.assertEqual(len(fetched), 8)

        updated_count = self.state_db.update_maintenance_retry_tasks(
            [
                {
                    "task_key": "k1",
                    "attempt_count": 2,
                    "last_status": "failed",
                    "last_error": "timeout",
                },
                {
                    "task_key": "k7",
                    "attempt_count": 1,
                    "last_status": "pending",
                    "last_error": None,
                },
            ]
        )
        self.assertEqual(updated_count, 2)

        after = self.state_db.get_maintenance_retry_tasks(["k1", "k7"])
        self.assertEqual(after["k1"]["attempt_count"], 2)
        self.assertEqual(after["k1"]["last_status"], "failed")
        self.assertEqual(after["k1"]["last_error"], "timeout")
        self.assertEqual(after["k7"]["attempt_count"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
