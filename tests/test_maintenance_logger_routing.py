"""
维护日志器测试。

职责：
1. 验证摘要日志写入路径。
2. 验证 debug 开关控制 debug 级落库。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from app.services.maintenance_logger import MaintenanceLogger


class _FakeStateDB:
    """状态库桩。"""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def append_maintenance_log(
        self,
        *,
        job_id: str,
        level: str,
        message: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        self.rows.append(
            {
                "job_id": job_id,
                "level": level,
                "message": message,
                "detail": detail,
            }
        )


class _FakeAppLogger:
    """应用日志器桩。"""

    def __init__(self) -> None:
        self.info_rows: list[str] = []
        self.warning_rows: list[str] = []
        self.error_rows: list[str] = []

    def info(self, pattern: str, *args: Any) -> None:
        self.info_rows.append(pattern % args)

    def warning(self, pattern: str, *args: Any) -> None:
        self.warning_rows.append(pattern % args)

    def error(self, pattern: str, *args: Any) -> None:
        self.error_rows.append(pattern % args)


class TestMaintenanceLoggerRouting(unittest.TestCase):
    """维护日志器行为测试。"""

    def test_info_warning_error_write_summary_logs(self) -> None:
        """
        输入：
        1. info/warning/error 三类调用。
        输出：
        1. app.log 与 maintenance_logs 均有摘要记录。
        用途：
        1. 验证主日志通路稳定。
        边界条件：
        1. warning 在 DB 中应写入 info 级前缀消息。
        """

        state_db = _FakeStateDB()
        app_logger = _FakeAppLogger()
        logger = MaintenanceLogger("job-1", state_db, app_logger, debug_enabled=False)

        logger.info("步骤0完成", {"tasks": [1, 2], "ok": 1})
        logger.warning("存在失败任务", {"failure_codes": ["sh.600000"], "count": 1})
        logger.error("维护失败", {"traceback": "...", "error": "x"})

        self.assertEqual(len(app_logger.info_rows), 1)
        self.assertEqual(len(app_logger.warning_rows), 1)
        self.assertEqual(len(app_logger.error_rows), 1)
        self.assertEqual(len(state_db.rows), 3)
        self.assertEqual(state_db.rows[0]["level"], "info")
        self.assertEqual(state_db.rows[1]["level"], "info")
        self.assertTrue(str(state_db.rows[1]["message"]).startswith("[warning] "))
        self.assertEqual(state_db.rows[2]["level"], "error")

    def test_debug_respects_runtime_switch(self) -> None:
        """
        输入：
        1. debug_enabled 关与开两种实例。
        输出：
        1. 仅 debug 开启实例写入 debug 记录。
        用途：
        1. 验证分模式 debug 控制行为。
        边界条件：
        1. debug 日志不写 app.log 也不写 DB，而是写到文件。
        2. 少量记录会缓冲，flush_debug 后才写入文件。
        """

        state_db = _FakeStateDB()
        app_logger = _FakeAppLogger()

        with tempfile.TemporaryDirectory() as tmpdir:
            debug_dir = Path(tmpdir)
            with mock.patch("app.services.maintenance_logger._DEBUG_LOG_DIR", debug_dir):
                logger_off = MaintenanceLogger("job-off", state_db, app_logger, debug_enabled=False)
                logger_on = MaintenanceLogger("job-on", state_db, app_logger, debug_enabled=True)

                logger_off.debug("步骤开始", {"step": 1})
                logger_on.debug("步骤开始", {"step": 2})

                # debug 关闭时不产生文件
                self.assertFalse((debug_dir / "maintenance_job-off.jsonl").exists())

                # 缓冲中，尚未写入文件
                debug_file = debug_dir / "maintenance_job-on.jsonl"
                self.assertFalse(debug_file.exists())

                # flush 后写入文件
                logger_on.flush_debug()
                self.assertTrue(debug_file.exists())
                lines = debug_file.read_text(encoding="utf-8").strip().split("\n")
                self.assertEqual(len(lines), 1)
                record = json.loads(lines[0])
                self.assertEqual(record["message"], "步骤开始")
                self.assertEqual(record["detail"], {"step": 2})
                self.assertEqual(record["job_id"], "job-on")

        # debug 不应写入 DB
        debug_rows = [row for row in state_db.rows if row["level"] == "debug"]
        self.assertEqual(len(debug_rows), 0)
        self.assertEqual(len(app_logger.info_rows), 0)
        self.assertEqual(len(app_logger.warning_rows), 0)
        self.assertEqual(len(app_logger.error_rows), 0)

    def test_debug_batch_auto_flush_at_threshold(self) -> None:
        """验证批量缓冲达到阈值时自动刷盘。"""

        from app.services.maintenance_logger import _DEBUG_BATCH_SIZE

        state_db = _FakeStateDB()
        app_logger = _FakeAppLogger()

        with tempfile.TemporaryDirectory() as tmpdir:
            debug_dir = Path(tmpdir)
            with mock.patch("app.services.maintenance_logger._DEBUG_LOG_DIR", debug_dir):
                logger = MaintenanceLogger("job-batch", state_db, app_logger, debug_enabled=True)
                debug_file = debug_dir / "maintenance_job-batch.jsonl"

                # 写入不足阈值的记录，不应自动刷盘
                for i in range(_DEBUG_BATCH_SIZE - 1):
                    logger.debug(f"entry-{i}", {"i": i})
                self.assertFalse(debug_file.exists())

                # 再写一条达到阈值，自动刷盘
                logger.debug(f"entry-{_DEBUG_BATCH_SIZE - 1}", {"i": _DEBUG_BATCH_SIZE - 1})
                self.assertTrue(debug_file.exists())
                lines = debug_file.read_text(encoding="utf-8").strip().split("\n")
                self.assertEqual(len(lines), _DEBUG_BATCH_SIZE)

    def test_debug_lazy_skips_evaluation_when_disabled(self) -> None:
        """验证 debug_lazy 在关闭时不调用回调。"""

        state_db = _FakeStateDB()
        app_logger = _FakeAppLogger()
        logger_off = MaintenanceLogger("job-off", state_db, app_logger, debug_enabled=False)

        called = [False]

        def _expensive():
            called[0] = True
            return {"big": "data"}

        logger_off.debug_lazy("贵操作", _expensive)
        self.assertFalse(called[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
