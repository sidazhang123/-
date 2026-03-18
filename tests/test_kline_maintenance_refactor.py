"""
维护引擎重构测试。

职责：
1. 验证 latest_update 任务构建的差集补齐逻辑。
2. 验证 historical_backfill 三段任务构建逻辑。
3. 验证统一预处理规则与失败重试总 2 轮。
"""

from __future__ import annotations

import queue
import tempfile
import unittest
from contextlib import contextmanager, nullcontext
from datetime import datetime
from pathlib import Path
from unittest import mock

import duckdb

from app.services import kline_maintenance as km


class _DummyLogger:
    """日志桩。"""

    def info(self, message: str, detail: dict | None = None) -> None:
        _ = message
        _ = detail

    def warning(self, message: str, detail: dict | None = None) -> None:
        _ = message
        _ = detail

    def error(self, message: str, detail: dict | None = None) -> None:
        _ = message
        _ = detail

    def debug(self, message: str, detail: dict | None = None) -> None:
        _ = message
        _ = detail

    def debug_lazy(self, message: str, detail_fn: object = None) -> None:
        _ = message

    def error_file_lazy(self, message: str, detail: object = None) -> None:
        _ = message
        _ = detail

    def flush_debug(self) -> None:
        pass


class _FakeJob:
    """simple_api 异步任务桩。"""

    def __init__(self, events: list[dict]) -> None:
        self.queue: queue.Queue = queue.Queue()
        for event in events:
            self.queue.put(event)

    def done(self) -> bool:
        return True

    def result(self, timeout: float | None = None) -> list[dict]:
        _ = timeout
        return []


@contextmanager
def _yield_fake_job(job: _FakeJob):
    yield job


class TestKlineMaintenanceRefactor(unittest.TestCase):
    """维护引擎核心行为测试。"""

    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory(prefix="maint-refactor-")
        self.db_path = Path(self._temp_dir.name) / "quant.duckdb"
        self.service = km.MarketDataMaintenanceService(source_db_path=self.db_path, logger=_DummyLogger())
        self.service.ensure_runtime_schema()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _insert_row(self, table: str, *, code: str, dt: str) -> None:
        with duckdb.connect(str(self.db_path), read_only=False) as con:
            con.execute(
                f"""
                insert into {table}(code, datetime, open, high, low, close, volume, amount)
                values (?, ?, 10, 11, 9, 10, 100, 1000)
                """,
                [code, dt],
            )

    def test_latest_task_build_diff_patch(self) -> None:
        """
        输入：
        1. step4 仅命中一只股票，step0 股票全集含两只。
        输出：
        1. step5 会为缺失股票补齐全周期任务。
        用途：
        1. 验证 latest_update 步骤4/5。
        边界条件：
        1. 任务去重后应保留 10 条（2 只 * 5 周期）。
        """

        latest_by_freq = {
            "15": {"sh.600000": datetime(2026, 2, 26, 14, 45)},
            "30": {"sh.600000": datetime(2026, 2, 26, 14, 30)},
            "60": {"sh.600000": datetime(2026, 2, 26, 14, 0)},
            "d": {"sh.600000": datetime(2026, 2, 26, 15, 0)},
            "w": {"sh.600000": datetime(2026, 2, 21, 15, 0)},
        }
        tasks = self.service._build_latest_tasks(
            latest_by_freq=latest_by_freq,
            end_time=datetime(2026, 2, 27, 15, 0),
            all_codes={"sh.600000", "sz.000001"},
        )
        self.assertEqual(len(tasks), 10)

        signatures = {task.signature() for task in tasks}
        for freq in km.KLINE_FREQ_ORDER:
            self.assertTrue(any(sign.startswith(f"sz.000001|{freq}|2018-12-01 09:30:00|") for sign in signatures))

    def test_historical_task_builders(self) -> None:
        """
        输入：
        1. d 缺口、分钟条数异常、w 缺口样本。
        输出：
        1. 三类构建函数均生成对应任务。
        用途：
        1. 验证 historical_backfill 步骤1/2/3。
        边界条件：
        1. 步骤2只标记异常分钟数据，实际删除推迟到抓到非空新数据后。
        """

        self._insert_row("klines_d", code="sh.600000", dt="2026-02-10 15:00:00")
        self._insert_row("klines_d", code="sh.600000", dt="2026-02-13 15:00:00")
        self._insert_row("klines_w", code="sh.600000", dt="2026-01-02 15:00:00")
        self._insert_row("klines_w", code="sh.600000", dt="2026-01-20 15:00:00")

        # 60 分钟异常：应有 4 条，这里只写 3 条。
        for idx in range(3):
            self._insert_row("klines_60", code="sh.600000", dt=f"2026-02-10 0{9+idx}:30:00")

        step1 = self.service._build_historical_gap_tasks_from_daily()
        step2, removed_rows = self.service._build_historical_minute_count_tasks()
        step3 = self.service._build_historical_weekly_gap_tasks()

        step1_signs = {task.signature() for task in step1}
        self.assertIn("sh.600000|d|2026-02-11 09:30:00|2026-02-11 16:00:00", step1_signs)
        self.assertIn("sh.600000|60|2026-02-12 09:30:00|2026-02-12 16:00:00", step1_signs)

        step2_signs = {task.signature() for task in step2}
        self.assertIn("sh.600000|60|2026-02-10 09:30:00|2026-02-10 16:00:00", step2_signs)
        self.assertGreaterEqual(removed_rows, 3)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            remaining_rows = int(
                con.execute(
                    """
                    select count(*)
                    from klines_60
                    where code = ?
                      and cast(datetime as date) = ?
                    """,
                    ["sh.600000", "2026-02-10"],
                ).fetchone()[0]
                or 0
            )
        self.assertEqual(remaining_rows, 3)

        step3_signs = {task.signature() for task in step3}
        self.assertIn("sh.600000|w|2026-01-03 09:30:00|2026-01-20 16:00:00", step3_signs)

    def test_preprocessor_rules(self) -> None:
        """
        输入：
        1. 含 vol/year/month/day/hour/minute 字段的数据行。
        输出：
        1. 字段改名、截断、删字段与 d/w 15:00 规范生效。
        用途：
        1. 验证统一预处理函数契约。
        边界条件：
        1. event 非 data 时应原样返回。
        """

        op = self.service._build_preprocessor_operator()
        payload = {
            "event": "data",
            "task": {"code": "600000", "freq": "d", "start_time": "2026-02-10", "end_time": "2026-02-10"},
            "rows": [
                {
                    "code": "600000",
                    "datetime": "2026-02-10 10:33:21",
                    "open": 10.126,
                    "high": 10.789,
                    "low": 9.111,
                    "close": 10.222,
                    "vol": 1234.98,
                    "amount": 98765.88,
                    "year": 2026,
                    "month": 2,
                    "day": 10,
                    "hour": 10,
                    "minute": 33,
                }
            ],
            "error": None,
        }

        out = op(payload)
        self.assertIsNotNone(out)
        rows = out["rows"]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["open"], 10.13)
        self.assertEqual(row["high"], 10.79)
        self.assertEqual(row["low"], 9.11)
        self.assertEqual(row["close"], 10.22)
        self.assertEqual(row["volume"], 1234)
        self.assertEqual(row["amount"], 98765)
        self.assertEqual(row["datetime"], "2026-02-10 15:00:00")
        self.assertNotIn("year", row)
        self.assertNotIn("vol", row)

    def test_fetch_retry_total_two_rounds(self) -> None:
        """
        输入：
        1. 首轮失败 1 条、次轮补成功场景。
        输出：
        1. get_stock_kline 调用 2 次，最终无失败任务。
        用途：
        1. 验证失败重试总 2 轮口径。
        边界条件：
        1. 成功任务应落库。
        """

        task_a = km.MaintenanceTask("sh.600000", "d", "2026-02-10", "2026-02-10")
        task_b = km.MaintenanceTask("sz.000001", "d", "2026-02-10", "2026-02-10")

        events_round_1 = [
            {
                "event": "data",
                "task": task_a.to_zsdtdx_payload(),
                "rows": [{"code": "sh.600000", "datetime": "2026-02-10 15:00:00", "open": 10, "high": 11, "low": 9, "close": 10, "volume": 1, "amount": 2}],
                "error": None,
            },
            {"event": "data", "task": task_b.to_zsdtdx_payload(), "rows": [], "error": "timeout"},
            {"event": "done", "total_tasks": 2, "success_tasks": 1, "failed_tasks": 1},
        ]
        events_round_2 = [
            {
                "event": "data",
                "task": task_b.to_zsdtdx_payload(),
                "rows": [{"code": "sz.000001", "datetime": "2026-02-10 15:00:00", "open": 20, "high": 21, "low": 19, "close": 20, "volume": 3, "amount": 4}],
                "error": None,
            },
            {"event": "done", "total_tasks": 1, "success_tasks": 1, "failed_tasks": 0},
        ]

        fake_jobs = [_FakeJob(events_round_1), _FakeJob(events_round_2)]

        with (
            mock.patch.object(km, "get_tdx_client", side_effect=lambda: nullcontext()),
            mock.patch.object(km, "managed_stock_kline_job", side_effect=lambda **kwargs: _yield_fake_job(fake_jobs.pop(0))) as mock_managed_job,
        ):
            stats = self.service._execute_fetch_and_write(
                mode="latest_update",
                tasks=[task_a, task_b],
                progress_start=60.0,
                progress_end=95.0,
            )

        self.assertEqual(mock_managed_job.call_count, 2)
        self.assertEqual(stats.retry_rounds_used, 2)
        self.assertEqual(stats.total_tasks, 2)
        self.assertEqual(stats.success_tasks, 2)
        self.assertEqual(stats.failed_tasks, 0)

        with duckdb.connect(str(self.db_path), read_only=True) as con:
            count = int(con.execute("select count(*) from klines_d").fetchone()[0] or 0)
        self.assertGreaterEqual(count, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
