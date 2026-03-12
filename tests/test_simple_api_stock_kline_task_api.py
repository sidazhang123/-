"""
simple_api.get_stock_kline 任务化入口测试。

职责：
1. 验证 task 同时支持 dict 和 StockKlineTask。
2. 验证旧签名参数被移除后会报错。
3. 验证 sync/async 模式路由到并行抓取器新接口。
边界：
1. 通过 mock get_fetcher 避免真实网络与进程调用。
"""

from __future__ import annotations

import queue as py_queue
import sys
import unittest
from pathlib import Path
from unittest import mock

_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE_ROOT))

from zsdtdx import simple_api as sa


class _FakeFetcher:
    """simple_api 测试用抓取器桩。"""

    def __init__(self):
        self.sync_calls = []
        self.async_calls = []

    def fetch_stock_tasks_sync(self, *, tasks, queue=None, preprocessor_operator=None):
        self.sync_calls.append(
            {
                "tasks": tasks,
                "queue": queue,
                "preprocessor_operator": preprocessor_operator,
            }
        )
        return [{"event": "data", "task": tasks[0], "rows": [], "error": "no_data", "worker_pid": 0}]

    def fetch_stock_tasks_async(self, *, tasks, queue=None, preprocessor_operator=None):
        self.async_calls.append(
            {
                "tasks": tasks,
                "queue": queue,
                "preprocessor_operator": preprocessor_operator,
            }
        )
        return "ASYNC_JOB"


class TestSimpleApiStockKlineTaskApi(unittest.TestCase):
    """simple_api 任务化入口测试集合。"""

    def test_task_supports_dict_and_class_in_sync_mode(self) -> None:
        """验证 task 可混用 dict 与 StockKlineTask，且会归一化后下发。"""
        fake_fetcher = _FakeFetcher()
        q = py_queue.Queue()

        with mock.patch("zsdtdx.simple_api.get_fetcher", return_value=fake_fetcher):
            result = sa.get_stock_kline(
                task=[
                    sa.StockKlineTask(code="600000", freq="d", start_time="2026-02-01", end_time="2026-02-14"),
                    {"code": "000001", "freq": "60min", "start_time": "2026-02-01", "end_time": "2026-02-14"},
                ],
                queue=q,
                mode="sync",
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(len(fake_fetcher.sync_calls), 1)
        sent_tasks = fake_fetcher.sync_calls[0]["tasks"]
        self.assertEqual(sent_tasks[0]["freq"], "d")
        self.assertEqual(sent_tasks[1]["freq"], "60")
        self.assertEqual(sent_tasks[0]["end_time"], "2026-02-14 16:00:00")
        self.assertIs(fake_fetcher.sync_calls[0]["queue"], q)

    def test_old_signature_removed(self) -> None:
        """验证旧签名参数 codes/freq/start_time/end_time 已不可用。"""
        with self.assertRaises(TypeError):
            sa.get_stock_kline(
                codes=["600000"],
                freq=["d"],
                start_time="2026-02-01",
                end_time="2026-02-14",
            )

    def test_same_start_end_date_auto_plus_one_day(self) -> None:
        """验证纯日期输入会规范化到同日 09:30:00-16:00:00。"""
        fake_fetcher = _FakeFetcher()
        with mock.patch("zsdtdx.simple_api.get_fetcher", return_value=fake_fetcher):
            _ = sa.get_stock_kline(
                task=[{"code": "600000", "freq": "d", "start_time": "2026-02-13", "end_time": "2026-02-13"}],
                mode="sync",
            )
        sent_task = fake_fetcher.sync_calls[0]["tasks"][0]
        self.assertEqual(sent_task["start_time"], "2026-02-13 09:30:00")
        self.assertEqual(sent_task["end_time"], "2026-02-13 16:00:00")

    def test_async_mode_routes_to_async_fetcher(self) -> None:
        """验证 mode=async 时不传 queue 也会自动创建队列并返回句柄对象。"""
        fake_fetcher = _FakeFetcher()

        with mock.patch("zsdtdx.simple_api.get_fetcher", return_value=fake_fetcher):
            job = sa.get_stock_kline(
                task=[{"code": "600000", "freq": "d", "start_time": "2026-02-01", "end_time": "2026-02-14"}],
                mode="async",
            )

        self.assertEqual(job, "ASYNC_JOB")
        self.assertEqual(len(fake_fetcher.async_calls), 1)
        self.assertTrue(hasattr(fake_fetcher.async_calls[0]["queue"], "put"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
