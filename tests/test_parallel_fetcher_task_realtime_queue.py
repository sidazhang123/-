"""
parallel_fetcher 任务实时队列测试。

职责：
1. 验证同步模式按任务输出 data 事件并最终输出 done 事件。
2. 验证 done 事件计数与成功/失败任务一致。
3. 验证异步模式立即返回 StockKlineJob 且可获取结果。
边界：
1. 通过 mock 迭代器和同步入口，避免真实网络和进程开销。
"""

from __future__ import annotations

import queue as py_queue
import sys
import time
import unittest
from pathlib import Path
from unittest import mock

_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE_ROOT))

from zsdtdx.parallel_fetcher import ParallelKlineFetcher, StockKlineJob


class TestParallelFetcherTaskRealtimeQueue(unittest.TestCase):
    """任务实时队列测试集合。"""

    def test_sync_queue_emits_data_and_done_with_counts(self) -> None:
        """验证 sync 模式会实时发 data，并在结束时发 done 统计。"""
        fetcher = ParallelKlineFetcher()
        fetcher.num_processes = 1
        q = py_queue.Queue()
        tasks = [
            {"code": "600000", "freq": "d", "start_time": "2026-02-01", "end_time": "2026-02-14"},
            {"code": "000001", "freq": "d", "start_time": "2026-02-01", "end_time": "2026-02-14"},
        ]
        payloads = [
            {"event": "data", "task": dict(tasks[0]), "rows": [{"code": "sh.600000"}], "error": None, "worker_pid": 1},
            {"event": "data", "task": dict(tasks[1]), "rows": [], "error": "no_data", "worker_pid": 1},
        ]

        with mock.patch.object(fetcher, "_iter_task_payloads_inproc_chunked", return_value=iter(payloads)):
            result = fetcher.fetch_stock_tasks_sync(tasks=tasks, queue=q)

        self.assertEqual(len(result), 2)
        queued = [q.get(timeout=1), q.get(timeout=1), q.get(timeout=1)]
        self.assertEqual(queued[0]["event"], "data")
        self.assertEqual(queued[1]["event"], "data")
        self.assertEqual(queued[2]["event"], "done")
        self.assertEqual(queued[2]["total_tasks"], 2)
        self.assertEqual(queued[2]["success_tasks"], 1)
        self.assertEqual(queued[2]["failed_tasks"], 1)

    def test_async_returns_job_immediately(self) -> None:
        """验证 async 模式立即返回 StockKlineJob，且可 result 取回结果。"""
        fetcher = ParallelKlineFetcher()
        tasks = [{"code": "600000", "freq": "d", "start_time": "2026-02-01", "end_time": "2026-02-14"}]
        sentinel = [{"event": "data", "task": dict(tasks[0]), "rows": [], "error": "no_data", "worker_pid": 0}]

        def _fake_parallel_iterator(_tasks):
            time.sleep(0.05)
            return iter(sentinel)

        with mock.patch.object(fetcher, "_ensure_async_prewarm", return_value=None), mock.patch.object(
            fetcher, "_iter_task_payloads_parallel_chunked", side_effect=_fake_parallel_iterator
        ):
            job = fetcher.fetch_stock_tasks_async(tasks=tasks)
            self.assertIsInstance(job, StockKlineJob)
            self.assertFalse(job.done())
            self.assertEqual(job.result(timeout=2), sentinel)
            self.assertTrue(job.done())

        self.assertIsInstance(job, StockKlineJob)


if __name__ == "__main__":
    unittest.main(verbosity=2)
