"""
preprocessor_operator 行为测试。

职责：
1. 验证预处理可改字段、改值、删字段。
2. 验证返回 None 时该条结果不会入队也不会进入返回值。
边界：
1. 通过 mock 串行迭代器避免真实网络请求。
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

from zsdtdx.parallel_fetcher import ParallelKlineFetcher


class TestStockKlinePreprocessorOperator(unittest.TestCase):
    """预处理函数行为测试集合。"""

    def test_preprocessor_transform_and_delete_fields(self) -> None:
        """验证预处理可执行字段改名、值加工和字段删除。"""
        fetcher = ParallelKlineFetcher()
        fetcher.num_processes = 1
        q = py_queue.Queue()
        task = {"code": "600000", "freq": "d", "start_time": "2026-02-01", "end_time": "2026-02-14"}
        raw_payload = {
            "event": "data",
            "task": dict(task),
            "rows": [{"code": "sh.600000", "amount": 100.0, "open": 10.0}],
            "error": None,
            "worker_pid": 123,
        }

        def _preprocess(payload: dict) -> dict | None:
            item = dict(payload)
            item["task_code"] = item["task"]["code"]
            item.pop("worker_pid", None)
            rows = []
            for row in item.get("rows", []):
                updated = dict(row)
                updated["turnover"] = updated.pop("amount", None)
                rows.append(updated)
            item["rows"] = rows
            return item

        with mock.patch.object(fetcher, "_iter_task_payloads_inproc_chunked", return_value=iter([raw_payload])):
            result = fetcher.fetch_stock_tasks_sync(
                tasks=[task],
                queue=q,
                preprocessor_operator=_preprocess,
            )

        self.assertEqual(len(result), 1)
        first = result[0]
        self.assertNotIn("worker_pid", first)
        self.assertEqual(first["task_code"], "600000")
        self.assertIn("turnover", first["rows"][0])
        self.assertNotIn("amount", first["rows"][0])

        queued_data = q.get(timeout=1)
        queued_done = q.get(timeout=1)
        self.assertEqual(queued_data["event"], "data")
        self.assertEqual(queued_done["event"], "done")

    def test_preprocessor_none_skips_payload(self) -> None:
        """验证预处理返回 None 时会过滤该条数据。"""
        fetcher = ParallelKlineFetcher()
        fetcher.num_processes = 1
        q = py_queue.Queue()
        task = {"code": "600000", "freq": "d", "start_time": "2026-02-01", "end_time": "2026-02-14"}
        raw_payload = {
            "event": "data",
            "task": dict(task),
            "rows": [{"code": "sh.600000"}],
            "error": None,
            "worker_pid": 111,
        }

        with mock.patch.object(fetcher, "_iter_task_payloads_inproc_chunked", return_value=iter([raw_payload])):
            result = fetcher.fetch_stock_tasks_sync(
                tasks=[task],
                queue=q,
                preprocessor_operator=lambda payload: None,
            )

        self.assertEqual(result, [])
        done_event = q.get(timeout=1)
        self.assertEqual(done_event["event"], "done")
        self.assertEqual(done_event["total_tasks"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
