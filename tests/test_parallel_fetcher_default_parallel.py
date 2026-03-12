"""
parallel_fetcher 默认并行入口测试。

职责：
1. 验证进程可用时 fetch_stock 默认走并行路径。
2. 验证进程不足时走串行兜底。
3. 验证入口日志不再出现“阈值”判定文案。
边界：
1. 通过 mock 替换 _fetch_parallel/_fetch_serial，避免真实网络与进程开销。
2. 仅验证入口分支与日志，不覆盖并行执行细节。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE_ROOT))

from zsdtdx import parallel_fetcher as pf


class TestParallelFetcherDefaultParallel(unittest.TestCase):
    """默认并行入口测试集合。"""

    def setUp(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 记录并行日志细节配置，避免用例间互相污染。
        边界条件：
        1. set_log_detail_options 返回旧值，tearDown 负责恢复。
        """

        self._previous_log_options = pf.set_log_detail_options()

    def tearDown(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 恢复并行日志细节配置，保持测试隔离。
        边界条件：
        1. 恢复异常不应影响其他断言。
        """

        try:
            pf.set_log_detail_options(**self._previous_log_options)
        except Exception:
            pass

    def test_fetch_stock_uses_parallel_when_process_available(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证 num_processes>1 时默认调用并行分支。
        边界条件：
        1. 少量任务也应走并行，不再依赖任务阈值。
        """

        fetcher = pf.ParallelKlineFetcher()
        fetcher.num_processes = 12
        expected_df = pd.DataFrame({"code": ["sh.600000"]})

        with (
            mock.patch.object(fetcher, "_fetch_parallel", return_value=expected_df) as mock_parallel,
            mock.patch.object(fetcher, "_fetch_serial", return_value=pd.DataFrame()) as mock_serial,
            mock.patch.object(pf, "_emit_log") as mock_emit_log,
        ):
            result = fetcher.fetch_stock(
                codes=["sh.600000"],
                freqs=["d"],
                start_time="2026-02-17 00:00:00",
                end_time="2026-02-18 00:00:00",
            )

        self.assertIs(result, expected_df)
        mock_parallel.assert_called_once()
        mock_serial.assert_not_called()
        messages = [str(call.args[1]) for call in mock_emit_log.call_args_list if len(call.args) >= 2]
        self.assertTrue(any("默认并行模式" in text for text in messages))
        self.assertFalse(any("阈值" in text for text in messages))

    def test_fetch_stock_uses_serial_when_process_not_available(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证 num_processes<=1 时走串行兜底分支。
        边界条件：
        1. 仅验证入口分支，不覆盖串行执行内部细节。
        """

        fetcher = pf.ParallelKlineFetcher()
        fetcher.num_processes = 1
        expected_df = pd.DataFrame({"code": ["sh.600001"]})

        with (
            mock.patch.object(fetcher, "_fetch_parallel", return_value=pd.DataFrame()) as mock_parallel,
            mock.patch.object(fetcher, "_fetch_serial", return_value=expected_df) as mock_serial,
            mock.patch.object(pf, "_emit_log") as mock_emit_log,
        ):
            result = fetcher.fetch_stock(
                codes=["sh.600001"],
                freqs=["60"],
                start_time="2026-02-17 00:00:00",
                end_time="2026-02-18 00:00:00",
            )

        self.assertIs(result, expected_df)
        mock_parallel.assert_not_called()
        mock_serial.assert_called_once()
        messages = [str(call.args[1]) for call in mock_emit_log.call_args_list if len(call.args) >= 2]
        self.assertTrue(any("进程数不足，使用串行模式" in text for text in messages))

    def test_chunk_log_detail_compact_by_default(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证默认日志细节仅保留计数与 task_sample，不输出全量 tasks。
        边界条件：
        1. sample_size 应限制 task_sample 条数。
        """

        pf.set_log_detail_options(compact_enabled=True, sample_size=2, include_full_tasks_debug=False)
        fetcher = pf.ParallelKlineFetcher()
        task_detail = [
            {"code": "sh.600000", "freq": "d", "start_time": "2026-02-17 00:00:00", "end_time": "2026-02-18 00:00:00"},
            {"code": "sz.000001", "freq": "60", "start_time": "2026-02-17 00:00:00", "end_time": "2026-02-18 00:00:00"},
            {"code": "sz.000002", "freq": "15", "start_time": "2026-02-17 00:00:00", "end_time": "2026-02-18 00:00:00"},
        ]

        payload = fetcher._build_chunk_task_detail(task_detail)

        self.assertEqual(payload["task_total_count"], 3)
        self.assertEqual(len(payload["task_sample"]), 2)
        self.assertNotIn("tasks", payload)

    def test_chunk_log_detail_restores_full_tasks_in_debug_mode(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 验证 debug 开关开启时 chunk 日志恢复全量 tasks。
        边界条件：
        1. 同时保留 task_sample 字段，兼容前端进度提取。
        """

        pf.set_log_detail_options(compact_enabled=True, sample_size=1, include_full_tasks_debug=True)
        fetcher = pf.ParallelKlineFetcher()
        task_detail = [
            {"code": "sh.600000", "freq": "d", "start_time": "2026-02-17 00:00:00", "end_time": "2026-02-18 00:00:00"},
            {"code": "sz.000001", "freq": "60", "start_time": "2026-02-17 00:00:00", "end_time": "2026-02-18 00:00:00"},
        ]

        payload = fetcher._build_chunk_task_detail(task_detail)

        self.assertEqual(payload["task_total_count"], 2)
        self.assertEqual(len(payload["task_sample"]), 1)
        self.assertIn("tasks", payload)
        self.assertEqual(payload["tasks"], task_detail)


if __name__ == "__main__":
    unittest.main(verbosity=2)
