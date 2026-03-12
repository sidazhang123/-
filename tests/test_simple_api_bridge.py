"""
simple_api 工程包装层测试。

职责：
1. 验证包装层会在 job 消费结束后主动销毁并行抓取器。
2. 验证 job 消费抛异常时仍会执行销毁。
"""

from __future__ import annotations

import unittest
from unittest import mock

from app.services import simple_api_bridge as bridge


class TestSimpleApiBridge(unittest.TestCase):
    def test_managed_stock_kline_job_destroys_fetcher_after_success(self) -> None:
        fake_job = object()

        with (
            mock.patch.object(bridge, "get_stock_kline", return_value=fake_job) as mock_get_stock_kline,
            mock.patch.object(bridge, "destroy_parallel_fetcher_safely", return_value={"destroyed": True}) as mock_destroy,
        ):
            with bridge.managed_stock_kline_job(
                task=[{"code": "600000", "freq": "d", "start_time": "2026-02-01", "end_time": "2026-02-14"}],
                mode="async",
            ) as job:
                self.assertIs(job, fake_job)

        self.assertEqual(mock_get_stock_kline.call_count, 1)
        self.assertEqual(mock_destroy.call_count, 1)

    def test_managed_stock_kline_job_destroys_fetcher_after_error(self) -> None:
        fake_job = object()

        with (
            mock.patch.object(bridge, "get_stock_kline", return_value=fake_job),
            mock.patch.object(bridge, "destroy_parallel_fetcher_safely", return_value={"destroyed": True}) as mock_destroy,
        ):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                with bridge.managed_stock_kline_job(
                    task=[{"code": "600000", "freq": "d", "start_time": "2026-02-01", "end_time": "2026-02-14"}],
                    mode="async",
                ):
                    raise RuntimeError("boom")

        self.assertEqual(mock_destroy.call_count, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)