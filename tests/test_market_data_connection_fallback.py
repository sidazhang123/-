"""
MarketDataDB 连接回退测试。

职责：
1. 验证连接配置冲突时，会回退到与任务一致的读写连接。
2. 验证源库文件被占用时，会回退到 shadow copy 只读连接。
"""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

import duckdb

from app.db.market_data import MarketDataDB


class TestMarketDataConnectionFallback(unittest.TestCase):
    """MarketDataDB 连接容错测试。"""

    def test_connect_fallback_to_read_write_on_config_conflict(self) -> None:
        """后台已持有读写连接时，前台读请求应自动回退到兼容配置。"""

        db = MarketDataDB("D:/quant.duckdb")
        writable_conn = object()

        with mock.patch(
            "app.db.duckdb_utils.duckdb.connect",
            side_effect=[
                duckdb.ConnectionException(
                    "Connection Error: Can't open a connection to same database file with a different configuration than existing connections"
                ),
                writable_conn,
            ],
        ) as mocked_connect:
            result = db._connect()

        self.assertIs(result, writable_conn)
        self.assertEqual(mocked_connect.call_count, 2)
        self.assertEqual(mocked_connect.call_args_list[0].kwargs["read_only"], True)
        self.assertEqual(mocked_connect.call_args_list[1].kwargs["read_only"], False)

    def test_connect_fallback_to_shadow_when_source_locked(self) -> None:
        """源库文件被其他程序占用时，应切到 shadow copy。"""

        db = MarketDataDB("D:/quant.duckdb")
        shadow_path = Path("C:/tmp/quant_shadow.duckdb")
        shadow_conn = object()

        with mock.patch(
            "app.db.duckdb_utils.duckdb.connect",
            side_effect=[
                duckdb.IOException('IO Error: Cannot open file "d:/quant.duckdb": 另一个程序正在使用此文件，进程无法访问。'),
                shadow_conn,
            ],
        ) as mocked_connect:
            with mock.patch.object(db, "_ensure_shadow_copy", return_value=shadow_path) as mocked_shadow:
                result = db._connect()

        self.assertIs(result, shadow_conn)
        mocked_shadow.assert_called_once_with()
        self.assertEqual(mocked_connect.call_args_list[1].args[0], str(shadow_path))
        self.assertEqual(mocked_connect.call_args_list[1].kwargs["read_only"], True)


if __name__ == "__main__":
    unittest.main(verbosity=2)