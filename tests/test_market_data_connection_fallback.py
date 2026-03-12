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
import pandas as pd

from app.db.market_data import MarketDataDB


class TestMarketDataConnectionFallback(unittest.TestCase):
    """MarketDataDB 连接容错测试。"""

    def test_open_read_session_reuses_single_connection_for_multiple_reads(self) -> None:
        """任务级只读会话应在多次读取间复用同一条连接。"""

        db = MarketDataDB("D:/quant.duckdb")
        mock_conn = mock.MagicMock()
        stock_cursor = mock.MagicMock()
        stock_cursor.fetchall.return_value = [("000001", "平安银行"), ("000002", "万科A")]
        kline_cursor = mock.MagicMock()
        kline_cursor.fetchdf.return_value = pd.DataFrame(
            [{
                "datetime": "2026-03-10 15:00:00",
                "open": 10.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.2,
                "volume": 1000.0,
                "amount": 10000.0,
            }]
        )
        mock_conn.execute.side_effect = [stock_cursor, stock_cursor, kline_cursor]

        with mock.patch.object(db, "_connect", return_value=mock_conn) as mocked_connect:
            with db.open_read_session() as session:
                all_stocks = session.get_all_stocks()
                resolution = session.resolve_stock_inputs(["平安银行"])
                df = session.fetch_ohlcv(
                    code="000001",
                    timeframe="d",
                    start_ts=None,
                    end_ts=None,
                )

        mocked_connect.assert_called_once_with()
        mock_conn.close.assert_called_once_with()
        self.assertEqual(all_stocks, [("000001", "平安银行"), ("000002", "万科A")])
        self.assertEqual(resolution.codes, ["000001"])
        self.assertFalse(df.empty)

    def test_resolve_stock_inputs_reuses_prefetched_rows(self) -> None:
        """已传入股票快照时，不应再次创建数据库连接。"""

        db = MarketDataDB("D:/quant.duckdb")
        stock_rows = [
            ("000001", "平安银行"),
            ("000002", "万科A"),
            ("600000", "浦发银行"),
        ]

        with mock.patch.object(db, "_connect") as mocked_connect:
            resolution = db.resolve_stock_inputs(["平安银行", "600000", "未知"], stock_rows=stock_rows)

        mocked_connect.assert_not_called()
        self.assertEqual(resolution.codes, ["000001", "600000"])
        self.assertEqual(resolution.unresolved, ["未知"])
        self.assertEqual(resolution.code_to_name["000002"], "万科A")

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