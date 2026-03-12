"""
`bigbro_buy` specialized engine tests.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from strategies.groups.bigbro_buy.engine import run_bigbro_buy_specialized


def _build_daily_rows(returns: list[float], *, start_open: float = 100.0) -> list[tuple[datetime, float, float]]:
    start = datetime(2026, 1, 1, 15, 0, 0)
    rows: list[tuple[datetime, float, float]] = []
    for idx, body_return in enumerate(returns):
        open_price = start_open + idx
        close_price = open_price * (1.0 + body_return)
        rows.append((start + timedelta(days=idx), open_price, close_price))
    return rows


class TestBigbroBuySpecializedEngine(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "engine_test.duckdb"
        con = duckdb.connect(str(self.db_path))
        try:
            con.execute(
                """
                create table klines_d (
                    code varchar,
                    datetime timestamp,
                    open double,
                    close double
                )
                """
            )

            pass_rows = _build_daily_rows([0.004, 0.006, 0.007, -0.003, 0.005, 0.004, 0.003, -0.002, 0.004, 0.006])
            fail_rows = _build_daily_rows([0.03, 0.028, 0.029, 0.031, 0.027, 0.03, 0.028, 0.029, 0.031, 0.03])

            for day_ts, open_price, close_price in pass_rows:
                con.execute(
                    "insert into klines_d(code, datetime, open, close) values (?, ?, ?, ?)",
                    ["sh.600000", day_ts, open_price, close_price],
                )
            for day_ts, open_price, close_price in fail_rows:
                con.execute(
                    "insert into klines_d(code, datetime, open, close) values (?, ?, ?, ?)",
                    ["sh.600001", day_ts, open_price, close_price],
                )
        finally:
            con.close()

        self.start_ts = datetime(2026, 1, 1, 0, 0, 0)
        self.end_ts = datetime(2026, 1, 31, 23, 59, 59)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def _default_params() -> dict:
        return {
            "d": {
                "enabled": True,
                "lookback_days": 10,
                "min_streak_days": 4,
                "bull_ratio": 0.8,
                "max_body_drop_pct": 0.01,
                "max_body_rise_pct": 0.025,
                "last_bar_only": True,
            },
            "combo": {
                "emit_signal": True,
            },
            "execution": {
                "fallback_to_backtrader": False,
            },
        }

    def test_specialized_hits_and_returns_single_signal_on_last_bar(self) -> None:
        result_map, metrics = run_bigbro_buy_specialized(
            source_db_path=self.db_path,
            start_ts=self.start_ts,
            end_ts=self.end_ts,
            codes=["sh.600000", "sh.600001"],
            code_to_name={"sh.600000": "A", "sh.600001": "B"},
            group_params=self._default_params(),
            strategy_group_id="bigbro_buy",
            strategy_name="大哥买点 v1",
            cache_scope="none",
        )

        self.assertIn("sh.600000", result_map)
        self.assertNotIn("sh.600001", result_map)

        hit = result_map["sh.600000"]
        self.assertEqual(hit.signal_count, 1)
        self.assertEqual(len(hit.signals), 1)
        payload = hit.signals[0]["payload"]
        self.assertIn("chart_interval_start_ts", payload)
        self.assertIn("chart_interval_end_ts", payload)
        self.assertIn("anchor_day_ts", payload)
        self.assertEqual(payload["combo"]["passed"], True)

        self.assertEqual(metrics["total_codes"], 2)
        self.assertGreater(metrics["total_daily_rows"], 0)
        self.assertGreaterEqual(metrics["candidate_windows"], 2)

    def test_specialized_respects_emit_signal_false(self) -> None:
        group_params = self._default_params()
        group_params["combo"] = {"emit_signal": False}

        result_map, metrics = run_bigbro_buy_specialized(
            source_db_path=self.db_path,
            start_ts=self.start_ts,
            end_ts=self.end_ts,
            codes=["sh.600000"],
            code_to_name={"sh.600000": "A"},
            group_params=group_params,
            strategy_group_id="bigbro_buy",
            strategy_name="大哥买点 v1",
            cache_scope="none",
        )

        self.assertEqual(result_map, {})
        self.assertEqual(metrics["kept_windows"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
