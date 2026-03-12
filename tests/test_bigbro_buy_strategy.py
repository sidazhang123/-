"""
`bigbro_buy` 策略规则测试。

职责：
1. 覆盖命中/不命中/边界场景。
2. 覆盖末日判定与组合层 emit_signal 开关。
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from typing import Any

from strategies.groups.bigbro_buy import strategy as bigbro_buy


class _FakeDailyData:
    def __init__(self, current_len: int, total_len: int):
        self._current_len = current_len
        self._total_len = total_len

    def __len__(self) -> int:
        return self._current_len

    def buflen(self) -> int:
        return self._total_len


class _FakeStrategy:
    def __init__(self, bars: list[dict[str, Any]], *, current_len: int | None = None, total_len: int | None = None):
        self._bars = bars
        now_len = len(bars) if current_len is None else current_len
        all_len = len(bars) if total_len is None else total_len
        self.data_by_tf = {"d": _FakeDailyData(now_len, all_len)}

    def tf_has_bars(self, tf: str, bars: int) -> bool:
        if tf != "d":
            return False
        return len(self._bars) >= bars

    def tf_line(self, tf: str, line_name: str, ago: int = 0) -> float | None:
        if tf != "d":
            return None
        idx = len(self._bars) - 1 - ago
        if idx < 0:
            return None
        value = self._bars[idx].get(line_name)
        return float(value) if value is not None else None

    def tf_datetime(self, tf: str, ago: int = 0) -> datetime | None:
        if tf != "d":
            return None
        idx = len(self._bars) - 1 - ago
        if idx < 0:
            return None
        value = self._bars[idx].get("day_ts")
        return value if isinstance(value, datetime) else None


def _build_bars(body_returns: list[float]) -> list[dict[str, Any]]:
    start = datetime(2026, 1, 1, 15, 0, 0)
    bars: list[dict[str, Any]] = []
    for index, body_return in enumerate(body_returns):
        open_price = 100.0 + index
        close_price = open_price * (1.0 + body_return)
        bars.append(
            {
                "day_ts": start + timedelta(days=index),
                "open": open_price,
                "close": close_price,
            }
        )
    return bars


class TestBigbroBuyStrategy(unittest.TestCase):
    def _default_group_params(self) -> dict[str, Any]:
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
        }

    def test_eval_d_passes_and_emits_window_fields(self) -> None:
        bars = _build_bars([0.004, 0.006, 0.007, -0.003, 0.005, 0.004, 0.003, -0.002, 0.004, 0.006])
        strategy = _FakeStrategy(bars)
        result = bigbro_buy.eval_d(strategy, self._default_group_params())

        self.assertTrue(result["ready"])
        self.assertTrue(result["passed"])
        values = result["values"]
        self.assertIn("chart_interval_start_ts", values)
        self.assertIn("chart_interval_end_ts", values)
        self.assertIn("anchor_day_ts", values)
        self.assertLess(values["max_body_rise"], 0.025)
        self.assertGreater(values["max_body_drop"], -0.01)

    def test_eval_d_fails_when_body_rise_exceeds_limit(self) -> None:
        bars = _build_bars([0.03, 0.028, 0.029, 0.031, 0.027, 0.03, 0.028, 0.029, 0.031, 0.03])
        strategy = _FakeStrategy(bars)
        result = bigbro_buy.eval_d(strategy, self._default_group_params())

        self.assertTrue(result["ready"])
        self.assertFalse(result["passed"])

    def test_eval_d_not_ready_when_bars_insufficient(self) -> None:
        bars = _build_bars([0.004, 0.006, 0.007, -0.003, 0.005, 0.004, 0.003, -0.002, 0.004])
        strategy = _FakeStrategy(bars)
        result = bigbro_buy.eval_d(strategy, self._default_group_params())

        self.assertFalse(result["ready"])
        self.assertFalse(result["passed"])

    def test_eval_d_last_bar_only_blocks_non_last_bar(self) -> None:
        bars = _build_bars([0.004, 0.006, 0.007, -0.003, 0.005, 0.004, 0.003, -0.002, 0.004, 0.006])
        strategy = _FakeStrategy(bars, current_len=9, total_len=10)
        result = bigbro_buy.eval_d(strategy, self._default_group_params())

        self.assertTrue(result["ready"])
        self.assertFalse(result["passed"])
        self.assertEqual(result["values"].get("is_last_daily_bar"), False)

    def test_eval_combo_respects_emit_signal_flag(self) -> None:
        bars = _build_bars([0.004, 0.006, 0.007, -0.003, 0.005, 0.004, 0.003, -0.002, 0.004, 0.006])
        strategy = _FakeStrategy(bars)
        d_result = bigbro_buy.eval_d(strategy, self._default_group_params())

        per_rule = {
            "w": bigbro_buy.eval_w(strategy, self._default_group_params()),
            "d": d_result,
            "60": bigbro_buy.eval_60(strategy, self._default_group_params()),
            "30": bigbro_buy.eval_30(strategy, self._default_group_params()),
            "15": bigbro_buy.eval_15(strategy, self._default_group_params()),
        }
        combo = bigbro_buy.eval_combo(
            strategy,
            per_rule,
            {
                **self._default_group_params(),
                "combo": {"emit_signal": False},
            },
        )

        self.assertTrue(combo["ready"])
        self.assertFalse(combo["passed"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
