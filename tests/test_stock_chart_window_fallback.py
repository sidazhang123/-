"""
stock-chart 窗口提取回退测试。

职责：
1. 验证旧版 flag_rally_v1 payload 没有 window_* 字段时，API 会回退到最后一个形态窗口。
2. 锁定图表缩放使用的 chart/window 区间不再错误覆盖整段历史。
"""

from __future__ import annotations

import unittest
from datetime import datetime

from app.api.routes import _extract_signal_window


class TestStockChartWindowFallback(unittest.TestCase):
    """stock-chart 命中窗口回退测试。"""

    def test_extract_signal_window_prefers_latest_pattern_when_window_missing(self) -> None:
        """旧版 flag_rally_v1 结果应回退到最后一个 patterns 窗口。"""

        payload = {
            "chart_interval_start_ts": "2026-02-10T10:15:00",
            "chart_interval_end_ts": "2026-03-10T10:30:00",
            "patterns": [
                {
                    "bull_start_ts": "2026-02-10T10:15:00",
                    "bull_end_ts": "2026-02-10T11:00:00",
                    "consolidation_start_ts": "2026-02-10T11:15:00",
                    "consolidation_end_ts": "2026-02-10T12:00:00",
                },
                {
                    "bull_start_ts": "2026-03-10T09:30:00",
                    "bull_end_ts": "2026-03-10T10:00:00",
                    "consolidation_start_ts": "2026-03-10T10:15:00",
                    "consolidation_end_ts": "2026-03-10T10:30:00",
                },
            ],
        }

        window = _extract_signal_window(payload)

        self.assertEqual(window["window_start_dt"], datetime.fromisoformat("2026-03-10T09:30:00"))
        self.assertEqual(window["window_end_dt"], datetime.fromisoformat("2026-03-10T10:30:00"))
        self.assertEqual(window["chart_start_dt"], datetime.fromisoformat("2026-03-10T09:30:00"))
        self.assertEqual(window["chart_end_dt"], datetime.fromisoformat("2026-03-10T10:30:00"))
        self.assertEqual(window["anchor_day_dt"], datetime.fromisoformat("2026-03-10T10:30:00"))


if __name__ == "__main__":
    unittest.main(verbosity=2)