"""
维护日历与代码归一测试。

职责：
1. 验证日历函数 A/B 的边界与逆序输入处理。
2. 验证代码归一规则（前缀、6位、5位 hk）。
"""

from __future__ import annotations

import time
import unittest
from datetime import date

from app.services.maintenance_calendar import weekdays_between_exclusive, week_monday_and_friday
from app.services.maintenance_codecs import normalize_code_for_compare, normalize_code_for_storage


class TestMaintenanceCalendarAndCodecs(unittest.TestCase):
    """维护基础工具测试集合。"""

    def test_week_monday_and_friday_on_weekend(self) -> None:
        """
        输入：
        1. 周末日期。
        输出：
        1. 对应自然周周一与周五。
        用途：
        1. 验证函数 A 的自然周口径。
        边界条件：
        1. 周日应回溯到当周周一。
        """

        monday, friday = week_monday_and_friday(date(2026, 3, 1))  # Sunday
        self.assertEqual(monday, date(2026, 2, 23))
        self.assertEqual(friday, date(2026, 2, 27))

    def test_weekdays_between_exclusive_filters_weekend(self) -> None:
        """
        输入：
        1. 跨周区间。
        输出：
        1. 仅工作日日期列表。
        用途：
        1. 验证函数 B 排除端点和周末。
        边界条件：
        1. 支持逆序输入。
        """

        forward = weekdays_between_exclusive("2026-02-27", "2026-03-04")
        backward = weekdays_between_exclusive("2026-03-04", "2026-02-27")
        expect = [date(2026, 3, 2), date(2026, 3, 3)]
        self.assertEqual(forward, expect)
        self.assertEqual(backward, expect)

    def test_calendar_performance_smoke(self) -> None:
        """
        输入：
        1. 2 万次高频调用。
        输出：
        1. 在秒级内完成。
        用途：
        1. 防止日历函数引入明显性能回退。
        边界条件：
        1. 阈值设置宽松，避免环境抖动误报。
        """

        start = time.perf_counter()
        for _ in range(20_000):
            _ = week_monday_and_friday("2026-03-03")
            _ = weekdays_between_exclusive("2026-01-01", "2026-01-31")
        elapsed = time.perf_counter() - start
        self.assertLess(elapsed, 1.5, f"日历函数性能回退: {elapsed:.3f}s")

    def test_code_normalization_rules(self) -> None:
        """
        输入：
        1. 前缀代码、6位数字、5位数字、非法代码。
        输出：
        1. 归一化后的比较/存储字符串。
        用途：
        1. 覆盖代码归一核心规则。
        边界条件：
        1. 非法输入应返回空字符串。
        """

        self.assertEqual(normalize_code_for_compare("sh.600000"), "sh.600000")
        self.assertEqual(normalize_code_for_compare("600000"), "sh.600000")
        self.assertEqual(normalize_code_for_compare("000001"), "sz.000001")
        self.assertEqual(normalize_code_for_compare("300001"), "sz.300001")
        self.assertEqual(normalize_code_for_compare("900001"), "bj.900001")
        self.assertEqual(normalize_code_for_compare("00700"), "hk.00700")
        self.assertEqual(normalize_code_for_storage("HK.00700"), "hk.00700")
        self.assertEqual(normalize_code_for_storage("bad-code"), "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
