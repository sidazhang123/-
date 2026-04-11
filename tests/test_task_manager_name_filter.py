"""TaskManager 股票名称过滤测试。

用途：
1. 验证 ST 过滤机制会排除名称包含 ST（不分大小写）或包含“退”的股票。
边界：
1. 仅验证名称谓词，不覆盖完整任务执行链路。
"""

from __future__ import annotations

from app.services.task_manager import TaskManager


class TestTaskManagerNameFilter:
    """验证 TaskManager 的名称过滤谓词。"""

    def test_excludes_st_name_case_insensitive(self):
        """名称包含 ST 时应被排除，大小写不敏感。"""
        assert TaskManager._should_exclude_by_name_filter("*ST中珠") is True
        assert TaskManager._should_exclude_by_name_filter("st中珠") is True

    def test_excludes_name_containing_tui(self):
        """名称包含“退”时应被排除。"""
        assert TaskManager._should_exclude_by_name_filter("中退股份") is True
        assert TaskManager._should_exclude_by_name_filter("退市整理") is True

    def test_keeps_normal_name(self):
        """普通名称不应被误排除。"""
        assert TaskManager._should_exclude_by_name_filter("贵州茅台") is False
        assert TaskManager._should_exclude_by_name_filter("东方明珠") is False
        assert TaskManager._should_exclude_by_name_filter("") is False