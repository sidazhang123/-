"""
数据库维护日历工具。

职责：
1. 提供维护任务高频复用的日期计算函数。
2. 统一处理 date/datetime/ISO 字符串输入。
3. 避免 pandas 依赖，保证低开销与可预测性能。
"""

from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache
from typing import Iterable


@lru_cache(maxsize=8192)
def _parse_date_text(value: str) -> date:
    """
    输入：
    1. value: 日期字符串，支持 YYYY-MM-DD / YYYY-MM-DD HH:MM:SS / ISO8601。
    输出：
    1. 解析后的 date 对象。
    用途：
    1. 为字符串输入提供缓存解析，降低重复解析开销。
    边界条件：
    1. 非法字符串会抛 ValueError。
    """

    token = str(value or "").strip()
    if not token:
        raise ValueError("日期字符串不能为空")
    if token.endswith("Z"):
        token = token[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(token).date()
    except ValueError:
        return date.fromisoformat(token)


def normalize_date(value: date | datetime | str) -> date:
    """
    输入：
    1. value: date/datetime/ISO 字符串。
    输出：
    1. 标准化后的 date。
    用途：
    1. 统一维护流程中所有日期入参口径。
    边界条件：
    1. 空字符串或不支持的类型会抛 ValueError。
    """

    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return _parse_date_text(value)
    raise ValueError(f"不支持的日期类型: {type(value).__name__}")


def week_monday_and_friday(value: date | datetime | str) -> tuple[date, date]:
    """
    输入：
    1. value: 任意日期输入。
    输出：
    1. (周一日期, 周五日期)。
    用途：
    1. 维护最新数据更新时定位自然周窗口。
    边界条件：
    1. 周定义固定为周一到周日。
    """

    current = normalize_date(value)
    weekday = current.weekday()
    monday = current.fromordinal(current.toordinal() - weekday)
    friday = monday.fromordinal(monday.toordinal() + 4)
    return monday, friday


def iter_weekdays_between_exclusive(
    start_value: date | datetime | str,
    end_value: date | datetime | str,
) -> Iterable[date]:
    """
    输入：
    1. start_value: 区间起点（排除）。
    2. end_value: 区间终点（排除）。
    输出：
    1. 迭代返回区间内的工作日日期（周一到周五）。
    用途：
    1. 历史维护中识别 d 周期缺失日期。
    边界条件：
    1. 支持逆序输入，会自动按时间先后处理。
    2. 周六周日会被剔除。
    """

    start_day = normalize_date(start_value)
    end_day = normalize_date(end_value)
    if start_day > end_day:
        start_day, end_day = end_day, start_day

    start_ordinal = start_day.toordinal() + 1
    end_ordinal = end_day.toordinal() - 1
    if start_ordinal > end_ordinal:
        return

    weekday = date.fromordinal(start_ordinal).weekday()
    for ordinal in range(start_ordinal, end_ordinal + 1):
        if weekday < 5:
            yield date.fromordinal(ordinal)
        weekday = 0 if weekday == 6 else weekday + 1


def weekdays_between_exclusive(
    start_value: date | datetime | str,
    end_value: date | datetime | str,
) -> list[date]:
    """
    输入：
    1. start_value: 区间起点（排除）。
    2. end_value: 区间终点（排除）。
    输出：
    1. 区间内所有工作日日期列表。
    用途：
    1. 供维护流程直接构建缺口任务列表。
    边界条件：
    1. 返回顺序固定为升序。
    """

    return list(iter_weekdays_between_exclusive(start_value, end_value))
