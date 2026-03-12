"""
数据库维护代码归一工具。

职责：
1. 统一股票代码比较口径，避免字符串直比误判。
2. 提供落库格式归一，保证与现有数据库格式一致。
3. 兼容显式市场前缀与纯数字代码输入。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


_ALLOWED_PREFIXES = ("sh", "sz", "bj", "hk")


@dataclass(frozen=True, slots=True)
class NormalizedCode:
    """
    输入：
    1. prefix: 市场前缀（sh/sz/bj/hk）。
    2. digits: 证券数字部分。
    输出：
    1. 不可变归一代码对象。
    用途：
    1. 为比较与序列化提供统一结构。
    边界条件：
    1. 仅表示合法股票代码。
    """

    prefix: str
    digits: str

    def to_storage(self) -> str:
        """
        输入：
        1. 无。
        输出：
        1. `prefix.digits` 存储格式字符串。
        用途：
        1. 统一数据库落库代码格式。
        边界条件：
        1. 输出始终小写。
        """

        return f"{self.prefix}.{self.digits}"


def _normalize_from_prefixed(token: str) -> NormalizedCode | None:
    """
    输入：
    1. token: 可能带市场前缀的代码字符串。
    输出：
    1. 合法时返回 NormalizedCode，否则返回 None。
    用途：
    1. 处理显式前缀场景。
    边界条件：
    1. 仅接受 sh./sz./bj./hk. 前缀。
    """

    if "." not in token:
        return None
    prefix_raw, digits_raw = token.split(".", 1)
    prefix = prefix_raw.strip().lower()
    digits = digits_raw.strip()
    if prefix not in _ALLOWED_PREFIXES:
        return None
    if not digits.isdigit():
        return None
    if prefix == "hk" and len(digits) != 5:
        return None
    if prefix in {"sh", "sz", "bj"} and len(digits) != 6:
        return None
    return NormalizedCode(prefix=prefix, digits=digits)


def _normalize_from_digits(token: str) -> NormalizedCode | None:
    """
    输入：
    1. token: 纯数字代码字符串。
    输出：
    1. 合法时返回 NormalizedCode，否则返回 None。
    用途：
    1. 处理无前缀输入场景。
    边界条件：
    1. 6 位按 A 股映射，5 位映射 hk。
    """

    if not token.isdigit():
        return None
    if len(token) == 5:
        return NormalizedCode(prefix="hk", digits=token)
    if len(token) != 6:
        return None
    head = token[0]
    if head == "6":
        return NormalizedCode(prefix="sh", digits=token)
    if head in {"0", "3"}:
        return NormalizedCode(prefix="sz", digits=token)
    if head == "9":
        return NormalizedCode(prefix="bj", digits=token)
    return None


def parse_normalized_code(raw_code: object) -> NormalizedCode | None:
    """
    输入：
    1. raw_code: 任意原始代码值。
    输出：
    1. 合法时返回归一对象，否则返回 None。
    用途：
    1. 作为所有代码归一函数的底层入口。
    边界条件：
    1. None、空字符串与非法格式返回 None。
    """

    token = str(raw_code or "").strip().lower()
    if not token:
        return None
    prefixed = _normalize_from_prefixed(token)
    if prefixed is not None:
        return prefixed
    return _normalize_from_digits(token)


def normalize_code_for_compare(raw_code: object) -> str:
    """
    输入：
    1. raw_code: 原始代码。
    输出：
    1. 可比较归一字符串（`prefix.digits`）；非法输入返回空字符串。
    用途：
    1. 集合差集与去重比较统一使用该口径。
    边界条件：
    1. 非法代码不会抛错，返回空串。
    """

    normalized = parse_normalized_code(raw_code)
    return normalized.to_storage() if normalized else ""


def normalize_code_for_storage(raw_code: object) -> str:
    """
    输入：
    1. raw_code: 原始代码。
    输出：
    1. 标准数据库落库存储字符串；非法输入返回空字符串。
    用途：
    1. 保证维护落库代码格式一致。
    边界条件：
    1. 本函数与比较口径保持同源规则。
    """

    return normalize_code_for_compare(raw_code)


def normalize_code_set(values: Iterable[object]) -> set[str]:
    """
    输入：
    1. values: 代码集合。
    输出：
    1. 归一后去重集合（自动剔除非法代码）。
    用途：
    1. 快速执行代码集合运算。
    边界条件：
    1. 空输入返回空集合。
    """

    result: set[str] = set()
    for raw in values:
        normalized = normalize_code_for_compare(raw)
        if normalized:
            result.add(normalized)
    return result
