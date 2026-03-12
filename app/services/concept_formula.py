"""
概念公式解析与展示工具。

职责：
1. 从 group_params 中解析概念预筛选配置。
2. 统一“名称 AND / 理由 OR / 可关闭”的公式语义。
3. 为任务摘要与结果页高亮提供稳定的序列化结构。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _normalize_terms(values: Any) -> tuple[str, ...]:
    """
    输入：
    1. values: 原始词项输入。
    输出：
    1. 去空白、去重后的词项元组。
    用途：
    1. 统一处理字符串列表和单字符串输入。
    边界条件：
    1. 空值、非法值返回空元组。
    """

    if isinstance(values, str):
        raw_items = [values]
    elif isinstance(values, (list, tuple)):
        raw_items = list(values)
    else:
        return ()

    result: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        token = str(item or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        result.append(token)
    return tuple(result)


@dataclass(frozen=True, slots=True)
class ConceptFormula:
    """
    概念预筛选公式。
    """

    enabled: bool = False
    concept_terms: tuple[str, ...] = ()
    reason_terms: tuple[str, ...] = ()

    def is_active(self) -> bool:
        """
        输入：
        1. 无。
        输出：
        1. 是否启用概念预筛选。
        用途：
        1. 统一判断当前任务是否需要执行公式过滤。
        边界条件：
        1. 仅当 enabled=true 且至少有一组条件时返回 True。
        """

        return bool(self.enabled and (self.concept_terms or self.reason_terms))

    def to_dict(self) -> dict[str, Any]:
        """
        输入：
        1. 无。
        输出：
        1. 可序列化字典。
        用途：
        1. 写入任务摘要与结果页接口返回。
        边界条件：
        1. 字段命名固定，前端依赖该结构。
        """

        return {
            "enabled": bool(self.enabled),
            "active": self.is_active(),
            "concept_terms": list(self.concept_terms),
            "reason_terms": list(self.reason_terms),
        }


def parse_concept_formula(group_params: dict[str, Any] | None) -> ConceptFormula:
    """
    输入：
    1. group_params: 任务策略参数。
    输出：
    1. 解析后的概念公式对象。
    用途：
    1. 从框架保留命名空间读取概念预筛选配置。
    边界条件：
    1. 缺失命名空间或字段非法时返回“关闭”的默认公式。
    """

    if not isinstance(group_params, dict):
        return ConceptFormula()
    universe_filters = group_params.get("universe_filters")
    if not isinstance(universe_filters, dict):
        return ConceptFormula()
    concepts = universe_filters.get("concepts")
    if not isinstance(concepts, dict):
        return ConceptFormula()

    enabled = bool(concepts.get("enabled", False))
    concept_terms = _normalize_terms(
        concepts.get("concept_terms")
        or concepts.get("names")
        or concepts.get("concept_names")
    )
    reason_terms = _normalize_terms(
        concepts.get("reason_terms")
        or concepts.get("keywords")
        or concepts.get("reason_keywords")
    )
    return ConceptFormula(enabled=enabled, concept_terms=concept_terms, reason_terms=reason_terms)