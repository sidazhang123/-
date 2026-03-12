"""
`xianren_zhilu_v1` 策略组包入口。

仙人指路形态策略：
1. 最新日线收上影线
2. 指路当日放量
3. 前N天股价下降
"""
from .strategy import GROUP_HANDLERS

__all__ = ["GROUP_HANDLERS"]
