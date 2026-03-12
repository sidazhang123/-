"""
`bu_zhi_dao_v1` 策略组包入口。

说明：
1. 对外暴露 `GROUP_HANDLERS` 以满足统一策略协议。
2. 该策略主路径为 specialized 引擎，handlers 作为协议占位/回退语义。
3. 具体筛选逻辑见 `engine.py`。
"""

from .strategy import GROUP_HANDLERS

__all__ = ["GROUP_HANDLERS"]
