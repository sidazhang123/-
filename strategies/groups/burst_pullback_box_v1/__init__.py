"""
`burst_pullback_box_v1` 策略组包入口。

说明：
1. 对外暴露 `GROUP_HANDLERS` 以满足统一策略协议。
2. 该策略默认走 specialized 引擎；handlers 主要承担注册与回退协商。
3. 复杂计算逻辑见 `engine.py`。
"""

from .strategy import GROUP_HANDLERS

__all__ = ["GROUP_HANDLERS"]
