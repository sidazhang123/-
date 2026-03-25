"""
`converging_triangle_v1` 策略组包入口。

说明：
1. 对外暴露统一协议对象 `GROUP_HANDLERS`。
2. 大收敛三角策略：识别日线/周线级别大三角震荡收敛末端形态。
3. 运行时由注册中心按 manifest 自动导入。
"""

from .strategy import GROUP_HANDLERS

__all__ = ["GROUP_HANDLERS"]
