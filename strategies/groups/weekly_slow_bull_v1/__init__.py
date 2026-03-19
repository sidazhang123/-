"""
`weekly_slow_bull_v1` 策略组包入口。

说明：
1. 对外暴露统一协议对象 `GROUP_HANDLERS`。
2. 主执行路径位于 `engine.py` 的 specialized 引擎。
3. 运行时由注册中心按 manifest 自动发现并导入。
"""

from .strategy import GROUP_HANDLERS

__all__ = ["GROUP_HANDLERS"]
