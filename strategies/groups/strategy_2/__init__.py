"""
`strategy_2` 策略组包入口。

说明：
1. 对外暴露统一协议对象 `GROUP_HANDLERS`。
2. 当前为模板策略，用于后续新策略快速复制扩展。
3. 运行时由注册中心按 manifest 自动导入。
"""

from .strategy import GROUP_HANDLERS

__all__ = ["GROUP_HANDLERS"]
