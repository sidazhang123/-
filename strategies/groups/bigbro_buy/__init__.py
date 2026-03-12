"""
`bigbro_buy` 策略组包入口。

说明：
1. 对外暴露统一协议对象 `GROUP_HANDLERS`。
2. 运行时由注册中心按 manifest 自动导入。
"""

from .strategy import GROUP_HANDLERS

__all__ = ["GROUP_HANDLERS"]
