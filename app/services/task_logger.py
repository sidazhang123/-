"""
任务日志服务。

职责：
1. 提供 `info/error` 统一日志入口。
2. 同步写入应用日志与状态库 `task_logs`，并维护任务日志计数。
3. 保障任务执行过程可追溯，便于前端实时展示与问题排查。

说明：
1. 该模块是日志写入适配层，业务层只需关注日志语义。
"""

from __future__ import annotations

import logging
from typing import Any

from app.db.state_db import StateDB


class TaskLogger:
    def __init__(self, task_id: str, state_db: StateDB, logger: logging.Logger):
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        2. state_db: 输入参数，具体约束以调用方和实现为准。
        3. logger: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `__init__` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        self.task_id = task_id
        self.state_db = state_db
        self.logger = logger

    def info(self, message: str, detail: dict[str, Any] | None = None) -> None:
        """
        输入：
        1. message: 输入参数，具体约束以调用方和实现为准。
        2. detail: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `info` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        self.logger.info("[%s] %s", self.task_id, message)
        self.state_db.append_log(task_id=self.task_id, level="info", message=message, detail=detail)

    def error(self, message: str, detail: dict[str, Any] | None = None) -> None:
        """
        输入：
        1. message: 输入参数，具体约束以调用方和实现为准。
        2. detail: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `error` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        self.logger.error("[%s] %s", self.task_id, message)
        self.state_db.append_log(task_id=self.task_id, level="error", message=message, detail=detail)
