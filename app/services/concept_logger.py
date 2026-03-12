"""
概念任务日志服务。

职责：
1. 统一概念更新任务摘要日志写入（app.log + concept_logs）。
2. 支持按任务运行时开关控制 debug 明细落库。
3. 保持概念任务与维护任务日志格式一致，便于前端复用。
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.db.state_db import StateDB


def compact_concept_detail_for_app_log(message: str, detail: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    输入：
    1. message: 日志消息。
    2. detail: 原始结构化明细。
    输出：
    1. app.log 可读的摘要 detail。
    用途：
    1. 在主日志中保留关键统计，避免大对象刷屏。
    边界条件：
    1. 非字典或空明细返回 None。
    """

    _ = message
    if not isinstance(detail, dict) or not detail:
        return None
    compact = dict(detail)
    for noisy_key in ("records", "record_sample", "all_codes", "codes"):
        compact.pop(noisy_key, None)
    return compact or None


class ConceptLogger:
    """
    概念任务日志写入器。
    """

    def __init__(
        self,
        job_id: str,
        state_db: StateDB,
        logger: logging.Logger,
        *,
        debug_enabled: bool = True,
    ) -> None:
        """
        输入：
        1. job_id: 概念任务 ID。
        2. state_db: 状态库实例。
        3. logger: 应用日志器。
        4. debug_enabled: 是否写入 debug 明细。
        输出：
        1. 无返回值。
        用途：
        1. 绑定概念任务日志上下文。
        边界条件：
        1. job_id 为空时上层应避免创建对象。
        """

        self.job_id = str(job_id)
        self.state_db = state_db
        self.logger = logger
        self.debug_enabled = bool(debug_enabled)

    def _format_text(self, message: str, detail: dict[str, Any] | None) -> str:
        """
        输入：
        1. message: 日志消息。
        2. detail: 结构化明细。
        输出：
        1. app.log 展示文本。
        用途：
        1. 提供统一日志输出格式。
        边界条件：
        1. detail 不可序列化时回退字符串。
        """

        if not detail:
            return str(message)
        try:
            return f"{message} | {json.dumps(detail, ensure_ascii=False, default=str)}"
        except Exception:
            return f"{message} | {detail}"

    def _append_db_log(self, *, level: str, message: str, detail: dict[str, Any] | None) -> None:
        """
        输入：
        1. level: 日志级别。
        2. message: 日志消息。
        3. detail: 结构化明细。
        输出：
        1. 无返回值。
        用途：
        1. 向 concept_logs 写入结构化日志。
        边界条件：
        1. 由 StateDB 负责落库失败异常传播。
        """

        self.state_db.append_concept_log(
            job_id=self.job_id,
            level=level,
            message=message,
            detail=detail,
        )

    def info(self, message: str, detail: dict[str, Any] | None = None) -> None:
        """
        输入：
        1. message: 信息日志消息。
        2. detail: 可选结构化明细。
        输出：
        1. 无返回值。
        用途：
        1. 写入概念摘要信息日志。
        边界条件：
        1. detail 会先裁剪再写入 app.log 与 info 级落库。
        """

        compact = compact_concept_detail_for_app_log(message, detail)
        self.logger.info("[concept:%s] %s", self.job_id, self._format_text(message, compact))
        self._append_db_log(level="info", message=message, detail=compact)

    def warning(self, message: str, detail: dict[str, Any] | None = None) -> None:
        """
        输入：
        1. message: 警告日志消息。
        2. detail: 可选结构化明细。
        输出：
        1. 无返回值。
        用途：
        1. 写入概念警告日志。
        边界条件：
        1. 前端主日志流按 info 级读取，因此 warning 落库为 info 前缀消息。
        """

        compact = compact_concept_detail_for_app_log(message, detail)
        self.logger.warning("[concept:%s] %s", self.job_id, self._format_text(message, compact))
        self._append_db_log(level="info", message=f"[warning] {message}", detail=compact)

    def error(self, message: str, detail: dict[str, Any] | None = None) -> None:
        """
        输入：
        1. message: 错误日志消息。
        2. detail: 可选结构化明细。
        输出：
        1. 无返回值。
        用途：
        1. 写入概念错误日志。
        边界条件：
        1. detail 会写入 error 日志供前端错误面板查询。
        """

        compact = compact_concept_detail_for_app_log(message, detail)
        self.logger.error("[concept:%s] %s", self.job_id, self._format_text(message, compact))
        self._append_db_log(level="error", message=message, detail=compact)

    def debug(self, message: str, detail: dict[str, Any] | None = None) -> None:
        """
        输入：
        1. message: debug 日志消息。
        2. detail: 可选结构化明细。
        输出：
        1. 无返回值。
        用途：
        1. 在 debug 模式下写入 concept_logs.debug。
        边界条件：
        1. debug 关闭时直接忽略，不写 app.log。
        """

        if not self.debug_enabled:
            return
        if not isinstance(detail, dict):
            detail = None
        self._append_db_log(level="debug", message=message, detail=detail)