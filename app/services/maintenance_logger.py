"""
数据维护日志服务。

职责：
1. 统一维护任务摘要日志写入（app.log + maintenance_logs）。
2. debug 明细写入 logs/debug/ 文件而非数据库，避免大对象占用写锁和内存。
3. 保持主程序对明细裁剪函数的兼容调用。
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from app.db.state_db import StateDB

_DEBUG_LOG_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent / "logs" / "debug"
_DEBUG_BATCH_SIZE = 32
_ERROR_LOG_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent / "logs" / "error"
_ERROR_BATCH_SIZE = 128


def compact_maintenance_detail_for_app_log(message: str, detail: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    输入：
    1. message: 日志消息。
    2. detail: 原始结构化明细。
    输出：
    1. app.log 可读的摘要 detail。
    用途：
    1. 在主日志中保留关键进度字段，避免大对象刷屏。
    边界条件：
    1. 非字典或空明细返回 None。
    """

    _ = message
    if not isinstance(detail, dict) or not detail:
        return None
    compact = dict(detail)
    for noisy_key in ("rows", "tasks", "task_sample", "failure_tasks", "failure_codes"):
        compact.pop(noisy_key, None)
    return compact or None


class MaintenanceLogger:
    """
    维护任务日志写入器。
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
        1. job_id: 维护任务 ID。
        2. state_db: 状态库实例。
        3. logger: 应用日志器。
        4. debug_enabled: 是否写入 debug 明细。
        输出：
        1. 无返回值。
        用途：
        1. 绑定维护任务日志上下文。
        边界条件：
        1. job_id 为空时上层应避免创建对象。
        """

        self.job_id = str(job_id)
        self.state_db = state_db
        self.logger = logger
        self.debug_enabled = bool(debug_enabled)
        self._debug_buffer: list[str] = []
        self._error_buffer: list[str] = []

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
        1. 向 maintenance_logs 写入结构化日志。
        边界条件：
        1. 由 StateDB 负责落库失败异常传播。
        """

        self.state_db.append_maintenance_log(
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
        1. 写入维护摘要信息日志。
        边界条件：
        1. detail 会先裁剪再写入 app.log 与 info 级落库。
        """

        compact = compact_maintenance_detail_for_app_log(message, detail)
        self.logger.info("[maintenance:%s] %s", self.job_id, self._format_text(message, compact))
        self._append_db_log(level="info", message=message, detail=compact)

    def warning(self, message: str, detail: dict[str, Any] | None = None) -> None:
        """
        输入：
        1. message: 警告日志消息。
        2. detail: 可选结构化明细。
        输出：
        1. 无返回值。
        用途：
        1. 写入维护警告日志。
        边界条件：
        1. 前端主日志流按 info 级读取，因此 warning 落库为 info 前缀消息。
        """

        compact = compact_maintenance_detail_for_app_log(message, detail)
        self.logger.warning("[maintenance:%s] %s", self.job_id, self._format_text(message, compact))
        self._append_db_log(level="info", message=f"[warning] {message}", detail=compact)

    def error(self, message: str, detail: dict[str, Any] | None = None) -> None:
        """
        输入：
        1. message: 错误日志消息。
        2. detail: 可选结构化明细。
        输出：
        1. 无返回值。
        用途：
        1. 写入维护错误日志。
        边界条件：
        1. detail 会写入 error 日志供前端错误面板查询。
        """

        compact = compact_maintenance_detail_for_app_log(message, detail)
        self.logger.error("[maintenance:%s] %s", self.job_id, self._format_text(message, compact))
        self._append_db_log(level="error", message=message, detail=compact)

    def debug(self, message: str, detail: dict[str, Any] | None = None) -> None:
        """
        输入：
        1. message: debug 日志消息。
        2. detail: 可选结构化明细。
        输出：
        1. 无返回值。
        用途：
        1. 在 debug 模式下写入 logs/debug/ 文件。
        边界条件：
        1. debug 关闭时直接忽略。
        """

        if not self.debug_enabled:
            return
        if not isinstance(detail, dict):
            detail = None
        self._buffer_debug_record(message=message, detail=detail)

    def debug_lazy(self, message: str, detail_fn: Callable[[], dict[str, Any] | None]) -> None:
        """
        输入：
        1. message: debug 日志消息。
        2. detail_fn: 延迟求值回调，仅在 debug 开启时调用。
        输出：
        1. 无返回值。
        用途：
        1. 避免大对象在 debug 关闭时仍被构造。
        边界条件：
        1. detail_fn 抛异常时记录错误信息。
        """

        if not self.debug_enabled:
            return
        try:
            detail = detail_fn()
        except Exception as exc:
            detail = {"_debug_lazy_error": f"{type(exc).__name__}: {exc}"}
        if not isinstance(detail, dict):
            detail = None
        self._buffer_debug_record(message=message, detail=detail)

    def flush_debug(self) -> None:
        """将 debug/error 缓冲区批量写入文件。任务结束时必须调用。"""
        self._flush_debug_buffer()
        self._flush_error_buffer()

    def error_file_lazy(self, message: str, detail: dict[str, Any] | None = None) -> None:
        """
        输入：
        1. message: 错误消息。
        2. detail: 可选结构化明细。
        输出：
        1. 无返回值。
        用途：
        1. 将高频错误按批写入 logs/error/ 文件，避免逐条刷盘。
        边界条件：
        1. 本方法仅写文件，不写数据库。
        """

        if not isinstance(detail, dict):
            detail = None
        record: dict[str, Any] = {
            "ts": datetime.now().isoformat(),
            "job_id": self.job_id,
            "level": "error",
            "message": str(message),
        }
        if detail is not None:
            record["detail"] = detail
        try:
            self._error_buffer.append(json.dumps(record, ensure_ascii=False, default=str))
        except Exception:
            return
        if len(self._error_buffer) >= _ERROR_BATCH_SIZE:
            self._flush_error_buffer()

    def _buffer_debug_record(self, *, message: str, detail: dict[str, Any] | None) -> None:
        """将 debug 记录序列化后放入缓冲区，达到阈值时自动刷盘。"""
        record: dict[str, Any] = {
            "ts": datetime.now().isoformat(),
            "job_id": self.job_id,
            "level": "debug",
            "message": message,
        }
        if detail is not None:
            record["detail"] = detail
        try:
            self._debug_buffer.append(json.dumps(record, ensure_ascii=False, default=str))
        except Exception:
            return
        if len(self._debug_buffer) >= _DEBUG_BATCH_SIZE:
            self._flush_debug_buffer()

    def _flush_debug_buffer(self) -> None:
        """将缓冲区内容一次性写入 JSONL 文件并清空。"""
        if not self._debug_buffer:
            return
        lines = self._debug_buffer
        self._debug_buffer = []
        try:
            _DEBUG_LOG_DIR.mkdir(parents=True, exist_ok=True)
            path = _DEBUG_LOG_DIR / f"maintenance_{self.job_id}.jsonl"
            with open(path, "a", encoding="utf-8") as f:
                f.write("\n".join(lines))
                f.write("\n")
        except Exception:
            pass

    def _flush_error_buffer(self) -> None:
        """将错误缓冲区一次性写入 JSONL 文件并清空。"""
        if not self._error_buffer:
            return
        lines = self._error_buffer
        self._error_buffer = []
        try:
            _ERROR_LOG_DIR.mkdir(parents=True, exist_ok=True)
            path = _ERROR_LOG_DIR / f"maintenance_{self.job_id}.jsonl"
            with open(path, "a", encoding="utf-8") as f:
                f.write("\n".join(lines))
                f.write("\n")
        except Exception:
            pass
