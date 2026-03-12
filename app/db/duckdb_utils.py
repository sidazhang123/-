"""
DuckDB 连接辅助工具。

职责：
1. 统一处理源库连接的重试、配置冲突兼容与文件占用兜底。
2. 为读路径提供可选 shadow copy 回退，降低前台查询对后台任务的干扰。
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Callable

import duckdb


def is_duckdb_file_lock_error(exc: Exception) -> bool:
    """判断是否属于 DuckDB 文件句柄占用类异常。

    兼容的典型文本包括 Windows 下的“另一个程序正在使用此文件”，
    以及 DuckDB 常见的 `already open`、`unique file handle conflict`、`permission denied` 等提示。
    """

    message = str(exc).lower()
    return (
        "另一个程序正在使用此文件" in str(exc)
        or "already open" in message
        or "unique file handle conflict" in message
        or "permission denied" in message
    )


def is_duckdb_config_conflict(exc: Exception) -> bool:
    """判断是否为同一数据库被不同连接配置打开导致的冲突。"""

    return "different configuration than existing connections" in str(exc).lower()


def connect_duckdb_compatible(
    db_path: Path | str,
    *,
    read_only: bool = False,
    retries: int = 20,
    retry_sleep_seconds: float = 0.05,
    shadow_copy_factory: Callable[[], Path] | None = None,
) -> duckdb.DuckDBPyConnection:
    """以兼容模式打开 DuckDB 连接。

    回退顺序：
    1. 先按请求的 `read_only` 配置直接连接。
    2. 若只读连接因配置冲突失败，则回退到非只读连接重试。
    3. 若遇到文件锁冲突，优先尝试 shadow copy；没有 shadow copy 工厂时，再尝试非只读回退。
    4. 多次重试仍失败时，抛出最后一次异常。
    """

    resolved_path = Path(db_path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    last_exc: Exception | None = None

    for _ in range(max(1, int(retries))):
        try:
            return duckdb.connect(str(resolved_path), read_only=read_only)
        except duckdb.ConnectionException as exc:
            last_exc = exc
            if not read_only or not is_duckdb_config_conflict(exc):
                raise
            try:
                return duckdb.connect(str(resolved_path), read_only=False)
            except Exception as fallback_exc:
                last_exc = fallback_exc
                if not is_duckdb_file_lock_error(fallback_exc):
                    raise
                time.sleep(retry_sleep_seconds)
        except duckdb.IOException as exc:
            last_exc = exc
            if not is_duckdb_file_lock_error(exc):
                raise
            if shadow_copy_factory is not None:
                try:
                    shadow_path = Path(shadow_copy_factory())
                    return duckdb.connect(str(shadow_path), read_only=True)
                except Exception as shadow_exc:
                    last_exc = shadow_exc
            elif read_only:
                try:
                    return duckdb.connect(str(resolved_path), read_only=False)
                except Exception as fallback_exc:
                    last_exc = fallback_exc
                    if not is_duckdb_file_lock_error(fallback_exc):
                        raise
            time.sleep(retry_sleep_seconds)

    if last_exc is not None:
        raise last_exc
    return duckdb.connect(str(resolved_path), read_only=read_only)