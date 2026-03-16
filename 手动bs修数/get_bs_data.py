"""中文说明：统一管理 BaoStock 数据拉取、缺口任务生成与断点恢复。

输入：外部通过 get_bs_data 读取同级目录任务 JSON 执行，通过 make_inconsistent_tasks / make_full_tasks 生成或追加任务。
输出：任务执行结果写入 quant DuckDB，任务文件与阶段库固定写入模块同级目录。
用途：替代旧的 manual_update.py 与 runtime.py，提供单文件实现与固定路径的恢复语义。
边界条件：当任务 JSON 缺失、为空或结构非法时，get_bs_data 直接报错；任务生成函数负责初始化或追加去重。
"""

from __future__ import annotations

import contextlib
import io
import json
import logging
import multiprocessing
import os
import shutil
import socket
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from queue import Empty
from typing import Any, Iterable

import duckdb
import pandas as pd
from pydantic_settings import BaseSettings, SettingsConfigDict

# Compatibility shim for baostock with pandas>=2.0
if not hasattr(pd.DataFrame, "append"):
    def _df_append(self, other, ignore_index=False, **kwargs):
        return pd.concat([self, other], ignore_index=ignore_index)

    pd.DataFrame.append = _df_append  # type: ignore[attr-defined]

try:
    import baostock as bs
except ImportError:  # pragma: no cover - optional for tests
    bs = None

try:
    import baostock.common.context as bs_context
except ImportError:  # pragma: no cover - optional for tests
    bs_context = None

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "QuantPattern"
    log_level: str = "INFO"

    db_path: str = r"D:\quant.duckdb"
    timezone: str = "Asia/Shanghai"

    baostock_user: str | None = None
    baostock_password: str | None = None
    baostock_timeout_seconds: int = 30
    baostock_retry_times: int = 3
    baostock_retry_sleep: float = 1.0
    baostock_max_processes: int = 6
    baostock_login_stagger_seconds: float = 0.35
    batch_no_durable_progress_warning_seconds: float = 60.0
    batch_no_durable_progress_retry_seconds: float = 180.0
    batch_stall_retry_times: int = 3

    @property
    def db_file(self) -> Path:
        return Path(self.db_path).expanduser().resolve()


settings = Settings()
ROOT = Path(__file__).resolve().parent
TASKS_FILE = ROOT / "bs_pending_tasks.json"
STAGE_ROOT = ROOT / "bs_stage_dbs"
LOG_FILE = ROOT / "get_bs_data.log"
TASK_DATE_FORMAT = "%Y-%m-%d"
MINUTE_BAR_TARGETS = {"15": 16, "30": 8, "60": 4}

__all__ = ["get_bs_data", "make_inconsistent_tasks", "make_full_tasks"]

_STAGE_BUFFER_ROWS_BY_FREQ = {
    "w": 20000,
    "d": 50000,
    "60": 80000,
    "30": 120000,
    "15": 160000,
}
_STAGE_BUFFER_MAX_PENDING_TASKS = 200
_STAGE_BUFFER_MAX_INTERVAL_SECONDS = 10.0
_CHECKPOINT_MIN_TASKS = 200
_CHECKPOINT_MIN_INTERVAL_SECONDS = 15.0


def _parse_task_date(value: str) -> date:
    """
    中文说明：把任务日期字符串解析为 date 对象。

    输入：YYYY-MM-DD 格式日期字符串。
    输出：对应的 date 对象。
    用途：统一校验任务起止日期，避免 JSON 中混入非法格式。
    边界条件：输入为空、格式错误或不可解析时抛出异常。
    """
    return date.fromisoformat(value)


def _format_task_date(value: date) -> str:
    """
    中文说明：把 date 对象格式化为任务日期字符串。

    输入：date 对象。
    输出：YYYY-MM-DD 字符串。
    用途：统一任务 JSON 与内部键值格式，避免日期格式漂移。
    边界条件：调用方需保证传入对象是合法日期。
    """
    return value.strftime(TASK_DATE_FORMAT)


def _task_key(task: dict[str, str]) -> tuple[str, str, str, str]:
    """
    中文说明：生成任务唯一键。

    输入：标准化后的任务字典。
    输出：(code, freq, start_date, end_date) 组成的唯一键。
    用途：用于追加去重、运行中断恢复与剩余任务计算。
    边界条件：调用方需保证任务已完成字段校验和标准化。
    """
    return (task["code"], task["freq"], task["start_date"], task["end_date"])


def _normalize_task(task: dict[str, Any]) -> dict[str, str]:
    """
    中文说明：把输入任务规范化为标准结构。

    输入：包含 code、freq、start_date、end_date 的原始任务对象。
    输出：字段完整、频率标准化、日期有序的任务字典。
    用途：统一 JSON 读写、任务去重和多进程执行输入。
    边界条件：字段缺失、日期非法或 start_date 晚于 end_date 时抛出异常。
    """
    if not isinstance(task, dict):
        raise RuntimeError("task item must be an object")
    code = str(task.get("code", "")).strip()
    freq = _normalize_freq(str(task.get("freq", "")).strip())
    start_date = str(task.get("start_date", "")).strip()
    end_date = str(task.get("end_date", "")).strip()
    if not code:
        raise RuntimeError("task item missing code")
    if freq not in _FREQ_TABLE:
        raise RuntimeError(f"task item has unsupported freq: {freq}")
    if not start_date or not end_date:
        raise RuntimeError("task item missing start_date or end_date")
    if len(start_date) == 10 and len(end_date) == 10 and start_date[4] == "-" and start_date[7] == "-" and end_date[4] == "-" and end_date[7] == "-":
        if start_date > end_date:
            raise RuntimeError(f"task date range is invalid: {code} {freq} {start_date} {end_date}")
        try:
            date.fromisoformat(start_date)
            date.fromisoformat(end_date)
            return {
                "code": code,
                "freq": freq,
                "start_date": start_date,
                "end_date": end_date,
            }
        except ValueError:
            pass
    start_value = _parse_task_date(start_date)
    end_value = _parse_task_date(end_date)
    if start_value > end_value:
        raise RuntimeError(f"task date range is invalid: {code} {freq} {start_date} {end_date}")
    return {
        "code": code,
        "freq": freq,
        "start_date": _format_task_date(start_value),
        "end_date": _format_task_date(end_value),
    }


def _task_cost(task: dict[str, str]) -> float:
    """
    中文说明：估算任务执行成本。

    输入：标准化后的任务字典。
    输出：用于均衡分配的浮点权重。
    用途：把分钟长区间任务分散到不同进程，降低尾部拖慢。
    边界条件：权重是启发式估算，不保证与真实耗时完全一致。
    """
    start_value = _parse_task_date(task["start_date"])
    end_value = _parse_task_date(task["end_date"])
    days = max(1, (end_value - start_value).days + 1)
    base = {"15": 10.0, "30": 6.0, "60": 4.0, "d": 1.0, "w": 0.5}[task["freq"]]
    if task["freq"] == "w":
        return max(1.0, days / 7.0) * base
    return days * base


def _sort_tasks_for_merge(tasks: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    中文说明：按合并顺序排序任务。

    输入：标准化后的任务列表。
    输出：按 code、freq、start_date、end_date 排序后的任务列表。
    用途：为区间去重与相邻合并提供稳定顺序。
    边界条件：任务列表为空时返回空列表。
    """
    return sorted(
        tasks,
        key=lambda item: (item["code"], item["freq"], item["start_date"], item["end_date"]),
    )


def _merge_overlapping_tasks(tasks: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    中文说明：合并同 code+freq 下重叠或相邻的任务区间。

    输入：标准化后的任务列表。
    输出：区间合并后的任务列表。
    用途：减少重复请求次数和 JSON 体积，是必做性能优化之一。
    边界条件：仅合并 code 和 freq 相同且区间相邻或交叠的任务。
    """
    merged: list[dict[str, str]] = []
    for task in _sort_tasks_for_merge(tasks):
        if not merged:
            merged.append(dict(task))
            continue
        previous = merged[-1]
        if previous["code"] != task["code"] or previous["freq"] != task["freq"]:
            merged.append(dict(task))
            continue
        previous_end = _parse_task_date(previous["end_date"])
        current_start = _parse_task_date(task["start_date"])
        current_end = _parse_task_date(task["end_date"])
        if _is_weekend_only_gap(previous_end, current_start):
            if current_end > previous_end:
                previous["end_date"] = task["end_date"]
            continue
        merged.append(dict(task))
    return merged


def _normalize_merge_and_dedupe_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, str]]:
    """
    中文说明：对任务列表做标准化、去重和区间合并。

    输入：原始任务列表。
    输出：标准化后的最小任务集合。
    用途：作为任务文件写回前和执行前的统一预处理入口。
    边界条件：遇到非法任务时抛出异常，不进行容错吞并。
    """
    unique_tasks: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for task in tasks:
        normalized = _normalize_task(task)
        unique_tasks[_task_key(normalized)] = normalized
    return _merge_overlapping_tasks(list(unique_tasks.values()))


def _assign_tasks_balanced(tasks: list[dict[str, str]], process_count: int) -> list[list[dict[str, str]]]:
    """
    中文说明：按估算成本近似均衡地把任务分配到多个进程。

    输入：标准化任务列表与目标进程数。
    输出：每个进程对应的任务子列表。
    用途：避免分钟长区间任务集中在同一进程导致尾部拖慢，同时让每个进程优先产出较轻任务的 durable 进度。
    边界条件：进程数小于 1 时按 1 处理；空任务列表返回空列表。
    """
    if not tasks:
        return []
    bucket_count = max(1, min(process_count, len(tasks)))
    buckets: list[list[dict[str, str]]] = [[] for _ in range(bucket_count)]
    bucket_costs = [0.0 for _ in range(bucket_count)]
    for task in sorted(tasks, key=_task_cost, reverse=True):
        bucket_index = min(range(bucket_count), key=lambda index: bucket_costs[index])
        buckets[bucket_index].append(task)
        bucket_costs[bucket_index] += _task_cost(task)
    ordered_buckets: list[list[dict[str, str]]] = []
    for bucket in buckets:
        if not bucket:
            continue
        ordered_buckets.append(
            sorted(
                bucket,
                key=lambda item: (
                    _task_cost(item),
                    list(_FREQ_TABLE).index(item["freq"]),
                    item["start_date"],
                    item["end_date"],
                    item["code"],
                ),
            )
        )
    return ordered_buckets


def _build_single_day_task(code: str, freq: str, task_date: date) -> dict[str, str]:
    """
    中文说明：构造单日任务。

    输入：股票代码、周期和单日日期。
    输出：start_date 与 end_date 相同的标准任务。
    用途：用于日线缺口、分钟条数不足等单日补数场景。
    边界条件：调用方需保证日期属于需要补数的有效日期。
    """
    day_text = _format_task_date(task_date)
    return {"code": code, "freq": _normalize_freq(freq), "start_date": day_text, "end_date": day_text}


def _is_weekend_only_gap(previous_end: date, current_start: date) -> bool:
    """
    中文说明：判断两个日期区间之间是否仅隔周末。

    输入：前一区间结束日和后一区间开始日。
    输出：若中间仅包含周六周日或直接相邻则返回 True。
    用途：用于任务合并时跨周末压缩区间，减少分钟和日线任务数量。
    边界条件：若存在任意工作日缺口则返回 False。
    """
    if current_start <= previous_end + timedelta(days=1):
        return True
    cursor = previous_end + timedelta(days=1)
    while cursor < current_start:
        if cursor.weekday() < 5:
            return False
        cursor += timedelta(days=1)
    return True


def _summarize_tasks_by_freq(tasks: list[dict[str, str]]) -> dict[str, int]:
    """
    中文说明：统计任务列表中各频率的任务数量。

    输入：标准化任务列表。
    输出：按频率聚合后的任务数量字典。
    用途：为任务生成与执行日志提供可快速扫描的频率分布摘要。
    边界条件：空列表返回空字典。
    """
    summary: dict[str, int] = {}
    for task in tasks:
        summary[task["freq"]] = summary.get(task["freq"], 0) + 1
    return summary


class BatchStalledError(RuntimeError):
    """
    批次停滞异常。

    输入：message 为停滞描述。
    输出：可被上层识别并触发批次级重试的异常对象。
    用途：区分普通子进程错误与长时间无 durable 进度导致的 watchdog 终止。
    边界条件：仅用于批次 watchdog 主动中断场景。
    """


def setup_logging(level: str, log_file: Path | None = None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, mode="a", encoding="utf-8", delay=False))
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )


def _ensure_db_dir(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _connect_db(db_path: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    if not read_only:
        _ensure_db_dir(db_path)
    return duckdb.connect(str(db_path), read_only=read_only)


def _stage_buffer_threshold(freq: str) -> int:
    return _STAGE_BUFFER_ROWS_BY_FREQ.get(_normalize_freq(freq), 100000)


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stocks (
    code VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    PRIMARY KEY (code)
);

CREATE TABLE IF NOT EXISTS klines_d (
    code VARCHAR NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume BIGINT NOT NULL,
    amount BIGINT,
    PRIMARY KEY (code, datetime)
);

CREATE TABLE IF NOT EXISTS klines_w (
    code VARCHAR NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume BIGINT NOT NULL,
    amount BIGINT,
    PRIMARY KEY (code, datetime)
);

CREATE TABLE IF NOT EXISTS klines_15 (
    code VARCHAR NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume BIGINT NOT NULL,
    amount BIGINT,
    PRIMARY KEY (code, datetime)
);

CREATE TABLE IF NOT EXISTS klines_30 (
    code VARCHAR NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume BIGINT NOT NULL,
    amount BIGINT,
    PRIMARY KEY (code, datetime)
);

CREATE TABLE IF NOT EXISTS klines_60 (
    code VARCHAR NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume BIGINT NOT NULL,
    amount BIGINT,
    PRIMARY KEY (code, datetime)
);
"""


def init_db(db_path: Path | None = None) -> None:
    target = (db_path or settings.db_file).resolve()
    start = time.monotonic()
    logger.info("init_db start path=%s", target)
    conn = _connect_db(target)
    try:
        conn.execute(_SCHEMA_SQL)
    finally:
        conn.close()
    elapsed_ms = int((time.monotonic() - start) * 1000)
    logger.info("init_db done path=%s ms=%s", target, elapsed_ms)


_FREQ_TABLE = {
    "d": "klines_d",
    "w": "klines_w",
    "15": "klines_15",
    "30": "klines_30",
    "60": "klines_60",
}


def _normalize_freq(freq: str) -> str:
    return {
        "D": "d",
        "W": "w",
        "15": "15",
        "30": "30",
        "60": "60",
        "d": "d",
        "w": "w",
        "15m": "15",
        "30m": "30",
        "60m": "60",
    }.get(freq, freq)


def _klines_table(freq: str) -> str:
    normalized = _normalize_freq(freq)
    table = _FREQ_TABLE.get(normalized)
    if not table:
        raise ValueError("unsupported freq")
    return table


def _replace_stocks(db_path: Path, rows: list[tuple[str, str]]) -> None:
    conn = _connect_db(db_path)
    try:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM stocks")
        if rows:
            conn.executemany(
                "INSERT INTO stocks(code, name) VALUES(?, ?)",
                rows,
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def _stage_db_path(stage_dir: Path, worker_index: int) -> Path:
    return stage_dir / f"worker_{worker_index}.duckdb"


def _create_stage_table(conn: duckdb.DuckDBPyConnection, freq: str) -> None:
    table = _klines_table(freq)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            code VARCHAR NOT NULL,
            datetime TIMESTAMP NOT NULL,
            open DOUBLE NOT NULL,
            high DOUBLE NOT NULL,
            low DOUBLE NOT NULL,
            close DOUBLE NOT NULL,
            volume BIGINT NOT NULL,
            amount BIGINT
        )
        """
    )


def _init_stage_db(conn: duckdb.DuckDBPyConnection) -> None:
    for freq in _FREQ_TABLE:
        _create_stage_table(conn, freq)


def _rows_to_stage_frame(rows: list[tuple]) -> pd.DataFrame:
    frame = pd.DataFrame.from_records(
        rows,
        columns=["code", "datetime", "open", "high", "low", "close", "volume", "amount"],
    )
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    return frame


def _flush_stage_rows(conn: duckdb.DuckDBPyConnection, freq: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    table = _klines_table(freq)
    temp_view = f"stage_flush_{freq}_{os.getpid()}"
    frame = _rows_to_stage_frame(rows)
    conn.execute("BEGIN")
    try:
        conn.register(temp_view, frame)
        conn.execute(
            f"""
            INSERT INTO {table}(code, datetime, open, high, low, close, volume, amount)
            SELECT code, datetime, open, high, low, close, volume, amount
            FROM {temp_view}
            """,
        )
        conn.unregister(temp_view)
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.unregister(temp_view)
        except Exception:
            pass
        conn.execute("ROLLBACK")
        raise
    return len(rows)


def _emit_durable_task_done(
    progress_queue,
    pending_done_tasks: list[tuple[str, str, str, str, int]],
) -> None:
    """
    中文说明：把已 durable 落盘的任务完成事件发送给主进程。

    输入：进度队列和待确认完成的任务列表。
    输出：无返回值。
    用途：只有任务对应的数据已经写入 stage DuckDB 后，主进程才会把该任务从 JSON 中移除。
    边界条件：传入空列表时不会发送任何事件。
    """
    for code, freq, start_date, end_date, row_count in pending_done_tasks:
        progress_queue.put(("done", code, freq, start_date, end_date, row_count))


def _flush_all_stage_buffers(
    stage_conn: duckdb.DuckDBPyConnection | None,
    stage_buffers: dict[str, list[tuple]],
    pending_done_by_freq: dict[str, list[tuple[str, str, str, str, int]]],
    progress_queue,
    *,
    reason: str,
) -> None:
    """
    中文说明：尝试把当前 worker 内存缓冲的所有数据落盘到 stage。

    输入：stage 连接、按频率分组的行缓冲、待确认 durable 的任务列表和进度队列。
    输出：无返回值。
    用途：在异常退出前尽量保留已拉取数据，减少重跑损耗。
    边界条件：任一频率 flush 失败时仅记录日志并继续处理其余频率。
    """
    if stage_conn is None:
        return
    for buffered_freq, buffered_rows in stage_buffers.items():
        if not buffered_rows:
            continue
        flush_started_at = time.perf_counter()
        try:
            flushed_rows = _flush_stage_rows(stage_conn, buffered_freq, buffered_rows)
            pending_done_tasks = pending_done_by_freq.get(buffered_freq, [])
            _emit_durable_task_done(progress_queue, pending_done_tasks)
            logger.info(
                "worker flush-all pid=%s reason=%s freq=%s rows=%s durable_done=%s elapsed=%.3fs",
                os.getpid(),
                reason,
                buffered_freq,
                flushed_rows,
                len(pending_done_tasks),
                time.perf_counter() - flush_started_at,
            )
            buffered_rows.clear()
            pending_done_tasks.clear()
        except Exception:
            logger.exception(
                "worker flush-all failed pid=%s reason=%s freq=%s buffered_rows=%s",
                os.getpid(),
                reason,
                buffered_freq,
                len(buffered_rows),
            )


def _merge_stage_dir_if_needed(db_path: Path, stage_dir: Path, freqs: Iterable[str]) -> dict[str, int]:
    """
    合并 staging 目录中已落盘的数据。

    输入：db_path 为主库路径，stage_dir 为批次 staging 目录，freqs 为批次频率列表。
    输出：每个频率成功合并的行数。
    用途：在批次重试或恢复前先合并已 durable 落盘的数据，减少重复拉取。
    边界条件：当 staging 目录不存在或没有可合并数据时返回全 0。
    """
    if not stage_dir.exists():
        return {str(freq): 0 for freq in freqs}
    active_freqs = _stage_dir_active_freqs(stage_dir, freqs)
    merged_counts = {str(freq): 0 for freq in freqs}
    if not active_freqs:
        return merged_counts
    merged_counts.update(_merge_stage_dir(db_path, stage_dir, active_freqs))
    return merged_counts


def _list_stage_db_files(stage_dir: Path) -> list[Path]:
    return sorted(
        file_path
        for file_path in stage_dir.glob("worker_*.duckdb")
        if file_path.exists() and file_path.stat().st_size > 0
    )


def _merge_staged_freq_dbs(db_path: Path, freq: str, stage_db_files: list[Path]) -> int:
    if not stage_db_files:
        return 0

    table = _klines_table(freq)
    conn = _connect_db(db_path)
    attached_aliases: list[str] = []
    total_rows = 0
    try:
        for index, stage_db_file in enumerate(stage_db_files):
            alias = f"stage_{index}"
            stage_db_sql = stage_db_file.as_posix().replace("'", "''")
            conn.execute(f"ATTACH '{stage_db_sql}' AS {alias}")
            attached_aliases.append(alias)

        conn.execute("BEGIN")
        for alias in attached_aliases:
            total_rows += int(conn.execute(f"SELECT COUNT(*) FROM {alias}.{table}").fetchone()[0])
            conn.execute(
                f"""
                INSERT INTO {table}(code, datetime, open, high, low, close, volume, amount)
                SELECT code, datetime, open, high, low, close, volume, amount
                FROM {alias}.{table}
                ON CONFLICT(code, datetime) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    amount=excluded.amount
                """
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        for alias in reversed(attached_aliases):
            conn.execute(f"DETACH {alias}")
        conn.close()
    return total_rows


def _merge_stage_dir(db_path: Path, stage_dir: Path, freqs: Iterable[str]) -> dict[str, int]:
    stage_db_files = _list_stage_db_files(stage_dir)
    merged_counts = {str(freq): 0 for freq in freqs}
    if not stage_db_files:
        return merged_counts

    for freq in freqs:
        merge_started_at = time.perf_counter()
        merged_rows = _merge_staged_freq_dbs(db_path, str(freq), stage_db_files)
        merged_counts[str(freq)] = merged_rows
        logger.info(
            "merge done freq=%s dbs=%s rows=%s elapsed=%.3fs",
            freq,
            len(stage_db_files),
            merged_rows,
            time.perf_counter() - merge_started_at,
        )
    return merged_counts


def _stage_dir_active_freqs(stage_dir: Path, freqs: Iterable[str]) -> list[str]:
    stage_db_files = _list_stage_db_files(stage_dir)
    if not stage_db_files:
        return []

    active_freqs: list[str] = []
    for freq in freqs:
        normalized_freq = _normalize_freq(str(freq))
        table = _klines_table(normalized_freq)
        has_rows = False
        for stage_db_file in stage_db_files:
            conn = _connect_db(stage_db_file, read_only=True)
            try:
                row_count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            finally:
                conn.close()
            if row_count > 0:
                has_rows = True
                break
        if has_rows:
            active_freqs.append(normalized_freq)
    return active_freqs


def _list_codes(db_path: Path, offset: int, limit: int) -> list[str]:
    conn = _connect_db(db_path, read_only=True)
    try:
        rows = conn.execute(
            "SELECT code FROM stocks ORDER BY code LIMIT ? OFFSET ?",
            [limit, offset],
        ).fetchall()
        return [str(row[0]) for row in rows]
    finally:
        conn.close()


class BaoStockService:
    def __init__(self) -> None:
        self._logged_in = False

    def login(self) -> None:
        if bs is None:
            raise RuntimeError("baostock is not installed")
        if self._logged_in:
            return
        _login_baostock()
        self._logged_in = True
        logger.info("baostock login ok")

    def logout(self) -> None:
        if bs and self._logged_in:
            bs.logout()
            self._logged_in = False

    def query_stocks(self) -> pd.DataFrame:
        self.login()
        result = bs.query_stock_basic()
        if result.error_code != "0":
            raise RuntimeError(result.error_msg)
        return result.get_data()


def _process_login() -> None:
    if bs is None:
        raise RuntimeError("baostock is not installed")
    _login_baostock()


def _process_logout() -> None:
    if bs:
        _call_baostock_quietly(bs.logout)


def _call_baostock_quietly(func, *args, **kwargs):
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
        result = func(*args, **kwargs)
    return result


def _is_baostock_not_logged_in_error(value: object) -> bool:
    text = str(value or "").lower()
    return "用户未登录" in str(value or "") or "you don't login" in text


def _apply_baostock_socket_timeout() -> None:
    if bs_context is None:
        return
    default_socket = getattr(bs_context, "default_socket", None)
    if default_socket is not None:
        default_socket.settimeout(settings.baostock_timeout_seconds)


def _login_baostock() -> None:
    previous_default_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(settings.baostock_timeout_seconds)
    try:
        if settings.baostock_user and settings.baostock_password:
            result = _call_baostock_quietly(
                bs.login,
                user_id=settings.baostock_user,
                password=settings.baostock_password,
            )
        else:
            result = _call_baostock_quietly(bs.login)
    finally:
        socket.setdefaulttimeout(previous_default_timeout)

    if result.error_code != "0":
        raise RuntimeError(f"baostock login failed: {result.error_msg}")
    _apply_baostock_socket_timeout()


def _retry_call(func, times: int, sleep: float):
    last_exc = None
    for i in range(times):
        try:
            if i > 0:
                _process_logout()
                _process_login()
            return _call_baostock_quietly(func)
        except Exception as exc:  # pragma: no cover - network dependent
            last_exc = exc
            is_not_logged_in = _is_baostock_not_logged_in_error(exc)
            wait_seconds = sleep * max(1, i + 1)
            logger.warning(
                "retrying baostock call pid=%s attempt=%s/%s timeout=%ss relogin=%s error=%s",
                os.getpid(),
                i + 1,
                times,
                settings.baostock_timeout_seconds,
                "yes" if i > 0 or is_not_logged_in else "next-attempt",
                exc,
            )
            time.sleep(wait_seconds)
    raise last_exc


def normalize_freq(freq: str) -> str:
    return _normalize_freq(freq)


def freq_to_bs(freq: str) -> str:
    mapping = {
        "d": "d",
        "w": "w",
        "15": "15",
        "30": "30",
        "60": "60",
    }
    if freq not in mapping:
        raise ValueError("unsupported freq")
    return mapping[freq]


def split_datetime(df: pd.DataFrame, freq: str) -> pd.Series:
    date_digits = df["date"].astype(str).str.replace(r"\D", "", regex=True)
    normalized_date = date_digits.str[:4] + "-" + date_digits.str[4:6] + "-" + date_digits.str[6:8]
    if freq in ("d", "w"):
        return normalized_date + " 15:00:00"

    time_digits = df["time"].astype(str).str.replace(r"\D", "", regex=True)
    normalized_time = pd.Series("", index=df.index, dtype="object")
    full_timestamp_mask = time_digits.str.len() >= 14
    normalized_time.loc[full_timestamp_mask] = time_digits.loc[full_timestamp_mask].str[8:14]
    short_time_mask = (~full_timestamp_mask) & (time_digits.str.len() >= 6)
    normalized_time.loc[short_time_mask] = time_digits.loc[short_time_mask].str[:6]
    formatted_time = (
        normalized_time.str[:2] + ":" + normalized_time.str[2:4] + ":" + normalized_time.str[4:6]
    )
    return normalized_date + " " + formatted_time


def clean_klines(df: pd.DataFrame, code: str, freq: str) -> list[tuple]:
    """清洗单只股票K线数据。

    输入: 原始K线DataFrame、股票代码、周期标识。
    输出: 可直接写入目标库的K线元组列表。
    用途: 统一时间字段与数值字段格式，并剔除无效K线。
    边界条件: 空DataFrame、缺失时间列、无法解析时间或全部无效行时返回空列表。
    """
    if df.empty:
        return []
    local = df.copy()
    if "time" in local.columns:
        local = local.dropna(subset=["time"])
    if local.empty:
        return []
    local["datetime"] = pd.to_datetime(
        split_datetime(local, freq),
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )
    local = local.dropna(subset=["datetime"])
    if local.empty:
        return []

    for column in ("open", "high", "low", "close", "volume"):
        local[column] = pd.to_numeric(local[column], errors="coerce").fillna(0)

    if "amount" in local.columns:
        local["amount"] = pd.to_numeric(local["amount"], errors="coerce").fillna(0)
    else:
        local["amount"] = 0

    local = local.loc[
        ~(
            (local["volume"] == 0)
            & (local["open"] == local["close"])
            & (local["open"] == local["high"])
            & (local["open"] == local["low"])
        )
    ]
    if local.empty:
        return []

    datetimes = local["datetime"].tolist()
    return list(
        zip(
            [code] * len(local),
            datetimes,
            local["open"].astype(float).tolist(),
            local["high"].astype(float).tolist(),
            local["low"].astype(float).tolist(),
            local["close"].astype(float).tolist(),
            local["volume"].astype(float).astype(int).tolist(),
            local["amount"].astype(float).astype(int).tolist(),
        )
    )


def clean_stocks(df: pd.DataFrame) -> list[tuple[str, str]]:
    if df.empty:
        return []
    local = df.copy()
    if "type" in local.columns:
        local = local[local["type"].astype(str) == "1"]
    if "status" in local.columns:
        local = local[local["status"].astype(str) != "0"]
    rows: list[tuple[str, str]] = []
    for _, row in local.iterrows():
        rows.append(
            (
                str(row.get("code")),
                str(row.get("code_name")),
            )
        )
    return rows


def fetch_stock_list(service: BaoStockService) -> pd.DataFrame:
    df = service.query_stocks()
    if not df.empty and "type" in df.columns:
        df = df[df["type"].astype(str) == "1"].copy()
    if not df.empty and "status" in df.columns:
        df = df[df["status"].astype(str) != "0"].copy()
    return df


def _worker_process_tasks(
    tasks: list[dict],
    progress_queue,
    error_flag,
    stage_dir: str,
    worker_index: int,
) -> None:
    """
    子进程函数：从任务列表获取数据

    :param tasks: 任务列表 [{"code": "sh.600000", "freq": "d", "start_date": "2018-12-01", "end_date": "2024-12-31"}, ...]
    :param progress_queue: 进度队列
    :param error_flag: 错误标志
    :param stage_dir: staging 目录
    :param worker_index: worker 序号
    """
    stage_root = Path(stage_dir)
    stage_db_file = _stage_db_path(stage_root, worker_index)
    stage_buffers: dict[str, list[tuple]] = {}
    pending_done_by_freq: dict[str, list[tuple[str, str, str, str, int]]] = {}
    last_flush_at_by_freq: dict[str, float] = {}
    stage_conn: duckdb.DuckDBPyConnection | None = None

    try:
        if bs is None:
            raise RuntimeError("baostock is not installed")

        stage_root.mkdir(parents=True, exist_ok=True)
        stage_conn = _connect_db(stage_db_file)
        _init_stage_db(stage_conn)

        if settings.baostock_login_stagger_seconds > 0 and worker_index > 0:
            time.sleep(worker_index * settings.baostock_login_stagger_seconds)

        login_started_at = time.perf_counter()
        _process_login()
        logger.info(
            "worker login ok pid=%s tasks=%s elapsed=%.3fs",
            os.getpid(),
            len(tasks),
            time.perf_counter() - login_started_at,
        )

        try:
            for task in tasks:
                try:
                    task_started_at = time.perf_counter()
                    code = task["code"]
                    freq = _normalize_freq(task["freq"])
                    start_date = task["start_date"]
                    end_date = task["end_date"]
                    bs_freq = freq_to_bs(freq)

                    def _do_query():
                        if bs_freq in ("d", "w"):
                            fields = "date,open,high,low,close,volume,amount"
                        else:
                            fields = "date,time,open,high,low,close,volume,amount"
                        result = bs.query_history_k_data_plus(
                            code,
                            fields,
                            start_date=start_date,
                            end_date=end_date,
                            frequency=bs_freq,
                            adjustflag="3",
                        )
                        if result.error_code != "0":
                            if "time" in result.error_msg and "time" in fields:
                                fields = "date,open,high,low,close,volume,amount"
                                result = bs.query_history_k_data_plus(
                                    code,
                                    fields,
                                    start_date=start_date,
                                    end_date=end_date,
                                    frequency=bs_freq,
                                    adjustflag="3",
                                )
                        if result.error_code != "0":
                            raise RuntimeError(result.error_msg)
                        return result.get_data()

                    query_started_at = time.perf_counter()
                    df = _retry_call(_do_query, settings.baostock_retry_times, settings.baostock_retry_sleep)
                    query_elapsed = time.perf_counter() - query_started_at

                    clean_started_at = time.perf_counter()
                    rows = clean_klines(df, code, freq)
                    clean_elapsed = time.perf_counter() - clean_started_at

                    stage_started_at = time.perf_counter()
                    freq_buffer = stage_buffers.setdefault(freq, [])
                    pending_done_tasks = pending_done_by_freq.setdefault(freq, [])
                    flushed_rows = 0
                    durable_done_count = 0
                    if rows:
                        freq_buffer.extend(rows)
                        pending_done_tasks.append((code, freq, start_date, end_date, len(rows)))
                    else:
                        progress_queue.put(("done", code, freq, start_date, end_date, 0))
                        durable_done_count = 1

                    flush_interval = max(1.0, _STAGE_BUFFER_MAX_INTERVAL_SECONDS)
                    max_pending_tasks = max(1, _STAGE_BUFFER_MAX_PENDING_TASKS)
                    now = time.perf_counter()
                    last_flush_at = last_flush_at_by_freq.get(freq, now)
                    should_flush = (
                        len(freq_buffer) >= _stage_buffer_threshold(freq)
                        or len(pending_done_tasks) >= max_pending_tasks
                        or (pending_done_tasks and now - last_flush_at >= flush_interval)
                    )
                    if should_flush and freq_buffer:
                        flushed_rows = _flush_stage_rows(stage_conn, freq, freq_buffer)
                        _emit_durable_task_done(progress_queue, pending_done_tasks)
                        durable_done_count = len(pending_done_tasks)
                        freq_buffer.clear()
                        pending_done_tasks.clear()
                        last_flush_at_by_freq[freq] = time.perf_counter()
                    stage_elapsed = time.perf_counter() - stage_started_at

                    if flushed_rows > 0 or durable_done_count > 0:
                        logger.info(
                            "worker flush pid=%s code=%s freq=%s start=%s end=%s rows=%s buffered=%s flushed=%s durable_done=%s query=%.3fs clean=%.3fs stage=%.3fs total=%.3fs",
                            os.getpid(),
                            code,
                            freq,
                            start_date,
                            end_date,
                            len(rows),
                            len(freq_buffer),
                            flushed_rows,
                            durable_done_count,
                            query_elapsed,
                            clean_elapsed,
                            stage_elapsed,
                            time.perf_counter() - task_started_at,
                        )

                except KeyboardInterrupt:
                    return
                except Exception as e:
                    _flush_all_stage_buffers(
                        stage_conn,
                        stage_buffers,
                        pending_done_by_freq,
                        progress_queue,
                        reason="task_error",
                    )
                    error_flag.set()
                    logger.exception(
                        "worker task failed pid=%s code=%s freq=%s error=%s",
                        os.getpid(),
                        task.get("code"),
                        task.get("freq"),
                        e,
                    )
                    progress_queue.put(("error", str(e)))
                    return
        finally:
            _flush_all_stage_buffers(
                stage_conn,
                stage_buffers,
                pending_done_by_freq,
                progress_queue,
                reason="worker_exit",
            )
            if bs:
                bs.logout()
            if stage_conn is not None:
                stage_conn.close()
    except KeyboardInterrupt:
        return
    except Exception as e:
        _flush_all_stage_buffers(
            stage_conn,
            stage_buffers,
            pending_done_by_freq,
            progress_queue,
            reason="worker_outer_error",
        )
        error_flag.set()
        progress_queue.put(("error", str(e)))


class DataService:
    def __init__(self) -> None:
        self.bs = BaoStockService()
        self.main_db_path: Path = settings.db_file

    def update_stocks(self) -> int:
        df = fetch_stock_list(self.bs)
        rows = clean_stocks(df)
        _replace_stocks(self.main_db_path, rows)
        count = len(df)
        logger.info("stocks updated: %s", count)
        return count

    def _recover_stage_root(self, stage_root: Path, freqs: list[str]) -> None:
        """
        中文说明：恢复同级目录下残留的阶段库数据。

        输入：阶段库根目录和本次任务涉及的频率列表。
        输出：无返回值。
        用途：在上次中断后先把已 durable 落盘的数据合并回主库，避免重复拉取。
        边界条件：目录不存在、目录为空或没有有效阶段库时直接返回。
        """
        if not stage_root.exists():
            return
        requested_freqs = {_normalize_freq(freq) for freq in freqs}
        recoverable_freqs = list(_FREQ_TABLE.keys())
        for stage_dir in sorted(path for path in stage_root.iterdir() if path.is_dir()):
            pending_stage_dbs = _list_stage_db_files(stage_dir)
            if not pending_stage_dbs:
                continue
            active_freqs = _stage_dir_active_freqs(stage_dir, recoverable_freqs)
            if not active_freqs:
                shutil.rmtree(stage_dir)
                continue
            extra_freqs = [freq for freq in active_freqs if freq not in requested_freqs]
            if extra_freqs:
                logger.warning(
                    "恢复到本次请求之外的残留 staging 数据目录=%s extra_freqs=%s requested_freqs=%s",
                    stage_dir,
                    extra_freqs,
                    sorted(requested_freqs),
                )
            logger.info("发现上次未合并的 staging 数据库 %d 个，目录=%s，先恢复入库", len(pending_stage_dbs), stage_dir)
            _merge_stage_dir(self.main_db_path, stage_dir, active_freqs)
            shutil.rmtree(stage_dir)

    def _cleanup_stage_root(self, stage_root: Path) -> None:
        """
        中文说明：清理空的阶段库根目录。

        输入：阶段库根目录路径。
        输出：无返回值。
        用途：确保运行完成后不残留无用目录。
        边界条件：目录不存在或目录非空时不会报错。
        """
        if stage_root.exists() and not any(stage_root.iterdir()):
            shutil.rmtree(stage_root)

    def _run_task_pool(
        self,
        pending_tasks_input: list[dict[str, str]],
        tasks_file: Path,
        stage_dir: Path,
    ) -> dict[str, int]:
        """
        中文说明：执行任务文件中的剩余任务并支持停滞重试。

        输入：标准化后的剩余任务列表、任务文件路径和单次运行使用的阶段库目录。
        输出：每个频率最终合并入主库的行数统计。
        用途：按估算成本均衡分配任务、维护 durable 进度、在停滞时仅重试未完成任务。
        边界条件：任务列表为空时返回 0；子进程错误或重试耗尽时抛出异常。
        """
        if not pending_tasks_input:
            return {freq: 0 for freq in _FREQ_TABLE}

        if stage_dir.exists():
            shutil.rmtree(stage_dir)
        stage_dir.mkdir(parents=True, exist_ok=True)

        merge_totals = {freq: 0 for freq in _FREQ_TABLE}
        remaining_tasks = list(pending_tasks_input)

        for attempt in range(1, max(1, settings.batch_stall_retry_times) + 1):
            if not remaining_tasks:
                break

            active_freqs = sorted({task["freq"] for task in remaining_tasks}, key=lambda freq: list(_FREQ_TABLE).index(freq))
            total_written = {freq: 0 for freq in active_freqs}

            configured_max_processes = max(1, settings.baostock_max_processes)
            num_processes = min(configured_max_processes, len(remaining_tasks))
            logger.info(
                "开始任务池 attempt=%d/%d tasks=%d processes=%d freqs=%s",
                attempt,
                max(1, settings.batch_stall_retry_times),
                len(remaining_tasks),
                num_processes,
                active_freqs,
            )

            task_chunks = _assign_tasks_balanced(remaining_tasks, num_processes)

            ctx = multiprocessing.get_context("spawn")
            progress_queue = ctx.Queue()
            error_flag = ctx.Event()
            processes = []

            for i, chunk in enumerate(task_chunks):
                if not chunk:
                    continue
                proc = ctx.Process(
                    target=_worker_process_tasks,
                    args=(chunk, progress_queue, error_flag, str(stage_dir), i)
                )
                processes.append(proc)
                proc.start()
                logger.info("启动进程 %d，处理 %d 个任务，预估成本=%.1f", proc.pid, len(chunk), sum(_task_cost(task) for task in chunk))

            completed_by_freq = {freq: 0 for freq in active_freqs}
            completed_tasks = 0
            total_tasks = len(remaining_tasks)
            pending_tasks = {_task_key(task): task for task in remaining_tasks}
            last_checkpoint_tasks = 0
            last_checkpoint_at = time.perf_counter()
            last_durable_progress_at = time.perf_counter()
            last_watchdog_warning_at = 0.0
            stall_error: BatchStalledError | None = None

            try:
                while completed_tasks < total_tasks:
                    if error_flag.is_set():
                        logger.error("检测到进程错误，终止所有进程")
                        for proc in processes:
                            if proc.is_alive():
                                proc.terminate()
                                proc.join(timeout=5)
                        raise RuntimeError("子进程发生错误，任务终止")

                    try:
                        result = progress_queue.get(timeout=1)
                    except Empty:
                        now = time.perf_counter()
                        idle_seconds = now - last_durable_progress_at
                        warning_seconds = max(1.0, settings.batch_no_durable_progress_warning_seconds)
                        retry_seconds = max(warning_seconds, settings.batch_no_durable_progress_retry_seconds)
                        if (
                            idle_seconds >= warning_seconds
                            and now - last_watchdog_warning_at >= warning_seconds
                        ):
                            alive_processes = sum(1 for proc in processes if proc.is_alive())
                            by_freq_summary = ", ".join(
                                f"{freq}:tasks={completed_by_freq[freq]},rows={total_written[freq]}"
                                for freq in active_freqs
                            )
                            logger.warning(
                                "任务池长时间无durable进度 idle=%.1fs completed=%d/%d alive_processes=%d by_freq=%s",
                                idle_seconds,
                                completed_tasks,
                                total_tasks,
                                alive_processes,
                                by_freq_summary,
                            )
                            last_watchdog_warning_at = now
                        if idle_seconds >= retry_seconds:
                            stall_error = BatchStalledError(
                                f"任务池无durable进度超过阈值: idle={idle_seconds:.1f}s completed={completed_tasks}/{total_tasks}"
                            )
                            logger.error("%s，终止当前批次尝试", stall_error)
                            error_flag.set()
                            break
                        continue

                    if result[0] == "error":
                        error_flag.set()
                        logger.error("子进程错误: %s", result[1])
                        for proc in processes:
                            if proc.is_alive():
                                proc.terminate()
                                proc.join(timeout=5)
                        raise RuntimeError(f"子进程发生错误: {result[1]}")

                    if result[0] == "done":
                        completed_tasks += 1
                        code = result[1]
                        freq = _normalize_freq(result[2])
                        start_date = result[3]
                        end_date = result[4]
                        row_count = result[5]
                        total_written[freq] += row_count
                        completed_by_freq[freq] += 1
                        pending_tasks.pop((code, freq, start_date, end_date), None)

                        now = time.perf_counter()
                        last_durable_progress_at = now
                        should_checkpoint = (
                            completed_tasks - last_checkpoint_tasks >= _CHECKPOINT_MIN_TASKS
                            or now - last_checkpoint_at >= _CHECKPOINT_MIN_INTERVAL_SECONDS
                            or completed_tasks == total_tasks
                        )
                        if should_checkpoint:
                            remaining = list(pending_tasks.values())
                            self._write_tasks_file(tasks_file, remaining)
                            by_freq_summary = ", ".join(
                                f"{freq}:tasks={completed_by_freq[freq]},rows={total_written[freq]}"
                                for freq in active_freqs
                            )
                            logger.info(
                                "任务池进度 progress=%d/%d remaining=%d by_freq=%s",
                                completed_tasks,
                                total_tasks,
                                len(remaining),
                                by_freq_summary,
                            )
                            last_checkpoint_tasks = completed_tasks
                            last_checkpoint_at = now
            finally:
                for proc in processes:
                    if proc.is_alive():
                        proc.terminate()
                    proc.join(timeout=5)
                    logger.info("进程 %d 已关闭", proc.pid)
                progress_queue.close()
                progress_queue.join_thread()

            remaining_tasks = list(pending_tasks.values())
            self._write_tasks_file(tasks_file, remaining_tasks)

            attempt_merge_totals = _merge_stage_dir_if_needed(self.main_db_path, stage_dir, active_freqs)
            for freq, row_count in attempt_merge_totals.items():
                merge_totals[freq] += row_count

            if not remaining_tasks:
                break

            if completed_tasks == total_tasks:
                logger.warning(
                    "任务已完成但 pending_tasks 仍有 %d 个任务，可能是任务合并导致，继续执行",
                    len(remaining_tasks),
                )
                break

            if stage_dir.exists():
                shutil.rmtree(stage_dir)
            stage_dir.mkdir(parents=True, exist_ok=True)

            if stall_error is None:
                raise RuntimeError("批次未完成但未检测到可重试的停滞原因")
            if attempt >= max(1, settings.batch_stall_retry_times):
                raise stall_error

            logger.warning(
                "任务池停滞后重试 next_attempt=%d/%d remaining=%d",
                attempt + 1,
                max(1, settings.batch_stall_retry_times),
                len(remaining_tasks),
            )

        if stage_dir.exists():
            shutil.rmtree(stage_dir)

        for freq, row_count in merge_totals.items():
            if row_count <= 0:
                continue
            logger.info(
                "task pool ok freq=%s merged=%d",
                freq,
                row_count,
            )
        return merge_totals

    def run_pending_tasks(
        self,
        tasks_file: Path = TASKS_FILE,
        stage_root: Path = STAGE_ROOT,
        main_db_path: Path | None = None,
    ) -> dict[str, int]:
        """
        中文说明：执行同级目录任务 JSON 中的全部待处理任务。

        输入：任务文件路径、阶段库根目录和可选主库路径。
        输出：每个频率写入主库的行数统计。
        用途：作为 get_bs_data 的内部主流程，负责恢复、并发执行、回写剩余任务和清理资源。
        边界条件：任务文件缺失、为空或结构非法时直接抛出异常。
        """
        self.main_db_path = (main_db_path or settings.db_file).resolve()
        init_db(self.main_db_path)

        logger.info("main db path=%s", self.main_db_path)
        run_stage_dir = stage_root / "active"

        try:
            tasks = self._load_required_tasks_file(tasks_file)
            normalized_tasks = _normalize_merge_and_dedupe_tasks(tasks)
            self._write_tasks_file(tasks_file, normalized_tasks)
            requested_freqs = sorted({task["freq"] for task in normalized_tasks}, key=lambda freq: list(_FREQ_TABLE).index(freq))

            self._recover_stage_root(stage_root, requested_freqs)
            logger.info("开始处理 %d 个任务 freqs=%s", len(normalized_tasks), requested_freqs)
            final_totals = self._run_task_pool(normalized_tasks, tasks_file, run_stage_dir)

            remaining_tasks = self._load_tasks_file(tasks_file)
            if not remaining_tasks:
                self._delete_tasks_file(tasks_file)

            self._cleanup_stage_root(stage_root)

            for freq in requested_freqs:
                logger.info("update ok freq=%s rows=%d", freq, final_totals.get(freq, 0))
            return {freq: final_totals.get(freq, 0) for freq in requested_freqs}

        except Exception as exc:
            logger.exception("run pending tasks failed: %s", exc)
            raise
        finally:
            self.bs.logout()

    def _delete_tasks_file(self, path: Path) -> None:
        """
        中文说明：删除任务文件及其备份。

        输入：主任务文件路径。
        输出：无返回值。
        用途：在所有任务完成后清理同级目录中的待执行状态。
        边界条件：文件不存在时静默跳过。
        """
        backup_path = self._tasks_file_backup_path(path)
        if path.exists():
            path.unlink()
        if backup_path.exists():
            backup_path.unlink()

    def _tasks_file_backup_path(self, path: Path) -> Path:
        """
        返回任务文件备份路径。

        输入：path 为主任务文件路径。
        输出：与主任务文件同目录的备份文件路径。
        用途：在主任务文件损坏时提供回退来源。
        边界条件：仅做路径计算，不校验文件是否存在。
        """
        return path.with_name(f"{path.name}.bak")

    def _validate_tasks_payload(self, data: object, source: Path) -> list[dict]:
        """
        校验任务文件内容结构。

        输入：data 为 JSON 反序列化结果，source 为来源文件路径。
        输出：结构合法的任务列表。
        用途：避免把损坏或错误格式的任务文件当成空任务处理。
        边界条件：当结构非法时抛出异常，调用方决定是否回退备份或终止。
        """
        if not isinstance(data, list):
            raise RuntimeError(f"tasks file is not a list: {source}")
        validated_tasks: list[dict] = []
        for index, item in enumerate(data):
            try:
                validated_tasks.append(_normalize_task(item))
            except Exception as exc:
                raise RuntimeError(f"tasks file item is invalid: {source} index={index} error={exc}") from exc
        return validated_tasks

    def _read_tasks_file_strict(self, path: Path) -> list[dict]:
        """
        严格读取并校验任务文件。

        输入：path 为任务文件路径。
        输出：校验通过的任务列表。
        用途：统一封装任务文件读取与结构检查。
        边界条件：文件不存在或内容非法时抛出异常。
        """
        if not path.exists():
            raise FileNotFoundError(path)
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        return self._validate_tasks_payload(data, path)

    def _write_tasks_file(self, path: Path, tasks: list[dict]) -> None:
        """
        原子写入任务文件并同步更新备份。

        输入：path 为主任务文件路径，tasks 为待写入任务列表。
        输出：无返回值。
        用途：持久化剩余任务，降低中断后任务文件损坏导致漏数的风险。
        边界条件：主文件替换失败时回退为直接写入；备份写入失败只记录日志，不影响主流程。
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(tasks, ensure_ascii=True)
        backup_path = self._tasks_file_backup_path(path)
        tmp_name = f"{path.name}.{os.getpid()}.tmp"
        tmp_path = path.with_name(tmp_name)
        tmp_path.write_text(payload, encoding="utf-8")
        for attempt in range(3):
            try:
                os.replace(tmp_path, path)
                try:
                    backup_path.write_text(payload, encoding="utf-8")
                except Exception as exc:
                    logger.warning("failed to write tasks backup %s: %s", backup_path, exc)
                return
            except PermissionError as exc:
                logger.warning("tasks file replace blocked, retrying: %s", exc)
                time.sleep(0.2 * (attempt + 1))
        path.write_text(payload, encoding="utf-8")
        try:
            backup_path.write_text(payload, encoding="utf-8")
        except Exception as exc:
            logger.warning("failed to write tasks backup %s: %s", backup_path, exc)
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except PermissionError:
                pass

    def _load_tasks_file(self, path: Path) -> list[dict]:
        """
        读取任务文件，必要时回退到备份。

        输入：path 为主任务文件路径。
        输出：待处理任务列表。
        用途：在主任务文件损坏时优先使用备份恢复，避免误判为无任务。
        边界条件：主文件与备份都损坏时抛出异常，阻止流程继续运行。
        """
        if not path.exists():
            return []
        try:
            return self._read_tasks_file_strict(path)
        except Exception as exc:
            backup_path = self._tasks_file_backup_path(path)
            logger.error("invalid tasks file %s: %s", path, exc)
            if backup_path.exists():
                try:
                    tasks = self._read_tasks_file_strict(backup_path)
                    logger.warning("tasks file recovered from backup %s", backup_path)
                    return tasks
                except Exception as backup_exc:
                    raise RuntimeError(
                        f"tasks file and backup are both invalid: {path}, {backup_path}"
                    ) from backup_exc
            raise RuntimeError(f"tasks file is invalid and no backup is available: {path}") from exc

    def _load_required_tasks_file(self, path: Path) -> list[dict[str, str]]:
        """
        中文说明：严格读取必须存在且非空的任务文件。

        输入：主任务文件路径。
        输出：标准化后的任务列表。
        用途：供 get_bs_data 使用，满足“缺失或为空直接报错”的需求。
        边界条件：文件不存在、空文件、空列表或结构非法时抛出异常。
        """
        if not path.exists():
            raise RuntimeError(f"tasks file does not exist: {path}")
        if not path.read_text(encoding="utf-8").strip():
            raise RuntimeError(f"tasks file is empty: {path}")
        tasks = self._load_tasks_file(path)
        if not tasks:
            raise RuntimeError(f"tasks file has no pending tasks: {path}")
        return tasks

    def append_generated_tasks(self, generated_tasks: list[dict[str, Any]], tasks_file: Path = TASKS_FILE) -> list[dict[str, str]]:
        """
        中文说明：把生成的任务写入或追加到同级任务文件。

        输入：新生成任务列表与目标任务文件路径。
        输出：本次新生成并标准化后的任务列表。
        用途：实现 make_inconsistent_tasks / make_full_tasks 的“初始化或追加去重”语义。
        边界条件：新生成任务为空时写入空集前会保留原文件；现有文件非法时直接抛出异常。
        """
        normalized_generated = _normalize_merge_and_dedupe_tasks(generated_tasks)
        existing_tasks: list[dict[str, str]] = []
        if tasks_file.exists() and tasks_file.read_text(encoding="utf-8").strip():
            existing_tasks = self._load_tasks_file(tasks_file)
        merged_tasks = _normalize_merge_and_dedupe_tasks(existing_tasks + normalized_generated)
        self._write_tasks_file(tasks_file, merged_tasks)
        return normalized_generated

def _resolve_main_db_path(main_db_path: str | Path | None) -> Path:
    """
    中文说明：解析主库路径。

    输入：可选字符串或 Path 形式的主库路径。
    输出：绝对路径形式的 DuckDB 主库路径。
    用途：统一 get_bs_data 与 make_* 的主库路径解析逻辑。
    边界条件：未传值时回退到 settings.db_file。
    """
    if main_db_path is None:
        return settings.db_file.resolve()
    return Path(main_db_path).expanduser().resolve()


def _build_range_task(code: str, freq: str, start_value: date, end_value: date) -> dict[str, str] | None:
    """
    中文说明：构造区间任务并过滤非法区间。

    输入：股票代码、周期、起止日期。
    输出：合法任务字典或 None。
    用途：统一 A/B/C 规则的任务构造逻辑。
    边界条件：起始日期晚于结束日期时返回 None。
    """
    if start_value > end_value:
        return None
    return {
        "code": code,
        "freq": _normalize_freq(freq),
        "start_date": _format_task_date(start_value),
        "end_date": _format_task_date(end_value),
    }


def _move_to_weekday_start(value: date) -> date:
    """
    中文说明：把日期推进到最近的工作日。

    输入：任意日期。
    输出：若原日期是周末则推进到下一个周一，否则原样返回。
    用途：过滤日线缺口两端纯周末的无效区间。
    边界条件：连续推进最多 2 天。
    """
    current = value
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current


def _move_to_weekday_end(value: date) -> date:
    """
    中文说明：把日期回退到最近的工作日。

    输入：任意日期。
    输出：若原日期是周末则回退到上一个周五，否则原样返回。
    用途：过滤日线缺口两端纯周末的无效区间。
    边界条件：连续回退最多 2 天。
    """
    current = value
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current


def _collect_earliest_gap_tasks(conn: duckdb.DuckDBPyConnection, earliest_value: date) -> list[dict[str, str]]:
    """
    中文说明：生成 A 规则缺口任务。

    输入：DuckDB 连接和最早应覆盖日期。
    输出：按每个 code+freq 检查后的区间任务列表。
    用途：补齐“最早数据晚于 earliest_date”的历史前段。
    边界条件：某个 code+freq 完全无数据时不会在此规则中生成任务。
    """
    earliest_text = _format_task_date(earliest_value)
    rows = conn.execute(
        """
        WITH earliest_points AS (
            SELECT 'd' AS freq, code, CAST(MIN(datetime) AS DATE) AS earliest_dt FROM klines_d GROUP BY code
            UNION ALL
            SELECT 'w' AS freq, code, CAST(MIN(datetime) AS DATE) AS earliest_dt FROM klines_w GROUP BY code
            UNION ALL
            SELECT '15' AS freq, code, CAST(MIN(datetime) AS DATE) AS earliest_dt FROM klines_15 GROUP BY code
            UNION ALL
            SELECT '30' AS freq, code, CAST(MIN(datetime) AS DATE) AS earliest_dt FROM klines_30 GROUP BY code
            UNION ALL
            SELECT '60' AS freq, code, CAST(MIN(datetime) AS DATE) AS earliest_dt FROM klines_60 GROUP BY code
        )
        SELECT freq, code, earliest_dt
        FROM earliest_points
        WHERE earliest_dt > ?
        ORDER BY code, freq
        """,
        [earliest_text],
    ).fetchall()
    tasks: list[dict[str, str]] = []
    for freq, code, earliest_dt in rows:
        task = _build_range_task(str(code), str(freq), earliest_value, earliest_dt)
        if task is not None:
            tasks.append(task)
    return tasks


def _collect_daily_gap_tasks(conn: duckdb.DuckDBPyConnection, earliest_value: date) -> list[dict[str, str]]:
    """
    中文说明：生成 B 规则日线缺口任务。

    输入：DuckDB 连接和最早应覆盖日期。
    输出：日线相邻缺口对应的任务列表。
    用途：补齐相邻日线之间除周六周日外的缺失日期区间。
    边界条件：若缺口仅包含周末则不会生成任务。
    """
    rows = conn.execute(
        """
        WITH ordered AS (
            SELECT
                code,
                CAST(datetime AS DATE) AS trade_date,
                LAG(CAST(datetime AS DATE)) OVER (PARTITION BY code ORDER BY datetime) AS prev_date
            FROM klines_d
        )
        SELECT code, prev_date, trade_date
        FROM ordered
        WHERE prev_date IS NOT NULL
          AND trade_date >= ?
          AND DATEDIFF('day', prev_date, trade_date) > 1
        """,
        [_format_task_date(earliest_value)],
    ).fetchall()
    tasks: list[dict[str, str]] = []
    for code, prev_date, trade_date in rows:
        start_value = _move_to_weekday_start(max(prev_date + timedelta(days=1), earliest_value))
        end_value = _move_to_weekday_end(trade_date - timedelta(days=1))
        task = _build_range_task(str(code), "d", start_value, end_value)
        if task is not None:
            tasks.append(task)
    return tasks


def _collect_weekly_gap_tasks(conn: duckdb.DuckDBPyConnection, earliest_value: date) -> list[dict[str, str]]:
    """
    中文说明：生成 C 规则周线缺口任务。

    输入：DuckDB 连接和最早应覆盖日期。
    输出：周线相邻间隔大于 7 天的任务列表。
    用途：补齐周线中断区间。
    边界条件：若补齐区间被 earliest_date 截断后为空，则不会生成任务。
    """
    rows = conn.execute(
        """
        WITH ordered AS (
            SELECT
                code,
                CAST(datetime AS DATE) AS trade_date,
                LAG(CAST(datetime AS DATE)) OVER (PARTITION BY code ORDER BY datetime) AS prev_date
            FROM klines_w
        )
        SELECT code, prev_date, trade_date
        FROM ordered
        WHERE prev_date IS NOT NULL
          AND trade_date >= ?
          AND DATEDIFF('day', prev_date, trade_date) > 7
        """,
        [_format_task_date(earliest_value)],
    ).fetchall()
    tasks: list[dict[str, str]] = []
    for code, prev_date, trade_date in rows:
        start_value = max(prev_date + timedelta(days=1), earliest_value)
        end_value = trade_date - timedelta(days=1)
        task = _build_range_task(str(code), "w", start_value, end_value)
        if task is not None:
            tasks.append(task)
    return tasks


def _collect_intraday_gap_tasks(conn: duckdb.DuckDBPyConnection, earliest_value: date) -> list[dict[str, str]]:
    """
    中文说明：生成 D 规则分钟周期完整性任务。

    输入：DuckDB 连接和最早应覆盖日期。
    输出：分钟周期条数为 0 或不足阈值的单日任务列表。
    用途：以 d 周期有效日期作为候选日，快速筛出分钟数据不完整的日期。
    边界条件：仅检查 d 周期已有日期，不对全量非周末日期做笛卡尔扫描。
    """
    earliest_text = _format_task_date(earliest_value)
    rows = conn.execute(
        """
        WITH daily_dates AS (
            SELECT code, CAST(datetime AS DATE) AS trade_date
            FROM klines_d
            WHERE CAST(datetime AS DATE) >= ?
            GROUP BY code, CAST(datetime AS DATE)
        ),
        minute_counts AS (
            SELECT '15' AS freq, code, CAST(datetime AS DATE) AS trade_date, COUNT(*) AS bar_count
            FROM klines_15
            WHERE CAST(datetime AS DATE) >= ?
            GROUP BY code, CAST(datetime AS DATE)
            UNION ALL
            SELECT '30' AS freq, code, CAST(datetime AS DATE) AS trade_date, COUNT(*) AS bar_count
            FROM klines_30
            WHERE CAST(datetime AS DATE) >= ?
            GROUP BY code, CAST(datetime AS DATE)
            UNION ALL
            SELECT '60' AS freq, code, CAST(datetime AS DATE) AS trade_date, COUNT(*) AS bar_count
            FROM klines_60
            WHERE CAST(datetime AS DATE) >= ?
            GROUP BY code, CAST(datetime AS DATE)
        ),
        required_freqs AS (
            SELECT '15' AS freq, 16 AS target_count
            UNION ALL
            SELECT '30' AS freq, 8 AS target_count
            UNION ALL
            SELECT '60' AS freq, 4 AS target_count
        )
        SELECT required_freqs.freq, daily_dates.code, daily_dates.trade_date
        FROM daily_dates
        CROSS JOIN required_freqs
        LEFT JOIN minute_counts
          ON minute_counts.freq = required_freqs.freq
         AND minute_counts.code = daily_dates.code
         AND minute_counts.trade_date = daily_dates.trade_date
        WHERE COALESCE(minute_counts.bar_count, 0) < required_freqs.target_count
        ORDER BY daily_dates.code, required_freqs.freq, daily_dates.trade_date
        """,
        [earliest_text, earliest_text, earliest_text, earliest_text],
    ).fetchall()
    tasks: list[dict[str, str]] = []
    current_code: str | None = None
    current_freq: str | None = None
    range_start: date | None = None
    range_end: date | None = None

    for freq, code, trade_date in rows:
        freq_text = str(freq)
        code_text = str(code)
        trade_day = trade_date
        if current_code == code_text and current_freq == freq_text and range_start is not None and range_end is not None:
            if _is_weekend_only_gap(range_end, trade_day):
                range_end = trade_day
                continue
            task = _build_range_task(code_text, freq_text, range_start, range_end)
            if task is not None:
                tasks.append(task)
        current_code = code_text
        current_freq = freq_text
        range_start = trade_day
        range_end = trade_day

    if current_code is not None and current_freq is not None and range_start is not None and range_end is not None:
        task = _build_range_task(current_code, current_freq, range_start, range_end)
        if task is not None:
            tasks.append(task)

    return tasks


def make_inconsistent_tasks(
    earliest_date: str = "2018-12-01",
    main_db_path: str | Path | None = None,
) -> list[dict[str, str]]:
    """
    中文说明：扫描 quant DuckDB 中的数据缺口并生成待补任务。

    输入：最早检查日期和可选主库路径。
    输出：本次新生成并写入或追加后的标准任务列表。
    用途：根据 A/B/C/D 四类不连续规则生成 bs_pending_tasks.json 所需任务。
    边界条件：主库不存在时会先初始化空表；若无缺口则返回空列表并保持去重写回语义。
    """
    setup_logging(settings.log_level, log_file=LOG_FILE)
    target_db = _resolve_main_db_path(main_db_path)
    earliest_value = _parse_task_date(earliest_date)
    overall_started_at = time.perf_counter()
    logger.info(
        "make_inconsistent_tasks start db=%s earliest_date=%s",
        target_db,
        _format_task_date(earliest_value),
    )
    init_db(target_db)

    conn = _connect_db(target_db, read_only=True)
    try:
        generated_tasks = []
        stage_started_at = time.perf_counter()
        earliest_gap_tasks = _collect_earliest_gap_tasks(conn, earliest_value)
        logger.info(
            "make_inconsistent_tasks A:头缺数据 count=%d elapsed=%.3fs",
            len(earliest_gap_tasks),
            time.perf_counter() - stage_started_at,
        )
        generated_tasks.extend(earliest_gap_tasks)

        stage_started_at = time.perf_counter()
        daily_gap_tasks = _collect_daily_gap_tasks(conn, earliest_value)
        logger.info(
            "make_inconsistent_tasks B：日线不连续 count=%d elapsed=%.3fs",
            len(daily_gap_tasks),
            time.perf_counter() - stage_started_at,
        )
        generated_tasks.extend(daily_gap_tasks)

        stage_started_at = time.perf_counter()
        weekly_gap_tasks = _collect_weekly_gap_tasks(conn, earliest_value)
        logger.info(
            "make_inconsistent_tasks C：周线不连续 count=%d elapsed=%.3fs",
            len(weekly_gap_tasks),
            time.perf_counter() - stage_started_at,
        )
        generated_tasks.extend(weekly_gap_tasks)

        stage_started_at = time.perf_counter()
        intraday_gap_tasks = _collect_intraday_gap_tasks(conn, earliest_value)
        logger.info(
            "make_inconsistent_tasks D：分钟缺数据 count=%d elapsed=%.3fs",
            len(intraday_gap_tasks),
            time.perf_counter() - stage_started_at,
        )
        generated_tasks.extend(intraday_gap_tasks)
    finally:
        conn.close()

    service = DataService()
    service.main_db_path = target_db
    existing_task_count = 0
    if TASKS_FILE.exists() and TASKS_FILE.read_text(encoding="utf-8").strip():
        existing_task_count = len(service._load_tasks_file(TASKS_FILE))

    append_started_at = time.perf_counter()
    appended_tasks = service.append_generated_tasks(generated_tasks, TASKS_FILE)
    final_pending_count = 0
    if TASKS_FILE.exists() and TASKS_FILE.read_text(encoding="utf-8").strip():
        final_pending_count = len(service._load_tasks_file(TASKS_FILE))
    logger.info(
        "make_inconsistent_tasks summary generated=%d appended=%d pending_before=%d pending_after=%d by_freq=%s append_elapsed=%.3fs total_elapsed=%.3fs",
        len(generated_tasks),
        len(appended_tasks),
        existing_task_count,
        final_pending_count,
        _summarize_tasks_by_freq(appended_tasks),
        time.perf_counter() - append_started_at,
        time.perf_counter() - overall_started_at,
    )
    return appended_tasks


def make_full_tasks(
    earliest_date: str = "2018-12-01",
    main_db_path: str | Path | None = None,
) -> list[dict[str, str]]:
    """
    中文说明：生成所有股票与五个周期的全量任务。

    输入：最早日期和可选主库路径。
    输出：本次新生成并写入或追加后的标准任务列表。
    用途：刷新 stocks 表后，为全部股票按笛卡尔积生成全量补数任务。
    边界条件：若 market 未返回股票列表，则返回空列表并写回去重后的任务文件。
    """
    setup_logging(settings.log_level, log_file=LOG_FILE)
    target_db = _resolve_main_db_path(main_db_path)
    earliest_value = _parse_task_date(earliest_date)
    earliest_text = _format_task_date(earliest_value)
    init_db(target_db)

    service = DataService()
    service.main_db_path = target_db
    stock_count = service.update_stocks()
    logger.info("make_full_tasks refreshed stocks=%d", stock_count)

    codes = _list_codes(target_db, offset=0, limit=10_000_000)
    today_text = _format_task_date(datetime.now().date())
    generated_tasks = [
        {
            "code": code,
            "freq": freq,
            "start_date": earliest_text,
            "end_date": today_text,
        }
        for code in codes
        for freq in _FREQ_TABLE
    ]
    appended_tasks = service.append_generated_tasks(generated_tasks, TASKS_FILE)
    logger.info("make_full_tasks generated=%d", len(appended_tasks))
    return appended_tasks


def get_bs_data(main_db_path: str | Path | None = None) -> dict[str, int]:
    """
    中文说明：读取同级任务 JSON 并执行 BaoStock 数据拉取。

    输入：可选主库路径，未传时使用 settings.db_path。
    输出：各频率写入主库的行数统计。
    用途：作为对外主入口，严格从同级目录 bs_pending_tasks.json 消费任务并在完成后清理子进程与阶段库。
    边界条件：任务文件缺失、为空或非法时直接抛出异常；运行完毕后会主动销毁创建的多进程。
    """
    setup_logging(settings.log_level, log_file=LOG_FILE)
    target_db = _resolve_main_db_path(main_db_path)
    service = DataService()
    return service.run_pending_tasks(tasks_file=TASKS_FILE, stage_root=STAGE_ROOT, main_db_path=target_db)
