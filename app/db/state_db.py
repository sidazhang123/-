"""
状态库访问层（StateDB）。

职责：
1. 管理任务状态、任务日志、筛选结果与前端配置持久化。
2. 负责 schema 初始化与版本升级策略（当前 schema_version=v6）。
3. 对外提供线程安全读写接口，供任务管理器与 API 路由调用。

设计约束：
1. 仅处理状态库 CRUD，不承载策略计算。
2. 任何字段结构调整必须同步更新 API 模型与交接文档。
"""

from __future__ import annotations

import json
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, TypeVar

import duckdb

from app.settings import LOG_KEEP_CONCEPT_JOBS, LOG_KEEP_MAINTENANCE_JOBS, LOG_KEEP_TASK_PER_TASK


def _json_dumps(obj: Any) -> str:
    """
    将任意对象序列化为 JSON 字符串。

    用途：
    1. 统一状态库存储时的 JSON 编码规则。
    边界条件：
    1. 不可直接 JSON 编码的对象会退化为 `str()` 表示，避免写库阶段因时间类型等对象失败。
    """
    return json.dumps(obj, ensure_ascii=False, default=str)


class StateDB:
    SCHEMA_VERSION = "6"
    RETRY_TASK_QUERY_BATCH_SIZE = 2000
    MONITOR_FORM_SETTINGS_KEY = "ui.monitor.form_settings.v2"
    LEGACY_MONITOR_FORM_SETTINGS_KEY = "ui.monitor.form_settings.v1"
    MAINTENANCE_FORM_SETTINGS_KEY = "ui.maintenance.form_settings.v1"

    def __init__(self, db_path: Path):
        """
        输入：
        1. db_path: 状态库 DuckDB 文件路径。
        输出：
        1. 无返回值。
        用途：
        1. 初始化读写分离连接和线程锁，为后续 schema 初始化与查询写入做准备。
        边界条件：
        1. 读连接和写连接各自串行化访问，避免共享单连接导致查询饥饿或线程安全问题。
        """
        self.db_path = db_path
        # 读写分离：避免维护写流量占用单连接导致查询接口饥饿。
        self._write_lock = threading.RLock()
        self._read_lock = threading.RLock()
        self._write_con = self._open_connection()
        self._read_con = self._open_connection()

    def _open_connection(self) -> duckdb.DuckDBPyConnection:
        """
        打开状态库连接，并对文件句柄冲突做短时重试。

        边界条件：
        1. 最多重试 40 次，每次间隔 50ms，总等待时间约 2 秒。
        2. 仅对文件被占用或句柄冲突类异常重试；其他 DuckDB 异常直接上抛。
        """
        last_exc: Exception | None = None
        for _ in range(40):
            try:
                return duckdb.connect(str(self.db_path))
            except (duckdb.IOException, duckdb.BinderException) as exc:
                last_exc = exc
                message = str(exc).lower()
                if (
                    "另一个程序正在使用此文件" not in str(exc)
                    and "already open" not in message
                    and "unique file handle conflict" not in message
                ):
                    raise
                time.sleep(0.05)
        if last_exc:
            raise last_exc
        return duckdb.connect(str(self.db_path))

    T = TypeVar("T")

    def _with_write_connection(self, fn: Callable[[duckdb.DuckDBPyConnection], T]) -> T:
        """
        输入：
        1. fn: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 使用写连接执行数据库操作。
        边界条件：
        1. 通过写锁串行化，保证连接线程安全。
        """
        with self._write_lock:
            return fn(self._write_con)

    def _with_read_connection(self, fn: Callable[[duckdb.DuckDBPyConnection], T]) -> T:
        """
        输入：
        1. fn: 接受只读连接的回调函数。
        输出：
        1. 返回回调函数执行结果。
        用途：
        1. 使用读连接执行只读查询。
        边界条件：
        1. 通过读锁串行化共享读连接的访问，避免多线程同时操作同一 DuckDB 连接。
        """
        with self._read_lock:
            return fn(self._read_con)

    def close(self) -> None:
        """
        关闭读写两条状态库连接。

        边界条件：
        1. 关闭过程对单个连接异常做吞掉处理，避免停机阶段因重复关闭中断整体清理。
        """
        with self._write_lock:
            try:
                self._write_con.close()
            except Exception:
                pass
        with self._read_lock:
            try:
                self._read_con.close()
            except Exception:
                pass

    def init_schema(self) -> None:
        """
        初始化或升级状态库 schema。

        当前目标版本：`schema_version = 6`。
        升级策略采用非破坏式迁移：优先保留任务和日志历史，再补齐新增表、索引和序列，最后更新 `app_meta`。
        """
        def _op(con: duckdb.DuckDBPyConnection) -> None:
            """
            使用写连接执行 schema 创建或升级。

            当检测到旧版本时，会先移除已废弃的旧维护元数据表，再补齐 concept 相关对象和当前索引集合。
            """
            con.execute(
                """
                create table if not exists app_meta (
                    meta_key varchar primary key,
                    meta_value varchar not null
                )
                """
            )

            row = con.execute(
                "select meta_value from app_meta where meta_key = 'schema_version'"
            ).fetchone()
            current_version = row[0] if row else None

            schema_needs_upgrade = current_version != self.SCHEMA_VERSION

            # 版本升级采用非破坏式迁移：
            # 1. 保留既有 tasks / maintenance_jobs / task_logs / task_results 数据；
            # 2. 通过 create table/index if not exists 补齐新增对象；
            # 3. 最后仅更新 app_meta.schema_version，避免升级时清空历史任务列表。
            if schema_needs_upgrade:
                con.execute("drop table if exists maintenance_no_source_days")
                con.execute("drop table if exists _maintenance_meta")

                # v5→v6: 合并 concept_jobs/concept_logs 到 maintenance_jobs/maintenance_logs
                mj_cols = [
                    str(r[0]).lower()
                    for r in con.execute(
                        "select column_name from information_schema.columns "
                        "where table_name = 'maintenance_jobs'"
                    ).fetchall()
                ]
                table_exists = len(mj_cols) > 0
                has_type_col = "type" in mj_cols
                if table_exists and not has_type_col:
                    con.execute("alter table maintenance_jobs add column type varchar default 'maintenance'")
                    con.execute("update maintenance_jobs set type = 'maintenance' where type is null")

            con.execute("create sequence if not exists task_logs_seq start 1")
            con.execute("create sequence if not exists task_results_seq start 1")
            con.execute("create sequence if not exists maintenance_logs_seq start 1")

            con.execute(
                """
                create table if not exists tasks (
                    task_id varchar primary key,
                    created_at timestamp not null,
                    updated_at timestamp not null,
                    started_at timestamp,
                    finished_at timestamp,
                    status varchar not null,
                    progress double default 0,
                    total_stocks integer default 0,
                    processed_stocks integer default 0,
                    result_count integer default 0,
                    info_log_count integer default 0,
                    error_log_count integer default 0,
                    current_code varchar,
                    source_db varchar not null,
                    start_ts timestamp,
                    end_ts timestamp,
                    strategy_group_id varchar not null,
                    strategy_name varchar not null,
                    strategy_description varchar not null,
                    params_json varchar,
                    summary_json varchar,
                    error_message varchar
                )
                """
            )

            con.execute(
                """
                create table if not exists task_logs (
                    log_id bigint default nextval('task_logs_seq'),
                    task_id varchar not null,
                    ts timestamp not null,
                    level varchar not null,
                    message varchar not null,
                    detail_json varchar
                )
                """
            )

            con.execute(
                """
                create table if not exists task_results (
                    result_id bigint default nextval('task_results_seq'),
                    task_id varchar not null,
                    code varchar not null,
                    name varchar,
                    signal_dt timestamp not null,
                    clock_tf varchar not null,
                    strategy_group_id varchar not null,
                    strategy_name varchar not null,
                    signal_label varchar not null,
                    rule_payload_json varchar,
                    created_at timestamp not null
                )
                """
            )

            con.execute(
                """
                create table if not exists maintenance_jobs (
                    job_id varchar primary key,
                    created_at timestamp not null,
                    updated_at timestamp not null,
                    started_at timestamp,
                    finished_at timestamp,
                    status varchar not null,
                    phase varchar,
                    progress double default 0,
                    source_db varchar not null,
                    mode varchar,
                    params_json varchar,
                    summary_json varchar,
                    error_message varchar,
                    type varchar not null default 'maintenance'
                )
                """
            )

            con.execute(
                """
                create table if not exists maintenance_logs (
                    log_id bigint primary key default nextval('maintenance_logs_seq'),
                    job_id varchar not null,
                    ts timestamp not null,
                    level varchar not null,
                    message varchar not null,
                    detail_json varchar
                )
                """
            )
            con.execute("create index if not exists idx_task_logs_task_id_log_id on task_logs(task_id, log_id)")
            maintenance_log_pk_exists = bool(
                con.execute(
                    """
                    select 1
                    from information_schema.table_constraints tc
                    join information_schema.key_column_usage kcu
                      on tc.constraint_name = kcu.constraint_name
                     and tc.table_name = kcu.table_name
                    where tc.table_name = 'maintenance_logs'
                      and tc.constraint_type = 'PRIMARY KEY'
                      and kcu.column_name = 'log_id'
                    limit 1
                    """
                ).fetchone()
            )
            if not maintenance_log_pk_exists:
                con.execute("drop index if exists idx_maintenance_logs_job_id_log_id")
                con.execute("alter table maintenance_logs add primary key (log_id)")

            con.execute(
                "create index if not exists idx_maintenance_logs_job_id_log_id on maintenance_logs(job_id, log_id)"
            )
            con.execute("create index if not exists idx_tasks_created_at on tasks(created_at)")
            con.execute("create index if not exists idx_maintenance_jobs_created_at on maintenance_jobs(created_at)")

            # legacy cleanup:
            # 1. 迁移 concept_jobs/concept_logs 遗留数据到 maintenance_*；
            # 2. 删除旧 concept_* 表，避免新版本继续保留空壳表。
            legacy_tables = {
                str(row[0]).lower()
                for row in con.execute(
                    "select table_name from information_schema.tables where table_name in ('concept_jobs', 'concept_logs')"
                ).fetchall()
            }
            current_mj_cols = {
                str(row[0]).lower()
                for row in con.execute(
                    "select column_name from information_schema.columns where table_name = 'maintenance_jobs'"
                ).fetchall()
            }
            if legacy_tables and "type" in current_mj_cols:
                if "concept_jobs" in legacy_tables:
                    con.execute(
                        """
                        insert into maintenance_jobs (
                            job_id, created_at, updated_at, started_at, finished_at,
                            status, phase, progress, source_db, mode, params_json,
                            summary_json, error_message, type
                        )
                        select
                            job_id, created_at, updated_at, started_at, finished_at,
                            status, phase, progress, source_db, 'concept_update', params_json,
                            summary_json, error_message, 'concept'
                        from concept_jobs
                        where job_id not in (select job_id from maintenance_jobs)
                        """
                    )
                if "concept_logs" in legacy_tables:
                    con.execute(
                        """
                        insert into maintenance_logs (job_id, ts, level, message, detail_json)
                        select job_id, ts, level, message, detail_json
                        from concept_logs
                        where job_id in (
                            select job_id from maintenance_jobs where type = 'concept'
                        )
                        """
                    )
                con.execute("drop table if exists concept_logs")
                con.execute("drop table if exists concept_jobs")
                try:
                    con.execute("drop sequence if exists concept_logs_seq")
                except Exception:
                    pass

            con.execute(
                """
                create table if not exists _maintenance_meta (
                    key varchar primary key,
                    value varchar not null,
                    updated_at timestamp not null
                )
                """
            )

            con.execute(
                """
                create table if not exists maintenance_retry_tasks (
                    task_key varchar primary key,
                    mode varchar not null,
                    code varchar not null,
                    freq varchar not null,
                    start_date date not null,
                    end_date date not null,
                    attempt_count integer not null default 0,
                    last_status varchar not null default 'pending',
                    last_error varchar,
                    created_at timestamp not null,
                    updated_at timestamp not null
                )
                """
            )
            con.execute(
                "create index if not exists idx_maintenance_retry_tasks_mode_status on maintenance_retry_tasks(mode, last_status)"
            )

            con.execute(
                """
                insert into app_meta (meta_key, meta_value)
                values ('schema_version', ?)
                on conflict (meta_key) do update set meta_value = excluded.meta_value
                """,
                [self.SCHEMA_VERSION],
            )

        self._with_write_connection(_op)

    def get_meta_value(self, meta_key: str) -> str | None:
        """
        输入：
        1. meta_key: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_meta_value` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        def _op(con: duckdb.DuckDBPyConnection):
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            row = con.execute(
                "select meta_value from app_meta where meta_key = ?",
                [meta_key],
            ).fetchone()
            return row[0] if row else None

        return self._with_read_connection(_op)

    def set_meta_value(self, meta_key: str, meta_value: str) -> None:
        """
        输入：
        1. meta_key: 输入参数，具体约束以调用方和实现为准。
        2. meta_value: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `set_meta_value` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        def _op(con: duckdb.DuckDBPyConnection) -> None:
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            con.execute(
                """
                insert into app_meta (meta_key, meta_value)
                values (?, ?)
                on conflict (meta_key) do update set meta_value = excluded.meta_value
                """,
                [meta_key, meta_value],
            )

        self._with_write_connection(_op)

    def get_meta_json(self, meta_key: str, default: Any = None) -> Any:
        """
        输入：
        1. meta_key: 输入参数，具体约束以调用方和实现为准。
        2. default: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_meta_json` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        raw = self.get_meta_value(meta_key)
        if raw is None:
            return default
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return default

    def set_meta_json(self, meta_key: str, value: Any) -> None:
        """
        输入：
        1. meta_key: 输入参数，具体约束以调用方和实现为准。
        2. value: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `set_meta_json` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        self.set_meta_value(meta_key, _json_dumps(value))

    def get_monitor_form_settings(self) -> dict[str, Any]:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_monitor_form_settings` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        value = self.get_meta_json(self.MONITOR_FORM_SETTINGS_KEY, default=None)
        if value is None:
            value = self.get_meta_json(self.LEGACY_MONITOR_FORM_SETTINGS_KEY, default={})
        if isinstance(value, dict):
            return value
        return {}

    def set_monitor_form_settings(self, settings: dict[str, Any]) -> None:
        """
        输入：
        1. settings: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `set_monitor_form_settings` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        self.set_meta_json(self.MONITOR_FORM_SETTINGS_KEY, settings)

    def get_maintenance_form_settings(self) -> dict[str, Any]:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_maintenance_form_settings` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        value = self.get_meta_json(self.MAINTENANCE_FORM_SETTINGS_KEY, default={})
        if isinstance(value, dict):
            return value
        return {}

    def set_maintenance_form_settings(self, settings: dict[str, Any]) -> None:
        """
        输入：
        1. settings: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `set_maintenance_form_settings` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        self.set_meta_json(self.MAINTENANCE_FORM_SETTINGS_KEY, settings)

    def get_maintenance_retry_tasks(
        self,
        task_keys: Iterable[str],
        *,
        query_batch_size: int | None = None,
    ) -> dict[str, dict[str, Any]]:
        """
        输入：
        1. task_keys: 任务键列表。
        输出：
        1. task_key -> retry 记录映射。
        用途：
        1. 历史维护任务执行前读取已存在任务状态。
        边界条件：
        1. 空输入返回空字典。
        """

        keys = [str(item or "").strip() for item in task_keys if str(item or "").strip()]
        if not keys:
            return {}

        def _op(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
            return con.execute(
                """
                select
                    m.task_key,
                    m.mode,
                    m.code,
                    m.freq,
                    m.start_date,
                    m.end_date,
                    m.attempt_count,
                    m.last_status,
                    m.last_error,
                    m.created_at,
                    m.updated_at
                from maintenance_retry_tasks m
                where m.task_key in (select unnest($keys::varchar[]))
                """,
                {"keys": keys},
            ).fetchall()

        rows = self._with_read_connection(_op)
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            result[str(row[0])] = {
                "task_key": row[0],
                "mode": row[1],
                "code": row[2],
                "freq": row[3],
                "start_date": row[4],
                "end_date": row[5],
                "attempt_count": int(row[6] or 0),
                "last_status": row[7],
                "last_error": row[8],
                "created_at": row[9],
                "updated_at": row[10],
            }
        return result

    def insert_maintenance_retry_tasks(self, rows: list[dict[str, Any]]) -> int:
        """
        输入：
        1. rows: 待插入任务列表。
        输出：
        1. 实际写入条数。
        用途：
        1. 首次发现历史缺口任务时批量落库。
        边界条件：
        1. 已存在 task_key 会被忽略。
        """

        payload: list[tuple[Any, ...]] = []
        seen_task_keys: set[str] = set()
        now = datetime.now()
        for item in rows:
            if not isinstance(item, dict):
                continue
            task_key = str(item.get("task_key") or "").strip()
            mode = str(item.get("mode") or "").strip()
            code = str(item.get("code") or "").strip()
            freq = str(item.get("freq") or "").strip()
            start_date = str(item.get("start_date") or "").strip()
            end_date = str(item.get("end_date") or "").strip()
            if not task_key or not mode or not code or not freq or not start_date or not end_date:
                continue
            if task_key in seen_task_keys:
                continue
            seen_task_keys.add(task_key)
            payload.append(
                (
                    task_key,
                    mode,
                    code,
                    freq,
                    start_date,
                    end_date,
                    int(item.get("attempt_count") or 0),
                    str(item.get("last_status") or "pending"),
                    item.get("last_error"),
                    now,
                    now,
                )
            )
        if not payload:
            return 0

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            """
            输入：
            1. con: 状态库写连接。
            输出：
            1. 无返回值。
            用途：
            1. 通过 unnest 向量化批量插入 retry 任务。
            边界条件：
            1. 已存在 task_key 继续保持忽略语义。
            """
            cols_task_key = [r[0] for r in payload]
            cols_mode = [r[1] for r in payload]
            cols_code = [r[2] for r in payload]
            cols_freq = [r[3] for r in payload]
            cols_start_date = [r[4] for r in payload]
            cols_end_date = [r[5] for r in payload]
            cols_attempt_count = [r[6] for r in payload]
            cols_last_status = [r[7] for r in payload]
            cols_last_error = [r[8] for r in payload]
            cols_created_at = [r[9] for r in payload]
            cols_updated_at = [r[10] for r in payload]
            con.execute(
                """
                insert into maintenance_retry_tasks(
                    task_key, mode, code, freq, start_date, end_date,
                    attempt_count, last_status, last_error, created_at, updated_at
                )
                select
                    src.task_key,
                    src.mode,
                    src.code,
                    src.freq,
                    src.start_date,
                    src.end_date,
                    src.attempt_count,
                    src.last_status,
                    src.last_error,
                    src.created_at,
                    src.updated_at
                from (
                    select
                        unnest($task_key::varchar[]) as task_key,
                        unnest($mode::varchar[]) as mode,
                        unnest($code::varchar[]) as code,
                        unnest($freq::varchar[]) as freq,
                        unnest($start_date::varchar[]) as start_date,
                        unnest($end_date::varchar[]) as end_date,
                        unnest($attempt_count::integer[]) as attempt_count,
                        unnest($last_status::varchar[]) as last_status,
                        unnest($last_error::varchar[]) as last_error,
                        unnest($created_at::timestamp[]) as created_at,
                        unnest($updated_at::timestamp[]) as updated_at
                ) src
                where not exists (
                    select 1
                    from maintenance_retry_tasks target
                    where target.task_key = src.task_key
                )
                """,
                {
                    "task_key": cols_task_key,
                    "mode": cols_mode,
                    "code": cols_code,
                    "freq": cols_freq,
                    "start_date": cols_start_date,
                    "end_date": cols_end_date,
                    "attempt_count": cols_attempt_count,
                    "last_status": cols_last_status,
                    "last_error": cols_last_error,
                    "created_at": cols_created_at,
                    "updated_at": cols_updated_at,
                },
            )

        self._with_write_connection(_op)
        return len(payload)

    def update_maintenance_retry_task(self, *, task_key: str, **fields: Any) -> None:
        """
        输入：
        1. task_key: 任务键。
        2. fields: 需要更新的字段。
        输出：
        1. 无返回值。
        用途：
        1. 更新 retry 任务状态与尝试次数。
        边界条件：
        1. fields 为空时不执行更新。
        """

        if not fields:
            return
        key = str(task_key or "").strip()
        if not key:
            return
        fields["updated_at"] = datetime.now()
        set_clause = ", ".join(f"{name} = ?" for name in fields.keys())
        values = list(fields.values()) + [key]

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            con.execute(f"update maintenance_retry_tasks set {set_clause} where task_key = ?", values)

        self._with_write_connection(_op)

    def update_maintenance_retry_tasks(self, rows: Iterable[dict[str, Any]]) -> int:
        """
        输入：
        1. rows: 批量更新数据，支持 attempt_count/last_status/last_error。
        输出：
        1. 实际更新条数。
        用途：
        1. 减少 historical_backfill 步骤4的逐条 update 开销。
        边界条件：
        1. 非法行会被跳过。
        2. 未提供 attempt_count 时保留现有值不变。
        """

        now = datetime.now()
        deduped_by_task_key: dict[str, tuple[Any, ...]] = {}
        for item in rows:
            if not isinstance(item, dict):
                continue
            task_key = str(item.get("task_key") or "").strip()
            if not task_key:
                continue
            raw_attempt = item.get("attempt_count")
            attempt_count = int(raw_attempt) if raw_attempt is not None else None
            deduped_by_task_key[task_key] = (
                (
                    attempt_count,
                    str(item.get("last_status") or "pending"),
                    item.get("last_error"),
                    now,
                    task_key,
                )
            )

        payload = list(deduped_by_task_key.values())

        if not payload:
            return 0

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            cols_attempt_count = [r[0] for r in payload]
            cols_last_status = [r[1] for r in payload]
            cols_last_error = [r[2] for r in payload]
            cols_updated_at = [r[3] for r in payload]
            cols_task_key = [r[4] for r in payload]
            con.execute(
                """
                update maintenance_retry_tasks as target
                set
                    attempt_count = coalesce(src.attempt_count, target.attempt_count),
                    last_status = src.last_status,
                    last_error = src.last_error,
                    updated_at = src.updated_at
                from (
                    select
                        unnest($attempt_count::integer[]) as attempt_count,
                        unnest($last_status::varchar[]) as last_status,
                        unnest($last_error::varchar[]) as last_error,
                        unnest($updated_at::timestamp[]) as updated_at,
                        unnest($task_key::varchar[]) as task_key
                ) as src
                where target.task_key = src.task_key
                """,
                {
                    "attempt_count": cols_attempt_count,
                    "last_status": cols_last_status,
                    "last_error": cols_last_error,
                    "updated_at": cols_updated_at,
                    "task_key": cols_task_key,
                },
            )

        self._with_write_connection(_op)
        return len(payload)

    def delete_maintenance_retry_tasks(
        self,
        task_keys: Iterable[str],
        *,
        batch_size: int = 50_000,
    ) -> int:
        """
        输入：
        1. task_keys: 任务键列表。
        2. batch_size: 每批删除的键数量上限。
        输出：
        1. 删除条数。
        用途：
        1. 历史维护任务成功后清理 retry 记录。
        边界条件：
        1. 空输入返回 0。
        2. 按批次执行避免单条超大 unnest SQL。
        """

        keys = [str(item or "").strip() for item in task_keys if str(item or "").strip()]
        if not keys:
            return 0

        deleted = 0
        for offset in range(0, len(keys), batch_size):
            batch = keys[offset : offset + batch_size]

            def _op(con: duckdb.DuckDBPyConnection, _batch: list[str] = batch) -> int:
                con.execute(
                    """
                    delete from maintenance_retry_tasks
                    where task_key in (select unnest($keys::varchar[]))
                    """,
                    {"keys": _batch},
                )
                return len(_batch)

            deleted += int(self._with_write_connection(_op) or 0)

        return deleted

    def bulk_update_retry_task_status(
        self,
        task_keys: Iterable[str],
        *,
        last_status: str,
        last_error: str | None,
        batch_size: int = 50_000,
        on_batch: Callable[[int, int], None] | None = None,
    ) -> int:
        """
        输入：
        1. task_keys: 任务键可迭代对象。
        2. last_status: 统一写入的状态值。
        3. last_error: 统一写入的错误描述。
        4. batch_size: 每批处理的键数量上限。
        5. on_batch: 每批完成后的回调，参数为 (已处理数, 总数)。
        输出：
        1. 实际更新条数。
        用途：
        1. 高效批量更新 retry 任务的 last_status/last_error，适用于百万级 no_data 等均一结果场景。
        边界条件：
        1. 不修改 attempt_count（由 pre-execution 阶段负责）。
        2. 每批独立事务，避免单条巨型 SQL 导致内存溢出。
        """

        keys = [str(item or "").strip() for item in task_keys if str(item or "").strip()]
        if not keys:
            return 0

        total = len(keys)
        updated = 0
        now = datetime.now()

        for offset in range(0, total, batch_size):
            batch = keys[offset : offset + batch_size]

            def _op(con: duckdb.DuckDBPyConnection, _batch: list[str] = batch) -> None:
                con.execute(
                    """
                    update maintenance_retry_tasks
                    set last_status = $status,
                        last_error = $error,
                        updated_at = $now
                    where task_key in (select unnest($keys::varchar[]))
                    """,
                    {
                        "status": last_status,
                        "error": last_error,
                        "now": now,
                        "keys": _batch,
                    },
                )

            self._with_write_connection(_op)
            updated += len(batch)
            if on_batch is not None:
                on_batch(updated, total)

        return updated

    def get_maintenance_meta(self, key: str) -> str | None:
        """
        输入：
        1. key: 维护元数据键。
        输出：
        1. 命中时返回字符串值；不存在返回 None。
        用途：
        1. 从状态库读取维护元数据。
        边界条件：
        1. key 为空时会按空字符串查询，通常应由上层保证非空。
        """

        def _op(con: duckdb.DuckDBPyConnection) -> str | None:
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            row = con.execute(
                "select value from _maintenance_meta where key = ?",
                [str(key)],
            ).fetchone()
            return str(row[0]) if row and row[0] is not None else None

        return self._with_read_connection(_op)

    def set_maintenance_meta(self, key: str, value: str) -> None:
        """
        输入：
        1. key: 维护元数据键。
        2. value: 元数据值。
        输出：
        1. 无返回值。
        用途：
        1. 写入状态库维护元数据。
        边界条件：
        1. 同键会覆盖更新并刷新 updated_at。
        """

        now = datetime.now()

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            con.execute(
                """
                insert into _maintenance_meta(key, value, updated_at)
                values (?, ?, ?)
                on conflict(key) do update set
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                [str(key), str(value), now],
            )

        self._with_write_connection(_op)

    def create_task(
        self,
        *,
        task_id: str,
        source_db: str,
        start_ts: datetime | None,
        end_ts: datetime | None,
        strategy_group_id: str,
        strategy_name: str,
        strategy_description: str,
        params: dict[str, Any],
    ) -> None:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        2. source_db: 输入参数，具体约束以调用方和实现为准。
        3. start_ts: 输入参数，具体约束以调用方和实现为准。
        4. end_ts: 输入参数，具体约束以调用方和实现为准。
        5. strategy_group_id: 输入参数，具体约束以调用方和实现为准。
        6. strategy_name: 输入参数，具体约束以调用方和实现为准。
        7. strategy_description: 输入参数，具体约束以调用方和实现为准。
        8. params: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `create_task` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        now = datetime.now()

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            con.execute(
                """
                insert into tasks (
                    task_id, created_at, updated_at, status, source_db, start_ts, end_ts,
                    strategy_group_id, strategy_name, strategy_description, params_json
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    task_id,
                    now,
                    now,
                    "queued",
                    source_db,
                    start_ts,
                    end_ts,
                    strategy_group_id,
                    strategy_name,
                    strategy_description,
                    _json_dumps(params),
                ],
            )

        self._with_write_connection(_op)

    def update_task_fields(self, task_id: str, **fields: Any) -> None:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        2. **fields: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `update_task_fields` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        if not fields:
            return
        fields["updated_at"] = datetime.now()
        set_clause = ", ".join(f"{k} = ?" for k in fields.keys())
        values = list(fields.values()) + [task_id]

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            con.execute(f"update tasks set {set_clause} where task_id = ?", values)

        self._with_write_connection(_op)

    def create_maintenance_job(
        self,
        *,
        job_id: str,
        source_db: str,
        mode: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        """创建数据维护任务主记录。"""

        now = datetime.now()
        params_json = _json_dumps(params or {})

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            con.execute(
                """
                insert into maintenance_jobs (
                    job_id, created_at, updated_at, status, source_db, mode, params_json
                )
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                [job_id, now, now, "queued", source_db, mode, params_json],
            )
            self._trim_maintenance_logs(con)

        self._with_write_connection(_op)

    def create_concept_job(
        self,
        *,
        job_id: str,
        source_db: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        """创建概念更新任务（存入 maintenance_jobs，type='concept'）。"""

        now = datetime.now()
        params_json = _json_dumps(params or {})

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            con.execute(
                """
                insert into maintenance_jobs (
                    job_id, created_at, updated_at, status, source_db, mode, params_json, type
                )
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [job_id, now, now, "queued", source_db, "concept_update", params_json, "concept"],
            )
            self._trim_maintenance_logs(con, job_type="concept")

        self._with_write_connection(_op)

    def update_maintenance_job_fields(self, job_id: str, **fields: Any) -> None:
        """更新数据维护任务字段。"""

        if not fields:
            return
        fields["updated_at"] = datetime.now()
        set_clause = ", ".join(f"{k} = ?" for k in fields.keys())
        values = list(fields.values()) + [job_id]

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            con.execute(f"update maintenance_jobs set {set_clause} where job_id = ?", values)

        self._with_write_connection(_op)

    def update_concept_job_fields(self, job_id: str, **fields: Any) -> None:
        """更新概念任务字段（同一张 maintenance_jobs 表）。"""
        self.update_maintenance_job_fields(job_id, **fields)

    def append_maintenance_log(
        self,
        *,
        job_id: str,
        level: str,
        message: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        """写入数据维护任务日志。"""

        now = datetime.now()
        detail_json = _json_dumps(detail) if detail else None

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            con.execute(
                """
                insert into maintenance_logs (job_id, ts, level, message, detail_json)
                values (?, ?, ?, ?, ?)
                """,
                [job_id, now, level, message, detail_json],
            )

        self._with_write_connection(_op)

    def append_concept_log(
        self,
        *,
        job_id: str,
        level: str,
        message: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        """写入概念任务日志（同一张 maintenance_logs 表）。"""
        self.append_maintenance_log(job_id=job_id, level=level, message=message, detail=detail)

    def append_log(
        self,
        *,
        task_id: str,
        level: str,
        message: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        2. level: 输入参数，具体约束以调用方和实现为准。
        3. message: 输入参数，具体约束以调用方和实现为准。
        4. detail: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `append_log` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        now = datetime.now()
        detail_json = _json_dumps(detail) if detail else None

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            con.execute(
                """
                insert into task_logs (task_id, ts, level, message, detail_json)
                values (?, ?, ?, ?, ?)
                """,
                [task_id, now, level, message, detail_json],
            )
            if level == "error":
                con.execute(
                    "update tasks set error_log_count = error_log_count + 1, updated_at = ? where task_id = ?",
                    [now, task_id],
                )
            else:
                con.execute(
                    "update tasks set info_log_count = info_log_count + 1, updated_at = ? where task_id = ?",
                    [now, task_id],
                )
            self._trim_task_logs(con, task_id=task_id, now=now)

        self._with_write_connection(_op)

    def _trim_task_logs(self, con: duckdb.DuckDBPyConnection, *, task_id: str, now: datetime) -> None:
        """
        输入：
        1. con: 写连接。
        2. task_id: 任务 ID。
        3. now: 当前时间，用于回写 tasks.updated_at。
        输出：
        1. 无返回值。
        用途：
        1. 按配置上限裁剪单任务日志条数，并同步 info/error 计数。
        边界条件：
        1. 上限小于等于 0 时不裁剪。
        2. 仅删除最旧日志，保留最新窗口。
        """

        keep_limit = int(LOG_KEEP_TASK_PER_TASK or 0)
        if keep_limit <= 0:
            return
        counts_row = con.execute(
            """
            select info_log_count, error_log_count
            from tasks
            where task_id = ?
            """,
            [task_id],
        ).fetchone()
        if not counts_row:
            return
        total_logs = int(counts_row[0] or 0) + int(counts_row[1] or 0)
        overflow = total_logs - keep_limit
        if overflow <= 0:
            return

        removed_rows = con.execute(
            """
            select level, count(*) as cnt
            from (
                select level
                from task_logs
                where task_id = ?
                order by log_id asc
                limit ?
            ) as removed
            group by level
            """,
            [task_id, overflow],
        ).fetchall()
        removed_info = 0
        removed_error = 0
        for level_value, count_value in removed_rows:
            token = str(level_value or "").strip().lower()
            if token == "error":
                removed_error += int(count_value or 0)
            elif token == "info":
                removed_info += int(count_value or 0)

        con.execute(
            """
            delete from task_logs
            where log_id in (
                select log_id
                from task_logs
                where task_id = ?
                order by log_id asc
                limit ?
            )
            """,
            [task_id, overflow],
        )
        if removed_info <= 0 and removed_error <= 0:
            return
        con.execute(
            """
            update tasks
            set
                info_log_count = case
                    when info_log_count > ? then info_log_count - ?
                    else 0
                end,
                error_log_count = case
                    when error_log_count > ? then error_log_count - ?
                    else 0
                end,
                updated_at = ?
            where task_id = ?
            """,
            [removed_info, removed_info, removed_error, removed_error, now, task_id],
        )

    def _trim_maintenance_logs(
        self, con: duckdb.DuckDBPyConnection, *, job_type: str | None = None
    ) -> None:
        """仅保留最近 N 个任务的全量日志，删除更旧任务日志。

        job_type 为 None 时按 LOG_KEEP_MAINTENANCE_JOBS 裁剪 maintenance 类型；
        job_type='concept' 时按 LOG_KEEP_CONCEPT_JOBS 裁剪 concept 类型。
        """
        if job_type == "concept":
            keep_jobs = int(LOG_KEEP_CONCEPT_JOBS or 0)
        else:
            keep_jobs = int(LOG_KEEP_MAINTENANCE_JOBS or 0)
        if keep_jobs <= 0:
            return

        type_filter = "maintenance" if job_type is None else job_type
        rows = con.execute(
            """
            select job_id
            from maintenance_jobs
            where type = ?
            order by coalesce(started_at, created_at) desc, created_at desc, job_id desc
            limit ?
            """,
            [type_filter, keep_jobs],
        ).fetchall()
        keep_job_ids = [str(row[0]) for row in rows if row and row[0]]

        # 获取该 type 下所有 job_id，用于删除多余日志
        all_rows = con.execute(
            "select job_id from maintenance_jobs where type = ?",
            [type_filter],
        ).fetchall()
        all_job_ids = [str(row[0]) for row in all_rows if row and row[0]]
        remove_job_ids = [jid for jid in all_job_ids if jid not in keep_job_ids]

        if not remove_job_ids:
            return

        placeholders = ", ".join(["?"] * len(remove_job_ids))
        con.execute(
            f"delete from maintenance_logs where job_id in ({placeholders})",
            remove_job_ids,
        )
        con.execute(
            f"delete from maintenance_jobs where job_id in ({placeholders})",
            remove_job_ids,
        )

    def _trim_concept_logs(self, con: duckdb.DuckDBPyConnection) -> None:
        """裁剪概念任务日志（委托给 _trim_maintenance_logs）。"""
        self._trim_maintenance_logs(con, job_type="concept")

    def add_result(
        self,
        *,
        task_id: str,
        code: str,
        name: str,
        signal_dt: datetime,
        clock_tf: str,
        strategy_group_id: str,
        strategy_name: str,
        signal_label: str,
        payload: dict[str, Any] | None,
    ) -> None:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        2. code: 输入参数，具体约束以调用方和实现为准。
        3. name: 输入参数，具体约束以调用方和实现为准。
        4. signal_dt: 输入参数，具体约束以调用方和实现为准。
        5. clock_tf: 输入参数，具体约束以调用方和实现为准。
        6. strategy_group_id: 输入参数，具体约束以调用方和实现为准。
        7. strategy_name: 输入参数，具体约束以调用方和实现为准。
        8. signal_label: 输入参数，具体约束以调用方和实现为准。
        9. payload: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `add_result` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        now = datetime.now()
        payload_json = _json_dumps(payload) if payload is not None else None

        def _op(con: duckdb.DuckDBPyConnection) -> None:
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            con.execute(
                """
                insert into task_results (
                    task_id, code, name, signal_dt, clock_tf,
                    strategy_group_id, strategy_name, signal_label, rule_payload_json, created_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    task_id,
                    code,
                    name,
                    signal_dt,
                    clock_tf,
                    strategy_group_id,
                    strategy_name,
                    signal_label,
                    payload_json,
                    now,
                ],
            )
            con.execute(
                "update tasks set result_count = result_count + 1, updated_at = ? where task_id = ?",
                [now, task_id],
            )

        self._with_write_connection(_op)

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_task` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        def _op(con: duckdb.DuckDBPyConnection):
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            return con.execute(
                """
                select
                    task_id, created_at, updated_at, started_at, finished_at, status,
                    progress, total_stocks, processed_stocks, result_count,
                    info_log_count, error_log_count, current_code,
                    source_db, start_ts, end_ts,
                    strategy_group_id, strategy_name, strategy_description,
                    params_json, summary_json, error_message
                from tasks
                where task_id = ?
                """,
                [task_id],
            ).fetchone()

        row = self._with_read_connection(_op)
        if not row:
            return None

        keys = [
            "task_id",
            "created_at",
            "updated_at",
            "started_at",
            "finished_at",
            "status",
            "progress",
            "total_stocks",
            "processed_stocks",
            "result_count",
            "info_log_count",
            "error_log_count",
            "current_code",
            "source_db",
            "start_ts",
            "end_ts",
            "strategy_group_id",
            "strategy_name",
            "strategy_description",
            "params_json",
            "summary_json",
            "error_message",
        ]
        result = dict(zip(keys, row))
        result["params"] = json.loads(result["params_json"]) if result.get("params_json") else {}
        result["summary"] = json.loads(result["summary_json"]) if result.get("summary_json") else {}
        result.pop("params_json", None)
        result.pop("summary_json", None)
        return result

    def get_maintenance_job(self, job_id: str) -> dict[str, Any] | None:
        """按任务 ID 读取维护任务详情。"""

        def _op(con: duckdb.DuckDBPyConnection):
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            return con.execute(
                """
                select
                    job_id, created_at, updated_at, started_at, finished_at,
                    status, phase, progress, source_db, mode, params_json, summary_json,
                    error_message, type
                from maintenance_jobs
                where job_id = ?
                """,
                [job_id],
            ).fetchone()

        row = self._with_read_connection(_op)
        if not row:
            return None

        keys = [
            "job_id",
            "created_at",
            "updated_at",
            "started_at",
            "finished_at",
            "status",
            "phase",
            "progress",
            "source_db",
            "mode",
            "params_json",
            "summary_json",
            "error_message",
            "type",
        ]
        result = dict(zip(keys, row))
        result["params"] = json.loads(result["params_json"]) if result.get("params_json") else {}
        result["summary"] = json.loads(result["summary_json"]) if result.get("summary_json") else {}
        result.pop("params_json", None)
        result.pop("summary_json", None)
        return result

    def get_concept_job(self, job_id: str) -> dict[str, Any] | None:
        """按任务 ID 读取概念更新任务详情（同一张 maintenance_jobs 表）。"""
        return self.get_maintenance_job(job_id)

    def get_logs(
        self,
        task_id: str,
        level: str,
        offset: int,
        limit: int,
        after_log_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        2. level: 输入参数，具体约束以调用方和实现为准。
        3. offset: 输入参数，具体约束以调用方和实现为准。
        4. limit: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_logs` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        params: list[Any] = [task_id]
        where = "where task_id = ?"
        if level in {"info", "error"}:
            where += " and level = ?"
            params.append(level)
        if after_log_id is not None:
            where += " and log_id > ?"
            params.append(int(after_log_id))
            page_sql = "limit ?"
            params.append(limit)
        else:
            page_sql = "limit ? offset ?"
            params.extend([limit, offset])

        def _op(con: duckdb.DuckDBPyConnection):
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            return con.execute(
                f"""
                select log_id, ts, level, message, detail_json
                from task_logs
                {where}
                order by log_id asc
                {page_sql}
                """,
                params,
            ).fetchall()

        rows = self._with_read_connection(_op)

        result: list[dict[str, Any]] = []
        for log_id, ts, level_v, message, detail_json in rows:
            result.append(
                {
                    "log_id": int(log_id),
                    "ts": ts,
                    "level": level_v,
                    "message": message,
                    "detail": json.loads(detail_json) if detail_json else None,
                }
            )
        return result

    def get_maintenance_logs(
        self,
        job_id: str,
        level: str,
        offset: int,
        limit: int,
        after_log_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """读取维护任务日志。"""

        params: list[Any] = [job_id]
        where = "where job_id = ?"
        if level in {"info", "error", "debug"}:
            where += " and level = ?"
            params.append(level)
        if after_log_id is not None:
            where += " and log_id > ?"
            params.append(int(after_log_id))
            page_sql = "limit ?"
            params.append(limit)
        else:
            page_sql = "limit ? offset ?"
            params.extend([limit, offset])

        def _op(con: duckdb.DuckDBPyConnection):
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            return con.execute(
                f"""
                select log_id, ts, level, message, detail_json
                from maintenance_logs
                {where}
                order by log_id asc
                {page_sql}
                """,
                params,
            ).fetchall()

        rows = self._with_read_connection(_op)
        result: list[dict[str, Any]] = []
        for log_id, ts, level_v, message, detail_json in rows:
            result.append(
                {
                    "log_id": int(log_id),
                    "ts": ts,
                    "level": level_v,
                    "message": message,
                    "detail": json.loads(detail_json) if detail_json else None,
                }
            )
        return result

    def get_concept_logs(
        self,
        job_id: str,
        level: str,
        offset: int,
        limit: int,
        after_log_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """读取概念任务日志（同一张 maintenance_logs 表）。"""
        return self.get_maintenance_logs(
            job_id=job_id, level=level, offset=offset, limit=limit, after_log_id=after_log_id
        )

    def get_results(self, task_id: str, offset: int, limit: int) -> list[dict[str, Any]]:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        2. offset: 输入参数，具体约束以调用方和实现为准。
        3. limit: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_results` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        def _op(con: duckdb.DuckDBPyConnection):
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            return con.execute(
                """
                select
                    code, name, signal_dt, clock_tf,
                    strategy_group_id, strategy_name, signal_label, rule_payload_json
                from task_results
                where task_id = ?
                order by signal_dt desc, code asc
                limit ? offset ?
                """,
                [task_id, limit, offset],
            ).fetchall()

        rows = self._with_read_connection(_op)

        result: list[dict[str, Any]] = []
        for code, name, signal_dt, clock_tf, strategy_group_id, strategy_name, signal_label, payload_json in rows:
            result.append(
                {
                    "code": code,
                    "name": name,
                    "signal_dt": signal_dt,
                    "clock_tf": clock_tf,
                    "strategy_group_id": strategy_group_id,
                    "strategy_name": strategy_name,
                    "signal_label": signal_label,
                    "payload": json.loads(payload_json) if payload_json else {},
                }
            )
        return result

    def list_tasks(self, offset: int, limit: int) -> list[dict[str, Any]]:
        """
        输入：
        1. offset: 输入参数，具体约束以调用方和实现为准。
        2. limit: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `list_tasks` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        def _op(con: duckdb.DuckDBPyConnection):
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            return con.execute(
                """
                select
                    task_id, created_at, started_at, finished_at, status,
                    progress, total_stocks, processed_stocks, result_count,
                    info_log_count, error_log_count, current_code,
                    strategy_group_id, strategy_name
                from tasks
                order by created_at desc, task_id desc
                limit ? offset ?
                """,
                [limit, offset],
            ).fetchall()

        rows = self._with_read_connection(_op)

        keys = [
            "task_id",
            "created_at",
            "started_at",
            "finished_at",
            "status",
            "progress",
            "total_stocks",
            "processed_stocks",
            "result_count",
            "info_log_count",
            "error_log_count",
            "current_code",
            "strategy_group_id",
            "strategy_name",
        ]
        return [dict(zip(keys, row)) for row in rows]

    def list_maintenance_jobs(
        self, offset: int, limit: int, *, job_type: str | None = None
    ) -> list[dict[str, Any]]:
        """分页读取维护任务列表。job_type 为 None 时返回所有类型。"""

        def _op(con: duckdb.DuckDBPyConnection):
            if job_type:
                return con.execute(
                    """
                    select
                        job_id, created_at, started_at, finished_at, status,
                        phase, progress, source_db, mode, error_message, type
                    from maintenance_jobs
                    where type = ?
                    order by created_at desc, job_id desc
                    limit ? offset ?
                    """,
                    [job_type, limit, offset],
                ).fetchall()
            return con.execute(
                """
                select
                    job_id, created_at, started_at, finished_at, status,
                    phase, progress, source_db, mode, error_message, type
                from maintenance_jobs
                order by created_at desc, job_id desc
                limit ? offset ?
                """,
                [limit, offset],
            ).fetchall()

        rows = self._with_read_connection(_op)
        keys = [
            "job_id",
            "created_at",
            "started_at",
            "finished_at",
            "status",
            "phase",
            "progress",
            "source_db",
            "mode",
            "error_message",
            "type",
        ]
        return [dict(zip(keys, row)) for row in rows]

    def list_concept_jobs(self, offset: int, limit: int) -> list[dict[str, Any]]:
        """分页读取概念任务列表（从 maintenance_jobs 按 type='concept' 过滤）。"""
        return self.list_maintenance_jobs(offset, limit, job_type="concept")

    def list_maintenance_jobs_by_status(
        self, statuses: Iterable[str], limit: int = 2000, *, job_type: str | None = None
    ) -> list[dict[str, Any]]:
        """按状态读取维护任务列表。job_type 为 None 时返回所有类型。"""

        status_list = [s for s in statuses if s]
        if not status_list:
            return []
        placeholders = ",".join(["?"] * len(status_list))

        def _op(con: duckdb.DuckDBPyConnection):
            type_filter = ""
            params: list[Any] = list(status_list)
            if job_type:
                type_filter = "and type = ?"
                params.append(job_type)
            params.append(limit)
            return con.execute(
                f"""
                select
                    job_id, status, created_at, updated_at, started_at, finished_at,
                    phase, progress, source_db, mode, params_json, summary_json, error_message, type
                from maintenance_jobs
                where status in ({placeholders}) {type_filter}
                order by created_at desc
                limit ?
                """,
                params,
            ).fetchall()

        rows = self._with_read_connection(_op)
        result: list[dict[str, Any]] = []
        for row in rows:
            params = json.loads(row[10]) if row[10] else {}
            summary = json.loads(row[11]) if row[11] else {}
            result.append(
                {
                    "job_id": row[0],
                    "status": row[1],
                    "created_at": row[2],
                    "updated_at": row[3],
                    "started_at": row[4],
                    "finished_at": row[5],
                    "phase": row[6],
                    "progress": row[7],
                    "source_db": row[8],
                    "mode": row[9],
                    "params": params,
                    "summary": summary,
                    "error_message": row[12],
                    "type": row[13],
                }
            )
        return result

    def list_concept_jobs_by_status(self, statuses: Iterable[str], limit: int = 2000) -> list[dict[str, Any]]:
        """按状态读取概念任务列表（从 maintenance_jobs 按 type='concept' 过滤）。"""
        return self.list_maintenance_jobs_by_status(statuses, limit, job_type="concept")

    def get_running_maintenance_job(self) -> dict[str, Any] | None:
        """返回当前进行中的维护任务（任意类型，若不存在返回 None）。"""

        jobs = self.list_maintenance_jobs_by_status(["queued", "running", "stopping"], limit=1)
        if not jobs:
            return None
        return jobs[0]

    def get_running_concept_job(self) -> dict[str, Any] | None:
        """返回当前进行中的概念任务（若不存在返回 None）。"""

        jobs = self.list_maintenance_jobs_by_status(["queued", "running", "stopping"], limit=1, job_type="concept")
        if not jobs:
            return None
        return jobs[0]

    def list_tasks_by_status(self, statuses: Iterable[str], limit: int = 2000) -> list[dict[str, Any]]:
        """
        输入：
        1. statuses: 输入参数，具体约束以调用方和实现为准。
        2. limit: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `list_tasks_by_status` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        status_list = [s for s in statuses if s]
        if not status_list:
            return []

        placeholders = ",".join(["?"] * len(status_list))

        def _op(con: duckdb.DuckDBPyConnection):
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            return con.execute(
                f"""
                select
                    task_id, status, created_at, updated_at, source_db, start_ts, end_ts,
                    strategy_group_id, strategy_name, strategy_description,
                    params_json, summary_json
                from tasks
                where status in ({placeholders})
                order by created_at desc
                limit ?
                """,
                [*status_list, limit],
            ).fetchall()

        rows = self._with_read_connection(_op)
        result: list[dict[str, Any]] = []
        for row in rows:
            params = json.loads(row[10]) if row[10] else {}
            summary = json.loads(row[11]) if row[11] else {}
            result.append(
                {
                    "task_id": row[0],
                    "status": row[1],
                    "created_at": row[2],
                    "updated_at": row[3],
                    "source_db": row[4],
                    "start_ts": row[5],
                    "end_ts": row[6],
                    "strategy_group_id": row[7],
                    "strategy_name": row[8],
                    "strategy_description": row[9],
                    "params": params,
                    "summary": summary,
                }
            )
        return result

    def get_task_recovery_snapshot(self, task_id: str) -> tuple[set[str], set[str]]:
        """
        输入：
        1. task_id: 任务 ID。
        输出：
        1. 返回 `(processed_codes, signal_code_set)` 二元组。
        用途：
        1. 在单次读连接访问中同时读取任务恢复所需的“已处理股票集合”和“已有信号股票集合”。
        边界条件：
        1. 不改变既有 completed/failed 口径与 task_results 去重口径。
        """

        task = self.get_task(task_id)
        if not task:
            return set(), set()

        summary = task.get("summary") if isinstance(task.get("summary"), dict) else {}
        resolved_codes = summary.get("resolved_codes") if isinstance(summary.get("resolved_codes"), list) else []
        processed_count = int(summary.get("processed_codes") or task.get("processed_stocks") or 0)
        processed_count = max(0, min(processed_count, len(resolved_codes)))
        processed_codes = {
            str(code)
            for code in resolved_codes[:processed_count]
            if isinstance(code, str) and code
        }

        def _op(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
            """
            输入：
            1. con: 状态库读连接。
            输出：
            1. 返回结果股票行。
            用途：
            1. 读取恢复任务所需的已命中股票集合。
            边界条件：
            1. 查询为空时返回空列表，由外层统一转为集合。
            """
            return con.execute(
                """
                select distinct code
                from task_results
                where task_id = ?
                """,
                [task_id],
            ).fetchall()

        signal_rows = self._with_read_connection(_op)
        signal_code_set = {str(row[0]) for row in signal_rows if row and row[0]}
        return processed_codes, signal_code_set

    def get_result_code_set(self, task_id: str) -> set[str]:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_result_code_set` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        _, signal_code_set = self.get_task_recovery_snapshot(task_id)
        return signal_code_set

    def get_result_stock_summaries(self, task_id: str) -> list[dict[str, Any]]:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_result_stock_summaries` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        def _op(con: duckdb.DuckDBPyConnection):
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            return con.execute(
                """
                select
                    code,
                    max(name) as name,
                    count(*) as signal_count,
                    min(signal_dt) as first_signal_dt,
                    max(signal_dt) as last_signal_dt
                from task_results
                where task_id = ?
                group by code
                order by signal_count desc, last_signal_dt desc, code asc
                """,
                [task_id],
            ).fetchall()

        rows = self._with_read_connection(_op)
        return [
            {
                "code": row[0],
                "name": row[1],
                "signal_count": int(row[2] or 0),
                "first_signal_dt": row[3],
                "last_signal_dt": row[4],
            }
            for row in rows
        ]

    def get_results_by_code(self, task_id: str, code: str) -> list[dict[str, Any]]:
        """
        输入：
        1. task_id: 输入参数，具体约束以调用方和实现为准。
        2. code: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `get_results_by_code` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        def _op(con: duckdb.DuckDBPyConnection):
            """
            输入：
            1. con: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_op` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            return con.execute(
                """
                select
                    code, name, signal_dt, clock_tf,
                    strategy_group_id, strategy_name, signal_label, rule_payload_json
                from task_results
                where task_id = ?
                  and code = ?
                order by signal_dt asc
                """,
                [task_id, code],
            ).fetchall()

        rows = self._with_read_connection(_op)
        result: list[dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "code": row[0],
                    "name": row[1],
                    "signal_dt": row[2],
                    "clock_tf": row[3],
                    "strategy_group_id": row[4],
                    "strategy_name": row[5],
                    "signal_label": row[6],
                    "payload": json.loads(row[7]) if row[7] else {},
                }
            )
        return result
