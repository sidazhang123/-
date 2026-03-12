"""
源库历史迁移服务。

职责：
1. 执行源库 schema 迁移与历史数据口径归一。
2. 支持轻量就地修复（in_place）与重建修复（rebuild）两种模式。
3. 将源库旧维护元数据迁移到 StateDB。
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Literal

import duckdb

from app.db.duckdb_utils import connect_duckdb_compatible
from app.db.state_db import StateDB
from app.settings import MAINTENANCE_ALLOWED_STOCK_PREFIXES

SOURCE_INTRADAY = "盘中"
SOURCE_POSTCLOSE = "盘后"
KLINE_NUMERIC_MIGRATION_KEY = "kline_numeric_format_v2"
LEGACY_META_IMPORT_KEY = "legacy_meta_import_v1"
KLINE_TABLE_BY_FREQ = {"15": "klines_15", "30": "klines_30", "60": "klines_60", "d": "klines_d", "w": "klines_w"}

REQUIRED_STOCK_COLUMNS = {
    "code": "VARCHAR",
    "name": "VARCHAR",
}
REQUIRED_STOCK_CONCEPT_COLUMNS = {
    "code": "VARCHAR",
    "board_name": "VARCHAR",
    "selected_reason": "VARCHAR",
    "updated_at": "TIMESTAMP",
}
REQUIRED_KLINE_COLUMNS = {
    "code": "VARCHAR",
    "datetime": "TIMESTAMP",
    "open": "DOUBLE",
    "high": "DOUBLE",
    "low": "DOUBLE",
    "close": "DOUBLE",
    "volume": "BIGINT",
    "amount": "BIGINT",
    "source": "VARCHAR",
}


def _normalize_source(raw_source: Any) -> str:
    """
    输入：
    1. raw_source: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `_normalize_source` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    token = str(raw_source or "").strip().lower()
    if token in {"tdx", "intraday", "盘中"}:
        return SOURCE_INTRADAY
    return SOURCE_POSTCLOSE


def _normalize_db_code(raw_code: Any) -> str:
    """
    输入：
    1. raw_code: 输入参数，具体约束以调用方和实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `_normalize_db_code` 对应的业务或工具逻辑。
    边界条件：
    1. 关键边界与异常分支按函数体内判断与调用约定处理。
    """
    token = str(raw_code or "").strip().lower()
    if not token:
        return ""
    if "." in token:
        prefix, tail = token.split(".", 1)
        prefix = f"{prefix.strip()}."
        tail = tail.strip()
        if prefix in MAINTENANCE_ALLOWED_STOCK_PREFIXES and tail:
            return f"{prefix}{tail}"
        return ""
    if token.startswith("6"):
        return f"sh.{token}"
    if token.startswith(("0", "3")):
        return f"sz.{token}"
    if token.startswith(("4", "8", "9")):
        return f"bj.{token}"
    return ""


class SourceSchemaMigrationService:
    """源库历史迁移服务。"""

    def __init__(
        self,
        *,
        source_db_path: Path | str,
        logger: Any,
        state_db: StateDB | None = None,
    ):
        """
        输入：
        1. source_db_path: 输入参数，具体约束以调用方和实现为准。
        2. logger: 输入参数，具体约束以调用方和实现为准。
        3. state_db: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `__init__` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        self.source_db_path = Path(source_db_path).resolve()
        self.logger = logger
        self.state_db = state_db

    def _meta_key(self, key: str) -> str:
        """
        输入：
        1. key: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_meta_key` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        return f"maintenance::{self.source_db_path.as_posix()}::{str(key).strip()}"

    def _get_meta(self, key: str) -> str | None:
        """
        输入：
        1. key: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_get_meta` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        if self.state_db is None:
            return None
        return self.state_db.get_maintenance_meta(self._meta_key(key))

    def _set_meta(self, key: str, value: str) -> None:
        """
        输入：
        1. key: 输入参数，具体约束以调用方和实现为准。
        2. value: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_set_meta` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        if self.state_db is not None:
            self.state_db.set_maintenance_meta(self._meta_key(key), str(value))

    def _connect_source_db(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """
        输入：
        1. read_only: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_connect_source_db` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        return connect_duckdb_compatible(self.source_db_path, read_only=read_only, retries=60, retry_sleep_seconds=0.05)

    def _table_exists(self, con: duckdb.DuckDBPyConnection, table: str) -> bool:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_table_exists` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        try:
            con.execute(f"select 1 from {table} limit 1")
            return True
        except Exception:
            return False

    def _column_types(self, con: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_column_types` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        rows = con.execute(f"pragma table_info('{table}')").fetchall()
        return {str(r[1]).strip().lower(): str(r[2]).strip().upper() for r in rows}

    def _pk_columns(self, con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_pk_columns` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        rows = con.execute(f"pragma table_info('{table}')").fetchall()
        return [str(r[1]).strip().lower() for r in rows if bool(r[5])]

    def _list_table_indexes(self, con: duckdb.DuckDBPyConnection, table: str) -> list[dict[str, str]]:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_list_table_indexes` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        rows = con.execute(
            """
            select index_name, sql
            from duckdb_indexes()
            where schema_name = 'main'
              and table_name = ?
              and sql is not null
            order by is_unique desc, index_name asc
            """,
            [str(table)],
        ).fetchall()
        return [
            {"index_name": str(r[0]), "create_sql": str(r[1])}
            for r in rows
            if r and r[0] is not None and r[1] is not None
        ]

    def _has_column(self, con: duckdb.DuckDBPyConnection, table: str, column: str) -> bool:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        3. column: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_has_column` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        try:
            rows = con.execute(f"pragma table_info('{table}')").fetchall()
        except Exception:
            return False
        return any(str(r[1]).strip().lower() == column.lower() for r in rows)

    def _create_stocks_if_missing(self, con: duckdb.DuckDBPyConnection) -> None:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_create_stocks_if_missing` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        con.execute(
            """
            create table if not exists stocks (
                code varchar not null,
                name varchar not null,
                primary key(code)
            )
            """
        )

    def _create_stock_concepts_if_missing(self, con: duckdb.DuckDBPyConnection) -> None:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 确保股票概念明细表存在。
        边界条件：
        1. 仅创建缺失表，不做历史结构修复。
        """
        con.execute(
            """
            create table if not exists stock_concepts (
                code varchar not null,
                board_name varchar not null,
                selected_reason varchar,
                updated_at timestamp not null,
                primary key(code, board_name)
            )
            """
        )

    def _create_kline_if_missing(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_create_kline_if_missing` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        con.execute(
            f"""
            create table if not exists {table} (
                code varchar not null,
                datetime timestamp not null,
                open double not null,
                high double not null,
                low double not null,
                close double not null,
                volume bigint not null,
                amount bigint not null,
                source varchar not null,
                primary key(code, datetime)
            )
            """
        )

    def _scan_invalid_code_samples(self, con: duckdb.DuckDBPyConnection, table: str, limit: int = 20) -> list[str]:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        3. limit: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_scan_invalid_code_samples` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        rows = con.execute(
            f"""
            select distinct code
            from {table}
            where code is null
               or not regexp_matches(code, '^(sh|sz|bj)\\.[0-9A-Za-z]+$')
            limit ?
            """,
            [max(1, int(limit))],
        ).fetchall()
        return [str(r[0]) for r in rows if r and r[0] is not None]

    def _scan_bad_daily_weekly_time_samples(
        self,
        con: duckdb.DuckDBPyConnection,
        table: str,
        limit: int = 20,
    ) -> list[str]:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        3. limit: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_scan_bad_daily_weekly_time_samples` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        rows = con.execute(
            f"""
            select cast(datetime as varchar)
            from {table}
            where strftime(datetime, '%H:%M:%S') <> '15:00:00'
            limit ?
            """,
            [max(1, int(limit))],
        ).fetchall()
        return [str(r[0]) for r in rows if r and r[0] is not None]

    def _scan_non_integer_numeric_samples(
        self,
        con: duckdb.DuckDBPyConnection,
        table: str,
        limit: int = 20,
    ) -> list[dict[str, str]]:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        3. limit: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_scan_non_integer_numeric_samples` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        rows = con.execute(
            f"""
            select cast(volume as varchar), cast(amount as varchar)
            from {table}
            where (volume - floor(volume)) <> 0
               or (amount - floor(amount)) <> 0
            limit ?
            """,
            [max(1, int(limit))],
        ).fetchall()
        return [{"volume": str(r[0]), "amount": str(r[1])} for r in rows]

    def _append_blocking(
        self,
        anomalies: dict[str, Any],
        *,
        table: str,
        reason: str,
        detail: Any,
    ) -> None:
        """
        输入：
        1. anomalies: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        3. reason: 输入参数，具体约束以调用方和实现为准。
        4. detail: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_append_blocking` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        anomalies.setdefault("blocking", []).append({"table": table, "reason": reason, "detail": detail})

    def _precheck_source_schema(self, con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_precheck_source_schema` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        anomalies: dict[str, Any] = {
            "missing_tables": [],
            "stocks": {},
            "kline_tables": {},
            "blocking": [],
        }

        if not self._table_exists(con, "stocks"):
            anomalies["missing_tables"].append("stocks")
        else:
            stocks_cols = self._column_types(con, "stocks")
            missing = [c for c in REQUIRED_STOCK_COLUMNS if c not in stocks_cols]
            mismatch = [
                {"column": c, "expected": t, "actual": stocks_cols.get(c)}
                for c, t in REQUIRED_STOCK_COLUMNS.items()
                if c in stocks_cols and stocks_cols[c] != t
            ]
            stock_pk = self._pk_columns(con, "stocks")
            bad_codes = self._scan_invalid_code_samples(con, "stocks")
            anomalies["stocks"] = {
                "missing_columns": missing,
                "column_type_mismatches": mismatch,
                "pk_columns": stock_pk,
                "bad_code_samples": bad_codes,
            }
            if missing:
                self._append_blocking(anomalies, table="stocks", reason="missing_columns", detail=missing)
            if mismatch:
                self._append_blocking(anomalies, table="stocks", reason="column_type_mismatches", detail=mismatch)
            if stock_pk != ["code"]:
                self._append_blocking(
                    anomalies,
                    table="stocks",
                    reason="pk_mismatch",
                    detail={"expected": ["code"], "actual": stock_pk},
                )
            if bad_codes:
                self._append_blocking(anomalies, table="stocks", reason="bad_code_samples", detail=bad_codes)

        for freq, table in KLINE_TABLE_BY_FREQ.items():
            table_info: dict[str, Any] = {"freq": freq}
            anomalies["kline_tables"][table] = table_info

            if not self._table_exists(con, table):
                table_info["missing_table"] = True
                anomalies["missing_tables"].append(table)
                continue

            cols = self._column_types(con, table)
            missing = [c for c in REQUIRED_KLINE_COLUMNS if c not in cols]
            mismatch = [
                {"column": c, "expected": t, "actual": cols.get(c)}
                for c, t in REQUIRED_KLINE_COLUMNS.items()
                if c in cols and cols[c] != t
            ]
            pk_columns = self._pk_columns(con, table)
            bad_codes = self._scan_invalid_code_samples(con, table)
            non_integer_samples = self._scan_non_integer_numeric_samples(con, table)

            table_info["missing_columns"] = missing
            table_info["column_type_mismatches"] = mismatch
            table_info["pk_columns"] = pk_columns
            table_info["bad_code_samples"] = bad_codes
            table_info["non_integer_numeric_samples"] = non_integer_samples

            if freq in {"d", "w"}:
                bad_time_samples = self._scan_bad_daily_weekly_time_samples(con, table)
                table_info["bad_time_samples"] = bad_time_samples
            else:
                bad_time_samples = []

            if missing and any(col != "source" for col in missing):
                self._append_blocking(anomalies, table=table, reason="missing_columns", detail=missing)

            non_trivial_mismatch = [
                item
                for item in mismatch
                if item["column"] not in {"volume", "amount", "source"}
            ]
            if non_trivial_mismatch:
                self._append_blocking(
                    anomalies,
                    table=table,
                    reason="column_type_mismatches",
                    detail=non_trivial_mismatch,
                )

            if pk_columns != ["code", "datetime"]:
                self._append_blocking(
                    anomalies,
                    table=table,
                    reason="pk_mismatch",
                    detail={"expected": ["code", "datetime"], "actual": pk_columns},
                )
            if bad_codes:
                self._append_blocking(anomalies, table=table, reason="bad_code_samples", detail=bad_codes)
            if bad_time_samples:
                self._append_blocking(
                    anomalies,
                    table=table,
                    reason="bad_time_samples",
                    detail=bad_time_samples,
                )

        return anomalies

    def _rebuild_stocks_to_target(self, con: duckdb.DuckDBPyConnection) -> int:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_rebuild_stocks_to_target` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        self._create_stocks_if_missing(con)
        self._create_stock_concepts_if_missing(con)
        has_code = self._has_column(con, "stocks", "code")
        has_name = self._has_column(con, "stocks", "name")
        if not has_code:
            con.execute("drop table stocks")
            self._create_stocks_if_missing(con)
            return 0

        con.execute("drop table if exists stocks_new")
        con.execute("create table stocks_new(code varchar not null, name varchar not null, primary key(code))")
        if has_name:
            rows = con.execute("select code, name from stocks").fetchall()
            dedup: dict[str, str] = {}
            for raw_code, raw_name in rows:
                code = _normalize_db_code(raw_code)
                if not code:
                    continue
                name = str(raw_name or "").strip() or code
                dedup.setdefault(code, name)
            if dedup:
                con.executemany(
                    "insert into stocks_new(code, name) values (?, ?)",
                    [(k, dedup[k]) for k in sorted(dedup.keys())],
                )
        con.execute("drop table stocks")
        con.execute("alter table stocks_new rename to stocks")
        return int(con.execute("select count(*) from stocks").fetchone()[0] or 0)

    def _normalize_sources(self, con: duckdb.DuckDBPyConnection) -> list[str]:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_normalize_sources` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        added: list[str] = []
        for table in KLINE_TABLE_BY_FREQ.values():
            self._create_kline_if_missing(con, table)
            if not self._has_column(con, table, "source"):
                con.execute(f"alter table {table} add column source varchar")
                added.append(table)
            con.execute(
                f"""
                update {table}
                set source = case
                    when lower(trim(coalesce(source, ''))) in ('tdx', 'intraday', '盘中') then '{SOURCE_INTRADAY}'
                    else '{SOURCE_POSTCLOSE}'
                end
                """
            )
        return added

    def _normalized_code_sql(self, column: str = "code") -> str:
        """
        输入：
        1. column: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_normalized_code_sql` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        token = f"lower(trim(coalesce({column}, '')))"
        return (
            "case "
            f"when {token} like 'sh.%' or {token} like 'sz.%' or {token} like 'bj.%' then {token} "
            f"when {token} like '6%' then 'sh.' || {token} "
            f"when {token} like '0%' or {token} like '3%' then 'sz.' || {token} "
            f"when {token} like '4%' or {token} like '8%' or {token} like '9%' then 'bj.' || {token} "
            "else '' end"
        )

    def _normalize_numeric(self, con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_normalize_numeric` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        result: list[dict[str, Any]] = []
        for table in KLINE_TABLE_BY_FREQ.values():
            con.execute(
                f"""
                update {table}
                set
                    open = round(coalesce(open, 0), 2),
                    high = round(coalesce(high, 0), 2),
                    low = round(coalesce(low, 0), 2),
                    close = round(coalesce(close, 0), 2),
                    volume = cast(round(coalesce(volume, 0), 0) as bigint),
                    amount = cast(round(coalesce(amount, 0), 0) as bigint)
                """
            )
            cnt = int(con.execute(f"select count(*) from {table}").fetchone()[0] or 0)
            result.append({"table": table, "rows": cnt})
        return result

    def _alter_kline_numeric_types_in_place(self, con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_alter_kline_numeric_types_in_place` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        changed: list[dict[str, Any]] = []
        for table in KLINE_TABLE_BY_FREQ.values():
            if not self._table_exists(con, table):
                continue
            cols = self._column_types(con, table)
            table_changes: list[dict[str, str]] = []
            for column in ("volume", "amount"):
                before_type = cols.get(column)
                if before_type is None:
                    continue
                if before_type != "BIGINT":
                    table_changes.append({"column": column, "from": before_type, "to": "BIGINT"})

            if table_changes:
                indexes = self._list_table_indexes(con, table)
                dropped_indexes: list[str] = []

                for idx in indexes:
                    index_name = idx["index_name"].replace('"', '""')
                    con.execute(f'drop index if exists "{index_name}"')
                    dropped_indexes.append(idx["index_name"])

                for item in table_changes:
                    con.execute(f"alter table {table} alter column {item['column']} type bigint")

                recreated_indexes: list[str] = []
                for idx in indexes:
                    con.execute(idx["create_sql"])
                    recreated_indexes.append(idx["index_name"])

                changed.append(
                    {
                        "table": table,
                        "changes": table_changes,
                        "dropped_indexes": dropped_indexes,
                        "recreated_indexes": recreated_indexes,
                    }
                )
        return changed

    def _rebuild_daily_weekly(self, con: duckdb.DuckDBPyConnection, table: str) -> int:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_rebuild_daily_weekly` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        temp = f"_{table}_rebuilt"
        code_sql = self._normalized_code_sql("code")
        con.execute(f"drop table if exists {temp}")
        con.execute(
            f"""
            create table {temp} as
            select
                normalized_code as code,
                cast(cast(datetime as date) as timestamp) + interval '15 hour' as datetime,
                round(coalesce(open, 0), 2) as open,
                round(coalesce(high, 0), 2) as high,
                round(coalesce(low, 0), 2) as low,
                round(coalesce(close, 0), 2) as close,
                cast(round(coalesce(volume, 0), 0) as bigint) as volume,
                cast(round(coalesce(amount, 0), 0) as bigint) as amount,
                case
                    when lower(trim(coalesce(source, ''))) in ('tdx', 'intraday', '盘中') then '{SOURCE_INTRADAY}'
                    else '{SOURCE_POSTCLOSE}'
                end as source
            from (
                select *, {code_sql} as normalized_code
                from {table}
            ) t
            where normalized_code <> ''
            qualify row_number() over (
                partition by normalized_code, cast(datetime as date)
                order by
                    case when lower(trim(coalesce(source, ''))) in ('tdx', 'intraday', '盘中') then 1 else 0 end asc,
                    datetime desc
            ) = 1
            """
        )
        con.execute(f"drop table {table}")
        con.execute(f"alter table {temp} rename to {table}")
        con.execute(f"alter table {table} add primary key(code, datetime)")
        return int(con.execute(f"select count(*) from {table}").fetchone()[0] or 0)

    def _rebuild_intraday_with_normalized_code(self, con: duckdb.DuckDBPyConnection, table: str) -> int:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        2. table: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_rebuild_intraday_with_normalized_code` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        temp = f"_{table}_rebuilt"
        code_sql = self._normalized_code_sql("code")
        con.execute(f"drop table if exists {temp}")
        con.execute(
            f"""
            create table {temp} as
            select
                normalized_code as code,
                datetime,
                round(coalesce(open, 0), 2) as open,
                round(coalesce(high, 0), 2) as high,
                round(coalesce(low, 0), 2) as low,
                round(coalesce(close, 0), 2) as close,
                cast(round(coalesce(volume, 0), 0) as bigint) as volume,
                cast(round(coalesce(amount, 0), 0) as bigint) as amount,
                case
                    when lower(trim(coalesce(source, ''))) in ('tdx', 'intraday', '盘中') then '{SOURCE_INTRADAY}'
                    else '{SOURCE_POSTCLOSE}'
                end as source
            from (
                select *, {code_sql} as normalized_code
                from {table}
            ) t
            where normalized_code <> ''
            qualify row_number() over (
                partition by normalized_code, datetime
                order by
                    case when lower(trim(coalesce(source, ''))) in ('tdx', 'intraday', '盘中') then 1 else 0 end asc
            ) = 1
            """
        )
        con.execute(f"drop table {table}")
        con.execute(f"alter table {temp} rename to {table}")
        con.execute(f"alter table {table} add primary key(code, datetime)")
        return int(con.execute(f"select count(*) from {table}").fetchone()[0] or 0)

    def _migrate_legacy_meta_once(self, con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
        """
        输入：
        1. con: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_migrate_legacy_meta_once` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        result = {
            "state_db_enabled": bool(self.state_db is not None),
            "legacy_table_present": False,
            "imported_count": 0,
            "dropped_legacy_table": False,
            "already_done": False,
        }
        if self.state_db is None:
            return result

        result["already_done"] = self._get_meta(LEGACY_META_IMPORT_KEY) == "done"
        result["legacy_table_present"] = self._table_exists(con, "_maintenance_meta")
        if not result["legacy_table_present"]:
            self._set_meta(LEGACY_META_IMPORT_KEY, "done")
            return result

        rows = con.execute("select key, value from _maintenance_meta").fetchall()
        if not result["already_done"]:
            for k, v in rows:
                key = str(k or "").strip()
                if not key:
                    continue
                self._set_meta(key, str(v or ""))
                result["imported_count"] += 1

        con.execute("drop table if exists _maintenance_meta")
        result["dropped_legacy_table"] = True
        self._set_meta(LEGACY_META_IMPORT_KEY, "done")
        return result

    def ensure_source_schema(
        self,
        mode: Literal["in_place", "rebuild"] = "in_place",
        anomaly_policy: Literal["abort", "rebuild_table"] = "abort",
    ) -> dict[str, Any]:
        """
        输入：
        1. mode: 输入参数，具体约束以调用方和实现为准。
        2. anomaly_policy: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `ensure_source_schema` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        requested_mode = str(mode or "in_place").strip().lower()
        if requested_mode not in {"in_place", "rebuild"}:
            requested_mode = "in_place"

        requested_policy = str(anomaly_policy or "abort").strip().lower()
        if requested_policy not in {"abort", "rebuild_table"}:
            requested_policy = "abort"

        with self._connect_source_db(read_only=False) as con:
            anomalies = self._precheck_source_schema(con)
            blocking = list(anomalies.get("blocking") or [])
            effective_mode = requested_mode

            if requested_mode == "in_place" and blocking:
                if requested_policy == "abort":
                    detail = {
                        "mode": requested_mode,
                        "anomaly_policy": requested_policy,
                        "blocking_count": len(blocking),
                        "blocking": blocking,
                    }
                    self.logger.error("源库 schema 预检查失败，已中止迁移", detail)
                    raise RuntimeError(json.dumps(detail, ensure_ascii=False))
                effective_mode = "rebuild"
                self.logger.info(
                    "源库 schema 预检查存在阻塞异常，按策略切换为重建模式",
                    {
                        "requested_mode": requested_mode,
                        "effective_mode": effective_mode,
                        "blocking_count": len(blocking),
                    },
                )

            if effective_mode == "rebuild":
                stocks_rows = self._rebuild_stocks_to_target(con)
            else:
                self._create_stocks_if_missing(con)
                stocks_rows = int(con.execute("select count(*) from stocks").fetchone()[0] or 0)

            self._create_stock_concepts_if_missing(con)

            added_source_columns = self._normalize_sources(con)
            rebuilt_tables: list[dict[str, Any]] = []
            normalized_numeric_tables: list[dict[str, Any]] = []
            altered_numeric_types: list[dict[str, Any]] = []

            if effective_mode == "rebuild":
                for table in ("klines_15", "klines_30", "klines_60"):
                    rebuilt_tables.append({"table": table, "rows": self._rebuild_intraday_with_normalized_code(con, table)})
                rebuilt_tables.append({"table": "klines_d", "rows": self._rebuild_daily_weekly(con, "klines_d")})
                rebuilt_tables.append({"table": "klines_w", "rows": self._rebuild_daily_weekly(con, "klines_w")})

                numeric_applied = self._get_meta(KLINE_NUMERIC_MIGRATION_KEY) != "done"
                normalized_numeric_tables = self._normalize_numeric(con) if numeric_applied else []
            else:
                altered_numeric_types = self._alter_kline_numeric_types_in_place(con)
                numeric_applied = bool(altered_numeric_types) or (self._get_meta(KLINE_NUMERIC_MIGRATION_KEY) != "done")

            if numeric_applied:
                self._set_meta(KLINE_NUMERIC_MIGRATION_KEY, "done")

            meta_migration = self._migrate_legacy_meta_once(con)
            con.commit()

        summary = {
            "mode": requested_mode,
            "effective_mode": effective_mode,
            "anomaly_policy": requested_policy,
            "anomalies": anomalies,
            "added_source_columns": added_source_columns,
            "rebuilt_tables": rebuilt_tables,
            "numeric_migration_applied": numeric_applied,
            "normalized_numeric_tables": normalized_numeric_tables,
            "altered_numeric_types": altered_numeric_types,
            "stocks_rows": stocks_rows,
            "migrated_legacy_meta_count": int(meta_migration.get("imported_count") or 0),
            "meta_migration": meta_migration,
        }
        self.logger.info("源库 schema 迁移完成", summary)
        return summary
