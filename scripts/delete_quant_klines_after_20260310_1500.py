"""
清理 quant.duckdb 中各 klines_xxx 表在指定时间点之后的数据。

职责：
1. 自动发现 quant.duckdb 内所有以 klines_ 开头的表。
2. 仅对包含 datetime 字段的目标表执行删除。
3. 删除 datetime > 2026-03-10 15:00:00 的记录并输出统计。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path("D:/quant.duckdb")
DELETE_CUTOFF = "2026-03-10 15:00:00"


@dataclass(frozen=True)
class TableDeletePlan:
    """
    输入：
    1. table: 表名。
    2. has_datetime: 是否包含 datetime 列。
    3. matched_rows: 命中删除条件的行数。
    输出：
    1. 表级删除计划对象。
    用途：
    1. 汇总并打印各表删除计划与执行结果。
    边界条件：
    1. 不包含 datetime 列时 matched_rows 固定为 0。
    """

    table: str
    has_datetime: bool
    matched_rows: int


def _discover_kline_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    """
    输入：
    1. con: DuckDB 连接对象。
    输出：
    1. 所有以 klines_ 开头的表名列表。
    用途：
    1. 自动发现需要处理的 K 线表。
    边界条件：
    1. 没有匹配表时返回空列表。
    """

    rows = con.execute(
        """
        select table_name
        from information_schema.tables
        where table_schema = 'main'
          and lower(table_name) like 'klines\_%' escape '\\'
        order by table_name asc
        """
    ).fetchall()
    return [str(row[0]) for row in rows]


def _table_has_datetime_column(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    """
    输入：
    1. con: DuckDB 连接对象。
    2. table: 表名。
    输出：
    1. 表是否存在 datetime 列。
    用途：
    1. 防止对非预期结构执行删除 SQL。
    边界条件：
    1. 表结构异常时返回 False。
    """

    count = con.execute(
        "select count(*) from pragma_table_info(?) where lower(name) = 'datetime'",
        [table],
    ).fetchone()[0]
    return bool(count)


def _build_delete_plan(con: duckdb.DuckDBPyConnection, table: str) -> TableDeletePlan:
    """
    输入：
    1. con: DuckDB 连接对象。
    2. table: 表名。
    输出：
    1. 该表的删除计划。
    用途：
    1. 预先统计删除行数，便于执行前后校验。
    边界条件：
    1. 缺少 datetime 列时不统计删除行数。
    """

    has_datetime = _table_has_datetime_column(con, table)
    if not has_datetime:
        return TableDeletePlan(table=table, has_datetime=False, matched_rows=0)

    matched_rows = int(
        con.execute(
            f"select count(*) from {table} where datetime > ?",
            [DELETE_CUTOFF],
        ).fetchone()[0]
        or 0
    )
    return TableDeletePlan(table=table, has_datetime=True, matched_rows=matched_rows)


def _delete_for_plan(con: duckdb.DuckDBPyConnection, plan: TableDeletePlan) -> int:
    """
    输入：
    1. con: DuckDB 连接对象。
    2. plan: 表级删除计划。
    输出：
    1. 该表实际删除行数。
    用途：
    1. 按计划删除命中记录。
    边界条件：
    1. 缺少 datetime 列或命中为 0 时直接返回 0。
    """

    if not plan.has_datetime or plan.matched_rows <= 0:
        return 0

    con.execute(f"delete from {plan.table} where datetime > ?", [DELETE_CUTOFF])
    return plan.matched_rows


def main() -> int:
    """
    输入：
    1. 无。
    输出：
    1. 进程退出码，0 表示成功。
    用途：
    1. 执行 quant.duckdb 的 klines_xxx 数据清理任务。
    边界条件：
    1. 数据库不存在时抛出可读错误并终止。
    """

    if not DB_PATH.exists():
        raise SystemExit(f"数据库不存在: {DB_PATH}")

    with duckdb.connect(str(DB_PATH), read_only=False) as con:
        tables = _discover_kline_tables(con)
        if not tables:
            print("未发现任何 klines_xxx 表，未执行删除。")
            return 0

        plans = [_build_delete_plan(con, table) for table in tables]

        print(f"数据库: {DB_PATH}")
        print(f"删除条件: datetime > {DELETE_CUTOFF}")
        print("执行计划:")
        for plan in plans:
            if not plan.has_datetime:
                print(f"- {plan.table}: 缺少 datetime 列，跳过")
                continue
            print(f"- {plan.table}: 待删除 {plan.matched_rows} 行")

        deleted_total = 0
        con.execute("begin transaction")
        try:
            for plan in plans:
                deleted_total += _delete_for_plan(con, plan)
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise

    print(f"删除完成，总计删除 {deleted_total} 行。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
