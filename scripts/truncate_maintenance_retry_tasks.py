"""
清空 screening_state.duckdb 中 maintenance_retry_tasks 表的所有数据。

职责：
1. 连接状态库并校验 maintenance_retry_tasks 表存在。
2. 在事务中执行 delete 清空全表。
3. 输出清空前后统计，便于审计执行结果。
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="清空 maintenance_retry_tasks 表")
    parser.add_argument(
        "--db-path",
        default="screening_state.duckdb",
        help="状态库路径，默认 screening_state.duckdb",
    )
    return parser.parse_args()


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    count = con.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = 'main' and table_name = ?
        """,
        [table_name],
    ).fetchone()[0]
    return bool(count)


def main() -> int:
    args = _parse_args()
    db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        raise SystemExit(f"数据库文件不存在: {db_path}")

    with duckdb.connect(str(db_path), read_only=False) as con:
        if not _table_exists(con, "maintenance_retry_tasks"):
            raise SystemExit("表不存在: maintenance_retry_tasks")

        before_rows = int(con.execute("select count(*) from maintenance_retry_tasks").fetchone()[0] or 0)

        con.execute("begin transaction")
        try:
            con.execute("delete from maintenance_retry_tasks")
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise

        after_rows = int(con.execute("select count(*) from maintenance_retry_tasks").fetchone()[0] or 0)

    deleted_rows = before_rows - after_rows
    print(f"DB: {db_path}")
    print(f"before_rows={before_rows}")
    print(f"deleted_rows={deleted_rows}")
    print(f"after_rows={after_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
