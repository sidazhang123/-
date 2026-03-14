"""
将 screening_state.duckdb 的 maintenance_retry_tasks.attempt_count 全部重置为 0。

职责：
1. 连接状态库并校验 maintenance_retry_tasks 表存在。
2. 将 attempt_count != 0 的记录批量更新为 0。
3. 输出更新前后统计，便于审计执行结果。
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="重置 maintenance_retry_tasks 的重试次数为 0")
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

        total_rows = int(con.execute("select count(*) from maintenance_retry_tasks").fetchone()[0] or 0)
        non_zero_rows = int(
            con.execute("select count(*) from maintenance_retry_tasks where attempt_count <> 0").fetchone()[0] or 0
        )

        con.execute("begin transaction")
        try:
            con.execute(
                """
                update maintenance_retry_tasks
                set attempt_count = 0
                where attempt_count <> 0
                """
            )
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise

        remaining_non_zero = int(
            con.execute("select count(*) from maintenance_retry_tasks where attempt_count <> 0").fetchone()[0] or 0
        )

    updated_rows = non_zero_rows - remaining_non_zero
    print(f"DB: {db_path}")
    print(f"total_rows={total_rows}")
    print(f"before_non_zero_attempts={non_zero_rows}")
    print(f"updated_rows={updated_rows}")
    print(f"after_non_zero_attempts={remaining_non_zero}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
