"""
数据库维护重构一次性准备脚本。

职责：
1. 先备份源库与状态库。
2. 清理源库 2026-02-01 及之后的 klines 数据并删除 source 列。
3. 重建 screening_state.duckdb 到新 schema。
"""

from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db.state_db import StateDB
from app.settings import SOURCE_DB_PATH, STATE_DB_PATH

KLINE_TABLES = ["klines_15", "klines_30", "klines_60", "klines_d", "klines_w"]
DELETE_CUTOFF = "2026-02-01 00:00:00"


@dataclass(slots=True)
class TablePlan:
    table: str
    exists: bool
    delete_rows: int
    has_source_column: bool


def _timestamp_token() -> str:
    """
    输入：
    1. 无。
    输出：
    1. `YYYYmmdd-HHMMSS` 时间戳。
    用途：
    1. 生成备份文件后缀。
    边界条件：
    1. 采用本地系统时间。
    """

    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _backup_file(path: Path, suffix: str) -> Path | None:
    """
    输入：
    1. path: 待备份文件路径。
    2. suffix: 备份后缀。
    输出：
    1. 备份文件路径；源文件不存在时返回 None。
    用途：
    1. 在改库前创建可回滚副本。
    边界条件：
    1. 目标目录不存在时自动创建。
    """

    if not path.exists():
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    backup_path = path.with_name(f"{path.name}.bak-{suffix}")
    shutil.copy2(path, backup_path)
    return backup_path


def _build_source_plan(source_db: Path) -> list[TablePlan]:
    """
    输入：
    1. source_db: 源库路径。
    输出：
    1. 源库各目标表操作计划。
    用途：
    1. dry-run 与 apply 共用计划生成逻辑。
    边界条件：
    1. 表不存在时 delete_rows 为 0。
    """

    plans: list[TablePlan] = []
    with duckdb.connect(str(source_db), read_only=True) as con:
        for table in KLINE_TABLES:
            exists = bool(
                con.execute(
                    """
                    select count(*)
                    from information_schema.tables
                    where lower(table_name) = lower(?)
                    """,
                    [table],
                ).fetchone()[0]
            )
            if not exists:
                plans.append(TablePlan(table=table, exists=False, delete_rows=0, has_source_column=False))
                continue

            delete_rows = int(
                con.execute(
                    f"""
                    select count(*)
                    from {table}
                    where datetime >= ?
                    """,
                    [DELETE_CUTOFF],
                ).fetchone()[0]
                or 0
            )
            has_source_column = bool(
                con.execute(f"select count(*) from pragma_table_info('{table}') where lower(name) = 'source'").fetchone()[0]
            )
            plans.append(
                TablePlan(
                    table=table,
                    exists=True,
                    delete_rows=delete_rows,
                    has_source_column=has_source_column,
                )
            )
    return plans


def _apply_source_plan(source_db: Path, plans: list[TablePlan]) -> dict[str, Any]:
    """
    输入：
    1. source_db: 源库路径。
    2. plans: 预生成计划。
    输出：
    1. 实际执行摘要。
    用途：
    1. 在事务内执行删数据与删列操作。
    边界条件：
    1. 仅对存在表执行操作。
    """

    deleted_rows = 0
    dropped_source_tables: list[str] = []
    with duckdb.connect(str(source_db), read_only=False) as con:
        con.execute("begin transaction")
        try:
            for plan in plans:
                if not plan.exists:
                    continue
                delete_count = int(
                    con.execute(
                        f"""
                        select count(*)
                        from {plan.table}
                        where datetime >= ?
                        """,
                        [DELETE_CUTOFF],
                    ).fetchone()[0]
                    or 0
                )
                con.execute(
                    f"""
                    delete from {plan.table}
                    where datetime >= ?
                    """,
                    [DELETE_CUTOFF],
                )
                deleted_rows += delete_count
                if plan.has_source_column:
                    con.execute(f"alter table {plan.table} drop column source")
                    dropped_source_tables.append(plan.table)
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise
    return {
        "deleted_rows": deleted_rows,
        "dropped_source_tables": dropped_source_tables,
    }


def _rebuild_state_db(state_db: Path) -> None:
    """
    输入：
    1. state_db: 状态库路径。
    输出：
    1. 无返回值。
    用途：
    1. 删除旧库后按新 schema 重建。
    边界条件：
    1. 文件不存在时仅执行初始化。
    """

    if state_db.exists():
        state_db.unlink()
    state_db.parent.mkdir(parents=True, exist_ok=True)
    db = StateDB(state_db)
    try:
        db.init_schema()
    finally:
        db.close()


def _print_plan(plans: list[TablePlan]) -> None:
    """
    输入：
    1. plans: 源库计划列表。
    输出：
    1. 无返回值。
    用途：
    1. 统一输出 dry-run/apply 前的计划摘要。
    边界条件：
    1. 输出按表顺序稳定。
    """

    print("源库执行计划：")
    for plan in plans:
        if not plan.exists:
            print(f"- {plan.table}: 表不存在，跳过")
            continue
        print(
            f"- {plan.table}: delete_rows={plan.delete_rows}, "
            f"drop_source_column={'yes' if plan.has_source_column else 'no'}"
        )


def run_prepare(
    *,
    source_db: Path,
    state_db: Path,
    apply: bool,
) -> dict[str, Any]:
    """
    输入：
    1. source_db: 源库路径。
    2. state_db: 状态库路径。
    3. apply: 是否执行真实改动。
    输出：
    1. 脚本执行摘要。
    用途：
    1. 执行或预览维护重构准备动作。
    边界条件：
    1. apply=False 时不会修改任何文件。
    """

    source_db = source_db.resolve()
    state_db = state_db.resolve()
    if not source_db.exists():
        raise FileNotFoundError(f"源库不存在: {source_db}")

    plans = _build_source_plan(source_db)
    _print_plan(plans)

    summary: dict[str, Any] = {
        "apply": bool(apply),
        "source_db": str(source_db),
        "state_db": str(state_db),
        "delete_cutoff": DELETE_CUTOFF,
        "plan": [plan.__dict__ for plan in plans],
        "backups": {},
    }

    if not apply:
        print("dry-run 完成：未执行改动。")
        return summary

    backup_suffix = _timestamp_token()
    source_backup = _backup_file(source_db, backup_suffix)
    state_backup = _backup_file(state_db, backup_suffix)
    summary["backups"] = {
        "source_db": str(source_backup) if source_backup else None,
        "state_db": str(state_backup) if state_backup else None,
    }

    source_result = _apply_source_plan(source_db, plans)
    summary["source_result"] = source_result

    _rebuild_state_db(state_db)
    summary["state_rebuilt"] = True
    print("apply 完成：源库与状态库已更新。")
    return summary


def main() -> None:
    """
    输入：
    1. 命令行参数。
    输出：
    1. 无返回值。
    用途：
    1. 脚本入口。
    边界条件：
    1. 默认 dry-run，需显式 `--apply` 执行改动。
    """

    parser = argparse.ArgumentParser(description="数据库维护重构准备脚本（默认 dry-run）")
    parser.add_argument("--source-db", default=str(SOURCE_DB_PATH), help="源库路径")
    parser.add_argument("--state-db", default=str(STATE_DB_PATH), help="状态库路径")
    parser.add_argument("--apply", action="store_true", help="执行实际改动（默认仅预览）")
    args = parser.parse_args()

    summary = run_prepare(
        source_db=Path(args.source_db),
        state_db=Path(args.state_db),
        apply=bool(args.apply),
    )
    print(summary)


if __name__ == "__main__":
    main()
