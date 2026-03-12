"""
将 screening_state.duckdb 中历史 debug 日志迁移到 logs/debug/*.jsonl。

说明：
1. 仅处理 maintenance_logs / concept_logs 中 level='debug' 的记录。
2. 不删除整张日志表，因为 info/error 仍由前端 API 使用。
3. 删除阶段按精确 log_id 执行，避免误删非 debug 日志。
4. 默认 dry-run；必须显式传入 --apply 才会写文件。
5. 如需删除已迁移的 DB debug 行，额外传入 --delete-db-debug。
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb


TABLE_TO_PREFIX = {
    "maintenance_logs": "maintenance",
    "concept_logs": "concept",
}


@dataclass(frozen=True)
class DebugRow:
    table: str
    log_id: int
    job_id: str
    ts: str
    message: str
    detail: dict[str, Any] | None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="迁移 screening_state.duckdb 中的 debug 日志到文件")
    parser.add_argument(
        "--db-path",
        default="screening_state.duckdb",
        help="DuckDB 文件路径，默认 screening_state.duckdb",
    )
    parser.add_argument(
        "--debug-dir",
        default="logs/debug",
        help="目标 debug 日志目录，默认 logs/debug",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="执行迁移写文件；默认仅 dry-run 预览",
    )
    parser.add_argument(
        "--delete-db-debug",
        action="store_true",
        help="迁移完成后删除 DB 中已迁移的 debug 行；必须与 --apply 一起使用",
    )
    return parser.parse_args()


def _load_debug_rows(con: duckdb.DuckDBPyConnection, table: str) -> list[DebugRow]:
    rows = con.execute(
        f"""
        select log_id, job_id, ts, message, detail_json
        from {table}
        where level = 'debug'
        order by job_id asc, log_id asc
        """
    ).fetchall()
    result: list[DebugRow] = []
    for log_id, job_id, ts, message, detail_json in rows:
        detail: dict[str, Any] | None = None
        if detail_json:
            parsed = json.loads(detail_json)
            detail = parsed if isinstance(parsed, dict) else {"value": parsed}
        result.append(
            DebugRow(
                table=table,
                log_id=int(log_id),
                job_id=str(job_id),
                ts=ts.isoformat(sep=" "),
                message=str(message),
                detail=detail,
            )
        )
    return result


def _target_path(debug_dir: Path, row: DebugRow) -> Path:
    prefix = TABLE_TO_PREFIX[row.table]
    return debug_dir / f"{prefix}_{row.job_id}.jsonl"


def _load_existing_source_ids(path: Path) -> set[tuple[str, int]]:
    if not path.exists():
        return set()
    ids: set[tuple[str, int]] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"目标文件存在非法 JSON 行: {path}#{line_number} | {exc}") from exc
            source_table = payload.get("source_table")
            source_log_id = payload.get("source_log_id")
            if isinstance(source_table, str) and isinstance(source_log_id, int):
                ids.add((source_table, source_log_id))
    return ids


def _build_record(row: DebugRow) -> str:
    record: dict[str, Any] = {
        "ts": row.ts,
        "job_id": row.job_id,
        "level": "debug",
        "message": row.message,
        "source_table": row.table,
        "source_log_id": row.log_id,
        "migrated_from_db": True,
    }
    if row.detail is not None:
        record["detail"] = row.detail
    return json.dumps(record, ensure_ascii=False, default=str)


def _delete_rows_by_ids(con: duckdb.DuckDBPyConnection, table: str, log_ids: list[int]) -> int:
    if not log_ids:
        return 0
    placeholders = ", ".join(["?"] * len(log_ids))
    con.execute(f"delete from {table} where log_id in ({placeholders})", log_ids)
    return len(log_ids)


def main() -> int:
    args = _parse_args()
    if args.delete_db_debug and not args.apply:
        raise SystemExit("--delete-db-debug 必须与 --apply 一起使用")

    db_path = Path(args.db_path).resolve()
    debug_dir = Path(args.debug_dir).resolve()
    if not db_path.exists():
        raise SystemExit(f"数据库文件不存在: {db_path}")

    con = duckdb.connect(str(db_path))
    rows_by_table = {table: _load_debug_rows(con, table) for table in TABLE_TO_PREFIX}
    all_rows = [row for rows in rows_by_table.values() for row in rows]
    rows_by_path: dict[Path, list[DebugRow]] = defaultdict(list)
    for row in all_rows:
        rows_by_path[_target_path(debug_dir, row)].append(row)

    existing_ids_by_path = {path: _load_existing_source_ids(path) for path in rows_by_path}

    pending_by_path: dict[Path, list[DebugRow]] = {}
    migrated_ids_by_table: dict[str, list[int]] = defaultdict(list)
    for path, rows in rows_by_path.items():
        existing_ids = existing_ids_by_path[path]
        pending_rows: list[DebugRow] = []
        for row in rows:
            source_key = (row.table, row.log_id)
            if source_key not in existing_ids:
                pending_rows.append(row)
            migrated_ids_by_table[row.table].append(row.log_id)
        pending_by_path[path] = pending_rows

    print(f"DB: {db_path}")
    print(f"DEBUG_DIR: {debug_dir}")
    print(f"MODE: {'apply' if args.apply else 'dry-run'}")
    print(f"DELETE_DB_DEBUG: {bool(args.delete_db_debug)}")
    print()

    for table, rows in rows_by_table.items():
        job_count = len({row.job_id for row in rows})
        print(f"{table}: debug_rows={len(rows)} jobs={job_count}")

    total_pending = sum(len(rows) for rows in pending_by_path.values())
    print(f"pending_write_rows={total_pending}")
    print(f"target_files={len(rows_by_path)}")

    for path, rows in sorted(rows_by_path.items(), key=lambda item: str(item[0])):
        pending_count = len(pending_by_path[path])
        print(f"  {path.name}: total={len(rows)} pending={pending_count}")

    if not args.apply:
        return 0

    debug_dir.mkdir(parents=True, exist_ok=True)
    written_count = 0
    for path, pending_rows in pending_by_path.items():
        if not pending_rows:
            continue
        with path.open("a", encoding="utf-8") as handle:
            for row in pending_rows:
                handle.write(_build_record(row))
                handle.write("\n")
                written_count += 1

    print(f"written_rows={written_count}")

    if args.delete_db_debug:
        deleted_total = 0
        con.execute("begin transaction")
        try:
            for table, log_ids in migrated_ids_by_table.items():
                deleted_total += _delete_rows_by_ids(con, table, sorted(set(log_ids)))
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise
        print(f"deleted_db_rows={deleted_total}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())