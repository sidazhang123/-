"""
压缩状态库（screening_state.duckdb）占用空间的运维脚本。

职责：
1. 从 app/app_config.yaml 读取状态库路径（paths.state_db），并解析为绝对路径。
2. 先执行 CHECKPOINT + VACUUM；若未明显缩小，再执行 EXPORT/IMPORT 重写压缩。
3. 始终先做同目录备份，压缩成功后自动替换原库，并输出压缩前后体积对比。

边界：
1. 运行前需确保服务已停止，避免并发连接导致文件占用或体积统计失真。
2. 仅处理 DuckDB 文件压缩，不改动业务表结构与业务数据语义。
"""

from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
import duckdb
import yaml


def _project_root() -> Path:
    """
    输入：
    1. 无。
    输出：
    1. 项目根目录绝对路径。
    用途：
    1. 统一定位 app_config 与默认数据库路径。
    边界条件：
    1. 脚本位于 scripts/ 目录时，项目根目录为父级目录。
    """

    return Path(__file__).resolve().parents[1]


def _load_state_db_path(root: Path) -> Path:
    """
    输入：
    1. root: 项目根目录。
    输出：
    1. 状态库绝对路径。
    用途：
    1. 从 app/app_config.yaml 读取 paths.state_db，保证与主应用配置一致。
    边界条件：
    1. 配置缺失或字段无效时，回退为 root/screening_state.duckdb。
    """

    config_path = root / "app" / "app_config.yaml"
    fallback = root / "screening_state.duckdb"
    if not config_path.exists():
        return fallback

    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return fallback

    paths = payload.get("paths") if isinstance(payload, dict) else {}
    raw_path = paths.get("state_db") if isinstance(paths, dict) else None
    if not isinstance(raw_path, str) or not raw_path.strip():
        return fallback

    candidate = Path(raw_path.strip())
    if candidate.is_absolute():
        return candidate
    return (root / candidate).resolve()


def _bytes_to_human(size_bytes: int) -> str:
    """
    输入：
    1. size_bytes: 字节数。
    输出：
    1. 人类可读大小字符串。
    用途：
    1. 统一打印体积信息，便于快速对比。
    边界条件：
    1. 输入为负数时按 0 处理。
    """

    value = float(max(0, int(size_bytes)))
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return "0.00 B"


def _collect_db_size(db_path: Path) -> dict[str, int]:
    """
    输入：
    1. db_path: DuckDB 主文件路径。
    输出：
    1. 包含 db_bytes / wal_bytes / total_bytes 的体积字典。
    用途：
    1. 同时统计主库与 WAL 占用，避免“文件没缩小”的误判。
    边界条件：
    1. 任一文件不存在时，对应大小按 0 处理。
    """

    wal_path = Path(f"{db_path}.wal")
    db_bytes = db_path.stat().st_size if db_path.exists() else 0
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
    return {
        "db_bytes": int(db_bytes),
        "wal_bytes": int(wal_bytes),
        "total_bytes": int(db_bytes + wal_bytes),
    }


def _print_size(prefix: str, size_info: dict[str, int]) -> None:
    """
    输入：
    1. prefix: 输出前缀文本。
    2. size_info: 体积字典。
    输出：
    1. 无返回值。
    用途：
    1. 以统一格式打印 db/wal/total 体积信息。
    边界条件：
    1. 字段缺失时按 0 输出。
    """

    db_bytes = int(size_info.get("db_bytes") or 0)
    wal_bytes = int(size_info.get("wal_bytes") or 0)
    total_bytes = int(size_info.get("total_bytes") or 0)
    print(
        f"{prefix}: db={_bytes_to_human(db_bytes)} ({db_bytes} B), "
        f"wal={_bytes_to_human(wal_bytes)} ({wal_bytes} B), "
        f"total={_bytes_to_human(total_bytes)} ({total_bytes} B)"
    )


def _sql_quote(value: str) -> str:
    """
    输入：
    1. value: 原始 SQL 字符串值。
    输出：
    1. 单引号转义后的 SQL 字面量内容。
    用途：
    1. 安全拼接 EXPORT/IMPORT DATABASE 的路径文本。
    边界条件：
    1. 仅做单引号转义，不做其他 SQL 语义改写。
    """

    return value.replace("'", "''")


def _quote_ident(identifier: str) -> str:
    """
    输入：
    1. identifier: SQL 标识符原始文本。
    输出：
    1. 双引号转义后的 SQL 标识符。
    用途：
    1. 在动态 SQL 中安全引用表名、列名。
    边界条件：
    1. 仅做双引号转义，不校验标识符语义合法性。
    """

    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _list_main_tables_from_connection(con: duckdb.DuckDBPyConnection) -> list[str]:
    """
    输入：
    1. con: 已打开的 DuckDB 连接。
    输出：
    1. main schema 下的基础表名列表（升序）。
    用途：
    1. 在复用连接时查询基准表集合，减少重复建连开销。
    边界条件：
    1. 仅返回 BASE TABLE，不包含视图与临时对象。
    """

    rows = con.execute(
        """
        select table_name
        from information_schema.tables
        where table_schema = 'main'
          and table_type = 'BASE TABLE'
        order by table_name asc
        """
    ).fetchall()
    return [str(row[0]) for row in rows]


def _list_main_tables(db_path: Path) -> list[str]:
    """
    输入：
    1. db_path: DuckDB 文件路径。
    输出：
    1. main schema 下的基础表名列表（升序）。
    用途：
    1. 作为结构与数据一致性校验的基准表集合。
    边界条件：
    1. 仅比较 BASE TABLE，不包含视图与临时对象。
    """

    with duckdb.connect(str(db_path), read_only=True) as con:
        return _list_main_tables_from_connection(con)


def _fetch_table_schema_signature(db_path: Path) -> list[tuple[str, int, str, str, bool, str | None, bool]]:
    """
    输入：
    1. db_path: DuckDB 文件路径。
    输出：
    1. 表结构签名列表：
       (table_name, cid, column_name, column_type, not_null, default_value, is_pk)。
    用途：
    1. 严格比较所有表列定义，确保结构不变。
    边界条件：
    1. 比较维度包含列顺序、类型、非空、默认值、主键标记。
    """

    signature: list[tuple[str, int, str, str, bool, str | None, bool]] = []
    with duckdb.connect(str(db_path), read_only=True) as con:
        table_names = _list_main_tables_from_connection(con)
        for table_name in table_names:
            table_literal = _sql_quote(table_name)
            rows = con.execute(f"select * from pragma_table_info('{table_literal}') order by cid asc").fetchall()
            for row in rows:
                signature.append(
                    (
                        table_name,
                        int(row[0]),
                        str(row[1]),
                        str(row[2]),
                        bool(row[3]),
                        None if row[4] is None else str(row[4]),
                        bool(row[5]),
                    )
                )
    return signature


def _fetch_index_signature(db_path: Path) -> list[tuple[str, str, bool, str, str]]:
    """
    输入：
    1. db_path: DuckDB 文件路径。
    输出：
    1. 索引签名列表：(table_name, index_name, is_unique, expressions, sql)。
    用途：
    1. 比较索引定义，确保索引集合与表达式未发生变化。
    边界条件：
    1. 仅比较 main schema 索引；若库无索引则返回空列表。
    """

    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            """
            select
                table_name,
                index_name,
                coalesce(is_unique, false) as is_unique,
                coalesce(expressions, '') as expressions,
                coalesce(sql, '') as sql
            from duckdb_indexes()
            where schema_name = 'main'
            order by table_name asc, index_name asc
            """
        ).fetchall()
    return [(str(t), str(i), bool(u), str(e), str(s)) for (t, i, u, e, s) in rows]


def _count_table_rows(db_path: Path) -> dict[str, int]:
    """
    输入：
    1. db_path: DuckDB 文件路径。
    输出：
    1. 表名到行数的映射。
    用途：
    1. 快速定位数据量是否发生变化。
    边界条件：
    1. 仅统计 main schema 下的基础表。
    """

    counts: dict[str, int] = {}
    with duckdb.connect(str(db_path), read_only=True) as con:
        for table_name in _list_main_tables_from_connection(con):
            table_ident = _quote_ident(table_name)
            count_value = con.execute(f"select count(*) from {table_ident}").fetchone()[0]
            counts[table_name] = int(count_value or 0)
    return counts


def _has_table_data_diff_in_attached_connection(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """
    输入：
    1. con: 已 attach refdb/candb 的连接。
    2. table_name: 需比较的表名。
    输出：
    1. 存在数据差异返回 True，否则 False。
    用途：
    1. 通过双向 EXCEPT ALL + LIMIT 1 判断表内数据集合是否完全一致。
    边界条件：
    1. 会比较重复行（multiset 语义），可识别重复记录数量差异。
    2. 采用存在性检测而非 count(*)，避免全量扫描差异结果集。
    """

    table_ident = _quote_ident(table_name)
    diff_row = con.execute(
        f"""
        select 1
        from (
            (select * from refdb.main.{table_ident}
             except all
             select * from candb.main.{table_ident})
            union all
            (select * from candb.main.{table_ident}
             except all
             select * from refdb.main.{table_ident})
        ) as diff_rows
        limit 1
        """
    ).fetchone()
    return diff_row is not None


def _validate_db_equivalence(reference_db: Path, candidate_db: Path) -> list[str]:
    """
    输入：
    1. reference_db: 参考数据库路径（通常为压缩前备份）。
    2. candidate_db: 候选数据库路径（当前库或重写产物）。
    输出：
    1. 校验失败信息列表；空列表表示完全一致。
    用途：
    1. 在替换前严格验证“数据不丢失、结构不变、索引不变”。
    边界条件：
    1. 任一维度不一致都会返回可读错误，调用方据此中止替换。
    """

    failures: list[str] = []

    ref_tables = _list_main_tables(reference_db)
    cand_tables = _list_main_tables(candidate_db)
    if ref_tables != cand_tables:
        failures.append("表集合不一致")
        return failures

    ref_schema = _fetch_table_schema_signature(reference_db)
    cand_schema = _fetch_table_schema_signature(candidate_db)
    if ref_schema != cand_schema:
        failures.append("表结构签名不一致")

    ref_indexes = _fetch_index_signature(reference_db)
    cand_indexes = _fetch_index_signature(candidate_db)
    if ref_indexes != cand_indexes:
        failures.append("索引签名不一致")

    ref_counts: dict[str, int] = {}
    cand_counts: dict[str, int] = {}
    ref_sql_path = _sql_quote(reference_db.as_posix())
    cand_sql_path = _sql_quote(candidate_db.as_posix())
    with duckdb.connect(":memory:") as con:
        con.execute(f"attach '{ref_sql_path}' as refdb (read_only)")
        con.execute(f"attach '{cand_sql_path}' as candb (read_only)")

        for table_name in ref_tables:
            table_ident = _quote_ident(table_name)
            ref_count, cand_count = con.execute(
                f"""
                select
                    (select count(*) from refdb.main.{table_ident}) as ref_count,
                    (select count(*) from candb.main.{table_ident}) as cand_count
                """
            ).fetchone()
            ref_counts[table_name] = int(ref_count or 0)
            cand_counts[table_name] = int(cand_count or 0)

            if ref_counts[table_name] == 0 and cand_counts[table_name] == 0:
                continue
            if _has_table_data_diff_in_attached_connection(con, table_name):
                failures.append(f"表数据不一致: {table_name}")

    if ref_counts != cand_counts:
        failures.append("表行数不一致")

    return failures


def _restore_from_backup(backup_path: Path, db_path: Path) -> None:
    """
    输入：
    1. backup_path: 备份文件路径。
    2. db_path: 目标数据库路径。
    输出：
    1. 无返回值。
    用途：
    1. 在关键校验失败时将数据库恢复到压缩前状态。
    边界条件：
    1. 仅恢复主库文件；若存在 WAL 会先删除，避免旧日志干扰。
    """

    wal_path = Path(f"{db_path}.wal")
    if wal_path.exists():
        wal_path.unlink()
    if db_path.exists():
        db_path.unlink()
    shutil.copy2(backup_path, db_path)


def _backup_db_file(db_path: Path) -> Path:
    """
    输入：
    1. db_path: 原始数据库路径。
    输出：
    1. 备份文件路径。
    用途：
    1. 在任何压缩动作前生成可回滚备份。
    边界条件：
    1. 备份文件名带时间戳，避免覆盖历史备份。
    """

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_name(f"{db_path.name}.bak_{ts}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def _run_checkpoint_vacuum(db_path: Path) -> None:
    """
    输入：
    1. db_path: 原始数据库路径。
    输出：
    1. 无返回值。
    用途：
    1. 先执行 force_checkpoint/checkpoint/vacuum，尝试直接回收空洞。
    边界条件：
    1. DuckDB 语句报错时直接上抛，由上层决定是否中止。
    """

    with duckdb.connect(str(db_path), read_only=False) as con:
        con.execute("PRAGMA force_checkpoint")
        con.execute("CHECKPOINT")
        con.execute("VACUUM")


def _rewrite_compact(db_path: Path, export_dir: Path, compact_db_path: Path) -> None:
    """
    输入：
    1. db_path: 原始数据库路径。
    2. export_dir: EXPORT DATABASE 中间目录。
    3. compact_db_path: 重建后的临时 compact 文件路径。
    输出：
    1. 无返回值。
    用途：
    1. 通过 EXPORT/IMPORT 重写数据库布局，实现更彻底的物理压缩。
    边界条件：
    1. 若中间目录已存在会先删除重建，避免历史残留污染结果。
    """

    if export_dir.exists():
        shutil.rmtree(export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)

    if compact_db_path.exists():
        compact_db_path.unlink()
    compact_wal_path = Path(f"{compact_db_path}.wal")
    if compact_wal_path.exists():
        compact_wal_path.unlink()

    export_sql_path = _sql_quote(export_dir.as_posix())
    with duckdb.connect(str(db_path), read_only=False) as con:
        con.execute(f"EXPORT DATABASE '{export_sql_path}'")

    import_sql_path = _sql_quote(export_dir.as_posix())
    with duckdb.connect(str(compact_db_path), read_only=False) as con:
        con.execute(f"IMPORT DATABASE '{import_sql_path}'")
        con.execute("CHECKPOINT")


def _replace_original_db(compact_db_path: Path, db_path: Path) -> None:
    """
    输入：
    1. compact_db_path: 新生成的紧凑数据库文件。
    2. db_path: 原始数据库文件。
    输出：
    1. 无返回值。
    用途：
    1. 用紧凑库替换原库，完成缩容落地。
    边界条件：
    1. 若原库不存在则直接重命名；若存在则先删除再替换。
    """

    if db_path.exists():
        db_path.unlink()
    compact_db_path.replace(db_path)

    compact_wal = Path(f"{compact_db_path}.wal")
    target_wal = Path(f"{db_path}.wal")
    if compact_wal.exists():
        if target_wal.exists():
            target_wal.unlink()
        compact_wal.replace(target_wal)


def _print_reduction(before: dict[str, int], after: dict[str, int], label: str) -> None:
    """
    输入：
    1. before: 压缩前体积字典。
    2. after: 压缩后体积字典。
    3. label: 阶段标签。
    输出：
    1. 无返回值。
    用途：
    1. 输出总占用变化与缩减比例。
    边界条件：
    1. 压缩前总大小为 0 时，比例按 0% 打印。
    """

    before_total = int(before.get("total_bytes") or 0)
    after_total = int(after.get("total_bytes") or 0)
    delta = before_total - after_total
    ratio = (delta / before_total * 100.0) if before_total > 0 else 0.0
    print(f"{label}: total_delta={delta} B, reduction={ratio:.2f}%")


def main() -> int:
    """
    输入：
    1. 无（脚本参数固定，路径来自 app_config）。
    输出：
    1. 进程退出码，成功返回 0。
    用途：
    1. 执行状态库压缩全流程并输出结果摘要。
    边界条件：
    1. 数据库不存在或压缩失败时抛出异常并终止。
    """

    root = _project_root()
    db_path = _load_state_db_path(root)
    if not db_path.exists():
        raise SystemExit(f"状态库不存在: {db_path}")

    print(f"项目根目录: {root}")
    print(f"状态库路径: {db_path}")

    before_size = _collect_db_size(db_path)
    _print_size("压缩前", before_size)

    backup_path = _backup_db_file(db_path)
    print(f"备份完成: {backup_path}")

    print("阶段1: 执行 CHECKPOINT + VACUUM")
    _run_checkpoint_vacuum(db_path)
    after_vacuum_size = _collect_db_size(db_path)
    _print_size("阶段1后", after_vacuum_size)
    _print_reduction(before_size, after_vacuum_size, "阶段1压缩效果")

    print("阶段1校验: 对比备份库与当前库（结构/索引/数据）")
    vacuum_failures = _validate_db_equivalence(backup_path, db_path)
    if vacuum_failures:
        _restore_from_backup(backup_path, db_path)
        raise RuntimeError("阶段1校验失败，已自动回滚: " + "；".join(vacuum_failures[:8]))
    print("阶段1校验通过")

    if after_vacuum_size["total_bytes"] < before_size["total_bytes"]:
        print("结论: 已通过 VACUUM 缩小占用，无需重写压缩。")
        return 0

    print("阶段2: 触发 EXPORT/IMPORT 重写压缩")
    export_dir = root / "_tmp_state_export_compact"
    compact_db_path = db_path.with_name(f"{db_path.stem}.compact{db_path.suffix}")
    _rewrite_compact(db_path, export_dir, compact_db_path)

    print("阶段2校验: 对比备份库与重写产物（结构/索引/数据）")
    compact_failures = _validate_db_equivalence(backup_path, compact_db_path)
    if compact_failures:
        raise RuntimeError("阶段2校验失败，终止替换: " + "；".join(compact_failures[:8]))
    print("阶段2校验通过")

    compact_size = _collect_db_size(compact_db_path)
    _print_size("阶段2产物", compact_size)
    _print_reduction(before_size, compact_size, "阶段2压缩效果")

    if compact_size["total_bytes"] <= after_vacuum_size["total_bytes"]:
        _replace_original_db(compact_db_path, db_path)
        final_size = _collect_db_size(db_path)
        _print_size("替换后", final_size)
        _print_reduction(before_size, final_size, "最终压缩效果")
        print("结论: 已完成重写压缩并替换原库。")
    else:
        print("结论: 重写产物未优于当前库，保留当前库不替换。")
        print(f"可手工检查重写产物: {compact_db_path}")

    if export_dir.exists():
        shutil.rmtree(export_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
