"""
修复klines_w非周五数据：调整至当周周五或删除（若当周周五已有数据）。
用途：
  遍历所有非周五周线，计算同周周五（向后取，Mon+4, Tue+3, Wed+2, Thu+1）。
  若同code同周周五已有记录 → 删除本条非周五数据。
  若同code同周周五无记录 → 将本条datetime更新为同周周五。
输出：
  先dry-run展示操作计划，确认后执行修改。
边界条件：
  DuckDB dayofweek: 1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri。
  非周五仅可能是Mon-Thu（数据中无Sat/Sun）。
  同周周五 = dt + (5 - dayofweek(dt)) days，保证向后。
"""
import duckdb
import sys

QUANT_DB = r"D:\quant.duckdb"

def run(dry_run=True):
    """
    输入：dry_run=True只统计不修改，False执行实际修改。
    输出：打印操作计划或执行结果。
    边界：quant.duckdb写锁可能冲突，需确保无其他连接。
    """
    con = duckdb.connect(QUANT_DB, read_only=dry_run)

    # 1. 查询所有非周五数据及其同周周五是否已存在
    print("=== 分析非周五数据 ===")
    plan = con.execute("""
        WITH non_friday AS (
            SELECT code, datetime as dt,
                   -- 同周周五 = 向后推到周五: +1(Thu)..+4(Mon)
                   datetime + INTERVAL (5 - dayofweek(datetime)) DAY as target_friday,
                   dayofweek(datetime) as dow, dayname(datetime) as day_name
            FROM klines_w
            WHERE dayofweek(datetime) != 5
        ),
        friday_exists AS (
            SELECT nf.code, nf.dt, nf.target_friday, nf.dow, nf.day_name,
                   CASE WHEN fw.code IS NOT NULL THEN 'delete' ELSE 'update' END as action
            FROM non_friday nf
            LEFT JOIN klines_w fw
                ON nf.code = fw.code
                AND CAST(fw.datetime AS DATE) = CAST(nf.target_friday AS DATE)
        )
        SELECT * FROM friday_exists
        ORDER BY code, dt
    """).fetchall()

    to_delete = [(r[0], r[1]) for r in plan if r[5] == 'delete']
    to_update = [(r[0], r[1], r[2]) for r in plan if r[5] == 'update']

    print(f"  总非周五记录: {len(plan)}")
    print(f"  需删除（当周周五已有数据）: {len(to_delete)}")
    print(f"  需调整至同周周五: {len(to_update)}")

    # 展示删除的sample
    if to_delete:
        print(f"\n=== 将被删除的记录 (共{len(to_delete)}条) ===")
        for code, dt in to_delete:
            # 查详情
            row = con.execute("""
                SELECT datetime, dayname(datetime), open, high, low, close, volume
                FROM klines_w WHERE code = ? AND datetime = ?
            """, [code, dt]).fetchone()
            fri_row = con.execute("""
                SELECT datetime, dayname(datetime), open, high, low, close, volume
                FROM klines_w WHERE code = ?
                  AND dayofweek(datetime) = 5
                  AND CAST(datetime AS DATE) = CAST(? + INTERVAL (5 - dayofweek(?)) DAY AS DATE)
            """, [code, dt, dt]).fetchone()
            print(f"  DELETE: {code} @ {row[0]} ({row[1]}) O={row[2]} H={row[3]} L={row[4]} C={row[5]} V={row[6]}")
            if fri_row:
                print(f"    KEEP:  {code} @ {fri_row[0]} ({fri_row[1]}) O={fri_row[2]} H={fri_row[3]} L={fri_row[4]} C={fri_row[5]} V={fri_row[6]}")

    # 展示调整的sample
    if to_update:
        print(f"\n=== 调整至同周周五的记录 (前20条) ===")
        for code, dt, target in to_update[:20]:
            row = con.execute("""
                SELECT dayname(datetime), open, high, low, close, volume
                FROM klines_w WHERE code = ? AND datetime = ?
            """, [code, dt]).fetchone()
            print(f"  UPDATE: {code} {str(dt)[:10]}({row[0]}) -> {str(target)[:10]}(Friday) O={row[1]} H={row[2]} L={row[3]} C={row[4]} V={row[5]}")

    if dry_run:
        print(f"\n=== DRY RUN 完成，未做任何修改 ===")
        print(f"若确认无误，请运行: py scripts/fix_klines_w_non_friday.py execute")
        con.close()
        return

    # 实际执行
    print(f"\n=== 开始执行修改 ===")

    # 先删除（当周周五已有数据的非周五记录）
    if to_delete:
        print(f"  删除 {len(to_delete)} 条...")
        for code, dt in to_delete:
            con.execute("DELETE FROM klines_w WHERE code = ? AND datetime = ?", [code, dt])
        print(f"  删除完成")

    # 再更新（删除完成后，剩余非周五记录全部调整至同周周五）
    remaining_nf = con.execute("""
        SELECT COUNT(*) FROM klines_w WHERE dayofweek(datetime) != 5
    """).fetchone()[0]
    if remaining_nf > 0:
        print(f"  更新 {remaining_nf} 条日期至同周周五...")
        con.execute("""
            UPDATE klines_w
            SET datetime = datetime + INTERVAL (5 - dayofweek(datetime)) DAY
            WHERE dayofweek(datetime) != 5
        """)
        print(f"  更新完成")

    # 验证
    remaining = con.execute("""
        SELECT COUNT(*) FROM klines_w WHERE dayofweek(datetime) != 5
    """).fetchone()[0]
    print(f"\n=== 验证: 剩余非周五记录 = {remaining} ===")
    if remaining == 0:
        print("  全部已修复！")
    else:
        print(f"  警告: 仍有 {remaining} 条非周五记录")

    con.close()
    print("\n=== 修数完成 ===")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "dryrun"
    if mode == "execute":
        run(dry_run=False)
    else:
        run(dry_run=True)
