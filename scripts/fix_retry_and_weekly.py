"""
修复分钟线175486异常retry任务 + 周线非周五数据排查脚本。
用途：
  - 查询retry表中175486异常任务并重置attempt_count
  - 确认异常code对应d线有无数据、集中日期
  - 查找klines_w中非周五数据并生成CSV报告
"""
import duckdb
import sys
import os
import csv
from datetime import timedelta

STATE_DB = os.path.join(os.path.dirname(os.path.dirname(__file__)), "screening_state.duckdb")
QUANT_DB = r"D:\quant.duckdb"

def step1_find_175486():
    """
    查找retry表中last_error包含175486的记录。
    输入：无
    输出：打印匹配记录，返回匹配行列表
    边界：无匹配时返回空列表
    """
    con = duckdb.connect(STATE_DB, read_only=True)
    rows = con.execute("""
        SELECT task_key, mode, code, freq, start_date, end_date,
               attempt_count, last_status, last_error, created_at, updated_at
        FROM maintenance_retry_tasks
        WHERE last_error LIKE '%175486%'
        ORDER BY code, freq
    """).fetchall()
    print(f"=== 175486 异常 retry 任务: {len(rows)} 条 ===")
    for r in rows:
        print(f"  task_key={r[0]}, mode={r[1]}, code={r[2]}, freq={r[3]}, "
              f"start={r[4]}, end={r[5]}, attempts={r[6]}, status={r[7]}")
        print(f"    error: {r[8][:120] if r[8] else 'N/A'}")
    con.close()
    return rows

def step2_check_daily_data(rows):
    """
    检查异常code在quant.duckdb的klines_d中是否有数据，以及日期分布。
    输入：rows - step1返回的retry记录列表
    输出：打印日线数据情况
    边界：code在d表无数据时标记
    """
    codes = list(set(r[2] for r in rows))
    print(f"\n=== 检查 {len(codes)} 个code的日线数据 ===")
    con = duckdb.connect(QUANT_DB, read_only=True)
    for code in sorted(codes):
        result = con.execute("""
            SELECT COUNT(*), MIN(dt), MAX(dt) FROM klines_d WHERE code = ?
        """, [code]).fetchone()
        cnt, min_dt, max_dt = result
        print(f"  {code}: d线 {cnt} 条, 范围 {min_dt} ~ {max_dt}")
    
    # 按日期分布统计
    dates = {}
    for r in rows:
        key = (str(r[4]), str(r[5]))
        dates[key] = dates.get(key, 0) + 1
    print(f"\n=== 日期分布 (start_date, end_date): {len(dates)} 组 ===")
    for (s, e), cnt in sorted(dates.items(), key=lambda x: -x[1])[:20]:
        print(f"  {s} ~ {e}: {cnt} 条")
    
    # 按freq分布
    freqs = {}
    for r in rows:
        freqs[r[3]] = freqs.get(r[3], 0) + 1
    print(f"\n=== 频率分布 ===")
    for f, cnt in sorted(freqs.items()):
        print(f"  freq={f}: {cnt} 条")
    
    con.close()

def step3_reset_attempt_count():
    """
    将175486异常retry任务的attempt_count重置为0。
    输入：无
    输出：打印受影响行数
    边界：若无匹配行则不修改
    """
    con = duckdb.connect(STATE_DB, read_only=False)
    result = con.execute("""
        UPDATE maintenance_retry_tasks
        SET attempt_count = 0, updated_at = CURRENT_TIMESTAMP
        WHERE last_error LIKE '%175486%'
    """)
    affected = con.execute("SELECT changes()").fetchone()[0]
    print(f"\n=== 重置 attempt_count: {affected} 条受影响 ===")
    con.close()

def step4_find_non_friday_weekly():
    """
    在quant.duckdb的klines_w中查找非周五的数据。
    输入：无
    输出：打印非周五数据统计
    边界：若无非周五数据则提示
    """
    con = duckdb.connect(QUANT_DB, read_only=True)
    # DuckDB dayofweek: 0=Sunday, 1=Monday, ..., 5=Friday, 6=Saturday
    rows = con.execute("""
        SELECT code, dt, open, high, low, close, volume,
               dayofweek(dt) as dow, dayname(dt) as day_name
        FROM klines_w
        WHERE dayofweek(dt) != 5
        ORDER BY code, dt
    """).fetchall()
    print(f"\n=== klines_w 非周五数据: {len(rows)} 条 ===")
    if rows:
        for r in rows[:20]:
            print(f"  code={r[0]}, dt={r[1]}, dow={r[7]}({r[8]}), O={r[2]} H={r[3]} L={r[4]} C={r[5]} V={r[6]}")
        if len(rows) > 20:
            print(f"  ... 共 {len(rows)} 条")
    con.close()
    return rows

def step5_generate_csv_report(non_friday_rows):
    """
    生成CSV报告，展示非周五数据前后5天内的同code其他数据。
    输入：non_friday_rows - 非周五周线数据列表
    输出：生成CSV文件
    边界：前后5天内无数据时留空
    """
    if not non_friday_rows:
        print("无非周五数据，跳过CSV生成")
        return
    
    con = duckdb.connect(QUANT_DB, read_only=True)
    
    report_rows = []
    for r in non_friday_rows:
        code, dt = r[0], r[1]
        # 查找前后5天的同code数据
        neighbors = con.execute("""
            SELECT dt, dayofweek(dt) as dow, dayname(dt) as day_name,
                   open, high, low, close, volume
            FROM klines_w
            WHERE code = ? AND dt != ? AND dt BETWEEN ?::DATE - INTERVAL '5 days' AND ?::DATE + INTERVAL '5 days'
            ORDER BY dt
        """, [code, dt, dt, dt]).fetchall()
        
        neighbor_info = ""
        has_friday_neighbor = False
        for n in neighbors:
            n_dt, n_dow, n_day = n[0], n[1], n[2]
            neighbor_info += f"{n_dt}({n_day}) "
            if n_dow == 5:
                has_friday_neighbor = True
        
        report_rows.append({
            "code": code,
            "dt": str(dt),
            "weekday": r[8],
            "open": r[2],
            "high": r[3],
            "low": r[4],
            "close": r[5],
            "volume": r[6],
            "前后5天同code数据": neighbor_info.strip() if neighbor_info else "无",
            "前后5天有周五数据": "是" if has_friday_neighbor else "否",
            "操作(删除本条非周五数据/将本条非周五数据日期调整至同周周五)": ""
        })
    
    con.close()
    
    csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "klines_w_non_friday_report.csv")
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=report_rows[0].keys())
        writer.writeheader()
        writer.writerows(report_rows)
    print(f"\n=== CSV报告已生成: {csv_path}, 共 {len(report_rows)} 条 ===")

if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "all"
    
    if action in ("find", "all"):
        rows = step1_find_175486()
        if rows:
            step2_check_daily_data(rows)
    
    if action in ("reset", "all"):
        step3_reset_attempt_count()
    
    if action in ("weekly", "all"):
        nf_rows = step4_find_non_friday_weekly()
        step5_generate_csv_report(nf_rows)
