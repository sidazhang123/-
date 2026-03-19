import duckdb
import json

con = duckdb.connect('screening_state.duckdb')

# 查看最近一次任务的结果
result = con.execute("""
    SELECT result_id, code, signal_dt, clock_tf, signal_label, rule_payload_json
    FROM task_results 
    WHERE task_id = '80c9ab01-17c6-4976-90a5-59f4ab9444d9'
    ORDER BY signal_dt DESC
    LIMIT 5
""").fetchone()

if result:
    print('result_id:', result[0])
    print('code:', result[1])
    print('signal_dt:', result[2])
    print('clock_tf:', result[3])
    print('signal_label:', result[4])
    print('rule_payload_json:')
    payload = json.loads(result[5])
    print(json.dumps(payload, indent=2, ensure_ascii=False))
