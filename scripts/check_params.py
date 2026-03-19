import duckdb
import json

con = duckdb.connect('screening_state.duckdb')
res = con.execute("""
    SELECT task_id, params_json, created_at 
    FROM tasks 
    WHERE strategy_group_id='weekly_slow_bull_v1' 
    ORDER BY created_at DESC 
    LIMIT 1
""").fetchone()

print('task_id:', res[0])
print('created_at:', res[2])
params = json.loads(res[1])
print('params:', json.dumps(params, indent=2))
