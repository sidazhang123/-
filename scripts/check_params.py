"""
查看指定策略组最近一条任务的参数。

用法：
    py scripts/check_params.py <strategy_group_id>
示例：
    py scripts/check_params.py consecutive_uptrends_v1
"""

import sys
import duckdb
import json

if len(sys.argv) < 2:
    print("用法: py scripts/check_params.py <strategy_group_id>")
    sys.exit(1)

strategy_group_id = sys.argv[1]

con = duckdb.connect('screening_state.duckdb')
res = con.execute("""
    SELECT task_id, params_json, created_at 
    FROM tasks 
    WHERE strategy_group_id = $1
    ORDER BY created_at DESC 
    LIMIT 1
""", [strategy_group_id]).fetchone()

if not res:
    print(f"未找到策略组 '{strategy_group_id}' 的任务记录。")
    sys.exit(0)

print('task_id:', res[0])
print('created_at:', res[2])
params = json.loads(res[1])
print('params:', json.dumps(params, indent=2))
