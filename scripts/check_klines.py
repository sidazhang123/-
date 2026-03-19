import duckdb
import pandas as pd

# 读取源数据库
con = duckdb.connect('D:/quant.duckdb')

# 查看有哪些表
tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
print('可用表:')
for t in tables[:20]:
    print(f'  {t[0]}')

# 检查表结构
cols = con.execute("PRAGMA table_info(klines_w)").fetchall()
print('klines_w表结构:')
for c in cols:
    print(f'  {c[1]}')

# 检查周线数据
df = con.execute("""
    SELECT code, datetime, close 
    FROM klines_w 
    WHERE code IN ('sh.688498', 'sz.000001') 
    ORDER BY code, datetime DESC 
    LIMIT 20
""").fetchdf()
print('\n周线数据:')
print(df)
