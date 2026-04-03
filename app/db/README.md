# 数据访问目录

## 目录定位

`app/db/` 封装两类 DuckDB 访问能力：

1. 源行情库与概念数据读取。
2. 状态库的 schema 管理、任务写入、日志查询和前端设置持久化。

## 当前文件

1. `market_data.py`：读取源库中的股票列表、K 线数据、概念信息和图表所需数据。
2. `state_db.py`：管理状态库 schema 初始化、版本升级、任务/维护/概念任务 CRUD。
3. `duckdb_utils.py`：DuckDB 兼容连接、文件锁识别和 shadow copy 回退工具。
4. `__init__.py`：包初始化。

## 状态库要点

1. `StateDB` 当前 schema 版本为 `8`。
2. 核心表包括 `tasks`、`task_logs`、`task_results`、`maintenance_jobs`、`maintenance_logs`、`maintenance_retry_tasks`、`concept_jobs`、`concept_logs`、`_maintenance_meta`、`app_meta`。
3. 监控页和维护页表单配置也由状态库存储，不再是纯前端本地状态。

## 源库读取要点

1. `market_data.py` 会在必要时使用 shadow copy 只读副本，避免源库被占用时查询长期阻塞。
2. 图表查询和概念查询都属于正式数据访问能力，修改字段语义时要同步检查 API 层和前端使用方。

## 维护建议

1. 任何表结构调整都必须同步更新 `README.md`、`HANDOFF.md` 和相关 API/服务层注释。
2. 连接重试、shadow copy 和文件锁识别属于关键稳定性逻辑，修改时要在注释中写清回退策略。
3. 若新增状态表或元数据表，命名和职责要在本 README 中记录，避免文档继续停留在旧 schema。
