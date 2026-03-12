# A股图形量价筛选平台

更新时间：2026-03-12

## 1. 项目定位

本项目是一个面向 A 股的多策略筛选平台，统一承载三类长流程能力：

1. 筛选任务：按策略组批量扫描股票并输出命中结果与图表窗口。
2. K 线维护任务：更新源行情库，支持 `latest_update` 与 `historical_backfill` 两种模式。
3. 概念更新任务：刷新概念板块与选股理由数据，供策略概念过滤和结果页展示使用。

技术栈：

1. 后端：`FastAPI + DuckDB`
2. 前端：原生 `HTML/JS`
3. 数据侧：`pandas`、`zsdtdx`、`backtrader`（仅兜底路径）

当前策略组目录位于 `strategies/groups/*`，现有策略组包括：

1. `burst_pullback_box_v1`：specialized，放量冲高回落箱体策略。
2. `bu_zhi_dao_v1`：specialized，日线 + 15 分钟组合策略。
3. `flag_rally_v1`：specialized，旗形上涨策略。
4. `xianren_zhilu_v1`：specialized，仙人指路形态策略。
5. `bigbro_buy`：specialized 主路径，保留 backtrader 回退。
6. `strategy_2`：specialized 模板策略，作为后续 AI 和开发者创建新策略的首选参考模板。

## 2. 启动方式

```bash
py -m pip install -r requirements.txt
py run.py
```

访问地址：

1. 监控页：`http://127.0.0.1:8000/`
2. 结果页：`http://127.0.0.1:8000/results`
3. 维护页：`http://127.0.0.1:8000/maintenance`

运行说明：

1. 服务实际监听地址固定为 `0.0.0.0:8000`。
2. `run.py` 会尽量探测并打印可访问的局域网 IPv4 地址，但这只影响展示，不改变监听地址。
3. 启动阶段会读取 `app/zsdtdx_config.yaml`，并按配置决定是否预热 `zsdtdx` 并行抓取器。

## 3. 目录结构

```text
.
├─ run.py
├─ requirements.txt
├─ README.md
├─ HANDOFF.md
├─ screening_state.duckdb
├─ logs/
│  └─ debug/
├─ cache/
│  └─ strategy_features/
├─ scripts/
│  ├─ README.md
│  ├─ prepare_maintenance_refactor.py
│  └─ migrate_db_debug_logs_to_files.py
├─ strategies/
│  ├─ __init__.py
│  ├─ engine_commons.py
│  └─ groups/
│     ├─ bigbro_buy/
│     ├─ burst_pullback_box_v1/
│     ├─ bu_zhi_dao_v1/
│     ├─ flag_rally_v1/
│     ├─ strategy_2/
│     └─ xianren_zhilu_v1/
└─ app/
   ├─ main.py
   ├─ app_config.yaml
   ├─ zsdtdx_config.yaml
   ├─ settings.py
   ├─ api/
   ├─ core/
   ├─ db/
   ├─ models/
   ├─ services/
   └─ static/
```

## 4. 核心模块职责

1. `run.py`：启动 Uvicorn，并为终端输出选择更合适的本机访问地址。
2. `app/main.py`：装配 FastAPI、日志系统、状态库、任务管理器、维护管理器与概念管理器。
3. `app/api/routes.py`：集中定义 API 路由、请求校验、结果拼装、流式接口和前端设置接口。
4. `app/db/market_data.py`：读取源行情库与概念数据，负责图表数据和概念信息查询。
5. `app/db/state_db.py`：持久化任务、日志、结果、维护状态、概念任务与前端配置，当前 schema 版本为 `4`。
6. `app/services/task_manager.py`：筛选任务创建、暂停、恢复、停止、引擎选择与结果写库。
7. `app/services/maintenance_manager.py`：维护任务生命周期与停止控制。
8. `app/services/kline_maintenance.py`：K 线维护执行引擎，支持最新更新与历史回填。
9. `app/services/concept_manager.py`：概念更新任务生命周期管理。
10. `app/services/concept_maintenance.py`：概念数据抓取、过滤、写库与进度汇总。
11. `app/services/simple_api_bridge.py`：统一隔离 zsdtdx simple_api 导入与进程池生命周期，提供自动清理抓取器的上下文封装。
12. `strategies/groups/strategy_2/*`：新策略模板，重点说明 specialized 入口契约、payload 时间窗口合同与概念过滤写法。

## 5. 配置与依赖要点

唯一配置入口是 `app/settings.py`，它会读取并校验 `app/app_config.yaml`。

关键配置分组：

1. `paths`：源库、状态库、日志、静态资源、策略目录路径。
2. `cache`：策略缓存根目录与容量上限。
3. `specialized_engine`：并发槽位、工作线程上限、默认降级策略。
4. `timeframes`：周期顺序与 DuckDB 表映射。
5. `maintenance`：维护窗口、批量参数、停止超时与重试轮次。
6. `concept`：概念抓取超时、并发数、重试次数与板块过滤名单。
7. `parallel_log`、`maintenance_log`、`log_keep`：并行抓取日志压缩策略和日志保留策略。
8. `app`：启动预热与停机优雅等待时间。

关键依赖：

1. `fastapi`、`uvicorn`：Web 服务与路由层。
2. `duckdb`：状态库与源库读写。
3. `pandas`：specialized 策略与维护流程数据处理。
4. `PyYAML`：配置解析。
5. `python-dateutil`：日期处理。
6. `openpyxl`：Excel 导出相关能力的依赖。
7. `zsdtdx`：行情与概念相关数据源接入。
8. `backtrader`：仅用于降级兜底路径，不是新策略推荐主路径。

## 6. 运行链路

### 6.1 筛选任务链路

1. 前端通过 `POST /api/tasks` 创建任务。
2. `TaskManager` 读取策略组 `manifest.json`，决定走 specialized 还是 backtrader 路径。
3. 若配置了概念预筛选，TaskManager 会先调用概念公式过滤股票池，再把过滤后的 `codes` 传入引擎。
4. 结果写入 `tasks`、`task_logs`、`task_results`、`task_stock_states`。
5. 前端通过状态、日志、结果、图表和流式接口展示任务执行情况。

### 6.2 K 线维护任务链路

1. 前端通过 `POST /api/maintenance/jobs` 创建任务。
2. `MaintenanceManager` 调用 `MarketDataMaintenanceService.run_update` 执行 `latest_update` 或 `historical_backfill`。
3. 过程日志写入 `maintenance_logs`，重试任务写入 `maintenance_retry_tasks`。
4. 停止逻辑优先协作取消，必要时会重建并行抓取器。

### 6.3 概念更新任务链路

1. 前端通过 `POST /api/concept/jobs` 创建概念更新任务。
2. `ConceptManager` 驱动 `ConceptMaintenanceService` 拉取、过滤、落库概念数据。
3. 过程状态与日志写入 `concept_jobs`、`concept_logs`。
4. 当概念任务运行中时，平台会拒绝创建新的筛选任务，避免概念过滤使用到不一致的数据快照。

## 7. API 总览

### 7.1 基础与配置接口

1. `GET /api/health`：健康检查。
2. `GET /api/maintenance/runtime-metadata`：维护运行时元数据。
3. `GET /api/strategy-groups`：列出全部策略组。
4. `GET /api/strategy-groups/{group_id}`：读取单个策略组 manifest 信息。
5. `GET/POST /api/ui-settings/monitor`：读取/保存监控页表单配置。
6. `GET/POST /api/ui-settings/maintenance`：读取/保存维护页配置。

### 7.2 筛选任务接口

1. `POST /api/tasks`：创建筛选任务。
2. `POST /api/tasks/{task_id}/pause`：暂停任务。
3. `POST /api/tasks/{task_id}/resume`：恢复任务。
4. `POST /api/tasks/{task_id}/stop`：停止任务。
5. `GET /api/tasks`：任务列表。
6. `GET /api/tasks/{task_id}`：任务状态。
7. `GET /api/tasks/{task_id}/logs`：任务日志。
8. `GET /api/tasks/{task_id}/results`：结果列表。
9. `GET /api/tasks/{task_id}/result-stocks`：结果股票汇总。
10. `GET /api/tasks/{task_id}/result-stock-concepts`：结果股票概念信息。
11. `GET /api/tasks/{task_id}/candles`：K 线数据查询。
12. `GET /api/tasks/{task_id}/stock-chart`：单股图表与窗口高亮。
13. `GET /api/tasks/{task_id}/stock-states`：逐股进度与错误状态。
14. `GET /api/tasks/{task_id}/stream`：任务日志流。
15. `GET /api/tasks/{task_id}/status-stream`：任务状态流。

### 7.3 维护与概念任务接口

1. `POST /api/maintenance/jobs`：创建 K 线维护任务。
2. `GET /api/maintenance/jobs`：维护任务列表。
3. `GET /api/maintenance/jobs/{job_id}`：维护任务状态。
4. `POST /api/maintenance/jobs/{job_id}/stop`：停止维护任务。
5. `GET /api/maintenance/jobs/{job_id}/logs`：维护任务日志。
6. `GET /api/maintenance/jobs/{job_id}/stream`：维护任务日志流。
7. `POST /api/concept/jobs`：创建概念更新任务。
8. `GET /api/concept/jobs`：概念任务列表。
9. `GET /api/concept/jobs/{job_id}`：概念任务状态。
10. `POST /api/concept/jobs/{job_id}/stop`：停止概念任务。
11. `GET /api/concept/jobs/{job_id}/logs`：概念任务日志。
12. `GET /api/concept/jobs/{job_id}/stream`：概念任务日志流。
13. `GET /api/stream/heartbeat`：前端流式连接保活。

## 8. 状态库结构

状态库文件默认为 `screening_state.duckdb`，由 `StateDB.init_schema()` 初始化与升级。

核心表：

1. `tasks`：筛选任务主表。
2. `task_logs`：筛选任务日志。
3. `task_results`：命中结果。
4. `task_stock_states`：逐股执行状态。
5. `maintenance_jobs`：维护任务主表。
6. `maintenance_logs`：维护任务日志。
7. `maintenance_retry_tasks`：维护失败重试任务。
8. `concept_jobs`：概念更新任务主表。
9. `concept_logs`：概念更新任务日志。
10. `_maintenance_meta`：维护过程内部元数据。
11. `app_meta`：应用元数据，包含 `schema_version=4`。

## 9. 策略开发约定

1. 新策略优先复制 `strategies/groups/strategy_2`，不要从旧策略目录直接复制。
2. 新策略默认走 specialized 引擎，`strategy.py` 仅作为 backtrader 协议占位与降级兜底。
3. `manifest.execution.specialized_entry` 必须指向 `engine.py` 中真实存在的函数。
4. 如需概念预筛选，统一写在 `default_params.universe_filters.concepts` 下，推荐字段是 `concept_terms` 与 `reason_terms`。
5. 信号 payload 中的 `chart_interval_start_ts` / `chart_interval_end_ts` 必须表示单次信号的实际图表展示窗口，不能写成全任务跨度或多段历史拼接后的总区间。

## 10. 运行期产物与文档联动

运行期常见产物：

1. `screening_state.duckdb`
2. `logs/app.log*`
3. `logs/debug/*.jsonl`（维护与概念任务的 debug 明细，按 job_id 分文件存储）
4. `cache/strategy_features/<strategy_group_id>/...`

文档协同约束：

1. 涉及接口、配置、任务链路、状态库表结构或策略模板契约变更时，必须同步更新 `README.md` 与 `HANDOFF.md`。
2. 涉及策略开发模板变更时，必须同步更新 `strategies/README.md` 与 `strategies/groups/strategy_2/*` 的说明文本。
3. 文档与代码注释统一使用中文描述目标态，文本编码统一为 UTF-8。
