# 图形量价选股交接文档（面向工程代理）

更新时间：2026-03-12

## 1. 接管目标

新会话代理在不依赖历史上下文的前提下，快速掌握：

1. 当前可运行基线与真实目录结构。
2. 筛选、维护、概念更新三条核心链路。
3. 策略组加载入口、策略模板约定与图表 payload 合同。
4. 修改时必须同步的文档、配置、API 模型与前端边界。

## 2. 当前基线

1. 策略目录统一为 `strategies/groups/*`。
2. 当前策略组共 6 个：
   1. `burst_pullback_box_v1`（specialized）
   2. `bu_zhi_dao_v1`（specialized）
   3. `flag_rally_v1`（specialized）
   4. `xianren_zhilu_v1`（specialized）
   5. `bigbro_buy`（specialized 主路径，保留 backtrader 回退）
   6. `strategy_2`（specialized 模板）
3. 服务监听固定 `0.0.0.0:8000`。
4. 状态库 schema 版本：`app_meta.schema_version = "4"`。
5. K 线维护模式仅支持：`latest_update` / `historical_backfill`，默认 `latest_update`。
6. 前端第三方依赖全部本地静态资源，不依赖 CDN。
7. 概念更新任务已并入主系统，运行中会阻止新筛选任务创建。
8. `scripts/` 目录仍保留维护准备脚本，但本轮审计与更新范围不包含 `scripts/`、`tests/` 内的实现文件。

## 3. 目录基线

```text
图形量价选股/
├─ run.py
├─ README.md
├─ HANDOFF.md
├─ requirements.txt
├─ screening_state.duckdb
├─ logs/
│  └─ debug/
├─ cache/
├─ scripts/
│  ├─ README.md
│  ├─ prepare_maintenance_refactor.py
│  └─ migrate_db_debug_logs_to_files.py
├─ strategies/
│  ├─ engine_commons.py
│  └─ groups/
│     ├─ bigbro_buy/
│     ├─ burst_pullback_box_v1/
│     ├─ bu_zhi_dao_v1/
│     ├─ flag_rally_v1/
│     ├─ strategy_2/
│     └─ xianren_zhilu_v1/
└─ app/
   ├─ app_config.yaml
   ├─ zsdtdx_config.yaml
   ├─ settings.py
   ├─ main.py
   ├─ api/
   ├─ core/
   ├─ db/
   ├─ models/
   ├─ services/
   └─ static/
```

## 4. 运行主链路

### 4.1 筛选任务链路

1. `POST /api/tasks` 进入 `app/api/routes.py`。
2. `TaskManager.create_task()` 解析策略组 manifest，决定执行路径：
   1. specialized：调用 `manifest.execution.specialized_entry`
   2. backtrader：走 `app/services/screener.py`
3. 若策略启用了概念预筛选，TaskManager 会先调用概念公式过滤股票池。
4. 结果落库：`tasks`、`task_logs`、`task_results`、`task_stock_states`。
5. 前端通过结果、图表、日志、状态流接口展示任务执行情况。

### 4.2 K 线维护任务链路

1. `POST /api/maintenance/jobs` 进入 `MaintenanceManager.create_job()`。
2. 进入 `MarketDataMaintenanceService.run_update()`，模式仅允许 `latest_update` / `historical_backfill`。
3. 维护状态落库：`maintenance_jobs`、`maintenance_logs`、`maintenance_retry_tasks`、`_maintenance_meta`。
4. 停止逻辑：先协作取消，超时后可触发并行抓取器重建。

### 4.3 概念更新任务链路

1. `POST /api/concept/jobs` 进入 `ConceptManager.create_job()`。
2. `ConceptMaintenanceService` 拉取、过滤、落库概念板块与选股理由数据。
3. 状态与日志落库：`concept_jobs`、`concept_logs`。
4. 筛选任务与概念更新任务互斥：运行中的概念任务会阻止新筛选任务创建。

## 5. 配置边界

唯一配置入口：`app/settings.py`，读取并校验 `app/app_config.yaml`。

修改约束：

1. 新增配置必须同时更新 `app/app_config.yaml` 默认样例与 `app/settings.py` 校验逻辑。
2. 删除配置必须同步清理调用方、前端依赖和文档。
3. 维护模式仅允许 `latest_update` 与 `historical_backfill`，禁止再引入历史别名。
4. 概念任务参数统一归入 `concept` 配置段，不要散落到策略配置或环境变量里。

## 6. 策略组契约

每个策略组目录至少包含：

1. `manifest.json`
2. `strategy.py`
3. `engine.py`（specialized 策略必须提供）

关键字段：

1. `engine`
2. `execution.specialized_entry`
3. `execution.required_timeframes`
4. `default_params`
5. `param_help`

模板约定：

1. 新策略优先复制 `strategies/groups/strategy_2`。
2. `strategy_2` 是 specialized 模板，不是 backtrader 模板。
3. `strategy.py` 在 specialized 策略中通常仅保留 backtrader 协议占位与降级兜底，不应承载主逻辑。

## 7. 图表与结果接口契约

`GET /api/tasks/{task_id}/stock-chart`：

1. `signals[].chart_interval_start_ts` 与 `signals[].chart_interval_end_ts` 必填。
2. `signals[].marker_mode` 由后端返回，值为 `anchor` 或 `interval_center`。
3. `signals[].marker_ts` 由后端统一计算，不依赖前端推断。
4. 若 payload 同时包含 `window_start_ts` / `window_end_ts`，两者应表示命中窗口，而 `chart_interval_*` 表示展示窗口；若语义一致，可以相同。

修改该语义时，必须同步更新：

1. `app/api/routes.py`
2. `app/static/results.js`
3. `strategies/groups/strategy_2/README.md`
4. `strategies/groups/strategy_2/engine.py`

## 8. 重要接口基线

筛选任务相关：

1. `POST /api/tasks`
2. `POST /api/tasks/{task_id}/pause`
3. `POST /api/tasks/{task_id}/resume`
4. `POST /api/tasks/{task_id}/stop`
5. `GET /api/tasks/{task_id}/results`
6. `GET /api/tasks/{task_id}/result-stock-concepts`
7. `GET /api/tasks/{task_id}/candles`
8. `GET /api/tasks/{task_id}/stock-chart`
9. `GET /api/tasks/{task_id}/stock-states`
10. `GET /api/tasks/{task_id}/stream`
11. `GET /api/tasks/{task_id}/status-stream`

维护与概念任务相关：

1. `POST /api/maintenance/jobs`
2. `GET /api/maintenance/jobs/{job_id}`
3. `POST /api/maintenance/jobs/{job_id}/stop`
4. `GET /api/maintenance/jobs/{job_id}/stream`
5. `POST /api/concept/jobs`
6. `GET /api/concept/jobs/{job_id}`
7. `POST /api/concept/jobs/{job_id}/stop`
8. `GET /api/concept/jobs/{job_id}/stream`

前端设置相关：

1. `GET/POST /api/ui-settings/monitor`
2. `GET/POST /api/ui-settings/maintenance`

## 9. 风险点

1. specialized + 进程池在个别环境可能触发 `BrokenProcessPool` 或底层抓取器重建成本偏高。
2. 大规模扫描时缓存增长较快，需关注 `cache.max_bytes` 与策略缓存策略。
3. 若直接修改策略 payload 时间字段，结果页图表初始窗口和标注位置容易失真。
4. 概念数据刷新与筛选任务存在互斥约束，绕过该约束可能导致概念过滤结果不一致。

## 10. 变更联动清单

1. 改接口：更新 `app/models/api_models.py`、`app/api/routes.py`、相关前端调用与 README/HANDOFF。
2. 改配置：更新 `app/app_config.yaml`、`app/settings.py`、README/HANDOFF 和必要的前端默认值。
3. 改策略参数：更新策略 `manifest.json` 的 `default_params` 与 `param_help`，必要时同步更新 `strategy_2` 模板说明。
4. 改图表 payload 语义：同步更新 API 路由、前端结果页和策略模板文档。
5. 改状态库结构：同步更新 `app/db/state_db.py`、README/HANDOFF 与相关查询代码，不保留文档中的旧 schema 描述。
