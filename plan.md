## Plan: screening_state 与 quant 优化实施

目标是在不改变现有 API 协议、日志游标语义、读写分离约束和源库表业务口径的前提下，先完成一轮低风险高收益优化。推荐顺序是：先处理 `screening_state.duckdb` 的写放大，再处理 `quant.duckdb` 的读连接复用与重复读取，最后再评估维护写入模型和索引层优化。这样可以先压掉最稳定、最确定的损耗点，再进入较大改造面。

**Steps**
1. Phase 0: 建立基线与埋点。
   在不改语义的前提下，为 `screening_state` 与 `quant` 两条链路建立最小性能基线。统计项至少包括：单只股票筛选触发的 `append_log` / `update_task_fields` / `add_result` / `upsert_stock_state` 次数；单次任务启动阶段 `stocks` 表读取次数；单只股票多周期 `fetch_ohlcv` 的连接创建次数；单次维护 flush 的 delete / insert 批次数。该阶段阻塞全部后续步骤。
2. Phase 1A: 优化 `screening_state.duckdb` 写路径事务边界。
   在 [app/db/state_db.py](c:/Users/sida/Desktop/图形量价选股/app/db/state_db.py) 中优先处理 `append_log`、`append_maintenance_log`、`append_concept_log`、`add_result`、`update_task_fields`、`upsert_stock_state`。目标不是改表结构，而是把当前一组复合写操作收敛到更明确的事务边界，减少 DuckDB 自动提交次数，并保持现有双连接读写分离模型不变。该阶段阻塞 3-5。
3. Phase 1B: 优化 `screening_state.duckdb` 日志裁剪策略。
   保留 [tests/test_state_db_log_cursor_and_retention.py](c:/Users/sida/Desktop/图形量价选股/tests/test_state_db_log_cursor_and_retention.py) 已锁定的语义，但重构 `_trim_task_logs`、`_trim_maintenance_logs`、`_trim_concept_logs` 的触发时机，避免“每写一条就做一次 count/trim 检查”。优先方案是阈值触发或批次触发；禁止改变 after_log_id 分页、保留最近任务数、单任务日志保留上限这三类合同。该阶段依赖 2。
4. Phase 1C: 降低筛选任务逐股写放大。
   在 [app/services/task_manager.py](c:/Users/sida/Desktop/图形量价选股/app/services/task_manager.py) 的 `_run_task` 与 `_run_specialized_engine` 中，把 `processed_stocks`、`progress`、`summary_json` 写入改为节流更新；把逐 signal 调用 `add_result` 评估为小批量 flush；把逐股 `upsert_stock_state` 收敛为更少的写次数；保留 `current_code` 的实时更新和任务终态写入的实时性。该阶段依赖 2，可与 5 并行。
5. Phase 1D: 合并 `screening_state.duckdb` 恢复阶段冗余读。
   在 [app/db/state_db.py](c:/Users/sida/Desktop/图形量价选股/app/db/state_db.py) 中新增统一读取接口，合并 `get_processed_stock_codes` 与 `get_result_code_set` 的两次查询，供 [app/services/task_manager.py](c:/Users/sida/Desktop/图形量价选股/app/services/task_manager.py) 启动恢复、specialized 失败后降级恢复路径复用。目标是减少任务恢复时的多次独立查询。该阶段依赖 1，可与 4 并行。
6. Phase 2A: 优化 `quant.duckdb` 任务读连接复用。
   在 [app/db/market_data.py](c:/Users/sida/Desktop/图形量价选股/app/db/market_data.py) 中引入任务级只读会话复用或等价的连接复用层，覆盖 `get_all_stocks`、`resolve_stock_inputs`、`filter_codes_with_complete_range`、`fetch_ohlcv`。目标是避免 backtrader 路径“一只股票多个周期多次开关连接”，同时保持现有 shadow copy 回退和连接冲突兼容行为不变。该阶段可独立于 Phase 1 执行，但建议排在 `screening_state` 首轮优化之后。
7. Phase 2B: 消除 `quant.duckdb` 启动阶段重复读取。
   围绕 [app/db/market_data.py](c:/Users/sida/Desktop/图形量价选股/app/db/market_data.py) 的 `get_all_stocks` 与 `resolve_stock_inputs`，改为共享同一份 `stocks` 快照或同一连接内读取结果，避免同一任务启动阶段重复扫 `stocks` 表。需要同步检查 [app/api/routes.py](c:/Users/sida/Desktop/图形量价选股/app/api/routes.py) 中结果页/图表页对 `MarketDataDB` 的调用是否也能复用相同模式。该阶段依赖 6。
8. Phase 2C: 统一 specialized 读源库连接策略。
   将 [strategies/engine_commons.py](c:/Users/sida/Desktop/图形量价选股/strategies/engine_commons.py) 的 `connect_source_readonly` 与主应用的 DuckDB 兼容连接策略对齐，避免 specialized 引擎绕过 `connect_duckdb_compatible`。目标是让 specialized 路径也具备配置冲突回退、文件占用回退 shadow copy 的能力，并降低不同路径行为不一致带来的隐患。该阶段依赖 6，可与 7 并行。
9. Phase 3A: 优化 `quant.duckdb` 维护写入事务与 flush 粒度。
   围绕 [app/services/kline_maintenance.py](c:/Users/sida/Desktop/图形量价选股/app/services/kline_maintenance.py) 的 `_flush_freq_rows`、`_flush_all_buffers` 与主循环中的 flush 时机，确认 delete + insert 组合始终在可控事务边界内执行；复核 `write_flush_rows` 默认值是否适配当前机器内存与数据量；保留现有 delete-then-insert 业务语义，不在首轮改成更激进的 merge/upsert 模式。该阶段可与 8 并行。
10. Phase 3B: 优化 `quant.duckdb` 的概念整表刷新写法。
    围绕 [app/services/concept_maintenance.py](c:/Users/sida/Desktop/图形量价选股/app/services/concept_maintenance.py) 的 `_replace_all_records`，评估把 `delete from stock_concepts + executemany insert` 改为“临时表装载后原子替换”或至少补上显式事务边界。目标是缩小表短暂为空的窗口，降低全量刷新时的写入峰值和中断风险。该阶段不阻塞其他 Phase 3 工作。
11. Phase 4: 最后评估 schema / index 微调。
    只有在完成前述改造并拿到真实基线对比后，才评估是否为 `screening_state` 的 `task_results`、`task_stock_states`、`maintenance_retry_tasks` 或 `quant` 的 `stocks` / `stock_concepts` / K 线表补充索引或调整查询模式。DuckDB 在高更新表上的额外索引可能拖慢写入，因此这一步明确后置，且必须基于基线数据决策。

**Relevant files**
- `c:/Users/sida/Desktop/图形量价选股/app/db/state_db.py` — `screening_state.duckdb` 的核心读写封装；重点关注 `append_log`、`append_maintenance_log`、`append_concept_log`、`add_result`、`update_task_fields`、`upsert_stock_state`、`get_processed_stock_codes`、`get_result_code_set`。
- `c:/Users/sida/Desktop/图形量价选股/app/services/task_manager.py` — 筛选任务主循环；当前逐股日志、进度、结果、stock state 写入都在这里放大。
- `c:/Users/sida/Desktop/图形量价选股/app/db/market_data.py` — `quant.duckdb` 主读路径；重点关注 `_connect`、`get_all_stocks`、`resolve_stock_inputs`、`filter_codes_with_complete_range`、`fetch_ohlcv`。
- `c:/Users/sida/Desktop/图形量价选股/strategies/engine_commons.py` — specialized 直连 `quant.duckdb` 的只读入口，需要与主应用兼容连接策略对齐。
- `c:/Users/sida/Desktop/图形量价选股/app/services/kline_maintenance.py` — `quant.duckdb` K 线维护写入路径；重点关注 `_flush_freq_rows`、`_flush_all_buffers`、`_load_latest_by_freq`、`_build_historical_*` 相关读写。
- `c:/Users/sida/Desktop/图形量价选股/app/services/concept_maintenance.py` — `quant.duckdb` 的 `stock_concepts` 全量刷新路径；重点关注 `_load_stock_inputs` 与 `_replace_all_records`。
- `c:/Users/sida/Desktop/图形量价选股/app/api/routes.py` — 结果页、图表页对 `MarketDataDB` 的使用点，检查是否需要一并对齐连接复用方式。
- `c:/Users/sida/Desktop/图形量价选股/tests/test_state_db_concurrency.py` — 锁定 `screening_state.duckdb` 的读写分离与读延迟约束。
- `c:/Users/sida/Desktop/图形量价选股/tests/test_state_db_log_cursor_and_retention.py` — 锁定 `screening_state.duckdb` 的日志游标与保留策略合同。
- `c:/Users/sida/Desktop/图形量价选股/tests/test_market_data_connection_fallback.py` — 锁定 `quant.duckdb` 读连接的配置冲突与 shadow copy 回退行为。
- `c:/Users/sida/Desktop/图形量价选股/tests/test_kline_maintenance_refactor.py` — 锁定 `quant.duckdb` 维护引擎的任务构建与写入契约。
- `c:/Users/sida/Desktop/图形量价选股/app/app_config.yaml` — flush 阈值、日志保留、轮询超时等配置来源；若新增节流阈值应统一落在这里。

**Verification**
1. 建立两套最小基准：`screening_state` 关注单次筛选任务总写次数、日志页轮询响应时延、任务恢复路径查询次数；`quant` 关注单任务读取连接创建次数、单股票多周期读取耗时、单次维护 flush 总耗时。
2. 运行 [tests/test_state_db_concurrency.py](c:/Users/sida/Desktop/图形量价选股/tests/test_state_db_concurrency.py)，确认 `screening_state` 的双连接读写分离和读延迟 SLA 未被破坏。
3. 运行 [tests/test_state_db_log_cursor_and_retention.py](c:/Users/sida/Desktop/图形量价选股/tests/test_state_db_log_cursor_and_retention.py) 与 [tests/test_state_db_retry_tasks.py](c:/Users/sida/Desktop/图形量价选股/tests/test_state_db_retry_tasks.py)，确认日志游标、保留策略、retry 批量逻辑保持兼容。
4. 运行 [tests/test_market_data_connection_fallback.py](c:/Users/sida/Desktop/图形量价选股/tests/test_market_data_connection_fallback.py)，确认 `quant.duckdb` 的只读连接仍保留配置冲突兼容与 shadow copy 回退语义。
5. 运行 [tests/test_kline_maintenance_refactor.py](c:/Users/sida/Desktop/图形量价选股/tests/test_kline_maintenance_refactor.py)，确认维护引擎任务构建、写入预处理与重试轮次语义未改变。
6. 做两次手工回归：一次中等股票数筛选任务，核对监控页/结果页/日志页是否仍实时正常；一次 `latest_update` 或 `historical_backfill`，核对 `quant.duckdb` 写入结果与停止行为不变。
7. 只有在前 3 个阶段完成后，再做基线对比，决定是否进入 Phase 4 的索引与 schema 微调。

**Decisions**
- 本计划只覆盖主应用链路：`app/`、`run.py`、API、筛选任务、维护任务、概念任务；不纳入 `concept.py` 与 `手动bs修数/get_bs_data.py`。
- 优先级按“低风险高收益”排序：先压 `screening_state` 写放大，再做 `quant` 读连接复用和重复读取优化，再做维护写入模型优化。
- 不改变现有 API 协议，不改变 `screening_state` 的 after_log_id 游标语义、日志保留语义、读写分离结构。
- 不改变 `quant` 当前 K 线表与概念表的业务口径，首轮只优化连接、事务和批量策略，不直接重做表设计。
- 新增阈值或节流参数时，统一通过 [app/app_config.yaml](c:/Users/sida/Desktop/图形量价选股/app/app_config.yaml) 配置，不新增 CLI 参数。

**Further Considerations**
1. 是否接受把筛选任务的部分高频 info 日志降为 debug。推荐先不作为首轮默认项，避免影响现有监控体验。
2. 是否允许为 `MarketDataDB` 引入显式 `close()` 或任务级上下文对象。推荐允许，这是 `quant` 首轮优化最自然的落点。
3. 是否在首轮同步补性能测试桩。推荐补最小版本，至少覆盖 100-500 只股票筛选和一次维护 flush 两个场景。