# 业务服务目录

## 目录定位

`app/services/` 承载任务编排、维护执行、概念刷新、策略注册和日志适配等业务核心，是应用行为真正发生的地方。

## 当前文件清单

1. `task_manager.py`：筛选任务创建、状态机控制、引擎选择、概念预筛选与结果写库。
2. `task_logger.py`：筛选任务日志适配。
3. `strategy_registry.py`：扫描 `strategies/groups/*/manifest.json` 并加载策略入口。
4. `screener.py`：backtrader 路径筛选执行器。
5. `maintenance_manager.py`：K 线维护任务生命周期与停止控制。
6. `maintenance_logger.py`：维护任务日志适配与 app 日志压缩。
7. `kline_maintenance.py`：K 线维护执行引擎。
8. `maintenance_calendar.py`：维护任务日期计算与高频日历工具。
9. `maintenance_codecs.py`：股票代码归一和数据源编码辅助。
10. `concept_manager.py`：概念更新任务生命周期管理。
11. `concept_logger.py`：概念任务日志适配。
12. `concept_maintenance.py`：概念抓取、过滤、进度统计与写库执行器。
13. `concept_formula.py`：概念过滤公式解析与标准化。
14. `backtest_manager.py`：回测任务生命周期管理与参数扫描编排。
15. `backtest_engine.py`：回测执行引擎，含前瞻指标预计算、并行检测与统计输出。
16. `backtest_stats.py`：回测统计分析，含逐股聚合、基础统计与直方图分箱。
17. `source_schema_migration.py`：源库 schema 迁移辅助逻辑。
18. `simple_api_bridge.py`：统一隔离 zsdtdx simple_api 导入与进程池生命周期，自动在 get_stock_kline 结束后销毁并行抓取器。

## 关键约束

1. `TaskManager` 与 `ConceptManager` 之间存在任务互斥边界，不能绕过。
2. 新策略加载必须通过 `StrategyRegistry` 统一发现，不要在服务层硬编码策略目录。
3. 概念预筛选逻辑属于框架行为，策略 `engine.py` 不应重复实现同一套裁剪。

## 维护建议

1. 新增服务模块时，必须补充模块级中文注释、README 文件清单和职责边界。
2. 业务状态机变更时，要同步更新根 README、HANDOFF 和相关路由/模型文档。
3. 停止逻辑、重试逻辑和降级逻辑属于高风险区域，注释必须写清触发条件和结果语义。
