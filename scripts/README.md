# 工程脚本目录

## 目录定位

存放非常驻运维脚本。

## 文件清单

- `prepare_maintenance_refactor.py`
- `migrate_db_debug_logs_to_files.py`：一次性脚本，将状态库中历史 debug 日志迁移到 `logs/debug/` JSONL 文件并清理 DB 中的 debug 行。
- `reset_maintenance_retry_attempts.py`：一次性脚本，将状态库 `maintenance_retry_tasks` 的 `attempt_count` 全部重置为 0。
- `truncate_maintenance_retry_tasks.py`：一次性脚本，清空状态库 `maintenance_retry_tasks` 表的所有记录。

## 使用方式

1. 预览执行计划（默认 dry-run）：
   - `python scripts/prepare_maintenance_refactor.py`
2. 执行真实改动：
   - `python scripts/prepare_maintenance_refactor.py --apply`

## 职责边界

1. 本目录仅承载对应分层职责，不跨层引入无关编排逻辑。
2. 涉及公共接口、数据结构或配置项变更时，必须同步更新 README/HANDOFF。
3. 代码注释、日志语义与错误信息优先使用中文，确保接手人员可快速理解。

## 维护建议

1. 提交前至少执行一次和本目录强相关的验证（脚本或手工链路检查）。
2. 新增配置参数请落在 `app/app_config.yaml` 并在 `app/settings.py` 完成读取与校验。
3. 当本目录新增子模块时，请同步补全模块级和函数级注释。
