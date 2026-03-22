# API 路由目录

## 目录定位

`app/api/` 定义前后端之间的 HTTP 协议边界。这里不仅负责常规的请求校验和响应拼装，还负责把任务系统、维护系统、概念更新系统以及流式日志接口统一暴露给前端页面。

## 当前文件

1. `routes.py`：唯一路由文件，集中定义全部 `/api/*` 端点。
2. `__init__.py`：包初始化，占位文件。

## 接口分类

1. 基础与配置：`/health`、`/maintenance/runtime-metadata`、`/strategy-groups/*`、`/ui-settings/*`。
2. 筛选任务：创建、暂停、恢复、停止、状态、结果、日志、图表与流式状态接口。
3. K 线维护任务：创建、列表、状态、停止、日志与日志流接口。
4. 概念更新任务：创建、列表、状态、停止、日志与日志流接口。
5. 流式辅助：`/stream/heartbeat` 用于前端保持连接活性。

## 关键协议约定

1. 图表接口依赖后端统一生成 `chart_interval_start_ts`、`chart_interval_end_ts`、`marker_ts` 和 `marker_mode`，前端不自行猜测。
2. `ui-settings/monitor` 与 `ui-settings/maintenance` 会把页面表单配置持久化到状态库，属于正式接口而不是临时缓存。
3. 路由层负责把业务异常转换成稳定的 HTTP 错误语义，但不负责策略计算与维护执行本身。

## 维护建议

1. 新增或修改端点时，必须同步更新 `app/models/api_models.py`、根 README、HANDOFF 和相关前端调用。
2. 调整图表窗口或日志流语义时，必须联动 `app/static/results.js`、`app/static/maintenance.js`、`app/static/monitor.js`。
3. 路由辅助函数如果包含多级回退或字段兼容逻辑，注释必须写清输入优先级和返回语义，不能保留空泛模板注释。
