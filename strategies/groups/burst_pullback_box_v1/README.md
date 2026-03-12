# 策略组 burst_pullback_box_v1

## 目录定位

`burst_pullback_box_v1` 是 specialized 主路径策略，目录内同时包含 manifest、specialized 引擎和 backtrader 协议占位文件。

## 当前文件

1. `manifest.json`：策略元信息、执行入口、参数默认值和参数说明。
2. `engine.py`：specialized 主扫描逻辑。
3. `strategy.py`：backtrader 协议占位或回退逻辑。
4. `__init__.py`：策略组包入口。

## 维护建议

1. 修改参数时，要同步更新 `manifest.json` 的 `default_params` 与 `param_help`。
2. 修改 payload 时间字段时，要联动结果页图表窗口高亮契约。
3. 若将本策略中的实现经验抽象成通用模板，应优先沉淀到 `strategy_2` 或 `strategies/README.md`，而不是只留在本目录内。
