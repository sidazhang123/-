# 策略组 xianren_zhilu_v1

## 目录定位

`xianren_zhilu_v1` 是 specialized 主路径策略，用于识别“仙人指路”类日线形态。

## 当前文件

1. `manifest.json`：定义策略入口、参数默认值和参数说明。
2. `engine.py`：specialized 扫描逻辑。
3. `strategy.py`：backtrader 协议占位或降级兜底。
4. `__init__.py`：策略包入口。

## 维护建议

1. 调整参数时，同步更新 `manifest.json` 的 `default_params` 与 `param_help`。
2. 若本策略沉淀出可复用的 specialized 写法，应优先同步回 `strategy_2`，保持模板对后续 AI 仍然有效。