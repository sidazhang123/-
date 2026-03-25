# 策略组 xianren_zhilu_v1

## 目录定位

`xianren_zhilu_v1` 是 specialized 主路径策略，用于在周线/日线两个周期上独立检测"仙人指路"K线形态，结果取并集（OR）。

## 仙人指路形态

1. 最新K线为阳线且上影线与实体比值 ≥ 阈值。
2. 当根K线放量：成交量 ≥ 前3根均量 × 倍数。
3. 前期均线下降：lookback_bars 根前的 MA 值 ≥ 当前 MA 值 × 比例。

## 当前文件

1. `manifest.json`：定义策略入口、参数默认值和 inline_template 参数说明。
2. `engine.py`：specialized 扫描逻辑（多周期 OR）。
3. `strategy.py`：backtrader 协议占位或降级兜底。
4. `__init__.py`：策略包入口。

## 维护建议

1. 调整参数时，同步更新 `manifest.json` 的 `default_params` 与 `param_help`。
2. 若本策略沉淀出可复用的 specialized 写法，应优先同步回 `strategy_2`，保持模板对后续 AI 仍然有效。