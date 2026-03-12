# 策略组 flag_rally_v1

## 目录定位

`flag_rally_v1` 是 specialized 主路径策略，使用日线与 15 分钟数据识别旗形上涨形态。

## 当前文件

1. `manifest.json`：定义 specialized 入口、所需周期和参数说明。
2. `engine.py`：旗形上涨扫描逻辑。
3. `strategy.py`：backtrader 协议占位或降级兜底。
4. `__init__.py`：策略包入口。

## 维护建议

1. 此目录不是模板目录，不要把它当成新策略的起点；新策略应优先复制 `strategy_2`。
2. 若调整旗形定义或输出窗口，必须同步更新 manifest 参数说明和结果页展示契约。
