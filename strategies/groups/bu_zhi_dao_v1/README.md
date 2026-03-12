# 策略组 bu_zhi_dao_v1

## 目录定位

`bu_zhi_dao_v1` 是 specialized 主路径策略，目录内保存 manifest、specialized 引擎和 backtrader 协议占位。

## 当前文件

1. `manifest.json`：策略入口与参数声明。
2. `engine.py`：主扫描逻辑。
3. `strategy.py`：协议占位和可能的回退路径。
4. `__init__.py`：包入口。

## 维护建议

1. 参数语义、payload 字段或执行周期发生变化时，要同步更新本 README、manifest 和策略模板文档。
2. 如果引入通用的写法或字段约定，优先回写到 `strategy_2`，避免模板和现役策略长期分叉。
