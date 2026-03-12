# 策略组 `bigbro_buy`

## 目录定位

`bigbro_buy` 默认走 `specialized` 引擎，使用 `d` 日线批量扫描；
`strategy.py` 保留 backtrader 回退逻辑。

## 文件清单

- `__init__.py`
- `manifest.json`
- `strategy.py`（backtrader 回退）
- `engine.py`（specialized 主路径）

## 规则摘要

1. 最近 10 个交易日内，枚举连续 `N>=4` 日窗口。
2. 要求 `阳线数 >= int(N*0.8)`。
3. 要求窗口内 `max((close-open)/open) < 2.5%`。
4. 要求窗口内 `min((close-open)/open) > -1%`。
5. 默认 `last_bar_only=true`，仅在任务区间最后一个交易日输出。

## 维护建议

1. 调整规则参数时同步更新 `manifest.json` 的 `default_params` 与 `param_help`。
2. 若改动 payload 的时间字段，需联调任务结果页区间高亮。
3. 提交前至少执行：
   - `python -m pytest tests/test_bigbro_buy_strategy.py`
   - `python -m pytest tests/test_bigbro_buy_engine.py`
