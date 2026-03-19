# 策略组 weekly_slow_bull_v1

## 目录定位

`weekly_slow_bull_v1` 是 specialized 主路径策略，使用周线与日线数据识别周线慢牛股。

## 规则说明

1. 计算周线 `MA10`、`MA20`。
2. 对每根周线计算四周斜率：`(当前均线 - 4周前均线) / 4`。
3. 要求最近 `ma10_recent_windows` 个不重叠周线窗口的 `MA10` 斜率全部落在 `[ma10_slope_min, ma10_slope_max]`。
4. 同时要求最近 `ma20_recent_windows` 个不重叠周线窗口的 `MA20` 斜率全部落在 `[ma20_slope_min, ma20_slope_max]`。
5. 另外要求最新日线收盘价处于日线 `MA20` 的正负 `close_to_ma20_pct` 范围内，默认是正负 5%。
6. 默认参数对应你的口径：最近 8 个周线窗口的 `MA10` 斜率都在 10 到 20 之间，最近 1 个周线窗口的 `MA20` 斜率在 12 到 20 之间，且最新日线收盘价贴近日线 `MA20`。
7. 这里的窗口按 `slope_gap_weeks` 不重叠采样。例如 `slope_gap_weeks=4` 且 `ma10_recent_windows=3` 时，检查的是 `[t,t-4]`、`[t-4,t-8]`、`[t-8,t-12]` 三个窗口。
8. 命中后，前端信号标签会直接展示最新日线相对 `MA20` 的实际偏离百分比，以及最近一个周线窗口的 `MA10/MA20` 斜率值。
9. 同时 payload 中会写入 `signal_summary`，便于前端详情或后续字段复用，无需重复拼接文案。

## 文件清单

1. `manifest.json`：策略元信息、默认参数和入口配置。
2. `engine.py`：specialized 主扫描逻辑。
3. `strategy.py`：backtrader 协议占位文件。
4. `__init__.py`：包入口。
