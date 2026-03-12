# 策略组目录规范

`strategies/groups/` 下的每个同级目录代表一个可被框架自动发现的策略组。当前目录既包含实际业务策略，也包含权威模板 `strategy_2`。

## 当前目录基线

1. `strategy_2/`：specialized 模板，后续 AI 与开发者创建新策略时应优先复制它。
2. `bigbro_buy/`、`burst_pullback_box_v1/`、`bu_zhi_dao_v1/`、`flag_rally_v1/`、`xianren_zhilu_v1/`：当前实际策略组。

## 标准结构

```text
strategies/groups/<strategy_group_id>/
├─ __init__.py
├─ manifest.json
├─ strategy.py
└─ engine.py              # specialized 策略必须提供
```

## 关键约定

1. `manifest.json`
   - `id` 必须唯一，并与目录名一致。
   - `module` 通常写为 `strategies.groups.<id>`。
   - `engine` 推荐使用 `specialized`。
   - 当 `engine = specialized` 时，必须配置 `execution.specialized_entry` 与 `execution.required_timeframes`。
2. `strategy.py`
   - 必须暴露 `GROUP_HANDLERS`。
   - 对 specialized 策略来说，通常只保留 backtrader 协议占位和降级兜底，不承载主逻辑。
3. `engine.py`
   - specialized 主入口由 `execution.specialized_entry` 指向。
   - 若启用缓存，缓存目录统一落到 `cache/strategy_features/<strategy_group_id>/`。

## 推荐实践

1. 新策略不要从旧策略目录直接复制，优先复制 `strategy_2/`。
2. 若策略需要概念预筛选，统一写在 `default_params.universe_filters.concepts` 下。
3. 策略文档必须写清 payload 时间窗口语义，避免结果页图表失真。

