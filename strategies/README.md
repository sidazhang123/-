# 策略编写指南

> 本文档面向后续 AI 和开发者。结论先行：新增策略时，默认复制 `strategies/groups/strategy_2`，按模板要求填写 `manifest.json` 和 `engine.py`，不要从旧策略目录随意拷贝，也不要把 backtrader 作为主执行路径。

## 一、目录总览

```text
strategies/
├── __init__.py
├── engine_commons.py
├── README.md
└── groups/
    ├── strategy_2/              # 唯一推荐的新策略模板
    ├── consecutive_uptrends_v1/
    ├── converging_triangle_v1/
    ├── flag_pattern_v1/
    ├── multi_tf_ma_uptrend_v1/
    ├── weekly_oversold_rsi_v1/
    └── xianren_zhilu_v1/
```

约定：

1. `groups/strategy_2/` 是权威模板目录，后续 AI 应优先复制它。
2. 其余 `groups/*` 是现役策略示例，不保证都保留了最佳模板风格。
3. 如果现役策略和 `strategy_2` 的写法冲突，以 `strategy_2` 当前注释与 README 约定为准。

## 二、先记住这几条强约束

1. 新策略默认使用 `specialized` 引擎，不要把 backtrader 当主路径。
2. 新策略优先复制 `strategy_2`，不要直接复制其他业务策略。
3. `engine.py` 是主逻辑文件，`strategy.py` 在 specialized 策略里通常只保留 backtrader 协议占位。
4. `manifest.execution.specialized_entry` 必须指向 `engine.py` 中真实存在的函数。
5. 信号 payload 必须正确填写 `chart_interval_start_ts` 与 `chart_interval_end_ts`，且它们必须表示单次信号的实际展示窗口。
6. 如需概念预筛选，只能使用 `default_params.universe_filters.concepts`，推荐字段固定为 `concept_terms` 与 `reason_terms`。
7. 概念预筛选由 TaskManager 在 engine 之前执行，engine 不得重复做相同裁剪。
8. **所有新策略的 `param_help` 中每个周期参数组必须采用 `_render: "inline_template"` 渲染模式**（配合 `_label`、`_tf_key`、`_templates`），不允许使用纯 `_comment` 文本方式。即使是单周期策略也必须遵守此规范。
9. 如果策略检测到可视化辅助线（趋势线、支撑/阻力线、三角形边沿等），应通过 payload 的 `overlay_lines` 字段传递，前端会自动在 K 线图上渲染。详见 `strategy_2/README.md` 第十一节。

## 三、为什么新策略应优先走 specialized

平台支持两种引擎：

| 引擎 | manifest 中的 `engine` 值 | 适用建议 |
|---|---|---|
| specialized | `"specialized"` | 新策略默认选它。批量 DuckDB + pandas 处理，性能高，契约清晰。 |
| backtrader | `"backtrader"` | 仅作为迁移期或故障时的降级兜底，不推荐新策略作为主路径。 |

选择 specialized 的原因：

1. 能按股票池批量加载数据，避免逐股 feed 的性能损耗。
2. 更容易把输入、输出、payload 契约写清楚，便于 AI 按模板实现。
3. 可以独立控制缓存、并发和多周期加载策略。
4. 结果结构可控，和 API/前端图表约定更稳定。

## 四、标准创建流程

### 第 1 步：复制模板目录

```text
复制 strategies/groups/strategy_2/ -> strategies/groups/<your_strategy_id>/
```

例如：`strategies/groups/my_pattern_v1/`

### 第 2 步：只改这几个文件

必须检查并按需修改：

1. `manifest.json`
2. `engine.py`
3. `__init__.py` 中的模块注释或导出名称（通常只需确认）

通常不需要改：

1. `strategy.py`：除非你明确要支持 `fallback_to_backtrader=true` 的真实回退逻辑。

### 第 3 步：正确填写 manifest.json

这是策略的元数据和前端展示来源。至少要核对以下字段：

| 字段 | 要求 |
|---|---|
| `id` | 必须和目录名一致，且全局唯一 |
| `name` | 前端展示名，建议含版本号 |
| `description` | 一句话描述策略逻辑 |
| `module` | 固定写成 `strategies.groups.<id>` |
| `engine` | 新策略应为 `"specialized"` |
| `execution.required_timeframes` | 只声明你实际会读取的周期 |
| `execution.specialized_entry` | 必须指向 `engine.py` 中真实函数，格式 `module:function` |
| `default_params` | 默认参数，前端可以覆盖 |
| `param_help` | 给前端和 AI 阅读的说明文本，必须和参数结构同步 |

推荐写法示例：

```jsonc
{
  "id": "my_pattern_v1",
  "name": "我的形态策略 v1",
  "description": "简要描述策略逻辑",
  "module": "strategies.groups.my_pattern_v1",
  "engine": "specialized",
  "execution": {
    "requires_time_window": false,
    "supports_intra_task_parallel": false,
    "cache_scope": "none",
    "required_timeframes": ["d"],
    "specialized_entry": "strategies.groups.my_pattern_v1.engine:run_my_pattern_v1_specialized"
  },
  "default_params": {
    "daily": {
      "lookback_days": 10,
      "threshold": 0.05
    },
    "universe_filters": {
      "concepts": {
        "enabled": false,
        "concept_terms": [],
        "reason_terms": []
      }
    },
    "execution": {
      "fallback_to_backtrader": false
    }
  },
  "param_help": {
    "_overview": [
      "这里写策略思路、主要输入和输出约束。"
    ]
  }
}
```

### 第 4 步：在 engine.py 写主逻辑

`engine.py` 是后续 AI 最应该修改的文件。入口签名必须保持 keyword-only，不要改成位置参数风格：

```python
def run_my_pattern_v1_specialized(
    *,
    source_db_path: Path,
  start_ts: datetime | None,
  end_ts: datetime | None,
    codes: list[str],
    code_to_name: dict[str, str],
    group_params: dict[str, Any],
    strategy_group_id: str,
    strategy_name: str,
    cache_scope: str,
    cache_dir: Path | None = None,
) -> tuple[dict[str, StockScanResult], dict[str, Any]]:
```

筛选页时间参数约定：

1. 前端不再暴露 `start_ts/end_ts` 输入框。
2. specialized 策略在未显式传入时间窗时，应按策略所需最小历史量自行推导默认观察窗。
3. `start_ts/end_ts` 仍可保留为脚本化调用或回测场景的可选入参，但不能再作为筛选页必填前置条件。

实现建议顺序：

1. 先规范化参数。
2. 再批量加载数据。
3. 再逐股扫描并构建 `StockScanResult`。
4. 最后汇总 `metrics`。

### 第 5 步：保留 strategy.py 的占位语义

对于 specialized 策略，`strategy.py` 默认只提供 backtrader 协议占位与回退兜底提示。

只有在这两种情况才需要真正改它：

1. 你明确要把 `fallback_to_backtrader` 打开。
2. 你已经实现了可维护的 backtrader 同步逻辑。

否则，保留模板即可。

### 第 6 步：等待自动发现

完成后不需要修改框架注册代码。`StrategyRegistry` 会扫描 `strategies/groups/*/manifest.json` 自动发现新策略。重启服务后即可在前端看到。

## 五、概念预筛选规则

如果策略需要在进入 engine 前按概念信息裁剪股票池，只能使用：

```json
{
  "universe_filters": {
    "concepts": {
      "enabled": true,
      "concept_terms": ["机器人", "液冷"],
      "reason_terms": ["订单", "中标"]
    }
  }
}
```

规则与边界：

1. 固定位置只能是 `default_params.universe_filters.concepts` 或运行时 `group_params.universe_filters.concepts`。
2. 新策略统一使用 `concept_terms` / `reason_terms`，不要继续写 `names`、`keywords`、`concept_names`、`reason_keywords` 等历史别名。
3. 语义固定为：`concept_terms` 是 AND，`reason_terms` 是 OR。
4. 匹配字段固定为：`concept_terms` 匹配 `stock_concepts.board_name`，`reason_terms` 匹配 `stock_concepts.selected_reason`。
5. 过滤发生在 TaskManager，而不是 engine 内部。
6. engine 拿到的 `codes` 可能已经被裁剪为空，这不是异常。
7. 概念更新任务运行中时，平台会阻止新筛选任务创建；策略侧不要绕过这个约束。

## 六、payload 与结果返回合同

`result_map` 返回规则：

1. 只包含命中信号或出错的股票。
2. 未命中且无异常的股票不要放入 `result_map`。
3. 单股异常应局部处理，不要让整批扫描直接中断。

单个信号推荐通过 `build_signal_dict()` 构建，必须包含：

1. `code`
2. `name`
3. `signal_dt`
4. `clock_tf`
5. `strategy_group_id`
6. `strategy_name`
7. `signal_label`
8. `payload`

其中 payload 的硬约束最重要：

1. `chart_interval_start_ts` 必填。
2. `chart_interval_end_ts` 必填。
3. 这两个字段必须表示“单次信号的实际图表展示窗口”，不能写成全任务窗口，也不能写成多段历史拼接后的总跨度。
4. 强烈建议同步填写 `window_start_ts` 与 `window_end_ts` 表示策略命中窗口。
5. 如需红点锚点，可额外提供 `anchor_day_ts`。

推荐写法：

```python
signal = build_signal_dict(
    code=code,
    name=name,
    signal_dt=signal_dt,
    clock_tf="d",
    strategy_group_id=strategy_group_id,
    strategy_name=strategy_name,
    signal_label="我的信号标签",
    payload={
        "window_start_ts": start_dt,
        "window_end_ts": end_dt,
        "chart_interval_start_ts": start_dt,
        "chart_interval_end_ts": end_dt,
        "anchor_day_ts": anchor_dt,
    },
)
```

## 七、engine_commons.py 可直接复用的工具

常用能力：

1. `StockScanResult`：单股扫描结果 dataclass。
2. `as_dict` / `as_bool` / `as_int` / `as_float`：参数安全转换工具。
3. `connect_source_readonly(source_db_path)`：只读连接源行情库。
4. `build_signal_dict(...)`：构建标准信号结构。

推荐做法：

1. 所有用户输入参数都先走 `as_*` 规范化。
2. 不要在 engine 中手写不一致的信号 dict 结构。
3. 不要为同类参数转换重复造轮子。

## 八、数据加载建议

源行情库可用表：

| 表名 | 周期 | `required_timeframes` 标识 |
|---|---|---|
| `klines_w` | 周线 | `"w"` |
| `klines_d` | 日线 | `"d"` |
| `klines_60` | 60 分钟 | `"60"` |
| `klines_30` | 30 分钟 | `"30"` |
| `klines_15` | 15 分钟 | `"15"` |

通用字段通常包括：`code`、`datetime`、`open`、`high`、`low`、`close`、`volume`。

跨股票批量读取时，推荐使用临时表 + JOIN，而不是逐股查询：

```sql
create temp table _tmp_codes (code varchar);

select t.code, t.datetime as day_ts, t.open, t.high, t.low, t.close, t.volume
from klines_d t
join _tmp_codes c on c.code = t.code
where t.datetime >= ? and t.datetime <= ?
order by t.code, t.datetime;
```

## 九、高级特性

### 9.1 多周期

需要多个周期时，在 manifest 中声明 `required_timeframes`，并在 engine 中分别加载对应表。

### 9.2 任务内并行

只有在计算密集、单股扫描逻辑重的策略中才建议启用 `supports_intra_task_parallel=true`。

### 9.3 缓存

`cache_scope` 可选 `none`、`daily_candidates`、`full_pipeline`。新策略默认从 `none` 开始，只有确认收益明显时再加缓存。

### 9.4 backtrader 回退

`default_params.execution.fallback_to_backtrader` 仅在迁移期或故障兜底时使用。新策略默认保持 `false`。

## 十、推荐参考顺序

1. 第一参考：`strategy_2`，看它的 README、manifest、engine.py、strategy.py。
2. 第二参考：`multi_tf_ma_uptrend_v1`，适合学习多周期均线 AND 逻辑 + inline_template 前端渲染。
3. 第三参考：`consecutive_uptrends_v1`，适合看多周期 OR 逻辑与可选急跌段检测。
4. 第四参考：`xianren_zhilu_v1`，适合看双周期 OR 逻辑与 K线形态检测。

## 十一、新策略上线前检查清单

- [ ] 目录名、`manifest.id`、`manifest.module` 三者一致。
- [ ] `engine` 为 `"specialized"`。
- [ ] `specialized_entry` 指向真实存在的 `engine.py` 函数。
- [ ] 入口函数保持 keyword-only 签名。
- [ ] 返回值是 `(dict[str, StockScanResult], dict[str, Any])`。
- [ ] `result_map` 仅包含命中或异常股票。
- [ ] 若使用概念预筛选，参数路径固定在 `universe_filters.concepts`。
- [ ] 使用的是 `concept_terms` / `reason_terms`，不是历史别名。
- [ ] payload 包含 `chart_interval_start_ts` / `chart_interval_end_ts`。
- [ ] `chart_interval_*` 表示单次信号窗口，不是全任务总窗口。
- [ ] `strategy.py` 没有被误改成主执行逻辑文件。
- [ ] 重启服务后，前端能看到新策略并成功创建任务。

## 十二、常见错误

1. 前端看不到新策略：检查 `manifest.json` 是否是合法 JSON，`module` 和 `specialized_entry` 是否正确。
2. specialized 入口找不到：确认 `specialized_entry` 格式是 `module.path:function_name`，且函数名真实存在。
3. 图表窗口显示错乱：大多是 payload 的 `chart_interval_*` 语义写错，不是前端问题。
4. 明明启用了概念过滤却没生效：先检查参数路径是否写对，再确认概念任务是否已经完成刷新。
