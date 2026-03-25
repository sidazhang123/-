# 策略组 strategy_2 目录

> 这是本仓库创建新策略时唯一推荐复制的模板目录。后续 AI 如果要新增策略，应先阅读本 README，再修改 `manifest.json` 和 `engine.py`，不要直接从其他业务策略目录起步。

## 一、这个目录到底是做什么的

`strategy_2` 是 specialized 模板策略，作用不是提供业务规则，而是明确告诉后续 AI：

1. 新策略应该怎样组织目录。
2. 哪些文件必须修改，哪些文件通常不要改。
3. `manifest.json`、`engine.py`、`strategy.py` 分别负责什么。
4. 输出给 TaskManager 和结果页的 payload 字段必须满足哪些契约。

如果你在实现新策略时发现这里和其他旧策略目录冲突，以本目录当前说明为准。

## 二、当前文件清单

1. `__init__.py`：策略包入口，通常无需改动。
2. `manifest.json`：策略元信息、入口配置、默认参数和参数说明。
3. `engine.py`：specialized 主逻辑模板，后续 AI 应主要修改这里。
4. `strategy.py`：backtrader 协议占位与降级兜底，不是主逻辑文件。

## 三、复制本模板后必须做的事

### 1. 重命名目录

把 `strategy_2` 复制成你的策略 ID，例如：

```text
strategies/groups/my_pattern_v1/
```

### 2. 修改 manifest.json

至少修改以下字段：

1. `id`
2. `name`
3. `description`
4. `module`
5. `execution.required_timeframes`
6. `execution.specialized_entry`
7. `default_params` 中与你策略相关的参数
8. `param_help` 中与参数和策略说明相关的文本

### 3. 修改 engine.py

后续 AI 的主工作应放在这里：

1. 修改 `STRATEGY_LABEL`
2. 修改默认参数常量
3. 按策略需要完善参数规范化函数
4. 按所需周期重写数据加载函数
5. 在 `_scan_one_code()` 中实现真正的选股逻辑
6. 保持主入口签名和返回值结构不变

### 4. 通常不要改 strategy.py

除非你明确要支持 backtrader 降级路径，否则保留模板内容即可。这里的函数主要用于：

1. 满足框架要求的 `GROUP_HANDLERS` 导出。
2. 在 specialized 失败且 `fallback_to_backtrader=true` 时提供兜底入口。

## 四、后续 AI 最容易犯错的地方

1. 把 `strategy.py` 当成主逻辑文件。
2. 改坏 `specialized_entry`，导致入口函数无法加载。
3. 改了 `engine.py` 的入口签名，导致 TaskManager 传参失败。
4. payload 中漏写 `chart_interval_start_ts` / `chart_interval_end_ts`。
5. 把 `chart_interval_*` 写成全任务区间，而不是单次信号窗口。
6. 把概念过滤逻辑又在 engine 里做了一遍，造成双重筛选。
7. 继续使用 `names`、`keywords` 等旧字段，而不是 `concept_terms`、`reason_terms`。

## 五、manifest 约定

### 5.1 `engine`

新策略默认必须写 `"specialized"`。

### 5.2 `execution.specialized_entry`

格式固定为：

```text
strategies.groups.<strategy_id>.engine:run_<strategy_id>_specialized
```

建议函数名也按这个格式命名，这样后续 AI 更不容易写错。

### 5.3 `required_timeframes`

只声明实际会加载的周期，不要无意义地把所有周期都填上。

### 5.4 `requires_time_window`

筛选页不再要求前端传入 `start_ts/end_ts`，模板策略应将 `requires_time_window` 保持为 `false`。
如果后续要支持脚本化回测，可以继续把这两个字段作为可选入参处理，但不要恢复成筛选页必填项。

### 5.5 `param_help`

这里不是装饰性文案，而是前端和后续 AI 真正会读的参数说明。要求：

1. `default_params` 中出现的关键参数，都应有对应说明。
2. `_overview` 要写清策略思路、输入周期和输出窗口语义。
3. 如果某个参数会影响 payload 或图表行为，要写清楚。
4. **强制要求**：所有周期参数组必须采用 `_render: "inline_template"` 渲染模式（配合 `_label`、`_tf_key`、`_templates` 数组），不允许使用纯 `_comment` 文本方式。即使是单周期策略也必须遵守此规范。

## 六、engine.py 约定

### 6.1 什么能改

1. `STRATEGY_LABEL`
2. 参数默认值常量
3. 参数规范化函数
4. 数据加载函数
5. `_scan_one_code()` 的具体逻辑
6. `metrics` 的统计字段内容

### 6.2 什么不要改

1. 主入口使用 keyword-only 参数的基本签名结构。
2. 返回值必须是 `(result_map, metrics)`。
3. `result_map` 只包含命中或异常股票的约定。
4. “概念过滤由 TaskManager 先执行”的边界。

### 6.3 时间参数约定

`engine.py` 主入口中的 `start_ts/end_ts` 应视为可选输入：

1. `end_ts` 为空时，默认按当前时间作为筛选锚点。
2. `start_ts` 为空时，应按策略实际最小历史量自行推导默认观察窗。
3. 不要再把它们当成筛选页必填参数。

### 6.4 payload 硬约束

每个信号的 payload 至少要保证：

1. `chart_interval_start_ts` 必填。
2. `chart_interval_end_ts` 必填。
3. 这两个字段表示单次信号实际展示窗口，不能偷懒写成全任务时间范围。
4. 推荐同时写 `window_start_ts`、`window_end_ts`。
5. 如果要在结果页显示锚点，写 `anchor_day_ts`。
6. 如果策略检测到可视化辅助线，写 `overlay_lines`（详见第十一节）。

## 七、概念预筛选约定

如果要在策略前裁剪股票池，统一写在：

```text
default_params.universe_filters.concepts
```

字段要求：

1. `enabled`
2. `concept_terms`
3. `reason_terms`

语义要求：

1. `concept_terms` = AND
2. `reason_terms` = OR
3. 过滤发生在 TaskManager 调用 engine 之前
4. engine 只需要接受“codes 可能已被裁剪”的事实，不要重复裁剪

## 八、推荐验证方式

至少检查：

1. 服务重启后，前端能看到新策略。
2. 可以成功创建任务。
3. 结果页没有因为 payload 时间字段错误而显示异常窗口。
4. 如果启用概念过滤，能在结果股票概念信息接口里看到一致结果。

## 九、多周期策略扩展

本模板默认为单日线策略。如果需要多周期（如 w/d/60/15），需做以下扩展：

### manifest.json 变更

1. `execution.required_timeframes` 声明所有用到的周期，如 `["w", "d", "60"]`。
2. `default_params` 按 `weekly`/`daily`/`min60`/`min15` 分段，每段含 `enabled` 开关。
3. `param_help` 中每个周期参数组**必须**使用 `_render: "inline_template"` 模式，搭配 `_label`、`_tf_key`、`_templates` 数组实现前端参数渲染。此规范对单周期和多周期策略均适用。

### engine.py 变更

1. 定义 `_TF_TABLE: dict[str, str]` 映射周期 key 到 klines 表名。
2. 定义 `_TF_ORDER: list[str]` 周期粗细排序。
3. 从 `engine_commons` 导入 `coarsest_tf()` 用于多周期信号合并。
4. 多周期之间可选 AND 逻辑（全部通过才出信号）或 OR 逻辑（任一通过即出信号）。

### 参考示例

1. `multi_tf_ma_uptrend_v1`：w/d/60/15 四周期 AND 逻辑 + inline_template 前端渲染。
2. `consecutive_uptrends_v1`：w/d/60 三周期 OR 逻辑 + 可选急跌段。
3. `flag_pattern_v1`：w/d/15 三周期 OR 逻辑 + HV 收敛检测。
4. `xianren_zhilu_v1`：w/d 双周期 OR 逻辑 + K线形态检测。
5. `converging_triangle_v1`：w/d 双周期 OR 逻辑 + 几何三角形检测 + overlay_lines 辅助线。

## 十、通用工具（engine_commons.py）

以下函数已提取到 `strategies/engine_commons.py`，新策略直接 import 即可：

## 十一、K 线图辅助线（overlay_lines）

如果策略检测到可视化的几何线条（趋势线、支撑/阻力线、通道边界、三角形边沿等），可以通过信号 payload 的 `overlay_lines` 字段传递给前端。前端会自动在 K 线蜡烛图上用 ECharts markLine 渲染，支持缩放平移跟随。

### 数据格式

```python
"overlay_lines": [
    {
        "label": "上沿",              # 可选，线条标签，显示在起点
        "start_ts": datetime(...),    # 必填，起点时间戳
        "start_price": 18.5,          # 必填，起点价格
        "end_ts": datetime(...),      # 必填，终点时间戳
        "end_price": 15.2,            # 必填，终点价格
        "color": "#ef4444",           # 可选，默认 #fbbf24（琥珀色）
        "dash": True,                 # 可选，是否虚线，默认 True
    },
]
```

### 工作原理

1. **engine.py**：策略在 payload 中填入 `overlay_lines` 列表。
2. **routes.py**：`_extract_signal_window` 自动提取并转换时间戳，传递给前端。
3. **results.js**：`renderChart` 读取 `overlay_lines`，将时间戳映射到图表 X 轴索引，用 ECharts markLine 在蜡烛图上绘制。

### 使用场景

- 收敛三角形上下沿（参考 `converging_triangle_v1`）
- 旗形模式的旗杆顶底 + 旗面上下沿
- 均线趋势线
- 通道线 / 楚岳带
- 任何价格空间中的旜线或水平线

### 注意事项

1. `start_ts` / `end_ts` 必须为 datetime 对象，routes.py 会自动通过 `_coerce_datetime()` 处理。
2. 线条时间范围应在 `chart_interval_start_ts ~ chart_interval_end_ts` 内，否则超出图表可见区域会被截断。
3. 每条线的颜色独立控制，建议上沿/下沿用不同颜色以增强可读性。
4. `overlay_lines` 为空列表或缺失时，前端不会绘制任何线条，无副作用。

1. `normalize_execution_params(group_params)` —— 标准执行参数规范化（仅 fallback_to_backtrader）。
2. `read_universe_filter_params(group_params)` —— 概念预筛选参数读取（仅 metrics 用途）。
3. `coarsest_tf(tfs)` —— 从周期列表中选出最粗粒度周期。
4. `STANDARD_TF_ORDER` —— 标准周期粗细排序常量 `["w", "d", "60", "30", "15"]`。

如策略有额外执行参数（如 `worker_count`），可保留本地版本的 `_normalize_execution_params`。

## 十一、简化执行清单

- [ ] 复制 `strategy_2` 为新目录。
- [ ] 改 `manifest.json` 的 id/name/description/module/specialized_entry。
- [ ] 改 `engine.py` 的标签、参数、加载逻辑和扫描逻辑。
- [ ] 不随意改 `strategy.py`。
- [ ] 保持入口签名不变。
- [ ] payload 正确填写 `chart_interval_*`。
- [ ] 如需概念过滤，只用 `concept_terms` / `reason_terms`。
