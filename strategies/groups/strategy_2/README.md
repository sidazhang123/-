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

### 5.4 `param_help`

这里不是装饰性文案，而是前端和后续 AI 真正会读的参数说明。要求：

1. `default_params` 中出现的关键参数，都应有对应说明。
2. `_overview` 要写清策略思路、输入周期和输出窗口语义。
3. 如果某个参数会影响 payload 或图表行为，要写清楚。

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

### 6.3 payload 硬约束

每个信号的 payload 至少要保证：

1. `chart_interval_start_ts` 必填。
2. `chart_interval_end_ts` 必填。
3. 这两个字段表示单次信号实际展示窗口，不能偷懒写成全任务时间范围。
4. 推荐同时写 `window_start_ts`、`window_end_ts`。
5. 如果要在结果页显示锚点，写 `anchor_day_ts`。

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

## 九、简化执行清单

- [ ] 复制 `strategy_2` 为新目录。
- [ ] 改 `manifest.json` 的 id/name/description/module/specialized_entry。
- [ ] 改 `engine.py` 的标签、参数、加载逻辑和扫描逻辑。
- [ ] 不随意改 `strategy.py`。
- [ ] 保持入口签名不变。
- [ ] payload 正确填写 `chart_interval_*`。
- [ ] 如需概念过滤，只用 `concept_terms` / `reason_terms`。
