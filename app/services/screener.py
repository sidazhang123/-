"""
Backtrader 执行路径服务。

职责：
1. 将多周期 DataFrame 装配为 Backtrader 数据源。
2. 按策略组 `GROUP_HANDLERS` 协议执行逐股规则判断。
3. 生成标准化信号结构，回传给任务管理器入库。

说明：
1. 该模块服务于 backtrader 轨道；specialized 专用引擎不走此路径。
2. 规则计算依赖策略组 handlers，模块本身不定义具体策略条件。
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

import backtrader as bt
import pandas as pd

from app.services.strategy_registry import StrategyGroupRuntime
from app.settings import TIMEFRAME_ORDER

SIGNAL_WINDOW_KEYS = (
    "anchor_day_ts",
    "window_start_ts",
    "window_end_ts",
    "chart_interval_start_ts",
    "chart_interval_end_ts",
)


def _extract_signal_window_fields(
    *,
    per_rule: dict[str, dict[str, Any]],
    combo_result: dict[str, Any],
) -> dict[str, Any]:
    """
    尝试从规则返回值中提取图表窗口字段并提升到 payload 顶层。

    优先级：
    1. combo.values
    2. d.values
    3. 其他周期 values（按 TIMEFRAME_ORDER 顺序）
    """

    sources: list[dict[str, Any]] = []
    combo_values = combo_result.get("values") if isinstance(combo_result, dict) else None
    if isinstance(combo_values, dict):
        sources.append(combo_values)

    d_values = per_rule.get("d", {}).get("values") if isinstance(per_rule.get("d"), dict) else None
    if isinstance(d_values, dict):
        sources.append(d_values)

    for tf in TIMEFRAME_ORDER:
        result = per_rule.get(tf)
        if not isinstance(result, dict):
            continue
        values = result.get("values")
        if isinstance(values, dict):
            sources.append(values)

    merged: dict[str, Any] = {}
    for key in SIGNAL_WINDOW_KEYS:
        for source in sources:
            if key in source and source.get(key) is not None:
                merged[key] = source.get(key)
                break
    return merged


class PandasAmountData(bt.feeds.PandasData):
    lines = ("amount",)
    params = (
        ("datetime", None),
        ("open", "open"),
        ("high", "high"),
        ("low", "low"),
        ("close", "close"),
        ("volume", "volume"),
        ("openinterest", -1),
        ("amount", "amount"),
    )


@dataclass
class StockRunSummary:
    code: str
    name: str
    processed_bars: int = 0
    signal_count: int = 0
    last_dt: datetime | None = None
    last_rules: dict[str, Any] = field(default_factory=dict)
    signals: list[dict[str, Any]] = field(default_factory=list)


class RuleScreenStrategy(bt.Strategy):
    params = (
        ("code", ""),
        ("name", ""),
        ("strategy_group_id", ""),
        ("strategy_name", ""),
        ("strategy_group_runtime", None),
        ("group_params", None),
        ("summary", None),
        ("error_collector", None),
        ("on_signal", None),
    )

    def __init__(self) -> None:
        """
        输入：
        1. 无显式输入参数（由 Backtrader params 属性注入）。
        输出：
        1. 无返回值。
        用途：
        1. 初始化多周期数据源映射、时钟周期、活跃周期列表与非活跃规则结果。
        边界条件：
        1. 无可用周期数据时 _pick_clock_tf 会抛出 RuntimeError。
        """
        self.data_by_tf: dict[str, Any] = {}
        for data in self.datas:
            tf = data._name
            self.data_by_tf[tf] = data

        self.strategy_group_id: str = self.p.strategy_group_id
        self.strategy_name: str = self.p.strategy_name
        self.runtime: StrategyGroupRuntime = self.p.strategy_group_runtime
        self.group_params: dict[str, Any] = self.p.group_params or {}
        self.summary: StockRunSummary = self.p.summary
        self.error_collector: list[str] = self.p.error_collector
        self.on_signal: Callable[[dict[str, Any]], None] | None = self.p.on_signal
        self.clock_tf = self._pick_clock_tf()
        self.active_timeframes = self._pick_active_timeframes()
        self._inactive_rule_results = self._build_inactive_rule_results()

    def _pick_clock_tf(self) -> str:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回最细粒度可用周期键（如 "15", "d"）。
        用途：
        1. 从 15/30/60/d/w 中选取最细粒度周期作为 Backtrader 时钟驱动源。
        边界条件：
        1. 无任何可用周期时抛出 RuntimeError。
        """
        for tf in ["15", "30", "60", "d", "w"]:
            if tf in self.data_by_tf:
                return tf
        raise RuntimeError("没有可用的周期数据源")

    def _pick_active_timeframes(self) -> tuple[str, ...]:
        required = tuple(self.runtime.meta.execution.required_timeframes or ())
        if not required:
            return tuple(TIMEFRAME_ORDER)
        required_set = set(required)
        return tuple(tf for tf in TIMEFRAME_ORDER if tf in required_set)

    def _build_inactive_rule_results(self) -> dict[str, dict[str, Any]]:
        inactive: dict[str, dict[str, Any]] = {}
        active_set = set(self.active_timeframes)
        for tf in TIMEFRAME_ORDER:
            if tf in active_set:
                continue
            inactive[tf] = {
                "passed": True,
                "enabled": False,
                "ready": True,
                "message": f"{tf} skipped by required_timeframes",
                "values": {},
            }
        return inactive

    def tf_base_shift(self, tf: str) -> int:
        # 源库中的各周期时间戳统一为周期结束时间（d/w 为当日 15:00），可直接读取当前索引。
        """
        输入：
        1. tf: 周期键（如 "d", "w"）。
        输出：
        1. 返回 0（无偏移）。
        用途：
        1. 获取周期数据的基准偏移（当前库时间戳规则下始终为 0）。
        边界条件：
        1. 无。
        """
        return 0

    def tf_has_bars(self, tf: str, bars: int) -> bool:
        """
        输入：
        1. tf: 周期键。
        2. bars: 所需最少 K 线数量。
        输出：
        1. 数据源是否已具备足够的 K 线数。
        用途：
        1. 在规则评估前检查周期数据是否就绪。
        边界条件：
        1. 周期不存在时返回 False。
        """
        data = self.data_by_tf.get(tf)
        if data is None:
            return False
        shift = self.tf_base_shift(tf)
        required = bars + (1 if shift < 0 else 0)
        return len(data) >= required

    def tf_line(self, tf: str, line_name: str, ago: int = 0) -> float | None:
        """
        输入：
        1. tf: 周期键。
        2. line_name: Backtrader 数据线名（open/high/low/close/volume/amount）。
        3. ago: 向前偏移量（0 = 当前 K 线）。
        输出：
        1. 指定周期、线名、偏移对应的 float 值；数据不足时返回 None。
        用途：
        1. 供策略规则处理器读取任意周期的价量数据。
        边界条件：
        1. ago < 0 时抛出 ValueError。
        2. 数据不足或属性不存在时返回 None。
        """
        if ago < 0:
            raise ValueError("ago 必须大于等于 0")
        if not self.tf_has_bars(tf, ago + 1):
            return None
        data = self.data_by_tf[tf]
        idx = self.tf_base_shift(tf) - ago
        try:
            return float(getattr(data, line_name)[idx])
        except Exception:
            return None

    def tf_datetime(self, tf: str, ago: int = 0) -> datetime | None:
        """
        输入：
        1. tf: 周期键。
        2. ago: 向前偏移量（0 = 当前 K 线）。
        输出：
        1. 对应 K 线的 datetime；数据不足时返回 None。
        用途：
        1. 获取指定周期某根 K 线的时间戳。
        边界条件：
        1. 数据不足时返回 None。
        """
        if not self.tf_has_bars(tf, ago + 1):
            return None
        data = self.data_by_tf[tf]
        idx = self.tf_base_shift(tf) - ago
        try:
            return bt.num2date(data.datetime[idx]).replace(tzinfo=None)
        except Exception:
            return None

    def _evaluate_rule(self, tf: str) -> dict[str, Any]:
        """
        输入：
        1. tf: 周期键（如 "d", "w", "60" 等）。
        输出：
        1. 返回规则结果 dict，含 passed/enabled/ready/message/values 等键。
        用途：
        1. 调用对应周期的规则处理器并返回评估结果。
        边界条件：
        1. 处理器返回非 dict 时视为未通过。
        2. 处理器抛异常时捕获并记入 error_collector，返回失败结果。
        """
        handler = self.runtime.per_tf_handlers[tf]
        try:
            result = handler(self, self.group_params)
            if not isinstance(result, dict):
                return {
                    "passed": False,
                    "enabled": True,
                    "ready": False,
                    "message": f"{tf} 规则返回值不是字典",
                    "values": {},
                }
            return result
        except Exception as exc:
            self.error_collector.append(f"规则[{tf}]执行异常: {exc}")
            return {
                "passed": False,
                "enabled": True,
                "ready": False,
                "message": f"{tf} 规则异常: {exc}",
                "values": {"traceback": traceback.format_exc(limit=2)},
            }

    def next(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. Backtrader 每根 K 线回调：执行各周期规则、组合判断，通过则生成信号。
        边界条件：
        1. 周期数据缺失时记为未通过。
        2. 规则/组合异常时捕获并记入 error_collector。
        """
        clock_data = self.data_by_tf[self.clock_tf]
        clock_dt = bt.num2date(clock_data.datetime[0]).replace(tzinfo=None)

        per_rule: dict[str, dict[str, Any]] = dict(self._inactive_rule_results)
        for tf in self.active_timeframes:
            if tf not in self.data_by_tf:
                per_rule[tf] = {
                    "passed": False,
                    "enabled": True,
                    "ready": False,
                    "message": f"{tf} 周期数据源缺失",
                    "values": {},
                }
                continue
            per_rule[tf] = self._evaluate_rule(tf)

        try:
            combo_result = self.runtime.combo_handler(self, per_rule, self.group_params)
        except Exception as exc:
            self.error_collector.append(f"组合规则异常: {exc}")
            combo_result = {
                "passed": False,
                "enabled": True,
                "ready": False,
                "message": f"组合规则异常: {exc}",
                "values": {"traceback": traceback.format_exc(limit=2)},
            }

        self.summary.processed_bars += 1
        self.summary.last_dt = clock_dt
        self.summary.last_rules = {"per_rule": per_rule, "combo": combo_result}

        if combo_result.get("passed") and combo_result.get("ready"):
            passed_timeframes = [
                tf
                for tf, result in per_rule.items()
                if result.get("enabled", True) and result.get("ready", False) and result.get("passed", False)
            ]

            try:
                signal_label = self.runtime.signal_label_builder(self, per_rule, combo_result, self.group_params)
            except Exception:
                signal_label = self.strategy_name

            payload = {
                "per_rule": per_rule,
                "combo": combo_result,
                "debug": {
                    "passed_timeframes": passed_timeframes,
                },
            }
            payload.update(
                _extract_signal_window_fields(
                    per_rule=per_rule,
                    combo_result=combo_result,
                )
            )

            self.summary.signal_count += 1
            signal = {
                "code": self.p.code,
                "name": self.p.name,
                "signal_dt": clock_dt,
                "clock_tf": self.clock_tf,
                "strategy_group_id": self.strategy_group_id,
                "strategy_name": self.strategy_name,
                "signal_label": str(signal_label or self.strategy_name),
                "payload": payload,
            }
            if self.on_signal is not None:
                self.on_signal(signal)
            else:
                self.summary.signals.append(signal)


def run_single_stock_backtrader(
    *,
    code: str,
    name: str,
    timeframe_dfs: dict[str, pd.DataFrame],
    strategy_group_id: str,
    strategy_name: str,
    strategy_group_runtime: StrategyGroupRuntime,
    group_params: dict[str, Any],
    on_signal: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[StockRunSummary, list[str]]:
    """
    输入：
    1. code: 股票代码。
    2. name: 股票名称。
    3. timeframe_dfs: 周期键 -> DataFrame 映射。
    4. strategy_group_id: 策略组标识。
    5. strategy_name: 策略名称。
    6. strategy_group_runtime: 策略组运行时对象（含 handlers/combo/signal_label_builder）。
    7. group_params: 策略参数 dict。
    8. on_signal: 可选信号回调（传入时实时推送，否则累积到 summary.signals）。
    输出：
    1. (StockRunSummary, errors) 元组。
    用途：
    1. 对单只股票执行 Backtrader 多周期规则扫描并收集信号。
    边界条件：
    1. 所有周期 DataFrame 均为空时返回空结果与错误提示。
    """
    summary = StockRunSummary(code=code, name=name)
    errors: list[str] = []

    cerebro = bt.Cerebro(stdstats=False, maxcpus=1)
    for tf in ["w", "d", "60", "30", "15"]:
        df = timeframe_dfs.get(tf)
        if df is None or df.empty:
            continue
        feed = PandasAmountData(dataname=df)
        cerebro.adddata(feed, name=tf)

    if not cerebro.datas:
        errors.append("没有可用的周期数据")
        return summary, errors

    cerebro.addstrategy(
        RuleScreenStrategy,
        code=code,
        name=name,
        strategy_group_id=strategy_group_id,
        strategy_name=strategy_name,
        strategy_group_runtime=strategy_group_runtime,
        group_params=group_params,
        summary=summary,
        error_collector=errors,
        on_signal=on_signal,
    )
    cerebro.run(runonce=False)
    return summary, errors
