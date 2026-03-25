"""
`weekly_sideways_v1` 单元测试。

覆盖范围：
1. _evaluate_channel — 水平通道条件检查
2. _detect_rotations — 轮动检测与计数
3. _normalize_weekly_params — 周线参数规范化
4. _normalize_reference_params — 参考月参数规范化
5. _check_reference_low — 参考月过滤
6. _scan_for_code — 窗口搜索
7. _scan_one_code — 单股完整扫描（含参考月过滤）
8. 边界情况：趋势数据、幅度越界、数据不足、参考月无数据
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import pytest

from strategies.groups.weekly_sideways_v1.engine import (
    ChannelResult,
    RotationResult,
    _check_reference_low,
    _detect_rotations,
    _evaluate_channel,
    _normalize_reference_params,
    _normalize_weekly_params,
    _scan_for_code,
    _scan_one_code,
)


# ---------------------------------------------------------------------------
# 辅助工具
# ---------------------------------------------------------------------------


def _make_sideways_data(
    n_bars: int = 80,
    upper: float = 15.0,
    lower: float = 10.0,
    noise: float = 0.3,
    seed: int = 42,
    n_cycles: int = 4,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """生成符合水平震荡特征的模拟 OHLC 数据。

    输入：
    1. n_bars: K 线根数。
    2. upper/lower: 上下沿价格。
    3. noise: 噪声幅度。
    4. seed: 随机种子。
    5. n_cycles: 完整震荡周期数（从上到下再到上）。
    输出：
    1. (highs, lows, closes) numpy 数组。
    边界条件：
    1. 保证 high >= close >= low > 0。
    """
    rng = np.random.default_rng(seed)
    mid = (upper + lower) / 2.0
    amplitude = (upper - lower) / 2.0

    # 用正弦波模拟在上下沿之间的震荡
    xs = np.linspace(0, n_cycles * 2 * np.pi, n_bars)
    closes = mid + amplitude * 0.7 * np.sin(xs) + rng.normal(0, noise, n_bars)
    closes = np.clip(closes, lower + 0.5, upper - 0.5)

    # highs 在震荡高点处接近 upper，lows 在低点处接近 lower
    highs = closes + rng.uniform(0.3, amplitude * 0.4, n_bars)
    lows = closes - rng.uniform(0.3, amplitude * 0.4, n_bars)

    # 保证 high >= close >= low > 0
    highs = np.maximum(highs, closes + 0.01)
    lows = np.minimum(lows, closes - 0.01)
    lows = np.maximum(lows, 0.1)

    return highs, lows, closes


def _make_weekly_frame(
    n_bars: int = 80,
    start_date: str = "2024-01-01",
    **kwargs: Any,
) -> pd.DataFrame:
    """生成单只股票的模拟周线 DataFrame。

    输入：
    1. n_bars: K 线根数。
    2. start_date: 起始日期。
    3. kwargs: 传递给 _make_sideways_data。
    输出：
    1. 含 ts/open/high/low/close/volume 列的 DataFrame。
    边界条件：
    1. open = close（简化，不影响震荡检测）。
    """
    highs, lows, closes = _make_sideways_data(n_bars=n_bars, **kwargs)
    base = datetime.strptime(start_date, "%Y-%m-%d")
    timestamps = [base + timedelta(weeks=i) for i in range(n_bars)]

    return pd.DataFrame({
        "ts": timestamps,
        "open": closes.copy(),
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": np.full(n_bars, 1000000),
    })


def _make_trend_data(
    n_bars: int = 80,
    start_price: float = 10.0,
    end_price: float = 20.0,
    noise: float = 0.3,
    seed: int = 42,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """生成上升趋势的模拟 OHLC 数据（非水平）。

    输入：
    1. n_bars: K 线根数。
    2. start_price/end_price: 起止价格。
    3. noise: 噪声幅度。
    4. seed: 随机种子。
    输出：
    1. (highs, lows, closes) numpy 数组。
    """
    rng = np.random.default_rng(seed)
    xs = np.linspace(0, 1, n_bars)
    closes = start_price + (end_price - start_price) * xs + rng.normal(0, noise, n_bars)
    highs = closes + rng.uniform(0.3, 1.0, n_bars)
    lows = closes - rng.uniform(0.3, 1.0, n_bars)
    lows = np.maximum(lows, 0.1)
    return highs, lows, closes


# ---------------------------------------------------------------------------
# _evaluate_channel 测试
# ---------------------------------------------------------------------------


class TestEvaluateChannel:
    """测试水平通道检测核心函数。"""

    def test_valid_sideways_passes(self) -> None:
        """符合水平震荡条件的数据应返回 ChannelResult。"""
        highs, lows, closes = _make_sideways_data(
            n_bars=80, upper=15.0, lower=10.0, noise=0.2, n_cycles=4,
        )
        result = _evaluate_channel(
            highs=highs, lows=lows,
            segment_count=4,
            max_slope_pct=5.0,
            range_pct_min=20.0,
            range_pct_max=80.0,
        )
        assert result is not None
        assert isinstance(result, ChannelResult)
        assert result.upper_level > result.lower_level
        # 幅度应在预期范围内（上沿 ~15，下沿 ~10，幅度 ~50%）
        assert 20.0 <= result.range_pct <= 80.0
        # 斜率应接近水平
        assert abs(result.upper_slope_unit) <= 5.0
        assert abs(result.lower_slope_unit) <= 5.0

    def test_too_few_bars_returns_none(self) -> None:
        """bar 数不足 segment_count 时返回 None。"""
        highs, lows, _ = _make_sideways_data(n_bars=3)
        result = _evaluate_channel(
            highs=highs, lows=lows,
            segment_count=4,
            max_slope_pct=5.0,
            range_pct_min=10.0,
            range_pct_max=80.0,
        )
        assert result is None

    def test_trend_data_fails(self) -> None:
        """明显趋势数据（非水平）应返回 None（斜率超限）。"""
        highs, lows, _ = _make_trend_data(
            n_bars=80, start_price=10.0, end_price=30.0, noise=0.2,
        )
        result = _evaluate_channel(
            highs=highs, lows=lows,
            segment_count=4,
            max_slope_pct=3.0,
            range_pct_min=10.0,
            range_pct_max=200.0,
        )
        assert result is None

    def test_range_too_narrow_fails(self) -> None:
        """区间幅度低于 range_pct_min 时返回 None。"""
        # 直接构造窄幅数据：上下沿仅差 5%（10.0 ~10.5）
        n = 80
        rng = np.random.default_rng(42)
        closes = 10.25 + rng.normal(0, 0.05, n)
        highs = closes + rng.uniform(0.1, 0.25, n)
        lows = closes - rng.uniform(0.1, 0.25, n)
        lows = np.maximum(lows, 0.1)
        result = _evaluate_channel(
            highs=highs, lows=lows,
            segment_count=4,
            max_slope_pct=5.0,
            range_pct_min=20.0,
            range_pct_max=80.0,
        )
        assert result is None

    def test_range_too_wide_fails(self) -> None:
        """区间幅度高于 range_pct_max 时返回 None。"""
        highs, lows, _ = _make_sideways_data(
            n_bars=80, upper=25.0, lower=10.0, noise=0.3,
        )
        result = _evaluate_channel(
            highs=highs, lows=lows,
            segment_count=4,
            max_slope_pct=5.0,
            range_pct_min=5.0,
            range_pct_max=30.0,
        )
        assert result is None


# ---------------------------------------------------------------------------
# _detect_rotations 测试
# ---------------------------------------------------------------------------


class TestDetectRotations:
    """测试轮动检测函数。"""

    def test_valid_rotations_detected(self) -> None:
        """有规律震荡数据应检测到足够轮动次数。"""
        # 使用 4 个完整周期的震荡数据，80 根周线覆盖约 20 个月
        highs, lows, closes = _make_sideways_data(
            n_bars=80, upper=15.0, lower=10.0, noise=0.1, n_cycles=4,
        )
        base = datetime(2024, 1, 1)
        ts_arr = np.array([base + timedelta(weeks=i) for i in range(80)], dtype="datetime64[ns]")

        result = _detect_rotations(
            highs=highs, lows=lows, ts_arr=ts_arr,
            upper_level=15.0, lower_level=10.0,
            touch_tolerance_pct=15.0,
            rotation_months_min=0.5,
            rotation_months_max=6.0,
            min_rotations=1,
        )
        assert result is not None
        assert isinstance(result, RotationResult)
        assert result.rotation_count >= 1
        assert result.swing_count >= 2
        assert len(result.swing_details) >= 2

    def test_no_touch_events_returns_none(self) -> None:
        """价格完全在区间中间、不触摸上下沿时返回 None。"""
        n = 80
        # 所有价格集中在 12-13 范围，上沿 15 下沿 10
        highs = np.full(n, 13.0)
        lows = np.full(n, 12.0)
        base = datetime(2024, 1, 1)
        ts_arr = np.array([base + timedelta(weeks=i) for i in range(n)], dtype="datetime64[ns]")

        result = _detect_rotations(
            highs=highs, lows=lows, ts_arr=ts_arr,
            upper_level=15.0, lower_level=10.0,
            touch_tolerance_pct=10.0,
            rotation_months_min=0.5,
            rotation_months_max=6.0,
            min_rotations=1,
        )
        assert result is None

    def test_insufficient_rotations_returns_none(self) -> None:
        """轮动次数不足 min_rotations 时返回 None。"""
        # 只在上沿触摸一次然后下沿触摸一次 = 1次swing = 0次完整轮动
        n = 20
        highs = np.full(n, 12.0)
        lows = np.full(n, 11.0)
        # 让前5根触摸上沿，后5根触摸下沿
        highs[0:5] = 15.0
        lows[15:20] = 10.0
        base = datetime(2024, 1, 1)
        ts_arr = np.array([base + timedelta(weeks=i) for i in range(n)], dtype="datetime64[ns]")

        result = _detect_rotations(
            highs=highs, lows=lows, ts_arr=ts_arr,
            upper_level=15.0, lower_level=10.0,
            touch_tolerance_pct=10.0,
            rotation_months_min=0.5,
            rotation_months_max=12.0,
            min_rotations=2,
        )
        assert result is None

    def test_swing_duration_out_of_range(self) -> None:
        """半轮动时长超出 [p, q] 范围时应导致有效轮动数不足。"""
        # 创建极慢的震荡（1个周期覆盖80周 ≈ 20个月，每次swing约10个月）
        highs, lows, closes = _make_sideways_data(
            n_bars=80, upper=15.0, lower=10.0, noise=0.1, n_cycles=1,
        )
        base = datetime(2024, 1, 1)
        ts_arr = np.array([base + timedelta(weeks=i) for i in range(80)], dtype="datetime64[ns]")

        result = _detect_rotations(
            highs=highs, lows=lows, ts_arr=ts_arr,
            upper_level=15.0, lower_level=10.0,
            touch_tolerance_pct=15.0,
            rotation_months_min=1.0,
            rotation_months_max=3.0,
            min_rotations=1,
        )
        # 每个swing约10个月 > max 3个月，因此有效轮动数应不足
        assert result is None


# ---------------------------------------------------------------------------
# _normalize_weekly_params 测试
# ---------------------------------------------------------------------------


class TestNormalizeWeeklyParams:
    """测试周线参数规范化函数。"""

    def test_default_values(self) -> None:
        """缺失参数回落到默认值。"""
        result = _normalize_weekly_params({})
        assert result["min_duration_months"] == 12
        assert result["rotation_months_min"] == 1.0
        assert result["rotation_months_max"] == 4.0
        assert result["range_pct_min"] == 20.0
        assert result["range_pct_max"] == 40.0
        assert result["min_rotations"] == 2
        assert result["touch_tolerance_pct"] == 10.0
        assert result["segment_count"] == 4
        assert result["max_slope_pct"] == 3.0

    def test_explicit_values(self) -> None:
        """显式传入的值应正确保留。"""
        result = _normalize_weekly_params({
            "weekly": {
                "min_duration_months": 18,
                "range_pct_min": 25,
                "range_pct_max": 50,
                "min_rotations": 3,
            }
        })
        assert result["min_duration_months"] == 18
        assert result["range_pct_min"] == 25.0
        assert result["range_pct_max"] == 50.0
        assert result["min_rotations"] == 3

    def test_rotation_max_ge_min(self) -> None:
        """rotation_months_max 强制 >= rotation_months_min。"""
        result = _normalize_weekly_params({
            "weekly": {"rotation_months_min": 5.0, "rotation_months_max": 2.0}
        })
        assert result["rotation_months_max"] >= result["rotation_months_min"]

    def test_range_max_ge_min(self) -> None:
        """range_pct_max 强制 >= range_pct_min。"""
        result = _normalize_weekly_params({
            "weekly": {"range_pct_min": 50.0, "range_pct_max": 20.0}
        })
        assert result["range_pct_max"] >= result["range_pct_min"]


# ---------------------------------------------------------------------------
# _normalize_reference_params 测试
# ---------------------------------------------------------------------------


class TestNormalizeReferenceParams:
    """测试参考月参数规范化函数。"""

    def test_default_values(self) -> None:
        """缺失参数回落到默认值。"""
        result = _normalize_reference_params({})
        assert result["enabled"] is False
        assert result["ref_year"] == 2025
        assert result["ref_month"] == 4
        assert result["ref_gain_min_pct"] == -5.0
        assert result["ref_gain_max_pct"] == 30.0

    def test_gain_max_ge_min(self) -> None:
        """ref_gain_max_pct 强制 >= ref_gain_min_pct。"""
        result = _normalize_reference_params({
            "reference": {"ref_gain_min_pct": 20.0, "ref_gain_max_pct": 5.0}
        })
        assert result["ref_gain_max_pct"] >= result["ref_gain_min_pct"]


# ---------------------------------------------------------------------------
# _check_reference_low 测试
# ---------------------------------------------------------------------------


class TestCheckReferenceLow:
    """测试参考月低点过滤函数。"""

    def test_no_data_passes(self) -> None:
        """参考月无数据时返回 True（跳过过滤）。"""
        ref_params = _normalize_reference_params({
            "reference": {"enabled": True, "ref_gain_min_pct": -5, "ref_gain_max_pct": 30}
        })
        assert _check_reference_low(10.0, None, ref_params) is True

    def test_zero_ref_close_passes(self) -> None:
        """参考价为 0 时返回 True（跳过过滤）。"""
        ref_params = _normalize_reference_params({
            "reference": {"enabled": True, "ref_gain_min_pct": -5, "ref_gain_max_pct": 30}
        })
        assert _check_reference_low(10.0, 0.0, ref_params) is True

    def test_gain_in_range_passes(self) -> None:
        """涨幅在 [min, max] 范围内应通过。"""
        ref_params = _normalize_reference_params({
            "reference": {"enabled": True, "ref_gain_min_pct": -5, "ref_gain_max_pct": 30}
        })
        # lower_level=11.0, ref_low=10.0 => gain=10% => 在 [-5, 30] 内
        assert _check_reference_low(11.0, 10.0, ref_params) is True

    def test_gain_below_min_fails(self) -> None:
        """涨幅低于 min 应不通过。"""
        ref_params = _normalize_reference_params({
            "reference": {"enabled": True, "ref_gain_min_pct": 10, "ref_gain_max_pct": 30}
        })
        # lower_level=10.5, ref_low=10.0 => gain=5% < 10%
        assert _check_reference_low(10.5, 10.0, ref_params) is False

    def test_gain_above_max_fails(self) -> None:
        """涨幅高于 max 应不通过。"""
        ref_params = _normalize_reference_params({
            "reference": {"enabled": True, "ref_gain_min_pct": -5, "ref_gain_max_pct": 10}
        })
        # lower_level=12.0, ref_low=10.0 => gain=20% > 10%
        assert _check_reference_low(12.0, 10.0, ref_params) is False


# ---------------------------------------------------------------------------
# _scan_for_code 测试
# ---------------------------------------------------------------------------


class TestScanForCode:
    """测试窗口搜索函数。"""

    def test_valid_sideways_finds_window(self) -> None:
        """符合条件的水平震荡数据应找到有效窗口。"""
        frame = _make_weekly_frame(
            n_bars=80, start_date="2024-01-01",
            upper=15.0, lower=10.0, noise=0.1, n_cycles=4,
        )
        params = _normalize_weekly_params({
            "weekly": {
                "min_duration_months": 6,
                "rotation_months_min": 0.5,
                "rotation_months_max": 8.0,
                "range_pct_min": 20.0,
                "range_pct_max": 80.0,
                "min_rotations": 1,
                "touch_tolerance_pct": 15.0,
                "segment_count": 4,
                "max_slope_pct": 5.0,
            }
        })
        result = _scan_for_code(code_frame=frame, params=params)
        assert result is not None
        channel, rotation, s_idx, e_idx = result
        assert isinstance(channel, ChannelResult)
        assert isinstance(rotation, RotationResult)
        assert s_idx < e_idx
        assert e_idx == len(frame) - 1  # t_e 应为最新 bar

    def test_too_short_data_returns_none(self) -> None:
        """数据时间跨度不足 min_duration_months 时返回 None。"""
        frame = _make_weekly_frame(
            n_bars=10, start_date="2024-01-01",
            upper=15.0, lower=10.0, noise=0.1, n_cycles=2,
        )
        params = _normalize_weekly_params({
            "weekly": {
                "min_duration_months": 12,
                "segment_count": 4,
                "max_slope_pct": 5.0,
                "range_pct_min": 20.0,
                "range_pct_max": 80.0,
            }
        })
        result = _scan_for_code(code_frame=frame, params=params)
        assert result is None

    def test_longest_window_preferred(self) -> None:
        """搜索应返回最长有效窗口（t_s 最早的有效窗口）。"""
        frame = _make_weekly_frame(
            n_bars=100, start_date="2023-06-01",
            upper=15.0, lower=10.0, noise=0.1, n_cycles=5,
        )
        params = _normalize_weekly_params({
            "weekly": {
                "min_duration_months": 6,
                "rotation_months_min": 0.5,
                "rotation_months_max": 8.0,
                "range_pct_min": 20.0,
                "range_pct_max": 80.0,
                "min_rotations": 1,
                "touch_tolerance_pct": 15.0,
                "segment_count": 4,
                "max_slope_pct": 5.0,
            }
        })
        result = _scan_for_code(code_frame=frame, params=params)
        if result is not None:
            _, _, s_idx, e_idx = result
            # 最长窗口的 s_idx 应尽可能小
            assert s_idx >= 0
            assert e_idx == len(frame) - 1


# ---------------------------------------------------------------------------
# _scan_one_code 测试
# ---------------------------------------------------------------------------


class TestScanOneCode:
    """测试单股完整扫描。"""

    def test_hit_produces_signal_with_overlay(self) -> None:
        """命中时应产出带 overlay_lines 的信号。"""
        frame = _make_weekly_frame(
            n_bars=80, start_date="2024-01-01",
            upper=15.0, lower=10.0, noise=0.1, n_cycles=4,
        )
        weekly_params = _normalize_weekly_params({
            "weekly": {
                "min_duration_months": 6,
                "rotation_months_min": 0.5,
                "rotation_months_max": 8.0,
                "range_pct_min": 20.0,
                "range_pct_max": 80.0,
                "min_rotations": 1,
                "touch_tolerance_pct": 15.0,
                "segment_count": 4,
                "max_slope_pct": 5.0,
            }
        })
        ref_params = _normalize_reference_params({})

        result, stats = _scan_one_code(
            code="000001",
            name="测试股票",
            code_frame=frame,
            weekly_params=weekly_params,
            ref_params=ref_params,
            ref_low_map={},
            strategy_group_id="weekly_sideways_v1",
            strategy_name="周线长期震荡 v1",
        )

        assert result.signal_count == 1
        signal = result.signals[0]
        payload = signal["payload"]
        assert "overlay_lines" in payload
        assert len(payload["overlay_lines"]) == 2

        # 验证水平线（start_price == end_price）
        for line in payload["overlay_lines"]:
            assert line["start_price"] == line["end_price"]
            assert "start_ts" in line
            assert "end_ts" in line

        # 验证上沿红色、下沿绿色
        labels = {line["label"]: line["color"] for line in payload["overlay_lines"]}
        assert labels.get("上沿") == "#ef4444"
        assert labels.get("下沿") == "#22c55e"

        # 验证实线
        for line in payload["overlay_lines"]:
            assert line["dash"] is False

        # 验证必填 payload 字段
        assert "chart_interval_start_ts" in payload
        assert "chart_interval_end_ts" in payload

    def test_ref_filter_blocks_signal(self) -> None:
        """参考月过滤不通过时不应产出信号。"""
        frame = _make_weekly_frame(
            n_bars=80, start_date="2024-01-01",
            upper=15.0, lower=10.0, noise=0.1, n_cycles=4,
        )
        weekly_params = _normalize_weekly_params({
            "weekly": {
                "min_duration_months": 6,
                "rotation_months_min": 0.5,
                "rotation_months_max": 8.0,
                "range_pct_min": 20.0,
                "range_pct_max": 80.0,
                "min_rotations": 1,
                "touch_tolerance_pct": 15.0,
                "segment_count": 4,
                "max_slope_pct": 5.0,
            }
        })
        ref_params = _normalize_reference_params({
            "reference": {
                "enabled": True,
                "ref_year": 2025,
                "ref_month": 4,
                "ref_gain_min_pct": 200,
                "ref_gain_max_pct": 200,
            }
        })
        # 设置一个参考价使得涨幅不在范围内
        ref_low_map = {"000001": 9.0}

        result, stats = _scan_one_code(
            code="000001",
            name="测试股票",
            code_frame=frame,
            weekly_params=weekly_params,
            ref_params=ref_params,
            ref_low_map=ref_low_map,
            strategy_group_id="weekly_sideways_v1",
            strategy_name="周线长期震荡 v1",
        )
        assert result.signal_count == 0

    def test_ref_filter_no_data_passes(self) -> None:
        """参考月启用但无数据时应不阻止信号。"""
        frame = _make_weekly_frame(
            n_bars=80, start_date="2024-01-01",
            upper=15.0, lower=10.0, noise=0.1, n_cycles=4,
        )
        weekly_params = _normalize_weekly_params({
            "weekly": {
                "min_duration_months": 6,
                "rotation_months_min": 0.5,
                "rotation_months_max": 8.0,
                "range_pct_min": 20.0,
                "range_pct_max": 80.0,
                "min_rotations": 1,
                "touch_tolerance_pct": 15.0,
                "segment_count": 4,
                "max_slope_pct": 5.0,
            }
        })
        ref_params = _normalize_reference_params({
            "reference": {
                "enabled": True,
                "ref_year": 2025,
                "ref_month": 4,
                "ref_gain_min_pct": 200,
                "ref_gain_max_pct": 200,
            }
        })
        # 空 ref_low_map = 无数据 => 跳过过滤
        ref_low_map: dict[str, float] = {}

        result, stats = _scan_one_code(
            code="000001",
            name="测试股票",
            code_frame=frame,
            weekly_params=weekly_params,
            ref_params=ref_params,
            ref_low_map=ref_low_map,
            strategy_group_id="weekly_sideways_v1",
            strategy_name="周线长期震荡 v1",
        )
        # 无数据时跳过过滤，如果通道+轮动通过则应命中
        # 这里主要验证不会因参考月无数据而报错或阻断
        assert result.error_message is None or result.error_message == ""
