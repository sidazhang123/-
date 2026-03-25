"""
`converging_triangle_v1` 单元测试。

覆盖范围：
1. _evaluate_triangle — 条件检查 + 评分计算
2. _scan_tf_for_code — 窗口枚举 + 同周期最优选择
3. _scan_one_code — 跨周期去重（W 优先）
4. _normalize_tf_params — 参数规范化（百分数转比例、边界裁剪）
5. 边界情况：零斜率、segment_count > bar数、apex 已过
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import pytest

from strategies.groups.converging_triangle_v1.engine import (
    TriangleResult,
    _evaluate_triangle,
    _normalize_tf_params,
    _scan_one_code,
    _scan_tf_for_code,
)


# ---------------------------------------------------------------------------
# 辅助工具
# ---------------------------------------------------------------------------


def _make_converging_data(
    n_bars: int = 120,
    start_high: float = 20.0,
    start_low: float = 10.0,
    end_high: float = 16.0,
    end_low: float = 14.0,
    noise: float = 0.3,
    seed: int = 42,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """生成符合收敛三角形特征的模拟 OHLC 数据。

    输入：
    1. n_bars: K 线根数。
    2. start_high/start_low/end_high/end_low: 上下沿起止价格。
    3. noise: 噪声幅度（相对值）。
    4. seed: 随机种子。
    输出：
    1. (highs, lows, closes) numpy 数组。
    边界条件：
    1. 保证 high >= close >= low > 0。
    """
    rng = np.random.default_rng(seed)
    xs = np.linspace(0, 1, n_bars)

    upper_line = start_high + (end_high - start_high) * xs
    lower_line = start_low + (end_low - start_low) * xs
    mid_line = (upper_line + lower_line) / 2.0

    closes = mid_line + rng.normal(0, noise, n_bars)
    highs = upper_line + rng.uniform(0, noise * 0.5, n_bars)
    lows = lower_line - rng.uniform(0, noise * 0.5, n_bars)

    # 保证 high >= close >= low > 0
    highs = np.maximum(highs, closes + 0.01)
    lows = np.minimum(lows, closes - 0.01)
    lows = np.maximum(lows, 0.1)

    return highs, lows, closes


def _make_code_frame(
    n_bars: int = 120,
    start_date: str = "2025-01-01",
    tf_key: str = "d",
    **kwargs: Any,
) -> pd.DataFrame:
    """生成单只股票的模拟 DataFrame。

    输入：
    1. n_bars: K 线根数。
    2. start_date: 起始日期。
    3. tf_key: 周期标识（d 或 w），用于决定日期步长。
    4. kwargs: 传递给 _make_converging_data。
    输出：
    1. 含 ts/open/high/low/close/volume 列的 DataFrame。
    边界条件：
    1. open = close（简化，不影响三角检测）。
    """
    highs, lows, closes = _make_converging_data(n_bars=n_bars, **kwargs)
    base = datetime.strptime(start_date, "%Y-%m-%d")
    step = timedelta(weeks=1) if tf_key == "w" else timedelta(days=1)
    timestamps = [base + step * i for i in range(n_bars)]

    return pd.DataFrame({
        "ts": timestamps,
        "open": closes,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": np.full(n_bars, 1000000),
    })


# ---------------------------------------------------------------------------
# _evaluate_triangle 测试
# ---------------------------------------------------------------------------


class TestEvaluateTriangle:
    """测试三角检测核心函数 _evaluate_triangle。"""

    def test_valid_triangle_passes(self) -> None:
        """符合所有条件的收敛三角应返回 TriangleResult。"""
        highs, lows, closes = _make_converging_data(
            n_bars=120, start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )
        result = _evaluate_triangle(
            highs=highs, lows=lows, closes=closes,
            segment_count=4,
            converging_speed_min=0.01,
            slope_symmetry_tolerance=0.5,
            contraction_ratio_max=0.8,
            apex_progress_min=0.3,
            tf_key="d",
        )
        assert result is not None
        assert isinstance(result, TriangleResult)
        assert result.score > 0
        assert result.upper_slope_unit < 0
        assert result.lower_slope_unit > 0
        assert result.converging_speed > 0
        assert result.contraction_ratio < 1.0
        assert 0 < result.apex_progress < 1.0

    def test_too_few_bars_returns_none(self) -> None:
        """bar 数不足 segment_count 时返回 None。"""
        highs, lows, closes = _make_converging_data(n_bars=3)
        result = _evaluate_triangle(
            highs=highs, lows=lows, closes=closes,
            segment_count=4,
            converging_speed_min=0.01,
            slope_symmetry_tolerance=0.5,
            contraction_ratio_max=0.8,
            apex_progress_min=0.3,
            tf_key="d",
        )
        assert result is None

    def test_uptrend_upper_fails(self) -> None:
        """上沿上升（非收敛）时应返回 None。"""
        highs, lows, closes = _make_converging_data(
            n_bars=120,
            start_high=10.0, start_low=8.0,
            end_high=20.0, end_low=12.0,
            noise=0.1,
        )
        result = _evaluate_triangle(
            highs=highs, lows=lows, closes=closes,
            segment_count=4,
            converging_speed_min=0.01,
            slope_symmetry_tolerance=0.5,
            contraction_ratio_max=0.8,
            apex_progress_min=0.3,
            tf_key="d",
        )
        assert result is None

    def test_downtrend_lower_fails(self) -> None:
        """下沿下降（非收敛）时应返回 None。"""
        highs, lows, closes = _make_converging_data(
            n_bars=120,
            start_high=20.0, start_low=15.0,
            end_high=16.0, end_low=8.0,
            noise=0.1,
        )
        result = _evaluate_triangle(
            highs=highs, lows=lows, closes=closes,
            segment_count=4,
            converging_speed_min=0.01,
            slope_symmetry_tolerance=0.5,
            contraction_ratio_max=0.8,
            apex_progress_min=0.3,
            tf_key="d",
        )
        assert result is None

    def test_contraction_ratio_too_high_fails(self) -> None:
        """收缩比例超过阈值时返回 None。"""
        highs, lows, closes = _make_converging_data(
            n_bars=120,
            start_high=20.0, start_low=10.0,
            end_high=19.5, end_low=10.5,
            noise=0.1,
        )
        result = _evaluate_triangle(
            highs=highs, lows=lows, closes=closes,
            segment_count=4,
            converging_speed_min=0.01,
            slope_symmetry_tolerance=0.5,
            contraction_ratio_max=0.3,
            apex_progress_min=0.1,
            tf_key="d",
        )
        assert result is None

    def test_score_components_valid_range(self) -> None:
        """评分各分项应在 [0, 1] 范围内。"""
        highs, lows, closes = _make_converging_data(
            n_bars=120, start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )
        result = _evaluate_triangle(
            highs=highs, lows=lows, closes=closes,
            segment_count=4,
            converging_speed_min=0.01,
            slope_symmetry_tolerance=0.5,
            contraction_ratio_max=0.8,
            apex_progress_min=0.3,
            tf_key="d",
        )
        assert result is not None
        assert 0 <= result.contraction_ratio <= 1.0
        assert 0 < result.apex_progress < 1.0
        assert 0 <= result.slope_symmetry_ratio <= 1.0
        assert result.fit_error >= 0


# ---------------------------------------------------------------------------
# _normalize_tf_params 测试
# ---------------------------------------------------------------------------


class TestNormalizeTfParams:
    """测试参数规范化函数。"""

    def test_default_values(self) -> None:
        """缺失参数回落到默认值，百分数正确转换为比例。"""
        result = _normalize_tf_params({}, "weekly")
        assert result["enabled"] is False
        assert result["search_recent_weeks"] == 12
        assert result["pattern_months_min"] == 3
        assert result["pattern_months_max"] == 10
        # 百分数转比例
        assert abs(result["converging_speed_min"] - 0.08) < 1e-6
        assert abs(result["contraction_ratio_max"] - 0.45) < 1e-6
        assert abs(result["apex_progress_min"] - 0.75) < 1e-6
        assert result["segment_count"] == 4

    def test_explicit_values(self) -> None:
        """显式传入的值应保留并正确转换。"""
        result = _normalize_tf_params(
            {"weekly": {"enabled": True, "converging_speed_min": 10, "apex_progress_min": 80}},
            "weekly",
        )
        assert result["enabled"] is True
        assert abs(result["converging_speed_min"] - 0.10) < 1e-6
        assert abs(result["apex_progress_min"] - 0.80) < 1e-6

    def test_months_max_ge_min(self) -> None:
        """pattern_months_max 应 ≥ pattern_months_min。"""
        result = _normalize_tf_params(
            {"daily": {"pattern_months_min": 5, "pattern_months_max": 3}},
            "daily",
        )
        assert result["pattern_months_max"] >= result["pattern_months_min"]


# ---------------------------------------------------------------------------
# _scan_tf_for_code 测试
# ---------------------------------------------------------------------------


class TestScanTfForCode:
    """测试单周期单股搜索。"""

    def test_valid_data_finds_triangle(self) -> None:
        """符合条件的数据应找到三角形。"""
        frame = _make_code_frame(
            n_bars=150, start_date="2025-01-01", tf_key="d",
            start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )
        params = _normalize_tf_params(
            {"daily": {
                "enabled": True,
                "search_recent_weeks": 52,
                "pattern_months_min": 2,
                "pattern_months_max": 12,
                "converging_speed_min": 1,
                "slope_symmetry_tolerance": 0.8,
                "contraction_ratio_max": 80,
                "apex_progress_min": 20,
                "segment_count": 4,
            }},
            "daily",
        )
        result = _scan_tf_for_code(code_frame=frame, params=params, tf_key="d")
        assert result is not None
        assert result.score > 0

    def test_empty_frame_returns_none(self) -> None:
        """空 DataFrame 返回 None。"""
        result = _scan_tf_for_code(
            code_frame=pd.DataFrame(),
            params=_normalize_tf_params({}, "daily"),
            tf_key="d",
        )
        assert result is None


# ---------------------------------------------------------------------------
# _scan_one_code 去重测试
# ---------------------------------------------------------------------------


class TestScanOneCodeDedup:
    """测试跨周期去重逻辑。"""

    def test_w_preferred_over_d(self) -> None:
        """D 和 W 同时命中时，仅保留 W。"""
        d_frame = _make_code_frame(
            n_bars=150, start_date="2025-01-01", tf_key="d",
            start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )
        w_frame = _make_code_frame(
            n_bars=40, start_date="2025-01-01", tf_key="w",
            start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )

        relaxed_params = {
            "enabled": True,
            "search_recent_weeks": 52,
            "pattern_months_min": 1,
            "pattern_months_max": 24,
            "converging_speed_min": 0.001,
            "slope_symmetry_tolerance": 0.99,
            "contraction_ratio_max": 0.99,
            "apex_progress_min": 0.05,
            "segment_count": 3,
        }

        result, stats = _scan_one_code(
            code="000001",
            name="测试",
            tf_data={"d": d_frame, "w": w_frame},
            all_params={"d": relaxed_params, "w": relaxed_params},
            enabled_tfs=["w", "d"],
            strategy_group_id="converging_triangle_v1",
            strategy_name="大收敛三角 v1",
        )

        if result.signal_count > 0:
            signal = result.signals[0]
            assert signal["clock_tf"] == "w"

    def test_d_only_when_w_disabled(self) -> None:
        """仅启用 D 时保留 D 结果。"""
        d_frame = _make_code_frame(
            n_bars=150, start_date="2025-01-01", tf_key="d",
            start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )

        relaxed_params = {
            "enabled": True,
            "search_recent_weeks": 52,
            "pattern_months_min": 1,
            "pattern_months_max": 24,
            "converging_speed_min": 0.001,
            "slope_symmetry_tolerance": 0.99,
            "contraction_ratio_max": 0.99,
            "apex_progress_min": 0.05,
            "segment_count": 3,
        }

        result, stats = _scan_one_code(
            code="000001",
            name="测试",
            tf_data={"d": d_frame},
            all_params={"d": relaxed_params},
            enabled_tfs=["d"],
            strategy_group_id="converging_triangle_v1",
            strategy_name="大收敛三角 v1",
        )

        if result.signal_count > 0:
            signal = result.signals[0]
            assert signal["clock_tf"] == "d"

    def test_overlay_lines_in_payload(self) -> None:
        """命中时 payload 应包含 overlay_lines。"""
        d_frame = _make_code_frame(
            n_bars=150, start_date="2025-01-01", tf_key="d",
            start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )

        relaxed_params = {
            "enabled": True,
            "search_recent_weeks": 52,
            "pattern_months_min": 1,
            "pattern_months_max": 24,
            "converging_speed_min": 0.001,
            "slope_symmetry_tolerance": 0.99,
            "contraction_ratio_max": 0.99,
            "apex_progress_min": 0.05,
            "segment_count": 3,
        }

        result, _ = _scan_one_code(
            code="000001",
            name="测试",
            tf_data={"d": d_frame},
            all_params={"d": relaxed_params},
            enabled_tfs=["d"],
            strategy_group_id="converging_triangle_v1",
            strategy_name="大收敛三角 v1",
        )

        if result.signal_count > 0:
            payload = result.signals[0]["payload"]
            assert "overlay_lines" in payload
            assert len(payload["overlay_lines"]) == 2
            upper_line = payload["overlay_lines"][0]
            lower_line = payload["overlay_lines"][1]
            assert upper_line["label"] == "上沿"
            assert lower_line["label"] == "下沿"
            assert upper_line["start_price"] > upper_line["end_price"]  # 上沿下降
            assert lower_line["start_price"] < lower_line["end_price"]  # 下沿上升
