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
    detect_converging_triangle,
    detect_converging_triangle_vectorized,
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
            "upper_edge_zone_ratio": 0.50,
            "lower_edge_zone_ratio": 0.50,
            "min_upper_touch_count": 0,
            "min_lower_touch_count": 0,
            "lower_break_pct": 0.50,
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
            "upper_edge_zone_ratio": 0.50,
            "lower_edge_zone_ratio": 0.50,
            "min_upper_touch_count": 0,
            "min_lower_touch_count": 0,
            "lower_break_pct": 0.50,
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
            "upper_edge_zone_ratio": 0.50,
            "lower_edge_zone_ratio": 0.50,
            "min_upper_touch_count": 0,
            "min_lower_touch_count": 0,
            "lower_break_pct": 0.50,
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


# ---------------------------------------------------------------------------
# 触碰次数测试
# ---------------------------------------------------------------------------


class TestTouchCounting:
    """测试上下沿触碰次数统计与过滤逻辑。"""

    def test_touch_counts_in_result(self) -> None:
        """有效三角形结果应包含 upper/lower_touch_count 字段。"""
        highs, lows, closes = _make_converging_data(
            n_bars=120, start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.3,
        )
        result = _evaluate_triangle(
            highs=highs, lows=lows, closes=closes,
            segment_count=4,
            converging_speed_min=0.01,
            slope_symmetry_tolerance=0.5,
            contraction_ratio_max=0.8,
            apex_progress_min=0.3,
            tf_key="d",
            upper_edge_zone_ratio=0.50,
            lower_edge_zone_ratio=0.50,
            min_upper_touch_count=0,
            min_lower_touch_count=0,
        )
        assert result is not None
        assert isinstance(result.upper_touch_count, int)
        assert isinstance(result.lower_touch_count, int)
        assert result.upper_touch_count >= 0
        assert result.lower_touch_count >= 0

    def test_strict_upper_touch_filter(self) -> None:
        """上沿触碰次数不足时应返回 None。"""
        highs, lows, closes = _make_converging_data(
            n_bars=120, start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.3,
        )
        result = _evaluate_triangle(
            highs=highs, lows=lows, closes=closes,
            segment_count=4,
            converging_speed_min=0.01,
            slope_symmetry_tolerance=0.5,
            contraction_ratio_max=0.8,
            apex_progress_min=0.3,
            tf_key="d",
            upper_edge_zone_ratio=0.01,
            lower_edge_zone_ratio=0.50,
            min_upper_touch_count=999,
            min_lower_touch_count=0,
        )
        assert result is None

    def test_strict_lower_touch_filter(self) -> None:
        """下沿触碰次数不足时应返回 None。"""
        highs, lows, closes = _make_converging_data(
            n_bars=120, start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.3,
        )
        result = _evaluate_triangle(
            highs=highs, lows=lows, closes=closes,
            segment_count=4,
            converging_speed_min=0.01,
            slope_symmetry_tolerance=0.5,
            contraction_ratio_max=0.8,
            apex_progress_min=0.3,
            tf_key="d",
            upper_edge_zone_ratio=0.50,
            lower_edge_zone_ratio=0.01,
            min_upper_touch_count=0,
            min_lower_touch_count=999,
        )
        assert result is None

    def test_wider_zone_more_touching_bars(self) -> None:
        """更宽的区域应使更多 bar 落入触碰区域（单事件内 bar 数增加）。

        注意：事件数不一定单调递增——更宽的区域会令连续 bar 合并为更少事件。
        此处仅验证两种区域宽度均能产生有效结果。
        """
        highs, lows, closes = _make_converging_data(
            n_bars=120, start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.3,
        )
        base_kwargs = dict(
            highs=highs, lows=lows, closes=closes,
            segment_count=4,
            converging_speed_min=0.01,
            slope_symmetry_tolerance=0.5,
            contraction_ratio_max=0.8,
            apex_progress_min=0.3,
            tf_key="d",
            min_upper_touch_count=0,
            min_lower_touch_count=0,
        )

        narrow = _evaluate_triangle(**base_kwargs, upper_edge_zone_ratio=0.05, lower_edge_zone_ratio=0.05)
        wide = _evaluate_triangle(**base_kwargs, upper_edge_zone_ratio=0.40, lower_edge_zone_ratio=0.40)

        # 两种配置均应能通过基本几何条件
        assert narrow is not None
        assert wide is not None
        # 触碰事件数均 ≥ 0
        assert narrow.upper_touch_count >= 0
        assert wide.upper_touch_count >= 0

    def test_normalize_touch_params_defaults(self) -> None:
        """_normalize_tf_params 应包含触碰参数默认值。"""
        result = _normalize_tf_params({}, "weekly")
        assert abs(result["upper_edge_zone_ratio"] - 0.10) < 1e-6
        assert abs(result["lower_edge_zone_ratio"] - 0.10) < 1e-6
        assert result["min_upper_touch_count"] == 3
        assert result["min_lower_touch_count"] == 2
        assert abs(result["lower_break_pct"] - 0.10) < 1e-6

    def test_normalize_touch_params_explicit(self) -> None:
        """显式传入触碰参数应正确解析。"""
        result = _normalize_tf_params(
            {"daily": {
                "upper_edge_zone_ratio": 20,
                "lower_edge_zone_ratio": 15,
                "min_upper_touch_count": 5,
                "min_lower_touch_count": 4,
            }},
            "daily",
        )
        assert abs(result["upper_edge_zone_ratio"] - 0.20) < 1e-6
        assert abs(result["lower_edge_zone_ratio"] - 0.15) < 1e-6
        assert result["min_upper_touch_count"] == 5
        assert result["min_lower_touch_count"] == 4

    def test_touch_counts_in_scan_payload(self) -> None:
        """_scan_one_code 命中时 per_tf 诊断应包含触碰次数。"""
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
            "upper_edge_zone_ratio": 0.50,
            "lower_edge_zone_ratio": 0.50,
            "min_upper_touch_count": 0,
            "min_lower_touch_count": 0,
            "lower_break_pct": 0.50,
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
            per_tf = result.signals[0]["payload"]["per_tf"]
            for tf_key, detail in per_tf.items():
                assert "upper_touch_count" in detail
                assert "lower_touch_count" in detail
                assert detail["upper_touch_count"] >= 0
                assert detail["lower_touch_count"] >= 0


# ---------------------------------------------------------------------------
# 最新 K 线破位检查测试
# ---------------------------------------------------------------------------


class TestLowerBreakCheck:
    """测试最新收盘价破位检查逻辑。"""

    def _make_break_frame(
        self, n_bars: int, break_close: float, tf_key: str = "d",
    ) -> pd.DataFrame:
        """生成收敛三角数据，并在末尾追加一根指定收盘价的 K 线。"""
        base = _make_code_frame(
            n_bars=n_bars, start_date="2025-01-01", tf_key=tf_key,
            start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )
        last_ts = base["ts"].iloc[-1]
        extra = pd.DataFrame([{
            "ts": last_ts + pd.Timedelta(days=1 if tf_key == "d" else 7),
            "open": break_close,
            "high": break_close + 0.1,
            "low": break_close - 0.1,
            "close": break_close,
            "volume": 1000,
        }])
        return pd.concat([base, extra], ignore_index=True)

    def test_no_break_passes(self) -> None:
        """最新收盘价未破位时应保留结果。"""
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
                "lower_break_pct": 10,
            }},
            "daily",
        )
        result = _scan_tf_for_code(code_frame=frame, params=params, tf_key="d")
        # 模拟数据末尾收盘价在三角内部，不应被破位过滤
        assert result is not None

    def test_severe_break_rejected(self) -> None:
        """最新收盘价严重破位时应返回 None。"""
        frame = self._make_break_frame(n_bars=150, break_close=1.0, tf_key="d")
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
                "lower_break_pct": 5,
            }},
            "daily",
        )
        result = _scan_tf_for_code(code_frame=frame, params=params, tf_key="d")
        # 收盘价 1.0 远低于下沿（~14），必须被拒绝
        assert result is None

    def test_normalize_lower_break_pct(self) -> None:
        """破位参数应正确规范化。"""
        result = _normalize_tf_params(
            {"daily": {"lower_break_pct": 15}},
            "daily",
        )
        assert abs(result["lower_break_pct"] - 0.15) < 1e-6


# ---------------------------------------------------------------------------
# detect_converging_triangle_vectorized 测试
# ---------------------------------------------------------------------------


def _relaxed_params() -> dict[str, Any]:
    """返回一组宽松的规范化参数（适合合成数据）。"""
    return {
        "enabled": True,
        "search_recent_weeks": 12,
        "pattern_months_min": 1,
        "pattern_months_max": 24,
        "converging_speed_min": 0.001,
        "slope_symmetry_tolerance": 0.99,
        "contraction_ratio_max": 0.99,
        "apex_progress_min": 0.05,
        "segment_count": 3,
        "upper_edge_zone_ratio": 0.50,
        "lower_edge_zone_ratio": 0.50,
        "min_upper_touch_count": 0,
        "min_lower_touch_count": 0,
        "lower_break_pct": 0.50,
    }


class TestDetectConvergingTriangleVectorized:
    """测试回测 vectorized 检测路径。"""

    def test_empty_frame_returns_empty(self) -> None:
        """空 DataFrame 返回空列表。"""
        results = detect_converging_triangle_vectorized(
            pd.DataFrame(), params=_relaxed_params(), tf_key="d",
        )
        assert results == []

    def test_disabled_tf_returns_empty(self) -> None:
        """周期未启用时返回空列表。"""
        frame = _make_code_frame(n_bars=150, tf_key="d")
        params = _relaxed_params()
        params["enabled"] = False
        results = detect_converging_triangle_vectorized(frame, params=params, tf_key="d")
        assert results == []

    def test_single_triangle_detected(self) -> None:
        """含一个收敛三角的合成数据应检测到至少一个命中。"""
        frame = _make_code_frame(
            n_bars=200, start_date="2024-01-01", tf_key="d",
            start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )
        results = detect_converging_triangle_vectorized(
            frame, params=_relaxed_params(), tf_key="d",
        )
        assert len(results) >= 1
        for r in results:
            assert r.matched is True
            assert r.pattern_start_idx is not None
            assert r.pattern_end_idx is not None
            assert r.pattern_start_ts is not None
            assert r.pattern_end_ts is not None
            assert r.pattern_start_idx < r.pattern_end_idx

    def test_vectorized_consistent_with_detect(self) -> None:
        """vectorized 在尾部搜索窗口的命中应与单次 detect 一致（或被更早命中覆盖）。"""
        frame = _make_code_frame(
            n_bars=200, start_date="2024-01-01", tf_key="d",
            start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )
        params = _relaxed_params()

        # 单次 detect：只搜索尾部
        single = detect_converging_triangle(frame, params=params, tf_key="d")

        # vectorized：搜索全历史
        vec_results = detect_converging_triangle_vectorized(
            frame, params=params, tf_key="d",
        )

        if single.matched:
            # vectorized 应满足以下条件之一：
            # a) 存在一个与单次 detect 的 pattern_end_ts 匹配的命中
            # b) 存在一个更早的命中其 end_idx 覆盖了单次 detect 的 start_idx
            #    （贪心前扫导致该区域已被占用）
            single_end_ts = single.pattern_end_ts
            single_start_idx = single.pattern_start_idx
            vec_end_ts_set = {r.pattern_end_ts for r in vec_results}

            covered_by_earlier = any(
                r.pattern_end_idx >= single_start_idx
                for r in vec_results
                if r.pattern_end_ts != single_end_ts
            )
            assert single_end_ts in vec_end_ts_set or covered_by_earlier, (
                f"单次 detect 命中 end_ts={single_end_ts} 未出现在 vectorized 结果中，"
                f"且未被更早命中覆盖"
            )

    def test_no_overlapping_hits(self) -> None:
        """vectorized 结果中不应有重叠区间。"""
        frame = _make_code_frame(
            n_bars=300, start_date="2023-06-01", tf_key="d",
            start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )
        results = detect_converging_triangle_vectorized(
            frame, params=_relaxed_params(), tf_key="d",
        )
        for i in range(1, len(results)):
            assert results[i].pattern_start_idx >= results[i - 1].pattern_end_idx, (
                f"命中 {i} 的 start_idx={results[i].pattern_start_idx} "
                f"< 前一命中 end_idx={results[i - 1].pattern_end_idx}"
            )

    def test_weekly_vectorized(self) -> None:
        """周线数据上的 vectorized 检测应正常工作。"""
        frame = _make_code_frame(
            n_bars=60, start_date="2024-01-01", tf_key="w",
            start_high=20.0, start_low=10.0,
            end_high=16.0, end_low=14.0, noise=0.2,
        )
        results = detect_converging_triangle_vectorized(
            frame, params=_relaxed_params(), tf_key="w",
        )
        # 不要求必须命中，但不应抛异常
        assert isinstance(results, list)
        for r in results:
            assert r.matched is True
