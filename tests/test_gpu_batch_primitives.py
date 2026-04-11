"""GPU 批量检测基础设施测试。

覆盖:
1. segmented 原语 (rolling_mean/shift/rolling_max/rolling_min/minimum_accumulate) — numpy 回退路径
2. assemble_batch / split_batch_hits 工具函数
3. 3 策略 detect_batch vs detect_vectorized 结果一致性
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from strategies.engine_commons import (
    assemble_batch,
    gpu_segmented_minimum_accumulate,
    gpu_segmented_rolling_max,
    gpu_segmented_rolling_mean,
    gpu_segmented_rolling_min,
    gpu_segmented_shift,
    split_batch_hits,
)

# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------

def _make_boundaries(*lengths: int) -> np.ndarray:
    """从 segment 长度列表构建 boundary 数组。"""
    b = [0]
    for ln in lengths:
        b.append(b[-1] + ln)
    return np.array(b, dtype=np.int64)


def _numpy_per_stock_rolling_mean(arr: np.ndarray, boundaries: np.ndarray, window: int) -> np.ndarray:
    """逐股票 numpy 参考实现：rolling mean。"""
    result = np.full(len(arr), np.nan)
    for i in range(len(boundaries) - 1):
        s, e = int(boundaries[i]), int(boundaries[i + 1])
        seg = pd.Series(arr[s:e])
        result[s:e] = seg.rolling(window).mean().values
    return result


def _numpy_per_stock_shift(arr: np.ndarray, boundaries: np.ndarray, periods: int) -> np.ndarray:
    """逐股票 numpy 参考实现：shift。"""
    result = np.full(len(arr), np.nan)
    for i in range(len(boundaries) - 1):
        s, e = int(boundaries[i]), int(boundaries[i + 1])
        seg = arr[s:e]
        if len(seg) > periods:
            result[s + periods:e] = seg[:-periods]
    return result


def _numpy_per_stock_rolling_max(arr: np.ndarray, boundaries: np.ndarray, window: int) -> np.ndarray:
    result = np.full(len(arr), np.nan)
    for i in range(len(boundaries) - 1):
        s, e = int(boundaries[i]), int(boundaries[i + 1])
        seg = pd.Series(arr[s:e])
        result[s:e] = seg.rolling(window).max().values
    return result


def _numpy_per_stock_rolling_min(arr: np.ndarray, boundaries: np.ndarray, window: int) -> np.ndarray:
    result = np.full(len(arr), np.nan)
    for i in range(len(boundaries) - 1):
        s, e = int(boundaries[i]), int(boundaries[i + 1])
        seg = pd.Series(arr[s:e])
        result[s:e] = seg.rolling(window).min().values
    return result


def _numpy_per_stock_minimum_accumulate(arr: np.ndarray, boundaries: np.ndarray) -> np.ndarray:
    result = np.empty(len(arr))
    for i in range(len(boundaries) - 1):
        s, e = int(boundaries[i]), int(boundaries[i + 1])
        result[s:e] = np.minimum.accumulate(arr[s:e])
    return result


# ---------------------------------------------------------------------------
# segmented 原语测试（纯 numpy 路径 — 无需 GPU）
# ---------------------------------------------------------------------------

class TestSegmentedRollingMean:
    def test_single_segment(self):
        arr = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        b = _make_boundaries(5)
        got = gpu_segmented_rolling_mean(arr, b, 3)
        expected = _numpy_per_stock_rolling_mean(arr, b, 3)
        np.testing.assert_allclose(got, expected, equal_nan=True)

    def test_two_segments(self):
        seg1 = np.arange(1.0, 11.0)  # 10 elements
        seg2 = np.arange(20.0, 26.0)  # 6 elements
        arr = np.concatenate([seg1, seg2])
        b = _make_boundaries(10, 6)
        got = gpu_segmented_rolling_mean(arr, b, 3)
        expected = _numpy_per_stock_rolling_mean(arr, b, 3)
        np.testing.assert_allclose(got, expected, equal_nan=True)

    def test_segment_shorter_than_window(self):
        arr = np.array([1.0, 2.0, 10.0, 20.0, 30.0, 40.0, 50.0])
        b = _make_boundaries(2, 5)
        got = gpu_segmented_rolling_mean(arr, b, 3)
        expected = _numpy_per_stock_rolling_mean(arr, b, 3)
        np.testing.assert_allclose(got, expected, equal_nan=True)

    def test_empty(self):
        arr = np.array([], dtype=np.float64)
        b = np.array([0], dtype=np.int64)
        got = gpu_segmented_rolling_mean(arr, b, 3)
        assert len(got) == 0


class TestSegmentedShift:
    def test_basic(self):
        seg1 = np.array([10.0, 20.0, 30.0, 40.0])
        seg2 = np.array([100.0, 200.0, 300.0])
        arr = np.concatenate([seg1, seg2])
        b = _make_boundaries(4, 3)
        got = gpu_segmented_shift(arr, b, 2)
        expected = _numpy_per_stock_shift(arr, b, 2)
        np.testing.assert_allclose(got, expected, equal_nan=True)

    def test_period_larger_than_segment(self):
        arr = np.array([1.0, 2.0, 100.0])
        b = _make_boundaries(2, 1)
        got = gpu_segmented_shift(arr, b, 3)
        expected = np.full(3, np.nan)
        np.testing.assert_allclose(got, expected, equal_nan=True)


class TestSegmentedRollingMax:
    def test_basic(self):
        np.random.seed(42)
        seg1 = np.random.rand(20)
        seg2 = np.random.rand(15)
        arr = np.concatenate([seg1, seg2])
        b = _make_boundaries(20, 15)
        got = gpu_segmented_rolling_max(arr, b, 5)
        expected = _numpy_per_stock_rolling_max(arr, b, 5)
        np.testing.assert_allclose(got, expected, equal_nan=True)


class TestSegmentedRollingMin:
    def test_basic(self):
        np.random.seed(42)
        seg1 = np.random.rand(20)
        seg2 = np.random.rand(15)
        arr = np.concatenate([seg1, seg2])
        b = _make_boundaries(20, 15)
        got = gpu_segmented_rolling_min(arr, b, 5)
        expected = _numpy_per_stock_rolling_min(arr, b, 5)
        np.testing.assert_allclose(got, expected, equal_nan=True)


class TestSegmentedMinimumAccumulate:
    def test_basic(self):
        seg1 = np.array([5.0, 3.0, 4.0, 2.0, 6.0])
        seg2 = np.array([10.0, 8.0, 9.0])
        arr = np.concatenate([seg1, seg2])
        b = _make_boundaries(5, 3)
        got = gpu_segmented_minimum_accumulate(arr, b)
        expected = _numpy_per_stock_minimum_accumulate(arr, b)
        np.testing.assert_allclose(got, expected)

    def test_three_segments(self):
        np.random.seed(123)
        seg1 = np.random.rand(10) * 100
        seg2 = np.random.rand(8) * 50
        seg3 = np.random.rand(12) * 200
        arr = np.concatenate([seg1, seg2, seg3])
        b = _make_boundaries(10, 8, 12)
        got = gpu_segmented_minimum_accumulate(arr, b)
        expected = _numpy_per_stock_minimum_accumulate(arr, b)
        np.testing.assert_allclose(got, expected)


# ---------------------------------------------------------------------------
# assemble_batch / split_batch_hits
# ---------------------------------------------------------------------------

class TestAssembleBatch:
    def test_basic(self):
        df1 = pd.DataFrame({"close": [1.0, 2.0, 3.0], "volume": [100, 200, 300]})
        df2 = pd.DataFrame({"close": [10.0, 20.0], "volume": [1000, 2000]})
        cols, boundaries, codes = assemble_batch(
            [df1, df2], ["close", "volume"], ["A", "B"],
        )
        assert codes == ["A", "B"]
        np.testing.assert_array_equal(boundaries, [0, 3, 5])
        np.testing.assert_allclose(cols["close"], [1, 2, 3, 10, 20])
        np.testing.assert_allclose(cols["volume"], [100, 200, 300, 1000, 2000])

    def test_skip_empty(self):
        df1 = pd.DataFrame({"x": [1.0]})
        df_empty = pd.DataFrame({"x": pd.array([], dtype=float)})
        df2 = pd.DataFrame({"x": [2.0, 3.0]})
        cols, boundaries, codes = assemble_batch(
            [df1, df_empty, df2], ["x"], ["A", "B", "C"],
        )
        assert codes == ["A", "C"]
        np.testing.assert_array_equal(boundaries, [0, 1, 3])


class TestSplitBatchHits:
    def test_basic(self):
        mask = np.array([False, True, False, True, False], dtype=bool)
        boundaries = np.array([0, 3, 5], dtype=np.int64)
        ts = np.array([100, 200, 300, 400, 500])
        codes = ["A", "B"]
        hits = split_batch_hits(mask, boundaries, ts, codes, "d")
        assert len(hits) == 2
        assert hits[0]["code"] == "A"
        assert hits[0]["pattern_start_ts"] == 200
        assert hits[1]["code"] == "B"
        assert hits[1]["pattern_start_ts"] == 400

    def test_no_hits(self):
        mask = np.zeros(5, dtype=bool)
        boundaries = np.array([0, 5], dtype=np.int64)
        ts = np.arange(5)
        hits = split_batch_hits(mask, boundaries, ts, ["X"], "w")
        assert hits == []


# ---------------------------------------------------------------------------
# detect_batch vs detect_vectorized 一致性
# ---------------------------------------------------------------------------

try:
    import cupy as cp
    _HAS_CUPY = True
except ImportError:
    _HAS_CUPY = False

_skip_no_cupy = pytest.mark.skipif(not _HAS_CUPY, reason="cupy not installed")


def _to_gpu_columns(cols_dict: dict[str, np.ndarray]) -> dict:
    """将 numpy 列字典转为 cupy GPU 数组。"""
    import cupy as cp
    return {k: cp.asarray(v) for k, v in cols_dict.items()}


def _generate_ohlcv(n: int, seed: int = 0) -> pd.DataFrame:
    """生成模拟 OHLCV 数据。"""
    rng = np.random.RandomState(seed)
    close = 10.0 + np.cumsum(rng.randn(n) * 0.3)
    close = np.maximum(close, 1.0)
    high = close + rng.rand(n) * 0.5
    low = close - rng.rand(n) * 0.5
    low = np.maximum(low, 0.1)
    open_ = low + rng.rand(n) * (high - low)
    vol = rng.randint(1000, 10000, size=n).astype(float)
    ts = np.arange(n, dtype=np.int64) * 86400
    return pd.DataFrame({
        "code": "TEST",
        "ts": ts,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": vol,
    })


@_skip_no_cupy
class TestWeeklyRsiBatchConsistency:
    """weekly_oversold_rsi detect_batch vs detect_vectorized 一致性。"""

    def test_single_stock_consistency(self):
        from strategies.groups.weekly_oversold_rsi_v1.engine import (
            detect_weekly_rsi_batch,
            detect_weekly_rsi_vectorized,
            prepare_weekly_rsi_features,
        )

        df = _generate_ohlcv(200, seed=42)
        params = {
            "rsi_period": 14,
            "rsi_threshold": 40.0,
            "historical_ratio_pct": 1.1,
        }
        prepared = prepare_weekly_rsi_features(df.copy(), params)

        # 向量化路径
        vec_results = detect_weekly_rsi_vectorized(prepared.copy(), params)
        vec_hit_indices = {r.pattern_end_idx for r in vec_results}

        # batch 路径（numpy fallback — columns 保持 numpy）
        stock_dfs = [prepared]
        cols_dict, boundaries, valid_codes = assemble_batch(
            stock_dfs, ["close", "rsi"], ["TEST"],
        )
        det_kwargs = {"params": params}
        gpu_cols = _to_gpu_columns(cols_dict)
        mask = detect_weekly_rsi_batch(gpu_cols, boundaries, det_kwargs)
        mask = cp.asnumpy(mask)
        batch_hit_indices = set(np.where(mask)[0])

        assert vec_hit_indices == batch_hit_indices, (
            f"向量化命中 {vec_hit_indices} != 批量命中 {batch_hit_indices}"
        )

    def test_multi_stock_consistency(self):
        from strategies.groups.weekly_oversold_rsi_v1.engine import (
            detect_weekly_rsi_batch,
            detect_weekly_rsi_vectorized,
            prepare_weekly_rsi_features,
        )

        params = {
            "rsi_period": 14,
            "rsi_threshold": 35.0,
            "historical_ratio_pct": 1.05,
        }

        stock_dfs = []
        vec_all_hits: dict[str, set[int]] = {}
        for i, code in enumerate(["A", "B", "C"]):
            df = _generate_ohlcv(150, seed=i * 10)
            df["code"] = code
            prepared = prepare_weekly_rsi_features(df.copy(), params)
            stock_dfs.append(prepared)
            vec_results = detect_weekly_rsi_vectorized(prepared.copy(), params)
            vec_all_hits[code] = {r.pattern_end_idx for r in vec_results}

        cols_dict, boundaries, valid_codes = assemble_batch(
            stock_dfs, ["close", "rsi"], ["A", "B", "C"],
        )
        gpu_cols = _to_gpu_columns(cols_dict)
        mask = detect_weekly_rsi_batch(gpu_cols, boundaries, {"params": params})
        mask = cp.asnumpy(mask)

        # 按 segment 拆分验证
        for seg_idx, code in enumerate(valid_codes):
            s = int(boundaries[seg_idx])
            e = int(boundaries[seg_idx + 1])
            seg_hits = set(np.where(mask[s:e])[0])
            assert seg_hits == vec_all_hits[code], (
                f"股票 {code}: 向量化 {vec_all_hits[code]} != 批量 {seg_hits}"
            )


@_skip_no_cupy
class TestXianrenZhiluBatchConsistency:
    """xianren_zhilu detect_batch vs detect_vectorized 一致性。"""

    def test_single_stock_consistency(self):
        from strategies.groups.xianren_zhilu_v1.engine import (
            detect_xianren_zhilu_batch,
            detect_xianren_zhilu_vectorized,
            prepare_xianren_zhilu_features,
        )

        df = _generate_ohlcv(300, seed=7)
        params = {
            "shadow_ratio": 50.0,
            "lower_shadow_pct": 30.0,
            "max_gain_pct": 2.0,
            "volume_multiplier": 1.2,
            "lookback_bars": 20,
            "ma_period": 10,
            "price_decline_ratio": 1.02,
        }
        prepared = prepare_xianren_zhilu_features(df.copy(), params)

        vec_results = detect_xianren_zhilu_vectorized(prepared.copy(), params)
        vec_hit_indices = {r.pattern_end_idx for r in vec_results}

        batch_columns = ["open", "high", "low", "close", "volume", "ma"]
        cols_dict, boundaries, valid_codes = assemble_batch(
            [prepared], batch_columns, ["TEST"],
        )
        gpu_cols = _to_gpu_columns(cols_dict)
        mask = detect_xianren_zhilu_batch(gpu_cols, boundaries, {"params": params})
        mask = cp.asnumpy(mask)
        batch_hit_indices = set(np.where(mask)[0])

        assert vec_hit_indices == batch_hit_indices, (
            f"向量化命中 {vec_hit_indices} != 批量命中 {batch_hit_indices}"
        )


@_skip_no_cupy
class TestMultiTfMaBatchConsistency:
    """multi_tf_ma_uptrend detect_batch vs detect_vectorized 一致性。"""

    def test_single_stock_consistency(self):
        from strategies.groups.multi_tf_ma_uptrend_v1.engine import (
            detect_multi_tf_ma_uptrend_batch,
            detect_multi_tf_ma_uptrend_vectorized,
            prepare_multi_tf_ma_features,
        )

        df = _generate_ohlcv(300, seed=99)
        params = {
            "enabled": True,
            "n": 20,
            "ma_period": 10,
            "slope_gap": 5,
            "slope_min": -5.0,
            "slope_max": 50.0,
            "continuous_check": False,
            "close_deviation_enabled": False,
            "close_deviation_pct": 5.0,
            "consolidation_enabled": False,
            "consolidation_bars": 10,
            "consolidation_max_amp_pct": 20.0,
        }
        prepared = prepare_multi_tf_ma_features(df.copy(), params)

        vec_results = detect_multi_tf_ma_uptrend_vectorized(
            prepared.copy(), params, tf_key="d",
        )
        vec_hit_indices = {r.pattern_end_idx for r in vec_results}

        batch_columns = ["open", "high", "low", "close", "volume", "ma", "slope_pct", "ma_dev"]
        cols_dict, boundaries, valid_codes = assemble_batch(
            [prepared], batch_columns, ["TEST"],
        )
        det_kwargs = {"params": params, "tf_key": "d"}
        gpu_cols = _to_gpu_columns(cols_dict)
        mask = detect_multi_tf_ma_uptrend_batch(gpu_cols, boundaries, det_kwargs)
        mask = cp.asnumpy(mask)
        batch_hit_indices = set(np.where(mask)[0])

        assert vec_hit_indices == batch_hit_indices, (
            f"向量化命中 {vec_hit_indices} != 批量命中 {batch_hit_indices}"
        )
