"""
GPU detect_batch 新增策略测试。

覆盖:
1. consecutive_uptrends_v1 detect_streak_batch (无急跌 + 含急跌)
   — GPU 批量结果 vs detect_streak_vectorized 一致性
2. weekly_oversold_rsi_v1 prepare_weekly_rsi_features_batch
   — GPU RSI vs CPU _compute_rsi_series 数值一致性
"""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def _make_boundaries(*lengths: int) -> np.ndarray:
    b = [0]
    for ln in lengths:
        b.append(b[-1] + ln)
    return np.array(b, dtype=np.int64)


def _make_ohlcv_df(n: int, *, bull_ranges: list[tuple[int, int]] | None = None,
                    seed: int = 42) -> pd.DataFrame:
    """生成合成 OHLCV DataFrame。

    bull_ranges: [(start, end), ...] 表示在这些区间强制 close > open (小阳)
    其余区间 close < open (阴线)
    """
    rng = np.random.RandomState(seed)
    base = 10.0
    ts = [datetime(2020, 1, 6) + timedelta(days=i * 7) for i in range(n)]
    opens = np.full(n, base) + rng.uniform(-0.5, 0.5, n)
    opens = np.maximum(opens, 0.5)

    closes = opens.copy()
    if bull_ranges:
        for s, e in bull_ranges:
            for i in range(s, min(e, n)):
                closes[i] = opens[i] * (1 + rng.uniform(0.001, 0.015))  # 0.1~1.5% 涨幅
        # 非 bull 区间设为阴线
        bull_set = set()
        for s, e in bull_ranges:
            for i in range(s, min(e, n)):
                bull_set.add(i)
        for i in range(n):
            if i not in bull_set:
                closes[i] = opens[i] * (1 - rng.uniform(0.01, 0.03))
    else:
        closes = opens * (1 + rng.uniform(-0.03, 0.03, n))

    highs = np.maximum(opens, closes) + rng.uniform(0, 0.3, n)
    lows = np.minimum(opens, closes) - rng.uniform(0, 0.3, n)
    lows = np.maximum(lows, 0.1)
    volumes = rng.uniform(1000, 5000, n)

    return pd.DataFrame({
        "code": "test001",
        "ts": ts,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
    })


# ---------------------------------------------------------------------------
# consecutive_uptrends_v1: detect_streak_batch 一致性
# ---------------------------------------------------------------------------

class TestStreakBatchConsistency:
    """GPU batch vs CPU vectorized 命中位置一致性。"""

    @pytest.fixture(autouse=True)
    def _skip_no_gpu(self):
        try:
            import cupy as cp
            cp.zeros(1)
        except Exception:
            pytest.skip("CuPy / GPU not available")

    def _run_comparison(self, *, selloff_enabled: bool):
        """CPU vectorized → 命中 end_idx 集合 vs GPU batch。"""
        import cupy as cp
        from strategies.engine_commons import assemble_batch, split_batch_hits
        from strategies.groups.consecutive_uptrends_v1.engine import (
            BACKTEST_HOOKS,
            detect_streak_batch,
            detect_streak_vectorized,
        )

        # 构造数据：2 只股票各 300 根 K 线
        # 股票1: 阳线 [50-57], [100-108], [200-206]
        # 股票2: 阳线 [20-26], [150-158]
        df1 = _make_ohlcv_df(300, bull_ranges=[(50, 58), (100, 109), (200, 207)], seed=42)
        df1["code"] = "stock_A"
        df2 = _make_ohlcv_df(300, bull_ranges=[(20, 27), (150, 159)], seed=99)
        df2["code"] = "stock_B"

        params = {
            "streak_len": 5,
            "bar_gain_max": 0.02,
            "selloff_enabled": selloff_enabled,
            "selloff_start_pct": 0.5,
            "selloff_return_pct": 0.7,
            "selloff_bars": 3,
        }
        det_kwargs = {"params": params}

        # CPU vectorized
        cpu_hits_1 = detect_streak_vectorized(df1.copy(), params)
        cpu_hits_2 = detect_streak_vectorized(df2.copy(), params)
        cpu_ends_1 = {r.pattern_end_idx for r in cpu_hits_1}
        cpu_ends_2 = {r.pattern_end_idx for r in cpu_hits_2}

        # GPU batch
        stock_dfs = [df1.reset_index(drop=True), df2.reset_index(drop=True)]
        stock_codes = ["stock_A", "stock_B"]
        batch_columns = detect_streak_batch._batch_columns
        cols_dict, boundaries, valid_codes = assemble_batch(stock_dfs, batch_columns, stock_codes)
        ts_flat = np.concatenate([df["ts"].values for df in stock_dfs])

        gpu_columns = {k: cp.asarray(v) for k, v in cols_dict.items()}
        matched_gpu = detect_streak_batch(gpu_columns, boundaries, det_kwargs)
        matched_cpu = cp.asnumpy(matched_gpu).astype(bool)

        pattern_span_fn = detect_streak_batch._pattern_span_fn
        gpu_hits = split_batch_hits(
            matched_cpu, boundaries, ts_flat, valid_codes, "w",
            pattern_span_fn=pattern_span_fn,
        )

        # 分按股票
        gpu_ends_A = {h["pattern_end_ts"] for h in gpu_hits if h["code"] == "stock_A"}
        gpu_ends_B = {h["pattern_end_ts"] for h in gpu_hits if h["code"] == "stock_B"}

        # CPU end_idx → ts
        cpu_ts_A = {df1["ts"].iloc[idx] for idx in cpu_ends_1}
        cpu_ts_B = {df2["ts"].iloc[idx] for idx in cpu_ends_2}

        assert gpu_ends_A == cpu_ts_A, f"stock_A mismatch: GPU={gpu_ends_A}, CPU={cpu_ts_A}"
        assert gpu_ends_B == cpu_ts_B, f"stock_B mismatch: GPU={gpu_ends_B}, CPU={cpu_ts_B}"

        # 验证 pattern_start 也一致
        for h in gpu_hits:
            assert h["pattern_start_ts"] <= h["pattern_end_ts"]

    def test_no_selloff(self):
        self._run_comparison(selloff_enabled=False)

    def test_with_selloff(self):
        self._run_comparison(selloff_enabled=True)

    def test_empty_data(self):
        """空数据不崩溃。"""
        import cupy as cp
        from strategies.groups.consecutive_uptrends_v1.engine import detect_streak_batch

        boundaries = _make_boundaries(0)
        gpu_columns = {"open": cp.array([], dtype=cp.float64),
                       "close": cp.array([], dtype=cp.float64)}
        params = {"streak_len": 5, "bar_gain_max": 0.02,
                  "selloff_enabled": False}
        matched = detect_streak_batch(gpu_columns, boundaries, {"params": params})
        assert len(matched) == 0


# ---------------------------------------------------------------------------
# weekly_oversold_rsi_v1: prepare_batch RSI 数值一致性
# ---------------------------------------------------------------------------

class TestRsiPrepareBatch:
    """GPU RSI vs CPU RSI 数值误差 < 1e-8。"""

    @pytest.fixture(autouse=True)
    def _skip_no_gpu(self):
        try:
            import cupy as cp
            cp.zeros(1)
        except Exception:
            pytest.skip("CuPy / GPU not available")

    def test_rsi_consistency(self):
        """多股票 GPU RSI 与 CPU RSI 逐值比对。"""
        import cupy as cp
        from strategies.engine_commons import assemble_batch
        from strategies.groups.weekly_oversold_rsi_v1.engine import (
            _compute_rsi_series,
            prepare_weekly_rsi_features_batch,
        )

        rng = np.random.RandomState(123)
        rsi_period = 14

        # 创建 3 只不同长度的股票
        lengths = [200, 50, 500]
        stock_dfs = []
        stock_codes = []
        cpu_rsi_list = []

        for idx, n in enumerate(lengths):
            closes = 10.0 + np.cumsum(rng.normal(0, 0.3, n))
            closes = np.maximum(closes, 0.5)
            ts = [datetime(2018, 1, 1) + timedelta(weeks=i) for i in range(n)]
            df = pd.DataFrame({
                "code": f"s{idx}",
                "ts": ts,
                "open": closes * 0.99,
                "high": closes * 1.02,
                "low": closes * 0.97,
                "close": closes,
                "volume": rng.uniform(1000, 5000, n),
            })
            stock_dfs.append(df)
            stock_codes.append(f"s{idx}")
            cpu_rsi_list.append(_compute_rsi_series(closes, rsi_period))

        # Assemble + upload
        raw_cols = ["open", "high", "low", "close", "volume"]
        raw_dict, boundaries, valid_codes = assemble_batch(stock_dfs, raw_cols, stock_codes)
        gpu_raw = {k: cp.asarray(v) for k, v in raw_dict.items()}

        params = {"rsi_period": rsi_period}
        gpu_result = prepare_weekly_rsi_features_batch(gpu_raw, boundaries, params)
        gpu_rsi = cp.asnumpy(gpu_result["rsi"])

        # 逐股票比对
        for seg_idx, (code, cpu_rsi) in enumerate(zip(stock_codes, cpu_rsi_list)):
            s = int(boundaries[seg_idx])
            e = int(boundaries[seg_idx + 1])
            gpu_seg = gpu_rsi[s:e]

            # 比对非 NaN 位置
            both_valid = ~np.isnan(cpu_rsi) & ~np.isnan(gpu_seg)
            assert both_valid.any(), f"{code}: no valid RSI values"
            np.testing.assert_allclose(
                gpu_seg[both_valid], cpu_rsi[both_valid],
                atol=1e-8, rtol=1e-10,
                err_msg=f"{code}: RSI mismatch",
            )

            # NaN 位置也对齐
            np.testing.assert_array_equal(
                np.isnan(cpu_rsi), np.isnan(gpu_seg),
                err_msg=f"{code}: NaN pattern mismatch",
            )

    def test_short_segment(self):
        """segment 长度 <= period 时全 NaN。"""
        import cupy as cp
        from strategies.engine_commons import assemble_batch
        from strategies.groups.weekly_oversold_rsi_v1.engine import (
            prepare_weekly_rsi_features_batch,
        )

        n = 10
        rsi_period = 14
        ts = [datetime(2020, 1, 1) + timedelta(weeks=i) for i in range(n)]
        df = pd.DataFrame({
            "code": "short",
            "ts": ts,
            "open": np.ones(n),
            "high": np.ones(n) * 1.1,
            "low": np.ones(n) * 0.9,
            "close": np.ones(n),
            "volume": np.ones(n) * 1000,
        })

        raw_cols = ["open", "high", "low", "close", "volume"]
        raw_dict, boundaries, _ = assemble_batch([df], raw_cols, ["short"])
        gpu_raw = {k: cp.asarray(v) for k, v in raw_dict.items()}

        result = prepare_weekly_rsi_features_batch(gpu_raw, boundaries, {"rsi_period": rsi_period})
        gpu_rsi = cp.asnumpy(result["rsi"])
        assert np.all(np.isnan(gpu_rsi)), "short segment should be all NaN"
