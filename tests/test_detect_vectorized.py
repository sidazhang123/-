"""
detect_vectorized 实现的正确性验证。

验证策略:
1. 构造已知数据，用 detect（滑窗）和 detect_vectorized（一次扫描）分别运行。
2. 比较两者产生的命中 pattern_end_idx 集合是否一致。
3. 由于滑窗路径受 slide_step 影响可能跳过部分 bar，
   vectorized 的命中集是滑窗命中集的超集（slide_step=1 时相等）。
"""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from strategies.engine_commons import DetectionResult


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────


def _make_ts_series(n: int, start: datetime | None = None, freq_days: int = 7) -> list[datetime]:
    """生成 n 个等间隔时间戳。"""
    base = start or datetime(2020, 1, 6)
    return [base + timedelta(days=i * freq_days) for i in range(n)]


def _sliding_window_detect(code_df: pd.DataFrame, detect_fn, det_kwargs: dict, slide_step: int = 1) -> list[int]:
    """模拟回测引擎的原始滑窗路径，返回命中的 pattern_end_idx 列表。"""
    n = len(code_df)
    pos = slide_step
    hits: list[int] = []
    while pos < n:
        sub_df = code_df.iloc[: pos + 1].copy()
        result: DetectionResult = detect_fn(sub_df, **det_kwargs)
        if result.matched and result.pattern_end_idx is not None:
            hits.append(result.pattern_end_idx)
            pos = result.pattern_end_idx + 1
        else:
            pos += slide_step
    return hits


# ─────────────────────────────────────────────────────────────────────────────
# weekly_oversold_rsi_v1
# ─────────────────────────────────────────────────────────────────────────────


class TestWeeklyOversoldRsiVectorized:
    """验证 weekly_oversold_rsi_v1 的 detect_vectorized 与滑窗 detect 一致性。"""

    @pytest.fixture
    def rsi_module(self):
        from strategies.groups.weekly_oversold_rsi_v1 import engine
        return engine

    def _build_rsi_frame(self, rsi_module, closes: list[float], rsi_period: int = 14) -> pd.DataFrame:
        """构造带 RSI 列的 DataFrame。"""
        n = len(closes)
        df = pd.DataFrame({
            "code": ["TEST"] * n,
            "ts": _make_ts_series(n),
            "close": closes,
        })
        params = {"rsi_period": rsi_period}
        return rsi_module.prepare_weekly_rsi_features(df, params)

    def test_vectorized_superset_of_sliding(self, rsi_module):
        """vectorized 命中集应包含 slide_step=1 的滑窗命中集。"""
        # 构造 RSI 趋势下行数据：先平稳后暴跌
        np.random.seed(42)
        n = 200
        closes = [50.0]
        for i in range(1, n):
            if i < 120:
                closes.append(closes[-1] + np.random.normal(0, 0.5))
            else:
                closes.append(closes[-1] - np.random.uniform(0.3, 1.5))
        closes = [max(c, 1.0) for c in closes]

        params = {"rsi_period": 14, "rsi_threshold": 30.0, "historical_ratio_pct": 1.2}
        df = self._build_rsi_frame(rsi_module, closes)

        # 滑窗
        sliding_hits = _sliding_window_detect(df, rsi_module.detect_weekly_rsi, {"params": params})

        # vectorized
        vec_results = rsi_module.detect_weekly_rsi_vectorized(df, params)
        vec_hits = [r.pattern_end_idx for r in vec_results]

        assert set(sliding_hits).issubset(set(vec_hits)), (
            f"滑窗命中 {sliding_hits} 不是 vectorized 命中 {vec_hits} 的子集"
        )

    def test_empty_returns_empty(self, rsi_module):
        """空数据返回空列表。"""
        df = pd.DataFrame()
        params = {"rsi_period": 14, "rsi_threshold": 30.0, "historical_ratio_pct": 1.2}
        results = rsi_module.detect_weekly_rsi_vectorized(df, params)
        assert results == []

    def test_no_match_returns_empty(self, rsi_module):
        """RSI 一直高于阈值时返回空列表。"""
        closes = [50.0 + i * 0.1 for i in range(100)]  # 持续上涨
        params = {"rsi_period": 14, "rsi_threshold": 20.0, "historical_ratio_pct": 1.2}
        df = self._build_rsi_frame(rsi_module, closes)
        results = rsi_module.detect_weekly_rsi_vectorized(df, params)
        assert results == []


# ─────────────────────────────────────────────────────────────────────────────
# xianren_zhilu_v1
# ─────────────────────────────────────────────────────────────────────────────


class TestXianrenZhiluVectorized:
    """验证 xianren_zhilu_v1 的 detect_vectorized 与滑窗 detect 一致性。"""

    @pytest.fixture
    def xr_module(self):
        from strategies.groups.xianren_zhilu_v1 import engine
        return engine

    def _build_xr_frame(self, xr_module, rows: list[dict], ma_period: int = 5) -> pd.DataFrame:
        """构造带 MA 列的 DataFrame。"""
        df = pd.DataFrame(rows)
        params = {"ma_period": ma_period}
        return xr_module.prepare_xianren_zhilu_features(df, params)

    def test_vectorized_superset_of_sliding(self, xr_module):
        """vectorized 命中集应包含 slide_step=1 的滑窗命中集。"""
        np.random.seed(123)
        n = 200
        rows = []
        base_ts = datetime(2020, 1, 6)
        price = 30.0
        for i in range(n):
            # 模拟下降后偶尔出现仙人指路
            price = max(price + np.random.normal(-0.05, 0.3), 5.0)
            o = price
            c = o + abs(np.random.normal(0, 0.3))  # 阳线
            h = c + abs(np.random.normal(0, 0.8))   # 上影线
            lo = o - abs(np.random.normal(0, 0.1))
            vol = np.random.uniform(1000, 5000)
            # 周期性放量
            if i % 20 == 0 and i > 20:
                vol *= 5
            rows.append({
                "code": "TEST",
                "ts": base_ts + timedelta(days=i),
                "open": round(o, 2),
                "high": round(h, 2),
                "low": round(lo, 2),
                "close": round(c, 2),
                "volume": round(vol, 2),
            })

        params = {
            "enabled": True,
            "ma_period": 5,
            "shadow_ratio": 0.6,
            "volume_multiplier": 1.3,
            "lookback_bars": 5,
            "price_decline_ratio": 1.0,  # 放宽以便有命中
        }
        df = self._build_xr_frame(xr_module, rows, ma_period=5)

        sliding_hits = _sliding_window_detect(df, xr_module.detect_xianren_zhilu, {"params": params})
        vec_results = xr_module.detect_xianren_zhilu_vectorized(df, params)
        vec_hits = [r.pattern_end_idx for r in vec_results]

        assert set(sliding_hits).issubset(set(vec_hits)), (
            f"滑窗命中 {sliding_hits} 不是 vectorized 命中 {vec_hits} 的子集"
        )

    def test_empty_returns_empty(self, xr_module):
        """空数据返回空列表。"""
        df = pd.DataFrame()
        params = {
            "shadow_ratio": 0.6,
            "volume_multiplier": 1.3,
            "lookback_bars": 5,
            "ma_period": 5,
            "price_decline_ratio": 1.2,
        }
        results = xr_module.detect_xianren_zhilu_vectorized(df, params)
        assert results == []


# ─────────────────────────────────────────────────────────────────────────────
# multi_tf_ma_uptrend_v1
# ─────────────────────────────────────────────────────────────────────────────


class TestMultiTfMaUptrendVectorized:
    """验证 multi_tf_ma_uptrend_v1 的 detect_vectorized 与滑窗 detect 一致性。"""

    @pytest.fixture
    def ma_module(self):
        from strategies.groups.multi_tf_ma_uptrend_v1 import engine
        return engine

    def _build_ma_frame(self, ma_module, closes: list[float], params: dict) -> pd.DataFrame:
        """构造带 ma/slope_pct/ma_dev 列的 DataFrame。"""
        n = len(closes)
        highs = [c * 1.02 for c in closes]
        lows = [c * 0.98 for c in closes]
        df = pd.DataFrame({
            "code": ["TEST"] * n,
            "ts": _make_ts_series(n, freq_days=1),
            "open": closes,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [1000.0] * n,
        })
        return ma_module.prepare_multi_tf_ma_features(df, params)

    def test_vectorized_superset_of_sliding_non_continuous(self, ma_module):
        """非连续模式：vectorized 命中集应包含 slide_step=1 的滑窗命中集。"""
        np.random.seed(7)
        n = 300
        # 构造上升趋势数据
        closes = [20.0]
        for i in range(1, n):
            closes.append(closes[-1] + np.random.uniform(-0.1, 0.3))
        closes = [max(c, 1.0) for c in closes]

        params = {
            "enabled": True,
            "n": 20,
            "ma_period": 10,
            "slope_gap": 4,
            "slope_min": 0.0,
            "slope_max": 50.0,
            "continuous_check": False,
            "close_deviation_enabled": False,
            "close_deviation_ma_period": 10,
            "close_deviation_pct": 5.0,
            "consolidation_enabled": False,
            "consolidation_bars": 3,
            "consolidation_max_amp_pct": 3.0,
        }
        df = self._build_ma_frame(ma_module, closes, params)

        sliding_hits = _sliding_window_detect(
            df, ma_module.detect_multi_tf_ma_uptrend,
            {"params": params, "tf_key": "d"},
        )
        vec_results = ma_module.detect_multi_tf_ma_uptrend_vectorized(df, params, "d")
        vec_hits = [r.pattern_end_idx for r in vec_results]

        assert set(sliding_hits).issubset(set(vec_hits)), (
            f"滑窗命中 {sliding_hits} 不是 vectorized 命中 {vec_hits} 的子集"
        )

    def test_vectorized_superset_continuous_mode(self, ma_module):
        """连续模式：vectorized 命中集应包含 slide_step=1 的滑窗命中集。"""
        np.random.seed(77)
        n = 300
        closes = [20.0]
        for i in range(1, n):
            closes.append(closes[-1] + np.random.uniform(-0.05, 0.25))
        closes = [max(c, 1.0) for c in closes]

        params = {
            "enabled": True,
            "n": 20,
            "ma_period": 10,
            "slope_gap": 4,
            "slope_min": 0.0,
            "slope_max": 50.0,
            "continuous_check": True,
            "close_deviation_enabled": False,
            "close_deviation_ma_period": 10,
            "close_deviation_pct": 5.0,
            "consolidation_enabled": False,
            "consolidation_bars": 3,
            "consolidation_max_amp_pct": 3.0,
        }
        df = self._build_ma_frame(ma_module, closes, params)

        sliding_hits = _sliding_window_detect(
            df, ma_module.detect_multi_tf_ma_uptrend,
            {"params": params, "tf_key": "d"},
        )
        vec_results = ma_module.detect_multi_tf_ma_uptrend_vectorized(df, params, "d")
        vec_hits = [r.pattern_end_idx for r in vec_results]

        assert set(sliding_hits).issubset(set(vec_hits)), (
            f"滑窗命中 {sliding_hits} 不是 vectorized 命中 {vec_hits} 的子集"
        )

    def test_empty_returns_empty(self, ma_module):
        """空数据返回空列表。"""
        df = pd.DataFrame()
        params = {
            "enabled": True, "n": 20, "ma_period": 10, "slope_gap": 4,
            "slope_min": 0.0, "slope_max": 50.0, "continuous_check": False,
            "close_deviation_enabled": False, "close_deviation_ma_period": 10,
            "close_deviation_pct": 5.0, "consolidation_enabled": False,
            "consolidation_bars": 3, "consolidation_max_amp_pct": 3.0,
        }
        results = ma_module.detect_multi_tf_ma_uptrend_vectorized(df, params, "d")
        assert results == []

    def test_disabled_returns_empty(self, ma_module):
        """禁用时返回空列表。"""
        params = {
            "enabled": False, "n": 20, "ma_period": 10, "slope_gap": 4,
            "slope_min": 0.0, "slope_max": 50.0, "continuous_check": False,
            "close_deviation_enabled": False, "close_deviation_ma_period": 10,
            "close_deviation_pct": 5.0, "consolidation_enabled": False,
            "consolidation_bars": 3, "consolidation_max_amp_pct": 3.0,
        }
        df = pd.DataFrame({"ts": [datetime(2020, 1, 1)], "slope_pct": [10.0],
                           "close": [50.0], "ma_dev": [50.0], "high": [51.0], "low": [49.0]})
        results = ma_module.detect_multi_tf_ma_uptrend_vectorized(df, params, "d")
        assert results == []


# ─────────────────────────────────────────────────────────────────────────────
# consecutive_uptrends_v1
# ─────────────────────────────────────────────────────────────────────────────


class TestConsecutiveUptrendsVectorized:
    """验证 consecutive_uptrends_v1 的 detect_streak_vectorized 与滑窗 detect_streak 一致性。"""

    @pytest.fixture
    def cu_module(self):
        from strategies.groups.consecutive_uptrends_v1 import engine
        return engine

    @staticmethod
    def _build_frame(closes: list[float], opens: list[float] | None = None) -> pd.DataFrame:
        """构造测试用 OHLCV DataFrame。open 默认为 close - 0.3（保证阳线）。"""
        n = len(closes)
        if opens is None:
            opens = [c - 0.3 for c in closes]
        return pd.DataFrame({
            "code": ["TEST"] * n,
            "ts": _make_ts_series(n, freq_days=1),
            "open": opens,
            "high": [max(o, c) + 0.1 for o, c in zip(opens, closes)],
            "low": [min(o, c) - 0.1 for o, c in zip(opens, closes)],
            "close": closes,
            "volume": [1000] * n,
        })

    def test_no_selloff_finds_known_streaks(self, cu_module):
        """无急跌模式：vectorized 应找到构造数据中的已知连阳段。"""
        # 构造数据：在 [20,25) 和 [60,65) 区间放入确定性小阳（涨 1%），
        # 其余区间全部构造为阴线（open > close）以确保不产生额外连阳。
        n = 80
        closes = []
        opens_list = []
        base = 10.0
        for i in range(n):
            if 20 <= i < 25 or 60 <= i < 65:
                base *= 1.01
                o = base / 1.01
            else:
                # 阴线：open > close
                o = base + 0.5
            closes.append(round(base, 4))
            opens_list.append(round(o, 4))

        df = self._build_frame(closes, opens_list)
        params = {
            "scan_bars": 200,
            "streak_len": 5,
            "bar_gain_max": 0.02,
            "selloff_enabled": False,
            "selloff_start_pct": 0.5,
            "selloff_return_pct": 0.5,
            "selloff_bars": 3,
        }

        vec_results = cu_module.detect_streak_vectorized(df, params)
        vec_ends = [r.pattern_end_idx for r in vec_results]
        vec_starts = [r.pattern_start_idx for r in vec_results]

        # 应恰好找到两段连阳：[20,24] 和 [60,64]
        assert len(vec_results) == 2, f"期望 2 段命中，实际 {len(vec_results)}: ends={vec_ends}"
        assert vec_starts[0] == 20 and vec_ends[0] == 24
        assert vec_starts[1] == 60 and vec_ends[1] == 64
        for r in vec_results:
            assert r.matched is True

    def test_selloff_finds_known_pattern(self, cu_module):
        """急跌模式：vectorized 应找到连阳 + 急跌段的已知组合。"""
        # 构造数据：[30,35) 5 根小阳（涨 ~1.5%），[36,39) 3 根急跌，其余阴线。
        n = 60
        closes = []
        opens_list = []
        base = 20.0
        for i in range(n):
            if 30 <= i < 35:
                base *= 1.015
                o = base / 1.015
            elif 36 <= i < 39:
                base *= 0.97
                o = base / 0.97
            else:
                o = base + 0.5  # 阴线
            closes.append(round(base, 4))
            opens_list.append(round(o, 4))

        df = self._build_frame(closes, opens_list)
        params = {
            "scan_bars": 200,
            "streak_len": 5,
            "bar_gain_max": 0.02,
            "selloff_enabled": True,
            "selloff_start_pct": 0.5,       # 偏移上限 = floor(0.5 * 5) = 2
            "selloff_return_pct": 0.3,       # 回撤 >= 30% 总涨幅
            "selloff_bars": 3,
        }

        vec_results = cu_module.detect_streak_vectorized(df, params)

        # 连阳段 [30,34]，急跌搜索从 35 开始，selloff_bars=3 → sell_end=37
        assert len(vec_results) >= 1, f"期望至少 1 段命中，实际 {len(vec_results)}"
        r = vec_results[0]
        assert r.matched is True
        assert r.pattern_start_idx == 30
        assert r.pattern_end_idx == 37, f"期望 end=37，实际 {r.pattern_end_idx}"

    def test_empty_returns_empty(self, cu_module):
        """空数据返回空列表。"""
        df = pd.DataFrame()
        params = {
            "scan_bars": 50, "streak_len": 5, "bar_gain_max": 0.02,
            "selloff_enabled": False, "selloff_start_pct": 0.5,
            "selloff_return_pct": 0.5, "selloff_bars": 3,
        }
        results = cu_module.detect_streak_vectorized(df, params)
        assert results == []
