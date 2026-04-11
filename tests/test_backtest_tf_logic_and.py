"""
tf_logic='and' 交集逻辑单元测试。

验证 backtest_manager._intersect_hits_and() 对多周期命中记录按时间窗口
做 AND 交集后只保留最粗周期中与所有更细周期对齐的命中。
"""

from __future__ import annotations

import pandas as pd
import pytest

from app.services.backtest_manager import _intersect_hits_and


# ── 固定的 tf_sections 配置 ──

TF_SECTIONS_WD = {
    "weekly": {"tf_key": "w", "table": "klines_w"},
    "daily":  {"tf_key": "d", "table": "klines_d"},
}

TF_SECTIONS_WD60 = {
    "weekly": {"tf_key": "w", "table": "klines_w"},
    "daily":  {"tf_key": "d", "table": "klines_d"},
    "min60":  {"tf_key": "60", "table": "klines_60"},
}


def _hit(code: str, tf_key: str, end_ts: str) -> dict:
    return {
        "code": code,
        "tf_key": tf_key,
        "pattern_start_ts": pd.Timestamp(end_ts) - pd.Timedelta(days=5),
        "pattern_end_ts": pd.Timestamp(end_ts),
    }


# ── 测试用例 ──

class TestIntersectHitsAnd:
    """_intersect_hits_and 单元测试。"""

    def test_single_section_passthrough(self):
        """单周期不做交集，直接返回。"""
        hits_w = [_hit("000001", "w", "2025-01-10")]
        result = _intersect_hits_and({"weekly": hits_w}, TF_SECTIONS_WD)
        assert len(result) == 1
        assert result[0]["code"] == "000001"

    def test_aligned_hits_kept(self):
        """周线和日线在同一周内命中 → 保留。"""
        hits_w = [_hit("000001", "w", "2025-01-10")]  # Friday
        hits_d = [_hit("000001", "d", "2025-01-08")]  # Wednesday same week
        result = _intersect_hits_and(
            {"weekly": hits_w, "daily": hits_d}, TF_SECTIONS_WD,
        )
        assert len(result) == 1
        assert result[0]["code"] == "000001"
        assert result[0]["tf_key"] == "w"  # 保留最粗周期

    def test_misaligned_hits_dropped(self):
        """周线和日线命中日期相差超过 7 天 → 丢弃。"""
        hits_w = [_hit("000001", "w", "2025-01-10")]
        hits_d = [_hit("000001", "d", "2025-01-20")]  # 10 days later
        result = _intersect_hits_and(
            {"weekly": hits_w, "daily": hits_d}, TF_SECTIONS_WD,
        )
        assert len(result) == 0

    def test_code_missing_in_finer_section(self):
        """日线无该代码命中 → 周线命中被丢弃。"""
        hits_w = [_hit("000001", "w", "2025-01-10")]
        hits_d = [_hit("000002", "d", "2025-01-08")]  # different code
        result = _intersect_hits_and(
            {"weekly": hits_w, "daily": hits_d}, TF_SECTIONS_WD,
        )
        assert len(result) == 0

    def test_multiple_codes_partial_match(self):
        """多只股票，只有部分在所有周期对齐。"""
        hits_w = [
            _hit("000001", "w", "2025-01-10"),
            _hit("000002", "w", "2025-01-10"),
        ]
        hits_d = [
            _hit("000001", "d", "2025-01-09"),
            # 000002 没有日线命中
        ]
        result = _intersect_hits_and(
            {"weekly": hits_w, "daily": hits_d}, TF_SECTIONS_WD,
        )
        assert len(result) == 1
        assert result[0]["code"] == "000001"

    def test_three_sections_all_aligned(self):
        """三周期 (w/d/60) AND，全部对齐时保留。"""
        hits_w = [_hit("000001", "w", "2025-01-10")]
        hits_d = [_hit("000001", "d", "2025-01-09")]
        hits_60 = [_hit("000001", "60", "2025-01-09 14:00:00")]
        result = _intersect_hits_and(
            {"weekly": hits_w, "daily": hits_d, "min60": hits_60},
            TF_SECTIONS_WD60,
        )
        assert len(result) == 1

    def test_three_sections_one_missing(self):
        """三周期 AND，60 分钟线无命中 → 丢弃。"""
        hits_w = [_hit("000001", "w", "2025-01-10")]
        hits_d = [_hit("000001", "d", "2025-01-09")]
        hits_60: list[dict] = []
        result = _intersect_hits_and(
            {"weekly": hits_w, "daily": hits_d, "min60": hits_60},
            TF_SECTIONS_WD60,
        )
        assert len(result) == 0

    def test_same_day_intraday_alignment(self):
        """日线（midnight）与 60 分钟线（同天 14:00）对齐。"""
        tf_sections = {
            "daily": {"tf_key": "d", "table": "klines_d"},
            "min60": {"tf_key": "60", "table": "klines_60"},
        }
        hits_d = [_hit("000001", "d", "2025-01-10")]          # midnight
        hits_60 = [_hit("000001", "60", "2025-01-10 14:00:00")]  # same day
        result = _intersect_hits_and(
            {"daily": hits_d, "min60": hits_60}, tf_sections,
        )
        # 日线 anchor=midnight, 60min hit at 14:00 same day → within [anchor-1day, anchor+1day)
        assert len(result) == 1

    def test_empty_sections(self):
        """两个 section 都无命中 → 返回空。"""
        result = _intersect_hits_and(
            {"weekly": [], "daily": []}, TF_SECTIONS_WD,
        )
        assert len(result) == 0

    def test_coarsest_empty_finer_has_hits(self):
        """最粗周期无命中 → 返回空（无锚点）。"""
        hits_d = [_hit("000001", "d", "2025-01-09")]
        result = _intersect_hits_and(
            {"weekly": [], "daily": hits_d}, TF_SECTIONS_WD,
        )
        assert len(result) == 0

    def test_window_boundary_inclusive(self):
        """恰好在窗口边界上的命中应被保留。"""
        # weekly window = 7 days, so [Jan 3, Jan 11) for anchor Jan 10
        hits_w = [_hit("000001", "w", "2025-01-10")]
        hits_d = [_hit("000001", "d", "2025-01-03")]  # exactly 7 days before
        result = _intersect_hits_and(
            {"weekly": hits_w, "daily": hits_d}, TF_SECTIONS_WD,
        )
        assert len(result) == 1

    def test_window_boundary_exclusive(self):
        """刚超出窗口的命中应被丢弃。"""
        hits_w = [_hit("000001", "w", "2025-01-10")]
        hits_d = [_hit("000001", "d", "2025-01-02")]  # 8 days before
        result = _intersect_hits_and(
            {"weekly": hits_w, "daily": hits_d}, TF_SECTIONS_WD,
        )
        assert len(result) == 0
