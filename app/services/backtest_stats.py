"""
回测统计分析模块。

职责:
1. Per-stock 聚合 (profit/drawdown 取 max, sharpe 取 min)
2. 基础统计量 (DuckDB 下推)
3. 高阶统计 (scipy: 偏度/峰度/正态检验)
4. 直方图分箱数据 (供前端 ECharts)
"""

from __future__ import annotations

import logging
from typing import Any

import duckdb
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from scipy import stats as scipy_stats
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False
    logger.warning("scipy 未安装，正态检验等高阶统计将不可用")


def compute_full_stats(
    hits_df: pd.DataFrame,
    forward_bars: tuple[int, int, int],
) -> dict[str, Any]:
    """
    对命中结果 DataFrame 计算完整统计。

    返回包含 per_stock_agg、basic_stats、advanced_stats、histograms 的字典。
    """
    if hits_df.empty:
        return {
            "total_hits": 0,
            "unique_stocks": 0,
            "per_forward": {},
        }

    x, y, z = forward_bars
    labels = [("x", x), ("y", y), ("z", z)]

    con = duckdb.connect()
    try:
        con.register("hits", hits_df)

        # Per-stock 聚合
        agg_cols = []
        for label, _ in labels:
            agg_cols.append(f"MAX(profit_{label}_pct) AS agg_profit_{label}")
            agg_cols.append(f"MAX(drawdown_{label}_pct) AS agg_drawdown_{label}")
            agg_cols.append(f"MIN(sharpe_{label}) AS agg_sharpe_{label}")

        agg_sql = f"SELECT code, {', '.join(agg_cols)} FROM hits GROUP BY code"
        con.execute(f"CREATE TABLE per_stock AS ({agg_sql})")

        unique_stocks = con.execute("SELECT COUNT(*) FROM per_stock").fetchone()[0]

        result: dict[str, Any] = {
            "total_hits": len(hits_df),
            "unique_stocks": unique_stocks,
            "per_forward": {},
        }

        # 各前瞻周期统计
        for label, n in labels:
            fwd_stats = _compute_single_forward_stats(
                con, label, n, unique_stocks
            )
            result["per_forward"][str(n)] = fwd_stats

    finally:
        con.close()

    return result


def _compute_single_forward_stats(
    con: duckdb.DuckDBPyConnection,
    label: str,
    n: int,
    unique_stocks: int,
) -> dict[str, Any]:
    """计算单个前瞻周期的统计量。"""
    metrics_result: dict[str, Any] = {}

    for metric_name, col_name in [
        ("profit", f"agg_profit_{label}"),
        ("drawdown", f"agg_drawdown_{label}"),
        ("sharpe", f"agg_sharpe_{label}"),
    ]:
        # 基础统计 (DuckDB)
        basic_sql = f"""
        SELECT
            COUNT({col_name}) AS n,
            AVG({col_name}) AS mean,
            STDDEV_SAMP({col_name}) AS std,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col_name}) AS q25,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY {col_name}) AS q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col_name}) AS q75,
            MIN({col_name}) AS min_val,
            MAX({col_name}) AS max_val
        FROM per_stock
        WHERE {col_name} IS NOT NULL
        """
        row = con.execute(basic_sql).fetchone()
        count, mean, std, q25, q50, q75, min_val, max_val = row

        metric_stats: dict[str, Any] = {
            "count": count or 0,
            "mean": _safe_float(mean),
            "std": _safe_float(std),
            "q25": _safe_float(q25),
            "median": _safe_float(q50),
            "q75": _safe_float(q75),
            "min": _safe_float(min_val),
            "max": _safe_float(max_val),
        }

        # 胜率 (profit only)
        if metric_name == "profit":
            win_sql = f"""
            SELECT
                COUNT(*) FILTER (WHERE {col_name} > 0) * 100.0 / NULLIF(COUNT({col_name}), 0)
            FROM per_stock
            WHERE {col_name} IS NOT NULL
            """
            win_rate = con.execute(win_sql).fetchone()[0]
            metric_stats["win_rate"] = _safe_float(win_rate)

        # 高阶统计 (scipy)
        if _HAS_SCIPY and count and count >= 8:
            values = con.execute(
                f"SELECT {col_name} FROM per_stock WHERE {col_name} IS NOT NULL"
            ).fetchdf()[col_name].values

            metric_stats.update(_advanced_stats(values))

        # 直方图分箱
        if count and count > 0:
            bin_width = _auto_bin_width(std, count)
            if bin_width and bin_width > 0:
                hist_sql = f"""
                SELECT
                    FLOOR({col_name} / {bin_width}) * {bin_width} AS bin_start,
                    COUNT(*) AS freq
                FROM per_stock
                WHERE {col_name} IS NOT NULL
                GROUP BY 1
                ORDER BY 1
                """
                hist_rows = con.execute(hist_sql).fetchall()
                metric_stats["histogram"] = [
                    {"bin_start": _safe_float(r[0]), "freq": r[1]}
                    for r in hist_rows
                ]
                metric_stats["bin_width"] = bin_width

        metrics_result[metric_name] = metric_stats

    return metrics_result


def _advanced_stats(values: np.ndarray) -> dict[str, Any]:
    """计算偏度、峰度、正态检验。"""
    result: dict[str, Any] = {}

    try:
        result["skewness"] = _safe_float(scipy_stats.skew(values))
        result["kurtosis"] = _safe_float(scipy_stats.kurtosis(values))
    except Exception:
        pass

    n = len(values)
    try:
        if n < 5000:
            stat, p = scipy_stats.shapiro(values)
            result["normality_test"] = "shapiro"
        else:
            stat, p = scipy_stats.normaltest(values)
            result["normality_test"] = "dagostino"

        result["normality_stat"] = _safe_float(stat)
        result["normality_p"] = _safe_float(p)
        result["normality_reject"] = bool(p <= 0.05) if p is not None else None
    except Exception:
        pass

    try:
        jb_stat, jb_p = scipy_stats.jarque_bera(values)
        result["jb_stat"] = _safe_float(jb_stat)
        result["jb_p"] = _safe_float(jb_p)
    except Exception:
        pass

    return result


def _auto_bin_width(std: float | None, count: int) -> float | None:
    """Freedman-Diaconis 简化: 2 * IQR / n^(1/3)，回退到 std-based。"""
    if not std or not count or count < 2:
        return None
    # 简化: 使用 3.5 * std / n^(1/3) (Scott's rule)
    bw = 3.5 * std / (count ** (1.0 / 3.0))
    if bw <= 0:
        return None
    # 取整到1位有效数字
    import math
    magnitude = 10 ** math.floor(math.log10(abs(bw)))
    return round(bw / magnitude) * magnitude or magnitude


def _safe_float(v: Any) -> float | None:
    """安全转换为 float，NaN/Inf → None。"""
    if v is None:
        return None
    try:
        f = float(v)
        if np.isfinite(f):
            return round(f, 4)
        return None
    except (TypeError, ValueError):
        return None
