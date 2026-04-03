"""
回测核心引擎。

四阶段流水线:
  A — DuckDB WINDOW 前瞻指标预计算 (两级缓存)
  B — 滑动检测 (Python, ProcessPoolExecutor 并行)
  C — 指标解析 (DuckDB JOIN + numpy Sharpe)
  D — 结果输出 (DuckDB COPY TO CSV)
"""

from __future__ import annotations

import importlib
import json
import logging
import multiprocessing
import os
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import duckdb
import numpy as np
import pandas as pd

from app import settings
from strategies.engine_commons import DetectionResult, connect_source_readonly

if TYPE_CHECKING:
    from multiprocessing import Queue as _MpQueue
    from multiprocessing.synchronize import Event as _MpEvent

logger = logging.getLogger(__name__)

# 年化因子: tf_key → sqrt(年化bar数)
_ANNUALIZE_FACTOR: dict[str, float] = {
    "w": np.sqrt(52),
    "d": np.sqrt(252),
    "60": np.sqrt(252 * 4),
    "30": np.sqrt(252 * 8),
    "15": np.sqrt(252 * 16),
}

# 半个月对应的交易 bar 数: 用于命中去重
_HALF_MONTH_BARS: dict[str, int] = {
    "w": 2,
    "d": 10,
    "60": 40,
    "30": 80,
    "15": 160,
}


# ─────────────────────────────────────────────────────────────────────────────
# 阶段 A: 前瞻指标预计算 + 缓存
# ─────────────────────────────────────────────────────────────────────────────

def _fwd_parquet_path(fwd_cache_dir: Path, tf_key: str, x: int, y: int, z: int) -> Path:
    return fwd_cache_dir / f"{tf_key}_{x}_{y}_{z}.parquet"


def _version_sidecar(parquet_path: Path) -> Path:
    return parquet_path.with_suffix(".ver")


def _read_parquet_version(path: Path) -> str | None:
    """读取 Parquet 缓存对应的 source_data_version（sidecar 文件）。"""
    sidecar = _version_sidecar(path)
    if not sidecar.exists():
        return None
    try:
        return sidecar.read_text(encoding="utf-8").strip() or None
    except Exception:
        return None


def _write_parquet_version(path: Path, version: str) -> None:
    """将 source_data_version 写入 sidecar 文件。"""
    try:
        _version_sidecar(path).write_text(version, encoding="utf-8")
    except Exception:
        pass


def precompute_forward_metrics(
    *,
    source_db_path: Path,
    tf_key: str,
    table_name: str,
    forward_bars: tuple[int, int, int],
    fwd_cache_dir: Path,
    source_data_version: str | None,
    log_fn: Callable[[str, str], None] | None = None,
) -> Path:
    """
    阶段 A: 用 DuckDB WINDOW 一次性预计算指定 TF 表全量前瞻指标。

    返回 Parquet 文件路径。命中缓存时直接返回，不重算。
    """
    x, y, z = forward_bars
    parquet_path = _fwd_parquet_path(fwd_cache_dir, tf_key, x, y, z)

    # 检查磁盘缓存
    if parquet_path.exists() and source_data_version:
        cached_ver = _read_parquet_version(parquet_path)
        if cached_ver == source_data_version:
            if log_fn:
                log_fn("info", f"[阶段A] {tf_key} 前瞻缓存命中: {parquet_path.name}")
            return parquet_path

    fwd_cache_dir.mkdir(parents=True, exist_ok=True)

    if log_fn:
        log_fn("info", f"[阶段A] {tf_key} 开始 WINDOW 预计算 ({table_name}, fwd={x}/{y}/{z})")

    t0 = time.perf_counter()

    con = duckdb.connect()
    try:
        src = connect_source_readonly(source_db_path)
        try:
            # 把源库 attach 进临时连接
            con.execute(f"ATTACH '{source_db_path.as_posix()}' AS src (READ_ONLY)")
        finally:
            src.close()

        # WINDOW 预计算
        sql = f"""
        SELECT code, datetime, open, high, low, close, volume,
            close AS buy_price,
            MAX(high) OVER wx AS fwd_{x}_max_high,
            MIN(low)  OVER wx AS fwd_{x}_min_low,
            MAX(high) OVER wy AS fwd_{y}_max_high,
            MIN(low)  OVER wy AS fwd_{y}_min_low,
            MAX(high) OVER wz AS fwd_{z}_max_high,
            MIN(low)  OVER wz AS fwd_{z}_min_low,
            LIST(close) OVER wz AS fwd_close_seq,
            COUNT(*) OVER wz AS fwd_bar_count
        FROM src.{table_name}
        WINDOW
            wx AS (PARTITION BY code ORDER BY datetime ROWS BETWEEN 1 FOLLOWING AND {x} FOLLOWING),
            wy AS (PARTITION BY code ORDER BY datetime ROWS BETWEEN 1 FOLLOWING AND {y} FOLLOWING),
            wz AS (PARTITION BY code ORDER BY datetime ROWS BETWEEN 1 FOLLOWING AND {z} FOLLOWING)
        """
        con.execute(f"CREATE TABLE _bt_fwd AS ({sql})")

        # 写 Parquet
        parquet_posix = parquet_path.as_posix()
        con.execute(
            f"COPY _bt_fwd TO '{parquet_posix}' (FORMAT PARQUET)"
        )
    finally:
        con.close()

    # 版本元数据写入 sidecar 文件
    ver_str = (source_data_version or "").strip()
    if ver_str:
        _write_parquet_version(parquet_path, ver_str)

    elapsed = round(time.perf_counter() - t0, 2)
    if log_fn:
        log_fn("info", f"[阶段A] {tf_key} 预计算完成: {elapsed}s → {parquet_path.name}")

    return parquet_path


# ─────────────────────────────────────────────────────────────────────────────
# 命中去重：半个月窗口内保留中位命中
# ─────────────────────────────────────────────────────────────────────────────

def _dedup_nearby_hits(
    hits: list[dict[str, Any]],
    tf_key: str,
) -> list[dict[str, Any]]:
    """
    将同一只股票在半个月内的连续命中聚为一簇，只保留中位那条。

    聚簇规则：按 pattern_end_ts 排序后，相邻两条命中间距 ≤ gap_bars 根
    K线则归为同一簇；簇内取 index = len//2 的中位记录。
    """
    if len(hits) <= 1:
        return hits

    gap_bars = _HALF_MONTH_BARS.get(tf_key, 10)

    # 按 pattern_end_ts 排序
    sorted_hits = sorted(hits, key=lambda h: h["pattern_end_ts"])

    clusters: list[list[dict]] = [[sorted_hits[0]]]
    for h in sorted_hits[1:]:
        prev = clusters[-1][-1]
        delta = (h["pattern_end_ts"] - prev["pattern_end_ts"])
        # Timestamp 差值转为 bar 数估算（兼容 datetime.timedelta / pd.Timedelta / np.timedelta64）
        secs = pd.Timedelta(delta).total_seconds()

        if tf_key == "w":
            bar_gap = secs / (7 * 86400)
        elif tf_key == "d":
            bar_gap = secs / 86400
        elif tf_key == "60":
            bar_gap = secs / 3600
        elif tf_key == "30":
            bar_gap = secs / 1800
        elif tf_key == "15":
            bar_gap = secs / 900
        else:
            bar_gap = secs / 86400

        if bar_gap <= gap_bars:
            clusters[-1].append(h)
        else:
            clusters.append([h])

    # 每簇取中位
    deduped: list[dict] = []
    for cluster in clusters:
        mid = len(cluster) // 2
        deduped.append(cluster[mid])

    return deduped


# ─────────────────────────────────────────────────────────────────────────────
# 阶段 B: 滑动检测
# ─────────────────────────────────────────────────────────────────────────────

def _detect_hits_for_codes(
    *,
    codes: list[str],
    parquet_path: str,
    tf_key: str,
    table_name: str,
    hooks_module_path: str,
    group_params: dict[str, Any],
    section_key: str,
    slide_step: int,
    max_forward_bar: int,
    stop_event_flag: bool = False,
    progress_queue: Any = None,
) -> list[dict[str, Any]]:
    """
    Worker 函数: 对一批股票代码做滑动检测。
    在 ProcessPoolExecutor 中运行，每个 worker 独立加载 Parquet。

    返回命中记录列表 [{code, tf_key, pattern_start_ts, pattern_end_ts}]。
    """
    # 动态导入策略引擎模块
    module = importlib.import_module(hooks_module_path)
    hooks = module.BACKTEST_HOOKS

    detect_fn = hooks["detect"]
    detect_vec_fn = hooks.get("detect_vectorized")
    prepare_fn = hooks["prepare"]
    normalize_fn = hooks["normalize_params"]

    det_kwargs = normalize_fn(group_params, section_key)
    params_for_prepare = det_kwargs.get("params")

    hits: list[dict[str, Any]] = []

    # 加载本分桶的数据
    con = duckdb.connect()
    try:
        codes_str = ", ".join(f"'{c}'" for c in codes)
        df_all = con.execute(
            f"SELECT code, datetime AS ts, open, high, low, close, volume "
            f"FROM read_parquet('{parquet_path}') "
            f"WHERE code IN ({codes_str}) "
            f"ORDER BY code, ts"
        ).fetchdf()
    finally:
        con.close()

    if df_all.empty:
        return hits

    for code, code_df in df_all.groupby("code", sort=False):
        code_df = code_df.reset_index(drop=True)
        code_hits: list[dict[str, Any]] = []

        # prepare (一次全量)
        if prepare_fn and params_for_prepare:
            code_df = prepare_fn(code_df, params_for_prepare)

        if detect_vec_fn is not None:
            # ── vectorized 路径: 一次性扫描整只股票的完整历史 ──
            results: list[DetectionResult] = detect_vec_fn(code_df, **det_kwargs)
            for result in results:
                p_end_ts = result.pattern_end_ts
                p_start_ts = result.pattern_start_ts
                if p_end_ts is None and result.pattern_end_idx is not None:
                    p_end_ts = code_df["ts"].iloc[result.pattern_end_idx]
                if p_start_ts is None and result.pattern_start_idx is not None:
                    p_start_ts = code_df["ts"].iloc[result.pattern_start_idx]
                if p_end_ts is not None:
                    code_hits.append({
                        "code": code,
                        "tf_key": tf_key,
                        "pattern_start_ts": p_start_ts,
                        "pattern_end_ts": p_end_ts,
                    })
        else:
            # ── 原始滑窗路径（已弃用，仅作 fallback） ──
            logger.warning(
                "策略 %s section=%s 缺少 detect_vectorized，使用已弃用的滑窗路径",
                hooks_module_path, section_key,
            )
            n = len(code_df)
            pos = slide_step  # 从第 slide_step 个bar开始

            while pos < n:
                sub_df = code_df.iloc[: pos + 1].copy()
                result: DetectionResult = detect_fn(sub_df, **det_kwargs)

                if result.matched:
                    p_end_ts = result.pattern_end_ts
                    p_start_ts = result.pattern_start_ts

                    if p_end_ts is None and result.pattern_end_idx is not None:
                        p_end_ts = sub_df["ts"].iloc[result.pattern_end_idx]
                    if p_start_ts is None and result.pattern_start_idx is not None:
                        p_start_ts = sub_df["ts"].iloc[result.pattern_start_idx]

                    if p_end_ts is not None:
                        code_hits.append({
                            "code": code,
                            "tf_key": tf_key,
                            "pattern_start_ts": p_start_ts,
                            "pattern_end_ts": p_end_ts,
                        })

                    if result.pattern_end_idx is not None:
                        pos = result.pattern_end_idx + 1
                    else:
                        pos += slide_step
                else:
                    pos += slide_step

        # 半个月窗口去重：同一只股票的密集命中只保留中位
        hits.extend(_dedup_nearby_hits(code_hits, tf_key))

        if progress_queue is not None:
            progress_queue.put(1)

    return hits


def run_detection_parallel(
    *,
    all_codes: list[str],
    parquet_path: Path,
    tf_key: str,
    table_name: str,
    hooks_module_path: str,
    group_params: dict[str, Any],
    section_key: str,
    slide_step: int,
    max_forward_bar: int,
    max_workers: int,
    stop_event: _MpEvent | None = None,
    progress_queue: _MpQueue | None = None,
    on_pool_started: Callable[[Any], None] | None = None,
    on_pool_finished: Callable[[Any], None] | None = None,
    log_fn: Callable[[str, str], None] | None = None,
) -> list[dict[str, Any]]:
    """
    阶段 B: 将股票分桶后并行检测。

    返回全部命中记录列表。
    """
    if not all_codes:
        return []

    t0 = time.perf_counter()
    if log_fn:
        log_fn("info", f"[阶段B] {tf_key}/{section_key} 开始检测 {len(all_codes)} 只股票 (workers={max_workers})")

    # 分桶
    buckets: list[list[str]] = [[] for _ in range(max_workers)]
    for i, code in enumerate(all_codes):
        buckets[i % max_workers].append(code)
    buckets = [b for b in buckets if b]  # 去空桶

    parquet_str = parquet_path.as_posix()
    all_hits: list[dict[str, Any]] = []

    if len(buckets) == 1:
        # 单桶: 直接在主进程执行，避免 ProcessPool 开销
        all_hits = _detect_hits_for_codes(
            codes=buckets[0],
            parquet_path=parquet_str,
            tf_key=tf_key,
            table_name=table_name,
            hooks_module_path=hooks_module_path,
            group_params=group_params,
            section_key=section_key,
            slide_step=slide_step,
            max_forward_bar=max_forward_bar,
            progress_queue=progress_queue,
        )
    else:
        with ProcessPoolExecutor(max_workers=len(buckets)) as pool:
            if on_pool_started is not None:
                on_pool_started(pool)
            futures = []
            try:
                for bucket in buckets:
                    f = pool.submit(
                        _detect_hits_for_codes,
                        codes=bucket,
                        parquet_path=parquet_str,
                        tf_key=tf_key,
                        table_name=table_name,
                        hooks_module_path=hooks_module_path,
                        group_params=group_params,
                        section_key=section_key,
                        slide_step=slide_step,
                        max_forward_bar=max_forward_bar,
                        progress_queue=progress_queue,
                    )
                    futures.append(f)

                for f in futures:
                    all_hits.extend(f.result())
            finally:
                if on_pool_finished is not None:
                    on_pool_finished(pool)

    elapsed = round(time.perf_counter() - t0, 2)
    if log_fn:
        log_fn("info", f"[阶段B] {tf_key}/{section_key} 检测完成: {len(all_hits)} 命中, {elapsed}s")

    return all_hits


# ─────────────────────────────────────────────────────────────────────────────
# 阶段 C: 指标解析 (DuckDB JOIN + numpy Sharpe)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_hit_metrics(
    *,
    hits: list[dict[str, Any]],
    parquet_paths: dict[str, Path],
    forward_bars: tuple[int, int, int],
    code_to_name: dict[str, str],
    log_fn: Callable[[str, str], None] | None = None,
) -> pd.DataFrame:
    """
    阶段 C: 将命中记录与前瞻指标 JOIN，计算 profit/drawdown/sharpe。

    返回 DataFrame，每行一条命中记录，含全部指标列。
    """
    if not hits:
        return pd.DataFrame()

    x, y, z = forward_bars
    max_fwd = max(forward_bars)

    t0 = time.perf_counter()
    if log_fn:
        log_fn("info", f"[阶段C] 开始指标解析: {len(hits)} 命中记录")

    con = duckdb.connect()
    try:
        # 注册命中记录
        hits_df = pd.DataFrame(hits)
        con.register("_bt_hits", hits_df)

        # 按 tf_key 分组 JOIN
        result_parts: list[pd.DataFrame] = []

        for tf_key, pq_path in parquet_paths.items():
            tf_hits = hits_df[hits_df["tf_key"] == tf_key]
            if tf_hits.empty:
                continue

            con.register(f"_bt_hits_{tf_key}", tf_hits)
            pq_posix = pq_path.as_posix()

            sql = f"""
            SELECT
                h.code, h.tf_key, h.pattern_start_ts, h.pattern_end_ts,
                m.buy_price,
                m.fwd_{x}_max_high, m.fwd_{x}_min_low,
                m.fwd_{y}_max_high, m.fwd_{y}_min_low,
                m.fwd_{z}_max_high, m.fwd_{z}_min_low,
                m.fwd_close_seq, m.fwd_bar_count
            FROM _bt_hits_{tf_key} h
            JOIN read_parquet('{pq_posix}') m
                ON h.code = m.code AND h.pattern_end_ts = m.datetime
            WHERE m.fwd_bar_count >= {max_fwd}
            """
            part = con.execute(sql).fetchdf()
            result_parts.append(part)

        if not result_parts:
            return pd.DataFrame()

        df = pd.concat(result_parts, ignore_index=True)

        # profit / drawdown
        for n, label in [(x, "x"), (y, "y"), (z, "z")]:
            max_high_col = f"fwd_{n}_max_high"
            min_low_col = f"fwd_{n}_min_low"
            # 盈利仅衡量收益，买入后未涨过买入价的情况钳位为 0
            df[f"profit_{label}_pct"] = ((df[max_high_col] - df["buy_price"]) / df["buy_price"] * 100).clip(lower=0)
            # 回撤仅衡量风险（亏损），买入后未跌破买入价的情况钳位为 0
            df[f"drawdown_{label}_pct"] = ((df["buy_price"] - df[min_low_col]) / df["buy_price"] * 100).clip(lower=0)

    finally:
        con.close()

    # Sharpe (numpy 2D 向量化)
    df = _compute_sharpe_batch(df, forward_bars)

    # 添加 name 列
    df["name"] = df["code"].map(code_to_name).fillna("")

    # 清理中间列
    drop_cols = [
        f"fwd_{x}_max_high", f"fwd_{x}_min_low",
        f"fwd_{y}_max_high", f"fwd_{y}_min_low",
        f"fwd_{z}_max_high", f"fwd_{z}_min_low",
        "fwd_close_seq", "fwd_bar_count",
    ]
    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

    elapsed = round(time.perf_counter() - t0, 2)
    if log_fn:
        log_fn("info", f"[阶段C] 指标解析完成: {len(df)} 有效命中, {elapsed}s")

    return df


def _compute_sharpe_batch(df: pd.DataFrame, forward_bars: tuple[int, int, int]) -> pd.DataFrame:
    """用 numpy 2D 向量化批量计算 Sharpe 比率。"""
    x, y, z = forward_bars

    if df.empty or "fwd_close_seq" not in df.columns:
        for label in ("x", "y", "z"):
            df[f"sharpe_{label}"] = np.nan
        return df

    # 提取 close 序列
    close_seqs = df["fwd_close_seq"].values
    buy_prices = df["buy_price"].values

    max_z = z
    n_hits = len(df)

    # 构建 2D 数组: [buy_price, close_1, close_2, ..., close_z]
    all_prices = np.full((n_hits, max_z + 1), np.nan)
    all_prices[:, 0] = buy_prices

    for i, seq in enumerate(close_seqs):
        if seq is not None and len(seq) > 0:
            arr = np.array(seq[:max_z], dtype=np.float64)
            all_prices[i, 1: 1 + len(arr)] = arr

    # 收益率
    with np.errstate(divide="ignore", invalid="ignore"):
        returns = np.diff(all_prices, axis=1) / all_prices[:, :-1]

    # 从 df 获取 tf_key 用于年化因子
    tf_keys = df["tf_key"].values

    for n, label in [(x, "x"), (y, "y"), (z, "z")]:
        r_n = returns[:, :n]
        with np.errstate(divide="ignore", invalid="ignore"):
            mean_r = np.nanmean(r_n, axis=1)
            std_r = np.nanstd(r_n, axis=1, ddof=1)
            raw_sharpe = mean_r / std_r

        # 年化
        sharpe_col = np.full(n_hits, np.nan)
        for j in range(n_hits):
            af = _ANNUALIZE_FACTOR.get(tf_keys[j], np.sqrt(252))
            if np.isfinite(raw_sharpe[j]):
                sharpe_col[j] = raw_sharpe[j] * af

        df[f"sharpe_{label}"] = sharpe_col

    return df


# ─────────────────────────────────────────────────────────────────────────────
# 阶段 D: 输出
# ─────────────────────────────────────────────────────────────────────────────

def write_hits_csv(df: pd.DataFrame, output_dir: Path) -> Path:
    """用 DuckDB COPY TO 写 CSV (C 引擎)。返回文件路径。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "hits.csv"

    if df.empty:
        df.to_csv(csv_path, index=False)
        return csv_path

    con = duckdb.connect()
    try:
        con.register("_hits_out", df)
        con.execute(f"COPY _hits_out TO '{csv_path.as_posix()}' (FORMAT CSV, HEADER)")
    finally:
        con.close()

    return csv_path


def write_stats_json(stats: dict[str, Any], output_dir: Path) -> Path:
    """写统计结果 JSON。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "stats.json"
    json_path.write_text(json.dumps(stats, ensure_ascii=False, default=str), encoding="utf-8")
    return json_path


# ─────────────────────────────────────────────────────────────────────────────
# 辅助: 加载策略 BACKTEST_HOOKS
# ─────────────────────────────────────────────────────────────────────────────

def load_backtest_hooks(strategy_group_id: str) -> tuple[dict[str, Any], str]:
    """
    加载指定策略组的 BACKTEST_HOOKS。

    返回 (hooks_dict, module_path_str)。
    """
    groups_dir = settings.STRATEGY_GROUPS_DIR
    engine_path = groups_dir / strategy_group_id / "engine.py"

    if not engine_path.exists():
        raise ValueError(f"策略引擎不存在: {engine_path}")

    module_path = f"strategies.groups.{strategy_group_id}.engine"
    module = importlib.import_module(module_path)

    hooks = getattr(module, "BACKTEST_HOOKS", None)
    if hooks is None:
        raise ValueError(f"策略 {strategy_group_id} 未定义 BACKTEST_HOOKS")

    return hooks, module_path


def load_code_to_name(source_db_path: Path) -> dict[str, str]:
    """从源行情库加载 code → name 映射。"""
    con = connect_source_readonly(source_db_path)
    try:
        rows = con.execute("SELECT code, name FROM stocks").fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        con.close()


def load_all_codes(source_db_path: Path, tf_table: str) -> list[str]:
    """从指定 TF 表加载全部股票代码。"""
    con = connect_source_readonly(source_db_path)
    try:
        rows = con.execute(f"SELECT DISTINCT code FROM {tf_table} ORDER BY code").fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()
