"""
回测核心引擎。

四阶段流水线:
  A — DuckDB WINDOW 前瞻指标预计算 (两级缓存)
  B — 滑动检测 (Python, ProcessPoolExecutor 并行)
  C — 指标解析 (DuckDB JOIN + GPU/numpy Sharpe)
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

import shutil

import duckdb
import numpy as np
import pandas as pd
import psutil

from app import settings
from strategies.engine_commons import DetectionResult, connect_source_readonly, gpu_available

if TYPE_CHECKING:
    from multiprocessing import Queue as _MpQueue

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

# 每根 K 线对应的秒数: 用于向量化去重
_TF_BAR_SECONDS: dict[str, int] = {
    "w": 7 * 86400,
    "d": 86400,
    "60": 3600,
    "30": 1800,
    "15": 900,
}


# ─────────────────────────────────────────────────────────────────────────────
# SweepHitsAccumulator — 大规模 sweep 命中存储（内存 / DuckDB 双模式）
# ─────────────────────────────────────────────────────────────────────────────

_SPILL_HARD_LIMIT = 50_000_000          # 总命中数硬上限（~10GB），仅极端情况触发
_SPILL_CHECK_INTERVAL = 500             # 每 N 次 extend 做一次 psutil 检查
_SPILL_MIN_AVAILABLE_BYTES = 1536 * 1024**2  # 可用内存低于 1.5GB 时触发落盘
_SPILL_FLUSH_SIZE = 100_000             # 落盘模式 pending 缓冲刷盘阈值


class SweepHitsAccumulator:
    """内存/pickle 自适应命中累积器。

    - 小 sweep: 全部在内存中，行为等同 ``dict[int, list[dict]]``。
    - 大 sweep: 到达阈值后 pickle 到磁盘，后续追加缓冲后批量写盘。
    """

    def __init__(self, spill_dir: Path | None = None) -> None:
        self._mem: dict[int, list[dict]] = {}
        self._spilled = False
        self._spill_dir = spill_dir
        self._spill_dir_actual: Path | None = None

        self.unique_keys: set[tuple] = set()      # (code, tf_key, pattern_end_ts)
        self.hit_counts: dict[int, int] = {}       # combo_idx → count
        self.total_hits: int = 0

        self._extend_calls: int = 0
        self._pending: dict[int, list[dict]] = {}
        self._pending_count: int = 0
        self._chunk_idx: int = 0

    # ── 公共接口 ──────────────────────────────────────────────────────────

    @property
    def is_spilled(self) -> bool:
        return self._spilled

    @property
    def spill_path(self) -> Path | None:
        return self._spill_dir_actual

    def extend(self, combo_idx: int, hits: list[dict]) -> None:
        """追加一个 combo 的命中列表。"""
        n = len(hits)
        if n == 0:
            return

        self.hit_counts[combo_idx] = self.hit_counts.get(combo_idx, 0) + n
        self.total_hits += n

        self.unique_keys.update(
            (h["code"], h["tf_key"], h["pattern_end_ts"]) for h in hits
        )

        if self._spilled:
            self._pending.setdefault(combo_idx, []).extend(hits)
            self._pending_count += n
            if self._pending_count >= _SPILL_FLUSH_SIZE:
                self._flush_pending()
        else:
            self._mem.setdefault(combo_idx, []).extend(hits)
            self._extend_calls += 1
            if self._should_spill():
                self._do_spill()

    def ensure_combo(self, combo_idx: int) -> None:
        """确保 combo_idx 存在（用于预初始化空 combo）。"""
        if combo_idx not in self.hit_counts:
            self.hit_counts[combo_idx] = 0
        if not self._spilled and combo_idx not in self._mem:
            self._mem[combo_idx] = []

    def items(self):
        """内存模式兼容 dict.items()。"""
        if self._spilled:
            raise RuntimeError("SweepHitsAccumulator 已落盘，请使用 query_combo_batch()")
        return self._mem.items()

    def __getitem__(self, combo_idx: int):
        if self._spilled:
            raise RuntimeError("SweepHitsAccumulator 已落盘，请使用 query_combo_batch()")
        return self._mem[combo_idx]

    def keys(self):
        return self.hit_counts.keys()

    def query_combo_batch(self, combo_indices: list[int]) -> pd.DataFrame:
        """查询一批 combo 的命中记录，返回 DataFrame。"""
        cols = ["_combo_idx", "code", "tf_key", "pattern_start_ts", "pattern_end_ts"]

        if not self._spilled:
            rows = []
            for ci in combo_indices:
                for h in self._mem.get(ci, []):
                    rows.append((ci, h["code"], h["tf_key"],
                                 h["pattern_start_ts"], h["pattern_end_ts"]))
            if not rows:
                return pd.DataFrame(columns=cols)
            return pd.DataFrame(rows, columns=cols)

        self._flush_pending()
        ci_set = set(combo_indices)
        rows = []
        for chunk in self._iter_chunks():
            for ci in combo_indices:
                for h in chunk.get(ci, []):
                    rows.append((ci, h["code"], h["tf_key"],
                                 h["pattern_start_ts"], h["pattern_end_ts"]))
        if not rows:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame(rows, columns=cols)

    def load_all_to_mem(self) -> dict[int, list[dict]]:
        """将所有数据（含已落盘）加载回内存 dict。"""
        if not self._spilled:
            return dict(self._mem)
        self._flush_pending()
        result: dict[int, list[dict]] = {}
        for chunk in self._iter_chunks():
            for ci, hits in chunk.items():
                result.setdefault(ci, []).extend(hits)
        return result

    def close(self) -> None:
        """删除 spill 文件。"""
        self._mem.clear()
        self._pending.clear()
        self._pending_count = 0
        if self._spill_dir_actual is not None:
            shutil.rmtree(self._spill_dir_actual, ignore_errors=True)
            self._spill_dir_actual = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ── 内部方法 ──────────────────────────────────────────────────────────

    def _should_spill(self) -> bool:
        if self.total_hits >= _SPILL_HARD_LIMIT:
            return True
        if self._extend_calls % _SPILL_CHECK_INTERVAL == 0:
            avail = psutil.virtual_memory().available
            if avail < _SPILL_MIN_AVAILABLE_BYTES:
                return True
        return False

    def _do_spill(self) -> None:
        """将内存数据 pickle 到磁盘。"""
        import pickle as _pickle
        d = self._spill_dir or (Path(settings.BACKTEST_CACHE_DIR) / "_spill")
        d.mkdir(parents=True, exist_ok=True)
        self._spill_dir_actual = d / f"acc_{id(self)}_{time.monotonic_ns()}"
        self._spill_dir_actual.mkdir(parents=True, exist_ok=True)

        if self._mem:
            self._write_chunk(self._mem)
            self._mem.clear()
        self._spilled = True

        avail_mb = psutil.virtual_memory().available / 1024**2
        logger.info(
            "[SweepHitsAccumulator] pickle落盘: total_hits=%d, available_mb=%.0f, "
            "spill_dir=%s", self.total_hits, avail_mb, self._spill_dir_actual,
        )

    def _write_chunk(self, data: dict[int, list[dict]]) -> None:
        import pickle as _pickle
        path = self._spill_dir_actual / f"chunk_{self._chunk_idx:06d}.pkl"
        with open(path, "wb") as f:
            _pickle.dump(data, f, protocol=_pickle.HIGHEST_PROTOCOL)
        self._chunk_idx += 1

    def _flush_pending(self) -> None:
        if not self._pending:
            return
        self._write_chunk(self._pending)
        self._pending = {}
        self._pending_count = 0

    def _iter_chunks(self):
        """逐个 yield 磁盘上的 chunk dict。"""
        import pickle as _pickle
        if self._spill_dir_actual and self._spill_dir_actual.exists():
            for p in sorted(self._spill_dir_actual.glob("chunk_*.pkl")):
                with open(p, "rb") as f:
                    yield _pickle.load(f)


def cleanup_spill_dir(spill_dir: Path | None = None) -> None:
    """清理 spill 目录中的残留文件（用于启动时调用）。"""
    d = spill_dir or (Path(settings.BACKTEST_CACHE_DIR) / "_spill")
    if d.exists():
        shutil.rmtree(d, ignore_errors=True)
        logger.info("[cleanup_spill_dir] 已清理: %s", d)


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
    skip_version_check: bool = False,
    log_fn: Callable[[str, str], None] | None = None,
) -> Path:
    """
    阶段 A: 用 DuckDB WINDOW 一次性预计算指定 TF 表全量前瞻指标。

    返回 Parquet 文件路径。命中缓存时直接返回，不重算。
    skip_version_check=True 时，只要文件存在就直接使用，不校验版本。
    """
    x, y, z = forward_bars
    parquet_path = _fwd_parquet_path(fwd_cache_dir, tf_key, x, y, z)

    # 检查磁盘缓存
    if parquet_path.exists():
        if skip_version_check:
            if log_fn:
                log_fn("info", f"[阶段A] {tf_key} 锁定缓存模式，直接使用: {parquet_path.name}")
            return parquet_path
        if source_data_version:
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
# 向量化 split + dedup (GPU sweep 专用, 消除逐-hit Python dict 循环)
# ─────────────────────────────────────────────────────────────────────────────

def _vectorized_split_dedup(
    matched_cpu: np.ndarray,
    boundaries: np.ndarray,
    ts_flat: np.ndarray,
    valid_codes: list[str],
    tf_key: str,
    pattern_span_fn,
    detect_batch_fn,
) -> list[dict[str, Any]]:
    """向量化 split_batch_hits + _dedup_nearby_hits。

    用 numpy 批量操作取代:
      - split_batch_hits 的逐 segment / 逐 hit Python dict 循环
      - _dedup_nearby_hits 的逐 stock 排序 + 聚簇
    仅对去重后的命中创建 Python dict, 典型场景减少 10-50× 对象分配。
    """
    hit_global = np.where(matched_cpu)[0]
    n_hits = hit_global.size
    if n_hits == 0:
        return []

    # ── 全局索引 → segment / local ──
    seg_idx = np.searchsorted(boundaries[1:], hit_global, side='right')
    local_idx = hit_global - boundaries[seg_idx]

    # ── pattern span (start / end) ──
    if pattern_span_fn is not None:
        cached_offsets = getattr(detect_batch_fn, '_cached_offsets', None)
        if cached_offsets is not None:
            if hasattr(cached_offsets, 'get'):       # CuPy → numpy
                cached_offsets = cached_offsets.get()
            gp = (boundaries[seg_idx] + local_idx).astype(np.intp)
            offsets = cached_offsets[gp].astype(np.int64)
            start_local = np.maximum(0, local_idx.astype(np.int64) + offsets)
            end_local = local_idx
        else:
            start_local = np.empty(n_hits, dtype=np.intp)
            end_local = np.empty(n_hits, dtype=np.intp)
            for i in range(n_hits):
                sl, el = pattern_span_fn(int(seg_idx[i]), int(local_idx[i]))
                start_local[i] = sl
                end_local[i] = el
    else:
        start_local = local_idx
        end_local = local_idx

    seg_base = boundaries[seg_idx]
    start_ts = ts_flat[(seg_base + start_local).astype(np.intp)]
    end_ts   = ts_flat[(seg_base + end_local).astype(np.intp)]

    # ── 向量化 dedup: lexsort → diff → cluster → 取中位 ──
    gap_bars = _HALF_MONTH_BARS.get(tf_key, 10)
    bar_secs = _TF_BAR_SECONDS.get(tf_key, 86400)
    gap_td = np.timedelta64(int(gap_bars * bar_secs), 's')

    order = np.lexsort((end_ts.view(np.int64), seg_idx))

    if n_hits == 1:
        keep = order
    else:
        s_seg = seg_idx[order]
        s_end = end_ts[order]
        is_break = (np.diff(s_seg) != 0) | (np.diff(s_end) > gap_td)

        cluster_id = np.empty(n_hits, dtype=np.int64)
        cluster_id[0] = 0
        cluster_id[1:] = np.cumsum(is_break)

        _, first_pos, counts = np.unique(
            cluster_id, return_index=True, return_counts=True,
        )
        keep = order[first_pos + counts // 2]

    # ── 仅对 deduped hit 创建 dict (tolist 避免逐元素 numpy 标量开销) ──
    k_seg   = seg_idx[keep].tolist()
    k_start = start_ts[keep].tolist()
    k_end   = end_ts[keep].tolist()

    return [
        {"code": valid_codes[k_seg[i]], "tf_key": tf_key,
         "pattern_start_ts": k_start[i], "pattern_end_ts": k_end[i]}
        for i in range(len(keep))
    ]


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


# ─────────────────────────────────────────────────────────────────────────────
# 阶段 B-GPU: GPU 批量检测（单次 + sweep）
# ─────────────────────────────────────────────────────────────────────────────

def _load_parquet_df(parquet_path: str, codes: list[str]) -> pd.DataFrame:
    """加载 Parquet 数据（供 GPU 批量路径使用）。"""
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit = '2GB'")
        codes_str = ", ".join(f"'{c}'" for c in codes)
        return con.execute(
            f"SELECT code, datetime AS ts, open, high, low, close, volume "
            f"FROM read_parquet('{parquet_path}') "
            f"WHERE code IN ({codes_str}) "
            f"ORDER BY code, ts"
        ).fetchdf()
    finally:
        con.close()


def _gpu_memory_snapshot(cp) -> dict[str, float]:
    """返回当前 CUDA / CuPy 内存快照（MB）。"""
    free_mem, total_mem = cp.cuda.Device().mem_info
    pool = cp.get_default_memory_pool()
    pinned_pool = cp.get_default_pinned_memory_pool()
    return {
        "device_total_mb": total_mem / 1024**2,
        "device_free_mb": free_mem / 1024**2,
        "device_used_mb": (total_mem - free_mem) / 1024**2,
        "pool_used_mb": pool.used_bytes() / 1024**2,
        "pool_reserved_mb": pool.total_bytes() / 1024**2,
        "pinned_pool_free_blocks": float(pinned_pool.n_free_blocks()),
    }


def _detect_gpu_batch_single(
    *,
    all_codes: list[str],
    parquet_path: Path,
    tf_key: str,
    hooks: dict[str, Any],
    group_params: dict[str, Any],
    section_key: str,
    log_fn: Callable[[str, str], None] | None = None,
    progress_queue: _MpQueue | None = None,
) -> list[dict[str, Any]]:
    """GPU 批量检测（单次回测）：一次 H2D 处理所有股票。"""
    from strategies.engine_commons import (
        assemble_batch,
        gpu_available,
        split_batch_hits,
        to_gpu,
        to_cpu,
    )
    import cupy as cp

    detect_batch_fn = hooks["detect_batch"]
    prepare_fn = hooks.get("prepare")
    normalize_fn = hooks["normalize_params"]

    det_kwargs = normalize_fn(group_params, section_key)
    params_for_prepare = det_kwargs.get("params") or det_kwargs.get("tf_params")

    t0 = time.perf_counter()

    # 加载数据
    df_all = _load_parquet_df(parquet_path.as_posix(), all_codes)
    if df_all.empty:
        return []

    # prepare per-stock (CPU) + collect
    stock_dfs: list[pd.DataFrame] = []
    stock_codes: list[str] = []
    ts_arrays: list[np.ndarray] = []

    for code, code_df in df_all.groupby("code", sort=False):
        code_df = code_df.reset_index(drop=True)
        if prepare_fn and params_for_prepare:
            code_df = prepare_fn(code_df, params_for_prepare)
        stock_dfs.append(code_df)
        stock_codes.append(code)
        ts_arrays.append(code_df["ts"].values)

    if not stock_dfs:
        return []

    # 获取 detect_batch 需要的列名
    batch_columns = detect_batch_fn.__func__._batch_columns if hasattr(detect_batch_fn, '__func__') else getattr(detect_batch_fn, '_batch_columns', None)
    if batch_columns is None:
        # 默认 OHLCV + 所有非 ts/code 的数值列
        sample = stock_dfs[0]
        batch_columns = [c for c in sample.columns if c not in ("code", "ts") and sample[c].dtype.kind == 'f']

    columns_dict, boundaries, valid_codes = assemble_batch(stock_dfs, batch_columns, stock_codes)
    ts_flat = np.concatenate(ts_arrays) if ts_arrays else np.array([])

    if log_fn:
        total_rows = sum(len(df) for df in stock_dfs)
        log_fn("info", f"[阶段B-GPU] {tf_key} 数据组装完成: {len(valid_codes)}股, {total_rows}行 → GPU")

    # H2D
    gpu_columns = {k: cp.asarray(v) for k, v in columns_dict.items()}

    # detect
    matched_gpu = detect_batch_fn(gpu_columns, boundaries, det_kwargs)

    # D2H
    matched_cpu = cp.asnumpy(matched_gpu).astype(bool)

    # build pattern_span_fn if detect_batch provides one
    pattern_span_fn = getattr(detect_batch_fn, '_pattern_span_fn', None)

    all_hits = split_batch_hits(
        matched_cpu, boundaries, ts_flat, valid_codes, tf_key,
        pattern_span_fn=pattern_span_fn,
    )

    # 去重
    final_hits: list[dict[str, Any]] = []
    code_hits_map: dict[str, list[dict]] = {}
    for h in all_hits:
        code_hits_map.setdefault(h["code"], []).append(h)
    for code, code_hits in code_hits_map.items():
        final_hits.extend(_dedup_nearby_hits(code_hits, tf_key))

    if progress_queue is not None:
        for _ in valid_codes:
            progress_queue.put(1)

    elapsed = round(time.perf_counter() - t0, 2)
    if log_fn:
        log_fn("info", f"[阶段B-GPU] {tf_key} GPU检测完成: {len(final_hits)}命中, {elapsed}s")

    return final_hits


def _detect_gpu_batch_sweep(
    *,
    all_codes: list[str],
    parquet_path: Path,
    tf_key: str,
    hooks: dict[str, Any],
    combo_params_list: list[tuple[int, dict[str, Any]]],
    section_key: str,
    log_fn: Callable[[str, str], None] | None = None,
    progress_queue: _MpQueue | None = None,
    spill_dir: Path | None = None,
) -> SweepHitsAccumulator:
    """GPU 批量检测（sweep 模式）：所有股票一次 H2D，逐参数组合执行 kernel。"""
    import json as _json
    from strategies.engine_commons import (
        assemble_batch,
        to_cpu,
    )
    import cupy as cp

    detect_batch_fn = hooks["detect_batch"]
    prepare_fn = hooks.get("prepare")
    normalize_fn = hooks["normalize_params"]

    accumulator = SweepHitsAccumulator(spill_dir=spill_dir)
    for ci, _ in combo_params_list:
        accumulator.ensure_combo(ci)

    t0 = time.perf_counter()

    # 加载数据
    df_all = _load_parquet_df(parquet_path.as_posix(), all_codes)
    if df_all.empty:
        return accumulator

    # 预规范化所有 combo
    combo_kwargs: list[tuple[int, dict[str, Any], Any]] = []
    for combo_idx, gp in combo_params_list:
        det_kwargs = normalize_fn(gp, section_key)
        params = det_kwargs.get("params") or det_kwargs.get("tf_params")
        if params and not params.get("enabled", True):
            continue
        combo_kwargs.append((combo_idx, det_kwargs, det_kwargs.get("params")))

    if not combo_kwargs:
        return accumulator

    # 按 prepare_key 分组 combo（只按 prepare 实际依赖的参数分组）
    prepare_key_fn = hooks.get("prepare_key")
    prepare_groups: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    for combo_idx, det_kwargs, params_for_prepare in combo_kwargs:
        if prepare_fn and params_for_prepare:
            if prepare_key_fn:
                pkey = _json.dumps(prepare_key_fn(params_for_prepare), sort_keys=True, default=str)
            else:
                pkey = _json.dumps(params_for_prepare, sort_keys=True, default=str)
        else:
            pkey = "__no_prepare__"
        prepare_groups.setdefault(pkey, []).append((combo_idx, det_kwargs))

    n_combos = len(combo_kwargs)
    combo_done = 0
    _total_reported = 0.0

    prepare_batch_fn = hooks.get("prepare_batch")
    n_prep_groups = len(prepare_groups)

    if log_fn:
        mode = "GPU特征" if prepare_batch_fn else "CPU特征"
        log_fn("info", f"[阶段B-GPU-sweep] {tf_key} {len(all_codes)}股 × {n_combos}组合, "
               f"{n_prep_groups} prepare组 ({mode})")

    # ── GPU prepare: 上传原始 OHLCV 一次，每组用 GPU 计算特征 ──
    gpu_raw: dict[str, Any] | None = None
    _raw_boundaries: np.ndarray | None = None
    _raw_valid_codes: list[str] | None = None
    _raw_ts_flat: np.ndarray | None = None

    if prepare_batch_fn is not None:
        stock_dfs_r: list[pd.DataFrame] = []
        stock_codes_r: list[str] = []
        ts_arrays_r: list[np.ndarray] = []
        for code, code_df in df_all.groupby("code", sort=False):
            stock_dfs_r.append(code_df.reset_index(drop=True))
            stock_codes_r.append(code)
            ts_arrays_r.append(code_df["ts"].values)

        if stock_dfs_r:
            raw_cols = ["open", "high", "low", "close", "volume"]
            raw_dict, _raw_boundaries, _raw_valid_codes = assemble_batch(
                stock_dfs_r, raw_cols, stock_codes_r,
            )
            _raw_ts_flat = np.concatenate(ts_arrays_r)
            gpu_raw = {k: cp.asarray(v) for k, v in raw_dict.items()}
            del raw_dict, stock_dfs_r, ts_arrays_r

        # GPU 路径已将原始数据上传显存，释放 CPU 端 df_all
        if gpu_raw is not None:
            del df_all

        if prepare_batch_fn is not None and gpu_raw is not None and log_fn:
            vram_mb = sum(v.nbytes for v in gpu_raw.values()) / 1024**2
            mem = _gpu_memory_snapshot(cp)
            log_fn(
                "info",
                f"[阶段B-GPU-sweep] 原始OHLCV数组={vram_mb:.0f}MB, "
                f"CUDA已用={mem['device_used_mb']:.0f}MB, "
                f"CuPy池已用/保留={mem['pool_used_mb']:.0f}/{mem['pool_reserved_mb']:.0f}MB, "
                f"设备可用/总量={mem['device_free_mb']:.0f}/{mem['device_total_mb']:.0f}MB"
            )

    for prep_idx, (pkey, combos_in_group) in enumerate(prepare_groups.items(), 1):
        # 取第一个 combo 的 params_for_prepare（同组 prepare 参数相同）
        first_det_kwargs = combos_in_group[0][1]
        params_for_prepare = first_det_kwargs.get("params") or first_det_kwargs.get("tf_params")

        t_prep = time.perf_counter()

        if prepare_batch_fn is not None and gpu_raw is not None:
            # ── GPU prepare: 在 GPU 上计算特征列 ──
            gpu_columns = prepare_batch_fn(gpu_raw, _raw_boundaries, params_for_prepare)
            boundaries = _raw_boundaries
            valid_codes = _raw_valid_codes
            ts_flat = _raw_ts_flat
        else:
            # ── CPU prepare (fallback) ──
            if prepare_fn and params_for_prepare:
                multi_stock = getattr(prepare_fn, '_multi_stock', False)
                if multi_stock:
                    prepared_all = prepare_fn(df_all.copy(), params_for_prepare)
                else:
                    parts: list[pd.DataFrame] = []
                    for code, code_df in df_all.groupby("code", sort=False):
                        parts.append(prepare_fn(code_df.copy(), params_for_prepare))
                    prepared_all = pd.concat(parts, ignore_index=True)
            else:
                prepared_all = df_all

            stock_dfs: list[pd.DataFrame] = []
            stock_codes: list[str] = []
            ts_arrays: list[np.ndarray] = []
            for code, code_df in prepared_all.groupby("code", sort=False):
                stock_dfs.append(code_df.reset_index(drop=True))
                stock_codes.append(code)
                ts_arrays.append(code_df["ts"].values)

            if not stock_dfs:
                continue

            batch_columns = getattr(detect_batch_fn, '_batch_columns', None)
            if batch_columns is None:
                sample = stock_dfs[0]
                batch_columns = [c for c in sample.columns if c not in ("code", "ts") and sample[c].dtype.kind == 'f']

            columns_dict, boundaries, valid_codes = assemble_batch(stock_dfs, batch_columns, stock_codes)
            ts_flat = np.concatenate(ts_arrays) if ts_arrays else np.array([])
            gpu_columns = {k: cp.asarray(v) for k, v in columns_dict.items()}

        if log_fn:
            prep_sec = round(time.perf_counter() - t_prep, 2)
            total_rows = int(boundaries[-1]) if len(boundaries) > 1 else 0
            if prepare_batch_fn is not None and gpu_raw is not None:
                log_fn("info", f"[阶段B-GPU-sweep] prepare组 {prep_idx}/{n_prep_groups} "
                       f"GPU特征={prep_sec}s, 待执行{len(combos_in_group)}组合")
            else:
                log_fn("info", f"[阶段B-GPU-sweep] prepare组 {prep_idx}/{n_prep_groups} "
                       f"{len(valid_codes)}股 {total_rows}行, CPU特征+H2D={prep_sec}s, "
                       f"待执行{len(combos_in_group)}组合")

        pattern_span_fn = getattr(detect_batch_fn, '_pattern_span_fn', None)

        # 逐 combo 执行 kernel
        group_combo_done = 0
        n_combos_in_group = len(combos_in_group)
        t_combos = time.perf_counter()
        _t_gpu_total = 0.0
        _t_post_total = 0.0

        if log_fn:
            log_fn("info", f"[阶段B-GPU-sweep] prepare组 {prep_idx}/{n_prep_groups} "
                   f"开始执行 {n_combos_in_group} combo 循环…")

        for combo_idx, det_kwargs in combos_in_group:
            _tc0 = time.perf_counter()
            matched_gpu = detect_batch_fn(gpu_columns, boundaries, det_kwargs)
            matched_cpu = cp.asnumpy(matched_gpu).astype(bool)
            _tc1 = time.perf_counter()
            _t_gpu_total += _tc1 - _tc0

            deduped_hits = _vectorized_split_dedup(
                matched_cpu, boundaries, ts_flat, valid_codes, tf_key,
                pattern_span_fn, detect_batch_fn,
            )
            if deduped_hits:
                accumulator.extend(combo_idx, deduped_hits)

            _t_post_total += time.perf_counter() - _tc1

            combo_done += 1
            group_combo_done += 1

            if log_fn and (group_combo_done <= 3
                           or group_combo_done % 20 == 0
                           or group_combo_done == n_combos_in_group):
                _elapsed = round(time.perf_counter() - t_combos, 2)
                _gpu_avg = _t_gpu_total / group_combo_done * 1000
                _cpu_avg = _t_post_total / group_combo_done * 1000
                log_fn("info",
                       f"[阶段B-GPU-sweep] prepare组 {prep_idx}/{n_prep_groups} "
                       f"combo {group_combo_done}/{n_combos_in_group} "
                       f"({_elapsed}s, GPU均={_gpu_avg:.1f}ms CPU均={_cpu_avg:.1f}ms)")

        # ── 进度上报：组完成后一次性写入 managed queue ──
        group_equiv = len(valid_codes) * group_combo_done / n_combos
        if progress_queue is not None and group_equiv > 0:
            progress_queue.put(group_equiv)
        _total_reported += group_equiv

        combos_sec = round(time.perf_counter() - t_combos, 2)
        if log_fn:
            log_fn("info", f"[阶段B-GPU-sweep] prepare组 {prep_idx}/{n_prep_groups} "
                   f"{n_combos_in_group}组合完成 总={combos_sec}s "
                   f"(GPU+D2H={_t_gpu_total:.1f}s CPU后处理={_t_post_total:.1f}s) "
                   f"累积命中={accumulator.total_hits}")

        # 释放本组特征显存（保留 gpu_raw 原始数据）
        if prepare_batch_fn is not None and gpu_raw is not None:
            for fk in [k for k in gpu_columns if k not in gpu_raw]:
                del gpu_columns[fk]
        del gpu_columns
        cp.cuda.Stream.null.synchronize()
        cp.get_default_memory_pool().free_all_blocks()

    # 释放原始显存
    if gpu_raw is not None:
        del gpu_raw
        cp.cuda.Stream.null.synchronize()
        cp.get_default_memory_pool().free_all_blocks()

    # 最终修正 processed_count = 实际股票数
    if progress_queue is not None:
        final_equiv = len(all_codes)
        shortfall = final_equiv - _total_reported
        if shortfall > 0.5:
            progress_queue.put(shortfall)

    elapsed = round(time.perf_counter() - t0, 2)
    if log_fn:
        spill_info = f", spilled={accumulator.is_spilled}" if accumulator.is_spilled else ""
        log_fn("info", f"[阶段B-GPU-sweep] {tf_key} GPU批量检测完成: "
               f"{accumulator.total_hits}总命中, {elapsed}s{spill_info}")

    return accumulator


def _try_load_hooks_for_gpu(hooks_module_path: str) -> dict[str, Any] | None:
    """尝试加载策略的 detect_batch hook。

    如果策略提供了 detect_batch 且 GPU 可用，返回 hooks dict；否则返回 None。
    """
    from strategies.engine_commons import gpu_available
    if not gpu_available():
        return None
    try:
        module = importlib.import_module(hooks_module_path)
        hooks = getattr(module, "BACKTEST_HOOKS", None)
        if hooks and hooks.get("detect_batch") is not None:
            return hooks
    except Exception:
        pass
    return None


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
    progress_queue: _MpQueue | None = None,
    on_pool_started: Callable[[Any], None] | None = None,
    on_pool_finished: Callable[[Any], None] | None = None,
    log_fn: Callable[[str, str], None] | None = None,
    pool: ProcessPoolExecutor | None = None,
) -> list[dict[str, Any]]:
    """
    阶段 B: 将股票分桶后并行检测。

    当 pool 不为 None 时，使用调用方提供的进程池（不管理其生命周期）。
    当策略提供 detect_batch hook 且 GPU 可用时，走 GPU 批量路径（绕过进程池）。
    返回全部命中记录列表。
    """
    if not all_codes:
        return []

    # ── GPU 批量路径 ──
    _hooks = _try_load_hooks_for_gpu(hooks_module_path)
    if _hooks is not None:
        if log_fn:
            log_fn("info", f"[阶段B] {tf_key}/{section_key} → GPU批量路径 ({len(all_codes)}股)")
        return _detect_gpu_batch_single(
            all_codes=all_codes,
            parquet_path=parquet_path,
            tf_key=tf_key,
            hooks=_hooks,
            group_params=group_params,
            section_key=section_key,
            log_fn=log_fn,
            progress_queue=progress_queue,
        )

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

    _submit_kwargs = dict(
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

    if pool is not None:
        # ── 使用外部进程池（sweep 模式共享池） ──
        futures = [
            pool.submit(_detect_hits_for_codes, codes=bucket, **_submit_kwargs)
            for bucket in buckets
        ]
        for f in futures:
            all_hits.extend(f.result())
    elif len(buckets) == 1:
        # 单桶: 直接在主进程执行，避免 ProcessPool 开销
        all_hits = _detect_hits_for_codes(codes=buckets[0], **_submit_kwargs)
    else:
        with ProcessPoolExecutor(max_workers=len(buckets)) as _pool:
            if on_pool_started is not None:
                on_pool_started(_pool)
            futures = []
            try:
                for bucket in buckets:
                    f = _pool.submit(
                        _detect_hits_for_codes, codes=bucket, **_submit_kwargs,
                    )
                    futures.append(f)

                for f in futures:
                    all_hits.extend(f.result())
            finally:
                if on_pool_finished is not None:
                    on_pool_finished(_pool)

    elapsed = round(time.perf_counter() - t0, 2)
    if log_fn:
        log_fn("info", f"[阶段B] {tf_key}/{section_key} 检测完成: {len(all_hits)} 命中, {elapsed}s")

    return all_hits


# ─────────────────────────────────────────────────────────────────────────────
# 阶段 B-batch: sweep 模式批量检测
# ─────────────────────────────────────────────────────────────────────────────

def _detect_hits_for_codes_batch(
    *,
    codes: list[str],
    parquet_path: str,
    tf_key: str,
    table_name: str,
    hooks_module_path: str,
    combo_params_list: list[tuple[int, dict[str, Any]]],
    section_key: str,
    slide_step: int,
    max_forward_bar: int,
    progress_queue: Any = None,
) -> dict[int, list[dict[str, Any]]]:
    """
    Batch worker: 单次加载 Parquet，对一批股票代码执行所有参数组合的检测。

    相比 _detect_hits_for_codes，此函数接收所有参数组合并在单次数据加载内遍历，
    从而消除 sweep 模式中反复加载 Parquet 和进程池的开销。

    combo_params_list: [(combo_idx, group_params), ...]
    返回 {combo_idx: [hit_records]}。
    """
    import json as _json

    module = importlib.import_module(hooks_module_path)
    hooks = module.BACKTEST_HOOKS

    detect_fn = hooks["detect"]
    detect_vec_fn = hooks.get("detect_vectorized")
    prepare_fn = hooks["prepare"]
    normalize_fn = hooks["normalize_params"]

    # 预规范化所有组合参数（跳过 section 未启用的组合）
    combo_kwargs: list[tuple[int, dict[str, Any], Any]] = []
    for combo_idx, gp in combo_params_list:
        det_kwargs = normalize_fn(gp, section_key)
        params = det_kwargs.get("params") or det_kwargs.get("tf_params")
        if params and not params.get("enabled", True):
            continue
        combo_kwargs.append((combo_idx, det_kwargs, det_kwargs.get("params")))

    all_results: dict[int, list[dict[str, Any]]] = {ci: [] for ci, _ in combo_params_list}

    if not combo_kwargs:
        return all_results

    # 加载 Parquet（单次）
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
        return all_results

    combo_total = len(combo_kwargs)
    combo_progress_step = max(1, combo_total // 8) if combo_total > 1 else 1
    worker_id = os.getpid()

    have_prepare = prepare_fn is not None
    use_vec = detect_vec_fn is not None

    for code, code_df in df_all.groupby("code", sort=False):
        code_df = code_df.reset_index(drop=True)

        # prepare 结果缓存：参数未变时复用
        last_prepare_key: str | None = None
        last_prepared_df: pd.DataFrame | None = None

        for combo_pos, (combo_idx, det_kwargs, params_for_prepare) in enumerate(combo_kwargs, start=1):
            # 确定工作 DataFrame（含 prepare 缓存）
            if have_prepare and params_for_prepare:
                prepare_key = _json.dumps(params_for_prepare, sort_keys=True, default=str)
                if prepare_key != last_prepare_key:
                    working_df = prepare_fn(code_df.copy(), params_for_prepare)
                    last_prepare_key = prepare_key
                    last_prepared_df = working_df
                else:
                    working_df = last_prepared_df
            else:
                working_df = code_df

            code_hits: list[dict[str, Any]] = []

            if use_vec:
                results: list[DetectionResult] = detect_vec_fn(working_df, **det_kwargs)
                for r in results:
                    p_end_ts = r.pattern_end_ts
                    p_start_ts = r.pattern_start_ts
                    if p_end_ts is None and r.pattern_end_idx is not None:
                        p_end_ts = working_df["ts"].iloc[r.pattern_end_idx]
                    if p_start_ts is None and r.pattern_start_idx is not None:
                        p_start_ts = working_df["ts"].iloc[r.pattern_start_idx]
                    if p_end_ts is not None:
                        code_hits.append({
                            "code": code,
                            "tf_key": tf_key,
                            "pattern_start_ts": p_start_ts,
                            "pattern_end_ts": p_end_ts,
                        })
            else:
                n = len(working_df)
                pos = slide_step
                while pos < n:
                    sub_df = working_df.iloc[: pos + 1].copy()
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

            all_results[combo_idx].extend(_dedup_nearby_hits(code_hits, tf_key))

            if (
                progress_queue is not None
                and combo_total > 1
                and combo_pos < combo_total
                and combo_pos % combo_progress_step == 0
            ):
                progress_queue.put(
                    {
                        "type": "combo_progress",
                        "worker": worker_id,
                        "code": code,
                        "done": combo_pos,
                        "total": combo_total,
                    }
                )

        if progress_queue is not None:
            progress_queue.put(
                {
                    "type": "stock_done",
                    "worker": worker_id,
                    "code": code,
                    "done": combo_total,
                    "total": combo_total,
                }
            )

    return all_results


def run_detection_parallel_batch(
    *,
    all_codes: list[str],
    parquet_path: Path,
    tf_key: str,
    table_name: str,
    hooks_module_path: str,
    combo_params_list: list[tuple[int, dict[str, Any]]],
    section_key: str,
    slide_step: int,
    max_forward_bar: int,
    max_workers: int,
    progress_queue: _MpQueue | None = None,
    on_pool_started: Callable[[Any], None] | None = None,
    on_pool_finished: Callable[[Any], None] | None = None,
    log_fn: Callable[[str, str], None] | None = None,
    spill_dir: Path | None = None,
) -> SweepHitsAccumulator:
    """
    阶段 B-batch: sweep 模式专用批量检测。

    相比 run_detection_parallel（每次只处理一个参数组合），此函数接收所有参数组合，
    仅创建一次进程池、每个 worker 仅加载一次 Parquet，极大节省 sweep 模式的 I/O
    和进程创建开销。

    返回 SweepHitsAccumulator（内存或磁盘模式）。
    """
    if not all_codes or not combo_params_list:
        acc = SweepHitsAccumulator(spill_dir=spill_dir)
        for ci, _ in combo_params_list:
            acc.ensure_combo(ci)
        return acc

    # ── GPU 批量路径 ──
    _hooks = _try_load_hooks_for_gpu(hooks_module_path)
    if _hooks is not None:
        if log_fn:
            log_fn("info", f"[阶段B-batch] {tf_key}/{section_key} → GPU批量sweep路径 "
                   f"({len(all_codes)}股 × {len(combo_params_list)}组合)")
        return _detect_gpu_batch_sweep(
            all_codes=all_codes,
            parquet_path=parquet_path,
            tf_key=tf_key,
            hooks=_hooks,
            combo_params_list=combo_params_list,
            section_key=section_key,
            log_fn=log_fn,
            progress_queue=progress_queue,
            spill_dir=spill_dir,
        )

    n_combos = len(combo_params_list)
    t0 = time.perf_counter()
    if log_fn:
        log_fn("info", f"[阶段B-batch] {tf_key}/{section_key} "
               f"开始批量检测 {len(all_codes)} 只股票 × {n_combos} 参数组合 "
               f"(workers={max_workers})")

    # 分桶
    buckets: list[list[str]] = [[] for _ in range(max_workers)]
    for i, code in enumerate(all_codes):
        buckets[i % max_workers].append(code)
    buckets = [b for b in buckets if b]

    parquet_str = parquet_path.as_posix()
    accumulator = SweepHitsAccumulator(spill_dir=spill_dir)
    for ci, _ in combo_params_list:
        accumulator.ensure_combo(ci)

    if len(buckets) == 1:
        result = _detect_hits_for_codes_batch(
            codes=buckets[0],
            parquet_path=parquet_str,
            tf_key=tf_key,
            table_name=table_name,
            hooks_module_path=hooks_module_path,
            combo_params_list=combo_params_list,
            section_key=section_key,
            slide_step=slide_step,
            max_forward_bar=max_forward_bar,
            progress_queue=progress_queue,
        )
        for ci, hits in result.items():
            accumulator.extend(ci, hits)
    else:
        with ProcessPoolExecutor(max_workers=len(buckets)) as pool:
            if on_pool_started is not None:
                on_pool_started(pool)
            try:
                futures = []
                for bucket in buckets:
                    f = pool.submit(
                        _detect_hits_for_codes_batch,
                        codes=bucket,
                        parquet_path=parquet_str,
                        tf_key=tf_key,
                        table_name=table_name,
                        hooks_module_path=hooks_module_path,
                        combo_params_list=combo_params_list,
                        section_key=section_key,
                        slide_step=slide_step,
                        max_forward_bar=max_forward_bar,
                        progress_queue=progress_queue,
                    )
                    futures.append(f)

                for f in futures:
                    batch_result = f.result()
                    for ci, hits in batch_result.items():
                        accumulator.extend(ci, hits)
            finally:
                if on_pool_finished is not None:
                    on_pool_finished(pool)

    elapsed = round(time.perf_counter() - t0, 2)
    if log_fn:
        spill_info = f", spilled={accumulator.is_spilled}" if accumulator.is_spilled else ""
        log_fn("info", f"[阶段B-batch] {tf_key}/{section_key} "
               f"批量检测完成: {accumulator.total_hits} 总命中, {elapsed}s{spill_info}")

    return accumulator


# ─────────────────────────────────────────────────────────────────────────────
# 阶段 C: 指标解析 (DuckDB JOIN + GPU/numpy Sharpe)
# ─────────────────────────────────────────────────────────────────────────────

_JOIN_BATCH_SIZE = 50_000  # 每批 JOIN 行数，防止 DuckDB OOM


def _join_parquet_batched(
    hits_df: pd.DataFrame,
    parquet_paths: dict[str, Path],
    forward_bars: tuple[int, int, int],
    select_extra_cols: str = "",
    log_fn: Callable[[str, str], None] | None = None,
    log_tag: str = "阶段C",
) -> pd.DataFrame:
    """
    分批 DuckDB JOIN: 将命中 DataFrame 与 Parquet 前瞻数据关联。

    hits_df 必须含 code / tf_key / pattern_end_ts 列。
    select_extra_cols: 额外 SELECT 从 hits 侧取的列，如 ", h.pattern_start_ts"。
    返回 JOIN 后的完整 DataFrame。
    """
    x, y, z = forward_bars
    max_fwd = max(forward_bars)
    total = len(hits_df)

    if total == 0:
        return pd.DataFrame()

    result_parts: list[pd.DataFrame] = []
    done = 0

    for tf_key, pq_path in parquet_paths.items():
        tf_hits = hits_df[hits_df["tf_key"] == tf_key]
        if tf_hits.empty:
            continue

        pq = pq_path.as_posix()
        n_tf = len(tf_hits)

        # 分批 JOIN
        for batch_start in range(0, n_tf, _JOIN_BATCH_SIZE):
            batch_end = min(batch_start + _JOIN_BATCH_SIZE, n_tf)
            batch = tf_hits.iloc[batch_start:batch_end]

            con = duckdb.connect()
            try:
                con.execute("SET memory_limit = '2GB'")
                tbl = f"_bt_join_{tf_key}"
                con.register(tbl, batch)

                sql = f"""
                SELECT
                    h.code, h.tf_key, h.pattern_end_ts{select_extra_cols},
                    m.buy_price,
                    m.fwd_{x}_max_high, m.fwd_{x}_min_low,
                    m.fwd_{y}_max_high, m.fwd_{y}_min_low,
                    m.fwd_{z}_max_high, m.fwd_{z}_min_low,
                    m.fwd_close_seq, m.fwd_bar_count
                FROM {tbl} h
                JOIN read_parquet('{pq}') m
                    ON h.code = m.code AND h.pattern_end_ts = m.datetime
                WHERE m.fwd_bar_count >= {max_fwd}
                """
                part = con.execute(sql).fetchdf()
                result_parts.append(part)
            finally:
                con.close()

            done += len(batch)
            if log_fn and total > _JOIN_BATCH_SIZE:
                log_fn("info",
                       f"[{log_tag}] JOIN 进度: {done}/{total} "
                       f"({done * 100 // total}%)")

    if not result_parts:
        return pd.DataFrame()

    return pd.concat(result_parts, ignore_index=True)

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

    t0 = time.perf_counter()
    if log_fn:
        log_fn("info", f"[阶段C] 开始指标解析: {len(hits)} 命中记录")

    hits_df = pd.DataFrame(hits)

    # 分批 JOIN（防 DuckDB OOM + 输出进度）
    df = _join_parquet_batched(
        hits_df,
        parquet_paths,
        forward_bars,
        select_extra_cols=", h.pattern_start_ts",
        log_fn=log_fn,
        log_tag="阶段C",
    )

    if df.empty:
        return pd.DataFrame()

    # profit / drawdown
    for n, label in [(x, "x"), (y, "y"), (z, "z")]:
        max_high_col = f"fwd_{n}_max_high"
        min_low_col = f"fwd_{n}_min_low"
        df[f"profit_{label}_pct"] = ((df[max_high_col] - df["buy_price"]) / df["buy_price"] * 100).clip(lower=0)
        df[f"drawdown_{label}_pct"] = ((df["buy_price"] - df[min_low_col]) / df["buy_price"] * 100).clip(lower=0)

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


def resolve_hit_metrics_sweep(
    *,
    combo_hits_map: dict[int, list[dict[str, Any]]] | None,
    parquet_paths: dict[str, Path],
    forward_bars: tuple[int, int, int],
    code_to_name: dict[str, str],
    log_fn: Callable[[str, str], None] | None = None,
    hit_accumulator: SweepHitsAccumulator | None = None,
) -> dict[int, pd.DataFrame]:
    """
    阶段 C (sweep 批量版): 去重 JOIN + GPU/CPU 指标计算 + 按 combo 分发。

    核心优化: 不同参数组合共享同一 (code, pattern_end_ts) 命中点 →
    仅对唯一命中做一次 JOIN 与指标计算，再广播回各组合。
    显存不足时自动分批 GPU 计算，不回退 CPU。

    当提供 hit_accumulator 时，从中获取 unique_keys（跳过 Step 1 遍历）；
    落盘模式下 Step 4 使用 query_combo_batch。
    """
    x, y, z = forward_bars

    t0 = time.perf_counter()

    # ── Step 1: 提取唯一命中键 ──
    if hit_accumulator is not None:
        unique_set = hit_accumulator.unique_keys
        total_hits = hit_accumulator.total_hits
        all_combo_keys = hit_accumulator.keys()
    else:
        unique_set = set()
        total_hits = 0
        for hits in combo_hits_map.values():
            for h in hits:
                unique_set.add((h["code"], h["tf_key"], h["pattern_end_ts"]))
                total_hits += 1
        all_combo_keys = combo_hits_map.keys()

    if not unique_set:
        return {ci: pd.DataFrame() for ci in all_combo_keys}

    n_unique = len(unique_set)
    n_combos = len(list(all_combo_keys))
    if log_fn:
        dedup_pct = (1 - n_unique / max(total_hits, 1)) * 100
        mode_label = "磁盘" if (hit_accumulator and hit_accumulator.is_spilled) else "内存"
        log_fn("info",
               f"[阶段C-batch] 开始 ({mode_label}模式): {total_hits} 总命中, "
               f"{n_unique} 唯一命中, {n_combos} 组合 "
               f"(去重 {dedup_pct:.1f}%)")

    # ── Step 2: 仅对唯一命中做 DuckDB JOIN（分批防 OOM + 进度输出） ──
    unique_df = pd.DataFrame(
        [{"code": c, "tf_key": t, "pattern_end_ts": p} for c, t, p in unique_set],
    )

    metrics_df = _join_parquet_batched(
        unique_df,
        parquet_paths,
        forward_bars,
        log_fn=log_fn,
        log_tag="阶段C-batch",
    )
    del unique_df

    if metrics_df.empty:
        if log_fn:
            log_fn("info", "[阶段C-batch] JOIN 结果为空")
        return {ci: pd.DataFrame() for ci in all_combo_keys}

    if log_fn:
        log_fn("info",
               f"[阶段C-batch] JOIN 完成: {len(metrics_df)} 唯一有效命中, "
               f"{round(time.perf_counter() - t0, 2)}s")

    # ── Step 3: 在唯一命中上计算 profit/drawdown + Sharpe ──
    _use_gpu = gpu_available()
    if _use_gpu:
        try:
            metrics_df = _compute_metrics_gpu_batched(metrics_df, forward_bars, log_fn)
        except Exception as exc:
            if log_fn:
                log_fn("warning", f"[阶段C-batch] GPU 失败, 回退 CPU: {exc}")
            _use_gpu = False

    if not _use_gpu:
        buy = metrics_df["buy_price"].values
        for n, label in [(x, "x"), (y, "y"), (z, "z")]:
            mh = metrics_df[f"fwd_{n}_max_high"].values
            ml = metrics_df[f"fwd_{n}_min_low"].values
            with np.errstate(divide="ignore", invalid="ignore"):
                metrics_df[f"profit_{label}_pct"] = np.clip((mh - buy) / buy * 100, 0, None)
                metrics_df[f"drawdown_{label}_pct"] = np.clip((buy - ml) / buy * 100, 0, None)
        metrics_df = _compute_sharpe_batch(metrics_df, forward_bars)

    # name 映射
    metrics_df["name"] = metrics_df["code"].map(code_to_name).fillna("")

    # 清理中间大列
    drop_cols = [
        f"fwd_{x}_max_high", f"fwd_{x}_min_low",
        f"fwd_{y}_max_high", f"fwd_{y}_min_low",
        f"fwd_{z}_max_high", f"fwd_{z}_min_low",
        "fwd_close_seq", "fwd_bar_count",
    ]
    metrics_df.drop(
        columns=[c for c in drop_cols if c in metrics_df.columns], inplace=True,
    )

    # ── Step 4: 按 combo 分发（分批 merge 避免 RAM 爆炸） ──
    _COMBO_BATCH = 500
    merge_keys = ["code", "tf_key", "pattern_end_ts"]
    result: dict[int, pd.DataFrame] = {}
    combo_indices = list(all_combo_keys)
    use_db = hit_accumulator is not None and hit_accumulator.is_spilled

    for batch_start in range(0, len(combo_indices), _COMBO_BATCH):
        batch_cis = combo_indices[batch_start: batch_start + _COMBO_BATCH]

        if use_db:
            batch_df = hit_accumulator.query_combo_batch(batch_cis)
        else:
            # 内存模式：从 dict 或 accumulator 收集
            b_cidx: list[int] = []
            b_code: list[str] = []
            b_tf: list[str] = []
            b_ps: list[Any] = []
            b_pe: list[Any] = []
            for ci in batch_cis:
                if hit_accumulator is not None:
                    hits_list = hit_accumulator[ci]
                else:
                    hits_list = combo_hits_map[ci]
                for h in hits_list:
                    b_cidx.append(ci)
                    b_code.append(h["code"])
                    b_tf.append(h["tf_key"])
                    b_ps.append(h["pattern_start_ts"])
                    b_pe.append(h["pattern_end_ts"])

            if not b_code:
                for ci in batch_cis:
                    result[ci] = pd.DataFrame()
                continue

            batch_df = pd.DataFrame({
                "_combo_idx": b_cidx,
                "code": b_code,
                "tf_key": b_tf,
                "pattern_start_ts": b_ps,
                "pattern_end_ts": b_pe,
            })
            del b_cidx, b_code, b_tf, b_ps, b_pe

        if batch_df.empty:
            for ci in batch_cis:
                result[ci] = pd.DataFrame()
            continue

        merged = batch_df.merge(metrics_df, on=merge_keys, how="inner")
        del batch_df

        for ci_val, grp in merged.groupby("_combo_idx", sort=False):
            result[int(ci_val)] = grp.drop(columns=["_combo_idx"]).reset_index(drop=True)
        del merged

        for ci in batch_cis:
            if ci not in result:
                result[ci] = pd.DataFrame()

    elapsed = round(time.perf_counter() - t0, 2)
    if log_fn:
        total_result = sum(len(v) for v in result.values())
        log_fn("info",
               f"[阶段C-batch] 完成: {total_result} 有效命中 "
               f"(唯一 {len(metrics_df)}), {elapsed}s")

    return result


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


# ── VRAM 感知分批 GPU 指标计算 ──

_GPU_VRAM_USAGE_RATIO = 0.70  # 使用 70% 空闲显存
_GPU_MIN_BATCH = 1024


def _compute_metrics_gpu_batched(
    df: pd.DataFrame,
    forward_bars: tuple[int, int, int],
    log_fn: Callable[[str, str], None] | None = None,
) -> pd.DataFrame:
    """显存感知分批 GPU 指标计算。OOM 时自动缩减批次重试，不回退 CPU。"""
    import cupy as cp

    n_hits = len(df)
    if n_hits == 0:
        return df

    z = forward_bars[2]
    stride = z + 1

    # 估算每行峰值显存（Sharpe 2D 矩阵 + profit/drawdown 列）
    bytes_per_hit = (stride + 4) * 8 + 7 * 8
    free_bytes, _ = cp.cuda.Device().mem_info
    budget = int(free_bytes * _GPU_VRAM_USAGE_RATIO)
    batch_size = max(_GPU_MIN_BATCH, budget // max(bytes_per_hit, 1))

    if n_hits <= batch_size:
        return _compute_metrics_gpu(df, forward_bars)

    n_batches = (n_hits + batch_size - 1) // batch_size
    if log_fn:
        log_fn("info",
               f"[阶段C-GPU] 显存分批: {n_hits} 命中, "
               f"{batch_size}/批, {n_batches} 批")

    parts: list[pd.DataFrame] = []
    pos = 0
    oom_retries = 0
    max_oom_retries = 5

    while pos < n_hits:
        end = min(pos + batch_size, n_hits)
        chunk = df.iloc[pos:end].copy()
        try:
            chunk = _compute_metrics_gpu(chunk, forward_bars)
            parts.append(chunk)
            pos = end
            oom_retries = 0
            cp.cuda.Stream.null.synchronize()
            cp.get_default_memory_pool().free_all_blocks()
        except cp.cuda.memory.OutOfMemoryError:
            cp.cuda.Stream.null.synchronize()
            cp.get_default_memory_pool().free_all_blocks()
            batch_size = max(_GPU_MIN_BATCH, batch_size // 2)
            oom_retries += 1
            if oom_retries > max_oom_retries:
                raise RuntimeError(
                    f"GPU OOM: batch_size={batch_size} 仍不足, "
                    f"已处理 {pos}/{n_hits}"
                )
            if log_fn:
                log_fn("warning",
                       f"[阶段C-GPU] OOM, 缩减批次至 {batch_size}, "
                       f"重试 {oom_retries}/{max_oom_retries}")

    return pd.concat(parts, ignore_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# 阶段 C: GPU 加速 (CuPy + CUDA RawKernel)
# ─────────────────────────────────────────────────────────────────────────────

_SHARPE_KERNEL_CODE = r"""
extern "C" __global__
void sharpe_kernel(
    const double* __restrict__ prices,
    const double* __restrict__ ann_factors,
    const int N,
    const int stride,
    const int fwd_x,
    const int fwd_y,
    const int fwd_z,
    double* __restrict__ out_sx,
    double* __restrict__ out_sy,
    double* __restrict__ out_sz
) {
    int tid = blockDim.x * blockIdx.x + threadIdx.x;
    if (tid >= N) return;

    const double* row = prices + (long long)tid * stride;
    double af = ann_factors[tid];

    int windows[3] = {fwd_x, fwd_y, fwd_z};
    double* outs[3] = {out_sx, out_sy, out_sz};

    for (int w = 0; w < 3; w++) {
        int n = windows[w];
        double sum_r = 0.0, sum_r2 = 0.0;
        int valid = 0;
        for (int i = 0; i < n && i < stride - 1; i++) {
            double prev = row[i];
            double curr = row[i + 1];
            if (isnan(prev) || isnan(curr) || prev == 0.0) continue;
            double r = (curr - prev) / prev;
            sum_r += r;
            sum_r2 += r * r;
            valid++;
        }
        if (valid < 2) {
            outs[w][tid] = 0.0 / 0.0;
        } else {
            double mean_r = sum_r / (double)valid;
            double var_r = (sum_r2 - (double)valid * mean_r * mean_r) / (double)(valid - 1);
            double std_r = sqrt(var_r);
            outs[w][tid] = (std_r < 1e-15) ? (0.0 / 0.0) : ((mean_r / std_r) * af);
        }
    }
}
"""

_sharpe_kernel_cache = None


def _get_sharpe_kernel():
    """懒加载并缓存 Sharpe CUDA 内核。"""
    global _sharpe_kernel_cache
    if _sharpe_kernel_cache is None:
        import cupy as cp
        _sharpe_kernel_cache = cp.RawKernel(_SHARPE_KERNEL_CODE, "sharpe_kernel")
    return _sharpe_kernel_cache


def _compute_metrics_gpu(
    df: pd.DataFrame,
    forward_bars: tuple[int, int, int],
) -> pd.DataFrame:
    """
    GPU 加速版: profit/drawdown + Sharpe 全量计算。

    用 CuPy 向量化替代 numpy profit/drawdown，
    用 CUDA RawKernel 替代 _compute_sharpe_batch 的两个 Python for 循环。
    """
    import cupy as cp

    x, y, z = forward_bars
    n_hits = len(df)

    # ── profit / drawdown (cupy 向量化) ──
    buy_gpu = cp.asarray(df["buy_price"].values, dtype=cp.float64)
    for n, label in [(x, "x"), (y, "y"), (z, "z")]:
        max_high = cp.asarray(df[f"fwd_{n}_max_high"].values, dtype=cp.float64)
        min_low = cp.asarray(df[f"fwd_{n}_min_low"].values, dtype=cp.float64)
        df[f"profit_{label}_pct"] = cp.asnumpy(cp.clip((max_high - buy_gpu) / buy_gpu * 100, 0, None))
        df[f"drawdown_{label}_pct"] = cp.asnumpy(cp.clip((buy_gpu - min_low) / buy_gpu * 100, 0, None))
    del buy_gpu

    # ── Sharpe (CUDA kernel) ──
    if "fwd_close_seq" not in df.columns or df.empty:
        for label in ("x", "y", "z"):
            df[f"sharpe_{label}"] = np.nan
        return df

    # 构建 2D 价格矩阵: [buy_price, close_1, ..., close_z]
    # pd.DataFrame 展开 list 列（C 后端, 比 Python for 循环快一个数量级）
    raw_seqs = df["fwd_close_seq"].tolist()
    seq_df = pd.DataFrame(raw_seqs)
    ncols = seq_df.shape[1]

    stride = z + 1
    prices_2d = np.full((n_hits, stride), np.nan, dtype=np.float64)
    prices_2d[:, 0] = df["buy_price"].values
    fill_cols = min(ncols, z)
    prices_2d[:, 1:fill_cols + 1] = seq_df.iloc[:, :fill_cols].to_numpy(
        dtype=np.float64, na_value=np.nan,
    )
    del raw_seqs, seq_df

    # 年化因子（向量化 map，无 Python 循环）
    ann_factors = (
        df["tf_key"]
        .map(_ANNUALIZE_FACTOR)
        .fillna(np.sqrt(252))
        .to_numpy(dtype=np.float64)
    )

    # 上传 GPU
    d_prices = cp.asarray(prices_2d)
    d_ann = cp.asarray(ann_factors)
    del prices_2d, ann_factors

    d_sx = cp.empty(n_hits, dtype=cp.float64)
    d_sy = cp.empty(n_hits, dtype=cp.float64)
    d_sz = cp.empty(n_hits, dtype=cp.float64)

    block = 256
    grid = (n_hits + block - 1) // block

    kernel = _get_sharpe_kernel()
    kernel(
        (grid,), (block,),
        (d_prices, d_ann, np.int32(n_hits), np.int32(stride),
         np.int32(x), np.int32(y), np.int32(z),
         d_sx, d_sy, d_sz),
    )

    df["sharpe_x"] = cp.asnumpy(d_sx)
    df["sharpe_y"] = cp.asnumpy(d_sy)
    df["sharpe_z"] = cp.asnumpy(d_sz)

    del d_prices, d_ann, d_sx, d_sy, d_sz

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
