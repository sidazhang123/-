"""
`burst_pullback_box_v1` 专用执行引擎（仅 d + 15m）。

设计目标：
1. 把“重计算”拆成两个阶段，先用日线 SQL 向量化压缩候选，再对候选做 15 分钟细扫。
2. 对外保持纯函数入口，便于被任务调度器调用，也便于策略开发者直接修改。
3. 在不牺牲结果正确性的前提下优先提速；可选裁剪参数默认关闭（无损优先）。

核心流程：
Phase 1（日线）:
- 在 DuckDB 一次性计算锚点日条件与前后约束，输出候选锚点。
- 该阶段支持落盘缓存（daily candidates）并做严格失效校验。

Phase 2（15m）:
- 仅对“存在候选锚点”的股票读取必要 15m 时间窗。
- 在每个锚点后 2~6 日范围内滑窗，筛出量稳+价稳+略涨窗口，生成信号。
- 支持按股票多进程并行。
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from app.settings import (
    CACHE_MAX_BYTES,
    CACHE_ROOT_DIR,
    SPECIALIZED_ENGINE_MAX_WORKERS,
    SPECIALIZED_ENGINE_VERSION,
)

from strategies.engine_commons import (
    StockScanResult,
    as_bool,
    as_dict,
    as_float,
    as_int,
)

# 向后兼容别名
BurstStockScanResult = StockScanResult

# 日线阶段默认参数：对应“放量长阳 + 前后结构约束”。
DEFAULT_DAILY_PARAMS = {
    "anchor_min_pct": 0.03,
    "anchor_amplitude_min": 0.04,
    "lookback_days": 10,
    "pre_max_volume_ratio": 0.5,
    "pre_mean_volume_ratio": 0.3,
    "pre_max_amplitude_ratio": 0.6,
    "post_days": 5,
    "post_max_gain_ratio": 2.0,
    "post_max_volume_ratio": 1.3,
}

# 15 分钟阶段默认参数：对应“2-6 日窗口内的箱体/略涨形态约束”。
DEFAULT_INTRADAY_PARAMS = {
    "start_offset_days_min": 2,
    "start_offset_days_max": 6,
    "bars_min": 5,
    "bars_max": 10,
    "volume_span_ratio_max": 0.2,
    "window_volume_mean_ratio_max": 1.0,
    "price_span_ratio_max": 0.1,
    "require_slight_up": True,
}

# 执行层参数：只影响算力和并发，不改变策略定义。
DEFAULT_EXECUTION_PARAMS = {
    "worker_count": 0,
    "max_anchor_days_per_code": 0,
}


def _normalize_daily_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化日线参数，确保所有计算字段完整且在合理范围内。"""

    raw = as_dict(group_params.get("daily"))
    return {
        "anchor_min_pct": as_float(raw.get("anchor_min_pct"), DEFAULT_DAILY_PARAMS["anchor_min_pct"], minimum=0.0),
        "anchor_amplitude_min": as_float(
            raw.get("anchor_amplitude_min"), DEFAULT_DAILY_PARAMS["anchor_amplitude_min"], minimum=0.0
        ),
        "lookback_days": as_int(raw.get("lookback_days"), DEFAULT_DAILY_PARAMS["lookback_days"], minimum=2, maximum=60),
        "pre_max_volume_ratio": as_float(
            raw.get("pre_max_volume_ratio"), DEFAULT_DAILY_PARAMS["pre_max_volume_ratio"], minimum=0.0
        ),
        "pre_mean_volume_ratio": as_float(
            raw.get("pre_mean_volume_ratio"), DEFAULT_DAILY_PARAMS["pre_mean_volume_ratio"], minimum=0.0
        ),
        "pre_max_amplitude_ratio": as_float(
            raw.get("pre_max_amplitude_ratio"), DEFAULT_DAILY_PARAMS["pre_max_amplitude_ratio"], minimum=0.0
        ),
        "post_days": as_int(raw.get("post_days"), DEFAULT_DAILY_PARAMS["post_days"], minimum=2, maximum=30),
        "post_max_gain_ratio": as_float(
            raw.get("post_max_gain_ratio"), DEFAULT_DAILY_PARAMS["post_max_gain_ratio"], minimum=0.0
        ),
        "post_max_volume_ratio": as_float(
            raw.get("post_max_volume_ratio"), DEFAULT_DAILY_PARAMS["post_max_volume_ratio"], minimum=0.0
        ),
    }


def _normalize_intraday_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化 15m 参数，并保证 bars_min <= bars_max、起始偏移区间合法。"""

    raw = as_dict(group_params.get("intraday"))
    bars_min = as_int(raw.get("bars_min"), DEFAULT_INTRADAY_PARAMS["bars_min"], minimum=2, maximum=120)
    bars_max = as_int(raw.get("bars_max"), DEFAULT_INTRADAY_PARAMS["bars_max"], minimum=bars_min, maximum=240)
    start_min = as_int(raw.get("start_offset_days_min"), DEFAULT_INTRADAY_PARAMS["start_offset_days_min"], minimum=1)
    start_max = as_int(
        raw.get("start_offset_days_max"),
        DEFAULT_INTRADAY_PARAMS["start_offset_days_max"],
        minimum=start_min,
        maximum=20,
    )
    return {
        "start_offset_days_min": start_min,
        "start_offset_days_max": start_max,
        "bars_min": bars_min,
        "bars_max": bars_max,
        "volume_span_ratio_max": as_float(
            raw.get("volume_span_ratio_max"), DEFAULT_INTRADAY_PARAMS["volume_span_ratio_max"], minimum=0.0
        ),
        "window_volume_mean_ratio_max": as_float(
            raw.get("window_volume_mean_ratio_max"),
            DEFAULT_INTRADAY_PARAMS["window_volume_mean_ratio_max"],
            minimum=0.0,
        ),
        "price_span_ratio_max": as_float(
            raw.get("price_span_ratio_max"), DEFAULT_INTRADAY_PARAMS["price_span_ratio_max"], minimum=0.0
        ),
        "require_slight_up": as_bool(raw.get("require_slight_up"), DEFAULT_INTRADAY_PARAMS["require_slight_up"]),
    }


def _normalize_execution_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化执行参数（并行度与可选裁剪）。"""

    raw = as_dict(group_params.get("execution"))
    return {
        "worker_count": as_int(raw.get("worker_count"), DEFAULT_EXECUTION_PARAMS["worker_count"], minimum=0, maximum=64),
        "max_anchor_days_per_code": as_int(
            raw.get("max_anchor_days_per_code"),
            DEFAULT_EXECUTION_PARAMS["max_anchor_days_per_code"],
            minimum=0,
            maximum=500,
        ),
    }


def _stable_hash(payload: Any) -> str:
    """生成稳定哈希，用于缓存键与参数指纹。"""

    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class DailyCandidateCache:
    """日线候选缓存：按键读写 pickle，并在写入后按 LRU 清理超额空间。"""

    def __init__(self, cache_dir: Path, max_bytes: int):
        """
        输入：
        1. cache_dir: 输入参数，具体约束以调用方和实现为准。
        2. max_bytes: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `__init__` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        self.cache_dir = cache_dir
        self.max_bytes = max_bytes
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _path_for_key(self, key: str) -> Path:
        """
        输入：
        1. key: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_path_for_key` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        return self.cache_dir / f"{key}.pkl"

    def load(self, key: str) -> Any | None:
        """
        输入：
        1. key: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `load` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        path = self._path_for_key(key)
        if not path.exists():
            return None
        try:
            frame = pd.read_pickle(path)
            os.utime(path, None)
            return frame
        except Exception:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass
            return None

    def save(self, key: str, frame: Any) -> None:
        """
        输入：
        1. key: 输入参数，具体约束以调用方和实现为准。
        2. frame: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `save` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        path = self._path_for_key(key)
        pd.to_pickle(frame, path)
        self._evict_lru_if_needed()

    def _evict_lru_if_needed(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_evict_lru_if_needed` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        files = [p for p in self.cache_dir.glob("*.pkl") if p.is_file()]
        total_bytes = sum(p.stat().st_size for p in files)
        if total_bytes <= self.max_bytes:
            return
        for path in sorted(files, key=lambda p: p.stat().st_mtime):
            try:
                size = path.stat().st_size
                path.unlink(missing_ok=True)
                total_bytes -= size
            except Exception:
                continue
            if total_bytes <= self.max_bytes:
                break


def _connect_source_readonly(source_db_path: Path) -> duckdb.DuckDBPyConnection:
    """
    以只读方式连接源库。

    若原始文件句柄冲突，则创建 shadow 副本后再连接，避免任务失败。
    """

    try:
        return duckdb.connect(str(source_db_path), read_only=True)
    except duckdb.IOException as exc:
        message = str(exc).lower()
        if "already open" not in message and "unique file handle conflict" not in message:
            raise
        shadow_path = Path(tempfile.gettempdir()) / f"{source_db_path.stem}_specialized_shadow.duckdb"
        should_copy = not shadow_path.exists()
        if not should_copy:
            try:
                should_copy = shadow_path.stat().st_mtime < source_db_path.stat().st_mtime
            except FileNotFoundError:
                should_copy = True
        if should_copy:
            shutil.copy2(source_db_path, shadow_path)
        return duckdb.connect(str(shadow_path), read_only=True)


def _build_daily_candidate_cache_key(
    *,
    source_db_path: Path,
    strategy_group_id: str,
    start_ts: datetime,
    end_ts: datetime,
    codes: list[str],
    daily_params: dict[str, Any],
    intraday_params: dict[str, Any],
) -> str:
    """构造日线候选缓存键（包含源库 mtime、参数哈希、时间窗与代码集合）。"""

    source_mtime_ns = source_db_path.stat().st_mtime_ns if source_db_path.exists() else 0
    payload = {
        "source_db_path": str(source_db_path.resolve()),
        "source_db_mtime_ns": source_mtime_ns,
        "strategy_group_id": strategy_group_id,
        "start_ts": start_ts.isoformat(),
        "end_ts": end_ts.isoformat(),
        "daily_param_hash": _stable_hash(daily_params),
        "intraday_param_hash": _stable_hash(intraday_params),
        "codes_hash": _stable_hash(sorted(codes)),
        "engine_version": SPECIALIZED_ENGINE_VERSION,
    }
    return _stable_hash(payload)


def _build_intraday_scan_cache_key(
    *,
    source_db_path: Path,
    strategy_group_id: str,
    start_ts: datetime,
    end_ts: datetime,
    code: str,
    intraday_params: dict[str, Any],
    anchors: list[dict[str, Any]],
) -> str:
    """
    构造 15m 阶段单股票缓存键。

    设计要点：
    1. 绑定源库路径与 mtime，避免读取陈旧结果；
    2. 绑定代码、时间窗、策略组、15m参数；
    3. 绑定锚点集合指纹（锚点变化即失效）。
    """

    source_mtime_ns = source_db_path.stat().st_mtime_ns if source_db_path.exists() else 0
    payload = {
        "namespace": "intraday_scan",
        "source_db_path": str(source_db_path.resolve()),
        "source_db_mtime_ns": source_mtime_ns,
        "strategy_group_id": strategy_group_id,
        "start_ts": start_ts.isoformat(),
        "end_ts": end_ts.isoformat(),
        "code": code,
        "intraday_param_hash": _stable_hash(intraday_params),
        "anchors_hash": _stable_hash(anchors),
        "engine_version": SPECIALIZED_ENGINE_VERSION,
    }
    return _stable_hash(payload)


def _query_daily_candidates(
    *,
    source_db_path: Path,
    start_ts: datetime,
    end_ts: datetime,
    codes: list[str],
    daily_params: dict[str, Any],
    intraday_params: dict[str, Any],
) -> pd.DataFrame:
    """
    Phase 1：日线候选筛选。

    SQL 一次完成锚点日、前置约束、后置约束与关键派生指标计算，返回候选锚点表。
    """

    lookback_days = daily_params["lookback_days"]
    post_days = daily_params["post_days"]
    start_offset_days_min = intraday_params["start_offset_days_min"]
    start_offset_days_max = intraday_params["start_offset_days_max"]
    padding_days = lookback_days + post_days + start_offset_days_max + 10

    scan_start = start_ts - timedelta(days=padding_days)
    scan_end = end_ts + timedelta(days=padding_days)

    code_filter = ""
    params: list[Any] = [scan_start, scan_end]
    apply_code_filter = bool(codes) and len(codes) < 4000
    if apply_code_filter:
        placeholders = ", ".join(["?"] * len(codes))
        code_filter = f" and code in ({placeholders})"
        params.extend(codes)

    # 用窗口函数一次性得到“前10日统计 + 后5日统计 + 关键 lead 值”，
    # 避免逐股逐日 Python 循环。
    sql = f"""
with base as (
    select
        code,
        datetime,
        open,
        high,
        low,
        close,
        volume
    from klines_d
    where datetime >= ?
      and datetime <= ?
      {code_filter}
),
daily0 as (
    select
        code,
        datetime as anchor_ts,
        open as anchor_open,
        high as anchor_high,
        low as anchor_low,
        close as anchor_close,
        volume as anchor_volume,
        lag(close) over (partition by code order by datetime) as prev_close
    from base
),
daily1 as (
    select
        *,
        (anchor_close - prev_close) / nullif(prev_close, 0) as day_gain,
        (anchor_high - anchor_low) / nullif(prev_close, 0) as day_amp
    from daily0
),
daily2 as (
    select
        *,
        max(anchor_volume) over (
            partition by code
            order by anchor_ts
            rows between {lookback_days} preceding and 1 preceding
        ) as pre_max_vol,
        avg(anchor_volume) over (
            partition by code
            order by anchor_ts
            rows between {lookback_days} preceding and 1 preceding
        ) as pre_avg_vol,
        max(day_amp) over (
            partition by code
            order by anchor_ts
            rows between {lookback_days} preceding and 1 preceding
        ) as pre_max_amp,
        max(anchor_high) over (
            partition by code
            order by anchor_ts
            rows between 1 following and {post_days} following
        ) as post_max_high,
        max(anchor_volume) over (
            partition by code
            order by anchor_ts
            rows between 1 following and {post_days} following
        ) as post_max_volume,
        lead(anchor_high, {post_days}) over (partition by code order by anchor_ts) as post_last_day_high,
        lead(anchor_open, 1) over (partition by code order by anchor_ts) as d1_open,
        lead(anchor_ts, {start_offset_days_min}) over (partition by code order by anchor_ts) as d2_day_ts,
        lead(anchor_ts, {start_offset_days_max}) over (partition by code order by anchor_ts) as d6_day_ts,
        lead(anchor_ts, {post_days}) over (partition by code order by anchor_ts) as d5_day_ts
    from daily1
)
select
    code,
    anchor_ts,
    anchor_open,
    anchor_high,
    anchor_low,
    anchor_close,
    anchor_volume,
    prev_close,
    day_gain,
    day_amp,
    pre_max_vol,
    pre_avg_vol,
    pre_max_amp,
    post_max_high,
    post_max_volume,
    post_last_day_high,
    d1_open,
    d2_day_ts,
    d6_day_ts,
    d5_day_ts,
    (post_max_high - d1_open) / nullif(d1_open, 0) as post5_max_gain_from_d1_open,
    (post_max_high - anchor_open) / nullif(anchor_open, 0) as post5_max_gain_from_anchor_open
from daily2
where anchor_ts >= ?
  and anchor_ts <= ?
  and day_gain > ?
  and day_amp > ?
  and pre_max_vol < anchor_volume * ?
  and pre_avg_vol < anchor_volume * ?
  and pre_max_amp < day_amp * ?
  and post_last_day_high < post_max_high
  and post5_max_gain_from_d1_open < day_gain * ?
  and post_max_volume < anchor_volume * ?
  and d1_open is not null
  and d2_day_ts is not null
  and d6_day_ts is not null
  and d5_day_ts is not null
order by code, anchor_ts
"""

    params.extend(
        [
            start_ts,
            end_ts,
            daily_params["anchor_min_pct"],
            daily_params["anchor_amplitude_min"],
            daily_params["pre_max_volume_ratio"],
            daily_params["pre_mean_volume_ratio"],
            daily_params["pre_max_amplitude_ratio"],
            daily_params["post_max_gain_ratio"],
            daily_params["post_max_volume_ratio"],
        ]
    )

    with _connect_source_readonly(source_db_path) as con:
        frame = con.execute(sql, params).fetchdf()

    if frame.empty:
        return frame

    for col in ["anchor_ts", "d2_day_ts", "d6_day_ts", "d5_day_ts"]:
        frame[col] = pd.to_datetime(frame[col], errors="coerce")

    frame = frame.dropna(subset=["anchor_ts", "d2_day_ts", "d6_day_ts", "d5_day_ts"]).copy()
    # 把“日线日期”映射到盘中可比较时间点，便于后续和 15m 时间戳对齐。
    frame["d5_close_ts"] = frame["d5_day_ts"].dt.normalize() + pd.Timedelta(hours=15)
    frame["d6_day_end_ts"] = frame["d6_day_ts"].dt.normalize() + pd.Timedelta(hours=15)
    return frame


def _count_daily_rows(
    *,
    source_db_path: Path,
    start_ts: datetime,
    end_ts: datetime,
    codes: list[str],
    daily_params: dict[str, Any],
    intraday_params: dict[str, Any],
) -> int:
    """统计本次任务日线扫描行数（用于压缩率指标）。"""

    lookback_days = daily_params["lookback_days"]
    post_days = daily_params["post_days"]
    start_offset_days_max = intraday_params["start_offset_days_max"]
    padding_days = lookback_days + post_days + start_offset_days_max + 10

    scan_start = start_ts - timedelta(days=padding_days)
    scan_end = end_ts + timedelta(days=padding_days)
    code_filter = ""
    params: list[Any] = [scan_start, scan_end]
    apply_code_filter = bool(codes) and len(codes) < 4000
    if apply_code_filter:
        placeholders = ", ".join(["?"] * len(codes))
        code_filter = f" and code in ({placeholders})"
        params.extend(codes)

    sql = f"""
    select count(*)
    from klines_d
    where datetime >= ?
      and datetime <= ?
      {code_filter}
    """
    with _connect_source_readonly(source_db_path) as con:
        count = con.execute(sql, params).fetchone()[0]
    return int(count or 0)


def _load_or_query_daily_candidates(
    *,
    source_db_path: Path,
    cache_dir: Path,
    strategy_group_id: str,
    start_ts: datetime,
    end_ts: datetime,
    codes: list[str],
    daily_params: dict[str, Any],
    intraday_params: dict[str, Any],
    cache_scope: str,
) -> tuple[pd.DataFrame, bool]:
    """按 cache_scope 读取或计算日线候选，并返回是否命中缓存。"""

    use_cache = cache_scope in {"daily_candidates", "full_pipeline"}
    cache = DailyCandidateCache(cache_dir, CACHE_MAX_BYTES) if use_cache else None

    cache_hit = False
    cache_key = ""
    if cache is not None:
        cache_key = _build_daily_candidate_cache_key(
            source_db_path=source_db_path,
            strategy_group_id=strategy_group_id,
            start_ts=start_ts,
            end_ts=end_ts,
            codes=codes,
            daily_params=daily_params,
            intraday_params=intraday_params,
        )
        cached = cache.load(cache_key)
        if cached is not None:
            cache_hit = True
            return cached, cache_hit

    frame = _query_daily_candidates(
        source_db_path=source_db_path,
        start_ts=start_ts,
        end_ts=end_ts,
        codes=codes,
        daily_params=daily_params,
        intraday_params=intraday_params,
    )
    if cache is not None and cache_key:
        cache.save(cache_key, frame)
    return frame, cache_hit


def _default_worker_count() -> int:
    """根据 CPU 自动推导 15m 扫描并行度。"""

    cpu_total = os.cpu_count() or 2
    return max(1, min(SPECIALIZED_ENGINE_MAX_WORKERS, cpu_total - 1))


def _scan_intraday_worker(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Phase 2 的 worker：处理单股票全部候选锚点。

    输入：
    - 同一股票的锚点集合
    - 15m 参数
    输出：
    - 该股票信号集合、处理条数、错误信息
    """

    code = payload["code"]
    name = payload["name"]
    source_db_path = Path(payload["source_db_path"])
    anchors = payload["anchors"]
    intraday_params = payload["intraday_params"]
    strategy_group_id = payload["strategy_group_id"]
    strategy_name = payload["strategy_name"]

    def finite_or_none(value: Any) -> float | None:
        """
        输入：
        1. value: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `finite_or_none` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        try:
            parsed = float(value)
        except Exception:
            return None
        if not math.isfinite(parsed):
            return None
        return parsed

    if not anchors:
        return {
            "code": code,
            "name": name,
            "processed_bars": 0,
            "signal_count": 0,
            "signals": [],
            "error_message": None,
        }

    bars_min = intraday_params["bars_min"]
    bars_max = intraday_params["bars_max"]
    volume_span_ratio_max = intraday_params["volume_span_ratio_max"]
    window_volume_mean_ratio_max = intraday_params["window_volume_mean_ratio_max"]
    price_span_ratio_max = intraday_params["price_span_ratio_max"]
    require_slight_up = intraday_params["require_slight_up"]

    try:
        min_start = min(anchor["d2_day_ts"] for anchor in anchors)
        max_end = max(anchor["d6_day_end_ts"] for anchor in anchors) + timedelta(days=2)
        with _connect_source_readonly(source_db_path) as con:
            rows = con.execute(
                """
                select datetime, open, high, low, close, volume
                from klines_15
                where code = ?
                  and datetime >= ?
                  and datetime <= ?
                order by datetime asc
                """,
                [code, min_start, max_end],
            ).fetchall()

        if not rows:
            return {
                "code": code,
                "name": name,
                "processed_bars": 0,
                "signal_count": 0,
                "signals": [],
                "error_message": None,
            }

        frame = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close", "volume"])
        frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
        frame = frame.dropna(subset=["datetime"]).reset_index(drop=True)
        if frame.empty:
            return {
                "code": code,
                "name": name,
                "processed_bars": 0,
                "signal_count": 0,
                "signals": [],
                "error_message": None,
            }

        dt_ns = frame["datetime"].to_numpy(dtype="datetime64[ns]")
        dt_py = np.array([ts.to_pydatetime() for ts in frame["datetime"]], dtype=object)
        highs = frame["high"].to_numpy(dtype=float)
        lows = frame["low"].to_numpy(dtype=float)
        volumes = frame["volume"].to_numpy(dtype=float)
        vol_cumsum = np.cumsum(volumes)

        def avg_volume(left: int, right: int) -> float:
            """
            输入：
            1. left: 输入参数，具体约束以调用方和实现为准。
            2. right: 输入参数，具体约束以调用方和实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `avg_volume` 对应的业务或工具逻辑。
            边界条件：
            1. 关键边界与异常分支按函数体内判断与调用约定处理。
            """
            total = vol_cumsum[right] - (vol_cumsum[left - 1] if left > 0 else 0.0)
            return float(total / (right - left + 1))

        all_signals: list[dict[str, Any]] = []

        for anchor in anchors:
            anchor_ts = anchor["anchor_ts"]
            anchor_open = finite_or_none(anchor["anchor_open"])
            anchor_volume = finite_or_none(anchor["anchor_volume"])
            prev_close = finite_or_none(anchor.get("prev_close"))
            anchor_gain = finite_or_none(anchor["day_gain"])
            anchor_amplitude = finite_or_none(anchor["day_amp"])
            pre_max_vol = finite_or_none(anchor.get("pre_max_vol"))
            pre_avg_vol = finite_or_none(anchor.get("pre_avg_vol"))
            pre_max_amp = finite_or_none(anchor.get("pre_max_amp"))
            post5_max_gain_from_open = finite_or_none(anchor["post5_max_gain_from_anchor_open"])
            d2_day_ts = anchor["d2_day_ts"]
            d6_day_end_ts = anchor["d6_day_end_ts"]
            d5_close_ts = anchor["d5_close_ts"]

            daily_thresholds = anchor.get("daily_thresholds") if isinstance(anchor, dict) else {}
            anchor_min_pct = finite_or_none((daily_thresholds or {}).get("anchor_min_pct"))
            anchor_amplitude_min = finite_or_none((daily_thresholds or {}).get("anchor_amplitude_min"))
            pre_max_volume_ratio = finite_or_none((daily_thresholds or {}).get("pre_max_volume_ratio"))
            pre_mean_volume_ratio = finite_or_none((daily_thresholds or {}).get("pre_mean_volume_ratio"))
            pre_max_amplitude_ratio = finite_or_none((daily_thresholds or {}).get("pre_max_amplitude_ratio"))

            if anchor_open is None or anchor_open <= 0:
                continue
            if anchor_volume is None or anchor_volume <= 0:
                continue
            if anchor_gain is None or anchor_amplitude is None:
                continue
            if post5_max_gain_from_open is None or post5_max_gain_from_open <= 0:
                continue

            start_left = int(np.searchsorted(dt_ns, np.datetime64(d2_day_ts), side="left"))
            start_right = int(np.searchsorted(dt_ns, np.datetime64(d6_day_end_ts), side="right")) - 1
            if start_left >= len(frame) or start_right < start_left:
                continue

            # baseline 采用“该锚点 d+2 起到窗口终点”的累计均量。
            baseline_start_idx = start_left
            windows: list[dict[str, Any]] = []
            for start_idx in range(start_left, min(start_right + 1, len(frame))):
                low_first = lows[start_idx]
                for bars in range(bars_min, bars_max + 1):
                    end_idx = start_idx + bars - 1
                    if end_idx >= len(frame):
                        break
                    vol_slice = volumes[start_idx : end_idx + 1]
                    vol_min = float(np.min(vol_slice))
                    vol_max = float(np.max(vol_slice))
                    if vol_min <= 0:
                        continue
                    vol_stability = (vol_max - vol_min) / vol_min
                    if vol_stability >= volume_span_ratio_max:
                        continue

                    baseline_avg = avg_volume(baseline_start_idx, end_idx)
                    if baseline_avg <= 0:
                        continue
                    window_avg = float(np.mean(vol_slice))
                    # 窗口均量必须低于 baseline 均量（乘以可配系数）。
                    if window_avg >= baseline_avg * window_volume_mean_ratio_max:
                        continue

                    high_max = float(np.max(highs[start_idx : end_idx + 1]))
                    low_min = float(np.min(lows[start_idx : end_idx + 1]))
                    span_ratio = (high_max - low_min) / anchor_open
                    span_cap = post5_max_gain_from_open * price_span_ratio_max
                    # 价格波动上限：相对锚点开盘归一化后，不能超过 post5 涨幅的给定比例。
                    if span_cap <= 0 or span_ratio >= span_cap:
                        continue

                    if require_slight_up:
                        low_last = lows[end_idx]
                        if not (low_last > low_first):
                            continue

                    window_end_ts = dt_py[end_idx]
                    trigger_ts = window_end_ts if window_end_ts >= d5_close_ts else d5_close_ts

                    windows.append(
                        {
                            "trigger_ts": trigger_ts,
                            "window_start_ts": dt_py[start_idx],
                            "window_end_ts": window_end_ts,
                            "bars": bars,
                            "vol_stability": vol_stability,
                            "window_avg_volume": window_avg,
                            "baseline_avg_volume": baseline_avg,
                            "span_ratio": span_ratio,
                            "span_cap": span_cap,
                        }
                    )

            if not windows:
                continue

            # 同锚点多窗口时：按 vol_stability 最小选最佳；并列最佳全部保留。
            best = min(windows, key=lambda item: item["vol_stability"])
            best_score = float(best["vol_stability"])
            selected = [item for item in windows if abs(float(item["vol_stability"]) - best_score) <= 1e-12]

            for item in selected:
                pre_max_volume_threshold = (
                    anchor_volume * pre_max_volume_ratio
                    if pre_max_volume_ratio is not None
                    else None
                )
                pre_mean_volume_threshold = (
                    anchor_volume * pre_mean_volume_ratio
                    if pre_mean_volume_ratio is not None
                    else None
                )
                pre_max_amplitude_threshold = (
                    anchor_amplitude * pre_max_amplitude_ratio
                    if pre_max_amplitude_ratio is not None
                    else None
                )

                daily_audit = {
                    "anchor_gain": {
                        "value": anchor_gain,
                        "threshold": anchor_min_pct,
                        "pass": (anchor_min_pct is not None and anchor_gain is not None and anchor_gain > anchor_min_pct),
                    },
                    "anchor_amplitude": {
                        "value": anchor_amplitude,
                        "threshold": anchor_amplitude_min,
                        "pass": (
                            anchor_amplitude_min is not None
                            and anchor_amplitude is not None
                            and anchor_amplitude > anchor_amplitude_min
                        ),
                    },
                    "pre_max_volume": {
                        "value": pre_max_vol,
                        "threshold": pre_max_volume_threshold,
                        "pass": (
                            pre_max_vol is not None
                            and pre_max_volume_threshold is not None
                            and pre_max_vol < pre_max_volume_threshold
                        ),
                    },
                    "pre_mean_volume": {
                        "value": pre_avg_vol,
                        "threshold": pre_mean_volume_threshold,
                        "pass": (
                            pre_avg_vol is not None
                            and pre_mean_volume_threshold is not None
                            and pre_avg_vol < pre_mean_volume_threshold
                        ),
                    },
                    "pre_max_amplitude": {
                        "value": pre_max_amp,
                        "threshold": pre_max_amplitude_threshold,
                        "pass": (
                            pre_max_amp is not None
                            and pre_max_amplitude_threshold is not None
                            and pre_max_amp < pre_max_amplitude_threshold
                        ),
                    },
                }
                daily_audit["all_passed"] = all(item.get("pass") is True for item in daily_audit.values() if isinstance(item, dict))

                payload_data = {
                    "anchor_day_ts": anchor_ts,
                    "chart_interval_start_ts": anchor_ts,
                    "chart_interval_end_ts": item["window_end_ts"],
                    "daily_metrics": {
                        "anchor_open": anchor_open,
                        "anchor_volume": anchor_volume,
                        "prev_close": prev_close,
                        "anchor_gain": anchor_gain,
                        "anchor_amplitude": anchor_amplitude,
                        "pre_max_volume": pre_max_vol,
                        "pre_mean_volume": pre_avg_vol,
                        "pre_max_amplitude": pre_max_amp,
                        "post5_max_gain_from_anchor_open": post5_max_gain_from_open,
                    },
                    "daily_thresholds": daily_thresholds,
                    "daily_audit": daily_audit,
                    "window": {
                        "start_ts": item["window_start_ts"],
                        "end_ts": item["window_end_ts"],
                        "bars": item["bars"],
                        "vol_stability": item["vol_stability"],
                        "window_avg_volume": item["window_avg_volume"],
                        "baseline_avg_volume": item["baseline_avg_volume"],
                        "price_span_ratio": item["span_ratio"],
                        "price_span_cap_ratio": item["span_cap"],
                    },
                    "timing": {
                        "d5_close_ts": d5_close_ts,
                        "trigger_ts": item["trigger_ts"],
                    },
                }
                all_signals.append(
                    {
                        "code": code,
                        "name": name,
                        "signal_dt": item["trigger_ts"],
                        "clock_tf": "15",
                        "strategy_group_id": strategy_group_id,
                        "strategy_name": strategy_name,
                        "signal_label": strategy_name,
                        "payload": payload_data,
                    }
                )

        return {
            "code": code,
            "name": name,
            "processed_bars": int(len(frame)),
            "signal_count": int(len(all_signals)),
            "signals": all_signals,
            "error_message": None,
        }
    except Exception as exc:
        return {
            "code": code,
            "name": name,
            "processed_bars": 0,
            "signal_count": 0,
            "signals": [],
            "error_message": f"{type(exc).__name__}: {exc}",
        }


def run_burst_pullback_box_specialized(
    *,
    source_db_path: Path,
    start_ts: datetime | None,
    end_ts: datetime | None,
    codes: list[str],
    code_to_name: dict[str, str],
    group_params: dict[str, Any],
    strategy_group_id: str,
    strategy_name: str,
    cache_scope: str,
    cache_dir: Path | None = None,
) -> tuple[dict[str, BurstStockScanResult], dict[str, Any]]:
    """
    专用引擎入口。

    返回：
    - result_map: code -> BurstStockScanResult
    - metrics: 阶段耗时、候选压缩率相关指标、并行度、缓存命中等
    """

    strategy_cache_dir = (cache_dir or (CACHE_ROOT_DIR / strategy_group_id)).resolve()

    daily_params = _normalize_daily_params(group_params)
    intraday_params = _normalize_intraday_params(group_params)
    execution_params = _normalize_execution_params(group_params)
    effective_end_ts = end_ts or datetime.now()
    screening_span_days = max(
        int(daily_params["lookback_days"])
        + int(daily_params["post_days"])
        + int(intraday_params["start_offset_days_max"])
        + 20,
        60,
    )
    effective_start_ts = start_ts or (effective_end_ts - timedelta(days=screening_span_days))
    total_daily_rows = _count_daily_rows(
        source_db_path=source_db_path,
        start_ts=effective_start_ts,
        end_ts=effective_end_ts,
        codes=codes,
        daily_params=daily_params,
        intraday_params=intraday_params,
    )

    daily_phase_start = time.perf_counter()
    daily_candidates, daily_cache_hit = _load_or_query_daily_candidates(
        source_db_path=source_db_path,
        cache_dir=strategy_cache_dir,
        strategy_group_id=strategy_group_id,
        start_ts=effective_start_ts,
        end_ts=effective_end_ts,
        codes=codes,
        daily_params=daily_params,
        intraday_params=intraday_params,
        cache_scope=cache_scope,
    )
    daily_phase_sec = time.perf_counter() - daily_phase_start

    if daily_candidates.empty:
        return {}, {
            "daily_phase_sec": round(daily_phase_sec, 4),
            "intraday_phase_sec": 0.0,
            "daily_cache_hit": daily_cache_hit,
            "anchor_count": 0,
            "codes_with_anchor": 0,
            "total_daily_rows": total_daily_rows,
            "total_codes": len(codes),
            "worker_count": 0,
        }

    if codes:
        code_set = set(codes)
        daily_candidates = daily_candidates[daily_candidates["code"].isin(code_set)].reset_index(drop=True)

    max_anchor_days_per_code = execution_params["max_anchor_days_per_code"]
    if max_anchor_days_per_code > 0:
        daily_candidates = (
            daily_candidates.sort_values(["code", "day_gain"], ascending=[True, False])
            .groupby("code", as_index=False, sort=False)
            .head(max_anchor_days_per_code)
            .sort_values(["code", "anchor_ts"], ascending=[True, True])
            .reset_index(drop=True)
        )

    anchors_by_code: dict[str, list[dict[str, Any]]] = {}
    for row in daily_candidates.to_dict(orient="records"):
        code = str(row["code"])
        anchors_by_code.setdefault(code, []).append(
            {
                "anchor_ts": pd.Timestamp(row["anchor_ts"]).to_pydatetime(),
                "anchor_open": float(row["anchor_open"]),
                "anchor_volume": float(row["anchor_volume"]),
                "prev_close": float(row.get("prev_close")) if row.get("prev_close") is not None else float("nan"),
                "day_gain": float(row["day_gain"]),
                "day_amp": float(row["day_amp"]),
                "pre_max_vol": float(row["pre_max_vol"]),
                "pre_avg_vol": float(row["pre_avg_vol"]),
                "pre_max_amp": float(row["pre_max_amp"]),
                "post5_max_gain_from_anchor_open": float(row["post5_max_gain_from_anchor_open"]),
                "d2_day_ts": pd.Timestamp(row["d2_day_ts"]).to_pydatetime(),
                "d6_day_end_ts": pd.Timestamp(row["d6_day_end_ts"]).to_pydatetime(),
                "d5_close_ts": pd.Timestamp(row["d5_close_ts"]).to_pydatetime(),
                "daily_thresholds": {
                    "anchor_min_pct": float(daily_params["anchor_min_pct"]),
                    "anchor_amplitude_min": float(daily_params["anchor_amplitude_min"]),
                    "pre_max_volume_ratio": float(daily_params["pre_max_volume_ratio"]),
                    "pre_mean_volume_ratio": float(daily_params["pre_mean_volume_ratio"]),
                    "pre_max_amplitude_ratio": float(daily_params["pre_max_amplitude_ratio"]),
                },
            }
        )

    if not anchors_by_code:
        return {}, {
            "daily_phase_sec": round(daily_phase_sec, 4),
            "intraday_phase_sec": 0.0,
            "daily_cache_hit": daily_cache_hit,
            "anchor_count": 0,
            "codes_with_anchor": 0,
            "total_daily_rows": total_daily_rows,
            "total_codes": len(codes),
            "worker_count": 0,
        }

    configured_workers = execution_params["worker_count"]
    worker_count = configured_workers if configured_workers > 0 else _default_worker_count()
    if worker_count > len(anchors_by_code):
        worker_count = len(anchors_by_code)
    worker_count = max(1, worker_count)

    intraday_phase_start = time.perf_counter()
    payloads = [
        {
            "source_db_path": str(source_db_path),
            "code": code,
            "name": code_to_name.get(code, ""),
            "anchors": anchors,
            "intraday_params": intraday_params,
            "strategy_group_id": strategy_group_id,
            "strategy_name": strategy_name,
        }
        for code, anchors in anchors_by_code.items()
    ]

    use_intraday_cache = cache_scope == "full_pipeline"
    intraday_cache = DailyCandidateCache(strategy_cache_dir, CACHE_MAX_BYTES) if use_intraday_cache else None
    payload_cache_keys: dict[str, str] = {}
    raw_results: list[dict[str, Any]] = []
    pending_payloads: list[dict[str, Any]] = []
    intraday_cache_hit_count = 0
    intraday_cache_miss_count = 0
    if intraday_cache is not None:
        for payload in payloads:
            code = str(payload["code"])
            cache_key = _build_intraday_scan_cache_key(
                source_db_path=source_db_path,
                strategy_group_id=strategy_group_id,
                start_ts=effective_start_ts,
                end_ts=effective_end_ts,
                code=code,
                intraday_params=intraday_params,
                anchors=payload["anchors"],
            )
            cached = intraday_cache.load(cache_key)
            if isinstance(cached, dict) and str(cached.get("code") or "") == code:
                raw_results.append(cached)
                intraday_cache_hit_count += 1
                continue
            payload_cache_keys[code] = cache_key
            pending_payloads.append(payload)
            intraday_cache_miss_count += 1
    else:
        pending_payloads = payloads
    active_worker_count = worker_count if pending_payloads else 0
    if active_worker_count > len(pending_payloads):
        active_worker_count = len(pending_payloads)
    intraday_parallel_fallback = False
    intraday_parallel_fallback_reason = ""
    intraday_parallel_recovered_codes = 0
    if active_worker_count > 0 and active_worker_count <= 1:
        for payload in pending_payloads:
            raw_results.append(_scan_intraday_worker(payload))
    elif active_worker_count > 0:
        try:
            with ProcessPoolExecutor(max_workers=active_worker_count) as pool:
                futures = [pool.submit(_scan_intraday_worker, payload) for payload in pending_payloads]
                for future in as_completed(futures):
                    raw_results.append(future.result())
        except BrokenProcessPool as exc:
            # Windows/环境差异下，子进程可能异常退出导致进程池损坏。
            # 这里不直接失败，而是改用当前进程串行继续，保证仍走 specialized 逻辑。
            finished_codes = {str(raw.get("code") or "") for raw in raw_results}
            remaining_payloads = [
                payload for payload in pending_payloads if str(payload.get("code") or "") not in finished_codes
            ]
            intraday_parallel_fallback = True
            intraday_parallel_fallback_reason = f"{type(exc).__name__}: {exc}"
            intraday_parallel_recovered_codes = len(remaining_payloads)
            for payload in remaining_payloads:
                raw_results.append(_scan_intraday_worker(payload))

    if intraday_cache is not None:
        for raw in raw_results:
            code = str(raw.get("code") or "")
            cache_key = payload_cache_keys.get(code)
            if not cache_key:
                continue
            if raw.get("error_message"):
                continue
            intraday_cache.save(cache_key, raw)

    intraday_phase_sec = time.perf_counter() - intraday_phase_start

    result_map: dict[str, BurstStockScanResult] = {}
    for raw in raw_results:
        code = str(raw["code"])
        result_map[code] = BurstStockScanResult(
            code=code,
            name=str(raw.get("name") or code_to_name.get(code, "")),
            processed_bars=int(raw.get("processed_bars") or 0),
            signal_count=int(raw.get("signal_count") or 0),
            signals=list(raw.get("signals") or []),
            error_message=raw.get("error_message"),
        )

    metrics = {
        "daily_phase_sec": round(daily_phase_sec, 4),
        "intraday_phase_sec": round(intraday_phase_sec, 4),
        "daily_cache_hit": daily_cache_hit,
        "anchor_count": int(len(daily_candidates)),
        "codes_with_anchor": int(len(anchors_by_code)),
        "total_daily_rows": int(total_daily_rows),
        "total_codes": int(len(codes)),
        "worker_count": int(active_worker_count),
        "intraday_parallel_fallback": intraday_parallel_fallback,
        "intraday_parallel_recovered_codes": int(intraday_parallel_recovered_codes),
        "intraday_cache_enabled": use_intraday_cache,
        "intraday_cache_hits": int(intraday_cache_hit_count),
        "intraday_cache_misses": int(intraday_cache_miss_count),
    }
    if intraday_parallel_fallback_reason:
        metrics["intraday_parallel_fallback_reason"] = intraday_parallel_fallback_reason
    return result_map, metrics
