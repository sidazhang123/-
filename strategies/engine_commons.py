"""
Specialized engine 公共基础设施。

职责：
1. 提供所有 specialized 引擎共用的数据类型、参数转换工具与信号构建工厂。
2. 保证新策略在 `strategy_2/engine.py` 模板基础上即可快速接入，无需重复编写样板代码。
3. 统一 TaskManager 消费侧所需的结果结构（StockScanResult）与信号字段合同。

向后兼容约定：
- 各策略可保留原有类型别名（如 `BigbroBuyStockScanResult = StockScanResult`）。
- 工具函数签名使用 keyword-only 参数以防调用侧的位置传参歧义。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np

# ---------------------------------------------------------------------------
# 结果数据类型
# ---------------------------------------------------------------------------

@dataclass
class DetectionResult:
    """单周期检测结果（供三层架构中 detect_xxx 函数统一返回）。

    输入：由各策略的 detect_xxx 函数创建并返回。
    输出：matched 表示是否命中，metrics 携带策略特有指标。
    用途：
      1. 筛选模式：编排器根据 matched 决定是否调用 build_xxx_payload。
      2. 回测模式：回测引擎滑窗调用 detect_xxx，收集 metrics 做收益分析。
    边界条件：
      1. matched=False 时，其余字段均可为 None / 空 dict。
      2. pattern_start_idx / pattern_end_idx 为相对输入 bars 的行索引（0-based）。
    """

    matched: bool
    pattern_start_idx: int | None = None
    pattern_end_idx: int | None = None
    pattern_start_ts: datetime | None = None
    pattern_end_ts: datetime | None = None
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class StockScanResult:
    """单股票扫描结果（供 TaskManager 统一落库）。

    所有 specialized 引擎的 run_xxx_specialized() 返回值中
    result_map 的 value 必须符合此结构。
    """

    code: str
    name: str
    processed_bars: int
    signal_count: int
    signals: list[dict[str, Any]]
    error_message: str | None = None


# ---------------------------------------------------------------------------
# 图表区间标准合同
# ---------------------------------------------------------------------------

CHART_INTERVAL_PAYLOAD_CONTRACT = {
    "chart_interval_start_ts": "required",
    "chart_interval_end_ts": "required",
    "anchor_day_ts": "optional",
}


# ---------------------------------------------------------------------------
# 参数安全转换工具
# ---------------------------------------------------------------------------

def as_dict(value: Any) -> dict[str, Any]:
    """把任意值转为 dict；非 dict 时返回空对象。"""
    return value if isinstance(value, dict) else {}


def as_bool(value: Any, default: bool) -> bool:
    """安全解析布尔参数，兼容字符串开关。"""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"1", "true", "yes", "y", "on"}:
            return True
        if token in {"0", "false", "no", "n", "off"}:
            return False
    return bool(value)


def as_int(
    value: Any,
    default: int,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    """安全解析整数参数并做边界裁剪。"""
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def as_float(
    value: Any,
    default: float,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    """安全解析浮点参数并做下界/上界裁剪。"""
    try:
        parsed = float(value)
    except Exception:
        parsed = default
    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


# ---------------------------------------------------------------------------
# DuckDB 只读连接
# ---------------------------------------------------------------------------

def connect_source_readonly(source_db_path: Path) -> duckdb.DuckDBPyConnection:
    """以只读模式连接 DuckDB 数据源。"""
    return duckdb.connect(str(source_db_path), read_only=True)


# ---------------------------------------------------------------------------
# 标准信号构建工厂
# ---------------------------------------------------------------------------

def build_signal_dict(
    *,
    code: str,
    name: str,
    signal_dt: datetime,
    clock_tf: str,
    strategy_group_id: str,
    strategy_name: str,
    signal_label: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """构建符合 TaskManager 落库要求的标准信号 dict。

    必填字段由 TaskManager._run_specialized_engine 中 add_result 消费：
    code, name, signal_dt, clock_tf, strategy_group_id, strategy_name,
    signal_label, payload.
    """
    return {
        "code": code,
        "name": name,
        "signal_dt": signal_dt,
        "clock_tf": clock_tf,
        "strategy_group_id": strategy_group_id,
        "strategy_name": strategy_name,
        "signal_label": signal_label,
        "payload": payload,
    }


# ---------------------------------------------------------------------------
# 通用参数规范化工具（各引擎共用，避免重复定义）
# ---------------------------------------------------------------------------

STANDARD_TF_ORDER: list[str] = ["w", "d", "60", "30", "15"]
"""标准周期粗细排序（粗 → 细），供多周期策略选取最粗命中周期。"""


def normalize_execution_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """规范化执行参数（标准版：仅提取 backtrader 回退开关）。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回执行层参数字典，包含 fallback_to_backtrader 布尔值。
    用途：
    1. 供无额外执行参数的 specialized 策略复用，避免各引擎重复定义。
    边界条件：
    1. 含有自定义执行参数（如 worker_count）的策略应保留本地版本。
    """
    raw = as_dict(group_params.get("execution"))
    return {
        "fallback_to_backtrader": as_bool(raw.get("fallback_to_backtrader"), False),
    }


def read_universe_filter_params(group_params: dict[str, Any]) -> dict[str, Any]:
    """读取框架保留的股票池预筛选配置。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回概念过滤参数，仅用于 metrics 和调试展示。
    用途：
    1. 记录 TaskManager 预筛选后的上下文，不在 engine 内二次裁剪股票池。
    边界条件：
    1. 引擎不重复执行同一轮概念过滤。
    """
    raw_universe = as_dict(group_params.get("universe_filters"))
    raw_concepts = as_dict(raw_universe.get("concepts"))
    concept_terms = raw_concepts.get("concept_terms")
    reason_terms = raw_concepts.get("reason_terms")
    return {
        "enabled": as_bool(raw_concepts.get("enabled"), False),
        "concept_terms": [str(item).strip() for item in concept_terms] if isinstance(concept_terms, list) else [],
        "reason_terms": [str(item).strip() for item in reason_terms] if isinstance(reason_terms, list) else [],
    }


def read_filter_st_params(group_params: dict[str, Any]) -> bool:
    """读取 ST / 退 股票过滤开关。

    输入：
    1. group_params: TaskManager 合并后的策略参数。
    输出：
    1. 返回是否启用名称过滤（True 表示排除名称包含 ST 或 退 的股票）。
    用途：
    1. 由 TaskManager 在进入 engine 前调用，engine 不需要处理。
    边界条件：
    1. 缺省时默认启用（True）。
    """
    raw = as_dict(group_params.get("filter_st"))
    return as_bool(raw.get("enabled"), True)


def coarsest_tf(tfs: list[str]) -> str:
    """返回给定周期列表中最粗粒度的周期。

    输入：
    1. tfs: 周期 key 列表。
    输出：
    1. 返回 STANDARD_TF_ORDER 中排序最靠前的匹配周期。
    边界条件：
    1. 列表为空时返回 "d"（日线兜底）。
    """
    for tf in STANDARD_TF_ORDER:
        if tf in tfs:
            return tf
    return "d"


# ---------------------------------------------------------------------------
# Scope / pattern 参数分离工具
#
# 注意：以下三个函数（get_scope_param_names / extract_scope_params / strip_scope_params）
# 目前仅作为预留接口，尚无外部调用方。backtest_manager.py 和 routes.py 在处理
# scope_params 时直接从 manifest 数据读取，未使用这些工具函数。
# 若未来各策略 _normalize_for_backtest 统一迁移至此工具，可移除本注释。
# ---------------------------------------------------------------------------


def get_scope_param_names(tf_section: dict[str, Any]) -> list[str]:
    """读取周期参数段中声明的 scope_params 列表。

    未声明时返回空列表（向后兼容）。
    """
    raw = tf_section.get("scope_params")
    if isinstance(raw, list):
        return [str(n) for n in raw]
    return []


def extract_scope_params(tf_section: dict[str, Any]) -> dict[str, Any]:
    """从周期参数段中提取 scope_params 声明的参数名及其值。"""
    names = get_scope_param_names(tf_section)
    return {k: tf_section[k] for k in names if k in tf_section}


def strip_scope_params(tf_section: dict[str, Any]) -> dict[str, Any]:
    """移除周期参数段中的 scope 参数及 scope_params 列表本身，返回纯 pattern 参数副本。"""
    names = set(get_scope_param_names(tf_section))
    names.add("scope_params")
    return {k: v for k, v in tf_section.items() if k not in names}


# ---------------------------------------------------------------------------
# GPU 加速工具（CuPy / NumPy 透明切换）
# ---------------------------------------------------------------------------

_GPU_AVAILABLE: bool | None = None
_GPU_FAIL_REASON: str = ""


def gpu_available() -> bool:
    """检测 CuPy GPU 加速是否可用（懒初始化，结果缓存）。"""
    global _GPU_AVAILABLE, _GPU_FAIL_REASON
    if _GPU_AVAILABLE is None:
        try:
            import cupy as cp
            # 执行一次真实 GPU 运算验证完整 CUDA 工具链可用
            cp.arange(1)
            _GPU_AVAILABLE = True
            _GPU_FAIL_REASON = ""
        except ImportError:
            _GPU_AVAILABLE = False
            _GPU_FAIL_REASON = "CuPy not installed"
        except Exception as exc:
            _GPU_AVAILABLE = False
            _GPU_FAIL_REASON = f"CUDA failed: {exc}"
    return _GPU_AVAILABLE


def gpu_compute_status(strategy_gpu_enabled: bool) -> str:
    """返回当前 GPU 计算状态描述（三种状态）。

    1. 策略未实现 CUDA 计算（无论 GPU 环境如何）→ CPU 计算
    2. 策略已实现 GPU 加速但 CuPy 未安装或 CUDA 失败 → 回退 CPU 计算
    3. 策略已实现 GPU 加速且 CuPy + CUDA 正常 → GPU 计算
    """
    if not strategy_gpu_enabled:
        return "CPU计算（该策略未实现CUDA加速）"
    if not gpu_available():
        return f"回退CPU计算（{_GPU_FAIL_REASON}）"
    return "GPU计算（CuPy + CUDA）"


def get_array_module():
    """返回 cupy（GPU 可用时）或 numpy。"""
    if gpu_available():
        import cupy
        return cupy
    return np


def _xp_for(arr):
    """根据数组类型返回对应的数组模块（cupy 或 numpy）。"""
    if hasattr(arr, "__cuda_array_interface__"):
        import cupy
        return cupy
    return np


def to_gpu(arr: np.ndarray):
    """将 numpy 数组转移至 GPU。GPU 不可用时返回原数组。"""
    if gpu_available():
        import cupy as cp
        return cp.asarray(arr)
    return arr


def to_cpu(arr) -> np.ndarray:
    """将 GPU 数组转回 CPU numpy。已经是 numpy 时直接返回。"""
    if gpu_available():
        import cupy as cp
        if isinstance(arr, cp.ndarray):
            return cp.asnumpy(arr)
    return arr if isinstance(arr, np.ndarray) else np.asarray(arr)


def gpu_rolling_mean(arr, window: int):
    """基于 cumsum 的滚动均值（兼容 numpy 和 cupy）。

    等价于 pd.Series(arr).rolling(window, min_periods=window).mean().values
    前 window-1 个元素为 NaN。
    """
    xp = _xp_for(arr)
    n = len(arr)
    result = xp.full(n, xp.nan, dtype=arr.dtype)
    if n < window:
        return result
    cs = xp.concatenate([xp.zeros(1, dtype=arr.dtype), xp.cumsum(arr)])
    result[window - 1:] = (cs[window:] - cs[:-window]) / window
    return result


def gpu_shift(arr, periods: int):
    """数组前移（正值向右移，NaN 填充），兼容 numpy 和 cupy。

    等价于 pd.Series(arr).shift(periods).values
    """
    xp = _xp_for(arr)
    n = len(arr)
    if periods <= 0:
        return arr.copy()
    result = xp.full(n, xp.nan, dtype=arr.dtype)
    if periods >= n:
        return result
    result[periods:] = arr[:-periods]
    return result


def gpu_rolling_max(arr, window: int):
    """滚动最大值（兼容 numpy 和 cupy）。前 window-1 个元素为 NaN。"""
    xp = _xp_for(arr)
    n = len(arr)
    if n < window:
        return xp.full(n, xp.nan, dtype=arr.dtype)
    result = arr.copy()
    for k in range(1, window):
        shifted = xp.full(n, -xp.inf, dtype=arr.dtype)
        shifted[k:] = arr[:-k]
        result = xp.maximum(result, shifted)
    result[:window - 1] = xp.nan
    return result


def gpu_rolling_min(arr, window: int):
    """滚动最小值（兼容 numpy 和 cupy）。前 window-1 个元素为 NaN。"""
    xp = _xp_for(arr)
    n = len(arr)
    if n < window:
        return xp.full(n, xp.nan, dtype=arr.dtype)
    result = arr.copy()
    for k in range(1, window):
        shifted = xp.full(n, xp.inf, dtype=arr.dtype)
        shifted[k:] = arr[:-k]
        result = xp.minimum(result, shifted)
    result[:window - 1] = xp.nan
    return result


def gpu_minimum_accumulate(arr):
    """累积最小值（兼容 numpy 和 cupy）。

    等价于 np.minimum.accumulate(arr)，但 cupy 不支持 accumulate，
    此处回退到 CPU 计算后再转回 GPU。
    """
    xp = _xp_for(arr)
    if xp is np:
        return np.minimum.accumulate(arr)
    # cupy 不支持 minimum.accumulate，回退 CPU
    import cupy as cp
    cpu_arr = cp.asnumpy(arr)
    result = np.minimum.accumulate(cpu_arr)
    return cp.asarray(result)


# ---------------------------------------------------------------------------
# GPU 批量检测基础设施（segmented 原语 + batch 组装/拆分）
# ---------------------------------------------------------------------------

def _build_boundary_mask(n: int, boundaries: np.ndarray, reset_count: int, xp):
    """构建 boundary reset 掩码：每个 segment 前 reset_count 个位置为 True。

    boundaries: [0, len0, len0+len1, ...], shape=(n_segments+1,)
    """
    mask = xp.zeros(n, dtype=bool)
    for i in range(len(boundaries) - 1):
        start = int(boundaries[i])
        end = min(start + reset_count, int(boundaries[i + 1]))
        if start < end:
            mask[start:end] = True
    return mask


def gpu_segmented_rolling_mean(arr, boundaries: np.ndarray, window: int):
    """分段滚动均值：每个 segment 内独立计算，不跨段。

    arr: cupy/numpy 1D array (flat concatenation of all stocks)
    boundaries: numpy int array, shape=(n_segments+1,), e.g. [0, 500, 1200, ...]
    window: rolling window size

    返回与 arr 同设备的数组，每个 segment 前 window-1 个位置为 NaN。
    """
    xp = _xp_for(arr)
    n = len(arr)
    if n == 0:
        return xp.array([], dtype=np.float64)

    # 全局 cumsum
    cs = xp.concatenate([xp.zeros(1, dtype=arr.dtype), xp.cumsum(arr)])
    result = xp.full(n, xp.nan, dtype=arr.dtype)
    result[window - 1:] = (cs[window:] - cs[:-window]) / window

    # 修正跨段污染：每个 segment 前 window-1 个位置重置为 NaN
    # 同时修正 cumsum 跨段偏移
    for i in range(1, len(boundaries) - 1):
        seg_start = int(boundaries[i])
        seg_end = int(boundaries[i + 1]) if i + 1 < len(boundaries) else n
        seg_len = seg_end - seg_start
        if seg_len == 0:
            continue
        # 该 segment 的正确 cumsum 起始
        seg_arr = arr[seg_start:seg_end]
        seg_cs = xp.concatenate([xp.zeros(1, dtype=arr.dtype), xp.cumsum(seg_arr)])
        valid_end = min(window - 1, seg_len)
        result[seg_start:seg_start + valid_end] = xp.nan
        actual_end = min(seg_len, seg_len)
        if seg_len >= window:
            result[seg_start + window - 1:seg_end] = (
                seg_cs[window:] - seg_cs[:-window]
            ) / window

    # 第一个 segment 已经被全局 cumsum 正确处理（起始为0）
    # 只需重置前 window-1 个
    first_end = min(window - 1, int(boundaries[1]) if len(boundaries) > 1 else n)
    result[:first_end] = xp.nan

    return result


def gpu_segmented_shift(arr, boundaries: np.ndarray, periods: int):
    """分段 shift：每个 segment 内独立 shift，不跨段。前 periods 个置 NaN。"""
    xp = _xp_for(arr)
    n = len(arr)
    if periods <= 0 or n == 0:
        return arr.copy() if n > 0 else xp.array([], dtype=np.float64)

    result = xp.full(n, xp.nan, dtype=arr.dtype)
    for i in range(len(boundaries) - 1):
        s = int(boundaries[i])
        e = int(boundaries[i + 1])
        seg_len = e - s
        if seg_len <= periods:
            continue  # 全 NaN
        result[s + periods:e] = arr[s:e - periods]
    return result


def gpu_segmented_rolling_max(arr, boundaries: np.ndarray, window: int):
    """分段滚动最大值：每个 segment 内独立计算。"""
    xp = _xp_for(arr)
    n = len(arr)
    if n == 0:
        return xp.array([], dtype=np.float64)

    result = xp.full(n, xp.nan, dtype=arr.dtype)
    for i in range(len(boundaries) - 1):
        s = int(boundaries[i])
        e = int(boundaries[i + 1])
        seg = arr[s:e]
        seg_len = len(seg)
        if seg_len < window:
            continue
        seg_result = seg.copy()
        for k in range(1, window):
            shifted = xp.full(seg_len, -xp.inf, dtype=arr.dtype)
            shifted[k:] = seg[:-k]
            seg_result = xp.maximum(seg_result, shifted)
        seg_result[:window - 1] = xp.nan
        result[s:e] = seg_result
    return result


def gpu_segmented_rolling_min(arr, boundaries: np.ndarray, window: int):
    """分段滚动最小值：每个 segment 内独立计算。"""
    xp = _xp_for(arr)
    n = len(arr)
    if n == 0:
        return xp.array([], dtype=np.float64)

    result = xp.full(n, xp.nan, dtype=arr.dtype)
    for i in range(len(boundaries) - 1):
        s = int(boundaries[i])
        e = int(boundaries[i + 1])
        seg = arr[s:e]
        seg_len = len(seg)
        if seg_len < window:
            continue
        seg_result = seg.copy()
        for k in range(1, window):
            shifted = xp.full(seg_len, xp.inf, dtype=arr.dtype)
            shifted[k:] = seg[:-k]
            seg_result = xp.minimum(seg_result, shifted)
        seg_result[:window - 1] = xp.nan
        result[s:e] = seg_result
    return result


def gpu_segmented_minimum_accumulate(arr, boundaries: np.ndarray):
    """分段累积最小值：每个 segment 内独立 accumulate。"""
    xp = _xp_for(arr)
    n = len(arr)
    if n == 0:
        return xp.array([], dtype=np.float64)

    result = xp.empty(n, dtype=arr.dtype)
    for i in range(len(boundaries) - 1):
        s = int(boundaries[i])
        e = int(boundaries[i + 1])
        seg = arr[s:e]
        if len(seg) == 0:
            continue
        # cupy 不支持 ufunc.accumulate，回退 CPU
        if xp is not np:
            import cupy as cp
            cpu_seg = cp.asnumpy(seg)
            seg_min = np.minimum.accumulate(cpu_seg)
            result[s:e] = cp.asarray(seg_min)
        else:
            result[s:e] = np.minimum.accumulate(seg)
    return result


def assemble_batch(
    stock_dfs: list[pd.DataFrame],
    columns: list[str],
    stock_codes: list[str],
) -> tuple[dict[str, np.ndarray], np.ndarray, list[str]]:
    """将多只股票的 DataFrame 拼接为 flat numpy 数组 + boundary 索引。

    返回:
      columns_dict: {col_name: flat_numpy_array}
      boundaries: numpy int64 array, shape=(n_stocks+1,)
      valid_codes: 与 boundaries 对应的股票代码列表（跳过空 df）
    """
    boundaries = [0]
    valid_codes: list[str] = []
    col_arrays: dict[str, list[np.ndarray]] = {c: [] for c in columns}

    for df, code in zip(stock_dfs, stock_codes):
        if df.empty:
            continue
        valid_codes.append(code)
        for c in columns:
            col_arrays[c].append(df[c].values.astype(np.float64))
        boundaries.append(boundaries[-1] + len(df))

    if not valid_codes:
        return {c: np.array([], dtype=np.float64) for c in columns}, np.array([0], dtype=np.int64), []

    columns_dict = {c: np.concatenate(col_arrays[c]) for c in columns}
    return columns_dict, np.array(boundaries, dtype=np.int64), valid_codes


def split_batch_hits(
    matched_mask: np.ndarray,
    boundaries: np.ndarray,
    ts_flat: np.ndarray,
    valid_codes: list[str],
    tf_key: str,
    *,
    pattern_span_fn=None,
) -> list[dict[str, Any]]:
    """将 flat boolean mask 按 boundary 拆分为 per-stock hit records。

    matched_mask: boolean array, shape=(total_rows,)
    boundaries: int array, shape=(n_stocks+1,)
    ts_flat: flat timestamp array
    valid_codes: code per segment
    pattern_span_fn: 可选回调 (code_idx, hit_idx_in_segment) → (start_idx, end_idx)
                     默认 start=end=hit_idx

    返回 [{code, tf_key, pattern_start_ts, pattern_end_ts}, ...]
    """
    hits: list[dict[str, Any]] = []
    for seg_idx in range(len(valid_codes)):
        s = int(boundaries[seg_idx])
        e = int(boundaries[seg_idx + 1])
        seg_mask = matched_mask[s:e]
        seg_hits = np.where(seg_mask)[0]
        code = valid_codes[seg_idx]
        for local_idx in seg_hits:
            local_idx = int(local_idx)
            if pattern_span_fn is not None:
                start_local, end_local = pattern_span_fn(seg_idx, local_idx)
            else:
                start_local = local_idx
                end_local = local_idx
            hits.append({
                "code": code,
                "tf_key": tf_key,
                "pattern_start_ts": ts_flat[s + start_local],
                "pattern_end_ts": ts_flat[s + end_local],
            })
    return hits
