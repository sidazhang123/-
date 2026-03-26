"""
全局配置加载模块。

职责：
1. 从 `app/app_config.yaml` 读取可配置参数，并与内置默认值合并。
2. 对配置项做类型与边界校验，启动阶段尽早暴露配置错误。
3. 对外导出统一配置常量，作为应用唯一配置入口。
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent
CONFIG_YAML_PATH = APP_DIR / "app_config.yaml"

_DEFAULT_CONFIG: dict[str, Any] = {
    "paths": {
        "source_db": r"D:\quant.duckdb",
        "state_db": "screening_state.duckdb",
        "log_dir": "logs",
        "static_dir": "app/static",
        "strategies_dir": "strategies",
    },
    "cache": {
        "root_dir": "cache/strategy_features",
        "max_bytes": 20 * 1024 * 1024 * 1024,
    },
    "specialized_engine": {
        "mutex": 1,
        "max_workers": 8,
        "version": "burst_v1.0",
        "fallback_to_backtrader": True,
    },
    "timeframes": {
        "order": ["w", "d", "60", "30", "15"],
        "to_table": {
            "15": "klines_15",
            "30": "klines_30",
            "60": "klines_60",
            "d": "klines_d",
            "w": "klines_w",
        },
    },
    "maintenance": {
        "discontinuity_lookback_days": 20,
        "backfill_lookback_days": 20,
        "no_source_recheck_hours": 6,
        "no_source_max_retries": 2,
        "fetch_window_merge_max_days": 10,
        "missing_require_source_data": True,
        "no_source_suppress_discontinuity": True,
        "source_default_start_date": "2018-12-01",
        "validation_max_codes": 300,
        "validation_wait_timeout_seconds": 120,
        "stop_soft_timeout_seconds": 10,
        "stop_force_rewarm_timeout_seconds": 60,
        "fetch_codes_chunk_size": 120,
        "latest_refresh_scope_mode": "token_code_scope",
        "fetch_workers": 1,
        "agg_workers": 1,
        "request_window_max_span_days": 31,
        "allowed_stock_prefixes": ["sh.", "sz.", "bj.", "hk."],
        "pipeline_queue_size": 200,
        "write_batch_codes": 240,
        "write_batch_max_rows": 150000,
        "queue_poll_timeout_seconds": 1.0,
        "write_flush_rows": 5000,
        "retry_rounds": 2,
        "debug": {
            "latest_update": True,
            "historical_data": True,
        },
    },
    "concept": {
        "request_timeout_seconds": 10.0,
        "max_workers": 5,
        "retry_count": 1,
        "exclude_board_names": [
            "融资融券", "深股通", "央国企改革", "沪股通", "预盈预增", "创业板综", "富时罗素", "预亏预减",
            "专精特新", "标准普尔", "机构重仓", "MSCI中国", "深成500", "中证500", "转债标的", "微盘股",
            "上证380", "HS300_", "QFII重仓", "证金持股", "参股银行", "股权激励", "养老金", "上证180_",
            "贬值受益", "ST股", "AH股", "PPP模式", "参股券商", "参股新三板", "深证100R", "创业成份",
            "百元股", "参股保险", "稀缺资源", "昨日涨停_含一字", "昨日涨停", "基金重仓", "AB股", "IPO受益",
            "央视50_", "上证50_", "参股期货", "科创板做市股", "银行", "茅指数", "股权转让", "昨日触板",
            "广电", "宁组合", "综合行业", "彩票概念", "社保重仓", "昨日连板_含一字", "租售同权", "举牌",
            "科创板做市商", "退税商店", "昨日连板", "赛马概念",
        ],
    },
    "parallel_log": {
        "compact_enabled": True,
        "sample_size": 8,
        "include_full_tasks_debug": False,
    },
    "maintenance_log": {
        "app_compact_enabled": True,
        "app_show_step_heartbeat": False,
        "app_policy": "progress_stage_warning",
    },
    "log_keep": {
        "jobs_per_category": 3,
    },
    "app": {
        "startup_prewarm_parallel_fetcher_enabled": True,
        "shutdown_grace_seconds": 15,
    },
}


def _deep_merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """
    输入：
    1. base: 默认配置字典。
    2. override: 用户覆盖配置字典。
    输出：
    1. 递归合并后的新字典。
    用途：
    1. 在保留默认值的前提下应用 YAML 覆盖项。
    边界条件：
    1. 仅当同名字段均为字典时递归合并；若类型不同或任一方不是字典，则直接用覆盖值替换。
    2. 所有写入结果都通过 `copy.deepcopy()` 隔离，避免外部对象后续修改影响全局配置。
    """

    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _read_yaml_config(config_path: Path) -> dict[str, Any]:
    """
    输入：
    1. config_path: YAML 配置文件路径。
    输出：
    1. 解析后的顶层字典。
    用途：
    1. 统一处理配置文件读取与 YAML 解析错误。
    边界条件：
    1. 文件缺失、YAML 语法错误、顶层非对象都会抛出异常，并由启动流程视为致命配置错误。
    2. 空文件会被视为 `{}`，表示完全使用内置默认配置。
    """

    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"YAML 语法错误: {config_path} | {exc}") from exc

    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"配置文件顶层必须是对象: {config_path}")
    return raw


def _expect_mapping(value: Any, key_path: str) -> dict[str, Any]:
    """
    输入：
    1. value: 待校验值。
    2. key_path: 配置路径，用于报错定位。
    输出：
    1. 通过校验的字典对象。
    用途：
    1. 统一校验“对象”类型配置项。
    边界条件：
    1. 非字典类型会抛出 ValueError。
    """

    if not isinstance(value, dict):
        raise ValueError(f"{key_path} 必须是对象")
    return value


def _expect_str(value: Any, key_path: str) -> str:
    """
    输入：
    1. value: 待校验值。
    2. key_path: 配置路径，用于报错定位。
    输出：
    1. 通过校验的字符串。
    用途：
    1. 统一校验字符串配置项。
    边界条件：
    1. 空字符串与非字符串都会抛出 ValueError。
    """

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key_path} 必须是非空字符串")
    return value.strip()


def _expect_bool(value: Any, key_path: str) -> bool:
    """
    输入：
    1. value: 待校验值。
    2. key_path: 配置路径，用于报错定位。
    输出：
    1. 通过校验的布尔值。
    用途：
    1. 统一校验布尔配置项。
    边界条件：
    1. 非 bool 类型会抛出 ValueError。
    """

    if not isinstance(value, bool):
        raise ValueError(f"{key_path} 必须是布尔值")
    return value


def _expect_int(value: Any, key_path: str, *, minimum: int | None = None) -> int:
    """
    输入：
    1. value: 待校验值。
    2. key_path: 配置路径，用于报错定位。
    3. minimum: 最小值约束（可空）。
    输出：
    1. 通过校验的整数。
    用途：
    1. 统一校验整数配置项及最小值边界。
    边界条件：
    1. bool 视为非法整数；不满足最小值约束会抛出 ValueError。
    """

    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key_path} 必须是整数")
    if minimum is not None and value < minimum:
        raise ValueError(f"{key_path} 必须大于等于 {minimum}")
    return value


def _expect_float(value: Any, key_path: str, *, minimum: float | None = None) -> float:
    """
    输入：
    1. value: 待校验值。
    2. key_path: 配置路径，用于报错定位。
    3. minimum: 最小值约束（可空）。
    输出：
    1. 通过校验的浮点值。
    用途：
    1. 统一校验数值配置项及最小值边界。
    边界条件：
    1. bool 视为非法数值；不满足最小值约束会抛出 ValueError。
    """

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{key_path} 必须是数值")
    parsed = float(value)
    if minimum is not None and parsed < minimum:
        raise ValueError(f"{key_path} 必须大于等于 {minimum}")
    return parsed


def _expect_string_list(value: Any, key_path: str) -> list[str]:
    """
    输入：
    1. value: 待校验值。
    2. key_path: 配置路径，用于报错定位。
    输出：
    1. 去空白后的字符串列表。
    用途：
    1. 统一校验字符串列表配置项。
    边界条件：
    1. 非列表、包含空字符串或非字符串元素会抛出 ValueError。
    """

    if not isinstance(value, list):
        raise ValueError(f"{key_path} 必须是字符串列表")
    normalized: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{key_path}[{index}] 必须是非空字符串")
        normalized.append(item.strip())
    return normalized


def _resolve_project_path(raw_path: str, key_path: str) -> Path:
    """
    输入：
    1. raw_path: 原始路径字符串。
    2. key_path: 配置路径，用于报错定位。
    输出：
    1. 绝对路径 `Path`。
    用途：
    1. 统一实现“相对路径相对项目根目录”解析规则。
    边界条件：
    1. 空字符串会抛出 ValueError。
    """

    token = _expect_str(raw_path, key_path)
    path = Path(token)
    if not path.is_absolute():
        path = PROJECT_DIR / path
    return path.resolve()


def _build_runtime_settings(raw_config: dict[str, Any]) -> dict[str, Any]:
    """
    输入：
    1. raw_config: YAML 读取后的原始配置对象。
    输出：
    1. 经过合并与校验的运行时配置常量字典。
    用途：
    1. 将分组配置转换为代码侧可直接使用的常量集合。
    边界条件：
    1. 任一关键项类型或边界不合法时抛出 ValueError。
    """

    merged = _deep_merge_dict(_DEFAULT_CONFIG, raw_config)

    paths_cfg = _expect_mapping(merged.get("paths"), "paths")
    source_db_path = _resolve_project_path(paths_cfg.get("source_db"), "paths.source_db")
    state_db_path = _resolve_project_path(paths_cfg.get("state_db"), "paths.state_db")
    log_dir = _resolve_project_path(paths_cfg.get("log_dir"), "paths.log_dir")
    static_dir = _resolve_project_path(paths_cfg.get("static_dir"), "paths.static_dir")
    strategies_dir = _resolve_project_path(paths_cfg.get("strategies_dir"), "paths.strategies_dir")

    cache_cfg = _expect_mapping(merged.get("cache"), "cache")
    cache_root_dir = _resolve_project_path(cache_cfg.get("root_dir"), "cache.root_dir")
    cache_max_bytes = _expect_int(cache_cfg.get("max_bytes"), "cache.max_bytes", minimum=1)

    specialized_cfg = _expect_mapping(merged.get("specialized_engine"), "specialized_engine")
    specialized_engine_mutex = _expect_int(specialized_cfg.get("mutex"), "specialized_engine.mutex", minimum=1)
    specialized_engine_max_workers = _expect_int(
        specialized_cfg.get("max_workers"),
        "specialized_engine.max_workers",
        minimum=1,
    )
    specialized_engine_version = _expect_str(specialized_cfg.get("version"), "specialized_engine.version")
    specialized_engine_fallback = _expect_bool(
        specialized_cfg.get("fallback_to_backtrader"),
        "specialized_engine.fallback_to_backtrader",
    )

    timeframes_cfg = _expect_mapping(merged.get("timeframes"), "timeframes")
    timeframe_order = _expect_string_list(timeframes_cfg.get("order"), "timeframes.order")
    timeframe_to_table_cfg = _expect_mapping(timeframes_cfg.get("to_table"), "timeframes.to_table")
    timeframe_to_table = {
        str(key).strip(): _expect_str(value, f"timeframes.to_table.{key}")
        for key, value in timeframe_to_table_cfg.items()
    }
    required_timeframes = {"15", "30", "60", "d", "w"}
    missing_timeframes = sorted(required_timeframes - set(timeframe_to_table.keys()))
    if missing_timeframes:
        raise ValueError(f"timeframes.to_table 缺少周期映射: {','.join(missing_timeframes)}")
    if len(set(timeframe_order)) != len(timeframe_order):
        raise ValueError("timeframes.order 存在重复周期")
    unknown_order_items = [tf for tf in timeframe_order if tf not in timeframe_to_table]
    if unknown_order_items:
        raise ValueError(f"timeframes.order 包含未知周期: {','.join(unknown_order_items)}")
    table_to_timeframe = {table: tf for tf, table in timeframe_to_table.items()}

    maintenance_cfg = _expect_mapping(merged.get("maintenance"), "maintenance")
    maintenance_discontinuity_lookback_days = _expect_int(
        maintenance_cfg.get("discontinuity_lookback_days"),
        "maintenance.discontinuity_lookback_days",
        minimum=1,
    )
    maintenance_backfill_lookback_days = _expect_int(
        maintenance_cfg.get("backfill_lookback_days"),
        "maintenance.backfill_lookback_days",
        minimum=1,
    )
    maintenance_no_source_recheck_hours = _expect_int(
        maintenance_cfg.get("no_source_recheck_hours"),
        "maintenance.no_source_recheck_hours",
        minimum=1,
    )
    maintenance_no_source_max_retries = _expect_int(
        maintenance_cfg.get("no_source_max_retries"),
        "maintenance.no_source_max_retries",
        minimum=1,
    )
    maintenance_fetch_window_merge_max_days = _expect_int(
        maintenance_cfg.get("fetch_window_merge_max_days"),
        "maintenance.fetch_window_merge_max_days",
        minimum=1,
    )
    maintenance_missing_require_source_data = _expect_bool(
        maintenance_cfg.get("missing_require_source_data"),
        "maintenance.missing_require_source_data",
    )
    maintenance_no_source_suppress_discontinuity = _expect_bool(
        maintenance_cfg.get("no_source_suppress_discontinuity"),
        "maintenance.no_source_suppress_discontinuity",
    )
    maintenance_source_default_start_date = _expect_str(
        maintenance_cfg.get("source_default_start_date"),
        "maintenance.source_default_start_date",
    )
    maintenance_validation_max_codes = _expect_int(
        maintenance_cfg.get("validation_max_codes"),
        "maintenance.validation_max_codes",
        minimum=20,
    )
    maintenance_validation_wait_timeout_seconds = _expect_int(
        maintenance_cfg.get("validation_wait_timeout_seconds"),
        "maintenance.validation_wait_timeout_seconds",
        minimum=1,
    )
    maintenance_stop_soft_timeout_seconds = _expect_float(
        maintenance_cfg.get("stop_soft_timeout_seconds"),
        "maintenance.stop_soft_timeout_seconds",
        minimum=0.0,
    )
    maintenance_stop_force_rewarm_timeout_seconds = _expect_float(
        maintenance_cfg.get("stop_force_rewarm_timeout_seconds"),
        "maintenance.stop_force_rewarm_timeout_seconds",
        minimum=1.0,
    )
    maintenance_fetch_codes_chunk_size = _expect_int(
        maintenance_cfg.get("fetch_codes_chunk_size"),
        "maintenance.fetch_codes_chunk_size",
        minimum=1,
    )
    maintenance_latest_refresh_scope_mode = _expect_str(
        maintenance_cfg.get("latest_refresh_scope_mode"),
        "maintenance.latest_refresh_scope_mode",
    ).lower()
    if maintenance_latest_refresh_scope_mode not in {"token_code_scope", "group_codes"}:
        raise ValueError("maintenance.latest_refresh_scope_mode 仅支持 token_code_scope/group_codes")
    maintenance_fetch_workers = _expect_int(
        maintenance_cfg.get("fetch_workers"),
        "maintenance.fetch_workers",
        minimum=1,
    )
    maintenance_agg_workers = _expect_int(
        maintenance_cfg.get("agg_workers"),
        "maintenance.agg_workers",
        minimum=1,
    )
    maintenance_request_window_max_span_days = _expect_int(
        maintenance_cfg.get("request_window_max_span_days"),
        "maintenance.request_window_max_span_days",
        minimum=1,
    )
    maintenance_allowed_stock_prefixes = tuple(
        _expect_string_list(
            maintenance_cfg.get("allowed_stock_prefixes"),
            "maintenance.allowed_stock_prefixes",
        )
    )
    maintenance_pipeline_queue_size = _expect_int(
        maintenance_cfg.get("pipeline_queue_size"),
        "maintenance.pipeline_queue_size",
        minimum=1,
    )
    maintenance_write_batch_codes = _expect_int(
        maintenance_cfg.get("write_batch_codes"),
        "maintenance.write_batch_codes",
        minimum=1,
    )
    maintenance_write_batch_max_rows = _expect_int(
        maintenance_cfg.get("write_batch_max_rows"),
        "maintenance.write_batch_max_rows",
        minimum=1,
    )
    maintenance_queue_poll_timeout_seconds = _expect_float(
        maintenance_cfg.get("queue_poll_timeout_seconds"),
        "maintenance.queue_poll_timeout_seconds",
        minimum=0.2,
    )
    maintenance_write_flush_rows = _expect_int(
        maintenance_cfg.get("write_flush_rows"),
        "maintenance.write_flush_rows",
        minimum=200,
    )
    maintenance_retry_rounds = _expect_int(
        maintenance_cfg.get("retry_rounds"),
        "maintenance.retry_rounds",
        minimum=1,
    )
    maintenance_debug_cfg = _expect_mapping(maintenance_cfg.get("debug"), "maintenance.debug")
    maintenance_debug_latest_update = _expect_bool(
        maintenance_debug_cfg.get("latest_update"),
        "maintenance.debug.latest_update",
    )
    maintenance_debug_historical_data = _expect_bool(
        maintenance_debug_cfg.get("historical_data"),
        "maintenance.debug.historical_data",
    )
    parallel_log_cfg = _expect_mapping(merged.get("parallel_log"), "parallel_log")
    parallel_log_compact_enabled = _expect_bool(
        parallel_log_cfg.get("compact_enabled"),
        "parallel_log.compact_enabled",
    )
    parallel_log_sample_size = _expect_int(
        parallel_log_cfg.get("sample_size"),
        "parallel_log.sample_size",
        minimum=1,
    )
    parallel_log_include_full_tasks_debug = _expect_bool(
        parallel_log_cfg.get("include_full_tasks_debug"),
        "parallel_log.include_full_tasks_debug",
    )
    maintenance_log_cfg = _expect_mapping(merged.get("maintenance_log"), "maintenance_log")
    maintenance_log_app_compact_enabled = _expect_bool(
        maintenance_log_cfg.get("app_compact_enabled"),
        "maintenance_log.app_compact_enabled",
    )
    maintenance_log_app_show_step_heartbeat = _expect_bool(
        maintenance_log_cfg.get("app_show_step_heartbeat"),
        "maintenance_log.app_show_step_heartbeat",
    )
    maintenance_log_app_policy = _expect_str(
        maintenance_log_cfg.get("app_policy"),
        "maintenance_log.app_policy",
    )
    if maintenance_log_app_policy not in {"progress_stage_warning", "full"}:
        raise ValueError("maintenance_log.app_policy 仅支持 progress_stage_warning/full")
    concept_cfg = _expect_mapping(merged.get("concept"), "concept")
    concept_request_timeout_seconds = _expect_float(
        concept_cfg.get("request_timeout_seconds"),
        "concept.request_timeout_seconds",
        minimum=0.1,
    )
    concept_max_workers = _expect_int(
        concept_cfg.get("max_workers"),
        "concept.max_workers",
        minimum=1,
    )
    concept_retry_count = _expect_int(
        concept_cfg.get("retry_count"),
        "concept.retry_count",
        minimum=0,
    )
    concept_exclude_board_names = tuple(
        _expect_string_list(concept_cfg.get("exclude_board_names"), "concept.exclude_board_names")
    )
    log_keep_cfg = _expect_mapping(merged.get("log_keep"), "log_keep")
    log_keep_jobs_per_category = _expect_int(
        log_keep_cfg.get("jobs_per_category"),
        "log_keep.jobs_per_category",
        minimum=1,
    )
    app_cfg = _expect_mapping(merged.get("app"), "app")
    app_startup_prewarm_parallel_fetcher_enabled = _expect_bool(
        app_cfg.get("startup_prewarm_parallel_fetcher_enabled"),
        "app.startup_prewarm_parallel_fetcher_enabled",
    )
    app_shutdown_grace_seconds = _expect_int(
        app_cfg.get("shutdown_grace_seconds"),
        "app.shutdown_grace_seconds",
        minimum=1,
    )

    return {
        "SOURCE_DB_PATH": source_db_path,
        "STATE_DB_PATH": state_db_path,
        "LOG_DIR": log_dir,
        "LOG_FILE": log_dir / "app.log",
        "STATIC_DIR": static_dir,
        "STRATEGIES_DIR": strategies_dir,
        "STRATEGY_GROUPS_DIR": strategies_dir / "groups",
        "CACHE_ROOT_DIR": cache_root_dir,
        "CACHE_MAX_BYTES": cache_max_bytes,
        "SPECIALIZED_ENGINE_MUTEX": specialized_engine_mutex,
        "SPECIALIZED_ENGINE_MAX_WORKERS": specialized_engine_max_workers,
        "SPECIALIZED_ENGINE_VERSION": specialized_engine_version,
        "SPECIALIZED_ENGINE_FALLBACK_TO_BACKTRADER": specialized_engine_fallback,
        "TIMEFRAME_ORDER": timeframe_order,
        "TIMEFRAME_TO_TABLE": timeframe_to_table,
        "TABLE_TO_TIMEFRAME": table_to_timeframe,
        "MAINTENANCE_DISCONTINUITY_LOOKBACK_DAYS": maintenance_discontinuity_lookback_days,
        "MAINTENANCE_BACKFILL_LOOKBACK_DAYS": maintenance_backfill_lookback_days,
        "MAINTENANCE_NO_SOURCE_RECHECK_HOURS": maintenance_no_source_recheck_hours,
        "MAINTENANCE_NO_SOURCE_MAX_RETRIES": maintenance_no_source_max_retries,
        "MAINTENANCE_FETCH_WINDOW_MERGE_MAX_DAYS": maintenance_fetch_window_merge_max_days,
        "MAINTENANCE_MISSING_REQUIRE_SOURCE_DATA": maintenance_missing_require_source_data,
        "MAINTENANCE_NO_SOURCE_SUPPRESS_DISCONTINUITY": maintenance_no_source_suppress_discontinuity,
        "MAINTENANCE_SOURCE_DEFAULT_START_DATE": maintenance_source_default_start_date,
        "MAINTENANCE_VALIDATION_MAX_CODES": maintenance_validation_max_codes,
        "MAINTENANCE_VALIDATION_WAIT_TIMEOUT_SECONDS": maintenance_validation_wait_timeout_seconds,
        "MAINTENANCE_STOP_SOFT_TIMEOUT_SECONDS": maintenance_stop_soft_timeout_seconds,
        "MAINTENANCE_STOP_FORCE_REWARM_TIMEOUT_SECONDS": maintenance_stop_force_rewarm_timeout_seconds,
        "MAINTENANCE_FETCH_CODES_CHUNK_SIZE": maintenance_fetch_codes_chunk_size,
        "MAINTENANCE_LATEST_REFRESH_SCOPE_MODE": maintenance_latest_refresh_scope_mode,
        "MAINTENANCE_FETCH_WORKERS": maintenance_fetch_workers,
        "MAINTENANCE_AGG_WORKERS": maintenance_agg_workers,
        "MAINTENANCE_REQUEST_WINDOW_MAX_SPAN_DAYS": maintenance_request_window_max_span_days,
        "MAINTENANCE_ALLOWED_STOCK_PREFIXES": maintenance_allowed_stock_prefixes,
        "APP_SHUTDOWN_GRACE_SECONDS": app_shutdown_grace_seconds,
        "MAINTENANCE_PIPELINE_QUEUE_SIZE": maintenance_pipeline_queue_size,
        "MAINTENANCE_WRITE_BATCH_CODES": maintenance_write_batch_codes,
        "MAINTENANCE_WRITE_BATCH_MAX_ROWS": maintenance_write_batch_max_rows,
        "MAINTENANCE_QUEUE_POLL_TIMEOUT_SECONDS": maintenance_queue_poll_timeout_seconds,
        "MAINTENANCE_WRITE_FLUSH_ROWS": maintenance_write_flush_rows,
        "MAINTENANCE_RETRY_ROUNDS": maintenance_retry_rounds,
        "MAINTENANCE_DEBUG_LATEST_UPDATE": maintenance_debug_latest_update,
        "MAINTENANCE_DEBUG_HISTORICAL_DATA": maintenance_debug_historical_data,
        "PARALLEL_LOG_COMPACT_ENABLED": parallel_log_compact_enabled,
        "PARALLEL_LOG_SAMPLE_SIZE": parallel_log_sample_size,
        "PARALLEL_LOG_INCLUDE_FULL_TASKS_DEBUG": parallel_log_include_full_tasks_debug,
        "MAINTENANCE_LOG_APP_COMPACT_ENABLED": maintenance_log_app_compact_enabled,
        "MAINTENANCE_LOG_APP_SHOW_STEP_HEARTBEAT": maintenance_log_app_show_step_heartbeat,
        "MAINTENANCE_LOG_APP_POLICY": maintenance_log_app_policy,
        "CONCEPT_REQUEST_TIMEOUT_SECONDS": concept_request_timeout_seconds,
        "CONCEPT_MAX_WORKERS": concept_max_workers,
        "CONCEPT_RETRY_COUNT": concept_retry_count,
        "CONCEPT_EXCLUDE_BOARD_NAMES": concept_exclude_board_names,
        "LOG_KEEP_JOBS_PER_CATEGORY": log_keep_jobs_per_category,
        "APP_STARTUP_PREWARM_PARALLEL_FETCHER_ENABLED": app_startup_prewarm_parallel_fetcher_enabled,
    }


try:
    _RUNTIME_SETTINGS = _build_runtime_settings(_read_yaml_config(CONFIG_YAML_PATH))
except Exception as exc:
    raise RuntimeError(f"加载配置失败: {CONFIG_YAML_PATH} | {exc}") from exc

SOURCE_DB_PATH = _RUNTIME_SETTINGS["SOURCE_DB_PATH"]
STATE_DB_PATH = _RUNTIME_SETTINGS["STATE_DB_PATH"]
LOG_DIR = _RUNTIME_SETTINGS["LOG_DIR"]
LOG_FILE = _RUNTIME_SETTINGS["LOG_FILE"]
STATIC_DIR = _RUNTIME_SETTINGS["STATIC_DIR"]
STRATEGIES_DIR = _RUNTIME_SETTINGS["STRATEGIES_DIR"]
STRATEGY_GROUPS_DIR = _RUNTIME_SETTINGS["STRATEGY_GROUPS_DIR"]
CACHE_ROOT_DIR = _RUNTIME_SETTINGS["CACHE_ROOT_DIR"]
CACHE_DIR = CACHE_ROOT_DIR
CACHE_MAX_BYTES = _RUNTIME_SETTINGS["CACHE_MAX_BYTES"]
SPECIALIZED_ENGINE_MUTEX = _RUNTIME_SETTINGS["SPECIALIZED_ENGINE_MUTEX"]
SPECIALIZED_ENGINE_MAX_WORKERS = _RUNTIME_SETTINGS["SPECIALIZED_ENGINE_MAX_WORKERS"]
SPECIALIZED_ENGINE_VERSION = _RUNTIME_SETTINGS["SPECIALIZED_ENGINE_VERSION"]
SPECIALIZED_ENGINE_FALLBACK_TO_BACKTRADER = _RUNTIME_SETTINGS["SPECIALIZED_ENGINE_FALLBACK_TO_BACKTRADER"]
TIMEFRAME_ORDER = _RUNTIME_SETTINGS["TIMEFRAME_ORDER"]
TIMEFRAME_TO_TABLE = _RUNTIME_SETTINGS["TIMEFRAME_TO_TABLE"]
TABLE_TO_TIMEFRAME = _RUNTIME_SETTINGS["TABLE_TO_TIMEFRAME"]
MAINTENANCE_DISCONTINUITY_LOOKBACK_DAYS = _RUNTIME_SETTINGS["MAINTENANCE_DISCONTINUITY_LOOKBACK_DAYS"]
MAINTENANCE_BACKFILL_LOOKBACK_DAYS = _RUNTIME_SETTINGS["MAINTENANCE_BACKFILL_LOOKBACK_DAYS"]
MAINTENANCE_NO_SOURCE_RECHECK_HOURS = _RUNTIME_SETTINGS["MAINTENANCE_NO_SOURCE_RECHECK_HOURS"]
MAINTENANCE_NO_SOURCE_MAX_RETRIES = _RUNTIME_SETTINGS["MAINTENANCE_NO_SOURCE_MAX_RETRIES"]
MAINTENANCE_FETCH_WINDOW_MERGE_MAX_DAYS = _RUNTIME_SETTINGS["MAINTENANCE_FETCH_WINDOW_MERGE_MAX_DAYS"]
MAINTENANCE_MISSING_REQUIRE_SOURCE_DATA = _RUNTIME_SETTINGS["MAINTENANCE_MISSING_REQUIRE_SOURCE_DATA"]
MAINTENANCE_NO_SOURCE_SUPPRESS_DISCONTINUITY = _RUNTIME_SETTINGS["MAINTENANCE_NO_SOURCE_SUPPRESS_DISCONTINUITY"]
MAINTENANCE_SOURCE_DEFAULT_START_DATE = _RUNTIME_SETTINGS["MAINTENANCE_SOURCE_DEFAULT_START_DATE"]
MAINTENANCE_VALIDATION_MAX_CODES = _RUNTIME_SETTINGS["MAINTENANCE_VALIDATION_MAX_CODES"]
MAINTENANCE_VALIDATION_WAIT_TIMEOUT_SECONDS = _RUNTIME_SETTINGS["MAINTENANCE_VALIDATION_WAIT_TIMEOUT_SECONDS"]
MAINTENANCE_STOP_SOFT_TIMEOUT_SECONDS = _RUNTIME_SETTINGS["MAINTENANCE_STOP_SOFT_TIMEOUT_SECONDS"]
MAINTENANCE_STOP_FORCE_REWARM_TIMEOUT_SECONDS = _RUNTIME_SETTINGS["MAINTENANCE_STOP_FORCE_REWARM_TIMEOUT_SECONDS"]
MAINTENANCE_FETCH_CODES_CHUNK_SIZE = _RUNTIME_SETTINGS["MAINTENANCE_FETCH_CODES_CHUNK_SIZE"]
MAINTENANCE_LATEST_REFRESH_SCOPE_MODE = _RUNTIME_SETTINGS["MAINTENANCE_LATEST_REFRESH_SCOPE_MODE"]
MAINTENANCE_FETCH_WORKERS = _RUNTIME_SETTINGS["MAINTENANCE_FETCH_WORKERS"]
MAINTENANCE_AGG_WORKERS = _RUNTIME_SETTINGS["MAINTENANCE_AGG_WORKERS"]
MAINTENANCE_REQUEST_WINDOW_MAX_SPAN_DAYS = _RUNTIME_SETTINGS["MAINTENANCE_REQUEST_WINDOW_MAX_SPAN_DAYS"]
MAINTENANCE_ALLOWED_STOCK_PREFIXES = _RUNTIME_SETTINGS["MAINTENANCE_ALLOWED_STOCK_PREFIXES"]
APP_SHUTDOWN_GRACE_SECONDS = _RUNTIME_SETTINGS["APP_SHUTDOWN_GRACE_SECONDS"]
APP_STARTUP_PREWARM_PARALLEL_FETCHER_ENABLED = _RUNTIME_SETTINGS["APP_STARTUP_PREWARM_PARALLEL_FETCHER_ENABLED"]
MAINTENANCE_PIPELINE_QUEUE_SIZE = _RUNTIME_SETTINGS["MAINTENANCE_PIPELINE_QUEUE_SIZE"]
MAINTENANCE_WRITE_BATCH_CODES = _RUNTIME_SETTINGS["MAINTENANCE_WRITE_BATCH_CODES"]
MAINTENANCE_WRITE_BATCH_MAX_ROWS = _RUNTIME_SETTINGS["MAINTENANCE_WRITE_BATCH_MAX_ROWS"]
MAINTENANCE_QUEUE_POLL_TIMEOUT_SECONDS = _RUNTIME_SETTINGS["MAINTENANCE_QUEUE_POLL_TIMEOUT_SECONDS"]
MAINTENANCE_WRITE_FLUSH_ROWS = _RUNTIME_SETTINGS["MAINTENANCE_WRITE_FLUSH_ROWS"]
MAINTENANCE_RETRY_ROUNDS = _RUNTIME_SETTINGS["MAINTENANCE_RETRY_ROUNDS"]
MAINTENANCE_DEBUG_LATEST_UPDATE = _RUNTIME_SETTINGS["MAINTENANCE_DEBUG_LATEST_UPDATE"]
MAINTENANCE_DEBUG_HISTORICAL_DATA = _RUNTIME_SETTINGS["MAINTENANCE_DEBUG_HISTORICAL_DATA"]
PARALLEL_LOG_COMPACT_ENABLED = _RUNTIME_SETTINGS["PARALLEL_LOG_COMPACT_ENABLED"]
PARALLEL_LOG_SAMPLE_SIZE = _RUNTIME_SETTINGS["PARALLEL_LOG_SAMPLE_SIZE"]
PARALLEL_LOG_INCLUDE_FULL_TASKS_DEBUG = _RUNTIME_SETTINGS["PARALLEL_LOG_INCLUDE_FULL_TASKS_DEBUG"]
MAINTENANCE_LOG_APP_COMPACT_ENABLED = _RUNTIME_SETTINGS["MAINTENANCE_LOG_APP_COMPACT_ENABLED"]
MAINTENANCE_LOG_APP_SHOW_STEP_HEARTBEAT = _RUNTIME_SETTINGS["MAINTENANCE_LOG_APP_SHOW_STEP_HEARTBEAT"]
MAINTENANCE_LOG_APP_POLICY = _RUNTIME_SETTINGS["MAINTENANCE_LOG_APP_POLICY"]
CONCEPT_REQUEST_TIMEOUT_SECONDS = _RUNTIME_SETTINGS["CONCEPT_REQUEST_TIMEOUT_SECONDS"]
CONCEPT_MAX_WORKERS = _RUNTIME_SETTINGS["CONCEPT_MAX_WORKERS"]
CONCEPT_RETRY_COUNT = _RUNTIME_SETTINGS["CONCEPT_RETRY_COUNT"]
CONCEPT_EXCLUDE_BOARD_NAMES = _RUNTIME_SETTINGS["CONCEPT_EXCLUDE_BOARD_NAMES"]
LOG_KEEP_JOBS_PER_CATEGORY = _RUNTIME_SETTINGS["LOG_KEEP_JOBS_PER_CATEGORY"]
