"""
行情维护执行服务（重构版）。

职责：
1. 以双模式引擎执行维护任务：latest_update / historical_backfill。
2. 统一调度 zsdtdx simple_api 异步队列并将结果增量落库。
3. 维护任务摘要日志与可选 debug 明细落库。
"""

from __future__ import annotations

import queue
import time
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable

import duckdb
import pandas as pd

from app.db.duckdb_utils import connect_duckdb_compatible
from app.db.state_db import StateDB
from app.services.maintenance_calendar import weekdays_between_exclusive, week_monday_and_friday
from app.services.maintenance_codecs import normalize_code_for_storage, normalize_code_set
from app.services.simple_api_bridge import (
    get_runtime_failures,
    get_runtime_metadata,
    get_stock_code_name,
    get_tdx_client,
    get_stock_kline,
    managed_stock_kline_job,
)
from app.settings import (
    MAINTENANCE_QUEUE_POLL_TIMEOUT_SECONDS,
    MAINTENANCE_RETRY_ROUNDS,
    MAINTENANCE_SOURCE_DEFAULT_START_DATE,
    MAINTENANCE_WRITE_FLUSH_ROWS,
)


KLINE_FREQ_ORDER: list[str] = ["15", "30", "60", "d", "w"]
KLINE_TABLE_BY_FREQ: dict[str, str] = {
    "15": "klines_15",
    "30": "klines_30",
    "60": "klines_60",
    "d": "klines_d",
    "w": "klines_w",
}
def _compute_latest_start_time(freq: str, latest_dt: datetime) -> datetime:
    """根据周期计算更新最新数据的 start_time。

    15/30/60/d: 最新 datetime 前一天 09:30。
    w: 最新 datetime 7 天前 09:30。
    """
    if freq == "w":
        base_date = latest_dt.date() - timedelta(days=7)
    else:
        base_date = latest_dt.date() - timedelta(days=1)
    return datetime.combine(base_date, dt_time(9, 30, 0))
_HIST_MINUTE_EXPECTED: dict[str, int] = {"60": 4, "30": 8, "15": 16}
_TASK_RETRY_ROUNDS: int = max(1, int(MAINTENANCE_RETRY_ROUNDS))
_QUEUE_POLL_TIMEOUT_SECONDS: float = max(0.2, float(MAINTENANCE_QUEUE_POLL_TIMEOUT_SECONDS))
_WRITE_FLUSH_ROWS: int = max(200, int(MAINTENANCE_WRITE_FLUSH_ROWS))


class MaintenanceStopRequested(Exception):
    """
    维护任务收到停止请求时抛出的内部异常。
    """


@dataclass(frozen=True, slots=True)
class MaintenanceTask:
    """
    维护任务条目。
    """

    code: str
    freq: str
    start_time: str
    end_time: str

    def signature(self) -> str:
        """
        输入：
        1. 无。
        输出：
        1. 任务唯一签名字符串。
        用途：
        1. 在重试、去重、状态映射中作为稳定键。
        边界条件：
        1. 字段顺序固定，避免签名漂移。
        """

        return f"{self.code}|{self.freq}|{self.start_time}|{self.end_time}"

    def to_api_payload(self) -> dict[str, str]:
        """
        输入：
        1. 无。
        输出：
        1. simple_api.get_stock_kline 所需 task 字典。
        用途：
        1. 构建异步抓取任务列表。
        边界条件：
        1. 字段均为字符串，和 simple_api 规范一致。
        """

        return {
            "code": self.code,
            "freq": self.freq,
            "start_time": self.start_time,
            "end_time": self.end_time,
        }


@dataclass(slots=True)
class FetchWriteStats:
    """
    单次抓取落库统计。
    """

    total_tasks: int
    success_tasks: int
    no_data_tasks: int
    failed_tasks: int
    retry_rounds_used: int
    rows_written_by_freq: dict[str, int]
    success_signatures: set[str]
    no_data_signatures: set[str]
    failed_signatures: set[str]
    failed_errors: dict[str, str]


@dataclass(slots=True)
class MaintenanceRunSummary:
    """
    维护任务摘要。
    """

    mode: str
    steps_total: int
    steps_completed: int
    total_tasks: int
    success_tasks: int
    no_data_tasks: int
    failed_tasks: int
    retry_rounds_used: int
    rows_written: dict[str, int]
    retry_skipped_tasks: int
    removed_corrupted_rows: int
    duration_seconds: float


def _coerce_datetime(value: Any) -> datetime | None:
    """
    输入：
    1. value: 任意时间值。
    输出：
    1. 解析后的 datetime；无法解析时返回 None。
    用途：
    1. 统一处理字符串/date/datetime 输入。
    边界条件：
    1. 支持 ISO8601 与 `YYYY-MM-DD HH:MM:SS`。
    """

    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, dt_time(0, 0, 0))
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        if token.endswith("Z"):
            token = token[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(token)
        except ValueError:
            return None
    return None


def _format_datetime_token(
    value: date | datetime | str,
    *,
    date_only: bool = False,
    boundary_clock: dt_time | None = None,
) -> str:
    """
    输入：
    1. value: 原始时间值。
    2. date_only: 是否仅输出日期。
    3. boundary_clock: 非 date_only 场景下，仅日期输入时补齐的默认时分秒。
    输出：
    1. 规范时间字符串。
    用途：
    1. 统一任务 start/end_time 的序列化格式。
    边界条件：
    1. 非法输入抛 ValueError。
    """

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            raise ValueError("任务时间不能为空")
        if date_only:
            dt = _coerce_datetime(raw)
            if dt is not None:
                return dt.date().isoformat()
            return date.fromisoformat(raw).isoformat()
        dt = _coerce_datetime(raw)
        if dt is None:
            fill_clock = boundary_clock or dt_time(0, 0, 0)
            dt = datetime.combine(date.fromisoformat(raw), fill_clock)
        elif boundary_clock is not None and ":" not in raw:
            dt = datetime.combine(dt.date(), boundary_clock)
        return dt.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, datetime):
        if date_only:
            return value.date().isoformat()
        return value.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, date):
        if date_only:
            return value.isoformat()
        fill_clock = boundary_clock or dt_time(0, 0, 0)
        return datetime.combine(value, fill_clock).strftime("%Y-%m-%d %H:%M:%S")

    raise ValueError(f"不支持的任务时间类型: {type(value).__name__}")


def _price_2(value: Any) -> float:
    """
    输入：
    1. value: 原始价格值。
    输出：
    1. 两位小数价格。
    用途：
    1. 统一 open/high/low/close 精度。
    边界条件：
    1. 异常值回退为 0.0。
    """

    try:
        return round(float(value), 2)
    except Exception:
        return 0.0


def _trunc_int(value: Any) -> int:
    """
    输入：
    1. value: 原始数值。
    输出：
    1. 截断后的整数（向 0 截断）。
    用途：
    1. 统一 volume/amount 处理规则。
    边界条件：
    1. 异常值回退为 0。
    """

    try:
        return int(float(value))
    except Exception:
        return 0


class MarketDataMaintenanceService:
    """
    行情维护服务（双模式）。
    """

    def __init__(
        self,
        *,
        source_db_path: Path | str,
        logger: Any,
        stop_checker: Callable[[], bool] | None = None,
        progress_reporter: Callable[[float, str | None, dict[str, Any] | None], None] | None = None,
        state_db: StateDB | None = None,
    ) -> None:
        """
        输入：
        1. source_db_path: 源行情库路径。
        2. logger: 维护日志器。
        3. stop_checker: 停止检查回调。
        4. progress_reporter: 进度回调。
        5. state_db: 状态库实例。
        输出：
        1. 无返回值。
        用途：
        1. 初始化维护执行上下文。
        边界条件：
        1. 未提供 stop_checker 时默认永不停止。
        """

        self.source_db_path = Path(source_db_path).resolve()
        self.logger = logger
        self.stop_checker = stop_checker or (lambda: False)
        self.progress_reporter = progress_reporter
        self.state_db = state_db
        self._temp_index = 0

    def _check_stop(self) -> None:
        """
        输入：
        1. 无。
        输出：
        1. 无返回值。
        用途：
        1. 协作取消检查点。
        边界条件：
        1. 检测到停止请求会抛 MaintenanceStopRequested。
        """

        if self.stop_checker():
            raise MaintenanceStopRequested("维护任务收到停止请求")

    def _report_progress(self, progress: float, *, phase: str | None = None, detail: dict[str, Any] | None = None) -> None:
        """
        输入：
        1. progress: 百分比进度。
        2. phase: 阶段标识。
        3. detail: 可选明细。
        输出：
        1. 无返回值。
        用途：
        1. 统一回调维护进度。
        边界条件：
        1. progress 会限制在 [0, 100]。
        """

        if not callable(self.progress_reporter):
            return
        safe_progress = min(100.0, max(0.0, float(progress)))
        self.progress_reporter(safe_progress, phase, detail)

    def _connect_source_db(self, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """
        输入：
        1. read_only: 是否只读连接。
        输出：
        1. DuckDB 连接对象。
        用途：
        1. 统一管理源库连接创建。
        边界条件：
        1. read_only=True 时仅执行查询。
        """

        return connect_duckdb_compatible(self.source_db_path, read_only=read_only, retries=20, retry_sleep_seconds=0.05)

    def _require_simple_api(self) -> None:
        """
        输入：
        1. 无。
        输出：
        1. 无返回值。
        用途：
        1. 检查 zsdtdx simple_api 运行依赖。
        边界条件：
        1. 依赖缺失抛 RuntimeError。
        """

        if get_tdx_client is None or get_stock_code_name is None or get_stock_kline is None:
            raise RuntimeError("zsdtdx.simple_api 不可用，请先安装并配置 zsdtdx")

    def _log_engine_runtime_diagnostics(self) -> None:
        """
        输入：
        1. 无。
        输出：
        1. 无返回值。
        用途：
        1. 在维护任务结束时记录 zsdtdx 引擎级运行时诊断信息。
        边界条件：
        1. zsdtdx API 不可用或调用异常时静默跳过。
        """

        if get_runtime_failures is None or get_runtime_metadata is None:
            return
        try:
            with get_tdx_client():
                meta = get_runtime_metadata()
                failures_df = get_runtime_failures()
            if meta:
                self.logger.info("zsdtdx运行时元数据", meta)
            if failures_df is not None and not failures_df.empty:
                rows = failures_df.head(20).to_dict("records")
                self.logger.warning(
                    "zsdtdx引擎级失败明细",
                    {"count": len(failures_df), "sample": rows},
                )
        except Exception:
            pass

    def ensure_runtime_schema(self) -> None:
        """
        输入：
        1. 无。
        输出：
        1. 无返回值。
        用途：
        1. 确保源库维护所需表结构存在。
        边界条件：
        1. 仅创建缺失表，不清理历史数据。
        """

        self.source_db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect_source_db(read_only=False) as con:
            con.execute(
                """
                create table if not exists stocks (
                    code varchar primary key,
                    name varchar
                )
                """
            )
            con.execute(
                """
                create table if not exists stock_concepts (
                    code varchar not null,
                    board_name varchar not null,
                    selected_reason varchar,
                    updated_at timestamp not null,
                    primary key (code, board_name)
                )
                """
            )
            for table in KLINE_TABLE_BY_FREQ.values():
                con.execute(
                    f"""
                    create table if not exists {table} (
                        code varchar not null,
                        datetime timestamp not null,
                        open double,
                        high double,
                        low double,
                        close double,
                        volume bigint,
                        amount bigint,
                        primary key (code, datetime)
                    )
                    """
                )

    def _make_task(
        self,
        *,
        code: Any,
        freq: str,
        start_time: date | datetime | str,
        end_time: date | datetime | str,
        date_only: bool = False,
    ) -> MaintenanceTask | None:
        """
        输入：
        1. code: 原始股票代码。
        2. freq: 周期。
        3. start_time: 起始时间。
        4. end_time: 结束时间。
        5. date_only: 是否按日期格式输出。
        输出：
        1. 合法任务对象；非法输入返回 None。
        用途：
        1. 统一任务构造与校验。
        边界条件：
        1. 非法代码、未知周期或时间异常会被过滤。
        """

        normalized_code = normalize_code_for_storage(code)
        freq_token = str(freq or "").strip().lower()
        if not normalized_code or freq_token not in KLINE_FREQ_ORDER:
            return None
        try:
            if date_only:
                start_token = _format_datetime_token(start_time, date_only=True)
                end_token = _format_datetime_token(end_time, date_only=True)
            else:
                start_token = _format_datetime_token(start_time, date_only=False, boundary_clock=dt_time(9, 30, 0))
                end_token = _format_datetime_token(end_time, date_only=False, boundary_clock=dt_time(16, 0, 0))
        except ValueError:
            return None
        return MaintenanceTask(
            code=normalized_code,
            freq=freq_token,
            start_time=start_token,
            end_time=end_token,
        )

    def _dedupe_tasks(self, tasks: Iterable[MaintenanceTask]) -> list[MaintenanceTask]:
        """
        输入：
        1. tasks: 任务序列。
        输出：
        1. 去重后的任务列表（保留首个顺序）。
        用途：
        1. 去除跨步骤重复任务。
        边界条件：
        1. 使用任务签名做唯一性判断。
        """

        result: list[MaintenanceTask] = []
        seen: set[str] = set()
        for task in tasks:
            sign = task.signature()
            if sign in seen:
                continue
            seen.add(sign)
            result.append(task)
        return result

    def _refresh_stocks_from_tdx(self) -> dict[str, str]:
        """
        输入：
        1. 无。
        输出：
        1. 归一化后的股票代码名称映射。
        用途：
        1. 执行 latest_update 步骤0全量覆盖 stocks。
        边界条件：
        1. 非法代码会被自动过滤。
        """

        self._require_simple_api()
        with get_tdx_client() as _client:
            stock_map = get_stock_code_name(use_cache=False)

        normalized_map: dict[str, str] = {}
        for raw_code, raw_name in (stock_map or {}).items():
            normalized = normalize_code_for_storage(raw_code)
            if not normalized:
                continue
            name = str(raw_name or "").strip()
            if normalized not in normalized_map:
                normalized_map[normalized] = name

        with self._connect_source_db(read_only=False) as con:
            con.execute("delete from stocks")
            if normalized_map:
                sorted_codes = sorted(normalized_map.keys())
                codes = sorted_codes
                names = [normalized_map[c] for c in sorted_codes]
                con.execute(
                    "insert into stocks(code, name) select unnest($1), unnest($2)",
                    [codes, names],
                )
        return normalized_map

    def _load_latest_by_freq(self) -> dict[str, dict[str, datetime]]:
        """
        输入：
        1. 无。
        输出：
        1. 各周期最新时间映射：freq -> code -> latest_datetime。
        用途：
        1. 构建 latest_update 增量起点。
        边界条件：
        1. 非法代码与空时间会被跳过。
        """

        result: dict[str, dict[str, datetime]] = {freq: {} for freq in KLINE_FREQ_ORDER}
        with self._connect_source_db(read_only=True) as con:
            for freq, table in KLINE_TABLE_BY_FREQ.items():
                rows = con.execute(
                    f"""
                    select code, max(datetime) as latest_dt
                    from {table}
                    group by code
                    """
                ).fetchall()
                for raw_code, latest_dt in rows:
                    normalized = normalize_code_for_storage(raw_code)
                    if not normalized:
                        continue
                    dt = _coerce_datetime(latest_dt)
                    if dt is None:
                        continue
                    result[freq][normalized] = dt
        return result

    def _build_latest_tasks(
        self,
        *,
        latest_by_freq: dict[str, dict[str, datetime]],
        end_time: datetime,
        all_codes: set[str],
    ) -> list[MaintenanceTask]:
        """
        输入：
        1. latest_by_freq: 最新时间映射。
        2. end_time: 统一结束时间（本周五 15:00）。
        3. all_codes: 步骤0代码全集。
        输出：
        1. latest_update 任务列表。
        用途：
        1. 落实步骤4和步骤5规则。
        边界条件：
        1. 缺失默认起点时回退到配置的 source_default_start_date。
        """

        tasks: list[MaintenanceTask] = []
        step4_code_set: set[str] = set()

        for freq in KLINE_FREQ_ORDER:
            self._check_stop()
            for idx, (code, latest_dt) in enumerate(latest_by_freq.get(freq, {}).items(), start=1):
                if idx % 1024 == 0:
                    self._check_stop()
                start_time = _compute_latest_start_time(freq, latest_dt)
                task = self._make_task(
                    code=code,
                    freq=freq,
                    start_time=start_time,
                    end_time=end_time,
                    date_only=False,
                )
                if task is None:
                    continue
                tasks.append(task)
                step4_code_set.add(code)

        default_start_date = date.fromisoformat(str(MAINTENANCE_SOURCE_DEFAULT_START_DATE))
        default_start_dt = datetime.combine(default_start_date, dt_time(9, 30, 0))
        missing_codes = sorted(code for code in all_codes if code not in step4_code_set)
        for code_idx, code in enumerate(missing_codes, start=1):
            if code_idx % 512 == 0:
                self._check_stop()
            for freq in KLINE_FREQ_ORDER:
                task = self._make_task(
                    code=code,
                    freq=freq,
                    start_time=default_start_dt,
                    end_time=end_time,
                    date_only=False,
                )
                if task is not None:
                    tasks.append(task)
        return self._dedupe_tasks(tasks)

    def _build_historical_gap_tasks_from_daily(self) -> list[MaintenanceTask]:
        """
        输入：
        1. 无。
        输出：
        1. 根据 d 周期缺口扩展出的任务列表。
        用途：
        1. 执行 historical_backfill 步骤1。
        边界条件：
        1. 仅生成工作日缺口，排除区间端点与周末。
        """

        tasks: list[MaintenanceTask] = []
        with self._connect_source_db(read_only=True) as con:
            rows = con.execute(
                """
                select code, cast(datetime as date) as trade_day
                from klines_d
                group by code, cast(datetime as date)
                order by code asc, trade_day asc
                """
            ).fetchall()

        current_code = ""
        previous_day: date | None = None
        for row_idx, (raw_code, trade_day) in enumerate(rows, start=1):
            if row_idx % 4096 == 0:
                self._check_stop()
            code = normalize_code_for_storage(raw_code)
            if not code or not isinstance(trade_day, date):
                continue
            if code != current_code:
                current_code = code
                previous_day = trade_day
                continue
            if previous_day is None:
                previous_day = trade_day
                continue
            missing_days = weekdays_between_exclusive(previous_day, trade_day)
            for missing_day in missing_days:
                for freq in ("d", "60", "30", "15"):
                    task = self._make_task(
                        code=code,
                        freq=freq,
                        start_time=missing_day,
                        end_time=missing_day,
                        date_only=False,
                    )
                    if task is not None:
                        tasks.append(task)
            previous_day = trade_day
        return tasks

    def _build_historical_minute_count_tasks(self) -> tuple[list[MaintenanceTask], int]:
        """
        输入：
        1. 无。
        输出：
        1. (分钟异常任务列表, 识别出的异常行数)。
        用途：
        1. 执行 historical_backfill 步骤2。
        边界条件：
        1. 仅针对 60/30/15，计数不符会标记回补任务；实际删除推迟到抓到非空新数据后。
        """

        tasks: list[MaintenanceTask] = []
        removed_rows = 0
        with self._connect_source_db(read_only=False) as con:
            for freq, expected in _HIST_MINUTE_EXPECTED.items():
                self._check_stop()
                table = KLINE_TABLE_BY_FREQ[freq]
                bad_rows = con.execute(
                    f"""
                    select code, cast(datetime as date) as trade_day
                    from {table}
                    group by code, cast(datetime as date)
                    having count(*) <> ?
                    order by code asc, trade_day asc
                    """,
                    [expected],
                ).fetchall()
                if not bad_rows:
                    continue

                bad_pairs: list[tuple[str, date]] = []
                for bad_idx, (raw_code, trade_day) in enumerate(bad_rows, start=1):
                    if bad_idx % 4096 == 0:
                        self._check_stop()
                    normalized = normalize_code_for_storage(raw_code)
                    if not normalized or not isinstance(trade_day, date):
                        continue
                    bad_pairs.append((normalized, trade_day))
                if not bad_pairs:
                    continue

                for pair_idx, (code, trade_day) in enumerate(bad_pairs, start=1):
                    if pair_idx % 256 == 0:
                        self._check_stop()
                    affected_count = int(
                        con.execute(
                            f"""
                            select count(*)
                            from {table}
                            where code = ?
                              and cast(datetime as date) = ?
                            """,
                            [code, trade_day],
                        ).fetchone()[0]
                        or 0
                    )
                    removed_rows += affected_count
                    task = self._make_task(
                        code=code,
                        freq=freq,
                        start_time=trade_day,
                        end_time=trade_day,
                        date_only=False,
                    )
                    if task is not None:
                        tasks.append(task)
        return tasks, removed_rows

    def _build_historical_weekly_gap_tasks(self) -> list[MaintenanceTask]:
        """
        输入：
        1. 无。
        输出：
        1. w 周期缺口任务列表。
        用途：
        1. 执行 historical_backfill 步骤3。
        边界条件：
        1. 仅当相邻记录日期差 > 7 天时生成任务窗口。
        """

        tasks: list[MaintenanceTask] = []
        with self._connect_source_db(read_only=True) as con:
            rows = con.execute(
                """
                select code, cast(datetime as date) as trade_day
                from klines_w
                group by code, cast(datetime as date)
                order by code asc, trade_day asc
                """
            ).fetchall()

        current_code = ""
        previous_day: date | None = None
        for row_idx, (raw_code, trade_day) in enumerate(rows, start=1):
            if row_idx % 4096 == 0:
                self._check_stop()
            code = normalize_code_for_storage(raw_code)
            if not code or not isinstance(trade_day, date):
                continue
            if code != current_code:
                current_code = code
                previous_day = trade_day
                continue
            if previous_day is None:
                previous_day = trade_day
                continue
            if (trade_day - previous_day).days > 7:
                start_day = previous_day + timedelta(days=1)
                end_day = trade_day
                task = self._make_task(
                    code=code,
                    freq="w",
                    start_time=start_day,
                    end_time=end_day,
                    date_only=False,
                )
                if task is not None:
                    tasks.append(task)
            previous_day = trade_day
        return tasks

    def _retry_task_key(self, task: MaintenanceTask, *, mode: str) -> str:
        """
        输入：
        1. task: 任务对象。
        2. mode: 任务模式。
        输出：
        1. maintenance_retry_tasks 主键。
        用途：
        1. 对历史维护任务做去重和尝试次数管理。
        边界条件：
        1. 键格式固定，便于排查。
        """

        return f"{mode}|{task.code}|{task.freq}|{task.start_time}|{task.end_time}"

    def _prepare_historical_retry_tasks(
        self,
        tasks: list[MaintenanceTask],
    ) -> tuple[list[MaintenanceTask], dict[str, str], int]:
        """
        输入：
        1. tasks: 原始历史任务列表。
        输出：
        1. (可执行任务, 任务签名->task_key 映射, 被剔除数量)。
        用途：
        1. 执行 historical_backfill 步骤4去重与尝试次数策略。
        边界条件：
        1. state_db 不可用时直接返回原任务。
        """

        if self.state_db is None or not tasks:
            return tasks, {}, 0

        t0 = time.monotonic()
        entries: list[tuple[MaintenanceTask, str, str]] = []
        task_key_by_signature: dict[str, str] = {}
        for task in tasks:
            signature = task.signature()
            task_key = self._retry_task_key(task, mode="historical_backfill")
            entries.append((task, signature, task_key))
            task_key_by_signature[signature] = task_key
        task_keys = list(task_key_by_signature.values())
        t1 = time.monotonic()
        self.logger.info(
            "retry 准备：构建 task_keys 完成",
            {"task_key_count": len(task_keys), "duration_seconds": round(t1 - t0, 4)},
        )

        existing_map = self.state_db.get_maintenance_retry_tasks(task_keys)
        t2 = time.monotonic()
        self.logger.info(
            "retry 准备：查询已有 retry 记录完成",
            {"existing_count": len(existing_map), "duration_seconds": round(t2 - t1, 4)},
        )

        executable: list[MaintenanceTask] = []
        skipped = 0
        new_rows: list[dict[str, Any]] = []
        retry_updates: list[dict[str, Any]] = []
        executable_signatures: set[str] = set()

        for task, signature, task_key in entries:
            existing = existing_map.get(task_key)
            if existing is None:
                new_rows.append(
                    {
                        "task_key": task_key,
                        "mode": "historical_backfill",
                        "code": task.code,
                        "freq": task.freq,
                        "start_date": task.start_time[:10],
                        "end_date": task.end_time[:10],
                        "attempt_count": 0,
                        "last_status": "pending",
                        "last_error": None,
                    }
                )
                executable.append(task)
                executable_signatures.add(signature)
                continue

            attempt_count = int(existing.get("attempt_count") or 0)
            if attempt_count >= 2:
                skipped += 1
                continue

            retry_updates.append(
                {
                    "task_key": task_key,
                    "attempt_count": attempt_count + 1,
                    "last_status": "pending",
                    "last_error": None,
                }
            )
            executable.append(task)
            executable_signatures.add(signature)

        t3 = time.monotonic()
        self.logger.info(
            "retry 准备：分类处理完成",
            {
                "new_count": len(new_rows),
                "retry_count": len(retry_updates),
                "skipped": skipped,
                "duration_seconds": round(t3 - t2, 4),
            },
        )

        if retry_updates:
            self.state_db.update_maintenance_retry_tasks(retry_updates)
        t4 = time.monotonic()

        if new_rows:
            self.state_db.insert_maintenance_retry_tasks(new_rows)
        t5 = time.monotonic()

        self.logger.info(
            "retry 准备：写入完成",
            {
                "update_duration_seconds": round(t4 - t3, 4),
                "insert_duration_seconds": round(t5 - t4, 4),
                "total_duration_seconds": round(t5 - t0, 4),
            },
        )

        self.logger.debug_lazy(
            "历史维护 retry 明细",
            lambda: {
                "raw_task_count": len(tasks),
                "executable_task_count": len(executable),
                "retry_skipped_tasks": skipped,
                "new_retry_rows": len(new_rows),
                "retry_update_rows": len(retry_updates),
                "task_sample": [task.to_api_payload() for task in executable[:10]],
            },
        )

        filtered_key_map = {
            signature: task_key
            for signature, task_key in task_key_by_signature.items()
            if signature in executable_signatures
        }
        return executable, filtered_key_map, skipped

    def _build_preprocessor_operator(self) -> Callable[[dict[str, Any]], dict[str, Any] | None]:
        """
        输入：
        1. 无。
        输出：
        1. simple_api 预处理函数。
        用途：
        1. 执行统一字段清洗规则。
        边界条件：
        1. 非 data 事件原样透传。
        """

        def _operator(payload: dict[str, Any]) -> dict[str, Any] | None:
            if not isinstance(payload, dict):
                return None
            event = str(payload.get("event") or "").strip().lower()
            if event != "data":
                return payload

            task = payload.get("task") if isinstance(payload.get("task"), dict) else {}
            freq = str(task.get("freq") or "").strip().lower()
            rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
            cleaned_rows: list[dict[str, Any]] = []

            for raw in rows:
                if not isinstance(raw, dict):
                    continue
                row = dict(raw)
                for key in ("year", "month", "day", "hour", "minute"):
                    row.pop(key, None)
                row["open"] = _price_2(row.get("open"))
                row["high"] = _price_2(row.get("high"))
                row["low"] = _price_2(row.get("low"))
                row["close"] = _price_2(row.get("close"))
                if "vol" in row:
                    if "volume" not in row:
                        row["volume"] = row.get("vol")
                    row.pop("vol", None)
                row["volume"] = _trunc_int(row.get("volume"))
                row["amount"] = _trunc_int(row.get("amount"))
                dt = _coerce_datetime(row.get("datetime"))
                if dt is None:
                    continue
                if freq in {"d", "w"}:
                    dt = dt.replace(hour=15, minute=0, second=0, microsecond=0)
                row["datetime"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                cleaned_rows.append(row)

            payload["rows"] = cleaned_rows
            return payload

        return _operator

    def _normalize_event_task(self, payload_task: dict[str, Any]) -> MaintenanceTask | None:
        """
        输入：
        1. payload_task: simple_api 事件中的 task 字典。
        输出：
        1. 标准任务对象。
        用途：
        1. 从队列事件回推任务签名。
        边界条件：
        1. 字段异常时返回 None。
        """

        if not isinstance(payload_task, dict):
            return None
        return self._make_task(
            code=payload_task.get("code"),
            freq=str(payload_task.get("freq") or ""),
            start_time=str(payload_task.get("start_time") or ""),
            end_time=str(payload_task.get("end_time") or ""),
            date_only=False if ":" in str(payload_task.get("start_time") or "") else True,
        )

    def _flush_freq_rows(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        freq: str,
        rows: list[dict[str, Any]],
    ) -> int:
        """
        输入：
        1. con: 写连接。
        2. freq: 周期。
        3. rows: 待写入行列表。
        输出：
        1. 实际写入行数。
        用途：
        1. 批量 upsert 单周期行数据。
        边界条件：
        1. 空数据直接返回 0。
        """

        if not rows:
            return 0

        table = KLINE_TABLE_BY_FREQ[freq]
        frame = pd.DataFrame.from_records(rows)
        if frame.empty:
            return 0

        required_cols = ["code", "datetime", "open", "high", "low", "close", "volume", "amount"]
        for col in required_cols:
            if col not in frame.columns:
                frame[col] = 0 if col in {"open", "high", "low", "close", "volume", "amount"} else ""
        frame = frame[required_cols]
        frame["code"] = frame["code"].map(normalize_code_for_storage)
        frame = frame[frame["code"] != ""]
        if frame.empty:
            return 0

        frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
        frame = frame.dropna(subset=["datetime"])
        if frame.empty:
            return 0
        if freq in {"d", "w"}:
            frame["datetime"] = frame["datetime"].dt.floor("D") + pd.Timedelta(hours=15)

        frame["open"] = frame["open"].astype(float)
        frame["high"] = frame["high"].astype(float)
        frame["low"] = frame["low"].astype(float)
        frame["close"] = frame["close"].astype(float)
        frame["volume"] = frame["volume"].astype("int64")
        frame["amount"] = frame["amount"].astype("int64")
        frame = frame.drop_duplicates(subset=["code", "datetime"], keep="last")
        if frame.empty:
            return 0

        self._temp_index += 1
        temp_name = f"_tmp_maintenance_rows_{freq}_{self._temp_index}"
        con.register(temp_name, frame)
        try:
            if freq in {"15", "30", "60"}:
                # 分钟级：按 (code, 日期) 删除当日全部旧行后插入
                con.execute(
                    f"""
                    delete from {table} as t
                    using (
                        select distinct code, cast(datetime as date) as dt
                        from {temp_name}
                    ) as s
                    where t.code = s.code
                      and cast(t.datetime as date) = s.dt
                    """
                )
            elif freq == "w":
                # 周线：按 (code, 自然周) 删除整周旧行后插入
                con.execute(
                    f"""
                    delete from {table} as t
                    using (
                        select distinct code, date_trunc('week', datetime) as wk
                        from {temp_name}
                    ) as s
                    where t.code = s.code
                      and date_trunc('week', t.datetime) = s.wk
                    """
                )
            else:
                # 日线：精确匹配 (code, datetime) 删除
                con.execute(
                    f"""
                    delete from {table} as t
                    using {temp_name} as s
                    where t.code = s.code
                      and t.datetime = s.datetime
                    """
                )
            con.execute(
                f"""
                insert into {table} (code, datetime, open, high, low, close, volume, amount)
                select code, datetime, open, high, low, close, volume, amount
                from {temp_name}
                """
            )
        finally:
            con.unregister(temp_name)
        return int(len(frame))

    def _flush_all_buffers(
        self,
        con: duckdb.DuckDBPyConnection,
        buffers: dict[str, list[dict[str, Any]]],
        rows_written_by_freq: dict[str, int],
    ) -> None:
        """
        输入：
        1. con: 写连接。
        2. buffers: 周期缓存行。
        3. rows_written_by_freq: 写入计数字典。
        输出：
        1. 无返回值。
        用途：
        1. 批量刷盘所有周期缓存。
        边界条件：
        1. 缓存为空时无操作。
        """

        for freq in KLINE_FREQ_ORDER:
            rows = buffers.get(freq) or []
            if not rows:
                continue
            flush_started = time.monotonic()
            buffered_count = len(rows)
            written = self._flush_freq_rows(con, freq=freq, rows=rows)
            rows_written_by_freq[freq] += written
            self.logger.debug_lazy(
                "批量刷盘明细",
                lambda _f=freq, _bc=buffered_count, _w=written, _cw=rows_written_by_freq[freq], _fs=flush_started: {
                    "freq": _f,
                    "buffered_rows": _bc,
                    "written_rows": _w,
                    "cumulative_written_rows": _cw,
                    "duration_seconds": round(time.monotonic() - _fs, 4),
                },
            )
            buffers[freq] = []

    def _append_event_rows_to_buffers(
        self,
        *,
        task: MaintenanceTask,
        rows: list[dict[str, Any]],
        buffers: dict[str, list[dict[str, Any]]],
    ) -> int:
        """
        输入：
        1. task: 当前任务。
        2. rows: 事件返回行。
        3. buffers: 周期缓存。
        输出：
        1. 实际追加到缓存的行数。
        用途：
        1. 队列消费阶段将事件行合并进批量缓冲区。
        边界条件：
        1. 非法代码或时间会被过滤。
        """

        freq = task.freq
        bucket = buffers.setdefault(freq, [])
        append_count = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            code = normalize_code_for_storage(row.get("code") or task.code)
            if not code:
                continue
            if row.get("datetime") in (None, ""):
                continue
            bucket.append(
                {
                    "code": code,
                    "datetime": row.get("datetime"),
                    "open": _price_2(row.get("open")),
                    "high": _price_2(row.get("high")),
                    "low": _price_2(row.get("low")),
                    "close": _price_2(row.get("close")),
                    "volume": _trunc_int(row.get("volume")),
                    "amount": _trunc_int(row.get("amount")),
                }
            )
            append_count += 1
        return append_count

    def _execute_fetch_and_write(
        self,
        *,
        mode: str,
        tasks: list[MaintenanceTask],
        progress_start: float,
        progress_end: float,
    ) -> FetchWriteStats:
        """
        输入：
        1. mode: 运行模式。
        2. tasks: 待抓取任务列表。
        3. progress_start: 进度区间起点。
        4. progress_end: 进度区间终点。
        输出：
        1. 抓取落库统计对象。
        用途：
        1. 执行步骤6-8（latest）或步骤5-7（historical）。
        边界条件：
        1. 失败任务最多重试到 `_TASK_RETRY_ROUNDS`。
        """

        if not tasks:
            return FetchWriteStats(
                total_tasks=0,
                success_tasks=0,
                failed_tasks=0,
                retry_rounds_used=0,
                rows_written_by_freq={freq: 0 for freq in KLINE_FREQ_ORDER},
                success_signatures=set(),
                failed_signatures=set(),
                failed_errors={},
            )

        self._require_simple_api()
        task_map = {task.signature(): task for task in tasks}
        pending_signatures: list[str] = [task.signature() for task in tasks]
        success_signatures: set[str] = set()
        no_data_signatures: set[str] = set()
        failed_errors: dict[str, str] = {}
        rows_written_by_freq: dict[str, int] = {freq: 0 for freq in KLINE_FREQ_ORDER}
        retry_rounds_used = 0
        cumulative_no_data_count = 0
        cumulative_processed = 0
        original_total = len(tasks)
        preprocessor = self._build_preprocessor_operator()

        for round_index in range(_TASK_RETRY_ROUNDS):
            self._check_stop()
            current_signatures = [sign for sign in pending_signatures if sign not in success_signatures]
            if not current_signatures:
                break
            retry_rounds_used = round_index + 1
            current_tasks = [task_map[sign] for sign in current_signatures]
            round_started = time.monotonic()
            round_rows_appended = 0
            self.logger.info(
                "拉取与写入开始",
                {
                    "mode": mode,
                    "round": retry_rounds_used,
                    "max_rounds": _TASK_RETRY_ROUNDS,
                    "task_count": len(current_tasks),
                },
            )
            self.logger.debug_lazy(
                "拉取请求参数",
                lambda: {
                    "mode": mode,
                    "round": retry_rounds_used,
                    "task_count": len(current_tasks),
                    "task_sample_head": [task.to_api_payload() for task in current_tasks[:20]],
                    "task_sample_tail": [task.to_api_payload() for task in current_tasks[-20:]],
                },
            )

            buffers: dict[str, list[dict[str, Any]]] = {freq: [] for freq in KLINE_FREQ_ORDER}
            round_failures: set[str] = set()
            round_error_samples: dict[str, int] = {}
            processed_count = 0
            no_data_count = 0
            success_count = 0
            total_count = len(current_tasks)
            pool_broken = False

            with self._connect_source_db(read_only=False) as con:
                try:
                    with get_tdx_client() as _client:
                        with managed_stock_kline_job(
                            task=[task.to_api_payload() for task in current_tasks],
                            mode="async",
                            queue=None,
                            preprocessor_operator=preprocessor,
                        ) as job:
                            while True:
                                self._check_stop()
                                try:
                                    event = job.queue.get(timeout=_QUEUE_POLL_TIMEOUT_SECONDS)
                                except queue.Empty:
                                    if job.done():
                                        break
                                    continue

                                if not isinstance(event, dict):
                                    continue
                                event_type = str(event.get("event") or "").strip().lower()
                                if event_type == "done":
                                    break
                                if event_type != "data":
                                    continue

                                event_task = self._normalize_event_task(event.get("task") if isinstance(event.get("task"), dict) else {})
                                if event_task is None:
                                    continue
                                sign = event_task.signature()
                                if sign not in task_map:
                                    continue
                                processed_count += 1
                                cumulative_processed += 1

                                error_text = str(event.get("error") or "").strip()
                                if error_text:
                                    if error_text == "no_data":
                                        no_data_count += 1
                                        success_signatures.add(sign)
                                        no_data_signatures.add(sign)
                                        round_failures.discard(sign)
                                    else:
                                        round_failures.add(sign)
                                        failed_errors[sign] = error_text
                                        round_error_samples[error_text] = round_error_samples.get(error_text, 0) + 1
                                        self.logger.error_file_lazy(
                                            "拉取任务失败",
                                            {
                                                "mode": mode,
                                                "round": retry_rounds_used,
                                                "task": sign,
                                                "error": error_text,
                                            },
                                        )
                                else:
                                    rows = event.get("rows") if isinstance(event.get("rows"), list) else []
                                    round_rows_appended += self._append_event_rows_to_buffers(
                                        task=event_task,
                                        rows=rows,
                                        buffers=buffers,
                                    )
                                    success_signatures.add(sign)
                                    success_count += 1
                                    round_failures.discard(sign)

                                need_flush = any(len(buffers[freq]) >= _WRITE_FLUSH_ROWS for freq in KLINE_FREQ_ORDER)
                                if need_flush:
                                    self._flush_all_buffers(con, buffers, rows_written_by_freq)

                                ratio = float(cumulative_processed) / float(max(1, original_total))
                                progress = progress_start + (progress_end - progress_start) * ratio
                                self._report_progress(
                                    progress,
                                    phase="running",
                                    detail={
                                        "mode": mode,
                                        "round": retry_rounds_used,
                                        "max_rounds": _TASK_RETRY_ROUNDS,
                                        "processed": processed_count,
                                        "total": total_count,
                                        "success": success_count,
                                        "no_data": no_data_count,
                                        "failed": len(round_failures),
                                        "rows_appended": round_rows_appended,
                                        "rows_written": dict(rows_written_by_freq),
                                    },
                                )

                            self._flush_all_buffers(con, buffers, rows_written_by_freq)
                            con.commit()
                            job.result()
                except BrokenProcessPool as exc:
                    pool_broken = True
                    self.logger.warning(
                        "并行进程池异常中断，标记未完成任务为失败",
                        {
                            "error": f"{type(exc).__name__}: {exc}",
                            "processed": processed_count,
                            "total": total_count,
                        },
                    )
                    try:
                        self._flush_all_buffers(con, buffers, rows_written_by_freq)
                        con.commit()
                    except Exception:
                        pass

            if pool_broken:
                for sign in current_signatures:
                    if sign not in success_signatures:
                        round_failures.add(sign)
                        error_text = failed_errors.setdefault(sign, "BrokenProcessPool: 子进程异常中断")
                        self.logger.error_file_lazy(
                            "拉取任务失败",
                            {
                                "mode": mode,
                                "round": retry_rounds_used,
                                "task": sign,
                                "error": error_text,
                            },
                        )
                try:
                    from zsdtdx.parallel_fetcher import force_restart_parallel_fetcher
                    summary = force_restart_parallel_fetcher(
                        prewarm=True,
                        prewarm_timeout_seconds=60.0,
                        max_rounds=3,
                    )
                    self.logger.info("并行进程池重建完成", summary if isinstance(summary, dict) else None)
                except Exception as rebuild_exc:
                    self.logger.error(
                        "并行进程池重建失败",
                        {"error": f"{type(rebuild_exc).__name__}: {rebuild_exc}"},
                    )

            pending_signatures = sorted(
                sign
                for sign in round_failures
                if sign not in success_signatures
            )
            self.logger.info(
                "拉取与写入轮次结束",
                {
                    "mode": mode,
                    "round": retry_rounds_used,
                    "processed": processed_count,
                    "total": total_count,
                    "success": success_count,
                    "no_data": no_data_count,
                    "failed": len(pending_signatures),
                    "rows_appended": round_rows_appended,
                    "error_samples": dict(list(round_error_samples.items())[:10]) if round_error_samples else None,
                },
            )
            cumulative_no_data_count += no_data_count
            self.logger.debug_lazy(
                "拉取与写入轮次明细",
                lambda: {
                    "mode": mode,
                    "round": retry_rounds_used,
                    "task_count": len(current_tasks),
                    "processed": processed_count,
                    "failed_task_count": len(pending_signatures),
                    "round_rows_appended": round_rows_appended,
                    "success_count": len(success_signatures),
                    "failed_task_sample": pending_signatures[:10],
                    "rows_written_by_freq": dict(rows_written_by_freq),
                    "duration_seconds": round(time.monotonic() - round_started, 4),
                },
            )
            if pending_signatures and round_index + 1 < _TASK_RETRY_ROUNDS:
                self.logger.warning(
                    "检测到失败任务，准备进入重试轮次",
                    {
                        "mode": mode,
                        "next_round": round_index + 2,
                        "failed_task_count": len(pending_signatures),
                    },
                )

        all_signatures = set(task_map.keys())
        final_failed = {sign for sign in all_signatures if sign not in success_signatures}
        success_tasks = len(all_signatures) - len(final_failed)
        return FetchWriteStats(
            total_tasks=len(all_signatures),
            success_tasks=success_tasks,
            no_data_tasks=cumulative_no_data_count,
            failed_tasks=len(final_failed),
            retry_rounds_used=retry_rounds_used,
            rows_written_by_freq=rows_written_by_freq,
            success_signatures=success_signatures,
            no_data_signatures=no_data_signatures,
            failed_signatures=final_failed,
            failed_errors=failed_errors,
        )

    def _run_latest_update(self) -> MaintenanceRunSummary:
        """
        输入：
        1. 无。
        输出：
        1. latest_update 模式摘要。
        用途：
        1. 执行最新数据更新步骤0-8。
        边界条件：
        1. 任意步骤异常会中断并上抛。
        """

        steps_total = 9
        steps_completed = 0
        mode = "latest_update"
        run_started = time.monotonic()
        rows_written = {freq: 0 for freq in KLINE_FREQ_ORDER}

        self._check_stop()
        self.ensure_runtime_schema()

        self.logger.info("步骤0开始：刷新股票代码全集并重写 stocks")
        stock_map = self._refresh_stocks_from_tdx()
        all_codes = normalize_code_set(stock_map.keys())
        steps_completed = 1
        self.logger.info("步骤0完成", {"stock_count": len(all_codes)})
        self.logger.debug_lazy("步骤0明细", lambda: {"stocks_preview": list(sorted(all_codes))[:20], "stock_count": len(all_codes)})
        self._report_progress(10.0, phase="prepare")

        self.logger.info("步骤1开始：计算本周周五日期")
        _, friday = week_monday_and_friday(date.today())
        steps_completed = 2
        self.logger.info("步骤1完成", {"friday": friday.isoformat()})
        self._report_progress(20.0, phase="prepare")

        self.logger.info("步骤2开始：读取各周期最新 datetime 并回退起点")
        latest_by_freq = self._load_latest_by_freq()
        latest_count_by_freq = {freq: len(latest_by_freq.get(freq, {})) for freq in KLINE_FREQ_ORDER}
        steps_completed = 3
        self.logger.info("步骤2完成", {"latest_count_by_freq": latest_count_by_freq})
        self.logger.debug_lazy("步骤2明细", lambda: {"latest_count_by_freq": latest_count_by_freq})
        self._report_progress(30.0, phase="prepare")

        self.logger.info("步骤3开始：设置统一 end_time")
        end_time = datetime.combine(friday, dt_time(15, 0, 0))
        steps_completed = 4
        self.logger.info("步骤3完成", {"end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")})
        self._report_progress(40.0, phase="prepare")

        self.logger.info("步骤4开始：构建基础任务列表")
        tasks = self._build_latest_tasks(
            latest_by_freq=latest_by_freq,
            end_time=end_time,
            all_codes=all_codes,
        )
        steps_completed = 5
        self.logger.info("步骤4完成", {"task_count": len(tasks)})
        self._report_progress(50.0, phase="prepare")

        self.logger.info("步骤5开始：按代码差集补齐新增任务")
        steps_completed = 6
        self.logger.info("步骤5完成", {"task_count_after_patch": len(tasks)})
        self._report_progress(60.0, phase="prepare")

        self.logger.info("步骤6-8开始：异步抓取、消费队列落库与失败重试")
        fetch_stats = self._execute_fetch_and_write(
            mode=mode,
            tasks=tasks,
            progress_start=60.0,
            progress_end=95.0,
        )
        rows_written = dict(fetch_stats.rows_written_by_freq)
        steps_completed = 9
        self.logger.info(
            "步骤6-8完成",
            {
                "total_tasks": fetch_stats.total_tasks,
                "success_tasks": fetch_stats.success_tasks,
                "no_data_tasks": fetch_stats.no_data_tasks,
                "failed_tasks": fetch_stats.failed_tasks,
                "retry_rounds_used": fetch_stats.retry_rounds_used,
                "rows_written": rows_written,
            },
        )
        if fetch_stats.failed_tasks > 0:
            sample_errors = []
            for sign in sorted(fetch_stats.failed_signatures)[:10]:
                sample_errors.append(
                    {
                        "task": sign,
                        "error": fetch_stats.failed_errors.get(sign, "unknown_error"),
                    }
                )
            self.logger.warning("存在最终失败任务", {"failed_task_count": fetch_stats.failed_tasks, "sample": sample_errors})
        self._log_engine_runtime_diagnostics()
        self._report_progress(100.0, phase="done")

        duration = round(time.monotonic() - run_started, 3)
        return MaintenanceRunSummary(
            mode=mode,
            steps_total=steps_total,
            steps_completed=steps_completed,
            total_tasks=fetch_stats.total_tasks,
            success_tasks=fetch_stats.success_tasks,
            no_data_tasks=fetch_stats.no_data_tasks,
            failed_tasks=fetch_stats.failed_tasks,
            retry_rounds_used=fetch_stats.retry_rounds_used,
            rows_written=rows_written,
            retry_skipped_tasks=0,
            removed_corrupted_rows=0,
            duration_seconds=duration,
        )

    def _run_historical_backfill(self) -> MaintenanceRunSummary:
        """
        输入：
        1. 无。
        输出：
        1. historical_backfill 模式摘要。
        用途：
        1. 执行历史维护步骤1-7。
        边界条件：
        1. 任务去重与重试次数受 maintenance_retry_tasks 控制。
        """

        steps_total = 7
        steps_completed = 0
        mode = "historical_backfill"
        run_started = time.monotonic()
        rows_written = {freq: 0 for freq in KLINE_FREQ_ORDER}
        removed_corrupted_rows = 0

        self._check_stop()
        self.ensure_runtime_schema()

        self.logger.info("步骤1开始：扫描 d 周期缺口并扩展 d/60/30/15 任务")
        step_started = time.monotonic()
        tasks_step1 = self._build_historical_gap_tasks_from_daily()
        steps_completed = 1
        self.logger.info("步骤1完成", {"task_count": len(tasks_step1)})
        self.logger.debug_lazy(
            "步骤1明细",
            lambda: {
                "task_count": len(tasks_step1),
                "task_sample": [task.to_api_payload() for task in tasks_step1[:10]],
                "duration_seconds": round(time.monotonic() - step_started, 4),
            },
        )
        self._report_progress(15.0, phase="prepare")

        self.logger.info("步骤2开始：检查 60/30/15 每日条数并标记异常日回补")
        step_started = time.monotonic()
        tasks_step2, removed_corrupted_rows = self._build_historical_minute_count_tasks()
        steps_completed = 2
        self.logger.info(
            "步骤2完成",
            {
                "task_count": len(tasks_step2),
                "corrupted_existing_rows": removed_corrupted_rows,
            },
        )
        self.logger.debug_lazy(
            "步骤2明细",
            lambda: {
                "task_count": len(tasks_step2),
                "corrupted_existing_rows": removed_corrupted_rows,
                "task_sample": [task.to_api_payload() for task in tasks_step2[:10]],
                "duration_seconds": round(time.monotonic() - step_started, 4),
            },
        )
        self._report_progress(30.0, phase="prepare")

        self.logger.info("步骤3开始：扫描 w 周期缺口窗口")
        step_started = time.monotonic()
        tasks_step3 = self._build_historical_weekly_gap_tasks()
        steps_completed = 3
        self.logger.info("步骤3完成", {"task_count": len(tasks_step3)})
        self.logger.debug_lazy(
            "步骤3明细",
            lambda: {
                "task_count": len(tasks_step3),
                "task_sample": [task.to_api_payload() for task in tasks_step3[:10]],
                "duration_seconds": round(time.monotonic() - step_started, 4),
            },
        )
        self._report_progress(45.0, phase="prepare")

        self.logger.info("步骤4开始：维护 retry 任务表与尝试次数")
        step_started = time.monotonic()
        merged_tasks = self._dedupe_tasks([*tasks_step1, *tasks_step2, *tasks_step3])
        executable_tasks, task_key_by_signature, retry_skipped = self._prepare_historical_retry_tasks(merged_tasks)
        steps_completed = 4
        self.logger.info(
            "步骤4完成",
            {
                "merged_task_count": len(merged_tasks),
                "executable_task_count": len(executable_tasks),
                "retry_skipped_tasks": retry_skipped,
            },
        )
        self.logger.debug_lazy(
            "步骤4明细",
            lambda: {
                "merged_task_count": len(merged_tasks),
                "executable_task_count": len(executable_tasks),
                "retry_skipped_tasks": retry_skipped,
                "merged_task_sample": [task.to_api_payload() for task in merged_tasks[:10]],
                "executable_task_sample": [task.to_api_payload() for task in executable_tasks[:10]],
                "duration_seconds": round(time.monotonic() - step_started, 4),
            },
        )
        self._report_progress(55.0, phase="prepare")

        self.logger.info("步骤5-7开始：异步抓取、消费队列落库与失败重试")
        fetch_stats = self._execute_fetch_and_write(
            mode=mode,
            tasks=executable_tasks,
            progress_start=55.0,
            progress_end=95.0,
        )
        rows_written = dict(fetch_stats.rows_written_by_freq)

        if self.state_db is not None and task_key_by_signature:
            cleanup_started = time.monotonic()
            # 1. success（非 no_data）：标记 success 并将 attempt_count 封顶，避免后续重复执行。
            real_success_signs = fetch_stats.success_signatures - fetch_stats.no_data_signatures
            success_task_keys = [
                task_key_by_signature[sign]
                for sign in real_success_signs
                if sign in task_key_by_signature
            ]
            success_seal_attempt = _TASK_RETRY_ROUNDS
            if success_task_keys:
                self.state_db.update_maintenance_retry_tasks(
                    [
                        {
                            "task_key": task_key,
                            "attempt_count": success_seal_attempt,
                            "last_status": "success",
                            "last_error": None,
                        }
                        for task_key in success_task_keys
                    ]
                )
            self._report_progress(96.0, phase="cleanup")

            # 2. no_data：批量设 last_status/last_error，不改 attempt_count（pre-execution 已 +1）
            no_data_task_keys = [
                task_key_by_signature[sign]
                for sign in fetch_stats.no_data_signatures
                if sign in task_key_by_signature
            ]
            if no_data_task_keys:
                self.state_db.bulk_update_retry_task_status(
                    no_data_task_keys,
                    last_status="no_data",
                    last_error="no_data",
                    batch_size=50_000,
                    on_batch=lambda done, total: self._report_progress(
                        96.0 + 3.0 * float(done) / float(max(1, total)),
                        phase="cleanup",
                    ),
                )

            # 3. failed：per-row 更新（体量极小，不改 attempt_count）
            failed_updates: list[dict[str, Any]] = []
            for sign in fetch_stats.failed_signatures:
                task_key = task_key_by_signature.get(sign)
                if not task_key:
                    continue
                failed_updates.append(
                    {
                        "task_key": task_key,
                        "last_status": "failed",
                        "last_error": fetch_stats.failed_errors.get(sign, "unknown_error"),
                    }
                )
            if failed_updates:
                self.state_db.update_maintenance_retry_tasks(failed_updates)
            self._report_progress(99.0, phase="cleanup")

            self.logger.info(
                "retry 回写完成",
                {
                    "success_update_count": len(success_task_keys),
                    "success_seal_attempt": success_seal_attempt,
                    "no_data_update_count": len(no_data_task_keys),
                    "failed_update_count": len(failed_updates),
                    "duration_seconds": round(time.monotonic() - cleanup_started, 3),
                },
            )

        steps_completed = 7
        self.logger.info(
            "步骤5-7完成",
            {
                "total_tasks": fetch_stats.total_tasks,
                "success_tasks": fetch_stats.success_tasks,
                "no_data_tasks": fetch_stats.no_data_tasks,
                "failed_tasks": fetch_stats.failed_tasks,
                "retry_rounds_used": fetch_stats.retry_rounds_used,
                "rows_written": rows_written,
            },
        )
        if fetch_stats.failed_tasks > 0:
            self.logger.warning(
                "存在最终失败任务",
                {
                    "failed_task_count": fetch_stats.failed_tasks,
                    "failed_sample": list(sorted(fetch_stats.failed_signatures))[:10],
                },
            )
        self._log_engine_runtime_diagnostics()
        self._report_progress(100.0, phase="done")

        duration = round(time.monotonic() - run_started, 3)
        return MaintenanceRunSummary(
            mode=mode,
            steps_total=steps_total,
            steps_completed=steps_completed,
            total_tasks=fetch_stats.total_tasks,
            success_tasks=fetch_stats.success_tasks,
            no_data_tasks=fetch_stats.no_data_tasks,
            failed_tasks=fetch_stats.failed_tasks,
            retry_rounds_used=fetch_stats.retry_rounds_used,
            rows_written=rows_written,
            retry_skipped_tasks=retry_skipped,
            removed_corrupted_rows=removed_corrupted_rows,
            duration_seconds=duration,
        )

    def run_update(self, mode: str = "latest_update") -> MaintenanceRunSummary:
        """
        输入：
        1. mode: 维护模式，支持 latest_update / historical_backfill。
        输出：
        1. 维护任务摘要。
        用途：
        1. 对外统一维护任务执行入口。
        边界条件：
        1. 非法模式抛 ValueError。
        """

        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode not in {"latest_update", "historical_backfill"}:
            raise ValueError(f"非法维护模式: {mode}")
        if normalized_mode == "latest_update":
            return self._run_latest_update()
        return self._run_historical_backfill()
