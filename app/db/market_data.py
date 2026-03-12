"""
行情数据访问层（MarketDataDB）。

职责：
1. 连接源 DuckDB，读取股票列表与多周期 OHLCV 数据。
2. 根据用户输入解析股票代码/名称，并返回可执行股票集合。
3. 在任务执行前进行“时间区间完整性过滤”，排除数据缺失的股票。
4. 提供图表查询接口，为前端结果页返回可视化 K 线数据。

设计约束：
1. 只负责数据读取与基础数据清洗，不承担策略判断。
2. 连接失败时支持影子库（shadow copy）兜底，降低源库文件锁冲突。
"""

from __future__ import annotations

import shutil
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from app.db.duckdb_utils import connect_duckdb_compatible
from app.services.concept_formula import ConceptFormula
from app.settings import TIMEFRAME_TO_TABLE


@dataclass
class StockResolution:
    codes: list[str]
    unresolved: list[str]
    code_to_name: dict[str, str]


@dataclass
class CoverageFilterResult:
    eligible_codes: list[str]
    excluded_codes: list[str]
    excluded_by_timeframe: dict[str, list[str]]
    expected_bars_by_timeframe: dict[str, int]


class MarketDataReadSession:
    """
    行情库只读会话。

    职责：
    1. 在单次任务或单次复合查询中复用同一条 DuckDB 只读连接。
    2. 复用 `stocks`、概念与 K 线读取逻辑，减少重复建连成本。

    设计约束：
    1. 会话对象仅在创建它的线程中使用，不做跨线程共享。
    2. 生命周期结束后必须关闭连接；关闭后的会话不可再次复用。
    """

    def __init__(self, owner: "MarketDataDB", connection: duckdb.DuckDBPyConnection):
        """
        输入：
        1. owner: 行情库访问对象。
        2. connection: 已建立的只读 DuckDB 连接。
        输出：
        1. 无返回值。
        用途：
        1. 绑定会话所属的数据库对象与底层连接。
        边界条件：
        1. 连接关闭后会话失效，后续访问会抛出运行时错误。
        """
        self._owner = owner
        self._connection: duckdb.DuckDBPyConnection | None = connection

    def __enter__(self) -> "MarketDataReadSession":
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回当前会话对象。
        用途：
        1. 支持 `with` 上下文形式管理连接生命周期。
        边界条件：
        1. 已关闭会话再次进入不会自动重连。
        """
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """
        输入：
        1. exc_type/exc/tb: 上下文退出时的异常信息。
        输出：
        1. 无返回值。
        用途：
        1. 在上下文退出时关闭底层连接。
        边界条件：
        1. 关闭异常会被吞掉，避免掩盖原始业务异常。
        """
        self.close()

    def _require_connection(self) -> duckdb.DuckDBPyConnection:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回当前可用 DuckDB 连接。
        用途：
        1. 统一校验会话是否仍处于可用状态。
        边界条件：
        1. 已关闭会话会抛出运行时错误，避免调用方误以为仍在复用连接。
        """
        if self._connection is None:
            raise RuntimeError("行情只读会话已关闭，无法继续访问")
        return self._connection

    def close(self) -> None:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 无返回值。
        用途：
        1. 显式关闭会话底层连接。
        边界条件：
        1. 重复关闭是幂等操作。
        """
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        try:
            connection.close()
        except Exception:
            pass

    def get_all_stocks(self) -> list[tuple[str, str]]:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回全量 `(code, name)` 列表。
        用途：
        1. 在同一会话中复用 `stocks` 表读取结果。
        边界条件：
        1. 会话关闭后不可调用。
        """
        return self._owner._fetch_stock_rows(con=self._require_connection())

    def get_stock_concepts_by_codes(self, codes: list[str]) -> dict[str, list[dict[str, Any]]]:
        """
        输入：
        1. codes: 股票代码列表。
        输出：
        1. `code -> 概念明细列表` 映射。
        用途：
        1. 在同一连接内批量读取股票概念信息。
        边界条件：
        1. 空输入直接返回空映射。
        """
        return self._owner._get_stock_concepts_by_codes(codes, con=self._require_connection())

    def filter_codes_by_concept_formula(
        self,
        *,
        codes: list[str],
        formula: ConceptFormula,
    ) -> tuple[list[str], dict[str, Any]]:
        """
        输入：
        1. codes: 待过滤股票代码列表。
        2. formula: 概念筛选公式。
        输出：
        1. 过滤后的股票列表和统计信息。
        用途：
        1. 在同一会话内完成概念预筛选，减少额外建连。
        边界条件：
        1. 公式未启用时直接返回原列表。
        """
        return self._owner._filter_codes_by_concept_formula(
            codes=codes,
            formula=formula,
            con=self._require_connection(),
        )

    def resolve_stock_inputs(
        self,
        raw_inputs: list[str],
        stock_rows: list[tuple[str, str]] | None = None,
    ) -> StockResolution:
        """
        输入：
        1. raw_inputs: 原始股票输入，可为代码或名称。
        2. stock_rows: 可选股票快照。
        输出：
        1. 股票解析结果。
        用途：
        1. 在同一会话中解析股票输入，避免额外连接创建。
        边界条件：
        1. 未提供 `stock_rows` 时会复用会话连接读取 `stocks` 表。
        """
        effective_rows = stock_rows if stock_rows is not None else self.get_all_stocks()
        return self._owner.resolve_stock_inputs(raw_inputs, stock_rows=effective_rows)

    def filter_codes_with_complete_range(
        self,
        *,
        codes: list[str],
        start_ts: datetime,
        end_ts: datetime,
        required_timeframes: list[str],
    ) -> CoverageFilterResult:
        """
        输入：
        1. codes: 待过滤股票代码列表。
        2. start_ts/end_ts: 校验时间窗口。
        3. required_timeframes: 必须完整的周期列表。
        输出：
        1. 完整性过滤结果。
        用途：
        1. 在同一只读连接中执行时间区间完整性过滤。
        边界条件：
        1. 空输入或无周期要求时保持原有快速返回语义。
        """
        return self._owner._filter_codes_with_complete_range(
            codes=codes,
            start_ts=start_ts,
            end_ts=end_ts,
            required_timeframes=required_timeframes,
            con=self._require_connection(),
        )

    def fetch_ohlcv(
        self,
        *,
        code: str,
        timeframe: str,
        start_ts: datetime | None,
        end_ts: datetime | None,
    ) -> pd.DataFrame:
        """
        输入：
        1. code: 股票代码。
        2. timeframe: 周期。
        3. start_ts/end_ts: 可选时间窗口。
        输出：
        1. 对应周期的 OHLCV DataFrame。
        用途：
        1. 在单任务内复用同一连接读取多周期 K 线。
        边界条件：
        1. 空结果保持原有空 DataFrame 语义。
        """
        return self._owner._fetch_ohlcv(
            code=code,
            timeframe=timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            con=self._require_connection(),
        )


class MarketDataDB:
    _shadow_lock = threading.Lock()

    def __init__(self, source_db_path: Path | str):
        """
        输入：
        1. source_db_path: 源行情库 DuckDB 路径。
        输出：
        1. 无返回值。
        用途：
        1. 初始化源库路径与影子库副本路径。
        边界条件：
        1. 影子库固定落到系统临时目录，用于连接失败时的只读兜底。
        """
        self.source_db_path = Path(source_db_path)
        self._shadow_db_path = Path(tempfile.gettempdir()) / f"{self.source_db_path.stem}_shadow.duckdb"

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """
        连接源行情库。

        连接策略：
        1. 优先只读连接源库。
        2. 遇到文件锁冲突时，允许通过 shadow copy 回退，降低前台查询被后台写入阻塞的概率。
        """
        return connect_duckdb_compatible(
            self.source_db_path,
            read_only=True,
            retries=20,
            retry_sleep_seconds=0.05,
            shadow_copy_factory=self._ensure_shadow_copy,
        )

    def open_read_session(self) -> MarketDataReadSession:
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回新的行情只读会话对象。
        用途：
        1. 为单任务或复合查询提供可复用的源库连接。
        边界条件：
        1. 每次调用都会创建新会话；调用方负责在完成后关闭。
        """
        return MarketDataReadSession(self, self._connect())

    def _ensure_shadow_copy(self) -> Path:
        """
        确保存在可用的源库影子副本。

        策略：
        1. 影子库不存在时复制一次。
        2. 若源库 `mtime` 更新晚于影子库，则重新复制。
        3. 复制过程受进程内锁保护，避免并发读请求同时重建影子库。
        """
        with self._shadow_lock:
            should_copy = not self._shadow_db_path.exists()
            if not should_copy:
                try:
                    should_copy = self._shadow_db_path.stat().st_mtime < self.source_db_path.stat().st_mtime
                except FileNotFoundError:
                    should_copy = True

            if should_copy:
                shutil.copy2(self.source_db_path, self._shadow_db_path)
        return self._shadow_db_path

    def ensure_available(self) -> None:
        """
        检查源库文件是否存在。

        缺失时直接抛 `FileNotFoundError`，由上层在创建任务或查询时转换为面向用户的错误语义。
        """
        if not self.source_db_path.exists():
            raise FileNotFoundError(f"源数据库不存在: {self.source_db_path}")

    def _fetch_stock_rows(
        self,
        con: duckdb.DuckDBPyConnection | None = None,
    ) -> list[tuple[str, str]]:
        """
        输入：
        1. con: 可选已打开 DuckDB 连接。
        输出：
        1. 返回按代码排序的 `(code, name)` 列表。
        用途：
        1. 统一 `stocks` 表读取逻辑，便于调用方复用同一份股票快照。
        边界条件：
        1. 未提供连接时自行短生命周期建连读取。
        """

        if con is not None:
            rows = con.execute("select code, name from stocks order by code").fetchall()
        else:
            with self._connect() as own_con:
                rows = own_con.execute("select code, name from stocks order by code").fetchall()
        return [(str(row[0]), str(row[1] or "")) for row in rows]

    def get_all_stocks(self) -> list[tuple[str, str]]:
        """
        返回 `stocks` 表中的全部 `(code, name)` 记录，按代码排序。
        """
        return self._fetch_stock_rows()

    def _get_stock_concepts_by_codes(
        self,
        codes: list[str],
        con: duckdb.DuckDBPyConnection | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        输入：
        1. codes: 股票代码列表。
        2. con: 可选已打开 DuckDB 连接。
        输出：
        1. `code -> 概念明细列表` 映射。
        用途：
        1. 批量读取股票概念与入选理由。
        边界条件：
        1. 空输入返回空映射。
        2. 返回结果会为每个输入 code 预置一个列表，因此“无概念”股票返回空列表而不是缺 key。
        """

        unique_codes = list(dict.fromkeys(code for code in codes if code))
        if not unique_codes:
            return {}
        query = """
            select code, board_name, selected_reason, updated_at
            from stock_concepts
            where code in (select unnest($1))
            order by code asc, board_name asc
        """
        if con is not None:
            rows = con.execute(query, [unique_codes]).fetchall()
        else:
            with self._connect() as own_con:
                rows = own_con.execute(query, [unique_codes]).fetchall()
        result: dict[str, list[dict[str, Any]]] = {code: [] for code in unique_codes}
        for code, board_name, selected_reason, updated_at in rows:
            result.setdefault(str(code), []).append(
                {
                    "board_name": str(board_name or ""),
                    "selected_reason": str(selected_reason or ""),
                    "updated_at": updated_at,
                }
            )
        return result

    def get_stock_concepts_by_codes(self, codes: list[str]) -> dict[str, list[dict[str, Any]]]:
        """
        输入：
        1. codes: 股票代码列表。
        输出：
        1. `code -> 概念明细列表` 映射。
        用途：
        1. 通过短生命周期连接读取股票概念信息。
        边界条件：
        1. 空输入返回空映射。
        """
        return self._get_stock_concepts_by_codes(codes)

    def _filter_codes_by_concept_formula(
        self,
        *,
        codes: list[str],
        formula: ConceptFormula,
        con: duckdb.DuckDBPyConnection | None = None,
    ) -> tuple[list[str], dict[str, Any]]:
        """
        输入：
        1. codes: 待过滤股票代码列表。
        2. formula: 概念筛选公式。
        输出：
        1. 过滤后的股票列表和统计信息。
        用途：
        1. 在进入策略前执行股票池概念预筛选。
        边界条件：
        1. 公式未启用时直接返回原列表。
        """

        unique_codes = list(dict.fromkeys(code for code in codes if code))
        if not unique_codes:
            return [], {"active": formula.is_active(), "raw_count": 0, "matched_count": 0, "excluded_count": 0}
        if not formula.is_active():
            return unique_codes, {
                "active": False,
                "raw_count": len(unique_codes),
                "matched_count": len(unique_codes),
                "excluded_count": 0,
                "concept_terms": list(formula.concept_terms),
                "reason_terms": list(formula.reason_terms),
            }

        concept_map = self._get_stock_concepts_by_codes(unique_codes, con=con)
        matched_codes: list[str] = []
        excluded_codes: list[str] = []
        for code in unique_codes:
            entries = concept_map.get(code) or []
            board_names = [str(item.get("board_name") or "") for item in entries]
            reasons = [str(item.get("selected_reason") or "") for item in entries]
            has_concepts = all(
                any(term in board_name for board_name in board_names)
                for term in formula.concept_terms
            ) if formula.concept_terms else True
            has_reason = any(
                term in reason
                for term in formula.reason_terms
                for reason in reasons
            ) if formula.reason_terms else True
            if has_concepts and has_reason:
                matched_codes.append(code)
            else:
                excluded_codes.append(code)

        return matched_codes, {
            "active": True,
            "raw_count": len(unique_codes),
            "matched_count": len(matched_codes),
            "excluded_count": len(excluded_codes),
            "excluded_code_preview": excluded_codes[:30],
            "concept_terms": list(formula.concept_terms),
            "reason_terms": list(formula.reason_terms),
        }

    def filter_codes_by_concept_formula(
        self,
        *,
        codes: list[str],
        formula: ConceptFormula,
    ) -> tuple[list[str], dict[str, Any]]:
        """
        输入：
        1. codes: 待过滤股票代码列表。
        2. formula: 概念筛选公式。
        输出：
        1. 过滤后的股票列表和统计信息。
        用途：
        1. 通过短生命周期连接执行概念预筛选。
        边界条件：
        1. 公式未启用时直接返回原列表。
        """
        return self._filter_codes_by_concept_formula(codes=codes, formula=formula)

    def resolve_stock_inputs(
        self,
        raw_inputs: list[str],
        stock_rows: list[tuple[str, str]] | None = None,
    ) -> StockResolution:
        """
        输入：
        1. raw_inputs: 原始股票输入，可为代码或名称。
        2. stock_rows: 可选的股票快照；传入时不再重复查询 `stocks` 表。
        输出：
        1. 解析后的股票代码、未命中输入与 `code_to_name` 映射。
        用途：
        1. 将用户输入统一解析为可执行股票代码集合。
        边界条件：
        1. 未提供 `stock_rows` 时会自行查询 `stocks` 表。
        2. 空输入默认返回全量股票代码。
        """
        normalized = [token.strip() for token in raw_inputs if token and token.strip()]
        stocks = stock_rows if stock_rows is not None else self._fetch_stock_rows()

        code_to_name = {code: name for code, name in stocks}
        name_to_codes: dict[str, list[str]] = {}
        for code, name in stocks:
            name_to_codes.setdefault(name, []).append(code)

        if not normalized:
            all_codes = sorted(code_to_name.keys())
            return StockResolution(codes=all_codes, unresolved=[], code_to_name=code_to_name)

        resolved_codes: list[str] = []
        unresolved: list[str] = []
        seen: set[str] = set()

        for token in normalized:
            if token in code_to_name:
                if token not in seen:
                    resolved_codes.append(token)
                    seen.add(token)
                continue
            if token in name_to_codes:
                for code in name_to_codes[token]:
                    if code not in seen:
                        resolved_codes.append(code)
                        seen.add(code)
                continue
            unresolved.append(token)

        return StockResolution(codes=resolved_codes, unresolved=unresolved, code_to_name=code_to_name)

    def _filter_codes_with_complete_range(
        self,
        *,
        codes: list[str],
        start_ts: datetime,
        end_ts: datetime,
        required_timeframes: list[str],
        con: duckdb.DuckDBPyConnection | None = None,
    ) -> CoverageFilterResult:
        """
        在给定时间区间内过滤“数据完整”的股票。

        判定口径（按周期逐一判定）：
        1. 先计算该周期在区间内的全市场期望 bar 数（distinct datetime）；
        2. 某股票在同区间内该周期 bar 数必须等于期望值，才视为该周期完整；
        3. 只有在所有 required_timeframes 都完整，才保留该股票。
        """
        unique_codes = list(dict.fromkeys(code for code in codes if code))
        if not unique_codes:
            return CoverageFilterResult(
                eligible_codes=[],
                excluded_codes=[],
                excluded_by_timeframe={},
                expected_bars_by_timeframe={},
            )

        if not required_timeframes:
            return CoverageFilterResult(
                eligible_codes=unique_codes,
                excluded_codes=[],
                excluded_by_timeframe={},
                expected_bars_by_timeframe={},
            )

        expected_by_tf: dict[str, int] = {}
        complete_sets: dict[str, set[str]] = {}
        excluded_by_tf: dict[str, list[str]] = {}

        if con is None:
            with self._connect() as own_con:
                return self._filter_codes_with_complete_range(
                    codes=codes,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    required_timeframes=required_timeframes,
                    con=own_con,
                )

        con.execute("drop table if exists _tmp_candidate_codes")
        con.execute("create temp table _tmp_candidate_codes (code varchar)")
        con.execute(
            "insert into _tmp_candidate_codes(code) select unnest($1)",
            [unique_codes],
        )

        for tf in required_timeframes:
            table = TIMEFRAME_TO_TABLE.get(tf)
            if not table:
                raise ValueError(f"不支持的周期: {tf}")

            expected_count = int(
                con.execute(
                    f"""
                    select count(distinct datetime)
                    from {table}
                    where datetime >= ?
                      and datetime <= ?
                    """,
                    [start_ts, end_ts],
                ).fetchone()[0]
                or 0
            )
            expected_by_tf[tf] = expected_count

            if expected_count <= 0:
                complete_sets[tf] = set()
                excluded_by_tf[tf] = list(unique_codes)
                continue

            rows = con.execute(
                f"""
                select t.code
                from {table} t
                join _tmp_candidate_codes c on c.code = t.code
                where t.datetime >= ?
                  and t.datetime <= ?
                group by t.code
                having count(distinct t.datetime) = ?
                """,
                [start_ts, end_ts, expected_count],
            ).fetchall()
            complete_codes = {row[0] for row in rows}
            complete_sets[tf] = complete_codes
            excluded_by_tf[tf] = [code for code in unique_codes if code not in complete_codes]

        eligible_set = set(unique_codes)
        for tf in required_timeframes:
            eligible_set &= complete_sets.get(tf, set())

        eligible_codes = [code for code in unique_codes if code in eligible_set]
        excluded_codes = [code for code in unique_codes if code not in eligible_set]

        return CoverageFilterResult(
            eligible_codes=eligible_codes,
            excluded_codes=excluded_codes,
            excluded_by_timeframe=excluded_by_tf,
            expected_bars_by_timeframe=expected_by_tf,
        )

    def filter_codes_with_complete_range(
        self,
        *,
        codes: list[str],
        start_ts: datetime,
        end_ts: datetime,
        required_timeframes: list[str],
    ) -> CoverageFilterResult:
        """
        在给定时间区间内过滤“数据完整”的股票。

        判定口径（按周期逐一判定）：
        1. 先计算该周期在区间内的全市场期望 bar 数（distinct datetime）；
        2. 某股票在同区间内该周期 bar 数必须等于期望值，才视为该周期完整；
        3. 只有在所有 required_timeframes 都完整，才保留该股票。
        """
        return self._filter_codes_with_complete_range(
            codes=codes,
            start_ts=start_ts,
            end_ts=end_ts,
            required_timeframes=required_timeframes,
        )

    def _fetch_ohlcv(
        self,
        *,
        code: str,
        timeframe: str,
        start_ts: datetime | None,
        end_ts: datetime | None,
        con: duckdb.DuckDBPyConnection | None = None,
    ) -> pd.DataFrame:
        """
        输入：
        1. code: 股票代码。
        2. timeframe: 周期。
        3. start_ts/end_ts: 可选时间窗口。
        4. con: 可选已打开 DuckDB 连接。
        输出：
        1. 对应周期的 OHLCV DataFrame。
        用途：
        1. 复用公共查询逻辑，便于会话模式复用同一条连接。
        边界条件：
        1. 空结果保持原有空 DataFrame 语义。
        """
        table = TIMEFRAME_TO_TABLE[timeframe]
        query = f"""
            select
                datetime,
                open,
                high,
                low,
                close,
                volume,
                coalesce(amount, 0) as amount
            from {table}
            where code = ?
              and (? is null or datetime >= ?)
              and (? is null or datetime <= ?)
            order by datetime asc
        """
        if con is not None:
            df = con.execute(query, [code, start_ts, start_ts, end_ts, end_ts]).fetchdf()
        else:
            with self._connect() as own_con:
                df = own_con.execute(query, [code, start_ts, start_ts, end_ts, end_ts]).fetchdf()

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime")
        return df

    def fetch_ohlcv(
        self,
        *,
        code: str,
        timeframe: str,
        start_ts: datetime | None,
        end_ts: datetime | None,
    ) -> pd.DataFrame:
        """
        输入：
        1. code: 股票代码。
        2. timeframe: 周期。
        3. start_ts/end_ts: 可选时间窗口。
        输出：
        1. 对应周期的 OHLCV DataFrame。
        用途：
        1. 通过短生命周期连接读取单股票单周期行情。
        边界条件：
        1. 空结果保持原有空 DataFrame 语义。
        """
        return self._fetch_ohlcv(
            code=code,
            timeframe=timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def fetch_candles_for_chart(
        self,
        *,
        code: str,
        timeframe: str,
        start_ts: datetime | None,
        end_ts: datetime | None,
        center_ts: datetime | None,
        window: int,
    ) -> list[dict[str, Any]]:
        """
        输入：
        1. code: 输入参数，具体约束以调用方和实现为准。
        2. timeframe: 输入参数，具体约束以调用方和实现为准。
        3. start_ts: 输入参数，具体约束以调用方和实现为准。
        4. end_ts: 输入参数，具体约束以调用方和实现为准。
        5. center_ts: 输入参数，具体约束以调用方和实现为准。
        6. window: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `fetch_candles_for_chart` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        table = TIMEFRAME_TO_TABLE[timeframe]

        with self._connect() as con:
            if center_ts is None:
                query = f"""
                    select datetime, open, high, low, close, volume
                    from {table}
                    where code = ?
                      and (? is null or datetime >= ?)
                      and (? is null or datetime <= ?)
                    order by datetime asc
                    limit ?
                """
                rows = con.execute(
                    query,
                    [code, start_ts, start_ts, end_ts, end_ts, max(window * 2, 800)],
                ).fetchall()
            else:
                left_rows = con.execute(
                    f"""
                    select datetime, open, high, low, close, volume
                    from {table}
                    where code = ?
                      and datetime <= ?
                      and (? is null or datetime >= ?)
                    order by datetime desc
                    limit ?
                    """,
                    [code, center_ts, start_ts, start_ts, window],
                ).fetchall()
                right_rows = con.execute(
                    f"""
                    select datetime, open, high, low, close, volume
                    from {table}
                    where code = ?
                      and datetime > ?
                      and (? is null or datetime <= ?)
                    order by datetime asc
                    limit ?
                    """,
                    [code, center_ts, end_ts, end_ts, window],
                ).fetchall()
                rows = list(reversed(left_rows)) + list(right_rows)

        return [
            {
                "datetime": row[0],
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
            }
            for row in rows
        ]

    def fetch_candles_between(
        self,
        *,
        code: str,
        timeframe: str,
        start_ts: datetime | None,
        end_ts: datetime | None,
        limit: int = 12000,
        order: str = "asc",
    ) -> list[dict[str, Any]]:
        """
        输入：
        1. code: 输入参数，具体约束以调用方和实现为准。
        2. timeframe: 输入参数，具体约束以调用方和实现为准。
        3. start_ts: 输入参数，具体约束以调用方和实现为准。
        4. end_ts: 输入参数，具体约束以调用方和实现为准。
        5. limit: 输入参数，具体约束以调用方和实现为准。
        6. order: "asc" 或 "desc"，控制返回顺序。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `fetch_candles_between` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        table = TIMEFRAME_TO_TABLE[timeframe]
        order_clause = "asc" if order == "asc" else "desc"
        query = f"""
            select datetime, open, high, low, close, volume
            from {table}
            where code = ?
              and (? is null or datetime >= ?)
              and (? is null or datetime <= ?)
            order by datetime {order_clause}
            limit ?
        """
        with self._connect() as con:
            rows = con.execute(
                query,
                [code, start_ts, start_ts, end_ts, end_ts, limit],
            ).fetchall()

        return [
            {
                "datetime": row[0],
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
            }
            for row in rows
        ]
