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

    def get_all_stocks(self) -> list[tuple[str, str]]:
        """
        返回 `stocks` 表中的全部 `(code, name)` 记录，按代码排序。
        """
        with self._connect() as con:
            rows = con.execute("select code, name from stocks order by code").fetchall()
        return [(row[0], row[1]) for row in rows]

    def get_stock_concepts_by_codes(self, codes: list[str]) -> dict[str, list[dict[str, Any]]]:
        """
        输入：
        1. codes: 股票代码列表。
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
        placeholders = ",".join(["?"] * len(unique_codes))
        with self._connect() as con:
            rows = con.execute(
                f"""
                select code, board_name, selected_reason, updated_at
                from stock_concepts
                where code in ({placeholders})
                order by code asc, board_name asc
                """,
                unique_codes,
            ).fetchall()
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

        concept_map = self.get_stock_concepts_by_codes(unique_codes)
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

    def resolve_stock_inputs(self, raw_inputs: list[str]) -> StockResolution:
        """
        输入：
        1. raw_inputs: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `resolve_stock_inputs` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
        """
        normalized = [token.strip() for token in raw_inputs if token and token.strip()]
        with self._connect() as con:
            stocks = con.execute("select code, name from stocks").fetchall()

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

        with self._connect() as con:
            con.execute("drop table if exists _tmp_candidate_codes")
            con.execute("create temp table _tmp_candidate_codes (code varchar)")
            con.executemany(
                "insert into _tmp_candidate_codes(code) values (?)",
                [(code,) for code in unique_codes],
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
        1. code: 输入参数，具体约束以调用方和实现为准。
        2. timeframe: 输入参数，具体约束以调用方和实现为准。
        3. start_ts: 输入参数，具体约束以调用方和实现为准。
        4. end_ts: 输入参数，具体约束以调用方和实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `fetch_ohlcv` 对应的业务或工具逻辑。
        边界条件：
        1. 关键边界与异常分支按函数体内判断与调用约定处理。
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
        with self._connect() as con:
            df = con.execute(query, [code, start_ts, start_ts, end_ts, end_ts]).fetchdf()

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime")
        return df

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
