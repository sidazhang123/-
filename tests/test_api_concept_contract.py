"""
概念任务 API 契约测试。

职责：
1. 验证概念任务创建、查询、日志接口的响应结构。
2. 验证结果页概念明细接口返回任务公式与概念列表。
边界：
1. 使用最小 FastAPI 测试应用，不依赖真实 app.main 启动副作用。
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock

import duckdb
from fastapi import HTTPException

from app.api.routes import (
    create_concept_job,
    get_concept_job,
    get_concept_logs,
    get_result_stock_concepts,
)


class _FakeConceptManager:
    def __init__(self) -> None:
        self._has_running_job = False

    def has_running_job(self) -> bool:
        return self._has_running_job

    def create_job(self) -> str:
        return "concept-job-1"

    def stop_job(self, job_id: str) -> dict[str, str]:
        return {"job_id": job_id, "status": "stopping", "message": "停止请求已记录"}


class _FakeMaintenanceManager:
    def has_running_job(self) -> bool:
        return False


class _FakeTaskManager:
    pass


class _FakeStrategyRegistry:
    pass


class _FakeStateDB:
    def __init__(self) -> None:
        self.task = {
            "task_id": "task-1",
            "status": "completed",
            "progress": 100.0,
            "total_stocks": 3,
            "processed_stocks": 3,
            "result_count": 2,
            "info_log_count": 0,
            "error_log_count": 0,
            "strategy_group_id": "g1",
            "strategy_name": "策略A",
            "strategy_description": "desc",
            "source_db": "fake-source.duckdb",
            "summary": {
                "concept_formula": {
                    "enabled": True,
                    "active": True,
                    "concept_terms": ["机器人", "液冷"],
                    "reason_terms": ["订单", "中标"],
                }
            },
            "params": {"run_mode": "full"},
        }
        self.result_stocks = [
            {
                "code": "sh.600000",
                "name": "浦发银行",
                "signal_count": 2,
                "first_signal_dt": None,
                "last_signal_dt": None,
            }
        ]
        self.concept_job = {
            "job_id": "concept-job-1",
            "status": "completed",
            "phase": "done",
            "progress": 100.0,
            "started_at": None,
            "finished_at": None,
            "error_message": None,
            "summary": {
                "steps_total": 10,
                "steps_completed": 10,
                "total_tasks": 3,
                "success_tasks": 3,
                "failed_tasks": 0,
                "records_written": 18,
                "filtered_records": 2,
                "duration_seconds": 12.5,
            },
        }
        self.concept_logs = [
            {"log_id": 11, "ts": None, "level": "info", "message": "start", "detail": None},
            {"log_id": 12, "ts": None, "level": "info", "message": "done", "detail": {"ok": True}},
        ]

    def get_task(self, task_id: str):
        return self.task if task_id == "task-1" else None

    def get_result_stock_summaries(self, task_id: str):
        return self.result_stocks if task_id == "task-1" else []

    def list_concept_jobs(self, offset: int, limit: int):
        return [self.concept_job]

    def get_concept_job(self, job_id: str):
        return self.concept_job if job_id == "concept-job-1" else None

    def get_concept_logs(self, job_id: str, level: str, offset: int, limit: int, after_log_id=None):
        if job_id != "concept-job-1":
            return []
        if after_log_id is None:
            return list(self.concept_logs)
        return [item for item in self.concept_logs if int(item["log_id"]) > int(after_log_id)]


class _FakeDuckDBRowsConnection:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        return self

    def fetchall(self):
        return list(self._rows)


class TestApiConceptContract(unittest.TestCase):
    """概念任务 API 契约测试。"""

    def setUp(self) -> None:
        self.request = SimpleNamespace(
            app=SimpleNamespace(
                state=SimpleNamespace(
                    state_db=_FakeStateDB(),
                    concept_manager=_FakeConceptManager(),
                    maintenance_manager=_FakeMaintenanceManager(),
                    task_manager=_FakeTaskManager(),
                    strategy_registry=_FakeStrategyRegistry(),
                )
            )
        )

    def test_create_concept_job_returns_job_id(self) -> None:
        resp = create_concept_job(self.request)
        self.assertEqual(resp.model_dump(), {"job_id": "concept-job-1"})

    def test_create_concept_job_maps_value_error_to_conflict(self) -> None:
        with mock.patch.object(self.request.app.state.concept_manager, "create_job", side_effect=ValueError("busy")):
            with self.assertRaises(HTTPException) as ctx:
                create_concept_job(self.request)
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(ctx.exception.detail, "busy")

    def test_get_concept_job_status_response(self) -> None:
        payload = get_concept_job("concept-job-1", self.request).model_dump()
        self.assertEqual(payload["job_id"], "concept-job-1")
        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["summary"]["records_written"], 18)
        self.assertEqual(payload["summary"]["filtered_records"], 2)

    def test_get_concept_logs_response_and_cursor(self) -> None:
        payload = get_concept_logs(
            "concept-job-1",
            self.request,
            level="info",
            after_log_id=None,
            offset=0,
            limit=10,
        ).model_dump()
        self.assertEqual(len(payload["items"]), 2)
        self.assertEqual(payload["next_after_log_id"], 12)

    def test_result_stock_concepts_response_contains_formula_and_items(self) -> None:
        concept_entries = {
            "sh.600000": [
                {
                    "board_name": "机器人",
                    "selected_reason": "订单持续兑现",
                    "updated_at": None,
                }
            ]
        }
        with mock.patch("app.api.routes.MarketDataDB.get_stock_concepts_by_codes", return_value=concept_entries):
            payload = get_result_stock_concepts("task-1", self.request).model_dump()
        self.assertEqual(payload["task_id"], "task-1")
        self.assertTrue(payload["formula"]["enabled"])
        self.assertEqual(payload["formula"]["concept_terms"], ["机器人", "液冷"])
        self.assertIn("sh.600000", payload["items"])
        self.assertEqual(payload["items"]["sh.600000"][0]["board_name"], "机器人")

    def test_result_stock_concepts_tolerates_duckdb_config_conflict(self) -> None:
        rows = [("sh.600000", "机器人", "订单持续兑现", None)]
        writable_conn = _FakeDuckDBRowsConnection(rows)

        with mock.patch(
            "app.db.duckdb_utils.duckdb.connect",
            side_effect=[
                duckdb.ConnectionException(
                    "Connection Error: Can't open a connection to same database file with a different configuration than existing connections"
                ),
                writable_conn,
            ],
        ):
            payload = get_result_stock_concepts("task-1", self.request).model_dump()

        self.assertIn("sh.600000", payload["items"])
        self.assertEqual(payload["items"]["sh.600000"][0]["board_name"], "机器人")


if __name__ == "__main__":
    unittest.main(verbosity=2)
