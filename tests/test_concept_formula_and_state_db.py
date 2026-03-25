"""
概念公式与状态库概念任务测试。

职责：
1. 验证概念公式配置解析契约稳定。
2. 验证 concept_logs 支持游标读取与最近任务保留策略。
边界：
1. 全部测试仅依赖临时 DuckDB，不依赖外部网络。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.db.state_db import StateDB
from app.services.concept_formula import parse_concept_formula


class TestConceptFormula(unittest.TestCase):
    """概念公式解析测试。"""

    def test_parse_formula_supports_aliases_and_normalization(self) -> None:
        formula = parse_concept_formula(
            {
                "universe_filters": {
                    "concepts": {
                        "enabled": True,
                        "names": [" 机器人 ", "液冷", "机器人"],
                        "keywords": ["中标", " 订单 ", ""],
                    }
                }
            }
        )

        self.assertTrue(formula.enabled)
        self.assertEqual(formula.concept_terms, ("机器人", "液冷"))
        self.assertEqual(formula.reason_terms, ("中标", "订单"))
        self.assertTrue(formula.is_active())

    def test_parse_formula_returns_inactive_when_disabled(self) -> None:
        formula = parse_concept_formula(
            {
                "universe_filters": {
                    "concepts": {
                        "enabled": False,
                        "concept_terms": ["算力"],
                        "reason_terms": ["订单"],
                    }
                }
            }
        )

        self.assertFalse(formula.is_active())
        self.assertEqual(
            formula.to_dict(),
            {
                "enabled": False,
                "active": False,
                "concept_terms": ["算力"],
                "reason_terms": ["订单"],
            },
        )


class TestStateDBConceptLogs(unittest.TestCase):
    """概念任务日志与保留策略测试。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="state-db-concept-")
        self.db_path = Path(self._tmp.name) / "screening_state.duckdb"
        self.state_db = StateDB(self.db_path)
        self.state_db.init_schema()

    def tearDown(self) -> None:
        try:
            self.state_db.close()
        finally:
            self._tmp.cleanup()

    def _create_job(self, job_id: str) -> None:
        self.state_db.create_concept_job(
            job_id=job_id,
            source_db="test-source.duckdb",
            params={"case": "concept"},
        )

    def test_concept_logs_after_log_id_cursor_query_stable(self) -> None:
        job_id = "concept-job-cursor"
        self._create_job(job_id)
        for idx in range(9):
            self.state_db.append_concept_log(
                job_id=job_id,
                level="info",
                message=f"c-log-{idx}",
                detail={"idx": idx},
            )

        first_page = self.state_db.get_concept_logs(job_id=job_id, level="info", offset=0, limit=4)
        self.assertEqual(len(first_page), 4)
        first_ids = [int(item["log_id"]) for item in first_page]
        self.assertEqual(first_ids, sorted(first_ids))

        cursor = int(first_page[-1]["log_id"])
        second_page = self.state_db.get_concept_logs(
            job_id=job_id,
            level="info",
            offset=999,
            limit=4,
            after_log_id=cursor,
        )
        self.assertEqual(len(second_page), 4)
        second_ids = [int(item["log_id"]) for item in second_page]
        self.assertEqual(second_ids, sorted(second_ids))
        self.assertTrue(all(log_id > cursor for log_id in second_ids))
        self.assertTrue(set(first_ids).isdisjoint(set(second_ids)))

    def test_concept_log_retention_keeps_recent_jobs_full_logs(self) -> None:
        old_job_id = "concept-old"
        keep_job_id = "concept-keep"
        latest_job_id = "concept-latest"

        with mock.patch("app.db.state_db.LOG_KEEP_CONCEPT_JOBS", 2):
            self._create_job(old_job_id)
            for idx in range(2):
                self.state_db.append_concept_log(
                    job_id=old_job_id,
                    level="info",
                    message=f"old-{idx}",
                    detail={"idx": idx},
                )
            self._create_job(keep_job_id)
            for idx in range(3):
                self.state_db.append_concept_log(
                    job_id=keep_job_id,
                    level="info",
                    message=f"keep-{idx}",
                    detail={"idx": idx},
                )
            self._create_job(latest_job_id)
            for idx in range(4):
                self.state_db.append_concept_log(
                    job_id=latest_job_id,
                    level="info",
                    message=f"latest-{idx}",
                    detail={"idx": idx},
                )

        old_logs = self.state_db.get_concept_logs(job_id=old_job_id, level="all", offset=0, limit=20)
        keep_logs = self.state_db.get_concept_logs(job_id=keep_job_id, level="all", offset=0, limit=20)
        latest_logs = self.state_db.get_concept_logs(job_id=latest_job_id, level="all", offset=0, limit=20)

        self.assertEqual(old_logs, [])
        self.assertEqual([item["message"] for item in keep_logs], ["keep-0", "keep-1", "keep-2"])
        self.assertEqual(
            [item["message"] for item in latest_logs],
            ["latest-0", "latest-1", "latest-2", "latest-3"],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
