"""
Concept 更新请求的 no-proxy 回归测试。

职责：
1. 验证概念更新服务内部 HTTP Session 禁用环境代理。
2. 验证 Eastmoney 请求通过专用 Session 发起，而非直接调用 requests.get。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from app.services.concept_maintenance import ConceptEmptyResultError, ConceptMaintenanceService


class _FakeResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "result": {
                "data": [
                    {
                        "BOARD_NAME": "机器人",
                        "SELECTED_BOARD_REASON": "订单增长",
                    }
                ]
            }
        }


class _FakeSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, float]] = []

    def get(self, url: str, timeout: float):
        self.calls.append((url, timeout))
        return _FakeResponse()


class TestConceptMaintenanceNoProxy(unittest.TestCase):
    def test_http_session_disables_environment_proxy(self) -> None:
        service = ConceptMaintenanceService(source_db_path="test.duckdb", logger=None)
        self.assertFalse(service.http_session.trust_env)

    def test_fetch_uses_dedicated_session_without_requests_get(self) -> None:
        service = ConceptMaintenanceService(source_db_path="test.duckdb", logger=None)
        fake_session = _FakeSession()
        service.http_session = fake_session

        records, filtered = service._fetch_concepts_for_stock("sh.600000", "浦发银行")

        self.assertEqual(filtered, 0)
        self.assertEqual(records, [("sh.600000", "机器人", "订单增长")])
        self.assertEqual(len(fake_session.calls), 1)
        request_url, request_timeout = fake_session.calls[0]
        self.assertIn("datacenter.eastmoney.com", request_url)
        self.assertEqual(request_timeout, service.timeout_seconds)

    def test_replace_all_records_deduplicates_same_code_and_board(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "concepts.duckdb"
            service = ConceptMaintenanceService(source_db_path=db_path, logger=None)
            service.ensure_runtime_schema()

            written = service._replace_all_records(
                [
                    ("sz.002315", "跨境电商", "题材入选"),
                    ("sz.002315", "跨境电商", "题材入选"),
                    ("sz.002315", "跨境电商", "新增催化"),
                    ("sz.002315", "机器人", "订单增长"),
                ]
            )

            self.assertEqual(written, 2)
            concept_map = service._connect_source_db().execute(
                "select code, board_name, selected_reason from stock_concepts order by code, board_name"
            ).fetchall()
            self.assertEqual(
                concept_map,
                [
                    ("sz.002315", "机器人", "订单增长"),
                    ("sz.002315", "跨境电商", "题材入选；新增催化"),
                ],
            )

    def test_run_update_preserves_old_records_when_stock_list_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "concepts.duckdb"
            service = ConceptMaintenanceService(source_db_path=db_path, logger=None)
            service.ensure_runtime_schema()
            service._replace_all_records([("sz.002315", "跨境电商", "旧数据")])
            service._load_stock_inputs = lambda: []

            with self.assertRaisesRegex(ConceptEmptyResultError, "已保留旧概念数据"):
                service.run_update()

            concept_map = service._connect_source_db().execute(
                "select code, board_name, selected_reason from stock_concepts order by code, board_name"
            ).fetchall()
            self.assertEqual(concept_map, [("sz.002315", "跨境电商", "旧数据")])

    def test_run_update_preserves_old_records_when_no_concepts_fetched(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "concepts.duckdb"
            service = ConceptMaintenanceService(source_db_path=db_path, logger=None)
            service.ensure_runtime_schema()
            service._replace_all_records([("sz.002315", "跨境电商", "旧数据")])
            service._load_stock_inputs = lambda: [("sz.002315", "焦点科技")]
            service._fetch_concepts_for_stock = lambda code, name: ([], 0)

            with self.assertRaisesRegex(ConceptEmptyResultError, "已保留旧概念数据"):
                service.run_update()

            concept_map = service._connect_source_db().execute(
                "select code, board_name, selected_reason from stock_concepts order by code, board_name"
            ).fetchall()
            self.assertEqual(concept_map, [("sz.002315", "跨境电商", "旧数据")])

    def test_run_update_exports_xlsx_to_source_db_sibling_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "quant.duckdb"
            service = ConceptMaintenanceService(source_db_path=db_path, logger=None)
            service._load_stock_inputs = lambda: [("sz.002315", "焦点科技")]
            service._fetch_concepts_for_stock = lambda code, name: (
                [
                    ("sz.002315", "跨境电商", "题材入选"),
                    ("sz.002315", "机器人", "订单增长"),
                ],
                0,
            )

            summary = service.run_update()

            self.assertEqual(summary.records_written, 2)
            xlsx_files = list(Path(temp_dir).glob("*个股东财概念.xlsx"))
            self.assertEqual(len(xlsx_files), 1)
            self.assertEqual(xlsx_files[0].parent, db_path.parent)
            with ZipFile(xlsx_files[0]) as archive:
                self.assertIn("xl/workbook.xml", archive.namelist())


if __name__ == "__main__":
    unittest.main(verbosity=2)