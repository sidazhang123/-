from __future__ import annotations

import unittest
from types import SimpleNamespace

from fastapi import FastAPI
from pydantic import ValidationError

from app.api import routes
from app.models.api_models import MonitorFormSettingsPayload


class _FakeStateDB:
    def __init__(self, settings: dict | None = None) -> None:
        self.settings = settings or {}
        self.saved_settings = None

    def get_monitor_form_settings(self) -> dict:
        return dict(self.settings)

    def set_monitor_form_settings(self, settings: dict) -> None:
        self.saved_settings = dict(settings)


class _FakeRegistry:
    def get_group_meta(self, group_id: str):
        if group_id != "demo":
            raise KeyError("missing group")
        return SimpleNamespace(
            default_params={
                "daily": {"lookback_days": 5, "close_to_ma20_pct": 0.05},
                "universe_filters": {"concepts": {"enabled": False, "concept_terms": [], "reason_terms": []}},
            }
        )

    def merge_group_params(self, group_id: str, group_params: dict):
        return group_params


class TestApiMonitorContract(unittest.TestCase):
    def test_monitor_form_settings_payload_uses_group_params_object(self) -> None:
        payload = MonitorFormSettingsPayload(
            source_db="D:/quant.duckdb",
            stocks_input="sh.600000",
            sample_size=20,
            strategy_group_id="demo",
            group_params={"daily": {"lookback_days": 7, "close_to_ma20_pct": 0.05}},
        )
        self.assertEqual(payload.group_params["daily"]["lookback_days"], 7)

        with self.assertRaises(ValidationError):
            MonitorFormSettingsPayload(strategy_group_id="demo", group_params_text="{}")

    def test_get_monitor_ui_settings_migrates_legacy_group_params_text(self) -> None:
        state_db = _FakeStateDB(
            {
                "strategy_group_id": "demo",
                "group_params_text": '{"__comment__":"root","daily":{"lookback_days":7,"close_to_ma20_pct":0.05}}',
            }
        )
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(state_db=state_db)))

        payload = routes.get_monitor_ui_settings(request)
        self.assertEqual(payload["settings"]["strategy_group_id"], "demo")
        self.assertEqual(payload["settings"]["group_params"]["daily"]["lookback_days"], 7)
        self.assertNotIn("group_params_text", payload["settings"])

    def test_save_monitor_ui_settings_validates_and_persists_group_params(self) -> None:
        state_db = _FakeStateDB()
        request = SimpleNamespace(
            app=SimpleNamespace(
                state=SimpleNamespace(
                    state_db=state_db,
                    strategy_registry=_FakeRegistry(),
                )
            )
        )

        payload = MonitorFormSettingsPayload(
            source_db="D:/quant.duckdb",
            stocks_input="sh.600000",
            sample_size=20,
            strategy_group_id="demo",
            group_params={
                "daily": {"lookback_days": 8, "close_to_ma20_pct": 0.06},
                "universe_filters": {"concepts": {"enabled": True, "concept_terms": ["robot"], "reason_terms": ["order"]}},
            },
        )
        resp = routes.save_monitor_ui_settings(payload, request)

        self.assertEqual(resp["settings"]["strategy_group_id"], "demo")
        self.assertEqual(state_db.saved_settings["group_params"]["daily"]["lookback_days"], 8)
        self.assertTrue(state_db.saved_settings["group_params"]["universe_filters"]["concepts"]["enabled"])

    def test_stock_states_route_removed(self) -> None:
        app = FastAPI()
        app.include_router(routes.router)
        task_paths = {
            (route.path, tuple(sorted(route.methods or [])))
            for route in app.routes
            if hasattr(route, "path")
        }
        self.assertNotIn(("/api/tasks/{task_id}/stock-states", ("GET",)), task_paths)


if __name__ == "__main__":
    unittest.main(verbosity=2)
