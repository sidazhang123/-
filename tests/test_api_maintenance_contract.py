"""
维护 API 模型契约测试。

职责：
1. 验证维护创建请求仅接收 mode。
2. 验证维护设置模型移除旧字段并保留展示配置字段。
"""

from __future__ import annotations

import unittest

from pydantic import ValidationError

from app.models.api_models import MaintenanceCreateRequest, MaintenanceFormSettingsPayload


class TestApiMaintenanceContract(unittest.TestCase):
    """维护 API 数据模型契约测试。"""

    def test_maintenance_create_request_mode_only(self) -> None:
        """
        输入：
        1. 新模式值与旧 source_db 字段。
        输出：
        1. 新模式通过，旧字段触发校验失败。
        用途：
        1. 锁定 `POST /api/maintenance/jobs` 请求契约。
        边界条件：
        1. extra 字段禁止。
        """

        req = MaintenanceCreateRequest(mode="latest_update")
        self.assertEqual(req.mode, "latest_update")
        req2 = MaintenanceCreateRequest(mode="historical_backfill")
        self.assertEqual(req2.mode, "historical_backfill")

        with self.assertRaises(ValidationError):
            MaintenanceCreateRequest(mode="latest_update", source_db="D:/quant.duckdb")

    def test_maintenance_form_settings_payload_removed_legacy_fields(self) -> None:
        """
        输入：
        1. 新设置字段与旧 lookback/max/source 字段。
        输出：
        1. 新字段通过，旧字段触发校验失败。
        用途：
        1. 锁定 `/api/ui-settings/maintenance` 新契约。
        边界条件：
        1. mode 仅允许 latest_update/historical_backfill。
        """

        payload = MaintenanceFormSettingsPayload(
            mode="historical_backfill",
            info_log_autoscroll=False,
            error_log_autoscroll=True,
        )
        self.assertEqual(payload.mode, "historical_backfill")
        self.assertFalse(payload.info_log_autoscroll)
        self.assertTrue(payload.error_log_autoscroll)

        with self.assertRaises(ValidationError):
            MaintenanceFormSettingsPayload(mode="latest_update", source_db="D:/quant.duckdb")
        with self.assertRaises(ValidationError):
            MaintenanceFormSettingsPayload(mode="latest_update", lookback_days=10)
        with self.assertRaises(ValidationError):
            MaintenanceFormSettingsPayload(mode="latest_update", max_codes=100)


if __name__ == "__main__":
    unittest.main(verbosity=2)
