from __future__ import annotations

import asyncio
import json
import logging
from queue import Empty, Queue
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd

from app.api.routes import backtest_event_stream
from app.services import backtest_engine
from app.services.backtest_manager import BacktestManager
from strategies.engine_commons import DetectionResult


class _DummyStateDB:
    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, Any]] = {}
        self.logs: list[dict[str, Any]] = []
        self.meta: dict[str, Any] = {}

    def get_running_backtest_job(self) -> dict[str, Any] | None:
        return None

    def get_backtest_job(self, job_id: str) -> dict[str, Any] | None:
        job = self.jobs.get(job_id)
        return dict(job) if job else None

    def update_backtest_job_fields(self, job_id: str, **fields: Any) -> None:
        job = self.jobs.setdefault(job_id, {"job_id": job_id, "summary": {}})
        job.update(fields)
        summary_json = fields.get("summary_json")
        if summary_json:
            job["summary"] = json.loads(summary_json)

    def append_backtest_log(self, *, job_id: str, level: str, message: str) -> None:
        self.logs.append({"job_id": job_id, "level": level, "message": message})

    def get_meta_value(self, key: str) -> Any:
        return self.meta.get(key)

    def get_backtest_logs(
        self,
        job_id: str,
        level: str,
        after_log_id: int | None = None,
    ) -> list[dict[str, Any]]:
        _ = (job_id, level, after_log_id)
        return []


def _make_manager() -> tuple[BacktestManager, _DummyStateDB]:
    state_db = _DummyStateDB()
    manager = BacktestManager(state_db, logging.getLogger("test.backtest_sweep_batch"))
    return manager, state_db


class _FakeDuckDBConnection:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def execute(self, _sql: str) -> "_FakeDuckDBConnection":
        return self

    def fetchdf(self) -> pd.DataFrame:
        return self._df.copy()

    def close(self) -> None:
        return None


def _install_fake_worker_hooks(monkeypatch, hooks: dict[str, Any], df: pd.DataFrame) -> None:
    monkeypatch.setattr(
        backtest_engine.importlib,
        "import_module",
        lambda _module_path: SimpleNamespace(BACKTEST_HOOKS=hooks),
    )
    monkeypatch.setattr(
        backtest_engine.duckdb,
        "connect",
        lambda: _FakeDuckDBConnection(df),
    )


def test_detect_hits_single_worker_stays_independent_from_batch_progress_state(monkeypatch) -> None:
    df = pd.DataFrame(
        {
            "code": ["sh.600000", "sh.600000", "sh.600000"],
            "ts": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
            "open": [1.0, 1.1, 1.2],
            "high": [1.1, 1.2, 1.3],
            "low": [0.9, 1.0, 1.1],
            "close": [1.05, 1.15, 1.25],
            "volume": [100, 110, 120],
        }
    )
    hooks = {
        "detect": lambda _df, **_kwargs: DetectionResult(matched=False),
        "detect_vectorized": lambda _df, **_kwargs: [
            DetectionResult(matched=True, pattern_start_idx=0, pattern_end_idx=1)
        ],
        "prepare": None,
        "normalize_params": lambda _params, _section_key: {"params": {"enabled": True}},
    }
    _install_fake_worker_hooks(monkeypatch, hooks, df)

    hits = backtest_engine._detect_hits_for_codes(
        codes=["sh.600000"],
        parquet_path="fake.parquet",
        tf_key="w",
        table_name="kline_weekly",
        hooks_module_path="tests.fake_hooks",
        group_params={"weekly": {"enabled": True}},
        section_key="weekly",
        slide_step=1,
        max_forward_bar=5,
    )

    assert len(hits) == 1
    assert hits[0]["code"] == "sh.600000"
    assert hits[0]["tf_key"] == "w"


def test_detect_hits_batch_worker_emits_partial_progress(monkeypatch) -> None:
    df = pd.DataFrame(
        {
            "code": ["sh.600000", "sh.600000", "sh.600000"],
            "ts": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
            "open": [1.0, 1.1, 1.2],
            "high": [1.1, 1.2, 1.3],
            "low": [0.9, 1.0, 1.1],
            "close": [1.05, 1.15, 1.25],
            "volume": [100, 110, 120],
        }
    )
    hooks = {
        "detect": lambda _df, **_kwargs: DetectionResult(matched=False),
        "detect_vectorized": lambda _df, **_kwargs: [],
        "prepare": None,
        "normalize_params": lambda params, _section_key: {"params": dict(params)},
    }
    _install_fake_worker_hooks(monkeypatch, hooks, df)
    progress_queue: Queue[dict[str, Any]] = Queue()

    results = backtest_engine._detect_hits_for_codes_batch(
        codes=["sh.600000"],
        parquet_path="fake.parquet",
        tf_key="d",
        table_name="kline_daily",
        hooks_module_path="tests.fake_hooks",
        combo_params_list=[
            (0, {"enabled": True, "slot": 0}),
            (1, {"enabled": True, "slot": 1}),
        ],
        section_key="daily",
        slide_step=1,
        max_forward_bar=5,
        progress_queue=progress_queue,
    )

    events: list[dict[str, Any]] = []
    while True:
        try:
            events.append(progress_queue.get_nowait())
        except Empty:
            break

    assert results == {0: [], 1: []}
    assert any(event.get("type") == "combo_progress" for event in events)
    assert any(event.get("type") == "stock_done" for event in events)


def test_run_sweep_skips_disabled_sections_and_batches_only_enabled_combos(
    monkeypatch,
    tmp_path: Path,
) -> None:
    manager, state_db = _make_manager()
    job_id = "job-sweep"
    state_db.jobs[job_id] = {"job_id": job_id, "status": "running", "summary": {}}

    hooks = {
        "tf_sections": {
            "weekly": {"tf_key": "w", "table": "kline_weekly"},
            "daily": {"tf_key": "d", "table": "kline_daily"},
        },
        "normalize_params": lambda params, section_key: {
            "params": {"enabled": bool(params[section_key]["enabled"])}
        },
    }
    job = {
        "forward_bars": [2, 5, 7],
        "slide_step": 1,
        "group_params": {
            "weekly": {"enabled": 0},
            "daily": {"enabled": 1},
        },
        "param_ranges": {
            "daily.enabled": {"min": 0, "max": 1, "step": 1},
        },
    }

    calls: dict[str, Any] = {
        "precompute": [],
        "load_codes": [],
        "batch": [],
        "resolved_hits": [],
    }

    def _fake_precompute_forward_metrics(**kwargs: Any) -> Path:
        calls["precompute"].append(kwargs["tf_key"])
        return tmp_path / f"{kwargs['tf_key']}.parquet"

    def _fake_load_all_codes(source_db_path: Path, table_name: str) -> list[str]:
        _ = source_db_path
        calls["load_codes"].append(table_name)
        return ["sh.600000", "sz.000001"]

    def _fake_batch(**kwargs: Any) -> "backtest_engine.SweepHitsAccumulator":
        from app.services import backtest_engine

        combo_params_list = kwargs["combo_params_list"]
        calls["batch"].append(
            {
                "section_key": kwargs["section_key"],
                "combo_indices": [idx for idx, _ in combo_params_list],
            }
        )
        acc = backtest_engine.SweepHitsAccumulator()
        for idx, _ in combo_params_list:
            acc.ensure_combo(idx)
        for idx, _ in combo_params_list:
            if idx == 1:
                acc.extend(idx, [
                    {
                        "code": "sh.600000",
                        "tf_key": kwargs["tf_key"],
                        "pattern_start_ts": "2026-01-01T00:00:00",
                        "pattern_end_ts": "2026-01-02T00:00:00",
                    }
                ])
        return acc

    def _fake_resolve_hit_metrics(**kwargs: Any) -> pd.DataFrame:
        calls["resolved_hits"].append(len(kwargs["hits"]))
        return pd.DataFrame()

    def _fake_resolve_hit_metrics_sweep(**kwargs: Any) -> dict[int, pd.DataFrame]:
        hit_acc = kwargs.get("hit_accumulator")
        combo_hits_map = kwargs.get("combo_hits_map")
        if hit_acc is not None:
            keys = list(hit_acc.keys())
            for ci in sorted(keys):
                calls["resolved_hits"].append(hit_acc.hit_counts.get(ci, 0))
            return {ci: pd.DataFrame() for ci in keys}
        for ci in sorted(combo_hits_map):
            calls["resolved_hits"].append(len(combo_hits_map[ci]))
        return {ci: pd.DataFrame() for ci in combo_hits_map}

    monkeypatch.setattr(
        "app.services.backtest_manager.backtest_engine.precompute_forward_metrics",
        _fake_precompute_forward_metrics,
    )
    monkeypatch.setattr(
        "app.services.backtest_manager.backtest_engine.load_all_codes",
        _fake_load_all_codes,
    )
    monkeypatch.setattr(
        "app.services.backtest_manager.backtest_engine.run_detection_parallel_batch",
        _fake_batch,
    )
    monkeypatch.setattr(
        "app.services.backtest_manager.backtest_engine.resolve_hit_metrics",
        _fake_resolve_hit_metrics,
    )
    monkeypatch.setattr(
        "app.services.backtest_manager.backtest_engine.resolve_hit_metrics_sweep",
        _fake_resolve_hit_metrics_sweep,
    )
    monkeypatch.setattr(
        "app.services.backtest_manager.backtest_engine.write_hits_csv",
        lambda hits_df, output_dir: None,
    )
    monkeypatch.setattr(
        "app.services.backtest_manager.backtest_engine.write_stats_json",
        lambda stats, output_dir: None,
    )
    monkeypatch.setattr(
        "app.services.backtest_manager.backtest_stats.compute_full_stats",
        lambda hits_df, forward_bars: {"per_forward": {}},
    )

    manager._run_sweep(
        job_id,
        job,
        hooks,
        "tests.fake_strategy",
        Path("source.duckdb"),
        tmp_path / "fwd-cache",
        tmp_path / "bt-cache",
        2,
        "v1",
        {"sh.600000": "浦发银行"},
        lambda level, message: None,
    )

    sweep_results = json.loads(
        (tmp_path / "bt-cache" / job_id / "sweep_results.json").read_text(encoding="utf-8")
    )

    assert calls["precompute"] == ["d"]
    assert calls["load_codes"] == ["kline_daily"]
    assert calls["batch"] == [{"section_key": "daily", "combo_indices": [1]}]
    assert calls["resolved_hits"] == [0, 1]
    assert [row["total_hits"] for row in sweep_results] == [0, 1]
    assert state_db.jobs[job_id]["combo_total"] == 2
    assert state_db.jobs[job_id]["combo_index"] == 2
    manager.shutdown()


def test_get_job_status_merges_runtime_phase_only_for_running_jobs() -> None:
    manager, state_db = _make_manager()
    job_id = "job-status-phase"
    state_db.jobs[job_id] = {"job_id": job_id, "status": "running", "summary": {}}

    manager._set_job_runtime_status(
        job_id,
        phase="detect",
        phase_label="检测 daily",
        phase_index=1,
        phase_total=2,
    )

    running_status = manager.get_job_status(job_id)
    assert running_status["phase"] == "detect"
    assert running_status["phase_label"] == "检测 daily"

    state_db.jobs[job_id]["status"] = "completed"
    completed_status = manager.get_job_status(job_id)
    assert completed_status.get("phase") is None
    assert completed_status.get("phase_label") is None
    manager.shutdown()


def test_backtest_sse_emits_status_when_only_phase_changes(monkeypatch) -> None:
    class _FakeManager:
        def __init__(self) -> None:
            self._idx = 0
            self._statuses = [
                {
                    "job_id": "job-1",
                    "status": "running",
                    "mode": "sweep",
                    "progress": 0.5,
                    "total_stocks": 12,
                    "processed_stocks": 6,
                    "combo_index": 0,
                    "combo_total": 2,
                    "phase": "detect",
                    "phase_label": "检测 daily",
                    "phase_index": 1,
                    "phase_total": 2,
                    "summary": {},
                },
                {
                    "job_id": "job-1",
                    "status": "running",
                    "mode": "sweep",
                    "progress": 0.5,
                    "total_stocks": 12,
                    "processed_stocks": 6,
                    "combo_index": 0,
                    "combo_total": 2,
                    "phase": "stats",
                    "phase_label": "统计参数组合",
                    "phase_index": 1,
                    "phase_total": 2,
                    "summary": {},
                },
                {
                    "job_id": "job-1",
                    "status": "completed",
                    "mode": "sweep",
                    "progress": 1.0,
                    "total_stocks": 12,
                    "processed_stocks": 12,
                    "combo_index": 2,
                    "combo_total": 2,
                    "summary": {},
                },
            ]

        def get_job_status(self, job_id: str) -> dict[str, Any]:
            _ = job_id
            if self._idx >= len(self._statuses):
                return self._statuses[-1]
            status = self._statuses[self._idx]
            self._idx += 1
            return status

        def get_logs(self, job_id: str, level: str, after_log_id: int = 0) -> list[dict[str, Any]]:
            _ = (job_id, level, after_log_id)
            return []

    async def _collect_events() -> list[str]:
        async def _never_disconnect() -> bool:
            return False

        request = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(backtest_manager=_FakeManager())),
            query_params={},
            is_disconnected=_never_disconnect,
        )
        response = await backtest_event_stream("job-1", request)
        events: list[str] = []
        async for chunk in response.body_iterator:
            events.append(chunk if isinstance(chunk, str) else chunk.decode("utf-8"))
        return events

    async def _fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("app.api.routes.asyncio.sleep", _fake_sleep)
    events = asyncio.run(_collect_events())
    job_status_events = [event for event in events if event.startswith("event: job-status")]

    assert len(job_status_events) == 3
    assert any('"phase_label": "检测 daily"' in event for event in job_status_events)
    assert any('"phase_label": "统计参数组合"' in event for event in job_status_events)


def test_backtest_sse_emits_status_when_only_total_stocks_changes(monkeypatch) -> None:
    class _FakeManager:
        def __init__(self) -> None:
            self._idx = 0
            self._statuses = [
                {
                    "job_id": "job-2",
                    "status": "running",
                    "mode": "sweep",
                    "progress": 0.0,
                    "total_stocks": 0,
                    "processed_stocks": 0,
                    "combo_index": 0,
                    "combo_total": 10,
                    "phase": "detect",
                    "phase_label": "检测 daily",
                    "phase_index": 1,
                    "phase_total": 1,
                    "summary": {},
                },
                {
                    "job_id": "job-2",
                    "status": "running",
                    "mode": "sweep",
                    "progress": 0.0,
                    "total_stocks": 5196,
                    "processed_stocks": 0,
                    "combo_index": 0,
                    "combo_total": 10,
                    "phase": "detect",
                    "phase_label": "检测 daily",
                    "phase_index": 1,
                    "phase_total": 1,
                    "summary": {},
                },
                {
                    "job_id": "job-2",
                    "status": "completed",
                    "mode": "sweep",
                    "progress": 1.0,
                    "total_stocks": 5196,
                    "processed_stocks": 5196,
                    "combo_index": 10,
                    "combo_total": 10,
                    "summary": {},
                },
            ]

        def get_job_status(self, job_id: str) -> dict[str, Any]:
            _ = job_id
            if self._idx >= len(self._statuses):
                return self._statuses[-1]
            status = self._statuses[self._idx]
            self._idx += 1
            return status

        def get_logs(self, job_id: str, level: str, after_log_id: int = 0) -> list[dict[str, Any]]:
            _ = (job_id, level, after_log_id)
            return []

    async def _collect_events() -> list[str]:
        async def _never_disconnect() -> bool:
            return False

        request = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(backtest_manager=_FakeManager())),
            query_params={},
            is_disconnected=_never_disconnect,
        )
        response = await backtest_event_stream("job-2", request)
        events: list[str] = []
        async for chunk in response.body_iterator:
            events.append(chunk if isinstance(chunk, str) else chunk.decode("utf-8"))
        return events

    async def _fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("app.api.routes.asyncio.sleep", _fake_sleep)
    events = asyncio.run(_collect_events())
    job_status_events = [event for event in events if event.startswith("event: job-status")]

    assert len(job_status_events) == 3
    assert any('"total_stocks": 0' in event for event in job_status_events)
    assert any('"total_stocks": 5196' in event for event in job_status_events)
