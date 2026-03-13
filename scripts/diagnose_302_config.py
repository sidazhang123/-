"""
诊断脚本：验证 zsdtdx set_config_path 对 302 前缀股票的支持。

用法：在工程根目录执行 `py scripts/diagnose_302_config.py`

目的：
1. 验证 sync 模式下 set_config_path 后能否正确识别 sz.302132。
2. 如果 sync 能识别 → 问题在 parallel worker 进程配置传播。
3. 如果 sync 也失败 → 问题在 zsdtdx 核心逻辑。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# 确保工程根目录在 sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))


def main() -> None:
    from zsdtdx.simple_api import get_client, get_stock_code_name, get_stock_kline, set_config_path

    config_path = str(project_root / "app" / "zsdtdx_config.yaml")

    # 步骤 1：设置配置路径
    resolved = set_config_path(config_path)
    print(f"[1] set_config_path => {resolved}")

    # 步骤 2：检查 get_stock_code_name 是否包含 302132
    with get_client() as _client:
        stock_map = get_stock_code_name(use_cache=False)

    found_302 = {code: name for code, name in (stock_map or {}).items() if str(code).startswith("sz.302") or str(code).startswith("302")}
    print(f"[2] get_stock_code_name 中 302 前缀股票: {json.dumps(found_302, ensure_ascii=False) if found_302 else '无'}")

    # 步骤 3：sync 模式拉取 sz.302132 k线
    task = [
        {
            "code": "sz.302132",
            "freq": "d",
            "start_time": "2026-03-01",
            "end_time": "2026-03-13",
        }
    ]
    print(f"[3] sync 模式拉取 sz.302132 日线...")
    try:
        job = get_stock_kline(task=task, mode="sync")
        result = job.result() if hasattr(job, "result") else job
        # 检查 queue 中的事件
        events = []
        if hasattr(job, "queue"):
            import queue as queue_mod

            while True:
                try:
                    ev = job.queue.get_nowait()
                    events.append(ev)
                except queue_mod.Empty:
                    break
        if events:
            for ev in events:
                event_type = ev.get("event") if isinstance(ev, dict) else None
                error = ev.get("error") if isinstance(ev, dict) else None
                rows = ev.get("rows") if isinstance(ev, dict) else None
                row_count = len(rows) if isinstance(rows, list) else 0
                print(f"    event={event_type}, error={error}, rows={row_count}")
        else:
            print(f"    result type={type(result).__name__}")
            if isinstance(result, list):
                print(f"    返回 {len(result)} 条记录")
                if result:
                    print(f"    首行: {result[0]}")
            elif isinstance(result, dict):
                print(f"    返回: {json.dumps(result, ensure_ascii=False, default=str)[:500]}")
            else:
                print(f"    返回: {str(result)[:500]}")
        print("[3] sync 拉取完成 ✓")
    except Exception as exc:
        print(f"[3] sync 拉取失败 ✗: {type(exc).__name__}: {exc}")

    # 步骤 4：对比 — 用一个确定存在的股票做 sync 拉取
    task_ok = [
        {
            "code": "sz.000001",
            "freq": "d",
            "start_time": "2026-03-10",
            "end_time": "2026-03-13",
        }
    ]
    print(f"[4] sync 对照拉取 sz.000001 日线...")
    try:
        job_ok = get_stock_kline(task=task_ok, mode="sync")
        result_ok = job_ok.result() if hasattr(job_ok, "result") else job_ok
        events_ok = []
        if hasattr(job_ok, "queue"):
            import queue as queue_mod

            while True:
                try:
                    ev = job_ok.queue.get_nowait()
                    events_ok.append(ev)
                except queue_mod.Empty:
                    break
        if events_ok:
            for ev in events_ok:
                event_type = ev.get("event") if isinstance(ev, dict) else None
                error = ev.get("error") if isinstance(ev, dict) else None
                rows = ev.get("rows") if isinstance(ev, dict) else None
                row_count = len(rows) if isinstance(rows, list) else 0
                print(f"    event={event_type}, error={error}, rows={row_count}")
        else:
            if isinstance(result_ok, list):
                print(f"    返回 {len(result_ok)} 条记录")
            elif isinstance(result_ok, dict):
                print(f"    返回: {json.dumps(result_ok, ensure_ascii=False, default=str)[:500]}")
            else:
                print(f"    返回: {str(result_ok)[:500]}")
        print("[4] 对照拉取完成 ✓")
    except Exception as exc:
        print(f"[4] 对照拉取失败 ✗: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
