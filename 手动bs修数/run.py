"""中文说明：提供串行调用缺口任务生成与数据拉取的本地启动入口。

输入：直接运行本文件时，优先复用同目录现有任务文件；仅在任务文件不存在时生成缺口任务，再执行 get_bs_data。
输出：生成任务仅写入 JSON，控制台仅打印拉取结果摘要。
用途：为本地手工运行提供一个清晰的单文件入口，不改变 get_bs_data.py 的对外接口。
边界条件：本文件仅负责串联调用，不额外处理任务文件缺失、数据库锁或网络异常，这些异常会向上抛出。
"""

from __future__ import annotations

from pathlib import Path

from get_bs_data import get_bs_data, make_inconsistent_tasks,make_full_tasks

MAIN_DB_PATH = Path(r"D:\quant.duckdb")
TASKS_FILE = Path(__file__).with_name("bs_pending_tasks.json")
#所有数据的起点，用于向前补全和重新全部获取
EARLIEST_DATE = "2018-12-01"

def main() -> None:
    # 如果要重新检测duckdb重跑，先【手动】删除旧的任务json文件，然后用这段
    if not TASKS_FILE.exists():
        make_inconsistent_tasks(main_db_path=MAIN_DB_PATH, earliest_date=EARLIEST_DATE)

    # 如果要重新populate data，先【手动】删除旧的任务json文件，然后用这段
    # if not TASKS_FILE.exists():
    #     make_full_tasks(main_db_path=MAIN_DB_PATH, earliest_date=EARLIEST_DATE)

    result = get_bs_data(main_db_path=MAIN_DB_PATH)
    print({"result": result})
    


if __name__ == "__main__":
    main()