"""
simple_api 工程内包装层。

职责：
1. 统一隔离 zsdtdx.simple_api 的导入与可用性判断。
2. 为 get_stock_kline 提供自动清理并行抓取器的上下文封装。
3. 避免业务层重复编写 destroy_parallel_fetcher 收尾逻辑。
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

try:  # pragma: no cover - 运行时依赖环境
    from zsdtdx.simple_api import destroy_parallel_fetcher
    from zsdtdx.simple_api import get_client as get_tdx_client
    from zsdtdx.simple_api import get_runtime_failures, get_runtime_metadata
    from zsdtdx.simple_api import get_stock_code_name, get_stock_kline
except Exception:  # pragma: no cover
    destroy_parallel_fetcher = None
    get_tdx_client = None
    get_runtime_failures = None
    get_runtime_metadata = None
    get_stock_code_name = None
    get_stock_kline = None


def destroy_parallel_fetcher_safely() -> dict[str, Any] | None:
    """
    输入：
    1. 无。
    输出：
    1. 销毁摘要；接口不可用或销毁失败时返回 None。
    用途：
    1. 主动释放 simple_api 并行抓取器的子进程与连接资源。
    边界条件：
    1. 外层调用方若需要记录日志，应自行处理返回值与异常摘要。
    """

    if destroy_parallel_fetcher is None:
        return None
    try:
        summary = destroy_parallel_fetcher()
    except Exception:
        return None
    return summary if isinstance(summary, dict) else None


@contextmanager
def managed_stock_kline_job(
    *,
    task: list[dict[str, Any]],
    mode: str,
    queue: Any = None,
    preprocessor_operator: Any = None,
) -> Iterator[Any]:
    """
    输入：
    1. task/mode/queue/preprocessor_operator: 透传给 simple_api.get_stock_kline。
    输出：
    1. 产出 simple_api 返回的 job 句柄。
    用途：
    1. 统一包装 get_stock_kline，确保调用完成后主动销毁并行抓取器。
    边界条件：
    1. 即使消费 job 过程中抛异常，也会在 finally 中尝试清理进程池。
    """

    if get_stock_kline is None:
        raise RuntimeError("zsdtdx.simple_api 不可用，请先安装并配置 zsdtdx")

    job = get_stock_kline(
        task=task,
        mode=mode,
        queue=queue,
        preprocessor_operator=preprocessor_operator,
    )
    try:
        yield job
    finally:
        destroy_parallel_fetcher_safely()