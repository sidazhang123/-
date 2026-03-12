"""
FastAPI 应用装配入口。

职责：
1. 初始化日志系统、状态库、策略注册中心与任务管理器。
2. 挂载静态资源目录并注册 API 路由。
3. 提供前端页面入口（监控页、结果页）所需的静态文件响应。

说明：
1. 该模块负责应用生命周期对象的装配，不包含策略业务逻辑。
2. 如新增中间件或生命周期钩子，应优先在本模块集中管理。
"""

from __future__ import annotations

import json
import logging
import multiprocessing
from logging.handlers import RotatingFileHandler
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from app.api.routes import router as api_router
from app.db.state_db import StateDB
from app.services.concept_manager import ConceptManager
from app.services.maintenance_manager import MaintenanceManager
from app.services.maintenance_logger import compact_maintenance_detail_for_app_log
from app.services.strategy_registry import StrategyRegistry
from app.services.task_manager import TaskManager
from app.settings import (
    APP_DIR,
    APP_SHUTDOWN_GRACE_SECONDS,
    APP_STARTUP_PREWARM_PARALLEL_FETCHER_ENABLED,
    LOG_DIR,
    LOG_FILE,
    MAINTENANCE_LOG_APP_COMPACT_ENABLED,
    SPECIALIZED_ENGINE_MAX_WORKERS,
    STATE_DB_PATH,
    STATIC_DIR,
)


def _attach_handler_once(target_logger: logging.Logger, handler: logging.Handler) -> None:
    """
    输入：
    1. target_logger: 目标日志器。
    2. handler: 待挂载的日志处理器。
    输出：
    1. 无返回值。
    用途：
    1. 为指定日志器去重挂载处理器，避免重复输出同一条日志。
    边界条件：
    1. 若处理器已存在则直接跳过，不做重复添加。
    """
    if handler in target_logger.handlers:
        return
    target_logger.addHandler(handler)


def _configure_uvicorn_loggers(file_handler: logging.Handler, stream_handler: logging.Handler) -> None:
    """
    输入：
    1. file_handler: 文件日志处理器。
    2. stream_handler: 控制台日志处理器。
    输出：
    1. 无返回值。
    用途：
    1. 将 Uvicorn 日志接入应用日志体系，并让 access log 仅写文件不输出到 CLI。
    边界条件：
    1. 需要在 Uvicorn 自身日志配置接管前执行；重复调用时保持幂等。
    """
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.setLevel(logging.INFO)
    uvicorn_logger.propagate = False
    _attach_handler_once(uvicorn_logger, file_handler)
    _attach_handler_once(uvicorn_logger, stream_handler)

    uvicorn_error_logger = logging.getLogger("uvicorn.error")
    uvicorn_error_logger.setLevel(logging.INFO)
    uvicorn_error_logger.propagate = False
    _attach_handler_once(uvicorn_error_logger, file_handler)
    _attach_handler_once(uvicorn_error_logger, stream_handler)

    uvicorn_access_logger = logging.getLogger("uvicorn.access")
    uvicorn_access_logger.setLevel(logging.INFO)
    uvicorn_access_logger.propagate = False
    _attach_handler_once(uvicorn_access_logger, file_handler)


def setup_logging() -> logging.Logger:
    """
    输入：
    1. 无显式输入参数。
    输出：
    1. 返回应用主日志器 `screening_app`。
    用途：
    1. 初始化滚动文件日志和控制台日志，并把 Uvicorn 日志接入同一套处理器。
    边界条件：
    1. 若日志器已完成初始化，则直接复用现有处理器，避免重复输出。
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("screening_app")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")

    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=20 * 1024 * 1024, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    _configure_uvicorn_loggers(file_handler, stream_handler)

    return logger


logger = setup_logging()

app = FastAPI(title="A股多周期量价筛选系统", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

state_db = StateDB(STATE_DB_PATH)
state_db.init_schema()
strategy_registry = StrategyRegistry()

task_manager = TaskManager(state_db=state_db, app_logger=logger, strategy_registry=strategy_registry)
maintenance_manager = MaintenanceManager(state_db=state_db, app_logger=logger)
concept_manager = ConceptManager(state_db=state_db, app_logger=logger)
app.state.state_db = state_db
app.state.task_manager = task_manager
app.state.maintenance_manager = maintenance_manager
app.state.concept_manager = concept_manager
app.state.strategy_registry = strategy_registry
app.state.app_logger = logger

app.include_router(api_router)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _on_parallel_fetcher_log(level: str, message: str, detail: dict[str, Any] | None = None) -> None:
    """
    输入：
    1. level: zsdtdx 并行抓取器日志级别。
    2. message: 日志消息文本。
    3. detail: 可选结构化明细。
    输出：
    1. 无返回值。
    用途：
    1. 将 zsdtdx.parallel_fetcher 日志统一写入 screening_app（CLI + app.log）。
    边界条件：
    1. detail 不可序列化时自动降级为字符串拼接。
    """
    text = str(message)
    compact_detail = detail if isinstance(detail, dict) else None
    if MAINTENANCE_LOG_APP_COMPACT_ENABLED:
        compact_detail = compact_maintenance_detail_for_app_log(message=text, detail=compact_detail)
    if compact_detail:
        try:
            text = f"{text} | {json.dumps(compact_detail, ensure_ascii=False, default=str)}"
        except Exception:
            text = f"{text} | {compact_detail}"
    text = f"[zsdtdx.parallel] {text}"

    token = str(level or "info").strip().lower()
    if token == "error":
        logger.error(text)
        return
    if token in {"warn", "warning"}:
        logger.warning(text)
        return
    logger.info(text)


@app.on_event("startup")
def on_startup() -> None:
    """
    输入：
    1. 无显式输入参数。
    输出：
    1. 无返回值。
    用途：
    1. 在服务启动阶段按配置完成 zsdtdx 并行进程池及连接预热。
    边界条件：
    1. 开关关闭时跳过预热但保留基础配置装配。
    2. 任一预热异常直接上抛，阻断服务启动。
    """
    from zsdtdx.parallel_fetcher import set_log_callback
    from zsdtdx.simple_api import get_fetcher, prewarm_parallel_fetcher, set_config_path

    zsdtdx_config_path = str(APP_DIR / "zsdtdx_config.yaml")
    resolved = set_config_path(zsdtdx_config_path)
    logger.info("zsdtdx 配置路径已设置: %s", resolved)

    set_log_callback(_on_parallel_fetcher_log)
    target_workers = max(1, int(SPECIALIZED_ENGINE_MAX_WORKERS))
    if not APP_STARTUP_PREWARM_PARALLEL_FETCHER_ENABLED:
        logger.info(
            "zsdtdx 并行抓取器启动预热已跳过 | %s",
            json.dumps(
                {
                    "startup_prewarm_parallel_fetcher_enabled": False,
                    "configured_workers": target_workers,
                },
                ensure_ascii=False,
            ),
        )
        return

    try:
        fetcher = get_fetcher()
        previous_workers = max(1, int(getattr(fetcher, "num_processes", target_workers)))
        fetcher.num_processes = target_workers
        logger.info(
            "zsdtdx 并行抓取器进程数已设置 | %s",
            json.dumps(
                {
                    "configured_workers": target_workers,
                    "previous_workers": previous_workers,
                    "inproc_workers": int(getattr(fetcher, "task_chunk_inproc_future_workers", 1)),
                },
                ensure_ascii=False,
            ),
        )
        summary = prewarm_parallel_fetcher()
    except Exception as exc:
        logger.exception("zsdtdx 并行抓取器预热失败，服务启动终止: %s", exc)
        raise
    logger.info(
        "zsdtdx 并行抓取器预热完成 | %s",
        json.dumps(summary, ensure_ascii=False, default=str),
    )


@app.get("/")
def index() -> FileResponse:
    """
    返回监控页首页 `index.html`。
    """
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/results")
def results_page() -> FileResponse:
    """
    返回结果页 `results.html`。
    """
    return FileResponse(STATIC_DIR / "results.html")


@app.get("/maintenance")
def maintenance_page() -> FileResponse:
    """
    返回维护页 `maintenance.html`。
    """
    return FileResponse(STATIC_DIR / "maintenance.html")


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    """
    输入：
    1. 无显式输入参数。
    输出：
    1. 返回 204 空响应。
    用途：
    1. 避免浏览器默认请求 `/favicon.ico` 产生 404 控制台错误。
    边界条件：
    1. 当前项目未提供站点图标文件，统一返回空内容。
    """
    return Response(status_code=204)


@app.on_event("shutdown")
def on_shutdown() -> None:
    """
    输入：
    1. 无显式输入参数。
    输出：
    1. 无返回值。
    用途：
    1. 在停机阶段依次关闭维护任务、筛选任务、zsdtdx 并行抓取器与状态库连接。
    边界条件：
    1. 各清理步骤都做异常兜底，避免单个清理失败阻断整体停机流程。
    """
    try:
        maintenance_manager.shutdown(
            grace_seconds=APP_SHUTDOWN_GRACE_SECONDS,
            forced_status="failed",
        )
    except Exception as exc:
        logger.exception("maintenance_manager shutdown 失败: %s", exc)

    try:
        task_manager.shutdown(
            grace_seconds=APP_SHUTDOWN_GRACE_SECONDS,
            forced_status="failed",
        )
    except Exception as exc:
        logger.exception("task_manager shutdown 失败: %s", exc)
    # 优雅销毁 zsdtdx 并行进程池，释放 worker 连接与进程资源。
    try:
        from zsdtdx.simple_api import destroy_parallel_fetcher
        summary = destroy_parallel_fetcher()
        logger.info("zsdtdx 并行拣取器已销毁 | %s", json.dumps(summary, ensure_ascii=False, default=str))
    except Exception as exc:
        logger.warning("zsdtdx 并行拣取器销毁异常: %s", exc)
    # 兜底清理本进程派生的子进程，避免 Ctrl+C 后残留 worker 占用数据库句柄。
    try:
        children = multiprocessing.active_children()
        if children:
            logger.warning("检测到残留子进程，开始清理: %s", [c.pid for c in children if c is not None])
        for child in children:
            try:
                child.terminate()
                child.join(timeout=5)
                if child.is_alive():
                    child.kill()
                    child.join(timeout=3)
            except Exception as exc:
                logger.exception("子进程清理失败: pid=%s error=%s", getattr(child, "pid", None), exc)
    except Exception as exc:
        logger.exception("枚举子进程失败: %s", exc)

    state_db.close()
