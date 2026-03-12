"""
测试公共初始化配置。

用途：
1. 统一将项目根目录加入 `sys.path`，确保 `app`、`strategies` 等包可被稳定导入。
边界：
1. 仅做导入路径初始化，不引入业务行为与测试夹具副作用。
"""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_workspace_on_path() -> None:
    """
    输入：
    1. 无显式参数，基于当前文件位置推导项目根目录。
    输出：
    1. 将项目根目录插入到 `sys.path` 首位（若尚未存在）。
    用途：
    1. 解决不同运行目录下 pytest 的模块导入一致性问题。
    边界条件：
    1. 当路径已存在时不重复插入，避免污染导入优先级。
    """

    workspace_root = Path(__file__).resolve().parents[1]
    root_text = str(workspace_root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)


_ensure_workspace_on_path()
