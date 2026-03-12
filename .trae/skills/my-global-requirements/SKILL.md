---
name: my-global-requirements
description: 默认用于所有 Codex 任务的执行规范。用于需求澄清、变更与兼容策略、中文注释、前端美观、参数配置化与本地静态依赖约束；仅在用户明确关闭时不执行。
---

# 我的全局要求

## 总则

- 默认在所有任务中应用本 Skill。
- 当用户明确要求忽略或关闭本 Skill 时，暂停执行本 Skill 规范。
- 在遵守上级系统/开发者/用户指令优先级的前提下执行本 Skill。

## 1) 需求澄清

- 不论是否在 Plan Mode，只要需求存在模糊或多解，先与用户对齐后再实施。
- 对齐内容包含：目标、范围、输出、兼容性与验收标准（按需补充）。
- 向用户提问或提供选项时需要解释问题或选项的背景、影响，不得在未解释的情况下在问题中展示对话未提及的缩写、术语、简写。

## 2) 变更与兼容

- 除非用户明确要求兼容，否则所有涉及改动的地方一律按目标态统一更新。
- 不保留中间状态或过渡逻辑。

## 3) 注释规范

- 每个代码文件必须包含模块级中文注释，说明模块用途与边界。
- 每个函数必须包含函数级中文注释，至少包含：输入、输出、用途、边界条件。
- 对明显自解释且极短函数可简化注释，但不得省略输入/输出与边界说明。
- 文档与注释只描述目标态，不表达改造过程或过渡方案。

## 4) 前端美观

- 涉及前端界面时，优先选择可维护、现代美观的 UI 实现。
- 根据任务复杂度选择成熟方案，避免一次性临时样式堆砌。
- 在不破坏现有设计系统前提下，保证桌面与移动端可用性。

## 5) 参数配置化

- 拒绝通过 CLI 参数暴露用户可调参数。
- 所有用户可调参数统一放入配置文件（例如 `config.yaml`）。
- 代码中读取配置文件并提供合理默认值与缺省处理。

## 6) 本地静态依赖

- 前端第三方库必须以本地静态资源形式提供。
- 禁止依赖用户访问时触发网络下载（如运行时 CDN 拉取）。
- 构建产物与部署包需包含运行所需的第三方静态资源。

## 7) 测试策略

- 仅涉及"改命名 / 改文档 / 改注释"的简单需求，实施完毕后不进行测试。

## 8) 脚本运行环境

- Agent 模式自动运行 PowerShell 脚本时，必须在每条命令前使用统一前置，避免中文乱码：
  - `[Console]::InputEncoding=[System.Text.UTF8Encoding]::new($false)`
  - `[Console]::OutputEncoding=[System.Text.UTF8Encoding]::new($false)`
  - `$OutputEncoding=[System.Text.UTF8Encoding]::new($false)`
  - `$PSDefaultParameterValues['*:Encoding']='utf8'`
  - `chcp 65001 > $null`
  - `$env:PYTHONIOENCODING='utf-8'`
- 需要"临时 NO PROXY"时，仅对当前命令进程设置，且与 UTF-8 前置同时出现：
  - `$env:NO_PROXY='*'; $env:no_proxy='*'`
  - `$env:HTTP_PROXY=''; $env:http_proxy=''; $env:HTTPS_PROXY=''; $env:https_proxy=''`

## 9) Python 本地解释器

- 确保 Python 脚本使用本地系统解释器直接运行，不创建虚拟环境。
- 直接使用 `python` 命令运行脚本，不使用 `python -m venv` 或 `virtualenv` 创建虚拟环境。
- 不激活 `.venv`、`venv` 或类似的虚拟环境目录。
- 依赖系统安装的 Python（如 PATH 中的 `python.exe`）。
- 运行 pytest 等测试框架时使用 `python -m pytest tests/`。
