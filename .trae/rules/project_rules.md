# Trae 项目规则配置

## 默认启用的 Skills

本项目的默认规则定义在 `my-global-requirements` skill 中，该 skill 会在所有任务中自动应用。

### 全局 Skills

| Skill | 描述 |
|-------|------|
| my-global-requirements | 全局执行规范（需求澄清、变更兼容、中文注释、前端美观、参数配置化、本地静态依赖、测试策略、脚本运行环境、Python本地解释器） |

### 条件触发 Skills

| 触发短语 | Skill | 描述 |
|---------|-------|------|
| 整理工程 | organize-project | 扫描全工程形成整体认知，清理冗余函数、变量、代码和可安全删除的 Python 文件，做代码整洁化，并把函数注释、README、HANDOFF 更新到当前实现版本 |

### 应用规则

- `my-global-requirements` 会在 **所有任务** 中默认启用
- 当用户明确要求忽略或关闭本 Skill 时，暂停执行本 Skill 规范
- 在遵守上级系统/开发者/用户指令优先级的前提下执行本 Skill
- 当用户说“整理工程”时，额外启用 `organize-project` skill
