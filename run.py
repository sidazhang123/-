"""
服务启动脚本。

职责：
1. 负责启动 Uvicorn，并加载 `app.main:app`。
2. 保持监听地址固定为 `0.0.0.0`，确保本机与局域网均可访问。
3. 自动探测当前机器可用的 WLAN/以太网 IPv4，仅用于命令行展示访问 URL。

维护要点：
1. 任何“显示地址”相关改动不得影响实际绑定地址。
2. 地址探测需兼容 PowerShell 与 ipconfig 两种来源。
"""

from __future__ import annotations

# 说明：
# 1. 服务始终绑定 0.0.0.0，保证本机 localhost 与局域网访问都可用；
# 2. WLAN/以太网 IPv4 自动探测仅用于 CLI 展示，不改监听地址；
# 3. IPv4 选择逻辑支持私网优先、链路本地兜底，避免只适配单一前缀场景。
import ipaddress
import re
import subprocess

_IPV4_PATTERN = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


def _ipv4_lan_priority(ip: str) -> int | None:
    """给 IPv4 打优先级，数值越小越适合局域网展示。"""

    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return None
    if addr.version != 4:
        return None
    if addr.is_unspecified or addr.is_loopback or addr.is_multicast:
        return None
    # 常见局域网地址（RFC1918）
    if addr.is_private:
        return 0
    # 链路本地地址（APIPA），可用于部分直连场景，作为次优兜底
    if addr.is_link_local:
        return 1
    # 其它非全局地址（如运营商 NAT 段）继续作为低优先级候选
    if not addr.is_global:
        return 2
    # 全局地址作为最后候选
    return 3


def _interface_priority(interface_alias: str) -> int:
    """网卡优先级：WLAN/Wi-Fi 优先，其次以太网，其它最后。"""

    token = (interface_alias or "").upper()
    if any(hint in token for hint in ("WLAN", "WI-FI", "WI FI", "WIRELESS")) or ("无线" in interface_alias):
        return 0
    if any(hint in token for hint in ("ETHERNET", "LAN")) or ("以太网" in interface_alias):
        return 1
    return 2


def _pick_best_lan_ipv4(candidates: list[tuple[str, str]]) -> str | None:
    """从候选 (网卡名, IP) 中选择最优局域网 IPv4。"""

    best_rank: tuple[int, int, int] | None = None
    best_ip: str | None = None
    for idx, (alias, ip) in enumerate(candidates):
        ip_rank = _ipv4_lan_priority(ip)
        if ip_rank is None:
            continue
        rank = (ip_rank, _interface_priority(alias), idx)
        if best_rank is None or rank < best_rank:
            best_rank = rank
            best_ip = ip
    return best_ip


def _run_command(command: list[str], timeout_sec: int = 15) -> str:
    """执行系统命令并返回标准输出，失败时返回空字符串。"""

    try:
        proc = subprocess.run(
            command,
            capture_output=True,
            text=True,
            errors="ignore",
            timeout=timeout_sec,
            check=False,
        )
        return proc.stdout or ""
    except Exception:
        return ""


def _detect_wlan_ipv4_by_powershell() -> str | None:
    """通过 PowerShell 读取网卡 IPv4，并按局域网优先级择优。"""

    script = (
        "Get-NetIPConfiguration "
        "| Where-Object { $_.IPv4Address } "
        "| ForEach-Object { "
        "  $alias = $_.InterfaceAlias; "
        "  $_.IPv4Address | ForEach-Object { \"$alias|$($_.IPAddress)\" } "
        "}"
    )
    output = _run_command(["powershell", "-NoProfile", "-Command", script])
    if not output:
        return None
    candidates: list[tuple[str, str]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        alias = ""
        payload = line
        if "|" in line:
            alias, payload = line.split("|", 1)
        for ip in _IPV4_PATTERN.findall(payload):
            candidates.append((alias.strip(), ip))
    return _pick_best_lan_ipv4(candidates)


def _detect_wlan_ipv4_by_ipconfig() -> str | None:
    """通过 ipconfig 兜底读取网卡 IPv4，并按局域网优先级择优。"""

    output = _run_command(["ipconfig"])
    if not output:
        return None

    current_alias = ""
    candidates: list[tuple[str, str]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # 仅识别“非缩进 + 冒号结尾”的行作为网卡块头。
        # 避免把缩进字段（如 Connection-specific DNS Suffix:）误判为新块。
        is_block_header = bool(raw_line) and (not raw_line[0].isspace()) and raw_line.rstrip().endswith(":")
        if is_block_header:
            current_alias = line.rstrip(":").strip()
            continue

        for ip in _IPV4_PATTERN.findall(line):
            candidates.append((current_alias, ip))

    return _pick_best_lan_ipv4(candidates)


def detect_wlan_ipv4() -> str | None:
    """
    探测最适合作为“局域网访问提示”的 IPv4 地址。

    流程：
    1. 先尝试 PowerShell 枚举网卡地址；
    2. 失败后回退到 `ipconfig` 文本解析；
    3. 对候选地址按“地址类型 + 网卡类型 + 出现顺序”综合打分。
    """

    ip = _detect_wlan_ipv4_by_powershell()
    if ip:
        return ip
    return _detect_wlan_ipv4_by_ipconfig()


def main() -> None:
    """应用入口。将 `app.main` 的导入放在函数内以降低多进程导入副作用。"""

    import uvicorn
    from app.main import app
    from app.settings import APP_SHUTDOWN_GRACE_SECONDS

    bind_host = "0.0.0.0"
    lan_ip = detect_wlan_ipv4()

    # 仅改变 CLI 展示，不改变实际监听地址。
    if lan_ip:
        print(f"Server URL (LAN): http://{lan_ip}:8000")
    print("Server URL (Local): http://127.0.0.1:8000")
    print(f"Server bind host: {bind_host}:8000")
    uvicorn.run(
        app,
        host=bind_host,
        port=8000,
        timeout_graceful_shutdown=max(1, int(APP_SHUTDOWN_GRACE_SECONDS)),
        log_config=None,
    )


if __name__ == "__main__":
    main()

