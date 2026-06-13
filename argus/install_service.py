"""Install / uninstall Argus as a macOS launchd user agent.

Usage:
    argus-service install    # install and start
    argus-service uninstall  # stop and uninstall
    argus-service status     # show current state
    argus-service logs       # tail the log file
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

console = Console()

_LABEL = "com.argus.autopilot"
_PLIST_DIR = Path.home() / "Library" / "LaunchAgents"
_PLIST_PATH = _PLIST_DIR / f"{_LABEL}.plist"
_LOG_DIR = Path.home() / "Library" / "Logs" / "argus"


def _argus_executable() -> str:
    exe = shutil.which("argus")
    if exe:
        return exe
    # Fallback: find it relative to the current Python
    python = sys.executable
    candidate = Path(python).parent / "argus"
    if candidate.exists():
        return str(candidate)
    raise RuntimeError(
        "Cannot find the `argus` executable. Run `pip install -e .` first."
    )


def _project_dir() -> str:
    # The .env file lives in the project root; run argus from there
    here = Path(__file__).parent.parent
    if (here / ".env").exists() or (here / ".env.example").exists():
        return str(here)
    return str(Path.cwd())


def _build_plist(exe: str, working_dir: str) -> str:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    stdout_log = _LOG_DIR / "argus.log"
    stderr_log = _LOG_DIR / "argus.error.log"
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{_LABEL}</string>

    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
    </array>

    <key>WorkingDirectory</key>
    <string>{working_dir}</string>

    <!-- Restart automatically if it exits -->
    <key>KeepAlive</key>
    <true/>

    <!-- Wait 10 s before restart to avoid tight crash loops -->
    <key>ThrottleInterval</key>
    <integer>10</integer>

    <!-- Only run when the user is logged in (needed for Keychain access) -->
    <key>LimitLoadToSessionType</key>
    <string>Aqua</string>

    <key>StandardOutPath</key>
    <string>{stdout_log}</string>

    <key>StandardErrorPath</key>
    <string>{stderr_log}</string>

    <!-- Persist across reboots / logouts + re-login -->
    <key>RunAtLoad</key>
    <true/>

    <key>EnvironmentVariables</key>
    <dict>
        <!-- Inherit PATH so `argus` can find its dependencies -->
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:{Path(exe).parent}</string>
    </dict>
</dict>
</plist>
"""


def _launchctl(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["launchctl", *args], capture_output=True, text=True)


def cmd_install() -> None:
    exe = _argus_executable()
    cwd = _project_dir()

    _PLIST_DIR.mkdir(parents=True, exist_ok=True)
    plist_content = _build_plist(exe, cwd)
    _PLIST_PATH.write_text(plist_content, encoding="utf-8")
    os.chmod(_PLIST_PATH, 0o644)

    # Unload first in case it was already loaded
    _launchctl("unload", str(_PLIST_PATH))
    result = _launchctl("load", "-w", str(_PLIST_PATH))

    if result.returncode == 0:
        console.print(Panel(
            f"[green]✓ Argus service installed and started[/green]\n\n"
            f"[dim]Plist: {_PLIST_PATH}[/dim]\n"
            f"[dim]Logs:  {_LOG_DIR}/argus.log[/dim]\n"
            f"[dim]Errors:{_LOG_DIR}/argus.error.log[/dim]\n\n"
            f"Argus will start automatically after every login.",
            title="[bold cyan]argus-service install[/bold cyan]",
            border_style="green",
        ))
    else:
        console.print(f"[red]launchctl load failed:[/red] {result.stderr}")
        sys.exit(1)


def cmd_uninstall() -> None:
    if not _PLIST_PATH.exists():
        console.print("[dim]No Argus launchd agent found.[/dim]")
        return

    _launchctl("unload", "-w", str(_PLIST_PATH))
    _PLIST_PATH.unlink()
    console.print("[green]✓ Argus service stopped and uninstalled.[/green]")


def cmd_status() -> None:
    result = _launchctl("list", _LABEL)
    if result.returncode != 0 or "Could not find service" in result.stderr:
        console.print("[yellow]Argus launchd agent is not loaded.[/yellow]")
    else:
        console.print(Panel(
            result.stdout or "(no output)",
            title=f"[bold cyan]launchctl list {_LABEL}[/bold cyan]",
            border_style="cyan",
        ))


def cmd_logs() -> None:
    log = _LOG_DIR / "argus.log"
    if not log.exists():
        console.print(f"[dim]Log not found: {log}[/dim]")
        return
    subprocess.run(["tail", "-f", str(log)])


def main() -> None:
    if sys.platform != "darwin":
        console.print(
            "[yellow]argus-service is for macOS (launchd).[/yellow]\n"
            "On Linux use systemd; see the README for a unit file template."
        )
        sys.exit(1)

    args = sys.argv[1:]
    cmd = args[0] if args else "status"

    dispatch = {
        "install":   cmd_install,
        "uninstall": cmd_uninstall,
        "status":    cmd_status,
        "logs":      cmd_logs,
    }

    if cmd not in dispatch:
        console.print(f"[red]Unknown command:[/red] {cmd}")
        console.print("Usage: argus-service [install|uninstall|status|logs]")
        sys.exit(1)

    dispatch[cmd]()


if __name__ == "__main__":
    main()
