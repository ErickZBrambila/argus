"""Interactive CLI for storing and rotating Argus secrets in the OS keychain.

Run with:
    argus-setup          # store / update all secrets
    argus-setup --show   # show what is currently stored (masked)
    argus-setup --clear  # remove all Argus secrets from keychain
"""

from __future__ import annotations

import getpass
import sys
from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from argus.secrets import (
    SECRET_KEYS,
    delete_secret,
    get_secret,
    list_stored,
    set_secret,
    _SERVICE,
)

console = Console()

_DESCRIPTIONS: dict[str, str] = {
    "ANTHROPIC_API_KEY":   "Anthropic API key  (required)",
    "GEMINI_API_KEY":      "Google Gemini API key  (optional — enables ensemble AI voting)",
    "ROBINHOOD_PASSWORD":  "Robinhood password  (required)",
    "ROBINHOOD_MFA_SECRET":"Robinhood TOTP secret  (optional — leave blank to skip)",
    "SMTP_PASSWORD":       "SMTP / Gmail app password  (optional)",
    "TWILIO_AUTH_TOKEN":   "Twilio auth token  (optional)",
    "SLACK_BOT_TOKEN":     "Slack bot token  (optional)",
}


def _mask(value: Optional[str]) -> str:
    if not value:
        return "[dim]not set[/dim]"
    visible = min(4, len(value) // 4)
    return f"[green]{value[:visible]}{'•' * 12}[/green]"


def _show_status() -> None:
    stored = list_stored()
    t = Table(box=box.SIMPLE_HEAVY, show_edge=False)
    t.add_column("Secret", style="cyan")
    t.add_column("Description", style="dim")
    t.add_column("Status", justify="center")

    for key in SECRET_KEYS:
        desc = _DESCRIPTIONS.get(key, "")
        if stored[key]:
            status = "[green]✓ stored[/green]"
        else:
            status = "[yellow]— not set[/yellow]"
        t.add_row(key, desc, status)

    console.print(Panel(t, title="[bold cyan]Argus Keychain Status[/bold cyan]", border_style="cyan"))
    console.print(f"[dim]Service name in keychain: [cyan]{_SERVICE}[/cyan][/dim]\n")


def _prompt_secret(key: str, existing: Optional[str]) -> Optional[str]:
    desc = _DESCRIPTIONS.get(key, key)
    optional = "(optional)" in desc

    if existing:
        console.print(f"  [cyan]{key}[/cyan]  {_mask(existing)}")
        answer = console.input("    [dim]Press Enter to keep, or type new value: [/dim]")
        return answer.strip() if answer.strip() else None   # None = keep existing
    else:
        console.print(f"  [cyan]{key}[/cyan]  [dim]{desc}[/dim]")
        if optional:
            value = console.input("    [dim]Value (Enter to skip): [/dim]")
        else:
            value = getpass.getpass("    Value: ")
        return value.strip() or None


def _run_setup() -> None:
    console.print(Panel(
        "[bold]Argus secrets will be stored in your OS keychain[/bold]\n"
        "[dim]macOS Keychain  •  Windows Credential Manager  •  Linux Secret Service[/dim]",
        border_style="cyan",
    ))

    _show_status()

    console.print("[bold]Enter secrets below.[/bold] Press Enter to keep existing values.\n")

    changed = 0
    for key in SECRET_KEYS:
        existing = get_secret(key)
        new_value = _prompt_secret(key, existing)
        if new_value is not None:
            set_secret(key, new_value)
            console.print(f"  [green]✓[/green] Saved [cyan]{key}[/cyan]")
            changed += 1
        console.print()

    if changed:
        console.print(f"[green]✓ {changed} secret(s) saved to keychain.[/green]")
    else:
        console.print("[dim]No changes made.[/dim]")

    console.print("\n[bold]Tip:[/bold] Your [cyan].env[/cyan] file only needs non-secret config now.")
    console.print("[dim]Remove these lines from .env if present:[/dim]")
    for key in SECRET_KEYS:
        console.print(f"  [dim]{key}=...[/dim]")


def _run_clear() -> None:
    stored = list_stored()
    to_delete = [k for k, v in stored.items() if v]
    if not to_delete:
        console.print("[dim]No Argus secrets found in keychain.[/dim]")
        return

    console.print(f"[red]This will delete {len(to_delete)} secret(s) from the keychain:[/red]")
    for k in to_delete:
        console.print(f"  [cyan]{k}[/cyan]")

    confirm = console.input("\nType [bold]yes[/bold] to confirm: ")
    if confirm.strip().lower() != "yes":
        console.print("[dim]Aborted.[/dim]")
        return

    for k in to_delete:
        delete_secret(k)
        console.print(f"  [red]✗[/red] Deleted [cyan]{k}[/cyan]")
    console.print("[green]Done.[/green]")


def main() -> None:
    args = sys.argv[1:]

    if "--show" in args or "-s" in args:
        _show_status()
    elif "--clear" in args or "-c" in args:
        _run_clear()
    else:
        _run_setup()


if __name__ == "__main__":
    main()
