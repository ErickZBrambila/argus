"""Rich terminal dashboard for the Argus trading agent."""

from __future__ import annotations

import datetime
import logging
from typing import Optional

from rich import box
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

logger = logging.getLogger(__name__)

console = Console()

_ACCOUNT_COLORS = {
    "agentic": "cyan",
    "default": "magenta",
    "main": "cyan",
}


def _pnl_style(value: float) -> str:
    return "green" if value > 0 else "red" if value < 0 else "white"


class NullTerminalDashboard:
    """No-op dashboard for headless / Docker environments (ARGUS_NO_TERMINAL=1)."""
    def start(self) -> None: pass
    def stop(self) -> None: pass
    def update(self, state: dict) -> None: pass


class _LiveRenderable:
    """Calls _render() on every Rich Live refresh so the countdown ticks live."""
    def __init__(self, dashboard: "TerminalDashboard") -> None:
        self._d = dashboard

    def __rich_console__(self, console, options):  # type: ignore[override]
        yield self._d._render()


class TerminalDashboard:
    def __init__(self) -> None:
        self._live: Optional[Live] = None
        self._state: dict = _empty_state()

    def start(self) -> None:
        self._live = Live(
            _LiveRenderable(self),
            console=console,
            refresh_per_second=1,
            screen=True,
        )
        self._live.start()

    def stop(self) -> None:
        if self._live:
            self._live.stop()
            self._live = None

    def update(self, state: dict) -> None:
        self._state.update(state)  # _render() is called automatically by Live each second

    def _render(self) -> Panel:
        s = self._state

        # ── Header ──────────────────────────────────────────────────────────
        mode = Text(" PAPER ", style="bold white on blue") if s.get("paper_trade") else Text(" LIVE ", style="bold white on red")
        kill = Text(" ⚡ KILL SWITCH ", style="bold white on red") if s.get("kill_switch") else Text("")
        paused = Text(" ⏸ PAUSED ", style="bold black on yellow") if s.get("paused") else Text("")
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Market session + countdown
        _SESSION_STYLES = {
            "open": "bold green", "premarket": "bold blue",
            "afterhours": "bold yellow", "closed": "dim",
        }
        session = s.get("market_session", "closed")
        session_label = {
            "open": "OPEN", "premarket": "PRE", "afterhours": "AH", "closed": "CLOSED",
        }.get(session, session.upper())

        countdown_str = ""
        next_scan = s.get("next_scan_at")
        if next_scan:
            try:
                nxt = datetime.datetime.fromisoformat(next_scan)
                secs = max(0, int((nxt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()))
                m, sec = divmod(secs, 60)
                countdown_str = f"{m}:{sec:02d}" if m else f"{sec}s"
            except Exception:
                pass

        # Token cost line
        token = s.get("token_usage", {})
        token_str = ""
        if token.get("total_calls", 0) > 0:
            token_str = (
                f"Claude ${token.get('claude',{}).get('cost_usd',0):.4f} · "
                f"Gemini ${token.get('gemini',{}).get('cost_usd',0):.4f} · "
                f"Total ${token.get('total_cost_usd',0):.4f}"
            )

        from argus import __version__
        header = Text()
        header.append(f"⬡ ARGUS v{__version__}  ", style="bold cyan")
        header.append_text(mode)
        header.append_text(kill)
        header.append_text(paused)
        header.append(f" [{session_label}]", style=_SESSION_STYLES.get(session, "dim"))
        mc_label, mc_val = _market_countdown()
        header.append(f"  {mc_label} ", style="dim")
        header.append(mc_val, style="bold white")
        if countdown_str:
            header.append(f"  next scan {countdown_str}", style="dim")
        header.append(f"  {ts}", style="dim")
        if token_str:
            header.append(f"\n  {token_str}", style="dim")

        accounts = s.get("accounts", {})

        # ── Per-account panels ───────────────────────────────────────────────
        account_panels = []
        for label, acct in accounts.items():
            color = _ACCOUNT_COLORS.get(label, "cyan")
            account_panels.append(_build_account_panel(label, acct, color))

        # ── Signals table ────────────────────────────────────────────────────
        signals_table = _build_signals_table(s.get("signals", []))

        # ── Layout ───────────────────────────────────────────────────────────
        layout = Table.grid(expand=True)
        layout.add_column()
        layout.add_row(header)

        if account_panels:
            acct_row = Table.grid(expand=True, padding=(0, 1))
            for _ in account_panels:
                acct_row.add_column(ratio=1)
            acct_row.add_row(*account_panels)
            layout.add_row(acct_row)
        else:
            layout.add_row(_build_legacy_panel(s))

        layout.add_row(signals_table)
        layout.add_row(_build_log_panel())

        return Panel(layout, border_style="cyan", padding=(0, 1))


def _market_countdown() -> tuple[str, str]:
    """Return (label, value) for time until next market open or close."""
    now_et = datetime.datetime.now(datetime.timezone.utc).astimezone(
        datetime.timezone(datetime.timedelta(hours=-4))  # EDT; close enough for display
    )
    try:
        import zoneinfo
        now_et = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York"))
    except Exception:
        pass

    h, m, s = now_et.hour, now_et.minute, now_et.second
    dow = now_et.weekday()  # 0=Mon … 6=Sun
    sec = h * 3600 + m * 60 + s
    OPEN, CLOSE = 9 * 3600 + 30 * 60, 16 * 3600

    def fmt(secs: int) -> str:
        secs = max(0, secs)
        hh, rem = divmod(secs, 3600)
        mm, ss  = divmod(rem, 60)
        return f"{hh}h {mm:02d}m" if hh else f"{mm:02d}m {ss:02d}s"

    if dow >= 5:                        # weekend
        days = 7 - dow                  # days until Monday
        return "Opens in", fmt(days * 86400 + OPEN - sec)
    if sec < OPEN:
        return "Opens in", fmt(OPEN - sec)
    if sec < CLOSE:
        return "Closes in", fmt(CLOSE - sec)
    days = 3 if dow == 4 else 1         # Friday → skip to Monday
    return "Opens in", fmt(days * 86400 + OPEN - sec)


def _build_goal_bar(equity: float, goal: float, color: str) -> Text:
    pct = min(1.0, equity / goal) if goal > 0 else 0.0
    filled = int(pct * 20)
    bar = "█" * filled + "░" * (20 - filled)
    style = "green" if pct >= 1.0 else color
    t = Text()
    t.append(f"[{bar}] ", style=style)
    t.append(f"{pct*100:.1f}%", style=f"bold {style}")
    if pct < 1.0:
        t.append(f"  ${goal - equity:,.0f} to go", style="dim")
    else:
        t.append("  PDT lifted!", style="bold green")
    return t


def _build_account_panel(label: str, acct: dict, color: str) -> Panel:
    equity      = acct.get("equity", 0.0)
    daily_pnl   = acct.get("daily_pnl", 0.0)
    daily_pct   = acct.get("daily_pnl_pct", 0.0)
    day_trades  = acct.get("day_trades", 0)
    auto        = acct.get("auto_trade", True)
    pending     = acct.get("pending_approvals", 0)
    kill        = acct.get("kill_switch", False)

    # Stats grid
    stats = Table.grid(padding=(0, 2))
    stats.add_column(style="dim")
    stats.add_column()
    stats.add_row("Equity",    Text(f"${equity:,.2f}", style=f"bold {color}"))
    stats.add_row("Daily P&L", Text(f"${daily_pnl:+,.2f}  ({daily_pct:+.2f}%)", style=_pnl_style(daily_pnl)))
    stats.add_row("Day trades", Text(f"{day_trades} / 3", style="yellow" if day_trades >= 2 else "white"))
    mode_label = "AUTO" if auto else "APPROVAL"
    mode_style = f"bold {color}" if auto else "bold yellow"
    stats.add_row("Mode",      Text(mode_label, style=mode_style))
    if pending:
        stats.add_row("Pending",   Text(f"{pending} awaiting approval", style="bold yellow"))
    if kill:
        stats.add_row("",          Text("KILL SWITCH ACTIVE", style="bold red"))

    # Positions
    positions = acct.get("positions", {})
    pos_table = Table(box=box.SIMPLE_HEAVY, expand=True, show_edge=False, show_header=bool(positions))
    pos_table.add_column("Symbol", style=color)
    pos_table.add_column("Entry",   justify="right")
    pos_table.add_column("Now",     justify="right")
    pos_table.add_column("P&L%",    justify="right")
    if not positions:
        pos_table.add_row("[dim]No open positions[/dim]", "", "", "")
    else:
        for sym, pos in positions.items():
            pct = pos.get("unrealized_pnl_pct", 0.0)
            pos_table.add_row(
                sym,
                f"${pos.get('entry_price', 0):.2f}",
                f"${pos.get('current_price', 0):.2f}",
                Text(f"{pct:+.2f}%", style=_pnl_style(pct)),
            )

    # Recent trades
    trades = acct.get("trades", [])
    tr_table = Table(box=box.SIMPLE_HEAVY, expand=True, show_edge=False, show_header=bool(trades))
    tr_table.add_column("Time",   style="dim")
    tr_table.add_column("Symbol", style=color)
    tr_table.add_column("Side",   justify="center")
    tr_table.add_column("Price",  justify="right")
    if not trades:
        tr_table.add_row("[dim]No trades yet[/dim]", "", "", "")
    else:
        for t in trades[:5]:
            side = t.get("side", "")
            tr_table.add_row(
                t.get("time", ""),
                t.get("symbol", ""),
                Text(side.upper(), style="green" if side == "buy" else "red"),
                f"${t.get('price', 0):.2f}",
            )

    goal = acct.get("equity_goal", 25_000.0)

    inner = Table.grid(expand=True)
    inner.add_column()
    inner.add_row(stats)
    inner.add_row(Text("Positions", style="dim italic"))
    inner.add_row(pos_table)
    inner.add_row(Text("Trades", style="dim italic"))
    inner.add_row(tr_table)
    inner.add_row(Text(""))
    inner.add_row(Text("$25K Goal", style="dim"))
    inner.add_row(_build_goal_bar(equity, goal, color))

    title = f"[bold {color}]{label.upper()}[/bold {color}]"
    return Panel(inner, title=title, border_style=color, padding=(0, 1))


def _build_signals_table(signals: list) -> Table:
    t = Table(title="Signals", box=box.SIMPLE_HEAVY, expand=True, show_edge=False)
    t.add_column("Symbol", style="cyan")
    t.add_column("Price",  justify="right")
    t.add_column("RSI",    justify="right")
    t.add_column("MACD H", justify="right")
    t.add_column("Signal", justify="center")
    t.add_column("Conf",   justify="right")

    for s in signals:
        sig = s.get("composite", "neutral")
        sig_style = {"bullish": "green", "bearish": "red", "neutral": "yellow"}.get(sig, "white")
        rsi = s.get("rsi")
        macd_h = s.get("macd_hist")
        conf = s.get("confidence", 0.0)
        t.add_row(
            s.get("symbol", ""),
            f"${s.get('price', 0):.2f}",
            f"{rsi:.1f}" if rsi is not None else "—",
            f"{macd_h:.4f}" if macd_h is not None else "—",
            Text(sig.upper(), style=sig_style),
            f"{conf:.0%}",
        )
    return t


def _build_legacy_panel(s: dict) -> Table:
    """Fallback single-account layout when no per-account data available."""
    stats = Table.grid(padding=(0, 2))
    stats.add_column(style="dim")
    stats.add_column()
    stats.add_row("Equity",     Text(f"${s.get('equity', 0):,.2f}", style="bold white"))
    stats.add_row("Daily P&L",  Text(f"${s.get('daily_pnl', 0):+,.2f} ({s.get('daily_pnl_pct', 0):+.2f}%)", style=_pnl_style(s.get("daily_pnl", 0))))
    stats.add_row("Trades",     Text(str(s.get("trade_count", 0))))
    stats.add_row("Day trades", Text(f"{s.get('day_trades', 0)} / 3"))
    return stats


def _build_log_panel() -> Panel:
    try:
        from argus.dashboard.log_buffer import get_recent
        entries = get_recent(12)
    except Exception:
        entries = []

    _LEVEL_STYLES = {
        "DEBUG":    "dim",
        "INFO":     "white",
        "WARNING":  "yellow",
        "ERROR":    "bold red",
        "CRITICAL": "bold white on red",
    }

    t = Table.grid(expand=True, padding=(0, 1))
    t.add_column(style="dim", no_wrap=True, width=8)    # time
    t.add_column(no_wrap=True, width=4)                  # level
    t.add_column(style="#58a6ff", no_wrap=True, width=12)# name
    t.add_column()                                        # message

    if not entries:
        t.add_row("", "", "", Text("No log entries yet", style="dim"))
    else:
        for e in entries:
            lvl   = e.get("level", "INFO")
            style = _LEVEL_STYLES.get(lvl, "white")
            short = {"DEBUG": "DBG", "INFO": "INF", "WARNING": "WRN",
                     "ERROR": "ERR", "CRITICAL": "CRT"}.get(lvl, lvl[:3])
            t.add_row(
                e.get("ts", ""),
                Text(short, style=style),
                e.get("name", "")[:12],
                Text(e.get("msg", ""), style=style, overflow="fold"),
            )

    return Panel(t, title="[dim]Log[/dim]", border_style="dim", padding=(0, 1))


def _empty_state() -> dict:
    return {
        "paper_trade": True,
        "kill_switch": False,
        "paused": False,
        "equity": 0.0,
        "daily_pnl": 0.0,
        "daily_pnl_pct": 0.0,
        "trade_count": 0,
        "day_trades": 0,
        "positions": {},
        "signals": [],
        "recent_trades": [],
        "accounts": {},
    }
