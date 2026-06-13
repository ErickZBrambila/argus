"""Rich terminal dashboard for the Argus trading agent."""

from __future__ import annotations

import datetime
import logging
from typing import Optional

from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

logger = logging.getLogger(__name__)

console = Console()


def _pnl_color(value: float) -> str:
    if value > 0:
        return "green"
    if value < 0:
        return "red"
    return "white"


class TerminalDashboard:
    def __init__(self) -> None:
        self._live: Optional[Live] = None
        self._state: dict = _empty_state()

    def start(self) -> None:
        self._live = Live(
            self._render(),
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
        self._state.update(state)
        if self._live:
            self._live.update(self._render())

    def _render(self) -> Panel:
        s = self._state
        paper_badge = (
            Text(" PAPER ", style="bold white on blue")
            if s.get("paper_trade")
            else Text(" LIVE ", style="bold white on red")
        )
        kill_badge = (
            Text(" KILL SWITCH ACTIVE ", style="bold white on red") if s.get("kill_switch") else Text("")
        )
        paused_badge = Text(" PAUSED ", style="bold white on yellow") if s.get("paused") else Text("")

        header = Text()
        header.append("⬡ ARGUS", style="bold cyan")
        header.append("  ")
        header.append_text(paper_badge)
        header.append(" ")
        header.append_text(kill_badge)
        header.append_text(paused_badge)
        header.append(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", style="dim")

        equity = s.get("equity", 0.0)
        daily_pnl = s.get("daily_pnl", 0.0)
        daily_pnl_pct = s.get("daily_pnl_pct", 0.0)
        trade_count = s.get("trade_count", 0)
        day_trades = s.get("day_trades", 0)

        stats = Table.grid(padding=(0, 2))
        stats.add_column()
        stats.add_column()
        stats.add_row(
            Text("Portfolio equity", style="dim"),
            Text(f"${equity:,.2f}", style="bold white"),
        )
        stats.add_row(
            Text("Daily P&L", style="dim"),
            Text(f"${daily_pnl:+,.2f}  ({daily_pnl_pct:+.2f}%)", style=_pnl_color(daily_pnl)),
        )
        stats.add_row(
            Text("Trades today", style="dim"),
            Text(str(trade_count), style="white"),
        )
        stats.add_row(
            Text("Day trades (5-day)", style="dim"),
            Text(f"{day_trades} / 3", style="yellow" if day_trades >= 2 else "white"),
        )

        positions_table = _build_positions_table(s.get("positions", {}))
        signals_table = _build_signals_table(s.get("signals", []))
        recent_trades_table = _build_trades_table(s.get("recent_trades", []))

        grid = Table.grid(expand=True, padding=(0, 1))
        grid.add_column(ratio=1)
        grid.add_column(ratio=1)
        grid.add_row(positions_table, signals_table)
        grid.add_row(recent_trades_table, Panel(stats, title="Stats", border_style="dim"))

        layout = Table.grid(expand=True)
        layout.add_column()
        layout.add_row(header)
        layout.add_row(grid)

        return Panel(layout, border_style="cyan", padding=(0, 1))


def _build_positions_table(positions: dict) -> Table:
    t = Table(title="Open Positions", box=box.SIMPLE_HEAVY, expand=True, show_edge=False)
    t.add_column("Symbol", style="cyan")
    t.add_column("Qty", justify="right")
    t.add_column("Entry", justify="right")
    t.add_column("Current", justify="right")
    t.add_column("P&L %", justify="right")
    t.add_column("Stop", justify="right", style="dim")

    for sym, pos in positions.items():
        pnl_pct = pos.get("unrealized_pnl_pct", 0.0)
        t.add_row(
            sym,
            f"{pos.get('quantity', 0):.4f}",
            f"${pos.get('entry_price', 0):.4f}",
            f"${pos.get('current_price', 0):.4f}",
            Text(f"{pnl_pct:+.2f}%", style=_pnl_color(pnl_pct)),
            f"${pos.get('stop_loss_price', 0):.4f}",
        )
    return t


def _build_signals_table(signals: list) -> Table:
    t = Table(title="Signals", box=box.SIMPLE_HEAVY, expand=True, show_edge=False)
    t.add_column("Symbol", style="cyan")
    t.add_column("Price", justify="right")
    t.add_column("RSI", justify="right")
    t.add_column("MACD H", justify="right")
    t.add_column("Signal", justify="center")

    for s in signals:
        sig = s.get("composite", "neutral")
        sig_style = {"bullish": "green", "bearish": "red", "neutral": "yellow"}.get(sig, "white")
        rsi = s.get("rsi")
        rsi_str = f"{rsi:.1f}" if rsi is not None else "—"
        macd_h = s.get("macd_hist")
        macd_h_str = f"{macd_h:.4f}" if macd_h is not None else "—"
        t.add_row(
            s.get("symbol", ""),
            f"${s.get('price', 0):.4f}",
            rsi_str,
            macd_h_str,
            Text(sig.upper(), style=sig_style),
        )
    return t


def _build_trades_table(trades: list) -> Table:
    t = Table(title="Recent Trades", box=box.SIMPLE_HEAVY, expand=True, show_edge=False)
    t.add_column("Time", style="dim")
    t.add_column("Symbol", style="cyan")
    t.add_column("Side", justify="center")
    t.add_column("Qty", justify="right")
    t.add_column("Price", justify="right")

    for trade in trades[:10]:
        side = trade.get("side", "")
        side_style = "green" if side == "buy" else "red"
        t.add_row(
            trade.get("time", ""),
            trade.get("symbol", ""),
            Text(side.upper(), style=side_style),
            f"{trade.get('quantity', 0):.4f}",
            f"${trade.get('price', 0):.4f}",
        )
    return t


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
    }
