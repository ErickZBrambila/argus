"""FastAPI dashboard server for Argus.

Serves the single-page dashboard (index.html) and exposes the REST + SSE
API that the frontend JavaScript consumes.

Usage:
    uvicorn argus.dashboard.server:app --reload --port 8000

The app object is also importable so the main Argus process can mount it
directly:
    from argus.dashboard.server import build_app
    app = build_app(agent=my_agent, risk=my_risk_manager)
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from pathlib import Path
from typing import AsyncGenerator, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

_SYMBOL_RE = re.compile(r"^[A-Z0-9.]{1,10}$")

logger = logging.getLogger(__name__)
_DASHBOARD_DIR = Path(__file__).parent


# ── Shared mutable state (replaced at runtime by real objects) ──────────────

class _DashboardState:
    """Thin wrapper so the real agent/risk objects can be injected later."""

    def __init__(self) -> None:
        self.agent = None        # argus.agent.AutopilotAgent (or equivalent)
        self.risk  = None        # argus.risk.RiskManager
        self.broker = None       # argus.broker.*
        self._paused: bool = False
        self._subscribers: list[asyncio.Queue] = []

    # ── SSE pub/sub ──────────────────────────────────────────────────────

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    def broadcast(self, event: str, data: object) -> None:
        """Push an SSE event to all connected clients (non-blocking)."""
        msg = {"event": event, "data": data}
        dead = []
        for q in self._subscribers:
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            self.unsubscribe(q)

    # ── Main-loop integration ────────────────────────────────────────────────

    def push_state(self, state: dict) -> None:
        """Called by the autopilot loop each scan cycle to push full state."""
        self._last_full_state = state
        # Broadcast named events that index.html listens for
        status_payload = {
            "equity":        state.get("equity", 0.0),
            "daily_pnl":     state.get("daily_pnl", 0.0),
            "daily_pnl_pct": state.get("daily_pnl_pct", 0.0),
            "paper_trade":   state.get("paper_trade", True),
            "paused":        state.get("paused", False),
            "risk": {
                "kill_switch":    state.get("kill_switch", False),
                "pdt_count":      state.get("day_trades", 0),
                "drawdown_pct":   0.0,
                "drawdown_limit": abs(self.risk.daily_drawdown_limit) * 100 if self.risk else 5.0,
                "max_positions":  self.risk.max_positions if self.risk else 5,
            },
        }
        self.broadcast("status", status_payload)
        self.broadcast("positions", state.get("positions", {}))
        self.broadcast("signals", state.get("signals", []))
        self.broadcast("trades", state.get("recent_trades", []))

    def is_paused(self) -> bool:
        return self._paused

    def get_force_close_symbol(self) -> Optional[str]:
        try:
            return self._force_close_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    _force_close_queue: asyncio.Queue = None
    _last_full_state: dict = {}


_state = _DashboardState()
_state._force_close_queue = asyncio.Queue(maxsize=32)


# ── Module-level convenience wrappers (used by main.py) ─────────────────────

def push_state(state: dict) -> None:
    _state.push_state(state)


def is_paused() -> bool:
    return _state.is_paused()


def get_force_close_symbol() -> Optional[str]:
    return _state.get_force_close_symbol()


def main(host: str = "127.0.0.1", port: int = 8000) -> None:
    uvicorn.run(app, host=host, port=port, log_level="warning")


# ── App factory ─────────────────────────────────────────────────────────────

def build_app(
    agent=None,
    risk=None,
    broker=None,
) -> FastAPI:
    """Return a configured FastAPI app. Inject live objects to enable real data."""
    if agent  is not None: _state.agent  = agent
    if risk   is not None: _state.risk   = risk
    if broker is not None: _state.broker = broker
    return app


app = FastAPI(title="Argus Dashboard", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── HTML shell ───────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index() -> HTMLResponse:
    """Serve the single-page dashboard."""
    html = (_DASHBOARD_DIR / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html)


# ── GET /api/status ──────────────────────────────────────────────────────────
# Returns overall bot health: equity, P&L, mode, pause state, risk snapshot.
#
# Response schema:
#   {
#     "equity":          float,      # total portfolio value
#     "daily_pnl":       float,      # realized + unrealized P&L since session open
#     "daily_pnl_pct":   float,      # daily_pnl / session_open_equity * 100
#     "paper_trade":     bool,
#     "paused":          bool,
#     "risk": {
#       "kill_switch":     bool,
#       "pdt_count":       int,       # day trades in rolling 5-day window
#       "drawdown_pct":    float,     # current drawdown as positive percentage
#       "drawdown_limit":  float,     # configured limit as positive percentage (e.g. 5.0)
#       "max_positions":   int
#     }
#   }

@app.get("/api/status")
async def get_status() -> dict:
    risk = _state.risk
    agent = _state.agent
    broker = _state.broker

    equity = 0.0
    if broker:
        try:
            equity = float(broker.get_portfolio_equity() or 0)
        except Exception:
            pass

    kill_switch = False
    pdt_count = 0
    drawdown_pct = 0.0
    drawdown_limit = 5.0
    if risk:
        kill_switch    = risk.kill_switch_active
        pdt_count      = risk._day_trade_count
        drawdown_limit = abs(risk.daily_drawdown_limit) * 100
        if risk._session_entry_equity > 0:
            drawdown_pct = max(
                0.0,
                (risk._session_entry_equity - equity) / risk._session_entry_equity * 100,
            )

    return {
        "equity":         equity,
        "daily_pnl":      0.0,      # wire up to storage layer
        "daily_pnl_pct":  0.0,
        "paper_trade":    getattr(agent, "paper_trade", True),
        "paused":         _state._paused,
        "risk": {
            "kill_switch":    kill_switch,
            "pdt_count":      pdt_count,
            "drawdown_pct":   round(drawdown_pct, 4),
            "drawdown_limit": drawdown_limit,
            "max_positions":  getattr(risk, "max_positions", 5) if risk else 5,
        },
    }


# ── GET /api/positions ───────────────────────────────────────────────────────
# Returns currently open positions with live P&L.
#
# Response schema: list of:
#   {
#     "symbol":             str,
#     "qty":                float,
#     "entry_price":        float,
#     "current_price":      float,
#     "unrealized_pnl":     float,
#     "unrealized_pnl_pct": float,   # e.g. 2.45 means +2.45%
#     "stop_price":         float,
#     "size_pct":           int      # position value as % of portfolio (0-100)
#   }

@app.get("/api/positions")
async def get_positions() -> list:
    broker = _state.broker
    risk   = _state.risk
    if not broker:
        return []
    try:
        raw = broker.get_open_positions()
    except Exception as exc:
        logger.warning("get_positions failed: %s", exc)
        return []

    equity = 0.0
    try:
        equity = float(broker.get_portfolio_equity() or 1)
    except Exception:
        equity = 1.0

    positions = []
    for p in raw:
        try:
            entry   = float(p.get("average_buy_price", 0) or 0)
            qty     = float(p.get("quantity", 0) or 0)
            current = float(p.get("current_price", entry) or entry)
            pnl     = (current - entry) * qty
            pnl_pct = ((current - entry) / entry * 100) if entry else 0.0
            stop    = risk.stop_loss_price(entry) if risk else entry * 0.95
            pos_val = current * qty
            size_pct = int(pos_val / equity * 100) if equity else 0
            positions.append({
                "symbol":             p.get("symbol", ""),
                "qty":                qty,
                "entry_price":        round(entry, 4),
                "current_price":      round(current, 4),
                "unrealized_pnl":     round(pnl, 2),
                "unrealized_pnl_pct": round(pnl_pct, 2),
                "stop_price":         round(stop, 4),
                "size_pct":           min(size_pct, 100),
            })
        except Exception as exc:
            logger.warning("Skipping position %s: %s", p, exc)
    return positions


# ── GET /api/trades ───────────────────────────────────────────────────────────
# Returns recent trade history (today's executions from the DB).
#
# Response schema: list of:
#   {
#     "id":           int,
#     "symbol":       str,
#     "side":         "BUY" | "SELL",
#     "qty":          float,
#     "price":        float,
#     "realized_pnl": float | null,   # null for open entries
#     "order_type":   str,            # "MARKET" | "STOP" | etc.
#     "executed_at":  str             # ISO-8601 UTC timestamp
#   }

@app.get("/api/trades")
async def get_trades() -> list:
    # Wire up to argus.storage when available.
    # Stub returns empty list; replace with DB query:
    #   from argus.storage.models import Trade
    #   async with db_session() as s:
    #       rows = await s.execute(select(Trade).order_by(Trade.executed_at.desc()).limit(50))
    #       return [r.to_dict() for r in rows.scalars()]
    return []


# ── GET /api/signals ──────────────────────────────────────────────────────────
# Returns latest computed technical signals for every watched symbol.
#
# Response schema: list of:
#   {
#     "symbol":     str,
#     "price":      float,
#     "rsi":        float | null,
#     "macd":       float | null,    # MACD line value
#     "macd_hist":  float | null,    # histogram (MACD - signal)
#     "bb_status":  "lower" | "mid" | "upper",
#     "composite":  "bullish" | "bearish" | "neutral",
#     "confidence": float            # 0.0–1.0
#   }
#
# Computed by argus.strategy.indicators.SignalEngine; cached between scan cycles.

@app.get("/api/signals")
async def get_signals() -> list:
    # Replace stub with:
    #   return [sig.to_dict() for sig in _state.agent.last_signals.values()]
    return []


# ── SSE  GET /events ──────────────────────────────────────────────────────────
# Server-Sent Events stream.  The client stays connected and receives named
# events pushed by the bot on every scan cycle or significant event.
#
# Event names  (set via "event: <name>" in the SSE wire format):
#   status    — same payload as GET /api/status
#   positions — same payload as GET /api/positions
#   trades    — same payload as GET /api/trades
#   signals   — same payload as GET /api/signals
#   feed      — { "type": "buy"|"sell"|"warn"|"info", "message": str }
#   ping      — keepalive (empty data)
#
# The client reconnects automatically on disconnect; the server sends a ping
# every 25 s to keep proxies/load-balancers from closing idle connections.

@app.get("/events")
async def sse_stream() -> StreamingResponse:
    queue = _state.subscribe()

    async def generate() -> AsyncGenerator[str, None]:
        try:
            yield ": connected\n\n"   # SSE comment as handshake
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=25.0)
                    event = msg["event"]
                    data  = json.dumps(msg["data"])
                    yield f"event: {event}\ndata: {data}\n\n"
                except asyncio.TimeoutError:
                    yield "event: ping\ndata: {}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            _state.unsubscribe(queue)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering
        },
    )


# ── POST /api/close/{symbol} ──────────────────────────────────────────────────
# Force-close an open position at market price.
#
# Path param: symbol — ticker to close (e.g. "AAPL")
# Response:   { "ok": true, "symbol": str, "message": str }
# Errors:     404 if no open position found, 503 if broker unavailable

@app.post("/api/close/{symbol}")
async def force_close(symbol: str) -> dict:
    symbol = symbol.strip().upper()
    if not _SYMBOL_RE.fullmatch(symbol):
        raise HTTPException(status_code=400, detail=f"Invalid symbol: {symbol!r}")

    msg = f"Force-close requested for {symbol}"
    logger.warning(msg)
    try:
        _state._force_close_queue.put_nowait(symbol)
    except asyncio.QueueFull:
        raise HTTPException(status_code=429, detail="Force-close queue full, try again")

    _state.broadcast("feed", {"type": "sell", "message": msg})
    return {"ok": True, "symbol": symbol, "message": msg}


# ── POST /api/pause ───────────────────────────────────────────────────────────
# Pause the autopilot scan loop.  Existing positions remain open; no new
# orders will be placed until /api/resume is called.
#
# Response: { "ok": true, "paused": true }

@app.post("/api/pause")
async def pause_autopilot() -> dict:
    _state._paused = True
    agent = _state.agent
    if agent and hasattr(agent, "pause"):
        agent.pause()
    _state.broadcast("status", {"paused": True})
    _state.broadcast("feed", {"type": "warn", "message": "Autopilot paused"})
    logger.warning("Autopilot paused via dashboard")
    return {"ok": True, "paused": True}


# ── POST /api/resume ──────────────────────────────────────────────────────────
# Resume the autopilot scan loop after a pause.
#
# Response: { "ok": true, "paused": false }

@app.post("/api/resume")
async def resume_autopilot() -> dict:
    _state._paused = False
    agent = _state.agent
    if agent and hasattr(agent, "resume"):
        agent.resume()
    _state.broadcast("status", {"paused": False})
    _state.broadcast("feed", {"type": "info", "message": "Autopilot resumed"})
    logger.info("Autopilot resumed via dashboard")
    return {"ok": True, "paused": False}
