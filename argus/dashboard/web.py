"""FastAPI web dashboard with SSE real-time updates."""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import queue as stdlib_queue
import re
import threading
from typing import AsyncGenerator

import pathlib

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger(__name__)

# Shared state injected by main loop
_state: dict = {}
_paused: bool = False

# Thread-safe queue: main loop (sync thread) → SSE broadcaster (async thread)
_sse_queue: stdlib_queue.Queue = stdlib_queue.Queue(maxsize=5)
# Per-subscriber async queues — only accessed within the event loop
_subscribers: set[asyncio.Queue] = set()

# Chart data source — registered by main loop
_chart_source_fn = None   # callable(symbol: str) -> list[dict]
_autopilot = None         # Autopilot instance for runtime control


def register_chart_source(fn) -> None:
    global _chart_source_fn
    _chart_source_fn = fn


def register_autopilot(ap) -> None:
    global _autopilot
    _autopilot = ap

# ── Approval queue (thread-safe: accessed by both sync main loop and async FastAPI) ──
_approval_lock = threading.Lock()
_pending_approvals: dict[str, dict] = {}   # trade_id → trade info
_approval_decisions: dict[str, str] = {}   # trade_id → "approved" | "denied"


def queue_approval(trade_id: str, trade_info: dict) -> None:
    with _approval_lock:
        _pending_approvals[trade_id] = {**trade_info, "queued_at": datetime.datetime.utcnow().isoformat()}
    _push_approvals_state()


def get_approval_decision(trade_id: str) -> str | None:
    """Returns 'approved', 'denied', or None if no decision yet."""
    with _approval_lock:
        return _approval_decisions.get(trade_id)


def clear_approval(trade_id: str) -> None:
    with _approval_lock:
        _pending_approvals.pop(trade_id, None)
        _approval_decisions.pop(trade_id, None)
    _push_approvals_state()


def _push_approvals_state() -> None:
    with _approval_lock:
        approvals = dict(_pending_approvals)
    _state["pending_approvals"] = approvals
    _sse_push(json.dumps(_state, default=str))


def _sse_push(data: str) -> None:
    """Thread-safe push to SSE broadcaster. Drops oldest item if full."""
    try:
        _sse_queue.put_nowait(data)
    except stdlib_queue.Full:
        try:
            _sse_queue.get_nowait()
        except stdlib_queue.Empty:
            pass
        try:
            _sse_queue.put_nowait(data)
        except stdlib_queue.Full:
            pass


app = FastAPI(title="Argus Dashboard", docs_url=None, redoc_url=None)
# CORS intentionally omitted: frontend is served from the same origin as the API.
_STATIC_DIR = pathlib.Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.on_event("startup")
async def _start_sse_broadcaster() -> None:
    asyncio.create_task(_sse_broadcaster())


async def _sse_broadcaster() -> None:
    """Drain the thread-safe _sse_queue and fan out to all async subscribers."""
    loop = asyncio.get_event_loop()
    while True:
        try:
            data = await loop.run_in_executor(None, _sse_queue.get, True, 0.5)
            for q in list(_subscribers):
                try:
                    q.put_nowait(data)
                except asyncio.QueueFull:
                    pass
        except Exception:
            pass


def push_state(state: dict) -> None:
    """Called by the main loop (sync thread) to push new state to all SSE clients."""
    global _state
    try:
        from argus.dashboard.log_buffer import get_recent
        state = {**state, "logs": get_recent(100)}
    except Exception:
        pass
    _state = state
    _sse_push(json.dumps(state, default=str))


def set_paused(v: bool) -> None:
    global _paused
    _paused = v


def is_paused() -> bool:
    return _paused


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/api/version")
async def get_version() -> dict:
    from argus import __version__
    return {"version": __version__}


@app.get("/api/status")
async def get_status() -> dict:
    return {
        "paused": _paused,
        "kill_switch": _state.get("kill_switch", False),
        "paper_trade": _state.get("paper_trade", True),
        "equity": _state.get("equity", 0.0),
        "daily_pnl": _state.get("daily_pnl", 0.0),
        "daily_pnl_pct": _state.get("daily_pnl_pct", 0.0),
        "trade_count": _state.get("trade_count", 0),
        "day_trades": _state.get("day_trades", 0),
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }


@app.get("/api/logs")
async def get_logs(n: int = 100) -> dict:
    try:
        from argus.dashboard.log_buffer import get_recent
        return {"logs": get_recent(n)}
    except Exception:
        return {"logs": []}


@app.get("/api/scan-interval")
async def get_scan_interval() -> dict:
    return {
        "scan_interval":     _state.get("scan_interval", 300),
        "interval_override": _state.get("interval_override"),
        "market_session":    _state.get("market_session", "unknown"),
        "next_scan_at":      _state.get("next_scan_at"),
    }


@app.post("/api/scan-interval")
async def set_scan_interval(body: dict) -> dict:
    if _autopilot is None:
        raise HTTPException(status_code=503, detail="Autopilot not available (mock mode?)")
    seconds = body.get("seconds")   # None = reset to adaptive
    if seconds is not None:
        seconds = int(seconds)
        if seconds < 15:
            raise HTTPException(status_code=400, detail="Minimum interval is 15 seconds")
        if seconds > 3600:
            raise HTTPException(status_code=400, detail="Maximum interval is 3600 seconds")
    _autopilot.set_scan_interval(seconds)
    return {"seconds": seconds, "status": "adaptive" if seconds is None else "override"}


@app.get("/api/positions")
async def get_positions() -> dict:
    return {"positions": _state.get("positions", {})}


@app.get("/api/trades")
async def get_trades() -> dict:
    return {"trades": _state.get("recent_trades", [])}


@app.get("/api/signals")
async def get_signals() -> dict:
    return {"signals": _state.get("signals", [])}


@app.post("/api/pause")
async def pause_trading() -> dict:
    set_paused(True)
    logger.info("Autopilot paused via web dashboard")
    return {"status": "paused"}


@app.post("/api/resume")
async def resume_trading() -> dict:
    set_paused(False)
    logger.info("Autopilot resumed via web dashboard")
    return {"status": "running"}


@app.post("/api/close/{symbol}")
async def force_close(symbol: str) -> dict:
    symbol = symbol.upper().strip()
    positions = _state.get("positions", {})
    if symbol not in positions:
        raise HTTPException(status_code=404, detail=f"{symbol} not in open positions")
    # Signal the main loop to close; the loop polls _force_close_queue
    _force_close_queue.put_nowait(symbol)
    logger.warning("Force-close requested for %s via web dashboard", symbol)
    return {"status": "close_requested", "symbol": symbol}


_force_close_queue: stdlib_queue.Queue = stdlib_queue.Queue(maxsize=10)
_promote_queue: stdlib_queue.Queue = stdlib_queue.Queue(maxsize=10)

_KNOWN_ACCOUNTS = {"agentic", "default", "main"}
_SYMBOL_RE = re.compile(r"^[A-Z0-9.]{1,10}$")


def get_force_close_symbol() -> str | None:
    try:
        return _force_close_queue.get_nowait()
    except stdlib_queue.Empty:
        return None


def get_promote_request() -> dict | None:
    try:
        return _promote_queue.get_nowait()
    except stdlib_queue.Empty:
        return None


@app.post("/api/promote/{symbol}")
async def promote_position(symbol: str, body: dict = {}) -> dict:
    symbol = symbol.upper().strip()
    if not _SYMBOL_RE.match(symbol):
        raise HTTPException(status_code=400, detail="Invalid symbol")
    from_account = str(body.get("from_account", "default"))
    to_account   = str(body.get("to_account",   "agentic"))
    if from_account not in _KNOWN_ACCOUNTS or to_account not in _KNOWN_ACCOUNTS:
        raise HTTPException(status_code=400, detail="Invalid account label")
    _promote_queue.put_nowait({"symbol": symbol, "from_account": from_account, "to_account": to_account})
    logger.info("Promote requested: %s %s → %s", symbol, from_account, to_account)
    return {"status": "queued", "symbol": symbol, "from": from_account, "to": to_account}


@app.get("/api/chart/{symbol}")
async def get_chart(symbol: str) -> dict:
    symbol = symbol.upper().strip()
    if not _SYMBOL_RE.match(symbol):
        raise HTTPException(status_code=400, detail="Invalid symbol")
    if _chart_source_fn is None:
        return {"candles": [], "symbol": symbol}
    try:
        raw = await asyncio.get_event_loop().run_in_executor(None, _chart_source_fn, symbol)
        candles = []
        for bar in (raw or []):
            try:
                # Accept pre-parsed integer timestamps or ISO string fields
                if isinstance(bar.get("time"), (int, float)):
                    t = int(bar["time"])
                else:
                    ts = bar.get("begins_at") or bar.get("timestamp") or ""
                    if not ts:
                        continue
                    import datetime as _dt
                    t = int(_dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
                candles.append({
                    "time":  t,
                    "open":  float(bar.get("open_price")  or bar.get("open")  or 0),
                    "high":  float(bar.get("high_price")  or bar.get("high")  or 0),
                    "low":   float(bar.get("low_price")   or bar.get("low")   or 0),
                    "close": float(bar.get("close_price") or bar.get("close") or 0),
                })
            except Exception:
                pass
        return {"candles": candles, "symbol": symbol}
    except Exception as exc:
        logger.warning("Chart data error for %s: %s", symbol, exc)
        return {"candles": [], "symbol": symbol}


@app.get("/api/flashcards")
async def get_flashcards() -> dict:
    cards = _state.get("flashcards", [])
    summary = _state.get("flashcard_summary", {})
    return {"flashcards": cards, "summary": summary}


@app.get("/api/approvals")
async def get_approvals() -> dict:
    with _approval_lock:
        return {"approvals": dict(_pending_approvals)}


@app.post("/api/approve/{trade_id}")
async def approve_trade(trade_id: str) -> dict:
    with _approval_lock:
        if trade_id not in _pending_approvals:
            raise HTTPException(status_code=404, detail="Trade not found or already decided")
        _approval_decisions[trade_id] = "approved"
    logger.info("Trade %s approved via dashboard", trade_id)
    _push_approvals_state()
    return {"status": "approved", "trade_id": trade_id}


@app.post("/api/deny/{trade_id}")
async def deny_trade(trade_id: str) -> dict:
    with _approval_lock:
        if trade_id not in _pending_approvals:
            raise HTTPException(status_code=404, detail="Trade not found or already decided")
        _approval_decisions[trade_id] = "denied"
    logger.info("Trade %s denied via dashboard", trade_id)
    _push_approvals_state()
    return {"status": "denied", "trade_id": trade_id}


# ── SSE stream ────────────────────────────────────────────────────────────────

@app.get("/events")
async def sse_stream() -> StreamingResponse:
    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    _subscribers.add(q)

    async def generator() -> AsyncGenerator[str, None]:
        try:
            if _state:
                yield f"data: {json.dumps(_state, default=str)}\n\n"
            while True:
                try:
                    data = await asyncio.wait_for(q.get(), timeout=30.0)
                    yield f"data: {data}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            _subscribers.discard(q)

    return StreamingResponse(generator(), media_type="text/event-stream")


# ── UI ────────────────────────────────────────────────────────────────────────

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Argus — Trading Dashboard</title>
<link rel="icon" type="image/png" sizes="32x32" href="/static/favicon-32x32.png">
<link rel="icon" type="image/png" sizes="16x16" href="/static/favicon-16x16.png">
<link rel="apple-touch-icon" sizes="180x180" href="/static/apple-touch-icon.png">
<style>
  :root {
    --bg: #0d1117;
    --surface: #161b22;
    --surface2: #21262d;
    --surface3: #1c2128;
    --border: #30363d;
    --border-subtle: #21262d;
    --text: #e6edf3;
    --text-dim: #6e7681;
    --muted: #8b949e;
    --accent: #00d4aa;
    --accent-dim: rgba(0,212,170,.12);
    --green: #3fb950;
    --red: #f85149;
    --yellow: #d29922;
    --blue: #58a6ff;
    --purple: #c084fc;
    --radius: 8px;
    --radius-sm: 5px;
    --radius-lg: 12px;
    --mono: "SF Mono","Fira Code","Cascadia Code",monospace;
    --font: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", sans-serif;
    --shadow-sm: 0 1px 3px rgba(0,0,0,.3);
    --shadow-md: 0 4px 12px rgba(0,0,0,.4);
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: var(--font); font-size: 14px; line-height: 1.5; min-height: 100vh; -webkit-font-smoothing: antialiased; }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }
  :focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; border-radius: 3px; }

  /* ── Layout ─────────────────────────────────────────────────────────────── */
  .app { display: flex; flex-direction: column; min-height: 100vh; }

  header {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 0 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    height: 60px;
    position: sticky;
    top: 0;
    z-index: 100;
    box-shadow: var(--shadow-sm);
  }

  /* Logo group */
  .logo {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-size: 17px;
    font-weight: 800;
    color: var(--accent);
    letter-spacing: 3px;
    text-transform: uppercase;
    user-select: none;
  }
  .logo img { height: 38px; display: block; background: var(--surface); border-radius: 6px; }
  .logo-wordmark { display: flex; flex-direction: column; gap: 0; line-height: 1; }
  .logo-name { font-size: 17px; font-weight: 800; letter-spacing: 3px; color: var(--accent); }
  .logo-version { font-size: 10px; font-weight: 500; color: var(--text-dim); letter-spacing: 0.5px; text-transform: none; font-family: var(--mono); }

  /* Header right cluster */
  .header-right { display: flex; align-items: center; gap: 10px; }

  /* Status badges */
  .badges { display: flex; gap: 6px; align-items: center; }
  .badge {
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    white-space: nowrap;
  }
  .badge-paper    { background: rgba(88,166,255,.18); color: var(--blue); border: 1px solid rgba(88,166,255,.3); }
  .badge-live     { background: rgba(248,81,73,.18); color: var(--red); border: 1px solid rgba(248,81,73,.3); }
  .badge-kill     { background: rgba(248,81,73,.25); color: var(--red); border: 1px solid var(--red); animation: pulse 1s infinite; }
  .badge-paused   { background: rgba(210,153,34,.18); color: var(--yellow); border: 1px solid rgba(210,153,34,.3); }
  @keyframes pulse { 0%,100% { opacity:1; box-shadow: 0 0 0 0 rgba(248,81,73,.4); } 50% { opacity:.8; box-shadow: 0 0 0 4px rgba(248,81,73,0); } }

  /* Session + countdown */
  .badge-session-open       { background: rgba(63,185,80,.15); color: var(--green); border: 1px solid rgba(63,185,80,.3); }
  .badge-session-premarket  { background: rgba(88,166,255,.12); color: var(--blue); border: 1px solid rgba(88,166,255,.25); }
  .badge-session-afterhours { background: rgba(210,153,34,.12); color: var(--yellow); border: 1px solid rgba(210,153,34,.25); }
  .badge-session-closed     { background: var(--surface2); color: var(--muted); border: 1px solid var(--border); }
  .countdown-wrap { display: flex; align-items: center; gap: 5px; font-size: 11.5px; color: var(--muted); }
  .countdown-val  { font-family: var(--mono); color: var(--text); font-weight: 700; min-width: 38px; letter-spacing: 0.5px; }

  /* ── Main grid ──────────────────────────────────────────────────────────── */
  main { padding: 16px; display: grid; gap: 16px; }
  @media (min-width: 640px)  { main { padding: 20px; gap: 18px; } }
  @media (min-width: 1024px) { main { grid-template-columns: repeat(2, 1fr); padding: 24px; gap: 20px; } }
  @media (min-width: 1400px) { main { grid-template-columns: repeat(3, 1fr); } }

  /* ── Cards ──────────────────────────────────────────────────────────────── */
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
  }
  .card-full { grid-column: 1 / -1; }

  /* Section title — uppercase label above content */
  .card-title {
    font-size: 10.5px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    color: var(--text-dim);
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .card-title::after {
    content: '';
    flex: 1;
    height: 1px;
    background: var(--border-subtle);
  }

  /* ── Stats ──────────────────────────────────────────────────────────────── */
  .stats-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }
  @media (min-width: 480px) { .stats-grid { grid-template-columns: repeat(4, 1fr); } }
  .stat { display: flex; flex-direction: column; gap: 3px; }
  .stat-label { font-size: 10.5px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }
  .stat-value { font-size: 20px; font-weight: 700; line-height: 1.1; }
  .stat-value.lg { font-size: 26px; }

  /* ── Tables ─────────────────────────────────────────────────────────────── */
  .table-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; border-radius: var(--radius-sm); }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  thead { position: sticky; top: 0; }
  th {
    color: var(--text-dim);
    font-weight: 600;
    text-align: left;
    padding: 8px 10px;
    font-size: 10.5px;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
    background: var(--surface);
  }
  td {
    padding: 9px 10px;
    border-bottom: 1px solid var(--border-subtle);
    vertical-align: middle;
  }
  tr:last-child td { border-bottom: none; }
  .tr-hover { transition: background .1s; }
  .tr-hover:hover { background: var(--surface2); }
  .txt-right  { text-align: right; }
  .txt-center { text-align: center; }

  /* ── Color utilities ────────────────────────────────────────────────────── */
  .green  { color: var(--green); }
  .red    { color: var(--red); }
  .yellow { color: var(--yellow); }
  .muted  { color: var(--muted); }
  .accent { color: var(--accent); font-weight: 600; }
  .mono   { font-family: var(--mono); }

  /* ── Pill badges ────────────────────────────────────────────────────────── */
  .pill {
    display: inline-flex;
    align-items: center;
    padding: 2px 7px;
    border-radius: 20px;
    font-size: 10.5px;
    font-weight: 700;
    letter-spacing: 0.4px;
    white-space: nowrap;
  }
  .pill-bullish { background: rgba(63,185,80,.14); color: var(--green); }
  .pill-bearish { background: rgba(248,81,73,.14); color: var(--red); }
  .pill-neutral { background: rgba(210,153,34,.14); color: var(--yellow); }
  .pill-buy     { background: rgba(63,185,80,.14); color: var(--green); }
  .pill-sell    { background: rgba(248,81,73,.14); color: var(--red); }

  /* ── Buttons ────────────────────────────────────────────────────────────── */
  .btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 7px 15px;
    border-radius: var(--radius-sm);
    border: none;
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    transition: filter .15s, box-shadow .15s, transform .08s;
    white-space: nowrap;
    font-family: var(--font);
  }
  .btn:active { transform: scale(.97); }
  .btn-primary { background: var(--accent); color: #000d0a; }
  .btn-primary:hover { filter: brightness(1.1); box-shadow: 0 0 10px rgba(0,212,170,.3); }
  .btn-danger  { background: var(--red); color: #fff; }
  .btn-danger:hover  { filter: brightness(1.12); box-shadow: 0 0 10px rgba(248,81,73,.3); }
  .btn-warning { background: var(--yellow); color: #0d0a00; }
  .btn-warning:hover { filter: brightness(1.1); }
  .btn-ghost   { background: transparent; color: var(--text); border: 1px solid var(--border); }
  .btn-ghost:hover { background: var(--surface2); border-color: var(--muted); }
  .controls { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }

  /* ── Interval selector ──────────────────────────────────────────────────── */
  .interval-select {
    background: var(--surface2);
    border: 1px solid var(--border);
    color: var(--text);
    border-radius: var(--radius-sm);
    padding: 7px 10px;
    font-size: 13px;
    font-family: var(--font);
    cursor: pointer;
    transition: border-color .15s;
  }
  .interval-select:hover { border-color: var(--muted); }
  .interval-select:focus { outline: none; border-color: var(--accent); }

  /* ── Modal ──────────────────────────────────────────────────────────────── */
  .modal-bg {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,.72);
    backdrop-filter: blur(4px);
    z-index: 200;
    align-items: center;
    justify-content: center;
  }
  .modal-bg.open { display: flex; }

  /* ── Tab navigation ─────────────────────────────────────────────────────── */
  .tab-bar { display: flex; gap: 2px; padding: 0 20px; background: var(--surface); border-bottom: 1px solid var(--border); }
  .tab-btn { padding: 10px 18px; font-size: 13px; font-weight: 600; color: var(--muted); background: none; border: none; border-bottom: 2px solid transparent; cursor: pointer; transition: color .15s, border-color .15s; letter-spacing: 0.2px; }
  .tab-btn:hover { color: var(--text); }
  .tab-btn.active { color: var(--accent); border-bottom-color: var(--accent); }
  .tab-pane { display: none; }
  .tab-pane.active { display: grid; grid-column: 1 / -1; gap: 16px; }
  @media (min-width: 640px)  { .tab-pane.active { gap: 18px; } }
  @media (min-width: 1024px) { .tab-pane.active { grid-template-columns: repeat(2, 1fr); gap: 20px; } }
  @media (min-width: 1400px) { .tab-pane.active { grid-template-columns: repeat(3, 1fr); } }
  .modal {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius-lg);
    padding: 28px;
    width: 90%;
    max-width: 420px;
    box-shadow: var(--shadow-md);
  }
  .modal h3 { margin-bottom: 10px; font-size: 16px; font-weight: 700; }
  .modal p  { color: var(--muted); margin-bottom: 22px; font-size: 13px; line-height: 1.55; }
  .modal-actions { display: flex; gap: 10px; justify-content: flex-end; }

  /* ── Misc indicator rows ────────────────────────────────────────────────── */
  .indicator-row { display: flex; justify-content: space-between; padding: 5px 0; border-bottom: 1px solid var(--border-subtle); font-size: 12px; }
  .indicator-row:last-child { border-bottom: none; }

  /* ── Footer timestamp ───────────────────────────────────────────────────── */
  .timestamp { font-size: 11px; color: var(--text-dim); text-align: right; padding: 0 24px 18px; }

  /* ── Empty state ────────────────────────────────────────────────────────── */
  .empty { text-align: center; color: var(--text-dim); padding: 28px 0; font-size: 12.5px; letter-spacing: 0.2px; }

  /* ── Per-account panels ─────────────────────────────────────────────────── */
  .acct-panels { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 14px; }
  .acct-panel {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 16px 18px;
    transition: box-shadow .2s, border-color .2s;
  }
  .acct-panel.agentic { border-color: rgba(0,212,170,.4); background: rgba(0,212,170,.03); }
  .acct-panel.agentic:hover { box-shadow: 0 0 0 1px rgba(0,212,170,.3), 0 4px 16px rgba(0,0,0,.4); }
  .acct-panel.default { border-color: rgba(192,132,252,.4); background: rgba(192,132,252,.03); }
  .acct-panel.default:hover { box-shadow: 0 0 0 1px rgba(192,132,252,.3), 0 4px 16px rgba(0,0,0,.4); }

  .acct-panel-title {
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .acct-panel-title::before {
    content: '';
    width: 6px;
    height: 6px;
    border-radius: 50%;
    flex-shrink: 0;
  }
  .acct-panel-title.agentic { color: var(--accent); }
  .acct-panel-title.agentic::before { background: var(--accent); box-shadow: 0 0 6px var(--accent); }
  .acct-panel-title.default { color: var(--purple); }
  .acct-panel-title.default::before { background: var(--purple); box-shadow: 0 0 6px var(--purple); }

  .acct-equity { font-size: 24px; font-weight: 700; line-height: 1.1; margin-bottom: 12px; font-variant-numeric: tabular-nums; }
  .acct-equity.agentic { color: var(--accent); }
  .acct-equity.default { color: var(--purple); }

  .acct-row { display: flex; justify-content: space-between; align-items: center; padding: 6px 0; border-bottom: 1px solid var(--border-subtle); font-size: 12.5px; }
  .acct-row:last-child { border-bottom: none; }
  .acct-row-label { color: var(--muted); }

  .acct-mode { display: inline-flex; align-items: center; padding: 2px 8px; border-radius: var(--radius-sm); font-size: 10.5px; font-weight: 700; letter-spacing: 0.3px; }
  .acct-mode-auto     { background: rgba(0,212,170,.12); color: var(--accent); border: 1px solid rgba(0,212,170,.25); }
  .acct-mode-approval { background: rgba(210,153,34,.12); color: var(--yellow); border: 1px solid rgba(210,153,34,.25); }

  /* Goal progress bar */
  .goal-wrap { margin-top: 14px; padding-top: 12px; border-top: 1px solid var(--border-subtle); }
  .goal-labels { display: flex; justify-content: space-between; font-size: 11px; margin-bottom: 6px; }
  .goal-title { color: var(--muted); font-weight: 600; letter-spacing: 0.3px; }
  .goal-pct { font-family: var(--mono); font-weight: 700; }
  .goal-track { height: 6px; background: var(--surface2); border-radius: 99px; overflow: hidden; }
  .goal-fill { height: 100%; border-radius: 99px; transition: width .6s ease; }
  .goal-fill.agentic { background: linear-gradient(90deg, var(--accent), #00ff99); }
  .goal-fill.default { background: linear-gradient(90deg, var(--purple), #f0abfc); }
  .goal-fill.done    { background: linear-gradient(90deg, var(--green), #86efac); }
  .goal-remaining { font-size: 11px; color: var(--muted); margin-top: 5px; text-align: right; font-family: var(--mono); }
  .goal-done-badge { display: inline-block; margin-top: 6px; padding: 2px 8px; border-radius: 99px; font-size: 10.5px; font-weight: 700; background: rgba(63,185,80,.15); color: var(--green); border: 1px solid rgba(63,185,80,.3); }

  .acct-positions-mini { margin-top: 12px; border-top: 1px solid var(--border-subtle); padding-top: 10px; }
  .acct-pos-row { display: flex; justify-content: space-between; align-items: center; font-size: 12.5px; padding: 4px 0; }
  .btn-promote { font-size: 10px; padding: 1px 6px; margin-left: 6px; background: rgba(192,132,252,.12); color: var(--purple); border: 1px solid rgba(192,132,252,.3); border-radius: 4px; cursor: pointer; }
  .btn-promote:hover { background: rgba(192,132,252,.25); }

  /* Performance analytics card */
  .perf-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px; margin-bottom: 18px; }
  .perf-stat { background: var(--surface2); border-radius: var(--radius); padding: 12px 14px; }
  .perf-stat-label { font-size: 10.5px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
  .perf-stat-value { font-size: 22px; font-weight: 700; font-family: var(--mono); font-variant-numeric: tabular-nums; }
  .perf-stat-sub { font-size: 11px; color: var(--muted); margin-top: 2px; }
  .perf-tables { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  .perf-section-title { font-size: 10.5px; font-weight: 700; color: var(--muted); text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 8px; }
  .perf-row { display: flex; justify-content: space-between; align-items: center; padding: 5px 0; border-bottom: 1px solid var(--border-subtle); font-size: 12.5px; }
  .perf-row:last-child { border-bottom: none; }
  .streak-win  { color: var(--green); font-weight: 700; }
  .streak-loss { color: var(--red);   font-weight: 700; }
  .acct-pos-sym { color: var(--accent); font-weight: 600; font-family: var(--mono); font-size: 12px; }

  .acct-total-bar {
    margin-top: 14px;
    padding-top: 12px;
    border-top: 1px solid var(--border);
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 12px;
    color: var(--muted);
  }
  .acct-total-bar strong { color: var(--text); }

  /* ── Approval queue ─────────────────────────────────────────────────────── */
  .approval-card { border-color: rgba(210,153,34,.4); background: rgba(210,153,34,.03); }
  .approval-item {
    border: 1px solid var(--border);
    border-left: 3px solid var(--yellow);
    border-radius: var(--radius);
    padding: 14px 16px;
    margin-bottom: 10px;
    background: var(--surface2);
  }
  .approval-item:last-child { margin-bottom: 0; }
  .approval-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
  .approval-symbol { font-size: 15px; font-weight: 700; color: var(--accent); font-family: var(--mono); }
  .pill-risk-low    { background: rgba(63,185,80,.14);  color: var(--green); }
  .pill-risk-medium { background: rgba(210,153,34,.14); color: var(--yellow); }
  .pill-risk-high   { background: rgba(248,81,73,.14);  color: var(--red); }
  .approval-reasoning {
    font-size: 12.5px;
    color: var(--muted);
    margin: 6px 0 12px;
    line-height: 1.6;
    border-left: 2px solid var(--border);
    padding-left: 10px;
  }
  .approval-meta { font-size: 11px; color: var(--text-dim); margin-bottom: 12px; }
  .approval-actions { display: flex; gap: 8px; }
  #approvals-section { display: none; }
  #approvals-section.has-items { display: block; }

  /* ── Token usage card ───────────────────────────────────────────────────── */
  .token-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  @media (min-width: 640px) { .token-grid { grid-template-columns: repeat(4, 1fr); } }
  .token-model {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px 16px;
    background: var(--surface2);
    transition: border-color .15s;
  }
  .token-model:hover { border-color: var(--muted); }
  .token-model-title { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 1.2px; margin-bottom: 10px; }
  .token-model-title.claude { color: var(--purple); }
  .token-model-title.gemini { color: var(--blue); }
  .token-model-title.total  { color: var(--accent); }
  .token-row { display: flex; justify-content: space-between; font-size: 12px; padding: 3px 0; border-bottom: 1px solid var(--border-subtle); }
  .token-row:last-of-type { border-bottom: none; }
  .token-label { color: var(--muted); }
  .token-val { font-family: var(--mono); font-weight: 600; font-size: 12px; }
  .token-cost { font-size: 20px; font-weight: 700; margin-top: 4px; margin-bottom: 10px; font-family: var(--mono); font-variant-numeric: tabular-nums; }

  /* ── Log tail ───────────────────────────────────────────────────────────── */
  .log-tail {
    background: #0a0d12;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    font-family: var(--mono);
    font-size: 11.5px;
    line-height: 1.65;
    overflow-y: auto;
    max-height: 340px;
    padding: 10px 14px;
  }
  .log-line { display: flex; gap: 10px; padding: 2px 0; border-bottom: 1px solid rgba(48,54,61,.3); }
  .log-line:last-child { border-bottom: none; }
  .log-ts   { color: var(--text-dim); flex-shrink: 0; font-size: 11px; }
  .log-lvl  { flex-shrink: 0; width: 28px; font-weight: 700; font-size: 11px; }
  .log-name { color: var(--blue); flex-shrink: 0; max-width: 90px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 11px; opacity: .75; }
  .log-msg  { color: var(--text); word-break: break-word; }
  .log-lvl-INF { color: var(--muted); }
  .log-lvl-WRN { color: var(--yellow); }
  .log-lvl-ERR { color: var(--red); }
  .log-lvl-CRT { color: var(--red); font-weight: 900; }
  .log-lvl-DBG { color: #444c56; }
  .log-line-WRN .log-msg { color: var(--yellow); opacity: .9; }
  .log-line-ERR .log-msg { color: var(--red); }
  .log-line-CRT .log-msg { color: var(--red); font-weight: 600; }
  .log-toolbar { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
  .log-filter-btns { display: flex; gap: 4px; }
  .log-filter-btn {
    padding: 3px 9px;
    border-radius: var(--radius-sm);
    font-size: 11px;
    font-weight: 600;
    cursor: pointer;
    border: 1px solid var(--border);
    background: none;
    color: var(--muted);
    font-family: var(--font);
    transition: all .12s;
  }
  .log-filter-btn:hover { color: var(--text); border-color: var(--muted); }
  .log-filter-btn.active { background: var(--surface2); color: var(--text); border-color: var(--accent); }
  .log-autoscroll { font-size: 11px; color: var(--muted); display: flex; align-items: center; gap: 5px; cursor: pointer; user-select: none; }
  .log-autoscroll input { accent-color: var(--accent); }

  /* ── Flashcards ─────────────────────────────────────────────────────────── */
  .fc-summary {
    display: flex;
    gap: 20px;
    flex-wrap: wrap;
    margin-bottom: 18px;
    padding-bottom: 16px;
    border-bottom: 1px solid var(--border);
  }
  .fc-stat { display: flex; flex-direction: column; gap: 2px; }
  .fc-stat .label { font-size: 10.5px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-dim); }
  .fc-stat-val { font-size: 20px; font-weight: 700; line-height: 1.1; font-variant-numeric: tabular-nums; }
  .fc-grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); }
  .fc {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    overflow: hidden;
    cursor: pointer;
    transition: border-color .15s, box-shadow .15s;
  }
  .fc:hover { border-color: var(--accent); box-shadow: 0 2px 8px rgba(0,0,0,.3); }
  .fc.fc-win  { border-left: 3px solid var(--green); }
  .fc.fc-loss { border-left: 3px solid var(--red); }
  .fc.fc-open { border-left: 3px solid var(--yellow); }
  .fc-front { padding: 13px 15px; background: var(--surface2); }
  .fc-top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
  .fc-symbol { font-size: 14px; font-weight: 700; color: var(--accent); font-family: var(--mono); letter-spacing: 0.5px; }
  .fc-badges { display: flex; gap: 5px; align-items: center; }
  .fc-indicators {
    display: grid;
    grid-template-columns: max-content 1fr max-content 1fr;
    gap: 4px 10px;
    font-size: 11px;
    margin-bottom: 10px;
  }
  .fc-indicators .muted { color: var(--text-dim); }
  .fc-ind-val { color: var(--text); font-weight: 600; font-family: var(--mono); }
  .fc-outcome { display: flex; justify-content: space-between; align-items: center; font-size: 12px; margin-top: 2px; }
  .fc-expand-hint { font-size: 10px; color: var(--text-dim); transition: opacity .15s; }
  .fc.expanded .fc-expand-hint { opacity: 0; pointer-events: none; }
  .fc-back { padding: 12px 15px; background: var(--surface3); border-top: 1px solid var(--border); display: none; }
  .fc.expanded .fc-back { display: block; }
  .fc-reasoning { font-size: 12.5px; color: var(--muted); line-height: 1.65; margin-bottom: 8px; }
  .fc-meta { font-size: 11px; color: var(--text-dim); border-top: 1px solid var(--border-subtle); padding-top: 6px; margin-top: 4px; }
  .pill-win  { background: rgba(63,185,80,.14);  color: var(--green); }
  .pill-loss { background: rgba(248,81,73,.14);  color: var(--red); }
  .pill-open { background: rgba(210,153,34,.14); color: var(--yellow); }
  .pill-buy  { background: rgba(63,185,80,.14);  color: var(--green); }
  .pill-sell { background: rgba(248,81,73,.14);  color: var(--red); }

  /* ── Hide-values toggle ─────────────────────────────────────────────────── */
  .btn-eye {
    background: none;
    border: 1px solid var(--border);
    color: var(--muted);
    padding: 5px 10px;
    border-radius: var(--radius-sm);
    cursor: pointer;
    font-size: 14px;
    transition: all .15s;
    line-height: 1;
  }
  .btn-eye:hover { border-color: var(--accent); color: var(--accent); }
  body.hide-values .private { filter: blur(7px); user-select: none; transition: filter .2s; }
  body.hide-values .private:hover { filter: blur(0); transition: filter .05s; }

  /* ── Price chart ────────────────────────────────────────────────────────── */
  .chart-toolbar { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px; margin-bottom: 14px; }
  .chart-tabs { display: flex; gap: 4px; flex-wrap: wrap; }
  .chart-tab {
    padding: 4px 13px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
    border: 1px solid var(--border);
    background: none;
    color: var(--muted);
    font-family: var(--mono);
    transition: all .15s;
  }
  .chart-tab:hover { border-color: var(--accent); color: var(--accent); }
  .chart-tab.active { background: var(--accent); color: #000d0a; border-color: var(--accent); font-weight: 700; }
  .chart-type-btns { display: flex; gap: 4px; }
  .chart-type-btn {
    padding: 4px 11px;
    border-radius: var(--radius-sm);
    font-size: 11px;
    font-weight: 600;
    cursor: pointer;
    border: 1px solid var(--border);
    background: none;
    color: var(--muted);
    font-family: var(--font);
    transition: all .15s;
  }
  .chart-type-btn:hover { border-color: var(--accent); color: var(--accent); }
  .chart-type-btn.active { background: var(--surface2); color: var(--text); border-color: var(--accent); }
  #price-chart { width: 100%; height: 340px; border-radius: var(--radius-sm); overflow: hidden; }
  .chart-legend { display: flex; gap: 16px; margin-top: 10px; font-size: 11px; color: var(--muted); flex-wrap: wrap; }
  .legend-item { display: flex; align-items: center; gap: 5px; }
  .legend-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .legend-dash { width: 18px; height: 2px; border-top: 2px dashed; flex-shrink: 0; }
</style>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
</head>
<body>
<div class="app">
  <header>
    <span class="logo">
      <img src="/static/icon.png" alt="Argus eye logo">
      <div class="logo-wordmark">
        <span class="logo-name">ARGUS</span>
        <span class="logo-version" id="version-badge"></span>
      </div>
    </span>
    <div class="header-right">
      <span class="badge badge-session-closed" id="session-badge">—</span>
      <div class="countdown-wrap">
        <span>Next scan</span>
        <span class="countdown-val" id="countdown">—</span>
      </div>
      <div class="badges" id="badges"></div>
      <button class="btn-eye" id="btn-eye" onclick="toggleValues()" title="Show/hide dollar amounts">👁</button>
    </div>
  </header>
  <nav class="tab-bar">
    <button class="tab-btn active" onclick="switchTab('dashboard')">Dashboard</button>
    <button class="tab-btn" onclick="switchTab('performance')">Performance</button>
  </nav>
  <main>

  <div class="tab-pane active" id="tab-dashboard">

    <!-- Token usage -->
    <div class="card card-full">
      <div class="card-title">Token Usage Today</div>
      <div class="token-grid" id="token-grid"><div class="empty">No AI calls yet</div></div>
    </div>

    <!-- Per-account panels -->
    <div class="card card-full">
      <div class="card-title">Accounts</div>
      <div class="acct-panels" id="accounts-panels">
        <div class="empty">Waiting for data…</div>
      </div>
      <div class="acct-total-bar" id="acct-total-bar" style="display:none">
        <span>Combined</span>
        <span class="private" id="stat-equity">—</span>
        <span class="private" id="stat-pnl">—</span>
        <span id="stat-trades" style="display:none"></span>
        <span id="stat-daytrades" style="display:none"></span>
      </div>
    </div>

    <!-- Controls -->
    <div class="card card-full">
      <div class="card-title">Controls</div>
      <div class="controls">
        <button class="btn btn-warning" id="btn-pause" onclick="togglePause()">⏸ Pause</button>
        <button class="btn btn-ghost" id="btn-refresh" onclick="fetchAll()">↻ Refresh</button>
        <select class="interval-select" id="interval-select" onchange="setScanInterval(this.value)" title="Scan interval">
          <option value="auto">Auto (adaptive)</option>
          <option value="30">30 sec</option>
          <option value="60">1 min</option>
          <option value="90">90 sec</option>
          <option value="120">2 min</option>
          <option value="300">5 min</option>
          <option value="600">10 min</option>
        </select>
      </div>
    </div>

    <!-- Price chart -->
    <div class="card card-full">
      <div class="card-title">Price Chart</div>
      <div class="chart-toolbar">
        <div class="chart-tabs" id="chart-tabs"></div>
        <div class="chart-type-btns">
          <button class="chart-type-btn active" id="btn-candles" onclick="setChartType('candles')">Candles</button>
          <button class="chart-type-btn" id="btn-line" onclick="setChartType('line')">Line</button>
        </div>
      </div>
      <div id="price-chart"></div>
      <div class="chart-legend">
        <div class="legend-item"><div class="legend-dot" style="background:#00D4AA"></div> Price</div>
        <div class="legend-item"><div class="legend-dot" style="background:#3fb950"></div> BUY</div>
        <div class="legend-item"><div class="legend-dot" style="background:#f85149"></div> SELL / stop-loss</div>
        <div class="legend-item"><div class="legend-dash" style="border-color:#60a5fa"></div> Trend projection</div>
      </div>
    </div>

    <!-- Pending approvals -->
    <div class="card card-full approval-card" id="approvals-section">
      <div class="card-title" style="color:var(--yellow);--border-subtle:rgba(210,153,34,.2)">⚠ Pending Approval — Default Account</div>
      <div id="approvals-list"></div>
    </div>

    <!-- Open positions -->
    <div class="card card-full">
      <div class="card-title">Open Positions</div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th class="txt-right">Qty</th>
              <th class="txt-right">Entry</th>
              <th class="txt-right">Current</th>
              <th class="txt-right">P&L %</th>
              <th class="txt-right">Stop</th>
              <th class="txt-center">Close</th>
            </tr>
          </thead>
          <tbody id="positions-body"></tbody>
        </table>
      </div>
    </div>

    <!-- Signals -->
    <div class="card">
      <div class="card-title">Technical Signals</div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th class="txt-right">Price</th>
              <th class="txt-right">RSI</th>
              <th class="txt-right">MACD H</th>
              <th class="txt-center">Signal</th>
            </tr>
          </thead>
          <tbody id="signals-body"></tbody>
        </table>
      </div>
    </div>

    <!-- Recent trades -->
    <div class="card">
      <div class="card-title">Recent Trades</div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Symbol</th>
              <th class="txt-center">Side</th>
              <th class="txt-right">Qty</th>
              <th class="txt-right">Price</th>
            </tr>
          </thead>
          <tbody id="trades-body"></tbody>
        </table>
      </div>
    </div>

    <!-- Flashcards -->
    <div class="card card-full">
      <div class="card-title">Decision Flashcards <span style="font-weight:400;text-transform:none;letter-spacing:0;color:var(--text-dim)">— tap a card to expand reasoning</span></div>
      <div class="fc-summary" id="fc-summary"></div>
      <div class="fc-grid" id="fc-grid"><div class="empty">No trades recorded yet</div></div>
    </div>

    <!-- Log tail -->
    <div class="card card-full">
      <div class="card-title">Agent Log</div>
      <div class="log-toolbar">
        <div class="log-filter-btns">
          <button class="log-filter-btn active" onclick="setLogFilter('ALL')">All</button>
          <button class="log-filter-btn" onclick="setLogFilter('INF')">Info</button>
          <button class="log-filter-btn" onclick="setLogFilter('WRN')">Warn</button>
          <button class="log-filter-btn" onclick="setLogFilter('ERR')">Error</button>
        </div>
        <label class="log-autoscroll">
          <input type="checkbox" id="log-autoscroll" checked> Auto-scroll
        </label>
      </div>
      <div class="log-tail" id="log-tail"><div class="empty">Waiting for log entries…</div></div>
    </div>

  </div><!-- /tab-dashboard -->

  <div class="tab-pane" id="tab-performance">

    <!-- Performance Analytics -->
    <div class="card card-full">
      <div class="card-title">Overall Performance</div>
      <div id="perf-container"><div class="empty">No closed trades yet — check back after first positions close.</div></div>
    </div>

    <!-- Per-symbol breakdown -->
    <div class="card card-full">
      <div class="card-title">By Symbol</div>
      <div id="perf-symbols"><div class="empty">No data yet</div></div>
    </div>

    <!-- Confidence accuracy -->
    <div class="card card-full">
      <div class="card-title">AI Confidence Accuracy</div>
      <div id="perf-confidence"><div class="empty">No data yet</div></div>
    </div>

  </div><!-- /tab-performance -->

  </main>
  <p class="timestamp" id="last-update" style="padding: 0 24px 16px;">Last update: —</p>
</div>

<!-- Confirm close modal -->
<div class="modal-bg" id="modal">
  <div class="modal">
    <h3>Confirm Force Close</h3>
    <p id="modal-text">Close position?</p>
    <div class="modal-actions">
      <button class="btn btn-ghost" onclick="closeModal()">Cancel</button>
      <button class="btn btn-danger" id="modal-confirm">Close Position</button>
    </div>
  </div>
</div>

<script>
let paused = false;
let _equityGoal = 25000;

function switchTab(name) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.textContent.toLowerCase() === name));
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.toggle('active', p.id === 'tab-' + name));
}

async function promotePosition(symbol, fromAccount) {
  if (!confirm(`Sell ${symbol} from ${fromAccount} and re-buy on Agentic?`)) return;
  const r = await fetch('/api/promote/' + symbol, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({from_account: fromAccount, to_account: 'agentic'})
  });
  const d = await r.json();
  alert(d.status === 'queued' ? `Promotion queued for ${symbol}` : JSON.stringify(d));
}

function renderPerformance(perf) {
  if (!perf || !perf.closed_trades) {
    document.getElementById('perf-container').innerHTML = '<div class="empty">No closed trades yet</div>';
    document.getElementById('perf-symbols').innerHTML = '<div class="empty">No data yet</div>';
    document.getElementById('perf-confidence').innerHTML = '<div class="empty">No data yet</div>';
    return;
  }

  const winRate = perf.win_rate != null ? (perf.win_rate * 100).toFixed(1) + '%' : '—';
  const winColor = perf.win_rate >= 0.5 ? 'var(--green)' : 'var(--red)';
  const streakCls = perf.streak_type === 'win' ? 'streak-win' : 'streak-loss';
  const streakLabel = perf.current_streak
    ? `${perf.current_streak} ${perf.streak_type} streak`
    : '—';
  const avgPnl = perf.avg_pnl_pct != null ? (perf.avg_pnl_pct >= 0 ? '+' : '') + perf.avg_pnl_pct.toFixed(2) + '%' : '—';
  const avgPnlColor = (perf.avg_pnl_pct || 0) >= 0 ? 'var(--green)' : 'var(--red)';
  const holdStr = perf.avg_hold_hours != null ? perf.avg_hold_hours.toFixed(1) + 'h' : '—';

  document.getElementById('perf-container').innerHTML = `
    <div class="perf-grid">
      <div class="perf-stat">
        <div class="perf-stat-label">Win Rate</div>
        <div class="perf-stat-value" style="color:${winColor}">${winRate}</div>
        <div class="perf-stat-sub">${perf.closed_trades} closed trades</div>
      </div>
      <div class="perf-stat">
        <div class="perf-stat-label">Avg P&L</div>
        <div class="perf-stat-value" style="color:${avgPnlColor}">${avgPnl}</div>
        <div class="perf-stat-sub">per trade</div>
      </div>
      <div class="perf-stat">
        <div class="perf-stat-label">Streak</div>
        <div class="perf-stat-value ${streakCls}">${perf.current_streak || 0}</div>
        <div class="perf-stat-sub">${streakLabel}</div>
      </div>
      <div class="perf-stat">
        <div class="perf-stat-label">Avg Hold</div>
        <div class="perf-stat-value">${holdStr}</div>
        <div class="perf-stat-sub">per position</div>
      </div>
      <div class="perf-stat">
        <div class="perf-stat-label">Best Trade</div>
        <div class="perf-stat-value" style="color:var(--green)">${perf.best_trade ? '+' + perf.best_trade.pnl_pct.toFixed(2) + '%' : '—'}</div>
        <div class="perf-stat-sub">${perf.best_trade ? perf.best_trade.symbol + ' · ' + perf.best_trade.date : ''}</div>
      </div>
      <div class="perf-stat">
        <div class="perf-stat-label">Worst Trade</div>
        <div class="perf-stat-value" style="color:var(--red)">${perf.worst_trade ? perf.worst_trade.pnl_pct.toFixed(2) + '%' : '—'}</div>
        <div class="perf-stat-sub">${perf.worst_trade ? perf.worst_trade.symbol + ' · ' + perf.worst_trade.date : ''}</div>
      </div>
    </div>`;

  // By symbol
  const symRows = Object.entries(perf.by_symbol || {}).map(([sym, s]) => {
    const wr = (s.win_rate * 100).toFixed(0) + '%';
    const avg = (s.avg_pnl_pct >= 0 ? '+' : '') + s.avg_pnl_pct.toFixed(2) + '%';
    const wrColor = s.win_rate >= 0.5 ? 'var(--green)' : 'var(--red)';
    const avgColor = s.avg_pnl_pct >= 0 ? 'var(--green)' : 'var(--red)';
    return `<div class="perf-row">
      <span style="font-family:var(--mono);font-weight:600">${sym}</span>
      <span style="color:var(--muted)">${s.trades} trades</span>
      <span style="color:${wrColor}">${wr} win rate</span>
      <span style="color:${avgColor}">${avg} avg</span>
    </div>`;
  }).join('') || '<div class="empty">No data</div>';
  document.getElementById('perf-symbols').innerHTML = symRows;

  // Confidence accuracy
  const confRows = Object.entries(perf.by_confidence || {}).map(([k, v]) => {
    if (!v.trades) return '';
    const wr = v.win_rate != null ? (v.win_rate * 100).toFixed(0) + '%' : '—';
    const wrColor = (v.win_rate || 0) >= 0.5 ? 'var(--green)' : 'var(--red)';
    return `<div class="perf-row">
      <span>Confidence ${v.label}</span>
      <span style="color:var(--muted)">${v.trades} trades</span>
      <span style="color:${wrColor};font-weight:600">${wr} win rate</span>
    </div>`;
  }).join('') || '<div class="empty">No data</div>';
  document.getElementById('perf-confidence').innerHTML = confRows;
}
let pendingCloseSymbol = null;
let valuesHidden = true;
let _nextScanAt = null;

// ── Countdown timer (runs every second independently of SSE) ──────────────────
function _updateCountdown() {
  const el = document.getElementById('countdown');
  if (!el) return;
  if (!_nextScanAt) { el.textContent = '—'; return; }
  const secs = Math.max(0, Math.round((_nextScanAt - Date.now()) / 1000));
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  el.textContent = m > 0 ? `${m}:${String(s).padStart(2,'0')}` : `${s}s`;
}
setInterval(_updateCountdown, 1000);

// ── Market session badge ──────────────────────────────────────────────────────
const _SESSION_LABELS = {
  open: 'MARKET OPEN', premarket: 'PRE-MARKET',
  afterhours: 'AFTER-HOURS', closed: 'CLOSED',
};
function _updateSessionBadge(session) {
  const el = document.getElementById('session-badge');
  if (!el) return;
  el.textContent = _SESSION_LABELS[session] || session.toUpperCase();
  el.className = `badge badge-session-${session || 'closed'}`;
}

// ── Interval selector ─────────────────────────────────────────────────────────
async function setScanInterval(val) {
  const body = val === 'auto' ? { seconds: null } : { seconds: parseInt(val) };
  try {
    await fetch('/api/scan-interval', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch(e) { console.error('setScanInterval failed', e); }
}

function _syncIntervalSelect(override) {
  const sel = document.getElementById('interval-select');
  if (!sel) return;
  sel.value = override != null ? String(override) : 'auto';
  if (!sel.querySelector(`option[value="${sel.value}"]`)) sel.value = 'auto';
}

// ── Token usage ───────────────────────────────────────────────────────────────
function renderTokenUsage(usage) {
  const grid = document.getElementById('token-grid');
  if (!grid) return;
  if (!usage || !usage.total_calls) {
    grid.innerHTML = '<div class="empty">No AI calls yet this session</div>';
    return;
  }
  const c = usage.claude || {};
  const g = usage.gemini || {};
  const fmtN = n => (n || 0).toLocaleString();
  const fmtC = n => '$' + (n || 0).toFixed(4);

  grid.innerHTML = `
    <div class="token-model">
      <div class="token-model-title claude">Claude (Opus)</div>
      <div class="token-cost ${c.cost_usd > 0.5 ? 'red' : 'green'}">${fmtC(c.cost_usd)}</div>
      <div class="token-row"><span class="token-label">Calls</span><span class="token-val">${fmtN(c.calls)}</span></div>
      <div class="token-row"><span class="token-label">Input</span><span class="token-val">${fmtN(c.input_tokens)}</span></div>
      <div class="token-row"><span class="token-label">Output</span><span class="token-val">${fmtN(c.output_tokens)}</span></div>
      <div class="token-row"><span class="token-label">Cache read</span><span class="token-val">${fmtN(c.cache_read_tokens)}</span></div>
    </div>
    <div class="token-model">
      <div class="token-model-title gemini">Gemini (Flash)</div>
      <div class="token-cost ${g.cost_usd > 0.1 ? 'yellow' : 'green'}">${fmtC(g.cost_usd)}</div>
      <div class="token-row"><span class="token-label">Calls</span><span class="token-val">${fmtN(g.calls)}</span></div>
      <div class="token-row"><span class="token-label">Input</span><span class="token-val">${fmtN(g.input_tokens)}</span></div>
      <div class="token-row"><span class="token-label">Output</span><span class="token-val">${fmtN(g.output_tokens)}</span></div>
    </div>
    <div class="token-model">
      <div class="token-model-title total">Total Today</div>
      <div class="token-cost">${fmtC(usage.total_cost_usd)}</div>
      <div class="token-row"><span class="token-label">Total calls</span><span class="token-val">${fmtN(usage.total_calls)}</span></div>
      <div class="token-row"><span class="token-label">Date</span><span class="token-val muted">${usage.date || '—'}</span></div>
    </div>`;
}

function toggleValues() {
  valuesHidden = !valuesHidden;
  document.body.classList.toggle('hide-values', valuesHidden);
  document.getElementById('btn-eye').textContent = valuesHidden ? '🙈' : '👁';
}

// Apply hidden state on load
document.addEventListener('DOMContentLoaded', () => {
  document.body.classList.add('hide-values');
});

function fmt(n, decimals=2) {
  if (n == null) return '—';
  return Number(n).toFixed(decimals);
}
function fmtDollar(n) {
  if (n == null) return '—';
  return '<span class="private">$' + Number(n).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2}) + '</span>';
}
function fmtPnl(val, pct) {
  const sign = val >= 0 ? '+' : '';
  return `<span class="private">${sign}$${Math.abs(val).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})} (${sign}${Number(pct).toFixed(2)}%)</span>`;
}
function pnlClass(n) {
  return n > 0 ? 'green' : n < 0 ? 'red' : '';
}
function pill(text, cls) {
  return `<span class="pill pill-${cls.toLowerCase()}">${text}</span>`;
}

function updateBadges(state) {
  const el = document.getElementById('badges');
  let html = '';
  if (state.paper_trade) html += '<span class="badge badge-paper">Paper</span>';
  else html += '<span class="badge badge-live">Live</span>';
  if (state.kill_switch) html += '<span class="badge badge-kill">Kill Switch</span>';
  if (state.paused || paused) html += '<span class="badge badge-paused">Paused</span>';
  el.innerHTML = html;
}

function renderGoalBar(equity, goal, cls) {
  const pct = Math.min(100, (equity / goal) * 100);
  const done = equity >= goal;
  const fillCls = done ? 'done' : cls;
  const remaining = goal - equity;
  return `<div class="goal-wrap">
    <div class="goal-labels">
      <span class="goal-title">$${(goal/1000).toFixed(0)}K PDT Goal</span>
      <span class="goal-pct" style="color:${done ? 'var(--green)' : 'var(--text)'}">${pct.toFixed(1)}%</span>
    </div>
    <div class="goal-track"><div class="goal-fill ${fillCls}" style="width:${pct}%"></div></div>
    ${done
      ? '<div class="goal-done-badge">PDT restriction lifted</div>'
      : `<div class="goal-remaining private">$${remaining.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})} remaining</div>`
    }
  </div>`;
}

function renderAccounts(accounts) {
  const panels = document.getElementById('accounts-panels');
  if (!accounts || !Object.keys(accounts).length) return;

  const COLOR = { agentic: '#00d4aa', default: '#c084fc' };

  panels.innerHTML = Object.entries(accounts).map(([label, a]) => {
    const cls = label === 'agentic' ? 'agentic' : 'default';
    const equity = a.equity || 0;
    const pnl = a.daily_pnl || 0;
    const pnlPct = a.daily_pnl_pct || 0;
    const pnlSign = pnl >= 0 ? '+' : '';
    const pnlCls = pnlClass(pnl);
    const modeLabel = a.auto_trade ? 'AUTO' : 'APPROVAL REQUIRED';
    const modeCls   = a.auto_trade ? 'acct-mode-auto' : 'acct-mode-approval';
    const pending   = a.pending_approvals || 0;
    const dayTrades = a.day_trades || 0;

    // Mini positions table
    const pos = a.positions || {};
    const posRows = Object.entries(pos).map(([sym, p]) => {
      const pct = p.unrealized_pnl_pct || 0;
      return `<div class="acct-pos-row">
        <span class="acct-pos-sym">${sym}</span>
        <span class="private muted">${fmtDollar(p.current_price)}</span>
        <span class="${pnlClass(pct)}">${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%</span>
      </div>`;
    }).join('') || '<div style="color:var(--muted);font-size:12px;padding:4px 0">No open positions</div>';

    return `<div class="acct-panel ${cls}">
      <div class="acct-panel-title ${cls}">${label.toUpperCase()}</div>
      <div class="acct-equity ${cls} private">${'$' + equity.toLocaleString('en-US', {minimumFractionDigits:2,maximumFractionDigits:2})}</div>
      <div class="acct-row">
        <span class="acct-row-label">Daily P&L</span>
        <span class="${pnlCls} private">${pnlSign}$${Math.abs(pnl).toFixed(2)} (${pnlSign}${pnlPct.toFixed(2)}%)</span>
      </div>
      <div class="acct-row">
        <span class="acct-row-label">Mode</span>
        <span class="acct-mode ${modeCls}">${modeLabel}</span>
      </div>
      <div class="acct-row">
        <span class="acct-row-label">Day Trades</span>
        <span class="${dayTrades >= 2 ? 'yellow' : ''}">${dayTrades} / 3</span>
      </div>
      ${pending ? `<div class="acct-row">
        <span class="acct-row-label">Pending</span>
        <span class="yellow">${pending} awaiting approval</span>
      </div>` : ''}
      <div class="acct-positions-mini">${posRows}</div>
      ${renderGoalBar(equity, _equityGoal, cls)}
    </div>`;
  }).join('');
}

function applyState(state) {
  paused = state.paused || false;
  if (state.equity_goal) _equityGoal = state.equity_goal;
  updateBadges(state);

  // Per-account panels
  renderAccounts(state.accounts);

  // Combined totals (hidden elements kept for backwards compat)
  const equity = state.equity || 0;
  const pnl = state.daily_pnl || 0;
  const pnlPct = state.daily_pnl_pct || 0;
  const dayTrades = state.day_trades || 0;

  const totalBar = document.getElementById('acct-total-bar');
  if (state.accounts && Object.keys(state.accounts).length > 1) {
    totalBar.style.display = 'flex';
    document.getElementById('stat-equity').innerHTML = fmtDollar(equity);
    const pnlEl = document.getElementById('stat-pnl');
    pnlEl.innerHTML = fmtPnl(pnl, pnlPct);
    pnlEl.className = pnlClass(pnl);
  } else {
    totalBar.style.display = 'none';
  }

  // Positions
  const pos = state.positions || {};
  const posBody = document.getElementById('positions-body');
  const posKeys = Object.keys(pos);
  if (!posKeys.length) {
    posBody.innerHTML = '<tr><td colspan="7" class="empty">No open positions</td></tr>';
  } else {
    posBody.innerHTML = posKeys.map(sym => {
      const p = pos[sym];
      const pct = p.unrealized_pnl_pct || 0;
      const rawSym = sym.replace(/\s*\[.*?\]/,'');
      return `<tr class="tr-hover">
        <td class="accent">${escHtml(sym)}</td>
        <td class="txt-right">${fmt(p.quantity,4)}</td>
        <td class="txt-right">${fmtDollar(p.entry_price)}</td>
        <td class="txt-right">${fmtDollar(p.current_price)}</td>
        <td class="txt-right ${pnlClass(pct)}">${pct >= 0 ? '+' : ''}${fmt(pct)}%</td>
        <td class="txt-right muted">${fmtDollar(p.stop_loss_price)}</td>
        <td class="txt-center">
          <button class="btn btn-danger" style="padding:5px 11px;font-size:11px;font-weight:700" onclick="confirmClose('${escHtml(rawSym)}')">Close</button>
          ${(p.account||'').toLowerCase()==='default' ? `<button class="btn-promote" onclick="promotePosition('${escHtml(rawSym)}','default')" title="Sell here, re-buy on Agentic">Promote ↑</button>` : ''}
        </td>
      </tr>`;
    }).join('');
  }

  // Signals
  const sigs = state.signals || [];
  const sigBody = document.getElementById('signals-body');
  if (!sigs.length) {
    sigBody.innerHTML = '<tr><td colspan="5" class="empty">No signals yet</td></tr>';
  } else {
    sigBody.innerHTML = sigs.map(s => {
      const rsi = s.rsi != null ? fmt(s.rsi,1) : '—';
      const hist = s.macd_hist != null ? fmt(s.macd_hist,4) : '—';
      const sig = s.composite || 'neutral';
      return `<tr class="tr-hover">
        <td class="accent">${escHtml(s.symbol)}</td>
        <td class="txt-right">${fmtDollar(s.price)}</td>
        <td class="txt-right">${rsi}</td>
        <td class="txt-right">${hist}</td>
        <td class="txt-center">${pill(sig.toUpperCase(), sig)}</td>
      </tr>`;
    }).join('');
  }

  // Trades
  const trades = state.recent_trades || [];
  const trBody = document.getElementById('trades-body');
  if (!trades.length) {
    trBody.innerHTML = '<tr><td colspan="5" class="empty">No trades yet</td></tr>';
  } else {
    trBody.innerHTML = trades.slice(0,15).map(t => {
      const side = t.side || '';
      return `<tr class="tr-hover">
        <td class="muted">${escHtml(t.time || '')}</td>
        <td class="accent">${escHtml(t.symbol)}</td>
        <td class="txt-center">${pill(escHtml(side.toUpperCase()), side)}</td>
        <td class="txt-right">${fmt(t.quantity,4)}</td>
        <td class="txt-right">${fmtDollar(t.price)}</td>
        <td class="txt-right muted" style="font-size:11px">${t.account || ''}</td>
      </tr>`;
    }).join('');
  }

  // Pending approvals
  const approvals = state.pending_approvals || {};
  const approvalIds = Object.keys(approvals);
  const approvalSection = document.getElementById('approvals-section');
  const approvalList = document.getElementById('approvals-list');
  if (approvalIds.length) {
    approvalSection.classList.add('has-items');
    approvalList.innerHTML = approvalIds.map(id => {
      const a = approvals[id];
      const riskCls = `pill-risk-${(a.risk_level||'medium').toLowerCase()}`;
      const side = a.action || 'BUY';
      return `<div class="approval-item">
        <div class="approval-header">
          <span class="approval-symbol">${escHtml(side)} ${escHtml(a.symbol)}</span>
          <span class="pill ${riskCls}">${escHtml((a.risk_level||'medium').toUpperCase())} RISK</span>
        </div>
        <div class="approval-meta">
          ${fmtDollar(a.dollar_amount)} &middot; Confidence ${fmt((a.confidence||0)*100,0)}% &middot; ${escHtml(a.account_label||'Default')}
        </div>
        <div class="approval-reasoning">${escHtml(a.reasoning||'')}</div>
        <div class="approval-actions">
          <button class="btn btn-primary" onclick="decideApproval('${id}','approve')">✓ Approve</button>
          <button class="btn btn-danger"  onclick="decideApproval('${id}','deny')">✗ Deny</button>
        </div>
      </div>`;
    }).join('');
  } else {
    approvalSection.classList.remove('has-items');
    approvalList.innerHTML = '';
  }

  // Cache flashcards globally for chart markers, then render
  window._flashcards = state.flashcards || [];
  // Scan timing
  if (state.next_scan_at) _nextScanAt = new Date(state.next_scan_at);
  _updateSessionBadge(state.market_session || 'closed');
  _syncIntervalSelect(state.interval_override);
  _updateCountdown();

  renderTokenUsage(state.token_usage);
  renderPerformance(state.performance);
  renderFlashcards(state);
  buildChartTabs(state.signals);
  renderLogs(state.logs || []);
  // Refresh markers if chart is showing (new closed trades may have arrived)
  if (_chartSymbol && _lastCandles[_chartSymbol]) {
    placeTradeMarkers(_chartSymbol, _lastCandles[_chartSymbol]);
  }

  document.getElementById('last-update').textContent = 'Last update: ' + new Date().toLocaleTimeString();
  document.getElementById('btn-pause').textContent = (state.paused || paused) ? '▶ Resume' : '⏸ Pause';
}

async function decideApproval(tradeId, decision) {
  await fetch(`/api/${decision}/${encodeURIComponent(tradeId)}`, {method:'POST'});
}

function renderFlashcards(state) {
  const cards = state.flashcards || [];
  const summary = state.flashcard_summary || {};

  // Summary bar
  const fcSum = document.getElementById('fc-summary');
  if (summary.total > 0) {
    const wr = summary.win_rate != null ? (summary.win_rate * 100).toFixed(0) + '%' : '—';
    const avgPnl = summary.avg_pnl_pct != null ? (summary.avg_pnl_pct >= 0 ? '+' : '') + summary.avg_pnl_pct.toFixed(2) + '%' : '—';
    const best = summary.best_pnl_pct != null ? '+' + summary.best_pnl_pct.toFixed(2) + '%' : '—';
    const worst = summary.worst_pnl_pct != null ? summary.worst_pnl_pct.toFixed(2) + '%' : '—';
    const wrClass = summary.win_rate >= 0.5 ? 'green' : summary.win_rate != null ? 'red' : '';
    fcSum.innerHTML = `
      <div class="fc-stat"><span class="label">Total Trades</span><span class="fc-stat-val">${summary.total}</span></div>
      <div class="fc-stat"><span class="label">Closed</span><span class="fc-stat-val">${summary.closed}</span></div>
      <div class="fc-stat"><span class="label">Win Rate</span><span class="fc-stat-val ${wrClass}">${wr}</span></div>
      <div class="fc-stat"><span class="label">Avg P&L</span><span class="fc-stat-val ${summary.avg_pnl_pct >= 0 ? 'green' : 'red'}">${avgPnl}</span></div>
      <div class="fc-stat"><span class="label">Best</span><span class="fc-stat-val green">${best}</span></div>
      <div class="fc-stat"><span class="label">Worst</span><span class="fc-stat-val red">${worst}</span></div>`;
  } else {
    fcSum.innerHTML = '';
  }

  const grid = document.getElementById('fc-grid');
  if (!cards.length) {
    grid.innerHTML = '<div class="empty">No trades recorded yet</div>';
    return;
  }

  // Preserve which cards the user has open across re-renders
  const openIds = new Set(
    [...grid.querySelectorAll('.fc.expanded')].map(el => el.dataset.tradeId)
  );

  grid.innerHTML = cards.map(c => {
    const closed = c.pnl_pct != null;
    const won = closed && c.pnl_pct > 0;
    const borderCls = closed ? (won ? 'fc-win' : 'fc-loss') : 'fc-open';
    const outcomeHtml = closed
      ? `<span class="pill ${won ? 'pill-win' : 'pill-loss'}">${won ? '▲' : '▼'} ${(c.pnl_pct >= 0 ? '+' : '') + c.pnl_pct.toFixed(2)}%</span>
         <span class="muted" style="font-size:11px">${c.hold_duration_hours ? c.hold_duration_hours.toFixed(1) + 'h held' : ''}</span>`
      : `<span class="pill pill-open">OPEN</span>`;

    const rsiVal = c.rsi != null ? c.rsi.toFixed(1) : '—';
    const macdVal = c.macd_hist != null ? (c.macd_hist >= 0 ? '+' : '') + c.macd_hist.toFixed(4) : '—';
    const macdCls = c.macd_hist > 0 ? 'green' : c.macd_hist < 0 ? 'red' : '';
    const rsiCls = c.rsi < 30 ? 'green' : c.rsi > 70 ? 'red' : '';
    const bbLabel = (c.bb_position || 'inside').replace(/_/g, ' ');
    const ts = c.timestamp ? new Date(c.timestamp).toLocaleString() : '';

    return `<div class="fc ${borderCls}" data-trade-id="${c.trade_id||''}" onclick="this.classList.toggle('expanded')">
      <div class="fc-front">
        <div class="fc-top">
          <span class="fc-symbol">${escHtml(c.symbol)}</span>
          <div class="fc-badges">
            <span class="pill pill-${escHtml((c.action||'buy').toLowerCase())}">${escHtml(c.action)}</span>
            <span class="pill pill-risk-${escHtml(c.risk_level||'medium')}">${escHtml((c.risk_level||'medium').toUpperCase())}</span>
          </div>
        </div>
        <div class="fc-indicators">
          <span class="muted">RSI</span><span class="fc-ind-val ${rsiCls}">${rsiVal}</span>
          <span class="muted">MACD hist</span><span class="fc-ind-val ${macdCls}">${macdVal}</span>
          <span class="muted">BB</span><span class="fc-ind-val">${bbLabel}</span>
          <span class="muted">Signal</span><span class="fc-ind-val">${c.signal_composite} ${(c.signal_confidence*100).toFixed(0)}%</span>
          <span class="muted">vs SMA20</span><span class="fc-ind-val">${c.price_vs_sma20||'—'}</span>
          <span class="muted">vs EMA50</span><span class="fc-ind-val">${c.price_vs_ema50||'—'}</span>
        </div>
        <div class="fc-outcome">
          ${outcomeHtml}
          <span class="fc-expand-hint">tap to expand ↓</span>
        </div>
      </div>
      <div class="fc-back">
        <div class="fc-reasoning">${escHtml(c.reasoning||'No reasoning recorded.')}</div>
        <div class="fc-meta">
          <span class="private">Entry $${(c.entry_price||0).toFixed(2)} · $${(c.dollar_amount||0).toFixed(2)}</span> · ${escHtml(c.account||'')} · ${escHtml(ts)}
        </div>
      </div>
    </div>`;
  }).join('');

  // Restore open state after re-render
  if (openIds.size) {
    grid.querySelectorAll('.fc[data-trade-id]').forEach(el => {
      if (openIds.has(el.dataset.tradeId)) el.classList.add('expanded');
    });
  }
}

async function togglePause() {
  const endpoint = paused ? '/api/resume' : '/api/pause';
  await fetch(endpoint, {method:'POST'});
  const status = await fetch('/api/status').then(r=>r.json());
  paused = status.paused;
  updateBadges(status);
  document.getElementById('btn-pause').textContent = paused ? '▶ Resume' : '⏸ Pause';
}

async function fetchAll() {
  const [status, positions, signals, trades, logs] = await Promise.all([
    fetch('/api/status').then(r=>r.json()),
    fetch('/api/positions').then(r=>r.json()),
    fetch('/api/signals').then(r=>r.json()),
    fetch('/api/trades').then(r=>r.json()),
    fetch('/api/logs?n=100').then(r=>r.json()),
  ]);
  applyState({...status, ...positions, ...signals, ...trades, logs: logs.logs || []});
}

// ── Log tail ──────────────────────────────────────────────────────────────────
let _logFilter = 'ALL';
const _LEVEL_SHORT = { DEBUG: 'DBG', INFO: 'INF', WARNING: 'WRN', ERROR: 'ERR', CRITICAL: 'CRT' };

function setLogFilter(f) {
  _logFilter = f;
  document.querySelectorAll('.log-filter-btn').forEach(b =>
    b.classList.toggle('active', b.textContent.trim() === f ||
      (f === 'ALL' && b.textContent.trim() === 'All') ||
      (f === 'INF' && b.textContent.trim() === 'Info') ||
      (f === 'WRN' && b.textContent.trim() === 'Warn') ||
      (f === 'ERR' && b.textContent.trim() === 'Error')));
  // Re-render from cached entries
  if (window._lastLogs) renderLogs(window._lastLogs);
}

function renderLogs(entries) {
  window._lastLogs = entries;
  const box = document.getElementById('log-tail');
  if (!entries || !entries.length) {
    box.innerHTML = '<div class="empty">No log entries yet</div>';
    return;
  }
  const filtered = _logFilter === 'ALL' ? entries : entries.filter(e => {
    const lvl = _LEVEL_SHORT[e.level] || e.level;
    return lvl === _logFilter;
  });
  if (!filtered.length) {
    box.innerHTML = '<div class="empty">No entries at this level</div>';
    return;
  }
  box.innerHTML = filtered.map(e => {
    const lvl = _LEVEL_SHORT[e.level] || e.level;
    return `<div class="log-line log-line-${lvl}">
      <span class="log-ts">${e.ts}</span>
      <span class="log-lvl log-lvl-${lvl}">${lvl}</span>
      <span class="log-name">${e.name || ''}</span>
      <span class="log-msg">${escHtml(e.msg)}</span>
    </div>`;
  }).join('');
  const auto = document.getElementById('log-autoscroll');
  if (auto && auto.checked) box.scrollTop = box.scrollHeight;
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function confirmClose(symbol) {
  pendingCloseSymbol = symbol;
  document.getElementById('modal-text').textContent = `Force close ${symbol} position? This will sell at market price.`;
  document.getElementById('modal-confirm').onclick = () => doClose(symbol);
  document.getElementById('modal').classList.add('open');
}

function closeModal() {
  document.getElementById('modal').classList.remove('open');
  pendingCloseSymbol = null;
}

async function doClose(symbol) {
  closeModal();
  try {
    await fetch(`/api/close/${encodeURIComponent(symbol)}`, {method:'POST'});
  } catch(e) { console.error(e); }
}

// ── Price chart ────────────────────────────────────────────────────────────
let _chart        = null;
let _candleSeries = null;
let _lineSeries   = null;
let _predSeries   = null;   // dashed prediction line
let _chartSymbol  = null;
let _chartType    = 'candles';   // 'candles' | 'line'
let _lastCandles  = {};

const _CHART_OPTS = {
  layout: { background: { color: '#161920' }, textColor: '#8892a4' },
  grid:   { vertLines: { color: '#2a2f3e' }, horzLines: { color: '#2a2f3e' } },
  crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
  rightPriceScale: { borderColor: '#2a2f3e' },
  timeScale: { borderColor: '#2a2f3e', timeVisible: true },
  handleScroll: true, handleScale: true,
};

function initChart() {
  const el = document.getElementById('price-chart');
  _chart = LightweightCharts.createChart(el, _CHART_OPTS);

  _candleSeries = _chart.addCandlestickSeries({
    upColor: '#00D4AA', downColor: '#f85149',
    borderUpColor: '#00D4AA', borderDownColor: '#f85149',
    wickUpColor: '#00D4AA', wickDownColor: '#f85149',
    visible: true,
  });

  _lineSeries = _chart.addAreaSeries({
    lineColor: '#00D4AA', topColor: 'rgba(0,212,170,0.18)',
    bottomColor: 'rgba(0,212,170,0.01)', lineWidth: 2,
    visible: false,
  });

  _predSeries = _chart.addLineSeries({
    color: '#60a5fa', lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle.Dashed,
    lastValueVisible: true, priceLineVisible: false,
    title: 'trend',
  });

  new ResizeObserver(() => _chart.applyOptions({ width: el.clientWidth })).observe(el);
}

function setChartType(type) {
  _chartType = type;
  document.getElementById('btn-candles').classList.toggle('active', type === 'candles');
  document.getElementById('btn-line').classList.toggle('active', type === 'line');
  if (!_chart) return;
  _candleSeries.applyOptions({ visible: type === 'candles' });
  _lineSeries.applyOptions({ visible: type === 'line' });
  // Markers only supported on candlestick series; re-place on switch
  if (_chartSymbol && _lastCandles[_chartSymbol]) {
    placeTradeMarkers(_chartSymbol, _lastCandles[_chartSymbol]);
  }
}

// ── Linear regression projection ─────────────────────────────────────────────
function _linReg(pts) {
  // pts: [{x, y}]  returns {slope, intercept}
  const n = pts.length;
  const sumX  = pts.reduce((s, p) => s + p.x, 0);
  const sumY  = pts.reduce((s, p) => s + p.y, 0);
  const sumXY = pts.reduce((s, p) => s + p.x * p.y, 0);
  const sumX2 = pts.reduce((s, p) => s + p.x * p.x, 0);
  const denom = n * sumX2 - sumX * sumX;
  if (!denom) return { slope: 0, intercept: sumY / n };
  return {
    slope:     (n * sumXY - sumX * sumY) / denom,
    intercept: (sumY - ((n * sumXY - sumX * sumY) / denom) * sumX) / n,
  };
}

function buildPrediction(candles, forwardBars = 10) {
  const lookback = Math.min(20, candles.length);
  const recent   = candles.slice(-lookback);
  const pts      = recent.map((c, i) => ({ x: i, y: c.close }));
  const { slope, intercept } = _linReg(pts);

  // Average bar interval in seconds
  const intervals = candles.slice(-5).map((c, i, a) => i ? c.time - a[i-1].time : 0).filter(Boolean);
  const avgInterval = intervals.length ? intervals.reduce((a, b) => a + b) / intervals.length : 86400;

  const lastClose = candles[candles.length - 1];
  const lastIdx   = lookback - 1;
  const predPoints = [];

  // Anchor: last real candle so line connects cleanly
  predPoints.push({ time: lastClose.time, value: round2(intercept + slope * lastIdx) });

  for (let i = 1; i <= forwardBars; i++) {
    predPoints.push({
      time:  lastClose.time + Math.round(avgInterval * i),
      value: round2(intercept + slope * (lastIdx + i)),
    });
  }
  return predPoints;
}

function round2(n) { return Math.round(n * 100) / 100; }

async function loadChart(symbol) {
  if (!_chart) initChart();
  _chartSymbol = symbol;
  document.querySelectorAll('.chart-tab').forEach(t =>
    t.classList.toggle('active', t.dataset.sym === symbol));
  try {
    const res  = await fetch(`/api/chart/${encodeURIComponent(symbol)}`);
    const data = await res.json();
    const candles = (data.candles || []).sort((a, b) => a.time - b.time);
    _lastCandles[symbol] = candles;

    _candleSeries.setData(candles);
    _lineSeries.setData(candles.map(c => ({ time: c.time, value: c.close })));

    // Prediction line
    if (candles.length >= 5) {
      _predSeries.setData(buildPrediction(candles));
    } else {
      _predSeries.setData([]);
    }

    placeTradeMarkers(symbol, candles);
    _chart.timeScale().fitContent();
  } catch(e) { console.error('Chart error', e); }
}

function placeTradeMarkers(symbol, candles) {
  // Markers only work on the visible primary series
  const series = _chartType === 'candles' ? _candleSeries : _lineSeries;
  if (!series || !candles.length) return;
  const cards = (window._flashcards || []).filter(c => c.symbol === symbol);
  if (!cards.length) { series.setMarkers([]); return; }

  const markers = [];
  for (const c of cards) {
    const ts = c.timestamp ? Math.floor(new Date(c.timestamp).getTime() / 1000) : null;
    if (!ts) continue;
    const nearest = candles.reduce((a, b) =>
      Math.abs(b.time - ts) < Math.abs(a.time - ts) ? b : a);
    if (c.action === 'BUY') {
      markers.push({ time: nearest.time, position: 'belowBar', color: '#3fb950',
        shape: 'arrowUp', text: `BUY ${(c.risk_level||'').toUpperCase()}`, size: 1 });
    }
    if (c.exit_price != null) {
      const exitTs = ts + Math.round((c.hold_duration_hours || 1) * 3600);
      const nearestExit = candles.reduce((a, b) =>
        Math.abs(b.time - exitTs) < Math.abs(a.time - exitTs) ? b : a);
      const won = c.pnl_pct > 0;
      markers.push({ time: nearestExit.time, position: 'aboveBar',
        color: won ? '#00D4AA' : '#f85149', shape: 'arrowDown',
        text: `${(c.outcome||'SELL').toUpperCase()} ${c.pnl_pct != null ? (c.pnl_pct >= 0 ? '+' : '') + c.pnl_pct.toFixed(1) + '%' : ''}`,
        size: 1 });
    }
  }
  markers.sort((a, b) => a.time - b.time);
  try { series.setMarkers(markers); } catch(_) {}
}

function buildChartTabs(signals) {
  const syms = [...new Set((signals || []).map(s => s.symbol))];
  if (!syms.length) return;
  const tabs = document.getElementById('chart-tabs');
  const existing = new Set([...tabs.querySelectorAll('.chart-tab')].map(t => t.dataset.sym));
  syms.forEach(sym => {
    if (!existing.has(sym)) {
      const btn = document.createElement('button');
      btn.className = 'chart-tab' + (sym === _chartSymbol ? ' active' : '');
      btn.dataset.sym = sym;
      btn.textContent = sym;
      btn.onclick = () => loadChart(sym);
      tabs.appendChild(btn);
    }
  });
  if (!_chartSymbol && syms.length) loadChart(syms[0]);
}

// SSE
function connectSSE() {
  const evtSource = new EventSource('/events');
  evtSource.onmessage = (e) => {
    try {
      const state = JSON.parse(e.data);
      applyState(state);
    } catch {}
  };
  evtSource.onerror = () => {
    setTimeout(connectSSE, 5000);
    evtSource.close();
  };
}

// Init
fetch('/api/version').then(r=>r.json()).then(d => {
  const el = document.getElementById('version-badge');
  if (el && d.version) el.textContent = 'v' + d.version;
}).catch(()=>{});
fetchAll();
connectSSE();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return _HTML


def main(host: str = "127.0.0.1", port: int = 8000) -> None:
    uvicorn.run(app, host=host, port=port, log_level="warning")
