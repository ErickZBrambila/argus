"""FastAPI web dashboard with SSE real-time updates."""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import threading
from typing import AsyncGenerator

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)

# Shared state injected by main loop
_state: dict = {}
_paused: bool = False
_sse_queue: asyncio.Queue = asyncio.Queue()

# Chart data source — registered by main loop
_chart_source_fn = None   # callable(symbol: str) -> list[dict]


def register_chart_source(fn) -> None:
    global _chart_source_fn
    _chart_source_fn = fn

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
    try:
        _sse_queue.put_nowait(json.dumps(_state, default=str))
    except asyncio.QueueFull:
        pass

app = FastAPI(title="Argus Dashboard", docs_url=None, redoc_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def push_state(state: dict) -> None:
    """Called by the main loop to push new state to all SSE clients."""
    global _state
    _state = state
    try:
        _sse_queue.put_nowait(json.dumps(state, default=str))
    except asyncio.QueueFull:
        pass


def set_paused(v: bool) -> None:
    global _paused
    _paused = v


def is_paused() -> bool:
    return _paused


# ── REST endpoints ────────────────────────────────────────────────────────────

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


_force_close_queue: asyncio.Queue = asyncio.Queue()


def get_force_close_symbol() -> str | None:
    try:
        return _force_close_queue.get_nowait()
    except asyncio.QueueEmpty:
        return None


@app.get("/api/chart/{symbol}")
async def get_chart(symbol: str) -> dict:
    symbol = symbol.upper().strip()
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
    async def generator() -> AsyncGenerator[str, None]:
        # Send current state immediately on connect
        if _state:
            yield f"data: {json.dumps(_state, default=str)}\n\n"
        while True:
            try:
                data = await asyncio.wait_for(_sse_queue.get(), timeout=30.0)
                yield f"data: {data}\n\n"
            except asyncio.TimeoutError:
                yield ": heartbeat\n\n"

    return StreamingResponse(generator(), media_type="text/event-stream")


# ── UI ────────────────────────────────────────────────────────────────────────

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Argus — Trading Dashboard</title>
<style>
  :root {
    --bg: #0d1117;
    --surface: #161b22;
    --surface2: #21262d;
    --border: #30363d;
    --text: #e6edf3;
    --muted: #8b949e;
    --accent: #00d4aa;
    --green: #3fb950;
    --red: #f85149;
    --yellow: #d29922;
    --blue: #58a6ff;
    --radius: 8px;
    --font: -apple-system, BlinkMacSystemFont, "Segoe UI", "Inter", sans-serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: var(--font); font-size: 14px; min-height: 100vh; }
  a { color: var(--accent); text-decoration: none; }

  /* Layout */
  .app { display: flex; flex-direction: column; min-height: 100vh; }
  header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 0 16px; display: flex; align-items: center; justify-content: space-between; height: 56px; position: sticky; top: 0; z-index: 100; }
  .logo { font-size: 18px; font-weight: 700; color: var(--accent); letter-spacing: 2px; }
  .badges { display: flex; gap: 8px; align-items: center; }
  .badge { padding: 3px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; }
  .badge-paper { background: #1f6feb; color: #fff; }
  .badge-live  { background: var(--red); color: #fff; }
  .badge-kill  { background: var(--red); color: #fff; animation: pulse 1s infinite; }
  .badge-paused { background: var(--yellow); color: #000; }
  @keyframes pulse { 0%,100% { opacity:1; } 50% { opacity:.5; } }

  main { padding: 16px; display: grid; gap: 16px; }
  @media (min-width: 640px)  { main { padding: 20px; } }
  @media (min-width: 1024px) { main { grid-template-columns: repeat(2, 1fr); padding: 24px; } }
  @media (min-width: 1400px) { main { grid-template-columns: repeat(3, 1fr); } }

  .card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 16px; }
  .card-full { grid-column: 1 / -1; }
  .card-title { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; color: var(--muted); margin-bottom: 12px; }

  /* Stats grid */
  .stats-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }
  @media (min-width: 480px) { .stats-grid { grid-template-columns: repeat(4, 1fr); } }
  .stat { display: flex; flex-direction: column; gap: 4px; }
  .stat-label { font-size: 11px; color: var(--muted); }
  .stat-value { font-size: 20px; font-weight: 700; }
  .stat-value.lg { font-size: 24px; }

  /* Tables */
  .table-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { color: var(--muted); font-weight: 600; text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--border); font-size: 11px; text-transform: uppercase; letter-spacing: .5px; }
  td { padding: 8px 8px; border-bottom: 1px solid var(--border); }
  tr:last-child td { border-bottom: none; }
  .tr-hover:hover { background: var(--surface2); }
  .txt-right { text-align: right; }
  .txt-center { text-align: center; }

  /* Colors */
  .green { color: var(--green); }
  .red   { color: var(--red); }
  .yellow { color: var(--yellow); }
  .muted { color: var(--muted); }
  .accent { color: var(--accent); font-weight: 600; }

  /* Pill badges */
  .pill { display: inline-block; padding: 2px 8px; border-radius: 20px; font-size: 11px; font-weight: 700; }
  .pill-bullish { background: rgba(63,185,80,.15); color: var(--green); }
  .pill-bearish { background: rgba(248,81,73,.15); color: var(--red); }
  .pill-neutral { background: rgba(210,153,34,.15); color: var(--yellow); }
  .pill-buy  { background: rgba(63,185,80,.15); color: var(--green); }
  .pill-sell { background: rgba(248,81,73,.15); color: var(--red); }

  /* Buttons */
  .btn { display: inline-flex; align-items: center; gap: 6px; padding: 8px 16px; border-radius: var(--radius); border: none; font-size: 13px; font-weight: 600; cursor: pointer; transition: opacity .15s; }
  .btn:hover { opacity: .8; }
  .btn-primary { background: var(--accent); color: #000; }
  .btn-danger  { background: var(--red); color: #fff; }
  .btn-warning { background: var(--yellow); color: #000; }
  .btn-ghost   { background: var(--surface2); color: var(--text); border: 1px solid var(--border); }
  .controls { display: flex; gap: 10px; flex-wrap: wrap; }

  /* Modal */
  .modal-bg { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.7); z-index: 200; align-items: center; justify-content: center; }
  .modal-bg.open { display: flex; }
  .modal { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 24px; width: 90%; max-width: 400px; }
  .modal h3 { margin-bottom: 12px; font-size: 16px; }
  .modal p { color: var(--muted); margin-bottom: 20px; font-size: 13px; }
  .modal-actions { display: flex; gap: 10px; justify-content: flex-end; }

  /* Signal bars */
  .indicator-row { display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid var(--border); font-size: 12px; }
  .indicator-row:last-child { border-bottom: none; }

  /* Timestamp */
  .timestamp { font-size: 11px; color: var(--muted); text-align: right; padding-top: 8px; }

  /* Empty state */
  .empty { text-align: center; color: var(--muted); padding: 32px 0; font-size: 13px; }

  /* Per-account panels */
  .acct-panels { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
  .acct-panel { border: 1px solid var(--border); border-radius: var(--radius); padding: 14px; }
  .acct-panel.agentic { border-color: #00d4aa; }
  .acct-panel.default { border-color: #c084fc; }
  .acct-panel-title { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 1.2px; margin-bottom: 10px; }
  .acct-panel-title.agentic { color: #00d4aa; }
  .acct-panel-title.default { color: #c084fc; }
  .acct-equity { font-size: 22px; font-weight: 700; }
  .acct-equity.agentic { color: #00d4aa; }
  .acct-equity.default { color: #c084fc; }
  .acct-row { display: flex; justify-content: space-between; align-items: center; padding: 5px 0; border-bottom: 1px solid var(--border); font-size: 12px; }
  .acct-row:last-child { border-bottom: none; }
  .acct-row-label { color: var(--muted); }
  .acct-mode { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; }
  .acct-mode-auto     { background: rgba(0,212,170,.15);  color: var(--accent); }
  .acct-mode-approval { background: rgba(210,153,34,.15); color: var(--yellow); }
  .acct-positions-mini { margin-top: 10px; }
  .acct-pos-row { display: flex; justify-content: space-between; font-size: 12px; padding: 3px 0; }
  .acct-pos-sym { color: var(--accent); font-weight: 600; }
  .acct-total-bar { margin-top: 10px; padding-top: 10px; border-top: 1px solid var(--border); display: flex; justify-content: space-between; font-size: 12px; color: var(--muted); }

  /* Approval queue */
  .approval-card { border-color: var(--yellow); }
  .approval-item { border: 1px solid var(--border); border-radius: var(--radius); padding: 12px 14px; margin-bottom: 10px; background: var(--surface2); }
  .approval-item:last-child { margin-bottom: 0; }
  .approval-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px; }
  .approval-symbol { font-size: 16px; font-weight: 700; color: var(--accent); }
  .pill-risk-low    { background: rgba(63,185,80,.15);  color: var(--green); }
  .pill-risk-medium { background: rgba(251,191,36,.15); color: var(--yellow); }
  .pill-risk-high   { background: rgba(248,81,73,.15);  color: var(--red); }
  .approval-reasoning { font-size: 12px; color: var(--muted); margin: 6px 0 10px; line-height: 1.5; }
  .approval-meta { font-size: 11px; color: var(--text-dim); margin-bottom: 10px; }
  .approval-actions { display: flex; gap: 8px; }
  #approvals-section { display: none; }
  #approvals-section.has-items { display: block; }

  /* Flashcards */
  .fc-summary { display: flex; gap: 24px; flex-wrap: wrap; margin-bottom: 16px; padding-bottom: 14px; border-bottom: 1px solid var(--border); }
  .fc-stat { display: flex; flex-direction: column; gap: 2px; }
  .fc-stat-val { font-size: 18px; font-weight: 700; }
  .fc-grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); }
  .fc { border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; cursor: pointer; transition: border-color .15s; }
  .fc:hover { border-color: var(--accent); }
  .fc.fc-win  { border-left: 3px solid var(--green); }
  .fc.fc-loss { border-left: 3px solid var(--red); }
  .fc.fc-open { border-left: 3px solid var(--yellow); }
  .fc-front { padding: 12px 14px; background: var(--surface2); }
  .fc-top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
  .fc-symbol { font-size: 15px; font-weight: 700; color: var(--accent); }
  .fc-badges { display: flex; gap: 5px; align-items: center; }
  .fc-indicators { display: grid; grid-template-columns: 1fr 1fr; gap: 4px 12px; font-size: 11px; color: var(--muted); margin-bottom: 8px; }
  .fc-ind-val { color: var(--text); font-weight: 600; font-family: "SF Mono","Fira Code",monospace; }
  .fc-outcome { display: flex; justify-content: space-between; align-items: center; font-size: 12px; }
  .fc-back { padding: 10px 14px; background: var(--surface); border-top: 1px solid var(--border); display: none; }
  .fc.expanded .fc-back { display: block; }
  .fc-reasoning { font-size: 12px; color: var(--muted); line-height: 1.55; margin-bottom: 6px; }
  .fc-meta { font-size: 11px; color: var(--text-dim); }
  .pill-win  { background: rgba(63,185,80,.15);  color: var(--green); }
  .pill-loss { background: rgba(248,81,73,.15);  color: var(--red); }
  .pill-open { background: rgba(251,191,36,.15); color: var(--yellow); }
  .pill-buy  { background: rgba(63,185,80,.15);  color: var(--green); }
  .pill-sell { background: rgba(248,81,73,.15);  color: var(--red); }

  /* Hide values toggle */
  .btn-eye { background: none; border: 1px solid var(--border); color: var(--muted); padding: 5px 10px; border-radius: var(--radius-sm); cursor: pointer; font-size: 13px; transition: all .15s; }
  .btn-eye:hover { border-color: var(--accent); color: var(--accent); }
  body.hide-values .private { filter: blur(6px); user-select: none; transition: filter .2s; }
  body.hide-values .private:hover { filter: blur(0); }

  /* Price chart */
  .chart-toolbar { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px; margin-bottom: 12px; }
  .chart-tabs { display: flex; gap: 4px; flex-wrap: wrap; }
  .chart-tab { padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 600; cursor: pointer; border: 1px solid var(--border); background: none; color: var(--muted); transition: all .15s; }
  .chart-tab:hover { border-color: var(--accent); color: var(--accent); }
  .chart-tab.active { background: var(--accent); color: #000; border-color: var(--accent); }
  .chart-type-btns { display: flex; gap: 4px; }
  .chart-type-btn { padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; cursor: pointer; border: 1px solid var(--border); background: none; color: var(--muted); transition: all .15s; }
  .chart-type-btn:hover { border-color: var(--accent); color: var(--accent); }
  .chart-type-btn.active { background: var(--surface2); color: var(--text); border-color: var(--accent); }
  #price-chart { width: 100%; height: 340px; }
  .chart-legend { display: flex; gap: 16px; margin-top: 8px; font-size: 11px; color: var(--muted); flex-wrap: wrap; }
  .legend-item { display: flex; align-items: center; gap: 5px; }
  .legend-dot { width: 8px; height: 8px; border-radius: 50%; }
  .legend-dash { width: 16px; height: 2px; border-top: 2px dashed; }
</style>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
</head>
<body>
<div class="app">
  <header>
    <span class="logo">⬡ ARGUS</span>
    <div style="display:flex;align-items:center;gap:10px">
      <div class="badges" id="badges"></div>
      <button class="btn-eye" id="btn-eye" onclick="toggleValues()" title="Show/hide dollar amounts">👁</button>
    </div>
  </header>
  <main>

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
      <div class="card-title" style="color:var(--yellow)">⚠ Pending Approval — Default Account</div>
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
      <div class="card-title">Decision Flashcards <span class="muted" style="font-weight:400;text-transform:none;letter-spacing:0">— click any card to see reasoning</span></div>
      <div class="fc-summary" id="fc-summary"></div>
      <div class="fc-grid" id="fc-grid"><div class="empty">No trades recorded yet</div></div>
    </div>

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
let pendingCloseSymbol = null;
let valuesHidden = true;

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
    </div>`;
  }).join('');
}

function applyState(state) {
  paused = state.paused || false;
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
      return `<tr class="tr-hover">
        <td class="accent">${sym}</td>
        <td class="txt-right">${fmt(p.quantity,4)}</td>
        <td class="txt-right">${fmtDollar(p.entry_price)}</td>
        <td class="txt-right">${fmtDollar(p.current_price)}</td>
        <td class="txt-right ${pnlClass(pct)}">${pct >= 0 ? '+' : ''}${fmt(pct)}%</td>
        <td class="txt-right muted">${fmtDollar(p.stop_loss_price)}</td>
        <td class="txt-center"><button class="btn btn-danger" style="padding:4px 10px;font-size:11px" onclick="confirmClose('${sym.replace(/\s*\[.*?\]/,'')}')">Close</button></td>
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
        <td class="accent">${s.symbol}</td>
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
        <td class="muted">${t.time || ''}</td>
        <td class="accent">${t.symbol}</td>
        <td class="txt-center">${pill(side.toUpperCase(), side)}</td>
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
          <span class="approval-symbol">${side} ${a.symbol}</span>
          <span class="pill ${riskCls}">${(a.risk_level||'medium').toUpperCase()} RISK</span>
        </div>
        <div class="approval-meta">
          ${fmtDollar(a.dollar_amount)} &middot; Confidence ${fmt((a.confidence||0)*100,0)}% &middot; ${a.account_label||'Default'}
        </div>
        <div class="approval-reasoning">${a.reasoning||''}</div>
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
  renderFlashcards(state);
  buildChartTabs(state.signals);
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
          <span class="fc-symbol">${c.symbol}</span>
          <div class="fc-badges">
            <span class="pill pill-${(c.action||'buy').toLowerCase()}">${c.action}</span>
            <span class="pill pill-risk-${c.risk_level||'medium'}">${(c.risk_level||'medium').toUpperCase()}</span>
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
        <div class="fc-outcome">${outcomeHtml}</div>
      </div>
      <div class="fc-back">
        <div class="fc-reasoning">${c.reasoning||'No reasoning recorded.'}</div>
        <div class="fc-meta">
          <span class="private">Entry $${(c.entry_price||0).toFixed(2)} · $${(c.dollar_amount||0).toFixed(2)}</span> · ${c.account||''} · ${ts}
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
  const [status, positions, signals, trades] = await Promise.all([
    fetch('/api/status').then(r=>r.json()),
    fetch('/api/positions').then(r=>r.json()),
    fetch('/api/signals').then(r=>r.json()),
    fetch('/api/trades').then(r=>r.json()),
  ]);
  applyState({...status, ...positions, ...signals, ...trades});
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
fetchAll();
connectSSE();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return _HTML


def main(host: str = "0.0.0.0", port: int = 8000) -> None:
    uvicorn.run(app, host=host, port=port, log_level="warning")
