"""FastAPI web dashboard with SSE real-time updates."""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
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
</style>
</head>
<body>
<div class="app">
  <header>
    <span class="logo">⬡ ARGUS</span>
    <div class="badges" id="badges"></div>
  </header>
  <main>

    <!-- Stats overview -->
    <div class="card card-full">
      <div class="card-title">Portfolio Overview</div>
      <div class="stats-grid">
        <div class="stat">
          <span class="stat-label">Equity</span>
          <span class="stat-value lg accent" id="stat-equity">—</span>
        </div>
        <div class="stat">
          <span class="stat-label">Daily P&L</span>
          <span class="stat-value" id="stat-pnl">—</span>
        </div>
        <div class="stat">
          <span class="stat-label">Trades Today</span>
          <span class="stat-value" id="stat-trades">—</span>
        </div>
        <div class="stat">
          <span class="stat-label">Day Trades (5d)</span>
          <span class="stat-value" id="stat-daytrades">—</span>
        </div>
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

function fmt(n, decimals=2) {
  if (n == null) return '—';
  return Number(n).toFixed(decimals);
}
function fmtDollar(n) {
  if (n == null) return '—';
  return '$' + Number(n).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
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

function applyState(state) {
  paused = state.paused || false;
  updateBadges(state);

  const equity = state.equity || 0;
  const pnl = state.daily_pnl || 0;
  const pnlPct = state.daily_pnl_pct || 0;
  const dayTrades = state.day_trades || 0;

  document.getElementById('stat-equity').textContent = fmtDollar(equity);
  const pnlEl = document.getElementById('stat-pnl');
  pnlEl.textContent = `${pnl >= 0 ? '+' : ''}${fmtDollar(pnl)} (${pnlPct >= 0 ? '+' : ''}${fmt(pnlPct)}%)`;
  pnlEl.className = 'stat-value ' + pnlClass(pnl);
  document.getElementById('stat-trades').textContent = state.trade_count || 0;
  const dtEl = document.getElementById('stat-daytrades');
  dtEl.textContent = `${dayTrades} / 3`;
  dtEl.className = 'stat-value ' + (dayTrades >= 2 ? 'yellow' : '');

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
        <td class="txt-center"><button class="btn btn-danger" style="padding:4px 10px;font-size:11px" onclick="confirmClose('${sym}')">Close</button></td>
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
      </tr>`;
    }).join('');
  }

  document.getElementById('last-update').textContent = 'Last update: ' + new Date().toLocaleTimeString();
  document.getElementById('btn-pause').textContent = (state.paused || paused) ? '▶ Resume' : '⏸ Pause';
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
