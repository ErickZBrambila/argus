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
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger(__name__)

# Shared state injected by main loop
_state: dict = {}
_state_lock = threading.Lock()
_paused: bool = False

# Thread-safe queue: main loop (sync thread) → SSE broadcaster (async thread)
_sse_queue: stdlib_queue.Queue = stdlib_queue.Queue(maxsize=5)
# Per-subscriber async queues — only accessed within the event loop
_subscribers: set[asyncio.Queue] = set()

# Chart data source — registered by main loop
_chart_source_fn = None   # callable(symbol: str) -> list[dict]
_search_fn = None          # callable(query: str) -> list[{symbol, name}]
_autopilot = None         # Autopilot instance for runtime control

# Equity curve — ring buffer of {time, value} points for the session
import collections as _collections
_equity_history: collections.deque = _collections.deque(maxlen=480)  # ~8h at 60s interval


def register_chart_source(fn) -> None:
    global _chart_source_fn
    _chart_source_fn = fn


def register_search(fn) -> None:
    global _search_fn
    _search_fn = fn


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
    with _state_lock:
        _state["pending_approvals"] = approvals
        snapshot = dict(_state)
    _sse_push(json.dumps(snapshot, default=str))


# ── Financial news RSS (background poller) ────────────────────────────────────
_news_cache: list[dict] = []
_news_lock = threading.Lock()

_NEWS_FEEDS = [
    "https://feeds.finance.yahoo.com/rss/2.0/headline?region=US&lang=en-US",
    "https://feeds.marketwatch.com/marketwatch/topstories/",
    "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
]


def _news_fetch_loop() -> None:
    import urllib.request
    import xml.etree.ElementTree as ET
    import time as _time

    while True:
        for url in _NEWS_FEEDS:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Argus/0.5.3 (+financial-dashboard)"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    root = ET.fromstring(resp.read(512 * 1024))  # 512 KB cap
                items = []
                for item in root.findall(".//item")[:15]:
                    title = (item.findtext("title") or "").strip()
                    link  = (item.findtext("link")  or "").strip()
                    # Only allow https:// URLs — blocks javascript: and data: XSS vectors
                    safe_url = link if link.startswith("https://") else None
                    if title:
                        items.append({"headline": title, "url": safe_url})
                if items:
                    with _news_lock:
                        _news_cache[:] = items
                    logger.debug("News: fetched %d headlines from %s", len(items), url)
                    break
            except Exception as exc:
                logger.debug("News fetch failed (%s): %s", url, exc)
        _time.sleep(300)  # refresh every 5 minutes


def _start_news_poller() -> None:
    t = threading.Thread(target=_news_fetch_loop, daemon=True, name="argus-news")
    t.start()


# ── Runtime watchlist (mutable; seeded from config at startup) ────────────────
_watchlist_lock = threading.Lock()
_runtime_watchlist: list[str] = []


def set_watchlist_base(symbols: list[str]) -> None:
    global _runtime_watchlist
    with _watchlist_lock:
        _runtime_watchlist = list(symbols)
    _state["watchlist"] = list(symbols)


def get_watchlist() -> list[str]:
    with _watchlist_lock:
        return list(_runtime_watchlist)


# ── Investigations (up to 3 AI deep-dives) ───────────────────────────────────
_MAX_INVESTIGATIONS = 3
_investigations: dict[str, dict] = {}
_investigation_lock = threading.Lock()
_investigate_fn = None   # callable(symbol, signal, headlines) → dict


def register_investigate(fn) -> None:
    global _investigate_fn
    _investigate_fn = fn


def _run_investigation(symbol: str) -> None:
    with _investigation_lock:
        if symbol not in _investigations:
            return
        _investigations[symbol]["status"] = "running"
        _investigations[symbol]["started_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    _push_investigation_state()

    try:
        signals = _state.get("signals", [])
        signal = next((s for s in signals if s.get("symbol") == symbol), {})
        with _news_lock:
            all_headlines = list(_news_cache)
        relevant = [h for h in all_headlines if symbol.lower() in h.get("headline", "").lower()]
        headlines = relevant[:8] if relevant else all_headlines[:6]

        if _investigate_fn is None:
            raise RuntimeError("No investigation function registered — set ANTHROPIC_API_KEY")

        result = _investigate_fn(symbol, signal, headlines)

        with _investigation_lock:
            if symbol not in _investigations:
                return  # user deleted the card while investigation was running
            _investigations[symbol].update({
                "status": "complete",
                "verdict":    result.get("verdict", "Unknown"),
                "confidence": float(result.get("confidence") or 0),
                "summary":    result.get("summary", ""),
                "findings":   result.get("findings") or [],
                "risks":      result.get("risks") or [],
                "timeframe":  result.get("timeframe", ""),
                "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            })
    except Exception as exc:
        logger.error("Investigation failed for %s: %s", symbol, exc)
        with _investigation_lock:
            if symbol not in _investigations:
                return
            _investigations[symbol].update({
                "status": "error",
                "error": str(exc),
                "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            })

    _push_investigation_state()


def _push_investigation_state() -> None:
    with _investigation_lock:
        inv = dict(_investigations)
    with _state_lock:
        _state["investigations"] = inv
        snapshot = {**_state, "investigations": inv}
    _sse_push(json.dumps(snapshot, default=str))


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

# ── Auth ──────────────────────────────────────────────────────────────────────
_dashboard_token: str = ""


def _configure_auth(token: str) -> None:
    global _dashboard_token
    _dashboard_token = token


def _require_auth(x_argus_token: str = Header(default="")) -> None:
    if _dashboard_token and x_argus_token != _dashboard_token:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Argus-Token header")


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


_AUTO_TRIGGER_THRESHOLD = 0.55  # confidence threshold for auto-investigation
_auto_triggered: set = set()    # symbols already auto-triggered this session

def _auto_trigger_investigations(state: dict) -> None:
    """Auto-queue an investigation when a signal is strong + confident enough."""
    if _investigate_fn is None:
        return
    signals = state.get("signals") or []
    for sig in signals:
        sym = sig.get("symbol", "")
        if not sym:
            continue
        composite = sig.get("composite", "neutral")
        confidence = float(sig.get("confidence") or 0)
        if composite not in ("bullish", "bearish"):
            continue
        if confidence < _AUTO_TRIGGER_THRESHOLD:
            continue
        with _investigation_lock:
            already_active = sym in _investigations and _investigations[sym].get("status") in ("queued", "running", "complete")
        if already_active or sym in _auto_triggered:
            continue
        if len(_investigations) >= _MAX_INVESTIGATIONS:
            continue
        _auto_triggered.add(sym)
        with _investigation_lock:
            _investigations[sym] = {
                "symbol": sym,
                "status": "queued",
                "queued_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "auto_triggered": True,
            }
        _push_investigation_state()
        threading.Thread(target=_run_investigation, args=(sym,), daemon=True, name=f"inv-{sym}").start()


def push_state(state: dict) -> None:
    """Called by the main loop (sync thread) to push new state to all SSE clients."""
    global _state
    try:
        from argus.dashboard.log_buffer import get_recent
        state = {**state, "logs": get_recent(100)}
    except Exception:
        pass
    equity = state.get("equity")
    if equity:
        _equity_history.append({
            "time": int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
            "value": float(equity),
        })
    with _state_lock:
        _state = state
        snapshot = {**state, "equity_history": list(_equity_history)}
    _sse_push(json.dumps(snapshot, default=str))
    _auto_trigger_investigations(state)


def set_paused(v: bool) -> None:
    global _paused
    _paused = v


def is_paused() -> bool:
    return _paused


def seed_news(items: list[dict]) -> None:
    """Seed the news cache (for dev/testing). Items: [{headline, url?}]."""
    with _news_lock:
        _news_cache[:] = items


def seed_equity(points: list[dict]) -> None:
    """Seed historical equity points (for dev/testing). Points: [{time, value}]."""
    _equity_history.extend(points)


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


@app.post("/api/scan-interval", dependencies=[Depends(_require_auth)])
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


@app.post("/api/pause", dependencies=[Depends(_require_auth)])
async def pause_trading() -> dict:
    set_paused(True)
    logger.info("Autopilot paused via web dashboard")
    return {"status": "paused"}


@app.post("/api/resume", dependencies=[Depends(_require_auth)])
async def resume_trading() -> dict:
    set_paused(False)
    logger.info("Autopilot resumed via web dashboard")
    return {"status": "running"}


@app.post("/api/close/{symbol}", dependencies=[Depends(_require_auth)])
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
_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.]{0,9}$")


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


@app.post("/api/promote/{symbol}", dependencies=[Depends(_require_auth)])
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


@app.get("/api/investigate")
async def get_investigations() -> dict:
    with _investigation_lock:
        return {"investigations": dict(_investigations)}


@app.post("/api/investigate", dependencies=[Depends(_require_auth)])
async def start_investigation(body: dict) -> dict:
    symbol = (body.get("symbol") or "").strip().upper()
    if not symbol or not _SYMBOL_RE.match(symbol):
        raise HTTPException(status_code=400, detail="Invalid symbol")
    with _investigation_lock:
        if len(_investigations) >= _MAX_INVESTIGATIONS and symbol not in _investigations:
            raise HTTPException(status_code=400, detail=f"Maximum {_MAX_INVESTIGATIONS} investigations already active — remove one first")
        _investigations[symbol] = {
            "symbol": symbol,
            "status": "queued",
            "queued_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    _push_investigation_state()
    threading.Thread(target=_run_investigation, args=(symbol,), daemon=True, name=f"inv-{symbol}").start()
    logger.info("Investigation queued: %s", symbol)
    return {"status": "queued", "symbol": symbol}


@app.delete("/api/investigate/{symbol}", dependencies=[Depends(_require_auth)])
async def remove_investigation(symbol: str) -> dict:
    symbol = symbol.upper().strip()
    if not _SYMBOL_RE.match(symbol):
        raise HTTPException(status_code=400, detail="Invalid symbol")
    with _investigation_lock:
        _investigations.pop(symbol, None)
    _auto_triggered.discard(symbol)   # allow re-trigger if signals re-emerge
    _push_investigation_state()
    return {"status": "removed", "symbol": symbol}


@app.get("/api/news")
async def get_news() -> dict:
    with _news_lock:
        return {"headlines": list(_news_cache)}


@app.get("/api/search", dependencies=[Depends(_require_auth)])
async def search_symbols(q: str = Query(default="", max_length=60)) -> dict:
    q = q.strip()
    if not q or _search_fn is None:
        return {"results": []}
    try:
        results = await asyncio.get_event_loop().run_in_executor(None, _search_fn, q)
        return {"results": results or []}
    except Exception as exc:
        logger.warning("Symbol search error: %s", exc)
        return {"results": []}


@app.get("/api/flashcards")
async def get_flashcards() -> dict:
    cards = _state.get("flashcards", [])
    summary = _state.get("flashcard_summary", {})
    return {"flashcards": cards, "summary": summary}


@app.get("/api/watchlist")
async def get_watchlist_api() -> dict:
    return {"watchlist": get_watchlist()}


@app.post("/api/watchlist", dependencies=[Depends(_require_auth)])
async def add_to_watchlist_api(body: dict) -> dict:
    # Changes are in-memory only; they revert to WATCHLIST in .env on restart.
    symbol = (body.get("symbol") or "").strip().upper()
    if not symbol or not _SYMBOL_RE.match(symbol):
        raise HTTPException(status_code=400, detail="Invalid symbol")
    with _watchlist_lock:
        if symbol not in _runtime_watchlist:
            _runtime_watchlist.append(symbol)
        wl = list(_runtime_watchlist)
    with _state_lock:
        _state["watchlist"] = wl
        snapshot = dict(_state)
    _sse_push(json.dumps(snapshot, default=str))
    logger.info("Watchlist add: %s → %s", symbol, wl)
    return {"watchlist": wl}


@app.delete("/api/watchlist/{symbol}", dependencies=[Depends(_require_auth)])
async def remove_from_watchlist_api(symbol: str) -> dict:
    # Changes are in-memory only; they revert to WATCHLIST in .env on restart.
    symbol = symbol.upper().strip()
    if not _SYMBOL_RE.match(symbol):
        raise HTTPException(status_code=400, detail="Invalid symbol")
    with _watchlist_lock:
        _runtime_watchlist[:] = [s for s in _runtime_watchlist if s != symbol]
        wl = list(_runtime_watchlist)
    with _state_lock:
        _state["watchlist"] = wl
        snapshot = dict(_state)
    _sse_push(json.dumps(snapshot, default=str))
    logger.info("Watchlist remove: %s → %s", symbol, wl)
    return {"watchlist": wl}


@app.get("/api/approvals")
async def get_approvals() -> dict:
    with _approval_lock:
        return {"approvals": dict(_pending_approvals)}


@app.post("/api/approve/{trade_id}", dependencies=[Depends(_require_auth)])
async def approve_trade(trade_id: str) -> dict:
    with _approval_lock:
        if trade_id not in _pending_approvals:
            raise HTTPException(status_code=404, detail="Trade not found or already decided")
        _approval_decisions[trade_id] = "approved"
    logger.info("Trade %s approved via dashboard", trade_id)
    _push_approvals_state()
    return {"status": "approved", "trade_id": trade_id}


@app.post("/api/deny/{trade_id}", dependencies=[Depends(_require_auth)])
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
                with _state_lock:
                    initial = {**_state, "equity_history": list(_equity_history)}
                yield f"data: {json.dumps(initial, default=str)}\n\n"
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
  .market-countdown { display: flex; align-items: center; gap: 5px; font-size: 11.5px; color: var(--muted); padding: 3px 8px; background: var(--surface2); border: 1px solid var(--border); border-radius: 6px; }
  .market-countdown .mc-label { white-space: nowrap; }
  .market-countdown .mc-val { font-family: var(--mono); font-weight: 700; font-size: 12px; color: var(--text); letter-spacing: 0.5px; min-width: 52px; }

  /* ── Ticker bar ─────────────────────────────────────────────────────────── */
  .ticker-bar {
    background: #0d1117;
    border-bottom: 1px solid var(--border);
    height: 34px;
    position: sticky;
    top: 60px;
    z-index: 99;
    display: flex;
    align-items: center;
    width: 100%;
  }
  .ticker-scrollport {
    flex: 1;
    min-width: 0;
    overflow: hidden;
    height: 100%;
    display: flex;
    align-items: center;
  }
  .ticker-track {
    display: inline-flex;
    align-items: center;
    white-space: nowrap;
    animation: ticker-scroll linear infinite;
    will-change: transform;
    flex-shrink: 0;
  }
  .ticker-track:hover { animation-play-state: paused; cursor: default; }
  .ticker-speed-wrap { display: flex; gap: 2px; padding: 0 6px; flex-shrink: 0; background: #0d1117; border-left: 1px solid var(--border); height: 100%; align-items: center; }
  .ticker-speed-btn { background: none; border: 1px solid transparent; border-radius: 3px; color: var(--muted); font-size: 11px; line-height: 1; padding: 2px 4px; cursor: pointer; transition: all .15s; }
  .ticker-speed-btn:hover, .ticker-speed-btn.active { border-color: var(--border); color: var(--text); background: var(--surface2); }
  @keyframes ticker-scroll {
    from { transform: translateX(0); }
    to   { transform: translateX(-16.6667%); }  /* 1/6 of 6 copies = seamless */
  }
  .ticker-item { display: inline-flex; align-items: center; gap: 5px; padding: 0 16px; font-size: 12px; font-family: var(--mono); flex-shrink: 0; }
  .ticker-sym   { font-weight: 700; color: var(--text); letter-spacing: .3px; }
  .ticker-price { color: var(--text); font-variant-numeric: tabular-nums; }
  .ticker-up    { color: var(--green); font-size: 10px; }
  .ticker-down  { color: var(--danger); font-size: 10px; }
  .ticker-flat  { color: var(--muted); font-size: 10px; }
  .ticker-dot   { color: var(--border); font-size: 9px; padding: 0 4px; flex-shrink: 0; }
  .ticker-divider { color: rgba(0,212,170,.4); font-size: 10px; padding: 0 14px; flex-shrink: 0; letter-spacing: 2px; }
  .ticker-news-item { display: inline-flex; align-items: center; gap: 7px; padding: 0 16px; flex-shrink: 0; }
  .ticker-news-badge { font-size: 9px; font-weight: 800; color: #000d0a; background: var(--accent); border-radius: 3px; padding: 1px 5px; letter-spacing: .5px; flex-shrink: 0; }
  .ticker-headline { font-size: 12px; color: var(--muted); max-width: 380px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; text-decoration: none; }
  a.ticker-headline:hover { color: var(--text); text-decoration: underline; cursor: pointer; }

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

  /* ── Charts tab — full-width single column, no multi-col grid ──────────── */
  #tab-charts.active { display: flex !important; flex-direction: column; grid-template-columns: none; gap: 14px; }

  /* Toolbar card */
  .ct-toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
  .wl-add-row { display: flex; gap: 6px; }
  .wl-add-input { background: var(--surface2); border: 1px solid var(--border); border-radius: var(--radius-sm); color: var(--text); font-size: 13px; padding: 7px 10px; font-family: var(--mono); text-transform: uppercase; width: 120px; }
  .wl-add-input::placeholder { text-transform: none; color: var(--text-dim); }
  .wl-add-input:focus { outline: none; border-color: var(--accent); }
  .wl-add-btn { background: var(--accent); color: #000d0a; border: none; border-radius: var(--radius-sm); font-weight: 700; font-size: 12px; padding: 7px 12px; cursor: pointer; white-space: nowrap; }
  .wl-add-btn:hover { background: #00ebc2; }
  .ct-divider { width: 1px; height: 20px; background: var(--border); flex-shrink: 0; }
  .suggest-chips { display: flex; flex-wrap: wrap; gap: 5px; }
  .suggest-chip { background: var(--surface2); border: 1px solid var(--border); color: var(--text); border-radius: 999px; font-size: 11px; font-weight: 600; padding: 3px 10px; cursor: pointer; transition: border-color .15s, color .15s; }
  .suggest-chip:hover { border-color: var(--accent); color: var(--accent); }

  /* Dashlet grid — draggable cards */
  .ct-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); gap: 14px; }
  .ct-dashlet {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px 16px 16px;
    cursor: grab;
    user-select: none;
    transition: box-shadow .15s, border-color .15s;
  }
  .ct-dashlet:hover { border-color: rgba(0,212,170,.35); box-shadow: 0 0 0 1px rgba(0,212,170,.15), 0 4px 18px rgba(0,0,0,.4); }
  .ct-dashlet.sortable-ghost { opacity: .35; }
  .ct-dashlet.sortable-drag { cursor: grabbing; box-shadow: 0 8px 32px rgba(0,0,0,.6); }
  .ct-dashlet-header { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; cursor: grab; }
  .ct-dashlet-sym { font-size: 15px; font-weight: 700; color: var(--accent); flex: 1; }
  .ct-dashlet-price { font-size: 13px; font-weight: 600; font-variant-numeric: tabular-nums; color: var(--text); }
  .ct-dashlet-sig { font-size: 11px; }
  .ct-dashlet-remove { background: none; border: none; color: var(--muted); cursor: pointer; font-size: 14px; padding: 2px 5px; border-radius: 4px; line-height: 1; }
  .ct-dashlet-remove:hover { color: var(--danger); background: rgba(248,81,73,.12); }
  .ct-chart-area { width: 100%; height: 240px; border-radius: var(--radius-sm); overflow: hidden; }
  .ct-dashlet-footer { display: flex; align-items: center; gap: 10px; margin-top: 10px; font-size: 11px; flex-wrap: wrap; }
  .ct-stat { color: var(--muted); }
  .ct-stat span { color: var(--text); font-weight: 700; font-variant-numeric: tabular-nums; }
  .ct-tf-btns { display: flex; gap: 2px; margin-left: auto; margin-right: 4px; }
  .ct-tf-btn { background: none; border: 1px solid transparent; border-radius: 3px; color: var(--muted); font-size: 9px; font-weight: 700; padding: 1px 5px; cursor: pointer; letter-spacing: .3px; }
  .ct-tf-btn.active, .ct-tf-btn:hover { background: var(--surface2); color: var(--text); border-color: var(--border); }
  .ct-type-btns { display: flex; gap: 3px; }
  .ct-type-btn { background: none; border: 1px solid var(--border); border-radius: 4px; color: var(--muted); font-size: 10px; font-weight: 700; padding: 2px 7px; cursor: pointer; }
  .ct-type-btn.active { background: var(--surface2); color: var(--text); border-color: var(--accent); }
  .ct-crosshair-tooltip { display: none; position: absolute; top: 6px; background: rgba(13,17,23,.88); border: 1px solid var(--border); border-radius: 4px; padding: 3px 8px; font-size: 10.5px; color: var(--text); font-family: var(--mono); pointer-events: none; white-space: nowrap; z-index: 10; }
  .eq-range-btn { background: none; border: 1px solid var(--border); border-radius: 4px; color: var(--muted); font-size: 10px; font-weight: 700; padding: 2px 8px; cursor: pointer; transition: all .15s; }
  .eq-range-btn.active, .eq-range-btn:hover { background: var(--surface2); color: var(--text); border-color: var(--accent); }

  /* ── Symbol search autocomplete ─────────────────────────────────────────── */
  .wl-search-wrap { position: relative; }
  .wl-add-input { width: 220px; }
  .wl-dropdown { position: absolute; top: calc(100% + 4px); left: 0; min-width: 280px; background: var(--surface2); border: 1px solid var(--border); border-radius: var(--radius-sm); z-index: 200; overflow: hidden; box-shadow: 0 8px 24px rgba(0,0,0,.55); display: none; }
  .wl-dropdown.open { display: block; }
  .wl-dd-item { display: flex; align-items: center; gap: 10px; padding: 9px 12px; cursor: pointer; transition: background .1s; }
  .wl-dd-item:hover, .wl-dd-item.wl-dd-sel { background: rgba(0,212,170,.12); }
  .wl-dd-sym { font-family: var(--mono); font-weight: 700; font-size: 13px; color: var(--accent); min-width: 55px; }
  .wl-dd-name { font-size: 12px; color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .wl-dd-empty { padding: 10px 14px; font-size: 12px; color: var(--text-dim); }
  .wl-dd-searching { padding: 10px 14px; font-size: 12px; color: var(--muted); }

  /* ── Investigations tab ─────────────────────────────────────────────────── */
  #tab-investigations.active { display: flex !important; flex-direction: column; grid-template-columns: none; gap: 14px; }
  .inv-add-bar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
  .inv-slots { font-size: 12px; color: var(--muted); margin-left: 4px; }
  .inv-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(360px, 1fr)); gap: 14px; }
  .inv-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 18px 20px;
    transition: border-color .2s, box-shadow .2s;
  }
  .inv-card.running  { border-color: rgba(210,153,34,.4); }
  .inv-card.bullish  { border-color: rgba(0,212,170,.35); background: rgba(0,212,170,.03); }
  .inv-card.bearish  { border-color: rgba(248,81,73,.35); background: rgba(248,81,73,.02); }
  .inv-card.neutral  { border-color: rgba(88,166,255,.25); }
  .inv-header { display: flex; align-items: center; gap: 8px; margin-bottom: 14px; flex-wrap: wrap; }
  .inv-sym  { font-size: 20px; font-weight: 800; color: var(--accent); letter-spacing: .5px; }
  .inv-status { font-size: 11px; font-weight: 700; padding: 2px 7px; border-radius: 999px; }
  .inv-status.running { color: var(--yellow); background: rgba(210,153,34,.15); border: 1px solid rgba(210,153,34,.3); }
  .inv-status.queued  { color: var(--muted);  background: var(--surface2); border: 1px solid var(--border); }
  .inv-status.complete { color: var(--green); background: rgba(63,185,80,.12); border: 1px solid rgba(63,185,80,.25); }
  .inv-status.error   { color: var(--danger); background: rgba(248,81,73,.12); border: 1px solid rgba(248,81,73,.25); }
  .inv-age { font-size: 11px; color: var(--text-dim); }
  .inv-actions { margin-left: auto; display: flex; gap: 6px; }
  .inv-btn { background: none; border: 1px solid var(--border); color: var(--muted); cursor: pointer; font-size: 11px; font-weight: 600; padding: 3px 9px; border-radius: var(--radius-sm); transition: all .15s; }
  .inv-btn:hover { border-color: var(--accent); color: var(--accent); }
  .inv-btn.danger:hover { border-color: var(--danger); color: var(--danger); }
  .inv-verdict { font-size: 17px; font-weight: 800; margin-bottom: 4px; }
  .inv-verdict.bullish { color: var(--green); }
  .inv-verdict.bearish { color: var(--danger); }
  .inv-verdict.neutral { color: var(--blue); }
  .inv-verdict.caution { color: var(--yellow); }
  .inv-meta { display: flex; align-items: center; gap: 10px; margin-bottom: 14px; font-size: 12px; color: var(--muted); }
  .inv-conf-bar { flex: 1; height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; }
  .inv-conf-fill { height: 100%; border-radius: 2px; background: var(--accent); transition: width .6s ease; }
  .inv-summary { font-size: 13px; color: var(--text); line-height: 1.65; margin-bottom: 16px; padding: 12px 14px; background: var(--surface2); border-radius: var(--radius-sm); border-left: 3px solid var(--accent); }
  .inv-section { margin-bottom: 12px; }
  .inv-section-lbl { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: .8px; color: var(--muted); margin-bottom: 6px; }
  .inv-list { list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: 5px; }
  .inv-list li { font-size: 12.5px; color: var(--text); display: flex; gap: 8px; line-height: 1.5; }
  .inv-list li::before { content: '▸'; color: var(--accent); flex-shrink: 0; margin-top: 1px; }
  .inv-risks li::before { content: '⚠'; color: var(--yellow); flex-shrink: 0; }
  .inv-news-section { margin-top: 16px; padding-top: 14px; border-top: 1px solid var(--border-subtle); }
  .inv-news-item { font-size: 11.5px; color: var(--muted); padding: 5px 0; border-bottom: 1px solid var(--border-subtle); line-height: 1.5; }
  .inv-news-item:last-child { border-bottom: none; }
  .inv-thinking { display: flex; align-items: center; gap: 12px; padding: 20px 0; color: var(--muted); font-size: 13px; }
  .inv-spinner { width: 20px; height: 20px; border: 2px solid var(--border); border-top-color: var(--accent); border-radius: 50%; animation: inv-spin .75s linear infinite; flex-shrink: 0; }
  @keyframes inv-spin { to { transform: rotate(360deg); } }

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
  .fc-grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); }
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
  .fc-front { padding: 14px 16px; background: var(--surface2); }
  .fc-top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
  .fc-symbol { font-size: 15px; font-weight: 700; color: var(--accent); font-family: var(--mono); letter-spacing: 0.5px; }
  .fc-confidence-row { display: flex; align-items: center; gap: 8px; margin-bottom: 9px; flex-wrap: wrap; }
  .fc-conf-label { font-size: 12px; color: var(--muted); }
  .fc-reasoning-preview {
    font-size: 12.5px;
    color: var(--text);
    font-style: italic;
    line-height: 1.55;
    margin-bottom: 10px;
    padding: 8px 11px;
    background: rgba(0,0,0,.18);
    border-radius: var(--radius-sm);
    border-left: 2px solid rgba(0,212,170,.35);
  }
  .fc-indicators {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 5px 12px;
    font-size: 12px;
    margin-bottom: 10px;
  }
  .fc-indicators .muted { color: var(--text-dim); }
  .fc-ind-val { color: var(--text); font-weight: 600; }
  .fc-outcome { display: flex; justify-content: space-between; align-items: center; font-size: 12px; margin-top: 2px; flex-wrap: wrap; gap: 4px; }
  .fc-expand-hint { font-size: 10px; color: var(--text-dim); transition: opacity .15s; white-space: nowrap; }
  .fc.expanded .fc-expand-hint { opacity: 0; pointer-events: none; }
  .fc-back { padding: 14px 16px; background: var(--surface3); border-top: 1px solid var(--border); display: none; }
  .fc.expanded .fc-back { display: block; }
  .fc-back-section { margin-bottom: 14px; }
  .fc-back-title { font-size: 10.5px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; color: var(--text-dim); margin-bottom: 7px; }
  .fc-reasoning { font-size: 12.5px; color: var(--muted); line-height: 1.65; margin-bottom: 4px; }
  .fc-meta { font-size: 11px; color: var(--text-dim); border-top: 1px solid var(--border-subtle); padding-top: 7px; margin-top: 4px; }
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
  .chart-tab { display: inline-flex; align-items: center; gap: 5px; padding: 4px 8px 4px 12px; }
  .chart-tab-x { background: none; border: none; cursor: pointer; color: inherit; opacity: .55; font-size: 12px; line-height: 1; padding: 0 1px; }
  .chart-tab-x:hover { opacity: 1; }
  .chart-tab.active .chart-tab-x { color: #000d0a; }
  .chart-add-btn { padding: 4px 10px; border-radius: 20px; font-size: 14px; font-weight: 700; cursor: pointer; border: 1px dashed var(--border); background: none; color: var(--muted); line-height: 1; transition: border-color .15s, color .15s; }
  .chart-add-btn:hover { border-color: var(--accent); color: var(--accent); }
  .chart-search-wrap { position: relative; display: none; }
  .chart-search-wrap.open { display: block; }
  .chart-search-input { background: var(--surface2); border: 1px solid var(--accent); border-radius: var(--radius-sm); color: var(--text); font-size: 12px; padding: 4px 10px; font-family: var(--mono); width: 180px; }
  .chart-search-input:focus { outline: none; }
  .chart-search-dd { position: absolute; top: calc(100% + 4px); left: 0; min-width: 260px; background: var(--surface2); border: 1px solid var(--border); border-radius: var(--radius-sm); z-index: 200; overflow: hidden; box-shadow: 0 8px 24px rgba(0,0,0,.55); }
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
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"
        integrity="sha384-JZigAjwiaZtkUbA44CWkPaT3iBb/mU5pO6QOANp+OqHd4q+1+7MG1kzp2OOP9ZfP"
        crossorigin="anonymous"></script>
<script src="https://unpkg.com/sortablejs@1.15.2/Sortable.min.js"
        integrity="sha384-BSxuMLxX+FCbTdYec3TbXlnMGEEM2QXTFdtDaveen71o+jswm2J36+xFqp8k4VHM"
        crossorigin="anonymous"></script>
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
      <div class="market-countdown" id="market-countdown">
        <span class="mc-label" id="mc-label">—</span>
        <span class="mc-val" id="mc-val">—</span>
      </div>
      <div class="countdown-wrap">
        <span>Next scan</span>
        <span class="countdown-val" id="countdown">—</span>
      </div>
      <div class="badges" id="badges"></div>
      <button class="btn-eye" id="btn-eye" onclick="toggleValues()" title="Show/hide dollar amounts">👁</button>
    </div>
  </header>
  <div class="ticker-bar">
    <div class="ticker-scrollport">
      <div class="ticker-track" id="ticker-track"></div>
    </div>
    <div class="ticker-speed-wrap">
      <button class="ticker-speed-btn" onclick="tickerSpeed(this,0.4)" title="Slow">🐢</button>
      <button class="ticker-speed-btn active" onclick="tickerSpeed(this,1)" title="Normal">▶</button>
      <button class="ticker-speed-btn" onclick="tickerSpeed(this,2.5)" title="Fast">⚡</button>
    </div>
  </div>
  <nav class="tab-bar">
    <button class="tab-btn active" onclick="switchTab('dashboard')">Dashboard</button>
    <button class="tab-btn" onclick="switchTab('performance')">Performance</button>
    <button class="tab-btn" onclick="switchTab('charts')">Charts</button>
    <button class="tab-btn" onclick="switchTab('investigations')">Investigations</button>
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

    <!-- Equity curve -->
    <div class="card card-full">
      <div class="card-title" style="display:flex;align-items:center;gap:10px;">
        Equity Curve
        <span id="eq-range-btns" style="display:flex;gap:4px;margin-left:auto">
          <button class="eq-range-btn active" onclick="eqSetRange('session')">Session</button>
          <button class="eq-range-btn" onclick="eqSetRange('1h')">1H</button>
          <button class="eq-range-btn" onclick="eqSetRange('30m')">30M</button>
        </span>
      </div>
      <div id="eq-chart" style="height:160px;position:relative"></div>
      <div id="eq-stats" style="display:flex;gap:20px;padding:8px 2px 0;font-size:12px;color:var(--muted)"></div>
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
        <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;flex:1;">
          <div class="chart-tabs" id="chart-tabs"></div>
          <button class="chart-add-btn" onclick="chartSearchOpen()" title="Add symbol">＋</button>
          <div class="chart-search-wrap" id="chart-search-wrap">
            <input class="chart-search-input" id="chart-search-input" placeholder="Search name or ticker…"
                   autocomplete="off"
                   oninput="chartSearchDebounce(this.value)"
                   onkeydown="chartSearchKeydown(event)"
                   onblur="setTimeout(chartSearchClose,150)">
            <div class="chart-search-dd" id="chart-search-dd"></div>
          </div>
        </div>
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
      <div class="card-title">Trade Decisions <span style="font-weight:400;text-transform:none;letter-spacing:0;color:var(--text-dim)">— what the AI did and why · click any card to see the full reasoning</span></div>
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

  <div class="tab-pane" id="tab-charts">

    <!-- Toolbar -->
    <div class="card card-full">
      <div class="ct-toolbar">
        <div class="wl-add-row">
          <div class="wl-search-wrap">
            <input class="wl-add-input" id="wl-add-input" placeholder="Search name or ticker…"
                   autocomplete="off"
                   oninput="ctSearchDebounce(this.value)"
                   onkeydown="ctSearchKeydown(event)"
                   onblur="setTimeout(ctDropdownClose,150)">
            <div class="wl-dropdown" id="wl-dropdown"></div>
          </div>
          <button class="wl-add-btn" onclick="ctAddFromInput()">+ Add</button>
        </div>
        <div class="ct-divider"></div>
        <div style="font-size:11px;font-weight:700;color:var(--muted);white-space:nowrap">Suggested:</div>
        <div class="suggest-chips" id="suggest-chips"></div>
      </div>
    </div>

    <!-- Draggable dashlet grid -->
    <div class="ct-grid" id="ct-grid">
      <div class="empty" style="grid-column:1/-1;padding:40px 0">No symbols in watchlist — add one above</div>
    </div>

  </div><!-- /tab-charts -->

  <div class="tab-pane" id="tab-investigations">

    <!-- Add symbol toolbar -->
    <div class="card card-full">
      <div class="inv-add-bar">
        <div class="wl-search-wrap">
          <input class="wl-add-input" id="inv-search-input" placeholder="Search symbol to investigate…"
                 autocomplete="off" style="width:260px"
                 oninput="invSearchDebounce(this.value)"
                 onkeydown="invSearchKeydown(event)"
                 onblur="setTimeout(invDropdownClose,150)">
          <div class="wl-dropdown" id="inv-search-dd"></div>
        </div>
        <button class="wl-add-btn" onclick="invStartFromInput()">🔍 Investigate</button>
        <span class="inv-slots" id="inv-slots">0 / 3 slots used</span>
      </div>
    </div>

    <!-- Investigation cards grid -->
    <div class="inv-grid" id="inv-grid">
      <div class="empty" style="grid-column:1/-1;padding:48px 0">
        No active investigations — search for a symbol above and Claude will deep-dive the technicals, news, and price action
      </div>
    </div>

  </div><!-- /tab-investigations -->

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

// Attach X-Argus-Token header to all mutating requests when auth is enabled
function apiFetch(url, opts = {}) {
  const tok = window._ARGUS_TOKEN || '';
  const headers = Object.assign({'Content-Type': 'application/json'}, opts.headers || {});
  if (tok) headers['X-Argus-Token'] = tok;
  return fetch(url, Object.assign({}, opts, {headers}));
}

function switchTab(name) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.textContent.toLowerCase() === name));
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.toggle('active', p.id === 'tab-' + name));
  if (name === 'charts') {
    ctInitSortable();
    // Initialize any charts that were deferred (created while tab was hidden)
    requestAnimationFrame(ctInitChartsForVisible);
  }
}

async function promotePosition(symbol, fromAccount) {
  if (!confirm(`Sell ${symbol} from ${fromAccount} and re-buy on Agentic?`)) return;
  const r = await apiFetch('/api/promote/' + symbol, {
    method: 'POST',
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

// ── Market open/close countdown ───────────────────────────────────────────────
function _marketCountdown() {
  const labelEl = document.getElementById('mc-label');
  const valEl   = document.getElementById('mc-val');
  if (!labelEl || !valEl) return;

  // All times in ET (America/New_York)
  const now = new Date();
  const etStr = now.toLocaleString('en-US', { timeZone: 'America/New_York', hour12: false,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit' });
  // Parse "MM/DD/YYYY, HH:MM:SS"
  const [datePart, timePart] = etStr.split(', ');
  const [m, d, y] = datePart.split('/').map(Number);
  const [h, min, s] = timePart.split(':').map(Number);
  const dow = new Date(`${y}-${String(m).padStart(2,'0')}-${String(d).padStart(2,'0')}T${timePart}`).getDay();
  const secOfDay = h * 3600 + min * 60 + s;

  const OPEN  = 9  * 3600 + 30 * 60;  // 9:30 AM ET
  const CLOSE = 16 * 3600;             // 4:00 PM ET
  const isWeekday = dow >= 1 && dow <= 5;

  function fmt(secs) {
    secs = Math.max(0, secs);
    const hh = Math.floor(secs / 3600);
    const mm = Math.floor((secs % 3600) / 60);
    const ss = secs % 60;
    if (hh > 0) return `${hh}h ${String(mm).padStart(2,'0')}m`;
    return `${String(mm).padStart(2,'0')}m ${String(ss).padStart(2,'0')}s`;
  }

  if (!isWeekday) {
    // Weekend — find seconds until Monday 9:30 AM
    const daysUntilMon = dow === 0 ? 1 : 2;
    const secsUntilMon = daysUntilMon * 86400 + OPEN - secOfDay;
    labelEl.textContent = 'Opens in';
    valEl.textContent   = fmt(secsUntilMon);
  } else if (secOfDay < OPEN) {
    labelEl.textContent = 'Opens in';
    valEl.textContent   = fmt(OPEN - secOfDay);
  } else if (secOfDay < CLOSE) {
    labelEl.textContent = 'Closes in';
    valEl.textContent   = fmt(CLOSE - secOfDay);
  } else {
    // After close — next open is tomorrow (or Monday if Friday)
    const daysAhead = dow === 5 ? 3 : 1;
    const secsUntilNext = daysAhead * 86400 + OPEN - secOfDay;
    labelEl.textContent = 'Opens in';
    valEl.textContent   = fmt(secsUntilNext);
  }
}
_marketCountdown();
setInterval(_marketCountdown, 1000);

// ── Interval selector ─────────────────────────────────────────────────────────
async function setScanInterval(val) {
  const body = val === 'auto' ? { seconds: null } : { seconds: parseInt(val) };
  try {
    await apiFetch('/api/scan-interval', {
      method: 'POST',
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
  return '$' + Number(n).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
}
function fmtPnl(val, pct) {
  const sign = val >= 0 ? '+' : '';
  return `${sign}$${Math.abs(val).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})} (${sign}${Number(pct).toFixed(2)}%)`;
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
      : `<div class="goal-remaining">$${remaining.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})} remaining</div>`
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
        <span class="muted">${fmtDollar(p.current_price)}</span>
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

  // Cache signals + flashcards globally for chart markers and charts tab
  window._latestSignals = state.signals || [];
  window._flashcards = state.flashcards || [];
  updateTicker(state.signals);
  if (state.watchlist) { ctApplyWatchlist(state.watchlist); buildChartTabs(state.watchlist); }
  if (state.investigations) renderInvestigations(state.investigations);
  if (state.equity_history) eqRender(state.equity_history);
  // Scan timing
  if (state.next_scan_at) _nextScanAt = new Date(state.next_scan_at);
  _updateSessionBadge(state.market_session || 'closed');
  _syncIntervalSelect(state.interval_override);
  _updateCountdown();

  renderTokenUsage(state.token_usage);
  renderPerformance(state.performance);
  renderFlashcards(state);
  renderLogs(state.logs || []);
  // Refresh markers if chart is showing (new closed trades may have arrived)
  if (_chartSymbol && _lastCandles[_chartSymbol]) {
    placeTradeMarkers(_chartSymbol, _lastCandles[_chartSymbol]);
  }

  document.getElementById('last-update').textContent = 'Last update: ' + new Date().toLocaleTimeString();
  document.getElementById('btn-pause').textContent = (state.paused || paused) ? '▶ Resume' : '⏸ Pause';
}

async function decideApproval(tradeId, decision) {
  await apiFetch(`/api/${decision}/${encodeURIComponent(tradeId)}`, {method:'POST'});
}

function _timeAgo(date) {
  const secs = Math.floor((Date.now() - date.getTime()) / 1000);
  if (secs < 60) return 'just now';
  if (secs < 3600) return Math.floor(secs / 60) + 'm ago';
  if (secs < 86400) return Math.floor(secs / 3600) + 'h ago';
  if (secs < 172800) return 'yesterday';
  return Math.floor(secs / 86400) + ' days ago';
}

function renderFlashcards(state) {
  const cards = state.flashcards || [];
  const summary = state.flashcard_summary || {};

  // Summary bar — plain English labels
  const fcSum = document.getElementById('fc-summary');
  if (summary.total > 0) {
    const wr = summary.win_rate != null ? (summary.win_rate * 100).toFixed(0) + '%' : '—';
    const avgPnl = summary.avg_pnl_pct != null ? (summary.avg_pnl_pct >= 0 ? '+' : '') + summary.avg_pnl_pct.toFixed(2) + '%' : '—';
    const best  = summary.best_pnl_pct  != null ? '+' + summary.best_pnl_pct.toFixed(2)  + '%' : '—';
    const worst = summary.worst_pnl_pct != null ? summary.worst_pnl_pct.toFixed(2) + '%' : '—';
    const wrClass = summary.win_rate >= 0.5 ? 'green' : summary.win_rate != null ? 'red' : '';
    fcSum.innerHTML = `
      <div class="fc-stat"><span class="label">Total Trades</span><span class="fc-stat-val">${summary.total}</span></div>
      <div class="fc-stat"><span class="label">Completed</span><span class="fc-stat-val">${summary.closed}</span></div>
      <div class="fc-stat"><span class="label">Win Rate</span><span class="fc-stat-val ${wrClass}">${wr}</span></div>
      <div class="fc-stat"><span class="label">Avg gain/loss %</span><span class="fc-stat-val ${(summary.avg_pnl_pct||0) >= 0 ? 'green' : 'red'}">${avgPnl}</span></div>
      <div class="fc-stat"><span class="label">Best trade</span><span class="fc-stat-val green">${best}</span></div>
      <div class="fc-stat"><span class="label">Worst trade</span><span class="fc-stat-val red">${worst}</span></div>`;
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

  const _BB_LABELS = {
    above_upper: 'stretched high',
    below_lower: 'stretched low',
    inside:      'normal range',
    unknown:     '—',
  };
  const _RISK_LABELS = {
    low:    'Low-risk trade',
    medium: 'Medium-risk trade',
    high:   'High-risk trade',
  };

  grid.innerHTML = cards.map(c => {
    const closed = c.pnl_pct != null;
    const won    = closed && c.pnl_pct > 0;
    const borderCls = closed ? (won ? 'fc-win' : 'fc-loss') : 'fc-open';

    // Outcome in human terms: dollar gain/loss + hold time
    let outcomeHtml;
    if (closed) {
      const dollarGain = (c.dollar_amount || 0) * ((c.pnl_pct || 0) / 100);
      const gainSign   = dollarGain >= 0 ? '+' : '';
      const pnlSign    = (c.pnl_pct || 0) >= 0 ? '+' : '';
      let holdStr = '';
      if (c.hold_duration_hours) {
        holdStr = c.hold_duration_hours >= 24
          ? Math.floor(c.hold_duration_hours / 24) + 'd ' + Math.round(c.hold_duration_hours % 24) + 'h'
          : c.hold_duration_hours.toFixed(1) + 'h';
      }
      outcomeHtml = `
        <span class="pill ${won ? 'pill-win' : 'pill-loss'}">${won ? '▲ Made' : '▼ Lost'} ${gainSign}$${Math.abs(dollarGain).toFixed(2)} (${pnlSign}${(c.pnl_pct||0).toFixed(2)}%)</span>
        ${holdStr ? `<span class="muted" style="font-size:11px">${escHtml(holdStr)} held</span>` : ''}`;
    } else {
      const sinceStr = c.timestamp ? _timeAgo(new Date(c.timestamp)) : '';
      outcomeHtml = `<span class="pill pill-open">In progress</span>${sinceStr ? `<span class="muted" style="font-size:11px">opened ${escHtml(sinceStr)}</span>` : ''}`;
    }

    // Plain-English indicator values
    const rsiVal  = c.rsi != null ? c.rsi.toFixed(1) : '—';
    const rsiCls  = c.rsi < 30 ? 'green' : c.rsi > 70 ? 'red' : '';
    const rsiNote = c.rsi < 30 ? ' — oversold' : c.rsi > 70 ? ' — overbought' : '';

    const macdRaw = c.macd_hist;
    const macdCls = macdRaw > 0 ? 'green' : macdRaw < 0 ? 'red' : '';
    const macdNote = macdRaw > 0 ? ' (building up)' : macdRaw < 0 ? ' (fading)' : '';

    const bbLabel  = _BB_LABELS[c.bb_position] || (c.bb_position || 'normal range').replace(/_/g, ' ');
    const smaLabel = c.price_vs_sma20 === 'above' ? 'above — bullish sign' : c.price_vs_sma20 === 'below' ? 'below — bearish sign' : '—';
    const emaLabel = c.price_vs_ema50 === 'above' ? 'above — bullish sign' : c.price_vs_ema50 === 'below' ? 'below — bearish sign' : '—';

    const condLabel = (c.signal_composite||'neutral').charAt(0).toUpperCase() + (c.signal_composite||'neutral').slice(1);
    const signalConf = c.signal_confidence != null ? (c.signal_confidence * 100).toFixed(0) + '%' : '—';
    const aiConf     = c.decision_confidence != null ? (c.decision_confidence * 100).toFixed(0) + '%' : '—';
    const riskLabel  = _RISK_LABELS[c.risk_level] || (c.risk_level || 'medium');
    const riskCls    = `pill-risk-${c.risk_level || 'medium'}`;

    // AI reasoning: show first sentence as a visible preview on the card face
    const reasoning = c.reasoning || '';
    const firstDot  = reasoning.search(/[.!?]/);
    const preview   = firstDot > 5
      ? reasoning.slice(0, firstDot + 1).trim()
      : reasoning.slice(0, 120).trim() + (reasoning.length > 120 ? '…' : '');

    // Timestamps
    const ts      = c.timestamp ? new Date(c.timestamp).toLocaleString() : '';
    const timeAgo = c.timestamp ? _timeAgo(new Date(c.timestamp)) : '';
    const actionLabel = (c.action || 'BUY').toUpperCase();

    return `<div class="fc ${borderCls}" data-trade-id="${c.trade_id||''}" onclick="this.classList.toggle('expanded')">
      <div class="fc-front">
        <div class="fc-top">
          <div style="display:flex;align-items:center;gap:8px">
            <span class="fc-symbol">${escHtml(c.symbol)}</span>
            <span class="pill pill-${escHtml(actionLabel.toLowerCase())}">${escHtml(actionLabel)}</span>
          </div>
          <span class="muted" style="font-size:11px">${escHtml(timeAgo)}</span>
        </div>
        <div class="fc-confidence-row">
          <span class="pill ${riskCls}">${escHtml(riskLabel)}</span>
          <span class="fc-conf-label">AI was <strong>${escHtml(aiConf)}</strong> confident</span>
        </div>
        ${preview ? `<div class="fc-reasoning-preview">"${escHtml(preview)}"</div>` : ''}
        <div class="fc-outcome">
          ${outcomeHtml}
          <span class="fc-expand-hint">See AI reasoning ↓</span>
        </div>
      </div>
      <div class="fc-back">
        <div class="fc-back-section">
          <div class="fc-back-title">Why the AI decided this</div>
          <div class="fc-reasoning">${escHtml(reasoning || 'No reasoning recorded.')}</div>
        </div>
        <div class="fc-back-section">
          <div class="fc-back-title">Market conditions at the time</div>
          <div class="fc-indicators">
            <span class="muted">Overall signal</span>
            <span class="fc-ind-val">${escHtml(condLabel)} — signals were ${escHtml(signalConf)} sure</span>
            <span class="muted">Momentum (RSI)</span>
            <span class="fc-ind-val ${rsiCls}">${escHtml(rsiVal)}${escHtml(rsiNote)}</span>
            <span class="muted">Trend strength</span>
            <span class="fc-ind-val ${macdCls}">${escHtml(String(macdRaw != null ? (macdRaw >= 0 ? '+' : '') + macdRaw.toFixed(4) : '—'))}${escHtml(macdNote)}</span>
            <span class="muted">Price range</span>
            <span class="fc-ind-val">${escHtml(bbLabel)}</span>
            <span class="muted">vs 20-day average</span>
            <span class="fc-ind-val">${escHtml(smaLabel)}</span>
            <span class="muted">vs 50-day trend line</span>
            <span class="fc-ind-val">${escHtml(emaLabel)}</span>
          </div>
        </div>
        <div class="fc-meta">
          Entry $${(c.entry_price||0).toFixed(2)} · $${(c.dollar_amount||0).toFixed(2)} invested · ${escHtml(c.account||'')} · ${escHtml(ts)}
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
  await apiFetch(endpoint, {method:'POST'});
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
    await apiFetch(`/api/close/${encodeURIComponent(symbol)}`, {method:'POST'});
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

function buildChartTabs(watchlist) {
  const syms = watchlist || [];
  if (!syms.length) return;
  const tabs = document.getElementById('chart-tabs');
  const existing = new Map([...tabs.querySelectorAll('.chart-tab')].map(t => [t.dataset.sym, t]));

  // Remove tabs for symbols no longer in watchlist
  for (const [sym, el] of existing) {
    if (!syms.includes(sym)) el.remove();
  }

  // Add tabs for new symbols (preserve order)
  syms.forEach(sym => {
    if (!existing.has(sym)) {
      const btn = document.createElement('button');
      btn.className = 'chart-tab' + (sym === _chartSymbol ? ' active' : '');
      btn.dataset.sym = sym;
      btn.onclick = () => loadChart(sym);
      btn.innerHTML = `${escHtml(sym)}<button class="chart-tab-x" onclick="event.stopPropagation();ctRemoveSymbol('${escHtml(sym)}')" title="Remove from watchlist">✕</button>`;
      tabs.appendChild(btn);
    }
  });

  if (!_chartSymbol && syms.length) loadChart(syms[0]);
}

// ── Dashboard price chart inline search ──────────────────────────────────────
let _csTimer = null, _csResults = [], _csSel = -1;

function chartSearchOpen() {
  const wrap = document.getElementById('chart-search-wrap');
  wrap.classList.add('open');
  const inp = document.getElementById('chart-search-input');
  inp.value = ''; inp.focus();
  _csResults = []; _csSel = -1;
}

function chartSearchClose() {
  document.getElementById('chart-search-wrap').classList.remove('open');
  document.getElementById('chart-search-dd').innerHTML = '';
}

function chartSearchDebounce(val) {
  clearTimeout(_csTimer); _csResults = []; _csSel = -1;
  if (!val.trim()) { document.getElementById('chart-search-dd').innerHTML = ''; return; }
  const dd = document.getElementById('chart-search-dd');
  dd.innerHTML = '<div class="wl-dd-searching">Searching…</div>';
  _csTimer = setTimeout(async () => {
    try {
      const r = await apiFetch('/api/search?q=' + encodeURIComponent(val.trim()));
      const d = await r.json();
      _csResults = d.results || [];
      dd.innerHTML = _csResults.length
        ? _csResults.map((item, i) =>
            `<div class="wl-dd-item${i===_csSel?' wl-dd-sel':''}" onmousedown="chartSearchPick('${escHtml(item.symbol)}')">
              <span class="wl-dd-sym">${escHtml(item.symbol)}</span>
              <span class="wl-dd-name">${escHtml(item.name||'')}</span>
            </div>`).join('')
        : '<div class="wl-dd-empty">No results</div>';
    } catch(_) {}
  }, 280);
}

function chartSearchKeydown(e) {
  if (e.key === 'Escape') { chartSearchClose(); return; }
  if (e.key === 'Enter') {
    const val = document.getElementById('chart-search-input').value.trim().toUpperCase();
    if (_csSel >= 0 && _csResults[_csSel]) chartSearchPick(_csResults[_csSel].symbol);
    else if (val) chartSearchPick(val);
    e.preventDefault(); return;
  }
  if (!_csResults.length) return;
  if (e.key === 'ArrowDown') { _csSel = Math.min(_csSel+1, _csResults.length-1); chartSearchDebounce(document.getElementById('chart-search-input').value); e.preventDefault(); }
  else if (e.key === 'ArrowUp') { _csSel = Math.max(_csSel-1, 0); chartSearchDebounce(document.getElementById('chart-search-input').value); e.preventDefault(); }
}

async function chartSearchPick(symbol) {
  chartSearchClose();
  try {
    const r = await apiFetch('/api/watchlist', { method: 'POST', body: JSON.stringify({ symbol }) });
    const d = await r.json();
    if (d.watchlist) { ctApplyWatchlist(d.watchlist); buildChartTabs(d.watchlist); loadChart(symbol); }
  } catch(e) { console.error(e); }
}

// ── Ticker bar ────────────────────────────────────────────────────────────────
let _tickerHeadlines = [];

async function fetchNewsHeadlines() {
  try {
    const r = await fetch('/api/news');
    const d = await r.json();
    _tickerHeadlines = d.headlines || [];
    updateTicker(window._latestSignals || []);
  } catch(_) {}
}

let _tickerSpeedMult = 1;
function tickerSpeed(btn, mult) {
  _tickerSpeedMult = mult;
  document.querySelectorAll('.ticker-speed-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const track = document.getElementById('ticker-track');
  if (track && track.scrollWidth) {
    const singleW = track.scrollWidth / 6;
    const dur = Math.max(5, Math.round(singleW / (120 * _tickerSpeedMult)));
    track.style.animationDuration = dur + 's';
  }
}

function updateTicker(signals) {
  const track = document.getElementById('ticker-track');
  if (!track || !signals || !signals.length) return;

  // Price items
  const priceHtml = signals.map(s => {
    const price = s.price != null ? '$' + s.price.toFixed(s.price < 10 ? 4 : 2) : '—';
    const bull  = s.composite === 'bullish', bear = s.composite === 'bearish';
    const arrow = bull ? '▲' : bear ? '▼' : '—';
    const cls   = bull ? 'ticker-up' : bear ? 'ticker-down' : 'ticker-flat';
    return `<span class="ticker-item">` +
      `<span class="ticker-sym">${escHtml(s.symbol)}</span> ` +
      `<span class="ticker-price">${price}</span> ` +
      `<span class="${cls}">${arrow}</span>` +
      `</span><span class="ticker-dot">·</span>`;
  }).join('');

  // News items
  const newsHtml = _tickerHeadlines.slice(0, 12).map(h => {
    const inner = h.url
      ? `<a class="ticker-headline" href="${escHtml(h.url)}" target="_blank" rel="noopener">${escHtml(h.headline)}</a>`
      : `<span class="ticker-headline">${escHtml(h.headline)}</span>`;
    return `<span class="ticker-news-item"><span class="ticker-news-badge">NEWS</span>${inner}</span><span class="ticker-dot">·</span>`;
  }).join('');

  const divider = newsHtml ? `<span class="ticker-divider">◆◆◆</span>` : '';
  const single = priceHtml + divider + newsHtml;

  // Use 6 copies so the track is always wider than the viewport — CSS animates
  // exactly 1/6 of the total (translateX(-16.667%)) for a seamless infinite loop
  track.innerHTML = single.repeat(6);

  // Duration: target ~120px/s × speed multiplier — measure after layout
  requestAnimationFrame(() => {
    const singleW = track.scrollWidth / 6;
    const dur = Math.max(5, Math.round(singleW / (120 * _tickerSpeedMult)));
    track.style.animationDuration = dur + 's';
  });
}

// ── Equity curve ─────────────────────────────────────────────────────────────

let _eqChart = null, _eqSeries = null, _eqHistory = [], _eqRange = 'session';

function eqInit() {
  if (_eqChart || !window.LightweightCharts) return;
  const el = document.getElementById('eq-chart');
  if (!el) return;
  _eqChart = LightweightCharts.createChart(el, {
    width: el.clientWidth,
    height: 160,
    layout: { background: { color: 'transparent' }, textColor: '#8b949e' },
    grid: { vertLines: { color: 'rgba(255,255,255,.05)' }, horzLines: { color: 'rgba(255,255,255,.05)' } },
    rightPriceScale: { borderColor: 'rgba(255,255,255,.08)' },
    timeScale: { borderColor: 'rgba(255,255,255,.08)', timeVisible: true, secondsVisible: false },
    crosshair: { mode: LightweightCharts.CrosshairMode.Magnet },
    handleScroll: false,
    handleScale: false,
  });
  _eqSeries = _eqChart.addAreaSeries({
    lineColor: '#00D4AA',
    topColor: 'rgba(0,212,170,.18)',
    bottomColor: 'rgba(0,212,170,.01)',
    lineWidth: 2,
    priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
  });
  new ResizeObserver(() => { if (_eqChart) _eqChart.resize(el.clientWidth, 160); }).observe(el);
}

function eqSetRange(r) {
  _eqRange = r;
  document.querySelectorAll('.eq-range-btn').forEach(b => b.classList.toggle('active', b.textContent.toLowerCase().replace(' ','') === r));
  eqRender(_eqHistory);
}

function eqRender(history) {
  _eqHistory = history || [];
  eqInit();
  if (!_eqSeries || !_eqHistory.length) return;

  const now = Math.floor(Date.now() / 1000);
  const cutoff = _eqRange === '30m' ? now - 1800
               : _eqRange === '1h'  ? now - 3600
               : 0;  // 'session' = all

  let pts = _eqHistory.filter(p => p.time >= cutoff);
  if (!pts.length) pts = _eqHistory.slice(-1);  // always show at least one point

  _eqSeries.setData(pts);
  _eqChart.timeScale().fitContent();

  // Stats bar
  const first = pts[0].value, last = pts[pts.length - 1].value;
  const chg = last - first, chgPct = first ? (chg / first) * 100 : 0;
  const high = Math.max(...pts.map(p => p.value));
  const low  = Math.min(...pts.map(p => p.value));
  const color = chg >= 0 ? 'var(--green)' : 'var(--danger)';
  const sign  = chg >= 0 ? '+' : '';
  document.getElementById('eq-stats').innerHTML =
    `<span>P&L: <strong style="color:${color}">${sign}$${Math.abs(chg).toFixed(2)} (${sign}${chgPct.toFixed(2)}%)</strong></span>` +
    `<span>High: <strong>$${high.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</strong></span>` +
    `<span>Low: <strong>$${low.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</strong></span>` +
    `<span>${pts.length} data points</span>`;
}

// ── Charts tab (draggable dashlets) ──────────────────────────────────────────
const _POPULAR = ['MSFT','AMZN','GOOGL','META','NFLX','AMD','INTC','JPM','BAC',
                  'COIN','SOL','DOGE','XRP','SPY','QQQ','ROKU','UBER','PLTR','RIVN','SOFI'];

let _ctWatchlist = [];   // current ordered watchlist
let _ctCharts = {};      // symbol → {chart, candle, line, pred, type} | null (pending)
let _ctSortable = null;
let _ctTimeframe = '1M'; // global timeframe: '5D','1M','3M','All'
let _ctCrosshairLocked = false; // prevent crosshair sync re-entrancy

const _CT_TF_DAYS = { '5D': 5, '1M': 30, '3M': 90, 'All': Infinity };

function ctInitSortable() {
  if (_ctSortable) return;
  const grid = document.getElementById('ct-grid');
  _ctSortable = Sortable.create(grid, {
    animation: 150,
    ghostClass: 'sortable-ghost',
    dragClass: 'sortable-drag',
    handle: '.ct-dashlet-header',
    onEnd: () => {
      _ctWatchlist = [...grid.querySelectorAll('.ct-dashlet')].map(el => el.dataset.sym);
    },
  });
}

function ctApplyWatchlist(symbols) {
  const prev = new Set(_ctWatchlist);
  const next = new Set(symbols);

  // Add new dashlets
  for (const sym of symbols) {
    if (!prev.has(sym)) ctAddDashlet(sym);
  }
  // Remove gone dashlets
  for (const sym of [...prev]) {
    if (!next.has(sym)) ctRemoveDashlet(sym);
  }

  _ctWatchlist = symbols;
  ctRefreshSignals();
  ctRefreshSuggestions(symbols);
}

function ctAddDashlet(sym) {
  if (document.getElementById('ct-card-' + sym)) return;
  const grid = document.getElementById('ct-grid');
  const empty = grid.querySelector('.empty');
  if (empty) empty.remove();

  const card = document.createElement('div');
  card.className = 'ct-dashlet';
  card.id = 'ct-card-' + sym;
  card.dataset.sym = sym;
  const tfs = ['5D','1M','3M','All'];
  const tfBtns = tfs.map(tf =>
    `<button class="ct-tf-btn${tf===_ctTimeframe?' active':''}" onclick="ctSetTimeframe('${tf}')">${tf}</button>`
  ).join('');

  card.innerHTML = `
    <div class="ct-dashlet-header">
      <span class="ct-dashlet-sym">${escHtml(sym)}</span>
      <span class="ct-dashlet-price" id="ct-price-${escHtml(sym)}">—</span>
      <span class="ct-dashlet-sig" id="ct-sig-${escHtml(sym)}"></span>
      <div class="ct-tf-btns">${tfBtns}</div>
      <button class="ct-dashlet-remove" onclick="event.stopPropagation();ctRemoveSymbol('${escHtml(sym)}')" title="Remove from watchlist">✕</button>
    </div>
    <div style="position:relative">
      <div class="ct-chart-area" id="ct-chart-${escHtml(sym)}"></div>
      <div class="ct-crosshair-tooltip" id="ct-tip-${escHtml(sym)}"></div>
    </div>
    <div class="ct-dashlet-footer">
      <span class="ct-stat">RSI <span id="ct-rsi-${escHtml(sym)}">—</span></span>
      <span class="ct-stat">MACD <span id="ct-macd-${escHtml(sym)}">—</span></span>
      <div class="ct-type-btns">
        <button class="ct-type-btn active" id="ct-candles-${escHtml(sym)}" onclick="ctSetType('${escHtml(sym)}','candles')">Candles</button>
        <button class="ct-type-btn" id="ct-line-${escHtml(sym)}" onclick="ctSetType('${escHtml(sym)}','line')">Line</button>
      </div>
    </div>`;
  grid.appendChild(card);
  _ctCharts[sym] = null;  // pending init

  // If Charts tab already active, init right away
  if (document.getElementById('tab-charts')?.classList.contains('active')) {
    requestAnimationFrame(() => ctInitChart(sym));
  }
}

function ctInitChart(sym) {
  const el = document.getElementById('ct-chart-' + sym);
  if (!el || _ctCharts[sym]) return;  // already initialized or element gone
  const chart = LightweightCharts.createChart(el, {
    ..._CHART_OPTS,
    layout: { ..._CHART_OPTS.layout },
    rightPriceScale: { borderColor: '#2a2f3e', visible: true },
    timeScale: { borderColor: '#2a2f3e', timeVisible: false },
    handleScroll: false, handleScale: true,
  });
  const candle = chart.addCandlestickSeries({
    upColor: '#00D4AA', downColor: '#f85149',
    borderUpColor: '#00D4AA', borderDownColor: '#f85149',
    wickUpColor: '#00D4AA', wickDownColor: '#f85149',
  });
  const line = chart.addAreaSeries({
    lineColor: '#00D4AA', topColor: 'rgba(0,212,170,0.18)',
    bottomColor: 'rgba(0,212,170,0.01)', lineWidth: 2, visible: false,
  });
  const pred = chart.addLineSeries({
    color: '#60a5fa', lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle.Dashed,
    lastValueVisible: false, priceLineVisible: false,
  });
  new ResizeObserver(() => {
    if (el.clientWidth > 0) chart.applyOptions({ width: el.clientWidth });
  }).observe(el);
  _ctCharts[sym] = { chart, candle, line, pred, type: 'candles' };

  // Synchronized crosshair — when this chart moves, update all others
  chart.subscribeCrosshairMove(param => {
    if (_ctCrosshairLocked) return;
    _ctCrosshairLocked = true;
    const tipEl = document.getElementById('ct-tip-' + sym);
    if (!param.time) {
      if (tipEl) tipEl.style.display = 'none';
      Object.values(_ctCharts).forEach(inst => {
        if (inst && inst !== _ctCharts[sym]) inst.chart.clearCrosshairPosition();
      });
      _ctCrosshairLocked = false;
      return;
    }
    // Show tooltip on this chart
    if (tipEl && param.seriesData) {
      const d = param.seriesData.get(candle) || param.seriesData.get(line);
      if (d) {
        const o = d.open ?? d.value, c2 = d.close ?? d.value;
        const chg = o ? ((c2 - o) / o * 100).toFixed(2) : null;
        const col = (d.close ?? d.value) >= (d.open ?? d.value) ? '#00D4AA' : '#f85149';
        tipEl.innerHTML = d.close != null
          ? `O:${d.open?.toFixed(2)} H:${d.high?.toFixed(2)} L:${d.low?.toFixed(2)} <strong style="color:${col}">C:${d.close?.toFixed(2)}</strong>${chg ? ` <span style="color:${col}">${chg>=0?'+':''}${chg}%</span>` : ''}`
          : `<strong style="color:${col}">${(d.value||0).toFixed(2)}</strong>`;
        if (param.point) {
          tipEl.style.display = 'block';
          tipEl.style.left = Math.min(param.point.x, el.clientWidth - tipEl.offsetWidth - 8) + 'px';
        }
      }
    }
    // Push crosshair to all other initialized charts at same time
    Object.entries(_ctCharts).forEach(([s, inst]) => {
      if (s === sym || !inst) return;
      const tgt = inst.type === 'candles' ? inst.candle : inst.line;
      try { inst.chart.setCrosshairPosition(NaN, param.time, tgt); } catch(_){}
    });
    _ctCrosshairLocked = false;
  });

  ctLoadChart(sym);
}

function ctInitChartsForVisible() {
  // Initialize any pending (null) charts now that the tab is visible
  Object.keys(_ctCharts).forEach(sym => {
    if (_ctCharts[sym] === null) ctInitChart(sym);
  });
}

function ctRemoveDashlet(sym) {
  const el = document.getElementById('ct-card-' + sym);
  if (el) el.remove();
  if (_ctCharts[sym]) { try { _ctCharts[sym].chart.remove(); } catch(_){} delete _ctCharts[sym]; }
  const grid = document.getElementById('ct-grid');
  if (!grid.querySelector('.ct-dashlet')) {
    grid.innerHTML = '<div class="empty" style="grid-column:1/-1;padding:40px 0">No symbols in watchlist — add one above</div>';
  }
}

function ctSetType(sym, type) {
  const inst = _ctCharts[sym];
  if (!inst) return;
  inst.type = type;
  inst.candle.applyOptions({ visible: type === 'candles' });
  inst.line.applyOptions({ visible: type === 'line' });
  document.getElementById('ct-candles-' + sym)?.classList.toggle('active', type === 'candles');
  document.getElementById('ct-line-' + sym)?.classList.toggle('active', type === 'line');
}

async function ctLoadChart(sym) {
  const inst = _ctCharts[sym];
  if (!inst) return;
  try {
    const res = await fetch(`/api/chart/${encodeURIComponent(sym)}`);
    const data = await res.json();
    const candles = (data.candles || []).sort((a, b) => a.time - b.time);
    _lastCandles[sym] = candles;
    ctApplyTimeframeToSym(sym);  // applies current _ctTimeframe + markers

  } catch(e) { console.error('ct chart error', sym, e); }
}

function ctApplyTimeframeToSym(sym) {
  const inst = _ctCharts[sym];
  const all = _lastCandles[sym];
  if (!inst || !all || !all.length) return;

  const days = _CT_TF_DAYS[_ctTimeframe] ?? Infinity;
  const cutoff = isFinite(days) ? (Date.now()/1000 - days * 86400) : 0;
  const candles = all.filter(c => c.time >= cutoff);
  if (!candles.length) return;

  inst.candle.setData(candles);
  inst.line.setData(candles.map(c => ({ time: c.time, value: c.close })));
  if (candles.length >= 5) inst.pred.setData(buildPrediction(candles));
  else inst.pred.setData([]);

  // Trade markers
  const cards = (window._flashcards || []).filter(c => c.symbol === sym);
  const markers = [];
  for (const c of cards) {
    const ts = c.timestamp ? Math.floor(new Date(c.timestamp).getTime() / 1000) : null;
    if (!ts || ts < cutoff) continue;
    const nearest = candles.reduce((a,b) => Math.abs(b.time-ts)<Math.abs(a.time-ts)?b:a);
    if (c.action === 'BUY') markers.push({ time: nearest.time, position: 'belowBar', color: '#3fb950', shape: 'arrowUp', text: 'B', size: 1 });
    if (c.exit_price != null) {
      const exitTs = ts + Math.round((c.hold_duration_hours || 1) * 3600);
      const ne = candles.reduce((a,b) => Math.abs(b.time-exitTs)<Math.abs(a.time-exitTs)?b:a);
      markers.push({ time: ne.time, position: 'aboveBar', color: c.pnl_pct>0?'#00D4AA':'#f85149', shape: 'arrowDown', text: c.pnl_pct!=null?(c.pnl_pct>=0?'+':'')+c.pnl_pct.toFixed(1)+'%':'S', size: 1 });
    }
  }
  markers.sort((a,b) => a.time - b.time);
  try { inst.candle.setMarkers(markers); } catch(_){}
  inst.chart.timeScale().fitContent();
}

function ctSetTimeframe(tf) {
  _ctTimeframe = tf;
  // Update active state on every dashlet's TF buttons
  document.querySelectorAll('.ct-tf-btn').forEach(b => b.classList.toggle('active', b.textContent === tf));
  // Re-render all initialized charts with the new timeframe
  Object.keys(_ctCharts).forEach(sym => {
    if (_ctCharts[sym]) ctApplyTimeframeToSym(sym);
  });
}

function ctRefreshSignals() {
  const sigs = window._latestSignals || [];
  for (const sym of _ctWatchlist) {
    const sig = sigs.find(s => s.symbol === sym);
    if (!sig) continue;
    const priceEl = document.getElementById('ct-price-' + sym);
    const sigEl   = document.getElementById('ct-sig-' + sym);
    const rsiEl   = document.getElementById('ct-rsi-' + sym);
    const macdEl  = document.getElementById('ct-macd-' + sym);
    if (priceEl) priceEl.textContent = sig.price != null ? '$' + sig.price.toFixed(2) : '—';
    if (sigEl) {
      const cls = sig.composite === 'bullish' ? 'bullish' : sig.composite === 'bearish' ? 'bearish' : 'neutral';
      sigEl.innerHTML = pill((sig.composite||'neutral').toUpperCase(), cls);
    }
    if (rsiEl) {
      const r = sig.rsi != null ? sig.rsi.toFixed(1) : '—';
      const c = sig.rsi < 30 ? '#00D4AA' : sig.rsi > 70 ? '#f85149' : 'var(--text)';
      rsiEl.innerHTML = `<span style="color:${c}">${r}</span>`;
    }
    if (macdEl) {
      const m = sig.macd_hist != null ? (sig.macd_hist>=0?'+':'')+sig.macd_hist.toFixed(4) : '—';
      const c = sig.macd_hist > 0 ? '#00D4AA' : '#f85149';
      macdEl.innerHTML = `<span style="color:${c}">${m}</span>`;
    }
  }
}

function ctRefreshSuggestions(watchlist) {
  const chips = document.getElementById('suggest-chips');
  if (!chips) return;
  const notWatched = _POPULAR.filter(s => !watchlist.includes(s));
  chips.innerHTML = notWatched.slice(0, 14).map(s =>
    `<button class="suggest-chip" onclick="ctAddSymbol('${escHtml(s)}')">${escHtml(s)}</button>`
  ).join('');
}

// ── Symbol search / autocomplete ──────────────────────────────────────────────
let _ctSearchTimer = null;
let _ctSearchResults = [];
let _ctSearchSel = -1;

function ctSearchDebounce(val) {
  clearTimeout(_ctSearchTimer);
  _ctSearchResults = []; _ctSearchSel = -1;
  if (!val.trim()) { ctDropdownClose(); return; }
  const dd = document.getElementById('wl-dropdown');
  dd.innerHTML = '<div class="wl-dd-searching">Searching…</div>';
  dd.classList.add('open');
  _ctSearchTimer = setTimeout(() => ctSearchFetch(val.trim()), 280);
}

async function ctSearchFetch(q) {
  try {
    const r = await apiFetch('/api/search?q=' + encodeURIComponent(q));
    const d = await r.json();
    _ctSearchResults = d.results || [];
    ctDropdownRender();
  } catch(e) { ctDropdownClose(); }
}

function ctDropdownRender() {
  const dd = document.getElementById('wl-dropdown');
  if (!_ctSearchResults.length) {
    dd.innerHTML = '<div class="wl-dd-empty">No results — try the exact ticker (e.g. AAPL)</div>';
  } else {
    dd.innerHTML = _ctSearchResults.map((item, i) => {
      const sel = i === _ctSearchSel ? ' wl-dd-sel' : '';
      return `<div class="wl-dd-item${sel}" onmousedown="ctPickResult('${escHtml(item.symbol)}')">
        <span class="wl-dd-sym">${escHtml(item.symbol)}</span>
        <span class="wl-dd-name">${escHtml(item.name || '')}</span>
      </div>`;
    }).join('');
  }
  dd.classList.add('open');
}

function ctDropdownClose() {
  const dd = document.getElementById('wl-dropdown');
  if (dd) { dd.classList.remove('open'); dd.innerHTML = ''; }
}

function ctPickResult(symbol) {
  document.getElementById('wl-add-input').value = '';
  ctDropdownClose();
  ctAddSymbol(symbol);
}

function ctSearchKeydown(e) {
  if (e.key === 'Escape') { ctDropdownClose(); return; }
  if (e.key === 'Enter') {
    if (_ctSearchSel >= 0 && _ctSearchResults[_ctSearchSel]) {
      ctPickResult(_ctSearchResults[_ctSearchSel].symbol);
    } else {
      ctAddFromInput();
    }
    e.preventDefault(); return;
  }
  if (!_ctSearchResults.length) return;
  if (e.key === 'ArrowDown') {
    _ctSearchSel = Math.min(_ctSearchSel + 1, _ctSearchResults.length - 1);
    ctDropdownRender(); e.preventDefault();
  } else if (e.key === 'ArrowUp') {
    _ctSearchSel = Math.max(_ctSearchSel - 1, 0);
    ctDropdownRender(); e.preventDefault();
  }
}

function ctAddFromInput() {
  const val = (document.getElementById('wl-add-input').value || '').trim().toUpperCase();
  if (val) ctAddSymbol(val);
}

async function ctAddSymbol(symbol) {
  if (!symbol) return;
  document.getElementById('wl-add-input').value = '';
  ctDropdownClose();
  try {
    const r = await apiFetch('/api/watchlist', { method: 'POST', body: JSON.stringify({ symbol }) });
    const d = await r.json();
    if (d.watchlist) ctApplyWatchlist(d.watchlist);
  } catch(e) { console.error(e); }
}

async function ctRemoveSymbol(symbol) {
  try {
    const r = await apiFetch(`/api/watchlist/${encodeURIComponent(symbol)}`, { method: 'DELETE' });
    const d = await r.json();
    if (d.watchlist) ctApplyWatchlist(d.watchlist);
  } catch(e) { console.error(e); }
}

// ── Investigations tab ────────────────────────────────────────────────────────

function _verdictClass(verdict) {
  if (!verdict) return 'neutral';
  const v = verdict.toLowerCase();
  if (v.includes('strong buy') || v.includes('bullish')) return 'bullish';
  if (v.includes('avoid') || v.includes('bearish')) return 'bearish';
  if (v.includes('caution') || v.includes('wait')) return 'caution';
  if (v.includes('watch') || v.includes('buy')) return 'bullish';
  return 'neutral';
}

function buildInvCard(sym, d) {
  const statusLabel = { queued:'⟳ Queued', running:'⟳ Analyzing…', complete:'✓ Complete', error:'✗ Error' }[d.status] || d.status;
  const ts   = d.completed_at ? _timeAgo(new Date(d.completed_at)) : '';
  const vc   = _verdictClass(d.verdict);
  const conf = Math.round((d.confidence || 0) * 100);

  let body = '';
  if (d.status === 'queued' || d.status === 'running') {
    body = `<div class="inv-thinking">
      <div class="inv-spinner"></div>
      <span>Claude is investigating <strong>${escHtml(sym)}</strong> — reading signals, news sentiment, and price action…</span>
    </div>`;
  } else if (d.status === 'error') {
    body = `<div style="color:var(--danger);font-size:13px;padding:10px 0">${escHtml(d.error || 'Analysis failed — check logs')}</div>`;
  } else {
    const findings = (d.findings || []).map(f => `<li>${escHtml(f)}</li>`).join('');
    const risks    = (d.risks    || []).map(r => `<li>${escHtml(r)}</li>`).join('');

    // Filter news for this symbol
    const symLower = sym.toLowerCase();
    const relevant = _tickerHeadlines
      .filter(h => h.headline.toLowerCase().includes(symLower))
      .slice(0, 5);
    const newsHtml = relevant.length
      ? relevant.map(h => h.url
          ? `<div class="inv-news-item"><a href="${escHtml(h.url)}" target="_blank" rel="noopener" style="color:inherit;text-decoration:none;" onmouseover="this.style.textDecoration='underline'" onmouseout="this.style.textDecoration='none'">${escHtml(h.headline)}</a></div>`
          : `<div class="inv-news-item">${escHtml(h.headline)}</div>`
        ).join('')
      : `<div class="inv-news-item" style="color:var(--text-dim)">No recent headlines found for ${escHtml(sym)}</div>`;

    body = `
      <div class="inv-verdict ${vc}">${escHtml(d.verdict || '—')}</div>
      <div class="inv-meta">
        <span>AI confidence: <strong>${conf}%</strong></span>
        <span>·</span>
        <span>Outlook: <strong>${escHtml(d.timeframe || '—')}</strong></span>
        <div class="inv-conf-bar"><div class="inv-conf-fill" style="width:${conf}%"></div></div>
      </div>
      <div class="inv-summary">${escHtml(d.summary || '')}</div>
      ${findings ? `<div class="inv-section">
        <div class="inv-section-lbl">Key Findings</div>
        <ul class="inv-list">${findings}</ul>
      </div>` : ''}
      ${risks ? `<div class="inv-section">
        <div class="inv-section-lbl">Risk Factors</div>
        <ul class="inv-list inv-risks">${risks}</ul>
      </div>` : ''}
      <div class="inv-news-section">
        <div class="inv-section-lbl">Relevant News</div>
        ${newsHtml}
      </div>`;
  }

  const rerunBtn = (d.status === 'complete' || d.status === 'error')
    ? `<button class="inv-btn" onclick="invRerun('${escHtml(sym)}')">↻ Re-run</button>` : '';

  return `<div class="inv-card ${d.status || ''} ${vc}" id="inv-card-${escHtml(sym)}"
              data-sym="${escHtml(sym)}" data-status="${escHtml(d.status||'')}">
    <div class="inv-header">
      <span class="inv-sym">${escHtml(sym)}</span>
      <span class="inv-status ${d.status||'queued'}">${statusLabel}</span>
      ${ts ? `<span class="inv-age">${ts}</span>` : ''}
      <div class="inv-actions">
        ${rerunBtn}
        <button class="inv-btn danger" onclick="invRemove('${escHtml(sym)}')">✕ Remove</button>
      </div>
    </div>
    ${body}
  </div>`;
}

function renderInvestigations(investigations) {
  const inv  = investigations || {};
  const keys = Object.keys(inv);
  const slotsEl = document.getElementById('inv-slots');
  if (slotsEl) slotsEl.textContent = `${keys.length} / 3 slots used`;

  const grid = document.getElementById('inv-grid');
  if (!keys.length) {
    grid.innerHTML = '<div class="empty" style="grid-column:1/-1;padding:48px 0">No active investigations — search for a symbol above</div>';
    return;
  }

  // Remove stale cards
  [...grid.querySelectorAll('.inv-card')].forEach(el => {
    if (!inv[el.dataset.sym]) el.remove();
  });

  // Add or refresh cards
  for (const sym of keys) {
    const d = inv[sym];
    const existing = document.getElementById('inv-card-' + sym);
    if (!existing) {
      const wrap = document.createElement('div');
      wrap.innerHTML = buildInvCard(sym, d);
      grid.appendChild(wrap.firstElementChild);
    } else if (existing.dataset.status !== (d.status || '')) {
      // Status changed — replace the whole card
      const wrap = document.createElement('div');
      wrap.innerHTML = buildInvCard(sym, d);
      existing.replaceWith(wrap.firstElementChild);
    }
  }

  // If all cards were removed, show empty state
  if (!grid.querySelector('.inv-card')) {
    grid.innerHTML = '<div class="empty" style="grid-column:1/-1;padding:48px 0">No active investigations</div>';
  }
}

// ── Investigation search / autocomplete ───────────────────────────────────────
let _invTimer = null, _invResults = [], _invSel = -1;

function invSearchDebounce(val) {
  clearTimeout(_invTimer); _invResults = []; _invSel = -1;
  const dd = document.getElementById('inv-search-dd');
  if (!val.trim()) { dd.classList.remove('open'); dd.innerHTML = ''; return; }
  dd.innerHTML = '<div class="wl-dd-searching">Searching…</div>'; dd.classList.add('open');
  _invTimer = setTimeout(async () => {
    try {
      const r = await apiFetch('/api/search?q=' + encodeURIComponent(val.trim()));
      const d = await r.json();
      _invResults = d.results || [];
      dd.innerHTML = _invResults.length
        ? _invResults.map((item, i) =>
            `<div class="wl-dd-item${i===_invSel?' wl-dd-sel':''}" onmousedown="invPick('${escHtml(item.symbol)}')">
              <span class="wl-dd-sym">${escHtml(item.symbol)}</span>
              <span class="wl-dd-name">${escHtml(item.name||'')}</span>
            </div>`).join('')
        : '<div class="wl-dd-empty">No results — try the ticker directly (e.g. AAPL)</div>';
    } catch(_) { dd.classList.remove('open'); }
  }, 280);
}

function invDropdownClose() {
  const dd = document.getElementById('inv-search-dd');
  if (dd) { dd.classList.remove('open'); dd.innerHTML = ''; }
}

function invSearchKeydown(e) {
  if (e.key === 'Escape') { invDropdownClose(); return; }
  if (e.key === 'Enter') {
    if (_invSel >= 0 && _invResults[_invSel]) invPick(_invResults[_invSel].symbol);
    else invStartFromInput();
    e.preventDefault(); return;
  }
  if (!_invResults.length) return;
  if (e.key === 'ArrowDown') { _invSel = Math.min(_invSel+1, _invResults.length-1); invSearchDebounce(document.getElementById('inv-search-input').value); e.preventDefault(); }
  else if (e.key === 'ArrowUp') { _invSel = Math.max(_invSel-1, 0); invSearchDebounce(document.getElementById('inv-search-input').value); e.preventDefault(); }
}

function invPick(symbol) {
  document.getElementById('inv-search-input').value = '';
  invDropdownClose();
  invStart(symbol);
}

function invStartFromInput() {
  const val = (document.getElementById('inv-search-input').value || '').trim().toUpperCase();
  if (val) invPick(val);
}

async function invStart(symbol) {
  invDropdownClose();
  try {
    const r = await apiFetch('/api/investigate', { method: 'POST', body: JSON.stringify({ symbol }) });
    if (!r.ok) {
      const err = await r.json();
      alert(err.detail || 'Could not start investigation');
    }
  } catch(e) { console.error(e); }
}

async function invRemove(symbol) {
  try {
    await apiFetch(`/api/investigate/${encodeURIComponent(symbol)}`, { method: 'DELETE' });
  } catch(e) { console.error(e); }
}

async function invRerun(symbol) {
  try {
    await apiFetch(`/api/investigate/${encodeURIComponent(symbol)}`, { method: 'DELETE' });
    await new Promise(r => setTimeout(r, 150));
    await apiFetch('/api/investigate', { method: 'POST', body: JSON.stringify({ symbol }) });
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
fetch('/api/version').then(r=>r.json()).then(d => {
  const el = document.getElementById('version-badge');
  if (el && d.version) el.textContent = 'v' + d.version;
}).catch(()=>{});
fetchAll();
connectSSE();
fetchNewsHeadlines();
setInterval(fetchNewsHeadlines, 5 * 60 * 1000);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    # Inject the token so the JS can attach it to all mutating requests.
    # The token is already known to anyone who can load the page (same-origin),
    # so embedding it here does not widen the attack surface.
    token_script = f"<script>window._ARGUS_TOKEN={json.dumps(_dashboard_token)};</script>"
    return _HTML.replace("</head>", token_script + "\n</head>", 1)


def main(host: str = "127.0.0.1", port: int = 8000, token: str = "") -> None:
    if token:
        _configure_auth(token)
        logger.info("Dashboard API authentication enabled")
    _start_news_poller()
    uvicorn.run(app, host=host, port=port, log_level="warning")
