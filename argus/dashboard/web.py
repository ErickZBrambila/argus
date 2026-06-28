"""FastAPI web dashboard with SSE real-time updates."""

from __future__ import annotations

import asyncio
import datetime
import hmac as _hmac
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

from argus.storage.models import get_session, add_to_db_watchlist, remove_from_db_watchlist

logger = logging.getLogger(__name__)

# Shared state injected by main loop
_state: dict = {}
_state_lock = threading.Lock()
_paused: bool = False

# Thread-safe queue: main loop (sync thread) → SSE broadcaster (async thread)
_sse_queue: stdlib_queue.Queue = stdlib_queue.Queue(maxsize=5)
# Per-subscriber async queues — only accessed within the event loop
_subscribers: set[asyncio.Queue] = set()
# Cap concurrent backtests so "Backtest All" (22 symbols) doesn't flood the thread pool
_backtest_sem = asyncio.Semaphore(4)

# Chart data source — registered by main loop
_chart_source_fn = None   # callable(symbol: str) -> list[dict]
_search_fn = None          # callable(query: str) -> list[{symbol, name}]
_autopilot = None         # Autopilot instance for runtime control

# Equity curve — ring buffer of {time, value} points for the session
import collections as _collections
import pathlib as _pathlib
_equity_history: collections.deque = _collections.deque(maxlen=480)  # ~8h at 60s interval
_equity_history_by_account: dict = {}  # label → deque(maxlen=480)
_EQUITY_PERSIST_PATH = _pathlib.Path(__file__).parent.parent.parent / "equity_history.json"

def _equity_load() -> None:
    """Load today's equity history from disk on startup."""
    global _equity_history, _equity_history_by_account
    try:
        if not _EQUITY_PERSIST_PATH.exists():
            return
        data = json.loads(_EQUITY_PERSIST_PATH.read_text())
        if data.get("date") != datetime.date.today().isoformat():
            return  # stale — start fresh
        pts = data.get("combined", [])
        _equity_history = _collections.deque(pts, maxlen=480)
        for label, acct_pts in data.get("by_account", {}).items():
            _equity_history_by_account[label] = _collections.deque(acct_pts, maxlen=480)
    except Exception as exc:
        logger.debug("Equity history load failed: %s", exc)

def _equity_save() -> None:
    """Persist current equity history to disk (called inside _state_lock)."""
    try:
        _EQUITY_PERSIST_PATH.write_text(json.dumps({
            "date": datetime.date.today().isoformat(),
            "combined": list(_equity_history),
            "by_account": {k: list(v) for k, v in _equity_history_by_account.items()},
        }))
    except Exception as exc:
        logger.debug("Equity history save failed: %s", exc)

_equity_load()  # restore on module import

# ── Alert log ─────────────────────────────────────────────────────────────────
_alert_log: _collections.deque = _collections.deque(maxlen=500)
_ALERT_PERSIST_PATH = _pathlib.Path(__file__).parent.parent.parent / "alert_history.json"


def _alert_load() -> None:
    global _alert_log
    try:
        if not _ALERT_PERSIST_PATH.exists():
            return
        data = json.loads(_ALERT_PERSIST_PATH.read_text())
        _alert_log = _collections.deque(data.get("entries", []), maxlen=500)
    except Exception as exc:
        logger.debug("Alert log load failed: %s", exc)


def _alert_save() -> None:
    try:
        _ALERT_PERSIST_PATH.write_text(json.dumps({
            "date": datetime.date.today().isoformat(),
            "entries": list(_alert_log),
        }))
    except Exception as exc:
        logger.debug("Alert log save failed: %s", exc)


def _push_alert_entry(subject: str, body: str) -> None:
    """Append an alert entry and push to SSE clients."""
    entry = {
        "time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "subject": subject,
        "body": body,
    }
    _alert_log.appendleft(entry)
    _alert_save()
    with _state_lock:
        snapshot = {**_state, "alert_log": list(_alert_log)}
    _sse_push(json.dumps(snapshot, default=str))


_alert_load()


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
_APPROVAL_PERSIST_PATH = _pathlib.Path(__file__).parent.parent.parent / "approval_queue.json"


def _approval_save() -> None:
    try:
        _APPROVAL_PERSIST_PATH.write_text(json.dumps(list(_pending_approvals.values()), default=str))
    except Exception as exc:
        logger.debug("Approval queue save failed: %s", exc)


def _approval_load() -> None:
    try:
        if not _APPROVAL_PERSIST_PATH.exists():
            return
        entries = json.loads(_APPROVAL_PERSIST_PATH.read_text())
        if not isinstance(entries, list):
            return
        for entry in entries:
            tid = entry.get("trade_id")
            if tid:
                _pending_approvals[tid] = entry
        logger.info("Loaded %d persisted approval(s) from disk", len(_pending_approvals))
    except Exception as exc:
        logger.debug("Approval queue load failed: %s", exc)


def get_persisted_approvals() -> dict[str, dict]:
    """Return current pending approvals — used by autopilot to restore acct state on startup."""
    with _approval_lock:
        return dict(_pending_approvals)


def queue_approval(trade_id: str, trade_info: dict) -> None:
    with _approval_lock:
        _pending_approvals[trade_id] = {**trade_info, "queued_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    _approval_save()
    _push_approvals_state()


def get_approval_decision(trade_id: str) -> str | None:
    """Returns 'approved', 'denied', or None if no decision yet."""
    with _approval_lock:
        return _approval_decisions.get(trade_id)


def clear_approval(trade_id: str) -> None:
    with _approval_lock:
        _pending_approvals.pop(trade_id, None)
        _approval_decisions.pop(trade_id, None)
    _approval_save()
    _push_approvals_state()


def _push_approvals_state() -> None:
    with _approval_lock:
        approvals = dict(_pending_approvals)
    with _state_lock:
        _state["pending_approvals"] = approvals
        snapshot = {**_state, "alert_log": list(_alert_log)}
    _sse_push(json.dumps(snapshot, default=str))


_approval_load()


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
    with _state_lock:
        _state["watchlist"] = list(symbols)


def prefill_state(cfg: dict | None = None) -> None:
    """Populate _state with boot-time data so /api/status has real values
    before the first scan cycle completes."""
    updates: dict = {}
    try:
        from argus.dashboard.token_tracker import get_summary as _tok
        updates["token_usage"] = _tok()
    except Exception:
        pass
    try:
        from argus.agent.decision import get_ai_status, get_model_info
        updates["ai_status"] = get_ai_status()
        updates["ai_models"] = get_model_info()
    except Exception:
        pass
    try:
        from argus.engine.session import get_market_session
        updates["market_session"] = get_market_session()
    except Exception:
        updates.setdefault("market_session", "closed")
    with _state_lock:
        _state.update(updates)
        if cfg:
            _state.setdefault("paper_trade", cfg.get("paper_trade", True))
            _state.setdefault("equity_goal", cfg.get("equity_goal", 0))
        _state.setdefault("kill_switch", False)
        _state.setdefault("paused", False)
        _state.setdefault("signals", [])
        _state.setdefault("positions", {})
        _state.setdefault("exit_only_symbols", [])
        _state.setdefault("sell_by_dates", {})


def get_watchlist() -> list[str]:
    with _watchlist_lock:
        return list(_runtime_watchlist)


def add_to_runtime_watchlist(symbol: str) -> None:
    with _watchlist_lock:
        if symbol not in _runtime_watchlist:
            _runtime_watchlist.append(symbol)


# ── Investigations (up to 3 AI deep-dives) ───────────────────────────────────
_MAX_INVESTIGATIONS = 3
_investigations: dict[str, dict] = {}
_investigation_lock = threading.Lock()
_investigate_fn = None   # callable(symbol, signal, headlines) → dict
_notifier = None         # Notifier instance injected by autopilot


def register_investigate(fn) -> None:
    global _investigate_fn
    _investigate_fn = fn


def register_notifier(notifier) -> None:
    global _notifier
    _notifier = notifier
    notifier.set_log_fn(_push_alert_entry)


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

        verdict    = result.get("verdict", "Unknown")
        confidence = float(result.get("confidence") or 0)
        summary    = result.get("summary", "")

        with _investigation_lock:
            if symbol not in _investigations:
                return  # user deleted the card while investigation was running
            _investigations[symbol].update({
                "status": "complete",
                "verdict":    verdict,
                "confidence": confidence,
                "summary":    summary,
                "findings":   result.get("findings") or [],
                "risks":      result.get("risks") or [],
                "timeframe":  result.get("timeframe", ""),
                "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            })

        if _notifier and verdict.upper() != "HOLD":
            try:
                emoji = "🟢" if "bull" in verdict.lower() or verdict.upper() == "BUY" else "🔴"
                _notifier.send(
                    f"{emoji} Investigation: {symbol} — {verdict.upper()} ({confidence:.0%})",
                    summary[:280] if summary else f"{symbol} deep-dive complete.",
                )
            except Exception as exc:
                logger.warning("Investigation alert failed for %s: %s", symbol, exc)

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

# ── Security headers middleware ───────────────────────────────────────────────
from starlette.middleware.base import BaseHTTPMiddleware

class _SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

app.add_middleware(_SecurityHeadersMiddleware)

# ── Auth ──────────────────────────────────────────────────────────────────────
_dashboard_token: str = ""


def _configure_auth(token: str) -> None:
    global _dashboard_token
    _dashboard_token = token


def _require_auth(
    x_argus_token: str = Header(default=""),
    token: str = Query(default=""),
) -> None:
    """Constant-time token check. Accepts X-Argus-Token header or ?token= query param.
    Query param is required for EventSource (browser cannot set custom headers on SSE)."""
    if _dashboard_token:
        provided = x_argus_token or token
        if not provided or not _hmac.compare_digest(provided.encode(), _dashboard_token.encode()):
            raise HTTPException(status_code=401, detail="Invalid or missing authentication")


@app.on_event("startup")
async def _start_sse_broadcaster() -> None:
    asyncio.create_task(_sse_broadcaster())
    if not _dashboard_token:
        from argus.config import get_settings
        host = get_settings().web_host
        if host != "127.0.0.1":
            logger.warning(
                "SECURITY: DASHBOARD_TOKEN is not set but WEB_HOST=%s — "
                "the dashboard is accessible on the network without authentication. "
                "Set DASHBOARD_TOKEN in your .env file.",
                host,
            )


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
        except asyncio.CancelledError:
            raise
        except stdlib_queue.Empty:
            pass  # expected: _sse_queue.get timeout with no data
        except Exception as exc:
            logger.warning("SSE broadcaster error: %s", exc)


_AUTO_TRIGGER_THRESHOLD = 0.55   # technical confidence threshold
_AUTO_TRIGGER_AI_CONF   = 0.60   # AI decision confidence threshold
_auto_triggered: set = set()     # symbols already auto-triggered this session


def _auto_trigger_investigations(state: dict) -> None:
    """Auto-queue an investigation when both the technical signal and AI decision agree."""
    if _investigate_fn is None:
        return
    signals = state.get("signals") or []
    for sig in signals:
        sym = sig.get("symbol", "")
        if not sym:
            continue
        composite  = sig.get("composite", "neutral")
        confidence = float(sig.get("confidence") or 0)
        ai_action  = sig.get("ai_action")        # enriched by main.py after AI tick
        ai_conf    = float(sig.get("ai_confidence") or 0)
        ai_consensus = sig.get("ai_consensus", False)

        # Technical gate: directional signal with meaningful confidence
        tech_ok = composite in ("bullish", "bearish") and confidence >= _AUTO_TRIGGER_THRESHOLD

        # AI gate: requires a real AI decision — suppress entirely if not yet available
        if ai_action is None:
            continue  # AI hasn't run yet for this symbol; wait for next tick
        ai_ok = ai_action != "HOLD" and ai_conf >= _AUTO_TRIGGER_AI_CONF and ai_consensus

        if not (tech_ok and ai_ok):
            continue

        with _investigation_lock:
            already_active = sym in _investigations and _investigations[sym].get("status") in ("queued", "running", "complete")
        if already_active or sym in _auto_triggered:
            continue
        if len(_investigations) >= _MAX_INVESTIGATIONS:
            continue

        _auto_triggered.add(sym)
        logger.info("Auto-triggering investigation: %s (tech=%s %.0f%% AI=%s %.0f%% consensus=%s)",
                    sym, composite, confidence*100, ai_action, ai_conf*100, ai_consensus)
        with _investigation_lock:
            _investigations[sym] = {
                "symbol": sym,
                "status": "queued",
                "queued_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "auto_triggered": True,
                "trigger_reason": f"{composite} {confidence:.0%} | AI {ai_action} {ai_conf:.0%}",
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
    # Preserve keys that are managed outside the scan loop
    for _k in ("watchlist", "investigations", "pending_approvals"):
        if _k not in state and _k in _state:
            state = {**state, _k: _state[_k]}
    now_ts = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    with _state_lock:
        equity = state.get("equity")
        if equity:
            _equity_history.append({"time": now_ts, "value": float(equity)})
        for _label, _acct_data in (state.get("accounts") or {}).items():
            _eq = _acct_data.get("equity")
            if _eq:
                if _label not in _equity_history_by_account:
                    _equity_history_by_account[_label] = _collections.deque(maxlen=480)
                _equity_history_by_account[_label].append({"time": now_ts, "value": float(_eq)})
        _state = state
        snapshot = {
            **state,
            "equity_history": list(_equity_history),
            "equity_history_by_account": {k: list(v) for k, v in _equity_history_by_account.items()},
            "alert_log": list(_alert_log),
        }
        _equity_save()
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


@app.get("/api/status", dependencies=[Depends(_require_auth)])
async def get_status() -> dict:
    with _state_lock:
        snap = dict(_state)
    snap["paused"] = _paused
    snap["timestamp"] = datetime.datetime.utcnow().isoformat()
    return snap


@app.get("/api/logs", dependencies=[Depends(_require_auth)])
async def get_logs(n: int = Query(default=100, ge=1, le=500)) -> dict:
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


@app.get("/api/positions", dependencies=[Depends(_require_auth)])
async def get_positions() -> dict:
    return {"positions": _state.get("positions", {})}


@app.get("/api/trades", dependencies=[Depends(_require_auth)])
async def get_trades() -> dict:
    return {"trades": _state.get("recent_trades", [])}


@app.get("/api/signals", dependencies=[Depends(_require_auth)])
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
    if not _SYMBOL_RE.match(symbol):
        raise HTTPException(status_code=400, detail="Invalid symbol")
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


_YF_PERIOD_MAP = {
    "day": "5d", "week": "1mo", "month": "1mo",
    "3month": "3mo", "year": "1y", "3year": "3y", "5year": "5y",
}

# Server-side chart cache: (symbol, span, interval) → (fetched_at_epoch, candles)
_chart_cache: dict[str, tuple[float, list]] = {}
_CHART_CACHE_TTL = 300  # 5 minutes


def _yf_chart_fallback(symbol: str, existing: list, span: str = "3month") -> list:
    """Fetch OHLCV + computed SMA-20/EMA-50 from yfinance.

    Used whenever broker data is stale or sparse. Returns the richer
    dataset (yfinance if more candles, else existing).
    """
    try:
        import yfinance as yf
        import pandas as pd
        try:
            from argus.broker.robinhood import CRYPTO_SYMBOLS as _CS
        except Exception:
            _CS: set = set()
        yf_sym = f"{symbol}-USD" if symbol in _CS else symbol
        period = _YF_PERIOD_MAP.get(span, "3mo")
        df = yf.download(yf_sym, period=period, interval="1d", auto_adjust=True, progress=False)
        if df.empty:
            return existing
        # Flatten MultiIndex columns produced by some yfinance versions
        if hasattr(df.columns, "levels"):
            df.columns = [c[0].lower() for c in df.columns]
        else:
            df.columns = [c.lower() for c in df.columns]

        # Compute indicators on the full series before iterating
        closes = df["close"].astype(float)
        sma20 = closes.rolling(20).mean()
        ema50 = closes.ewm(span=50, adjust=False).mean()
        delta = closes.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss
        rsi14 = 100 - (100 / (1 + rs))

        candles = []
        for i, (ts, row) in enumerate(df.iterrows()):
            try:
                t = int(ts.timestamp())
                o  = float(row.get("open",  0))
                h  = float(row.get("high",  0))
                lo = float(row.get("low",   0))
                c  = float(row.get("close", 0))
                if not c or (o == h == lo == c):
                    continue
                v = float(row.get("volume") or 0)
                s20 = sma20.iloc[i]
                e50 = ema50.iloc[i]
                r14 = rsi14.iloc[i]
                candles.append({
                    "time": t, "open": o, "high": h, "low": lo, "close": c, "volume": v,
                    "sma_20": None if pd.isna(s20) else round(float(s20), 4),
                    "ema_50": None if pd.isna(e50) else round(float(e50), 4),
                    "rsi": None if pd.isna(r14) else round(float(r14), 2),
                })
            except Exception:
                pass

        # Patch today's candle with real-time price while market is open
        if candles:
            import datetime as _dt
            today_ts = int(_dt.datetime.combine(_dt.date.today(), _dt.time.min).timestamp())
            last = candles[-1]
            if last["time"] == today_ts:
                try:
                    live = float(yf.Ticker(yf_sym).fast_info.last_price or 0)
                    if live > 0:
                        last["close"] = live
                        last["high"]  = max(last["high"], live)
                        last["low"]   = min(last["low"],  live)
                        logger.info("Patched %s today candle close → %.2f (live)", symbol, live)
                except Exception:
                    pass

        logger.info("yfinance for %s/%s: %d candles", symbol, span, len(candles))
        return candles if len(candles) > len(existing) else existing
    except Exception as exc:
        logger.warning("yfinance chart fallback failed for %s: %s", symbol, exc)
        return existing


@app.get("/api/chart/{symbol}")
async def get_chart(
    symbol: str,
    span: str = Query("3month"),
    interval: str = Query("day")
) -> dict:
    import time as _time
    symbol = symbol.upper().strip()
    if not _SYMBOL_RE.match(symbol):
        raise HTTPException(status_code=400, detail="Invalid symbol")

    # Serve from cache if fresh enough
    cache_key = f"{symbol}|{span}|{interval}"
    cached = _chart_cache.get(cache_key)
    if cached:
        fetched_at, cached_candles = cached
        if _time.monotonic() - fetched_at < _CHART_CACHE_TTL:
            return {"candles": cached_candles, "symbol": symbol, "cached": True}

    if _chart_source_fn is None:
        return {"candles": [], "symbol": symbol}
    try:
        raw = await asyncio.get_event_loop().run_in_executor(
            None, lambda: _chart_source_fn(symbol, span=span, interval=interval)
        )
        candles = []
        for bar in (raw or []):
            try:
                if isinstance(bar.get("time"), (int, float)):
                    t = int(bar["time"])
                else:
                    ts = bar.get("begins_at") or bar.get("timestamp") or ""
                    if not ts:
                        continue
                    import datetime as _dt
                    t = int(_dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
                o = float(bar.get("open_price")  or bar.get("open")  or 0)
                h = float(bar.get("high_price")  or bar.get("high")  or 0)
                lo= float(bar.get("low_price")   or bar.get("low")   or 0)
                c = float(bar.get("close_price") or bar.get("close") or 0)
                v = float(bar.get("volume") or 0)
                if not c or (o == h == lo == c):
                    continue
                candles.append({
                    "time": t, "open": o, "high": h, "low": lo, "close": c, "volume": v,
                    "rsi": bar.get("rsi"), "sma_20": bar.get("sma_20"), "ema_50": bar.get("ema_50")
                })
            except Exception:
                pass

        # Patch with yfinance when broker data is stale (common for daily charts)
        import datetime as _dt
        today_ts = int(_dt.datetime.combine(_dt.date.today(), _dt.time.min).timestamp())
        needs_patch = (
            not candles
            or len(candles) < 5
            or (interval == "day" and candles[-1]["time"] < today_ts)
        )
        if needs_patch:
            _span = span  # capture for lambda
            candles = await asyncio.get_event_loop().run_in_executor(
                None, lambda: _yf_chart_fallback(symbol, candles, span=_span)
            )

        _chart_cache[cache_key] = (_time.monotonic(), candles)
        return {"candles": candles, "symbol": symbol}
    except Exception as exc:
        logger.warning("Chart data error for %s: %s", symbol, exc)
        return {"candles": [], "symbol": symbol}


@app.get("/api/investigate", dependencies=[Depends(_require_auth)])
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


@app.get("/api/flashcards", dependencies=[Depends(_require_auth)])
async def get_flashcards() -> dict:
    cards = _state.get("flashcards", [])
    summary = _state.get("flashcard_summary", {})
    # Calculate scorecard on the fly or retrieve from state
    # Best to retrieve from state if pushed by main loop
    scorecard = _state.get("readiness_scorecard", {})
    return {"flashcards": cards, "summary": summary, "scorecard": scorecard}


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
            try:
                with get_session() as session:
                    add_to_db_watchlist(session, symbol)
            except Exception as e:
                logger.warning("Failed to persist watchlist addition: %s", e)
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
        try:
            with get_session() as session:
                remove_from_db_watchlist(session, symbol)
        except Exception as e:
            logger.warning("Failed to persist watchlist removal: %s", e)
        wl = list(_runtime_watchlist)
    with _state_lock:
        _state["watchlist"] = wl
        snapshot = dict(_state)
    _sse_push(json.dumps(snapshot, default=str))
    logger.info("Watchlist remove: %s → %s", symbol, wl)
    return {"watchlist": wl}


@app.post("/api/watchlist/{symbol}/exit-only", dependencies=[Depends(_require_auth)])
async def set_exit_only_api(symbol: str, payload: dict) -> dict:
    symbol = symbol.upper().strip()
    if not _SYMBOL_RE.match(symbol):
        raise HTTPException(status_code=400, detail="Invalid symbol")
    value = bool(payload.get("value", True))
    try:
        from argus.storage.models import set_exit_only, get_exit_only_symbols
        with get_session() as session:
            set_exit_only(session, symbol, value)
            session.flush()
            exit_only = get_exit_only_symbols(session)
    except Exception as exc:
        logger.error("set_exit_only failed for %s: %s", symbol, exc)
        raise HTTPException(status_code=500, detail="Failed to update exit-only status")
    with _state_lock:
        _state["exit_only_symbols"] = list(exit_only)
        snapshot = dict(_state)
    _sse_push(json.dumps(snapshot, default=str))
    logger.info("Exit-only %s → %s", symbol, value)
    return {"symbol": symbol, "exit_only": value, "exit_only_symbols": list(exit_only)}


@app.post("/api/watchlist/{symbol}/sell-by", dependencies=[Depends(_require_auth)])
async def set_sell_by_api(symbol: str, payload: dict) -> dict:
    symbol = symbol.upper().strip()
    if not _SYMBOL_RE.match(symbol):
        raise HTTPException(status_code=400, detail="Invalid symbol")
    raw_date = payload.get("date")  # ISO string "YYYY-MM-DD" or null/""
    import datetime as _dt2
    date_val = None
    if raw_date:
        try:
            date_val = _dt2.date.fromisoformat(str(raw_date).strip())
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date — use YYYY-MM-DD")
    try:
        from argus.storage.models import set_sell_by_date, get_sell_by_dates
        with get_session() as session:
            set_sell_by_date(session, symbol, date_val)
            session.flush()
            sell_by = get_sell_by_dates(session)
    except Exception as exc:
        logger.error("set_sell_by_date failed for %s: %s", symbol, exc)
        raise HTTPException(status_code=500, detail="Failed to update sell-by date")
    with _state_lock:
        _state["sell_by_dates"] = sell_by
        snapshot = dict(_state)
    _sse_push(json.dumps(snapshot, default=str))
    logger.info("sell_by_date %s → %s", symbol, date_val)
    return {"symbol": symbol, "sell_by_date": date_val.isoformat() if date_val else None,
            "sell_by_dates": sell_by}


@app.get("/api/approvals", dependencies=[Depends(_require_auth)])
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


@app.post("/api/alerts/clear", dependencies=[Depends(_require_auth)])
async def clear_alerts() -> dict:
    global _alert_log
    _alert_log.clear()
    _alert_save()
    with _state_lock:
        snapshot = {**_state, "alert_log": []}
    _sse_push(json.dumps(snapshot, default=str))
    return {"status": "cleared"}


# ── Backtester ───────────────────────────────────────────────────────────────

@app.post("/api/backtest", dependencies=[Depends(_require_auth)])
async def run_backtest(payload: dict) -> dict:
    symbol = (payload.get("symbol") or "").strip().upper()
    span   = payload.get("span", "year")
    if not symbol or not _SYMBOL_RE.match(symbol):
        raise HTTPException(status_code=400, detail="Invalid symbol")
    if span not in ("month", "3month", "year", "3year", "5year"):
        span = "year"
    if _autopilot is None:
        raise HTTPException(status_code=503, detail="Autopilot not running")
    try:
        from argus.engine.backtest import BacktestEngine
        broker = _autopilot._accounts[0].broker
        engine = BacktestEngine(broker)
        loop = asyncio.get_event_loop()
        async with _backtest_sem:
            result = await loop.run_in_executor(None, engine.run, symbol, span)
        return result.to_dict()
    except ValueError as exc:
        logger.warning("Backtest validation error for %s: %s", symbol, exc)
        raise HTTPException(status_code=422, detail="Insufficient historical data for backtest")
    except Exception as exc:
        logger.error("Backtest failed for %s: %s", symbol, exc)
        raise HTTPException(status_code=500, detail="Backtest failed")


# ── SSE stream ────────────────────────────────────────────────────────────────

@app.get("/events", dependencies=[Depends(_require_auth)])
async def sse_stream() -> StreamingResponse:
    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    _subscribers.add(q)

    async def generator() -> AsyncGenerator[str, None]:
        try:
            if _state:
                with _state_lock:
                    initial = {
                        **_state,
                        "equity_history": list(_equity_history),
                        "equity_history_by_account": {k: list(v) for k, v in _equity_history_by_account.items()},
                        "alert_log": list(_alert_log),
                    }
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
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="theme-color" content="#161b22">
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

  @keyframes flash-green { 0% { background: rgba(63,185,80,0.3); } 100% { background: transparent; } }
  @keyframes flash-red { 0% { background: rgba(248,81,73,0.3); } 100% { background: transparent; } }
  .flash-green { animation: flash-green 1s ease-out; }
  .flash-red { animation: flash-red 1s ease-out; }

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

  /* ── Price chip rail ────────────────────────────────────────────────────── */
  .price-chip-rail {
    background: #0d1117;
    border-bottom: 1px solid var(--border);
    height: 36px;
    position: sticky;
    top: 60px;
    z-index: 99;
    display: flex;
    align-items: center;
    overflow: hidden;
  }
  .price-chip-track {
    display: inline-flex;
    align-items: center;
    white-space: nowrap;
    animation: chip-scroll 60s linear infinite;
    will-change: transform;
    flex-shrink: 0;
  }
  .price-chip-track:hover { animation-play-state: paused; }
  @keyframes chip-scroll {
    from { transform: translateX(0); }
    to   { transform: translateX(-50%); }
  }
  .price-chip-sep { color: var(--border); font-size: 9px; padding: 0 10px; flex-shrink: 0; }
  .price-chip {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    padding: 3px 10px;
    font-family: var(--mono);
    font-size: 12px;
    white-space: nowrap;
    flex-shrink: 0;
    transition: all .15s;
    cursor: pointer;
  }
  .price-chip:hover { border-color: var(--accent); background: var(--surface2); }
  .price-chip:active { transform: scale(0.96); }
  .price-chip-sym   { font-weight: 700; color: var(--accent); letter-spacing: .3px; }
  .price-chip-price { color: var(--text); font-variant-numeric: tabular-nums; }
  .price-chip-price.up, .price-chip-arrow.up   { color: var(--green); }
  .price-chip-price.down, .price-chip-arrow.down { color: var(--danger); }
  .price-chip-price.flat, .price-chip-arrow.flat { color: var(--muted); }
  .price-chip-arrow { font-size: 10px; }

  /* ── News ticker bar ─────────────────────────────────────────────────────── */
  .ticker-bar {
    background: #0d1117;
    border-bottom: 1px solid var(--border);
    height: 34px;
    position: sticky;
    top: 96px;
    z-index: 98;
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
  .tab-bar { 
    display: flex; 
    gap: 2px; 
    padding: 0 20px; 
    background: var(--surface); 
    border-bottom: 1px solid var(--border);
    z-index: 100;
  }
  .tab-btn { 
    padding: 10px 18px; 
    font-size: 13px; 
    font-weight: 600; 
    color: var(--muted); 
    background: none; 
    border: none; 
    border-bottom: 2px solid transparent; 
    cursor: pointer; 
    transition: color .15s, border-color .15s; 
    letter-spacing: 0.2px; 
  }
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
  .ct-exit-only-badge { font-size: 9px; font-weight: 800; padding: 2px 6px; border-radius: 4px; letter-spacing: .5px; background: rgba(210,153,34,.2); color: var(--warn); border: 1px solid rgba(210,153,34,.4); white-space: nowrap; }
  .ct-exit-only-btn { background: none; border: 1px solid var(--border); color: var(--muted); padding: 2px 8px; border-radius: 5px; font-size: 10px; font-weight: 700; cursor: pointer; white-space: nowrap; }
  .ct-exit-only-btn:hover { border-color: var(--warn); color: var(--warn); }
  .ct-exit-only-btn.active { background: rgba(210,153,34,.15); color: var(--warn); border-color: var(--warn); }
  .ct-deadline-badge { font-size: 9px; font-weight: 800; padding: 2px 6px; border-radius: 4px; letter-spacing: .5px; background: rgba(248,81,73,.15); color: var(--bear); border: 1px solid rgba(248,81,73,.4); white-space: nowrap; cursor: default; }
  .ct-deadline-badge.soon { background: rgba(248,81,73,.3); animation: deadline-pulse 1.5s ease-in-out infinite; }
  @keyframes deadline-pulse { 0%,100%{opacity:1} 50%{opacity:.6} }
  .ct-sell-by-wrap { display: flex; align-items: center; gap: 4px; }
  .ct-sell-by-input { background: var(--surface); border: 1px solid var(--border); color: var(--text); padding: 2px 6px; border-radius: 5px; font-size: 10px; cursor: pointer; }
  .ct-sell-by-input:focus { border-color: var(--bear); outline: none; }
  .ct-sell-by-btn { background: none; border: 1px solid var(--border); color: var(--muted); padding: 2px 7px; border-radius: 5px; font-size: 10px; font-weight: 700; cursor: pointer; }
  .ct-sell-by-btn:hover { border-color: var(--bear); color: var(--bear); }
  .ct-sell-by-clear { color: var(--muted); font-size: 10px; cursor: pointer; background: none; border: none; padding: 0 2px; display: none; }
  .ct-sell-by-clear.visible { display: inline; }
  .ct-dashlet.exit-only { border-color: rgba(210,153,34,.35); }
  .ct-chart-area { width: 100%; height: 240px; border-radius: var(--radius-sm); overflow: hidden; }
  .ct-dashlet-footer { display: flex; align-items: center; gap: 10px; margin-top: 10px; font-size: 11px; flex-wrap: wrap; }
  .ct-backtest-wrap { display: flex; align-items: center; gap: 4px; margin-left: auto; }
  .ct-bt-span-btn { background: var(--surface3); border: 1px solid var(--border); color: var(--muted); padding: 2px 7px; border-radius: 4px; font-size: 10px; font-weight: 700; cursor: pointer; }
  .ct-bt-span-btn.active { background: rgba(88,166,255,.15); color: var(--blue); border-color: var(--blue); }
  .ct-bt-span-btn:hover:not(.active) { border-color: var(--muted); color: var(--text); }
  .ct-backtest-btn { background: var(--surface3); border: 1px solid var(--border); color: var(--muted); padding: 3px 9px; border-radius: 5px; font-size: 11px; cursor: pointer; }
  .ct-backtest-btn:hover { border-color: var(--accent); color: var(--accent); }
  .ct-backtest-btn.loading { opacity: .5; pointer-events: none; }
  .ct-backtest-result { margin-top: 10px; background: var(--surface3); border-radius: 8px; padding: 10px 12px; font-size: 12px; }
  .ct-bt-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; margin-top: 8px; }
  .ct-bt-stat { background: var(--surface2); border-radius: 6px; padding: 7px 8px; text-align: center; }
  .ct-bt-stat-label { font-size: 10px; color: var(--muted); letter-spacing: .3px; text-transform: uppercase; }
  .ct-bt-stat-val { font-size: 15px; font-weight: 700; margin-top: 2px; font-variant-numeric: tabular-nums; }
  /* Batch backtest */
  .ct-batch-btn { background: var(--surface3); border: 1px solid var(--border); color: var(--muted); padding: 4px 12px; border-radius: 6px; font-size: 12px; cursor: pointer; white-space: nowrap; }
  .ct-batch-btn:hover { border-color: var(--accent); color: var(--accent); }
  .ct-batch-btn.loading { opacity: .5; pointer-events: none; }
  #ct-bt-batch { margin-top: 0; }
  .ct-bt-batch-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
  .ct-bt-batch-span { font-size: 11px; color: var(--muted); font-weight: 700; letter-spacing: .5px; }
  .ct-bt-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .ct-bt-table th { color: var(--muted); font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: .5px; padding: 6px 8px; border-bottom: 1px solid var(--border); text-align: right; cursor: pointer; user-select: none; white-space: nowrap; }
  .ct-bt-table th:first-child { text-align: left; }
  .ct-bt-table th:hover { color: var(--text); }
  .ct-bt-table th.sort-asc::after { content: ' ↑'; }
  .ct-bt-table th.sort-desc::after { content: ' ↓'; }
  .ct-bt-table td { padding: 7px 8px; border-bottom: 1px solid rgba(48,54,61,.5); text-align: right; font-variant-numeric: tabular-nums; }
  .ct-bt-table td:first-child { text-align: left; font-weight: 700; color: var(--accent); cursor: pointer; }
  .ct-bt-table td:first-child:hover { text-decoration: underline; }
  .ct-bt-table tr:last-child td { border-bottom: none; }
  .ct-bt-table tr:hover td { background: var(--surface3); }
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

  /* ── Alerts tab ─────────────────────────────────────────────────────────── */
  #tab-alerts.active { display: flex; flex-direction: column; gap: 0; grid-template-columns: none; }
  .alert-feed { display: flex; flex-direction: column; gap: 6px; padding: 4px 0; }
  .alert-entry { display: flex; gap: 12px; align-items: flex-start; padding: 10px 14px; background: var(--surface); border-radius: var(--radius-sm); border-left: 3px solid var(--border); }
  .alert-entry.buy  { border-left-color: var(--bull); }
  .alert-entry.sell { border-left-color: var(--bear); }
  .alert-entry.kill { border-left-color: #ff8c00; }
  .alert-entry.approval { border-left-color: var(--accent); }
  .alert-entry.investigation { border-left-color: #a78bfa; }
  .alert-entry.error { border-left-color: var(--bear); }
  .alert-inline-actions { display: flex; gap: 8px; margin-top: 6px; }
  .alert-inline-btn { padding: 3px 10px; border-radius: 6px; border: none; font-size: 11px; font-weight: 600; cursor: pointer; }
  .alert-inline-btn.approve { background: rgba(63,185,80,.2); color: #3fb950; }
  .alert-inline-btn.approve:hover { background: rgba(63,185,80,.35); }
  .alert-inline-btn.deny { background: rgba(248,81,73,.2); color: #f85149; }
  .alert-inline-btn.deny:hover { background: rgba(248,81,73,.35); }
  .alert-time { font-size: 11px; color: var(--muted); font-family: var(--mono); min-width: 52px; padding-top: 2px; white-space: nowrap; }
  .alert-subject { font-size: 13px; font-weight: 600; color: var(--text); }
  .alert-body { font-size: 12px; color: var(--muted); margin-top: 2px; line-height: 1.4; }
  .alert-clear-btn { align-self: flex-end; margin-bottom: 8px; font-size: 12px; padding: 4px 12px; }

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
  .token-val.green  { color: var(--green); }
  .token-val.yellow { color: var(--yellow, #f0b429); }
  .token-val.red    { color: var(--danger); }
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

  /* ── Timezone picker ────────────────────────────────────────────────────── */
  .tz-select {
    background: var(--surface2);
    border: 1px solid var(--border);
    color: var(--muted);
    font-size: 11px;
    font-family: var(--mono);
    padding: 4px 6px;
    border-radius: var(--radius-sm);
    cursor: pointer;
    outline: none;
    transition: border-color .15s, color .15s;
  }
  .tz-select:hover, .tz-select:focus { border-color: var(--accent); color: var(--text); }
  .ai-status-wrap { display: flex; align-items: center; gap: 3px; }
  .ai-dot {
    font-size: 13px; line-height: 1; cursor: default;
    transition: color .3s;
    color: var(--muted);
  }
  .ai-dot[data-status="green"]  { color: #4caf82; }
  .ai-dot[data-status="yellow"] { color: #f0b429; }
  .ai-dot[data-status="red"]    { color: #e05a5a; }
  .ai-dot[data-status="gray"]   { color: var(--muted); }

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

  /* ── Mobile / iPhone ────────────────────────────────────────────────────── */
  @media (max-width: 639px) {
    /* iOS safe-area: notch + home indicator */
    body {
      padding-left:   env(safe-area-inset-left,   0px);
      padding-right:  env(safe-area-inset-right,  0px);
      padding-bottom: calc(64px + env(safe-area-inset-bottom, 0px));
    }
    .header {
      padding-left:  max(16px, env(safe-area-inset-left,  16px));
      padding-right: max(16px, env(safe-area-inset-right, 16px));
      height: auto; min-height: 56px;
    }

    /* Fixed Bottom Navigation */
    .tab-bar {
      position: fixed;
      bottom: 0;
      left: 0;
      right: 0;
      height: calc(60px + env(safe-area-inset-bottom, 0px));
      padding-bottom: env(safe-area-inset-bottom, 0px);
      background: rgba(13, 17, 23, 0.85);
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      border-bottom: none;
      border-top: 1px solid var(--border);
      justify-content: space-around;
      padding-left: env(safe-area-inset-left, 10px);
      padding-right: env(safe-area-inset-right, 10px);
      overflow: visible;
    }
    .tab-btn {
      flex: 1;
      padding: 0;
      height: 60px;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      font-size: 10px;
      border-bottom: none;
      border-top: 2px solid transparent;
      gap: 4px;
    }
    .tab-btn.active {
      color: var(--accent);
      border-top-color: var(--accent);
      border-bottom-color: transparent;
    }
    /* Add tiny icons for mobile buttons (CSS mask) */
    .tab-btn::before {
      content: '';
      width: 20px;
      height: 20px;
      background: currentColor;
      display: block;
    }
    /* Using simple SVG icons via data-uri */
    .tab-btn:nth-child(1)::before { mask: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"></rect><rect x="14" y="3" width="7" height="7"></rect><rect x="14" y="14" width="7" height="7"></rect><rect x="3" y="14" width="7" height="7"></rect></svg>') no-repeat center; -webkit-mask: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"></rect><rect x="14" y="3" width="7" height="7"></rect><rect x="14" y="14" width="7" height="7"></rect><rect x="3" y="14" width="7" height="7"></rect></svg>') no-repeat center; }
    .tab-btn:nth-child(2)::before { mask: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"></polyline><polyline points="17 6 23 6 23 12"></polyline></svg>') no-repeat center; -webkit-mask: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"></polyline><polyline points="17 6 23 6 23 12"></polyline></svg>') no-repeat center; }
    .tab-btn:nth-child(3)::before { mask: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"></path><path d="M18 17V9"></path><path d="M13 17V5"></path><path d="M8 17v-3"></path></svg>') no-repeat center; -webkit-mask: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"></path><path d="M18 17V9"></path><path d="M13 17V5"></path><path d="M8 17v-3"></path></svg>') no-repeat center; }
    .tab-btn:nth-child(4)::before { mask: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>') no-repeat center; -webkit-mask: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>') no-repeat center; }
    .tab-btn:nth-child(5)::before { mask: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"></path><path d="M13.73 21a2 2 0 0 1-3.46 0"></path></svg>') no-repeat center; -webkit-mask: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"></path><path d="M13.73 21a2 2 0 0 1-3.46 0"></path></svg>') no-repeat center; }

    /* Header right: drop low-priority items to save space */
    .countdown-wrap { display: none; }
    .tz-select      { display: none; }
    .header-right   { gap: 6px; }
    .mc-label       { display: none; }

    /* Tab bar: reset desktop horizontal scroll */
    .tab-bar::-webkit-scrollbar { display: none; }

    /* Content padding */
    main  { padding: 10px; gap: 10px; }
    .card { padding: 12px 11px; }

    /* Prevent iOS auto-zoom on input focus (requires ≥16px) */
    input, select, textarea { font-size: 16px !important; }

    /* Touch targets: Apple HIG minimum 44pt */
    .btn, .tab-btn, .wl-add-btn, .btn-eye,
    .ticker-speed-btn, .wl-dd-item { min-height: 44px; }

    /* Performance: stack trade/position tables */
    .perf-tables { grid-template-columns: 1fr; }

    /* Investigations: single column (minmax(360px) is too wide for 390px phones) */
    .inv-grid { grid-template-columns: 1fr; }

    /* Charts: taller on mobile for easier reading */
    #price-chart      { height: 280px; }
    .ct-chart-area    { height: 210px; }
    #eq-chart         { height: 140px; }

    /* Dropdowns: don't overflow viewport */
    .wl-dropdown, .chart-search-dd { min-width: unset; width: calc(100vw - 32px); }

    /* Log viewer: shorter so it doesn't eat the whole screen */
    .log-tail { height: 140px; }

    /* Alert body: allow wrapping */
    .alert-entry { gap: 8px; }
    .alert-time  { min-width: 42px; }
  }
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
      <select class="tz-select" id="tz-select" onchange="applyTz(this.value)" title="Chart timezone"></select>
      <button class="btn-eye" id="btn-eye" onclick="toggleValues()" title="Show/hide dollar amounts">👁</button>
    </div>
  </header>
  <div class="price-chip-rail" id="price-chip-rail"></div>
  <div class="ticker-bar">
    <div class="ticker-scrollport">
      <div class="ticker-track" id="ticker-track"></div>
    </div>
    <div class="ticker-speed-wrap">
      <button class="ticker-speed-btn active" onclick="tickerSpeed(this,0.4)" title="Slow">🐢</button>
      <button class="ticker-speed-btn" onclick="tickerSpeed(this,1)" title="Normal">▶</button>
      <button class="ticker-speed-btn" onclick="tickerSpeed(this,2.5)" title="Fast">⚡</button>
    </div>
  </div>
  <nav class="tab-bar">
    <button class="tab-btn active" onclick="switchTab('dashboard')">Dashboard</button>
    <button class="tab-btn" onclick="switchTab('performance')">Performance</button>
    <button class="tab-btn" onclick="switchTab('charts')">Charts</button>
    <button class="tab-btn" onclick="switchTab('investigations')">Investigations</button>
    <button class="tab-btn" onclick="switchTab('alerts')" id="alerts-tab-btn">Alerts</button>
  </nav>
  <main>

  <div class="tab-pane active" id="tab-dashboard">

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
          <div class="ct-tf-btns" style="margin-left: 8px;">
            <button class="ct-tf-btn" onclick="setChartTimeframe('5minute','day')" data-tf="5minute-day">1D</button>
            <button class="ct-tf-btn" onclick="setChartTimeframe('10minute','week')" data-tf="10minute-week">1W</button>
            <button class="ct-tf-btn" onclick="setChartTimeframe('hour','month')" data-tf="hour-month">1M</button>
            <button class="ct-tf-btn active" onclick="setChartTimeframe('day','3month')" data-tf="day-3month">3M</button>
            <button class="ct-tf-btn" onclick="setChartTimeframe('day','year')" data-tf="day-year">1Y</button>
          </div>
        </div>
        <div class="chart-type-btns">
          <button class="chart-type-btn active" id="btn-candles" onclick="setChartType('candles')">Candles</button>
          <button class="chart-type-btn" id="btn-line" onclick="setChartType('line')">Line</button>
        </div>
      </div>
      <div id="price-chart"></div>
      <div id="rsi-chart" style="height:100px;border-top:1px solid var(--border);margin-top:10px;"></div>
      <div id="chart-sparse-warn" style="display:none;margin:6px 0 0;padding:6px 10px;background:rgba(240,180,41,.08);border:1px solid rgba(240,180,41,.3);border-radius:6px;font-size:12px;color:#f0b429;"></div>
      <div class="chart-legend">
        <div class="legend-item"><div class="legend-dot" style="background:#00D4AA"></div> Price</div>
        <div class="legend-item"><div class="legend-dot" style="background:#FFD700"></div> SMA-20</div>
        <div class="legend-item"><div class="legend-dot" style="background:#58a6ff"></div> EMA-50</div>
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

    <!-- Trade Decisions -->
    <div class="card card-full">
      <div class="card-title">Trade Decisions <span style="font-weight:400;text-transform:none;letter-spacing:0;color:var(--text-dim)">— what the AI did and why · click any card to see the full reasoning</span></div>
      <div class="fc-summary" id="fc-summary"></div>
      <div class="fc-grid" id="fc-grid"><div class="empty">No trades recorded yet</div></div>
    </div>

    <!-- Token usage -->
    <div class="card card-full">
      <div class="card-title">Token Usage Today</div>
      <div class="token-grid" id="token-grid"><div class="empty">No AI calls yet</div></div>
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

    <!-- Go-Live Readiness -->
    <div class="card card-full" id="readiness-card" style="border-color: var(--accent); background: rgba(0,212,170,0.02);">
      <div class="card-title" style="color: var(--accent);">Go-Live Readiness Scorecard</div>
      <div id="readiness-container">
        <div class="empty">Calculating readiness...</div>
      </div>
    </div>

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
        <div class="ct-divider"></div>
        <button class="ct-batch-btn" id="ct-batch-btn" onclick="ctBatchBacktest()">⏱ Backtest All</button>
      </div>
    </div>

    <!-- Batch backtest results -->
    <div class="card card-full" id="ct-bt-batch" style="display:none">
      <div class="ct-bt-batch-header">
        <span style="font-size:13px;font-weight:700">Batch Backtest Results</span>
        <div style="display:flex;align-items:center;gap:8px">
          <span class="ct-bt-batch-span" id="ct-batch-span-label"></span>
          <div style="display:flex;gap:4px">
            <button class="ct-bt-span-btn active" id="ct-batch-span-1y" onclick="ctBatchSetSpan('year',this)">1Y</button>
            <button class="ct-bt-span-btn" id="ct-batch-span-3y" onclick="ctBatchSetSpan('3year',this)">3Y</button>
            <button class="ct-bt-span-btn" id="ct-batch-span-5y" onclick="ctBatchSetSpan('5year',this)">5Y</button>
          </div>
          <button onclick="document.getElementById('ct-bt-batch').style.display='none'" style="background:none;border:none;color:var(--muted);cursor:pointer;font-size:14px">✕</button>
        </div>
      </div>
      <div id="ct-bt-batch-body"></div>
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

  <div class="tab-pane" id="tab-alerts">
    <div class="card card-full" style="grid-column:1/-1">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
        <h2 class="card-title" style="margin:0">Alert History</h2>
        <button class="btn alert-clear-btn" onclick="clearAlertLog()">Clear</button>
      </div>
      <div class="alert-feed" id="alert-feed">
        <div class="empty" style="padding:48px 0">No alerts yet — BUY/SELL executions, investigations, and kill switch events will appear here</div>
      </div>
    </div>
  </div><!-- /tab-alerts -->

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

async function promotePosition(symbol, fromAccount, btn) {
  if (!confirm(`Sell ${symbol} from ${fromAccount} and re-buy on Agentic?`)) return;
  if (btn) { btn.disabled = true; btn.textContent = 'Queuing…'; }
  try {
    const r = await apiFetch('/api/promote/' + symbol, {
      method: 'POST',
      body: JSON.stringify({from_account: fromAccount, to_account: 'agentic'})
    });
    const d = await r.json();
    if (btn) {
      btn.textContent = d.status === 'queued' ? 'Queued ✓' : 'Done ✓';
      btn.style.color = 'var(--bull)';
      btn.style.borderColor = 'var(--bull)';
    }
  } catch(e) {
    if (btn) { btn.disabled = false; btn.textContent = 'Promote ↑'; }
  }
}

function renderPerformance(perf, accounts) {
  // Always render account equity summary (independent of closed trades)
  const perfContainer = document.getElementById('perf-container');
  const acctHtml = accounts ? Object.entries(accounts).map(([label, a]) => {
    const eq = a.equity || 0;
    const rPnl = a.since_reset_pnl ?? null;
    const rPct = a.since_reset_pnl_pct ?? null;
    const rSign = (rPnl ?? 0) >= 0 ? '+' : '';
    const rCls = (rPnl ?? 0) >= 0 ? 'var(--green)' : 'var(--red)';
    const dPnl = a.daily_pnl || 0;
    const dPct = a.daily_pnl_pct || 0;
    const dSign = dPnl >= 0 ? '+' : '';
    const dCls = dPnl >= 0 ? 'var(--green)' : 'var(--red)';
    const acctColor = label === 'agentic' ? '#00d4aa' : '#c084fc';
    return `<div class="perf-stat">
      <div class="perf-stat-label" style="color:${acctColor}">${label.toUpperCase()}</div>
      <div class="perf-stat-value private">$${eq.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</div>
      ${rPnl !== null ? `<div class="perf-stat-sub" style="color:${rCls}">Reset: ${rSign}$${Math.abs(rPnl).toFixed(2)} (${rSign}${(rPct||0).toFixed(2)}%)</div>` : ''}
      <div class="perf-stat-sub" style="color:${dCls}">Today: ${dSign}$${Math.abs(dPnl).toFixed(2)} (${dSign}${dPct.toFixed(2)}%)</div>
    </div>`;
  }).join('') : '';

  if (!perf || !perf.closed_trades) {
    perfContainer.innerHTML = `<div class="perf-grid">${acctHtml}</div><div class="empty" style="margin-top:12px">No closed trades yet</div>`;
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

  perfContainer.innerHTML = `
    <div class="perf-grid">
      ${acctHtml}
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

function renderReadiness(scorecard, aiVote) {
  if (!scorecard || !scorecard.sample_size) {
    document.getElementById('readiness-container').innerHTML = '<div class="empty">Calculating goals...</div>';
    return;
  }

  const s = scorecard;
  const readyPill = (s.is_ready && aiVote && aiVote.agreed)
    ? '<span class="pill pill-win" style="font-size:12px;padding:4px 12px">READY FOR LIVE TRADING</span>' 
    : '<span class="pill pill-neutral" style="font-size:12px;padding:4px 12px">ACCUMULATING DATA</span>';

  const rows = [
    { label: 'Sample Size', val: `${s.sample_size.val} / ${s.sample_size.goal}`, ok: s.sample_size.ok, hint: 'Minimum trades for significance' },
    { label: 'Profit Factor', val: `${s.profit_factor.val} / ${s.profit_factor.goal}`, ok: s.profit_factor.ok, hint: 'Gross Profit / Gross Loss' },
    { label: 'Calibration', val: s.calibration.val, ok: s.calibration.ok, hint: 'High-conf vs Low-conf win rate' },
    { label: 'Efficiency', val: s.token_efficiency.val, ok: s.token_efficiency.ok, hint: 'Net profit vs API costs' }
  ];

  let voteHtml = '';
  if (aiVote) {
    const cv = aiVote.claude || {vote:'NO', reasoning:'-'};
    const gv = aiVote.gemini || {vote:'NO', reasoning:'-'};
    voteHtml = `
      <div style="margin-top:20px; border-top: 1px solid var(--border); padding-top:16px;">
        <div class="card-title" style="margin-bottom:12px; font-size:9.5px;">AI Ensemble Audit</div>
        <div style="display:grid; grid-template-columns: 1fr 1fr; gap:12px;">
          <div class="perf-stat" style="border-left: 3px solid ${cv.vote === 'YES' ? 'var(--green)' : 'var(--red)'}">
            <div style="display:flex; justify-content:space-between; align-items:center;">
              <span style="font-weight:700; color:var(--purple); font-size:11px;">CLAUDE</span>
              <span class="pill ${cv.vote === 'YES' ? 'pill-win' : 'pill-loss'}">${cv.vote}</span>
            </div>
            <div style="font-size:11.5px; color:var(--muted); margin-top:6px; line-height:1.4;">${escHtml(cv.reasoning)}</div>
          </div>
          <div class="perf-stat" style="border-left: 3px solid ${gv.vote === 'YES' ? 'var(--green)' : 'var(--red)'}">
            <div style="display:flex; justify-content:space-between; align-items:center;">
              <span style="font-weight:700; color:var(--blue); font-size:11px;">GEMINI</span>
              <span class="pill ${gv.vote === 'YES' ? 'pill-win' : 'pill-loss'}">${gv.vote}</span>
            </div>
            <div style="font-size:11.5px; color:var(--muted); margin-top:6px; line-height:1.4;">${escHtml(gv.reasoning)}</div>
          </div>
        </div>
      </div>
    `;
  }

  document.getElementById('readiness-container').innerHTML = `
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;">
      <div style="font-size:13px;color:var(--muted);max-width:320px;">
        These goals track statistical evidence of a profitable edge and AI alignment before deploying real capital.
      </div>
      <div>${readyPill}</div>
    </div>
    <div class="perf-grid" style="grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));">
      ${rows.map(r => `
        <div class="perf-stat" style="border:1px solid ${r.ok ? 'rgba(63,185,80,0.2)' : 'var(--border)'}">
          <div class="perf-stat-label">${r.label}</div>
          <div class="perf-stat-value" style="color:${r.ok ? 'var(--green)' : 'var(--text)'}">${r.val}</div>
          <div class="perf-stat-sub">${r.hint}</div>
        </div>
      `).join('')}
    </div>
    ${voteHtml}
  `;
}
let pendingCloseSymbol = null;
let valuesHidden = true;
let _nextScanAt = null;

// ── Timezone preference ────────────────────────────────────────────────────────
// List of selectable timezones with short labels
const _TZ_OPTIONS = [
  { tz: 'America/New_York',    label: 'ET' },
  { tz: 'America/Chicago',     label: 'CT' },
  { tz: 'America/Denver',      label: 'MT' },
  { tz: 'America/Los_Angeles', label: 'PT' },
  { tz: 'America/Anchorage',   label: 'AKT' },
  { tz: 'Pacific/Honolulu',    label: 'HT' },
];

function _detectTz() {
  const sys = Intl.DateTimeFormat().resolvedOptions().timeZone;
  return localStorage.getItem('argus_tz') || sys;
}

let _tz = _detectTz();

function _fmtChartTime(ts, tickMarkType) {
  // tickMarkFormatter receives (time, tickMarkType): 0=Year,1=Month,2=DayOfMonth,3=Time,4=TimeWithSeconds
  // localization.timeFormatter receives only (time) — tickMarkType is undefined there.
  // Daily candles from Robinhood are midnight UTC (hourUtc===0, minuteUtc===0).
  const d = new Date(ts * 1000);
  const isDaily = (d.getUTCHours() === 0 && d.getUTCMinutes() === 0)
               || (tickMarkType !== undefined && tickMarkType <= 2);
  if (isDaily) {
    return d.toLocaleDateString('en-US', { timeZone: _tz, month: 'short', day: 'numeric' });
  }
  return d.toLocaleTimeString('en-US', {
    timeZone: _tz, hour: '2-digit', minute: '2-digit', hour12: false,
  });
}

function applyTz(tz) {
  _tz = tz;
  localStorage.setItem('argus_tz', tz);
  const loc = { localization: { timeFormatter: _fmtChartTime } };
  try { if (window._chart)   window._chart.applyOptions(loc);   } catch(_) {}
  try { if (window._eqChart) window._eqChart.applyOptions(loc); } catch(_) {}
  // Reapply to any open dashlet charts
  if (typeof _ctCharts !== 'undefined') {
    Object.values(_ctCharts).forEach(d => {
      try { if (d && d.chart) d.chart.applyOptions(loc); } catch(_) {}
    });
  }
}

function _initTzSelect() {
  const sel = document.getElementById('tz-select');
  if (!sel) return;
  const sys = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const saved = localStorage.getItem('argus_tz');
  const current = saved || sys;
  _tz = current;

  // Build options list; add "System" entry if system TZ isn't in our list
  const inList = _TZ_OPTIONS.some(o => o.tz === sys);
  const opts = inList ? _TZ_OPTIONS
    : [{ tz: sys, label: 'Auto' }, ..._TZ_OPTIONS];

  sel.innerHTML = opts.map(o =>
    `<option value="${o.tz}"${o.tz === current ? ' selected' : ''}>${o.label}</option>`
  ).join('');
}

document.addEventListener('DOMContentLoaded', _initTzSelect);

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

  // Use formatToParts — avoids toLocaleString string-parsing fragility and the
  // Safari "24:00:00" midnight bug when hour12:false is used.
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  }).formatToParts(new Date());
  const p = {};
  parts.forEach(({ type, value }) => { p[type] = parseInt(value); });
  const h   = p.hour % 24;   // formatToParts can give 24 for midnight; normalise to 0
  const min = p.minute;
  const s   = p.second;
  const dow = new Date(p.year, p.month - 1, p.day).getDay();
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
  const fmtAvg = n => '$' + (n || 0).toFixed(5);

  // Avg cost per call
  const cAvg = c.calls ? c.cost_usd / c.calls : 0;
  const gAvg = g.calls ? g.cost_usd / g.calls : 0;
  const totalAvg = usage.total_calls ? usage.total_cost_usd / usage.total_calls : 0;

  // Color thresholds: Claude thinking-off ≈ $0.0025/call; thinking-on ≈ $0.010/call
  const cAvgCls = cAvg > 0.005 ? 'red' : cAvg > 0.002 ? 'yellow' : cAvg > 0 ? 'green' : '';
  // Gemini baseline ≈ $0.0001/call
  const gAvgCls = gAvg > 0.001 ? 'red' : gAvg > 0.0005 ? 'yellow' : gAvg > 0 ? 'green' : '';

  const aiStatus = (window._argusState && window._argusState.ai_status) || {};
  const aiModels = (window._argusState && window._argusState.ai_models) || {};
  const statusDot = (model) => {
    const s = aiStatus[model] || 'gray';
    const colors = { green: '#4caf82', yellow: '#f0b429', red: '#e05a5a', gray: '#555' };
    const labels = { green: 'OK', yellow: 'Billing/quota', red: 'Auth error', gray: 'Not yet called' };
    return `<span style="color:${colors[s]};margin-right:5px" title="${labels[s]}">●</span>`;
  };
  const claudeModel = (aiModels.claude || 'claude').replace('claude-','').replace(/-\d+$/,'');
  const geminiModel = (aiModels.gemini || 'gemini').replace('gemini-','');

  grid.innerHTML = `
    <div class="token-model">
      <div class="token-model-title claude">${statusDot('claude')}Claude · ${claudeModel}</div>
      <div class="token-cost ${c.cost_usd > 0.5 ? 'red' : 'green'}">${fmtC(c.cost_usd)}</div>
      <div class="token-row"><span class="token-label">Calls</span><span class="token-val">${fmtN(c.calls)}</span></div>
      <div class="token-row"><span class="token-label">Avg/call</span><span class="token-val ${cAvgCls}" title="${cAvg > 0.005 ? 'High — thinking may be on' : cAvg > 0.002 ? 'Moderate' : 'Normal'}">${fmtAvg(cAvg)}</span></div>
      <div class="token-row"><span class="token-label">Input</span><span class="token-val">${fmtN(c.input_tokens)}</span></div>
      <div class="token-row"><span class="token-label">Output</span><span class="token-val">${fmtN(c.output_tokens)}</span></div>
      <div class="token-row"><span class="token-label">Cache read</span><span class="token-val">${fmtN(c.cache_read_tokens)}</span></div>
    </div>
    <div class="token-model">
      <div class="token-model-title gemini">${statusDot('gemini')}Gemini · ${geminiModel}</div>
      <div class="token-cost ${g.cost_usd > 0.1 ? 'yellow' : 'green'}">${fmtC(g.cost_usd)}</div>
      <div class="token-row"><span class="token-label">Calls</span><span class="token-val">${fmtN(g.calls)}</span></div>
      <div class="token-row"><span class="token-label">Avg/call</span><span class="token-val ${gAvgCls}">${fmtAvg(gAvg)}</span></div>
      <div class="token-row"><span class="token-label">Input</span><span class="token-val">${fmtN(g.input_tokens)}</span></div>
      <div class="token-row"><span class="token-label">Output</span><span class="token-val">${fmtN(g.output_tokens)}</span></div>
    </div>
    <div class="token-model">
      <div class="token-model-title total">Total Today</div>
      <div class="token-cost">${fmtC(usage.total_cost_usd)}</div>
      <div class="token-row"><span class="token-label">Total calls</span><span class="token-val">${fmtN(usage.total_calls)}</span></div>
      <div class="token-row"><span class="token-label">Avg/call</span><span class="token-val">${fmtAvg(totalAvg)}</span></div>
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
    const resetPnl = a.since_reset_pnl ?? null;
    const resetPnlPct = a.since_reset_pnl_pct ?? null;
    const resetSign = (resetPnl ?? 0) >= 0 ? '+' : '';
    const resetCls = pnlClass(resetPnl ?? 0);
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
      <div class="acct-equity ${cls} private" id="acct-equity-${label}">${'$' + equity.toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2})}</div>
      <div class="acct-row">
        <span class="acct-row-label">Today P&L</span>
        <span class="${pnlCls} private" id="acct-pnl-${label}">${pnlSign}$${Math.abs(pnl).toFixed(2)} (${pnlSign}${pnlPct.toFixed(2)}%)</span>
      </div>
      ${resetPnl !== null ? `<div class="acct-row">
        <span class="acct-row-label">Since Reset</span>
        <span class="${resetCls} private">${resetSign}$${Math.abs(resetPnl).toFixed(2)} (${resetSign}${(resetPnlPct||0).toFixed(2)}%)</span>
      </div>` : ''}
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

function updateAiStatus(ai) {
  if (!ai) return;
  ['claude', 'gemini'].forEach(model => {
    const el = document.getElementById(`ai-dot-${model}`);
    if (!el) return;
    const status = ai[model] || 'gray';
    el.dataset.status = status;
    const labels = { green: 'OK', yellow: 'Billing/quota issue', red: 'Auth error', gray: 'Not yet called' };
    el.title = `${model.charAt(0).toUpperCase() + model.slice(1)}: ${labels[status] || status}`;
  });
}

function triggerFlashes(state, prev) {
  // Accounts
  if (state.accounts && prev.accounts) {
    Object.keys(state.accounts).forEach(label => {
      const a = state.accounts[label];
      const pa = prev.accounts[label];
      if (!pa) return;
      if (a.equity !== pa.equity) {
        const el = document.getElementById(`acct-equity-${label}`);
        if (el) {
          const cls = a.equity > pa.equity ? 'flash-green' : 'flash-red';
          el.classList.add(cls);
          setTimeout(() => el.classList.remove(cls), 1000);
        }
      }
    });
  }
  // Positions
  if (state.positions && prev.positions) {
    Object.keys(state.positions).forEach(sym => {
      const p = state.positions[sym];
      const pp = prev.positions[sym];
      if (!pp) return;
      const safeSym = sym.replace(/\s*\[.*?\]/,'').replace(/[^A-Z0-9]/g, '_');
      if (p.current_price !== pp.current_price) {
        const el = document.getElementById(`pos-price-${safeSym}`);
        if (el) {
          const cls = p.current_price > pp.current_price ? 'flash-green' : 'flash-red';
          el.classList.add(cls);
          setTimeout(() => el.classList.remove(cls), 1000);
        }
      }
      if (p.unrealized_pnl_pct !== pp.unrealized_pnl_pct) {
        const el = document.getElementById(`pos-pnl-${safeSym}`);
        if (el) {
          const cls = p.unrealized_pnl_pct > pp.unrealized_pnl_pct ? 'flash-green' : 'flash-red';
          el.classList.add(cls);
          setTimeout(() => el.classList.remove(cls), 1000);
        }
      }
    });
  }
}

let _lastState = null;
function applyState(state) {
  const prevState = _lastState;
  _lastState = state;
  window._argusState = state;
  paused = state.paused || false;
  if (state.equity_goal) _equityGoal = state.equity_goal;
  updateBadges(state);
  updateAiStatus(state.ai_status);

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
      const safeSym = rawSym.replace(/[^A-Z0-9]/g, '_');
      return `<tr class="tr-hover">
        <td class="accent">${escHtml(sym)}</td>
        <td class="txt-right">${fmt(p.quantity,4)}</td>
        <td class="txt-right">${fmtDollar(p.entry_price)}</td>
        <td class="txt-right" id="pos-price-${safeSym}">${fmtDollar(p.current_price)}</td>
        <td class="txt-right ${pnlClass(pct)}" id="pos-pnl-${safeSym}">${pct >= 0 ? '+' : ''}${fmt(pct)}%</td>
        <td class="txt-right muted">${fmtDollar(p.stop_loss_price)}</td>
        <td class="txt-center">
          <button class="btn btn-danger" style="padding:5px 11px;font-size:11px;font-weight:700" onclick="confirmClose('${escHtml(rawSym)}')">Close</button>
          ${(p.account||'').toLowerCase()==='default' ? `<button class="btn-promote" onclick="promotePosition('${escHtml(rawSym)}','default',this)" title="Sell here, re-buy on Agentic">Promote ↑</button>` : ''}
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
  updatePriceChips(state.signals, state.watchlist);
  updateTicker(state.signals);
  if (state.watchlist) { ctApplyWatchlist(state.watchlist); buildChartTabs(state.watchlist); }
  if (state.exit_only_symbols !== undefined) ctApplyExitOnlyState(state.exit_only_symbols);
  if (state.sell_by_dates !== undefined) ctApplySellByState(state.sell_by_dates);
  if (state.investigations) renderInvestigations(state.investigations);
  if (state.equity_history) eqRender(state.equity_history, state.equity_history_by_account || {});
  // Scan timing
  if (state.next_scan_at) _nextScanAt = new Date(state.next_scan_at);
  _updateSessionBadge(state.market_session || 'closed');
  _syncIntervalSelect(state.interval_override);
  _updateCountdown();

  renderTokenUsage(state.token_usage);
  renderPerformance(state.performance, state.accounts);
  renderReadiness(state.readiness_scorecard, state.ai_vote);
  renderFlashcards(state);
  if (state.alert_log) renderAlerts(state.alert_log, state.pending_approvals);
  renderLogs(state.logs || []);
  // Refresh markers if chart is showing (new closed trades may have arrived)
  if (_chartSymbol && _lastCandles[_chartSymbol]) {
    placeTradeMarkers(_chartSymbol, _lastCandles[_chartSymbol]);
  }

  if (prevState) triggerFlashes(state, prevState);

  document.getElementById('last-update').textContent = 'Last update: ' + new Date().toLocaleTimeString();
  document.getElementById('btn-pause').textContent = (state.paused || paused) ? '▶ Resume' : '⏸ Pause';
}

async function decideApproval(tradeId, decision) {
  // Immediate optimistic UI — replace all approve/deny buttons for this trade with a status chip
  document.querySelectorAll(`[onclick*="${tradeId}"]`).forEach(btn => {
    const wrap = btn.closest('.approval-actions, .alert-inline-btns');
    if (wrap) {
      const label = decision === 'approve' ? '✓ Approved — executing at next scan' : '✗ Denied';
      const cls   = decision === 'approve' ? 'color:var(--bull)' : 'color:var(--bear)';
      wrap.innerHTML = `<span style="font-size:12px;font-weight:700;${cls}">${label}</span>`;
    }
  });
  try {
    await apiFetch(`/api/${decision}/${encodeURIComponent(tradeId)}`, {method:'POST'});
  } catch(e) {
    // 404 = already decided (e.g. double-click) — UI already updated, silently ignore
  }
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
  const status = await apiFetch('/api/status').then(r=>r.json());
  paused = status.paused;
  updateBadges(status);
  document.getElementById('btn-pause').textContent = paused ? '▶ Resume' : '⏸ Pause';
}

async function fetchAll() {
  const [status, positions, signals, trades, logs] = await Promise.all([
    apiFetch('/api/status').then(r=>r.json()),
    apiFetch('/api/positions').then(r=>r.json()),
    apiFetch('/api/signals').then(r=>r.json()),
    apiFetch('/api/trades').then(r=>r.json()),
    apiFetch('/api/logs?n=100').then(r=>r.json()),
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
      <span class="log-ts">${escHtml(e.ts)}</span>
      <span class="log-lvl log-lvl-${lvl}">${lvl}</span>
      <span class="log-name">${escHtml(e.name || '')}</span>
      <span class="log-msg">${escHtml(e.msg)}</span>
    </div>`;
  }).join('');
  const auto = document.getElementById('log-autoscroll');
  if (auto && auto.checked) box.scrollTop = box.scrollHeight;
}

function escHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
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
let _volumeSeries = null;
let _smaSeries    = null;
let _emaSeries    = null;
let _predSeries   = null;   // dashed prediction line
let _rsiChart     = null;
let _rsiSeries    = null;
let _chartSymbol  = null;
let _chartType    = 'candles';   // 'candles' | 'line'
let _chartSpan     = '3month';
let _chartInterval = 'day';
let _lastCandles  = {};

async function setChartTimeframe(interval, span) {
  _chartInterval = interval;
  _chartSpan     = span;
  document.querySelectorAll('.ct-tf-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tf === `${interval}-${span}`);
  });
  if (_chartSymbol) loadChart(_chartSymbol);
}

const _CHART_OPTS = {
  layout: { background: { color: '#161920' }, textColor: '#8892a4' },
  grid:   { vertLines: { color: '#2a2f3e' }, horzLines: { color: '#2a2f3e' } },
  crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
  rightPriceScale: { borderColor: '#2a2f3e' },
  timeScale: { borderColor: '#2a2f3e', timeVisible: true, tickMarkFormatter: _fmtChartTime },
  handleScroll: true, handleScale: false,
  localization: { timeFormatter: _fmtChartTime },
};

function initChart() {
  const el = document.getElementById('price-chart');
  _chart = LightweightCharts.createChart(el, _CHART_OPTS);
  window._chart = _chart;

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

  _volumeSeries = _chart.addHistogramSeries({
    color: '#26a69a',
    priceFormat: { type: 'volume' },
    priceScaleId: '', // overlay
  });
  _volumeSeries.priceScale().applyOptions({
    scaleMargins: { top: 0.8, bottom: 0 },
  });

  _smaSeries = _chart.addLineSeries({ color: '#FFD700', lineWidth: 1.5, title: 'SMA-20' });
  _emaSeries = _chart.addLineSeries({ color: '#58a6ff', lineWidth: 1.5, title: 'EMA-50' });

  _predSeries = _chart.addLineSeries({
    color: '#60a5fa', lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle.Dashed,
    lastValueVisible: true, priceLineVisible: false,
    title: 'trend',
  });

  // RSI Chart
  const rsiEl = document.getElementById('rsi-chart');
  _rsiChart = LightweightCharts.createChart(rsiEl, {
    ..._CHART_OPTS,
    height: 100,
    timeScale: { ..._CHART_OPTS.timeScale, visible: false }, // hide time scale, synced with main
  });
  _rsiSeries = _rsiChart.addLineSeries({ color: '#c084fc', lineWidth: 1.5, title: 'RSI' });
  _rsiSeries.createPriceLine({ price: 70, color: '#f85149', lineWidth: 1, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: '70' });
  _rsiSeries.createPriceLine({ price: 30, color: '#3fb950', lineWidth: 1, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: '30' });

  // Sync RSI with Main Chart
  _chart.timeScale().subscribeVisibleTimeRangeChange(range => {
    _rsiChart.timeScale().setVisibleRange(range);
  });
  _rsiChart.timeScale().subscribeVisibleTimeRangeChange(range => {
    _chart.timeScale().setVisibleRange(range);
  });
  _chart.subscribeCrosshairMove(param => {
    if (param.time) _rsiChart.setCrosshairPosition(param.point.x, param.time, _rsiSeries);
    else _rsiChart.clearCrosshairPosition();
  });
  _rsiChart.subscribeCrosshairMove(param => {
    if (param.time) _chart.setCrosshairPosition(param.point.x, param.time, _candleSeries);
    else _chart.clearCrosshairPosition();
  });

  new ResizeObserver(() => {
    _chart.applyOptions({ width: el.clientWidth });
    _rsiChart.applyOptions({ width: el.clientWidth });
  }).observe(el);
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
    const res  = await fetch(`/api/chart/${encodeURIComponent(symbol)}?span=${_chartSpan}&interval=${_chartInterval}`);
    const data = await res.json();
    const candles = (data.candles || []).sort((a, b) => a.time - b.time);
    _lastCandles[symbol] = candles;

    _candleSeries.setData(candles);
    _lineSeries.setData(candles.map(c => ({ time: c.time, value: c.close })));

    // Volume
    _volumeSeries.setData(candles.filter(c => c.volume != null).map(c => ({
      time: c.time,
      value: c.volume || 0,
      color: c.close >= c.open ? 'rgba(0, 212, 170, 0.5)' : 'rgba(248, 81, 73, 0.5)'
    })));

    // Indicators
    _smaSeries.setData(candles.filter(c => c.sma_20 != null).map(c => ({ time: c.time, value: c.sma_20 })));
    _emaSeries.setData(candles.filter(c => c.ema_50 != null).map(c => ({ time: c.time, value: c.ema_50 })));
    _rsiSeries.setData(candles.filter(c => c.rsi != null).map(c => ({ time: c.time, value: c.rsi })));

    // Prediction line — needs at least 5 points for a meaningful trend
    if (candles.length >= 5) {
      _predSeries.setData(buildPrediction(candles));
    } else {
      _predSeries.setData([]);
    }

    // Sparse data warning
    const warn = document.getElementById('chart-sparse-warn');
    if (warn) {
      if (candles.length < 5) {
        warn.textContent = `Only ${candles.length} day${candles.length === 1 ? '' : 's'} of data available for ${symbol} — recently listed or thinly traded.`;
        warn.style.display = 'block';
      } else {
        warn.style.display = 'none';
      }
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
    updatePriceChips(window._latestSignals || [], window._argusState && window._argusState.watchlist);
    updateTicker(window._latestSignals || []);
  } catch(e) { console.warn('fetchNewsHeadlines failed', e); }
}

let _tickerSpeedMult = 0.4;
function tickerSpeed(btn, mult) {
  _tickerSpeedMult = mult;
  document.querySelectorAll('.ticker-speed-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const track = document.getElementById('ticker-track');
  if (track && track.scrollWidth) {
    const dur = Math.max(5, Math.round((track.scrollWidth / 6) / (120 * _tickerSpeedMult)));
    track.style.animationDuration = dur + 's';
  }
  const chipTrack = document.querySelector('.price-chip-track');
  if (chipTrack && chipTrack.scrollWidth) {
    const dur = Math.max(10, Math.round((chipTrack.scrollWidth / 2) / (80 * _tickerSpeedMult)));
    chipTrack.style.animationDuration = dur + 's';
  }
}

// updatePriceChips — revolving marquee, one chip per watchlist symbol
function updatePriceChips(signals, watchlist) {
  const rail = document.getElementById('price-chip-rail');
  if (!rail) return;

  const priceMap = {};
  (signals || []).forEach(s => { if (s.symbol) priceMap[s.symbol] = s; });

  const wl = watchlist || (window._argusState && window._argusState.watchlist) || [];
  if (!wl.length) { rail.innerHTML = ''; return; }

  const chipsHtml = wl.map(sym => {
    const s = priceMap[sym];
    const price = s && s.price != null ? '$' + s.price.toFixed(s.price < 10 ? 4 : 2) : '—';
    const change = s ? (s.change_pct || 0) : 0;
    const arrow = change > 0 ? '▲' : change < 0 ? '▼' : '—';
    const arrowCls = change > 0 ? 'up' : change < 0 ? 'down' : 'flat';
    const priceColor = change > 0 ? 'var(--green)' : change < 0 ? 'var(--red)' : 'var(--muted)';
    const title = s ? `${change >= 0 ? '+' : ''}${(change).toFixed(2)}% today` : '';
    return `<span class="price-chip" title="${escHtml(title)}" onclick="ctQuickView('${escHtml(sym)}')">`
      + `<span class="price-chip-sym">${escHtml(sym)}</span>`
      + `<span class="price-chip-price" style="color:${priceColor}">${price}</span>`
      + `<span class="price-chip-arrow ${arrowCls}">${arrow}</span>`
      + `</span><span class="price-chip-sep">·</span>`;
  }).join('');

  // Double content for seamless loop
  rail.innerHTML = `<div class="price-chip-track">${chipsHtml}${chipsHtml}</div>`;

  requestAnimationFrame(() => {
    const track = rail.querySelector('.price-chip-track');
    if (track) {
      const dur = Math.max(10, Math.round((track.scrollWidth / 2) / (80 * _tickerSpeedMult)));
      track.style.animationDuration = dur + 's';
    }
  });
}

function ctQuickView(symbol) {
  switchTab('charts');
  loadChart(symbol);
}

// updateTicker — scrolling news marquee only (prices moved to price chip rail)
function updateTicker(signals) {
  const track = document.getElementById('ticker-track');
  if (!track) return;
  if (!_tickerHeadlines.length) return;

  // News items only
  const newsHtml = _tickerHeadlines.slice(0, 12).map(h => {
    const inner = h.url
      ? `<a class="ticker-headline" href="${escHtml(h.url)}" target="_blank" rel="noopener">${escHtml(h.headline)}</a>`
      : `<span class="ticker-headline">${escHtml(h.headline)}</span>`;
    return `<span class="ticker-news-item"><span class="ticker-news-badge">NEWS</span>${inner}</span><span class="ticker-dot">·</span>`;
  }).join('');

  if (!newsHtml) return;

  // 6 copies for seamless infinite CSS scroll (translateX -16.667%)
  track.innerHTML = newsHtml.repeat(6);

  requestAnimationFrame(() => {
    const singleW = track.scrollWidth / 6;
    const dur = Math.max(5, Math.round(singleW / (120 * _tickerSpeedMult)));
    track.style.animationDuration = dur + 's';
  });
}

// ── Equity curve ─────────────────────────────────────────────────────────────

const _EQ_COLORS = {
  agentic: { line: '#00D4AA', top: 'rgba(0,212,170,.15)', bot: 'rgba(0,212,170,.01)' },
  default: { line: '#c084fc', top: 'rgba(192,132,252,.10)', bot: 'rgba(192,132,252,.01)' },
};

let _eqChart = null, _eqSeriesByAcct = {}, _eqCombinedSeries = null;
let _eqHistory = [], _eqHistoryByAcct = {}, _eqRange = 'session';

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
    timeScale: {
      borderColor: 'rgba(255,255,255,.08)',
      timeVisible: true,
      secondsVisible: false,
      tickMarkFormatter: _fmtChartTime,
    },
    crosshair: { mode: LightweightCharts.CrosshairMode.Magnet },
    handleScroll: false,
    handleScale: false,
    localization: { timeFormatter: _fmtChartTime },
  });
  window._eqChart = _eqChart;
  // Per-account area series — each in account color (no combined line; avoids 2× scale confusion)
  for (const [label, c] of Object.entries(_EQ_COLORS)) {
    _eqSeriesByAcct[label] = _eqChart.addAreaSeries({
      lineColor: c.line,
      topColor: c.top,
      bottomColor: c.bot,
      lineWidth: 2,
      priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
    });
  }
  new ResizeObserver(() => { if (_eqChart) _eqChart.resize(el.clientWidth, 160); }).observe(el);
}

function eqSetRange(r) {
  _eqRange = r;
  document.querySelectorAll('.eq-range-btn').forEach(b => b.classList.toggle('active', b.textContent.toLowerCase().replace(' ','') === r));
  eqRender(_eqHistory, _eqHistoryByAcct);
}

function _eqFilterSort(history) {
  const now = Math.floor(Date.now() / 1000);
  const cutoff = _eqRange === '30m' ? now - 1800
               : _eqRange === '1h'  ? now - 3600
               : 0;
  let pts = (history || []).filter(p => p.time >= cutoff);
  if (!pts.length) pts = (history || []).slice(-1);
  const seen = new Set();
  return pts
    .sort((a, b) => a.time - b.time)
    .filter(p => { if (seen.has(p.time)) return false; seen.add(p.time); return true; });
}

function eqRender(history, historyByAcct) {
  _eqHistory = history || [];
  _eqHistoryByAcct = historyByAcct || {};
  eqInit();
  if (!_eqChart) return;

  const hasAcctData = Object.keys(_eqHistoryByAcct).length > 0;

  if (hasAcctData) {
    for (const [label, series] of Object.entries(_eqSeriesByAcct)) {
      series.setData(_eqFilterSort(_eqHistoryByAcct[label] || []));
    }
  } else {
    // No per-account data yet — show combined on the agentic (teal) series as placeholder
    const pts = _eqFilterSort(_eqHistory);
    for (const [label, series] of Object.entries(_eqSeriesByAcct)) {
      series.setData(label === 'agentic' ? pts : []);
    }
  }

  _eqChart.timeScale().fitContent();

  // Stats bar: per-account P&L in their colors
  let statsHtml = '';
  for (const [label, c] of Object.entries(_EQ_COLORS)) {
    const pts = _eqFilterSort(_eqHistoryByAcct[label] || []);
    if (!pts.length) continue;
    const chg = pts[pts.length-1].value - pts[0].value;
    const chgPct = pts[0].value ? (chg / pts[0].value) * 100 : 0;
    const sign = chg >= 0 ? '+' : '';
    const col  = chg >= 0 ? 'var(--green)' : 'var(--danger)';
    statsHtml += `<span style="color:${c.line};font-weight:700;margin-right:4px">●</span>` +
      `<span style="margin-right:12px">${label.toUpperCase()}: <strong style="color:${col}">${sign}$${Math.abs(chg).toFixed(2)}</strong></span>`;
  }
  const statsEl = document.getElementById('eq-stats');
  if (statsEl) statsEl.innerHTML = statsHtml || '&nbsp;';
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

  const isExitOnly = (window._argusState && (window._argusState.exit_only_symbols||[])).includes(sym);
  const _sbd = (window._argusState && window._argusState.sell_by_dates && window._argusState.sell_by_dates[sym]) || '';
  const _sbdDays = _sbd ? Math.ceil((new Date(_sbd+'T00:00:00') - new Date()) / 86400000) : null;
  const _hasSbd = !!_sbd;
  card.innerHTML = `
    <div class="ct-dashlet-header">
      <span class="ct-dashlet-sym">${escHtml(sym)}</span>
      ${isExitOnly ? '<span class="ct-exit-only-badge" id="ct-eo-badge-${escHtml(sym)}">EXIT ONLY</span>' : ''}
      ${_hasSbd ? `<span class="ct-deadline-badge${_sbdDays<=7?' soon':''}" id="ct-dl-badge-${escHtml(sym)}" title="Sell by ${escHtml(_sbd)}">DEADLINE ${_sbdDays}d</span>` : `<span class="ct-deadline-badge" id="ct-dl-badge-${escHtml(sym)}" style="display:none"></span>`}
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
      <div class="ct-backtest-wrap">
        <button class="ct-bt-span-btn active" id="ct-bt-s1y-${escHtml(sym)}" onclick="ctBtSetSpan('${escHtml(sym)}','year',this)">1Y</button>
        <button class="ct-bt-span-btn" id="ct-bt-s3y-${escHtml(sym)}" onclick="ctBtSetSpan('${escHtml(sym)}','3year',this)">3Y</button>
        <button class="ct-bt-span-btn" id="ct-bt-s5y-${escHtml(sym)}" onclick="ctBtSetSpan('${escHtml(sym)}','5year',this)">5Y</button>
        <button class="ct-backtest-btn" id="ct-bt-btn-${escHtml(sym)}" onclick="ctRunBacktest('${escHtml(sym)}')">⏱ Backtest</button>
        <button class="ct-exit-only-btn${isExitOnly?' active':''}" id="ct-eo-btn-${escHtml(sym)}" onclick="ctToggleExitOnly('${escHtml(sym)}')" title="No new buys — hold until natural exit">⚠ Exit Only</button>
        <div class="ct-sell-by-wrap" title="Auto-sell deadline (tax-loss or target exit)">
          <input class="ct-sell-by-input" type="date" id="ct-sbd-${escHtml(sym)}" value="${escHtml(_sbd)}"
                 onchange="ctSetSellBy('${escHtml(sym)}',this.value)">
          <button class="ct-sell-by-btn" onclick="ctSetSellBy('${escHtml(sym)}',document.getElementById('ct-sbd-${escHtml(sym)}').value)">📅 Deadline</button>
          <button class="ct-sell-by-clear${_hasSbd?' visible':''}" id="ct-sbd-clr-${escHtml(sym)}" onclick="ctClearSellBy('${escHtml(sym)}')" title="Clear deadline">✕</button>
        </div>
      </div>
    </div>
    <div class="ct-backtest-result" id="ct-bt-${escHtml(sym)}" style="display:none"></div>`;
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

const _ctBtSpan = {};  // sym -> 'year' | '3year' | '5year'
const _ctBtSpanLabel = { year: '1Y', '3year': '3Y', '5year': '5Y' };

function ctBtSetSpan(sym, span, btn) {
  _ctBtSpan[sym] = span;
  ['year','3year','5year'].forEach(s => {
    const sfx = s === 'year' ? '1y' : s === '3year' ? '3y' : '5y';
    const el = document.getElementById(`ct-bt-s${sfx}-${sym}`);
    if (el) el.classList.toggle('active', s === span);
  });
}

async function ctRunBacktest(sym) {
  const btn = document.getElementById('ct-bt-btn-' + sym);
  const panel = document.getElementById('ct-bt-' + sym);
  if (!btn || !panel) return;
  const span = _ctBtSpan[sym] || 'year';
  const spanLabel = _ctBtSpanLabel[span] || '1Y';
  btn.classList.add('loading');
  btn.textContent = '⏱ Running…';
  panel.style.display = 'none';
  try {
    const r = await fetch('/api/backtest', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...(window._ARGUS_TOKEN?{'X-Argus-Token':window._ARGUS_TOKEN}:{})},
      body: JSON.stringify({symbol: sym, span}),
    });
    const d = await r.json();
    if (!r.ok) { panel.innerHTML = `<span style="color:var(--bear)">${escHtml(d.detail||'Backtest failed')}</span>`; panel.style.display = ''; return; }
    panel.innerHTML = _ctBtResultHtml(d, spanLabel);
    panel.style.display = '';
  } catch (e) {
    panel.textContent = 'Network error';
    panel.style.display = '';
  } finally {
    btn.classList.remove('loading');
    btn.textContent = '⏱ Backtest';
  }
}

function _ctBtResultHtml(d, spanLabel) {
  const ret = d.total_return_pct;
  const retColor = ret >= 0 ? 'var(--bull)' : 'var(--bear)';
  const ddColor = d.max_drawdown_pct > 20 ? 'var(--bear)' : d.max_drawdown_pct > 10 ? 'var(--warn)' : 'var(--text)';
  const pfColor = d.profit_factor >= 1.5 ? 'var(--bull)' : d.profit_factor >= 1.0 ? 'var(--text)' : 'var(--bear)';
  return `<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px">
      <span style="font-size:11px;font-weight:700;color:var(--muted)">BACKTEST · ${escHtml(spanLabel)} · INDICATORS ONLY</span>
      <span style="font-size:11px;color:var(--muted)">${escHtml(d.start_date||'').slice(0,10)} → ${escHtml(d.end_date||'').slice(0,10)}</span>
    </div>
    <div class="ct-bt-grid">
      <div class="ct-bt-stat"><div class="ct-bt-stat-label">Return</div><div class="ct-bt-stat-val" style="color:${retColor}">${ret>=0?'+':''}${ret.toFixed(1)}%</div></div>
      <div class="ct-bt-stat"><div class="ct-bt-stat-label">Win Rate</div><div class="ct-bt-stat-val">${(d.win_rate*100).toFixed(0)}%</div></div>
      <div class="ct-bt-stat"><div class="ct-bt-stat-label">Profit Factor</div><div class="ct-bt-stat-val" style="color:${pfColor}">${d.profit_factor.toFixed(2)}x</div></div>
      <div class="ct-bt-stat"><div class="ct-bt-stat-label">Max Drawdown</div><div class="ct-bt-stat-val" style="color:${ddColor}">${d.max_drawdown_pct.toFixed(1)}%</div></div>
    </div>
    <div style="margin-top:6px;font-size:11px;color:var(--muted)">${d.trade_count} trades · $10k starting capital · $1k per position · 5% stop-loss</div>`;
}

// ── Batch backtest ────────────────────────────────────────────────────────────
let _ctBatchSpan = 'year';

function ctBatchSetSpan(span, btn) {
  _ctBatchSpan = span;
  document.querySelectorAll('#ct-bt-batch .ct-bt-span-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  ctBatchBacktest();
}

async function ctBatchBacktest() {
  const wl = _ctWatchlist || [];
  if (!wl.length) return;
  const btn = document.getElementById('ct-batch-btn');
  const panel = document.getElementById('ct-bt-batch');
  const body = document.getElementById('ct-bt-batch-body');
  const spanLabel = _ctBtSpanLabel[_ctBatchSpan] || '1Y';
  document.getElementById('ct-batch-span-label').textContent = spanLabel + ' · INDICATORS ONLY';
  if (btn) { btn.classList.add('loading'); btn.textContent = '⏱ Running…'; }
  panel.style.display = '';
  body.innerHTML = `<div style="color:var(--muted);font-size:12px;padding:12px 0">Running ${wl.length} backtests…</div>`;

  const results = await Promise.all(wl.map(async sym => {
    try {
      const r = await fetch('/api/backtest', {
        method: 'POST',
        headers: {'Content-Type':'application/json', ...(window._ARGUS_TOKEN?{'X-Argus-Token':window._ARGUS_TOKEN}:{})},
        body: JSON.stringify({symbol: sym, span: _ctBatchSpan}),
      });
      const d = await r.json();
      return r.ok ? {...d, symbol: sym} : {symbol: sym, error: d.detail};
    } catch { return {symbol: sym, error: 'Network error'}; }
  }));

  _ctRenderBatchTable(results, spanLabel);
  if (btn) { btn.classList.remove('loading'); btn.textContent = '⏱ Backtest All'; }
}

let _ctBatchSortCol = 'total_return_pct', _ctBatchSortAsc = false;
let _ctBatchResults = [];

function _ctRenderBatchTable(results, spanLabel) {
  _ctBatchResults = results;
  const body = document.getElementById('ct-bt-batch-body');
  const cols = [
    {key:'symbol',       label:'Symbol',        fmt: v => `<span onclick="ctQuickView('${escHtml(v)}')">${escHtml(v)}</span>`},
    {key:'total_return_pct', label:'Return',    fmt: v => `<span style="color:${v>=0?'var(--bull)':'var(--bear)'}">${v>=0?'+':''}${v.toFixed(1)}%</span>`},
    {key:'win_rate',     label:'Win Rate',      fmt: v => `${(v*100).toFixed(0)}%`},
    {key:'profit_factor',label:'Profit Factor', fmt: (v,row) => `<span style="color:${v>=1.5?'var(--bull)':v>=1?'var(--text)':'var(--bear)'}">${v.toFixed(2)}x</span>`},
    {key:'max_drawdown_pct', label:'Max DD',    fmt: v => `<span style="color:${v>20?'var(--bear)':v>10?'var(--warn)':'var(--text)'}">${v.toFixed(1)}%</span>`},
    {key:'trade_count',  label:'Trades',        fmt: v => v},
  ];

  const sorted = [...results].sort((a, b) => {
    if (a.error && !b.error) return 1;
    if (!a.error && b.error) return -1;
    const av = a[_ctBatchSortCol] ?? -Infinity;
    const bv = b[_ctBatchSortCol] ?? -Infinity;
    return _ctBatchSortAsc ? av - bv : bv - av;
  });

  const thHtml = cols.map(c => {
    const active = c.key === _ctBatchSortCol;
    const dir = active ? (_ctBatchSortAsc ? 'sort-asc' : 'sort-desc') : '';
    return `<th class="${dir}" onclick="ctBatchSort('${c.key}')">${c.label}</th>`;
  }).join('');

  const rowsHtml = sorted.map(row => {
    if (row.error) return `<tr><td>${escHtml(row.symbol)}</td><td colspan="5" style="color:var(--muted);text-align:left">${escHtml(row.error)}</td></tr>`;
    return `<tr>${cols.map(c => `<td>${c.fmt(row[c.key], row)}</td>`).join('')}</tr>`;
  }).join('');

  // Summary row
  const ok = results.filter(r => !r.error);
  const avgRet = ok.reduce((s,r) => s + r.total_return_pct, 0) / (ok.length || 1);
  const avgPf  = ok.reduce((s,r) => s + r.profit_factor, 0) / (ok.length || 1);
  const avgWr  = ok.reduce((s,r) => s + r.win_rate, 0) / (ok.length || 1);
  const totalTrades = ok.reduce((s,r) => s + r.trade_count, 0);

  body.innerHTML = `
    <table class="ct-bt-table">
      <thead><tr>${thHtml}</tr></thead>
      <tbody>${rowsHtml}</tbody>
      <tfoot><tr style="font-weight:700;color:var(--muted)">
        <td>Avg (${ok.length} symbols)</td>
        <td style="color:${avgRet>=0?'var(--bull)':'var(--bear)'}">${avgRet>=0?'+':''}${avgRet.toFixed(1)}%</td>
        <td>${(avgWr*100).toFixed(0)}%</td>
        <td style="color:${avgPf>=1.5?'var(--bull)':avgPf>=1?'var(--text)':'var(--bear)'}">${avgPf.toFixed(2)}x</td>
        <td>—</td>
        <td>${totalTrades}</td>
      </tr></tfoot>
    </table>
    <div style="margin-top:8px;font-size:11px;color:var(--muted)">$10k starting capital · $1k per position · 5% stop-loss · no AI layer</div>`;
}

function ctBatchSort(col) {
  if (_ctBatchSortCol === col) _ctBatchSortAsc = !_ctBatchSortAsc;
  else { _ctBatchSortCol = col; _ctBatchSortAsc = false; }
  const spanLabel = _ctBtSpanLabel[_ctBatchSpan] || '1Y';
  _ctRenderBatchTable(_ctBatchResults, spanLabel);
}

// ── Exit-only toggle ─────────────────────────────────────────────────────────
async function ctToggleExitOnly(sym) {
  const btn = document.getElementById('ct-eo-btn-' + sym);
  const isActive = btn && btn.classList.contains('active');
  const newVal = !isActive;
  await apiFetch(`/api/watchlist/${encodeURIComponent(sym)}/exit-only`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({value: newVal}),
  });
  // SSE will push updated state — no manual DOM update needed
}

function ctApplyExitOnlyState(exitOnlySymbols) {
  const set = new Set(exitOnlySymbols || []);
  for (const sym of _ctWatchlist) {
    const btn = document.getElementById('ct-eo-btn-' + sym);
    const card = document.getElementById('ct-card-' + sym);
    const badgeId = 'ct-eo-badge-' + sym;
    if (btn) btn.classList.toggle('active', set.has(sym));
    if (card) card.classList.toggle('exit-only', set.has(sym));
    // Update or inject badge in header
    const header = card && card.querySelector('.ct-dashlet-header');
    if (header) {
      let badge = document.getElementById(badgeId);
      if (set.has(sym) && !badge) {
        badge = document.createElement('span');
        badge.id = badgeId;
        badge.className = 'ct-exit-only-badge';
        badge.textContent = 'EXIT ONLY';
        header.insertBefore(badge, header.children[1]);
      } else if (!set.has(sym) && badge) {
        badge.remove();
      }
    }
  }
}

async function ctSetSellBy(sym, dateStr) {
  if (!dateStr) return;
  await apiFetch(`/api/watchlist/${encodeURIComponent(sym)}/sell-by`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({date: dateStr}),
  });
  // SSE will push updated state
}

async function ctClearSellBy(sym) {
  await apiFetch(`/api/watchlist/${encodeURIComponent(sym)}/sell-by`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({date: null}),
  });
}

function ctApplySellByState(sellByDates) {
  const map = sellByDates || {};
  const now = new Date();
  for (const sym of _ctWatchlist) {
    const badge = document.getElementById('ct-dl-badge-' + sym);
    const inp   = document.getElementById('ct-sbd-' + sym);
    const clr   = document.getElementById('ct-sbd-clr-' + sym);
    const dateStr = map[sym] || '';
    if (dateStr) {
      const daysLeft = Math.ceil((new Date(dateStr + 'T00:00:00') - now) / 86400000);
      const isSoon = daysLeft <= 7;
      if (badge) {
        badge.textContent = 'DEADLINE ' + daysLeft + 'd';
        badge.title = 'Sell by ' + dateStr;
        badge.className = 'ct-deadline-badge' + (isSoon ? ' soon' : '');
        badge.style.display = '';
      }
      if (inp) inp.value = dateStr;
      if (clr) clr.classList.add('visible');
    } else {
      if (badge) { badge.style.display = 'none'; badge.textContent = ''; }
      if (inp)   inp.value = '';
      if (clr)   clr.classList.remove('visible');
    }
  }
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

// SSE — EventSource cannot send custom headers, so token is passed as query param
function connectSSE() {
  const tok = window._ARGUS_TOKEN || '';
  const url = '/events' + (tok ? '?token=' + encodeURIComponent(tok) : '');
  const evtSource = new EventSource(url);
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

// ── Alerts tab ───────────────────────────────────────────────────────────────
function classifyAlert(subject) {
  const s = (subject || '').toUpperCase();
  if (s.includes('KILL') || s.includes('DRAWDOWN')) return 'kill';
  if (s.includes('ERROR')) return 'error';
  if (s.includes('APPROVAL')) return 'approval';
  if (s.includes('INVESTIGAT') || s.includes('🟢') || s.includes('🔴')) return 'investigation';
  if (s.includes('BUY')) return 'buy';
  if (s.includes('SELL')) return 'sell';
  return '';
}

function renderAlerts(alertLog, pendingApprovals) {
  const feed = document.getElementById('alert-feed');
  if (!feed) return;
  // Badge on tab button
  const btn = document.getElementById('alerts-tab-btn');
  if (btn && alertLog.length) btn.textContent = `Alerts (${alertLog.length})`;
  if (!alertLog.length) {
    feed.innerHTML = '<div class="empty" style="padding:48px 0">No alerts yet — BUY/SELL executions, investigations, and kill switch events will appear here</div>';
    return;
  }
  // Build lookup: "BUY|AAPL" -> trade_id for inline approve/deny buttons
  const approvalMap = {};
  for (const [id, info] of Object.entries(pendingApprovals || {})) {
    approvalMap[`${(info.action||'BUY').toUpperCase()}|${(info.symbol||'').toUpperCase()}`] = id;
  }
  feed.innerHTML = alertLog.map(a => {
    const cls = classifyAlert(a.subject);
    const d = new Date(a.time);
    const t = d.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
    let actionBtns = '';
    const m = (a.subject||'').match(/Approval needed:\s*(\w+)\s+(\w+)/i);
    if (m) {
      const tid = approvalMap[`${m[1].toUpperCase()}|${m[2].toUpperCase()}`];
      if (tid) {
        actionBtns = `<div class="alert-inline-actions">
          <button class="alert-inline-btn approve" onclick="decideApproval('${tid}','approve')">✓ Approve</button>
          <button class="alert-inline-btn deny" onclick="decideApproval('${tid}','deny')">✗ Deny</button>
        </div>`;
      }
    }
    return `<div class="alert-entry ${cls}">
      <span class="alert-time">${t}</span>
      <div>
        <div class="alert-subject">${escHtml(a.subject)}</div>
        <div class="alert-body">${escHtml(a.body)}</div>
        ${actionBtns}
      </div>
    </div>`;
  }).join('');
}

async function clearAlertLog() {
  await apiFetch('/api/alerts/clear', {method:'POST'});
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


_MOBILE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="theme-color" content="#0d1117">
<title>Argus</title>
<style>
:root {
  --bg:#0d1117; --surface:#161b22; --surface2:#1c2128; --surface3:#21262d;
  --border:#30363d; --text:#e6edf3; --muted:#8b949e; --accent:#00d084;
  --bull:#3fb950; --bear:#f85149; --warn:#d29922; --purple:#a78bfa;
  --blue:#58a6ff; --mono:'SF Mono',Monaco,monospace; --radius:12px;
  --nav-h:64px; --safe-b:env(safe-area-inset-bottom,0px);
  --safe-t:env(safe-area-inset-top, 20px);
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;overflow:hidden}

/* ── Layout ── */
#app{display:flex;flex-direction:column;height:100dvh;height:100vh;padding-top:var(--safe-t)}
#screen{flex:1;overflow-y:auto;overflow-x:hidden;-webkit-overflow-scrolling:touch;padding-bottom:calc(var(--nav-h) + var(--safe-b) + 16px)}

/* ── Top Price Rail ── */
.m-price-rail {
  overflow: hidden;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
  height: 38px;
  display: flex;
  align-items: center;
}
.m-rail-track {
  display: flex;
  align-items: center;
  gap: 0;
  white-space: nowrap;
  will-change: transform;
  animation: m-rail-scroll 80s linear infinite;
}
.m-rail-track:hover { animation-play-state: paused; }
@keyframes m-rail-scroll {
  from { transform: translateX(0); }
  to   { transform: translateX(-50%); }
}
.m-chip {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 0 14px;
  font-family: var(--mono);
  font-size: 12px;
  white-space: nowrap;
  cursor: pointer;
  flex-shrink: 0;
}
.m-chip-sep { color: var(--border); padding: 0 2px; font-size: 10px; flex-shrink: 0; }
.m-chip-sym { font-weight: 800; color: var(--accent); }
.m-chip-price { font-weight: 600; }

/* ── Bottom nav ── */
#nav{position:fixed;bottom:0;left:0;right:0;height:calc(var(--nav-h) + var(--safe-b));padding-bottom:var(--safe-b);background:rgba(22,27,34,0.8);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border-top:1px solid var(--border);display:flex;z-index:100}
.nav-btn{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:4px;background:none;border:none;color:var(--muted);font-size:10px;font-weight:600;letter-spacing:.3px;cursor:pointer;padding-top:8px}
.nav-btn svg { width: 22px; height: 22px; stroke: currentColor; fill: none; transition: transform 0.1s; }
.nav-btn:active svg { transform: scale(0.9); }
.nav-btn.active{color:var(--accent)}
.nav-badge{position:relative}
.nav-badge::after{content:attr(data-count);position:absolute;top:-4px;right:-8px;background:var(--bear);color:#fff;font-size:9px;font-weight:700;min-width:16px;height:16px;border-radius:99px;display:flex;align-items:center;justify-content:center;padding:0 3px;display:none}
.nav-badge[data-count]:not([data-count=""])::after{display:flex}

/* ── Readiness Scorecard (Mobile) ── */
.m-readiness {
  background: rgba(0, 208, 132, 0.03);
  border: 1px solid rgba(0, 208, 132, 0.2);
  border-radius: var(--radius);
  padding: 14px;
  margin-bottom: 12px;
}
.m-readiness-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
  margin-top: 10px;
}
.m-goal-box {
  background: var(--surface2);
  padding: 10px;
  border-radius: 8px;
  border-left: 3px solid var(--border);
}
.m-goal-box.ok { border-left-color: var(--accent); }
.m-goal-lbl { font-size: 10px; color: var(--muted); text-transform: uppercase; margin-bottom: 2px; }
.m-goal-val { font-size: 15px; font-weight: 800; }

/* ── Panes ── */
.pane{display:none;padding:16px;gap:12px;flex-direction:column}
.pane.active{display:flex}

/* ── Cards ── */
.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px}
.card-label{font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px}
.card-big{font-size:clamp(24px,8vw,36px);font-weight:800;font-variant-numeric:tabular-nums;line-height:1.1;letter-spacing:-1px}
.card-sub{font-size:13px;color:var(--muted);margin-top:4px;font-variant-numeric:tabular-nums}

/* ── Account row ── */
.acct-row{display:flex;gap:10px}
.acct-card{flex:1;background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:12px}
.acct-name{font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.5px;text-transform:uppercase;margin-bottom:4px}
.acct-eq{font-size:22px;font-weight:800;font-variant-numeric:tabular-nums;line-height:1}
.acct-pnl{font-size:12px;margin-top:3px;font-variant-numeric:tabular-nums}
.acct-kill{font-size:10px;color:var(--warn);font-weight:700;margin-top:4px}

/* ── Signal list ── */
.sig-row{display:flex;align-items:center;gap:10px;padding:12px 0;border-bottom:1px solid var(--border)}
.sig-row:last-child{border-bottom:none}
.sig-sym{font-size:16px;font-weight:800;font-family:var(--mono);min-width:52px;color:var(--text)}
.sig-price{font-size:14px;font-weight:600;font-variant-numeric:tabular-nums;color:var(--text);flex:1}
.sig-badge{font-size:11px;font-weight:700;padding:4px 10px;border-radius:99px;letter-spacing:.3px}
.sig-badge.buy {background:rgba(63,185,80,.15);color:var(--bull);border:1px solid rgba(63,185,80,.3)}
.sig-badge.sell{background:rgba(248,81,73,.15);color:var(--bear);border:1px solid rgba(248,81,73,.3)}
.sig-badge.hold{background:rgba(139,148,158,.1);color:var(--muted);border:1px solid var(--border)}
.sig-conf{font-size:11px;color:var(--muted);min-width:36px;text-align:right}

/* ── Trade feed ── */
.trade-row{display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--border)}
.trade-row:last-child{border-bottom:none}
.trade-side{font-size:11px;font-weight:700;padding:3px 8px;border-radius:99px;min-width:42px;text-align:center}
.trade-side.buy {background:rgba(63,185,80,.15);color:var(--bull)}
.trade-side.sell{background:rgba(248,81,73,.15);color:var(--bear)}
.trade-sym{font-size:15px;font-weight:700;font-family:var(--mono);flex:1}
.trade-val{font-size:13px;color:var(--muted);font-variant-numeric:tabular-nums}
.trade-time{font-size:11px;color:var(--muted)}

/* ── Alert feed ── */
.alert-row{display:flex;gap:10px;align-items:flex-start;padding:12px;background:var(--surface2);border-radius:10px;border-left:3px solid var(--border)}
.alert-row.buy {border-left-color:var(--bull)}
.alert-row.sell{border-left-color:var(--bear)}
.alert-row.kill{border-left-color:var(--warn)}
.alert-row.approval{border-left-color:var(--accent)}
.alert-row.investigation{border-left-color:var(--purple)}
.alert-row.error{border-left-color:var(--bear)}
.alert-t{font-size:10px;color:var(--muted);font-family:var(--mono);white-space:nowrap;padding-top:2px;min-width:40px}
.alert-subj{font-size:13px;font-weight:600}
.alert-body{font-size:12px;color:var(--muted);margin-top:2px;line-height:1.4}
.alert-inline-actions{display:flex;gap:8px;margin-top:8px}
.alert-inline-btn{padding:5px 14px;border-radius:8px;border:none;font-size:12px;font-weight:700;cursor:pointer}
.alert-inline-btn.approve{background:rgba(63,185,80,.2);color:var(--bull)}
.alert-inline-btn.deny{background:rgba(248,81,73,.2);color:var(--bear)}

/* ── Chart ── */
#m-chart-pills{display:flex;gap:6px;overflow-x:auto;padding-bottom:4px;scrollbar-width:none}
#m-chart-pills::-webkit-scrollbar{display:none}
.m-pill{padding:6px 14px;border-radius:99px;font-size:13px;font-weight:700;font-family:var(--mono);background:var(--surface2);border:1px solid var(--border);color:var(--muted);white-space:nowrap;cursor:pointer;flex-shrink:0;min-height:36px}
.m-pill.active{background:var(--accent);color:#000d0a;border-color:var(--accent)}
#m-chart-area{width:100%;height:min(380px,45dvh);border-radius:var(--radius);overflow:hidden;margin-top:10px;background:var(--surface2)}
.m-ohlc{display:flex;justify-content:space-between;margin-top:10px;font-size:12px;color:var(--muted);font-family:var(--mono)}
.m-ohlc span b{color:var(--text)}

/* ── Investigate ── */
.inv-search-wrap{position:relative}
.inv-m-input{width:100%;background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:14px 16px;font-size:16px;color:var(--text);outline:none}
.inv-m-input:focus{border-color:var(--accent)}
.inv-m-btn{width:100%;margin-top:8px;padding:14px;background:var(--accent);color:#000d0a;font-weight:700;font-size:15px;border:none;border-radius:10px;cursor:pointer;min-height:52px}
.inv-m-card{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:14px;position:relative}
.inv-m-sym{font-size:18px;font-weight:800;font-family:var(--mono)}
.inv-m-status{font-size:12px;color:var(--muted);margin-top:2px}
.inv-m-verdict{font-size:15px;font-weight:700;margin-top:8px}
.inv-m-summary{font-size:12px;color:var(--muted);margin-top:6px;line-height:1.5}
.inv-m-del{position:absolute;top:12px;right:12px;background:none;border:none;color:var(--muted);font-size:18px;cursor:pointer;min-width:44px;min-height:44px;display:flex;align-items:center;justify-content:center}
.inv-m-dd{position:absolute;top:calc(100%+4px);left:0;right:0;background:var(--surface2);border:1px solid var(--border);border-radius:10px;z-index:50;overflow:hidden;display:none}
.inv-m-dd-item{padding:12px 16px;display:flex;align-items:center;gap:10px;cursor:pointer;border-bottom:1px solid var(--border)}
.inv-m-dd-item:last-child{border-bottom:none}
.inv-m-dd-sym{font-family:var(--mono);font-weight:700;color:var(--accent);min-width:50px}
.inv-m-dd-name{font-size:13px;color:var(--muted)}

/* ── Misc ── */
.pill-up{color:var(--bull)} .pill-dn{color:var(--bear)}
.empty-state{text-align:center;color:var(--muted);padding:48px 0;font-size:14px}
.section-title{font-size:12px;font-weight:700;color:var(--muted);letter-spacing:.5px;text-transform:uppercase;margin-bottom:8px}
.kill-banner{background:rgba(210,153,34,.12);border:1px solid rgba(210,153,34,.3);border-radius:10px;padding:10px 14px;font-size:13px;color:var(--warn);font-weight:600;display:flex;align-items:center;gap:8px}
.btn-clear{background:var(--surface2);border:1px solid var(--border);color:var(--muted);padding:8px 16px;border-radius:8px;font-size:13px;cursor:pointer;align-self:flex-end;min-height:44px}

/* ── Connection status dot ── */
#conn-dot{width:8px;height:8px;border-radius:99px;background:var(--bull);flex-shrink:0;transition:background .4s;box-shadow:0 0 6px var(--bull)}
#conn-dot.offline{background:var(--bear);box-shadow:0 0 6px var(--bear)}
#conn-dot.reconnecting{background:var(--warn);box-shadow:0 0 6px var(--warn)}
#conn-status-bar{display:flex;align-items:center;gap:5px;font-size:11px;color:var(--muted)}

/* ── Market status bar ── */
#m-status-bar{display:flex;align-items:center;justify-content:space-between;padding:5px 14px;background:var(--surface);border-bottom:1px solid var(--border);flex-shrink:0}
.m-sess-badge{font-size:10px;font-weight:700;padding:3px 9px;border-radius:99px;letter-spacing:.3px;white-space:nowrap}
.m-sess-open {background:rgba(63,185,80,.15);color:var(--bull);border:1px solid rgba(63,185,80,.3)}
.m-sess-pre  {background:rgba(88,166,255,.12);color:var(--blue);border:1px solid rgba(88,166,255,.25)}
.m-sess-after{background:rgba(210,153,34,.12);color:var(--warn);border:1px solid rgba(210,153,34,.25)}
.m-sess-closed{background:var(--surface2);color:var(--muted);border:1px solid var(--border)}
#m-mc{display:flex;align-items:center;gap:4px;font-family:var(--mono)}
#m-mc-label{font-size:10px;color:var(--muted)}
#m-mc-val{font-size:11px;font-weight:700;min-width:52px;text-align:right;font-variant-numeric:tabular-nums}

/* ── Actionable signal summary ── */
.top-signals{display:flex;flex-direction:column;gap:6px;margin-bottom:4px}
.top-sig-chip{display:flex;align-items:center;gap:10px;background:var(--surface2);border-radius:8px;padding:10px 12px;border-left:3px solid var(--border)}
.top-sig-chip.buy{border-left-color:var(--bull)} .top-sig-chip.sell{border-left-color:var(--bear)}
.top-sig-chip-sym{font-family:var(--mono);font-weight:800;font-size:15px;min-width:52px}
.top-sig-chip-action{font-size:12px;font-weight:700;flex:1}
.top-sig-chip-conf{font-size:11px;color:var(--muted)}

/* ── Open positions ── */
.pos-row{display:flex;align-items:center;gap:8px;padding:10px 0;border-bottom:1px solid var(--border)}
.pos-row:last-child{border-bottom:none}
.pos-sym{font-family:var(--mono);font-weight:800;font-size:15px;min-width:52px}
.pos-qty{font-size:12px;color:var(--muted);flex:1}
.pos-val{font-size:14px;font-weight:700;font-variant-numeric:tabular-nums}
.pos-pnl{font-size:12px;font-variant-numeric:tabular-nums}

/* ── Pending approvals ── */
.appr-card{background:rgba(0,208,132,.06);border:1px solid rgba(0,208,132,.2);border-radius:10px;padding:12px}
.appr-sym{font-family:var(--mono);font-size:16px;font-weight:800;color:var(--accent)}
.appr-detail{font-size:12px;color:var(--muted);margin-top:3px;line-height:1.4}
.appr-reasoning{font-size:12px;color:var(--text);margin-top:6px;line-height:1.5;opacity:.85}
.appr-actions{display:flex;gap:8px;margin-top:10px}
.appr-btn{flex:1;padding:10px;border:none;border-radius:8px;font-size:13px;font-weight:700;cursor:pointer;min-height:44px}
.appr-btn.approve{background:var(--bull);color:#000}
.appr-btn.deny{background:rgba(248,81,73,.15);color:var(--bear);border:1px solid rgba(248,81,73,.3)}

/* ── Alert filter chips ── */
.filter-chips{display:flex;gap:6px;overflow-x:auto;padding-bottom:4px;scrollbar-width:none}
.filter-chips::-webkit-scrollbar{display:none}
.filter-chip{padding:6px 14px;border-radius:99px;font-size:12px;font-weight:700;background:var(--surface2);border:1px solid var(--border);color:var(--muted);white-space:nowrap;cursor:pointer;flex-shrink:0;min-height:36px}
.filter-chip.active{background:var(--accent);color:#000d0a;border-color:var(--accent)}

/* ── Timeframe pills for charts ── */
.tf-pills{display:flex;gap:6px;margin-top:8px}
.tf-pill{padding:5px 14px;border-radius:99px;font-size:12px;font-weight:700;background:var(--surface2);border:1px solid var(--border);color:var(--muted);cursor:pointer;min-height:36px}
.tf-pill.active{background:rgba(88,166,255,.15);color:var(--blue);border-color:var(--blue)}

/* ── Investigate from signals ── */
.sig-inv-btn{font-size:11px;padding:4px 8px;border:1px solid var(--border);border-radius:6px;background:none;color:var(--muted);cursor:pointer;min-height:32px}
.sig-inv-btn:active{background:var(--surface3)}

/* ── Pull-to-refresh ── */
#ptr{height:0;overflow:hidden;display:flex;align-items:center;justify-content:center;font-size:12px;color:var(--muted);transition:height .2s}
#ptr.ready{color:var(--accent)}
</style>
</head>
<body>
<div id="app">
  <div class="m-price-rail" id="m-price-rail"></div>
  <div id="m-status-bar">
    <div id="conn-status-bar">
      <span id="conn-dot"></span>
      <span id="conn-label">Connecting…</span>
    </div>
    <span class="m-sess-badge m-sess-closed" id="m-session-badge">CLOSED</span>
    <div id="m-mc">
      <span id="m-mc-label">Opens in</span>
      <span id="m-mc-val">—</span>
    </div>
  </div>
  <div id="screen">
    <div id="ptr">↓ Release to refresh</div>

    <!-- HOME -->
    <div class="pane active" id="pane-home">
      <div id="m-readiness-wrap"></div>
      <div class="card">
        <div class="card-label">Total Equity</div>
        <div class="card-big" id="m-equity">—</div>
        <div class="card-sub" id="m-pnl">—</div>
      </div>
      <div class="acct-row" id="m-accts"></div>
      <div id="m-kill-wrap"></div>
      <div id="m-top-signals-wrap"></div>
      <div class="card" id="m-positions-card" style="display:none">
        <div class="section-title">Open Positions</div>
        <div id="m-positions"></div>
      </div>
      <div id="m-approvals-wrap"></div>
      <div class="card">
        <div class="section-title">Recent Trades</div>
        <div id="m-trades"><div class="empty-state">No trades yet</div></div>
      </div>
    </div>

    <!-- SIGNALS -->
    <div class="pane" id="pane-signals">
      <div class="card">
        <div class="section-title">Signals</div>
        <div id="m-signals"><div class="empty-state">Waiting for scan…</div></div>
      </div>
    </div>

    <!-- CHARTS -->
    <div class="pane" id="pane-charts">
      <div id="m-chart-pills"></div>
      <div class="tf-pills">
        <button class="tf-pill" onclick="mTf('1D')">1D</button>
        <button class="tf-pill active" onclick="mTf('1W')">1W</button>
        <button class="tf-pill" onclick="mTf('1M')">1M</button>
        <button class="tf-pill" onclick="mTf('3M')">3M</button>
      </div>
      <div id="m-chart-area"></div>
      <div class="m-ohlc" id="m-ohlc" style="display:none">
        <span>O <b id="mo-o">—</b></span>
        <span>H <b id="mo-h">—</b></span>
        <span>L <b id="mo-l">—</b></span>
        <span>C <b id="mo-c">—</b></span>
      </div>
    </div>

    <!-- ALERTS -->
    <div class="pane" id="pane-alerts">
      <div class="filter-chips">
        <button class="filter-chip active" onclick="mAlertFilter('all',this)">All</button>
        <button class="filter-chip" onclick="mAlertFilter('buy',this)">Buy</button>
        <button class="filter-chip" onclick="mAlertFilter('sell',this)">Sell</button>
        <button class="filter-chip" onclick="mAlertFilter('investigation',this)">Research</button>
        <button class="filter-chip" onclick="mAlertFilter('kill',this)">Kill</button>
      </div>
      <div id="m-alert-feed"><div class="empty-state">No alerts yet</div></div>
      <button class="btn-clear" onclick="mClearAlertsConfirm()" style="margin-top:8px">Clear all alerts</button>
    </div>

    <!-- INVESTIGATE -->
    <div class="pane" id="pane-investigate">
      <div class="card">
        <div class="inv-search-wrap">
          <input class="inv-m-input" id="inv-m-input" placeholder="Symbol to investigate…"
                 autocomplete="off" oninput="mInvSearch(this.value)"
                 onblur="setTimeout(mInvDdClose,150)">
          <div class="inv-m-dd" id="inv-m-dd"></div>
        </div>
        <button class="inv-m-btn" onclick="mInvStart()">🔍 Deep Dive</button>
      </div>
      <div id="m-inv-cards"><div class="empty-state">No active investigations</div></div>
    </div>

  </div><!-- /screen -->

  <!-- Bottom nav -->
  <nav id="nav">
    <button class="nav-btn active" onclick="mTab('home')" id="nav-home">
      <svg viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"></rect><rect x="14" y="3" width="7" height="7"></rect><rect x="14" y="14" width="7" height="7"></rect><rect x="3" y="14" width="7" height="7"></rect></svg>
      Home
    </button>
    <button class="nav-btn" onclick="mTab('signals')" id="nav-signals">
      <svg viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline></svg>
      Signals
    </button>
    <button class="nav-btn" onclick="mTab('charts')" id="nav-charts">
      <svg viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"></path><path d="M18 17V9"></path><path d="M13 17V5"></path><path d="M8 17v-3"></path></svg>
      Charts
    </button>
    <button class="nav-btn nav-badge" onclick="mTab('alerts')" id="nav-alerts">
      <svg viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"></path><path d="M13.73 21a2 2 0 0 1-3.46 0"></path></svg>
      Alerts
    </button>
    <button class="nav-btn" onclick="mTab('investigate')" id="nav-investigate">
      <svg viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
      Research
    </button>
  </nav>
</div>

<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"
        integrity="sha384-JZigAjwiaZtkUbA44CWkPaT3iBb/mU5pO6QOANp+OqHd4q+1+7MG1kzp2OOP9ZfP"
        crossorigin="anonymous"></script>
<script>
const esc = s => String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmt$ = v => '$' + Number(v).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
const fmtPct = v => (v>=0?'+':'')+Number(v).toFixed(2)+'%';
let _state = {};
let _mChart = null, _mSeries = null, _mSym = null;
let _mTf = '1W';   // chart timeframe
let _alertFilter = 'all';
let _allAlerts = [];

// ── Tab switching ──────────────────────────────────────────────────────────
function mTab(name) {
  document.querySelectorAll('.pane').forEach(p => p.classList.toggle('active', p.id === 'pane-' + name));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.toggle('active', b.id === 'nav-' + name));
  if (name === 'charts' && _mSym) requestAnimationFrame(() => mLoadChart(_mSym));
}
// ── State rendering ────────────────────────────────────────────────────────
function mApply(state) {
  _state = state;

  // Price Rail + Readiness + Connection
  mRenderPriceRail(state.signals);
  mRenderReadiness(state.readiness_scorecard);
  _setConn('online');

  // Equity
  if (state.equity != null) {
    document.getElementById('m-equity').textContent = fmt$(state.equity);
  }
  if (state.performance) {
    const pnl = state.performance.daily_pnl_pct ?? 0;
    const el = document.getElementById('m-pnl');
    el.textContent = fmtPct(pnl) + ' today';
    el.className = 'card-sub ' + (pnl >= 0 ? 'pill-up' : 'pill-dn');
  }

  // Accounts
  const acctWrap = document.getElementById('m-accts');
  const accounts = state.accounts || {};
  if (Object.keys(accounts).length) {
    acctWrap.innerHTML = Object.entries(accounts).map(([label, a]) => {
      const pnl = a.daily_pnl_pct ?? 0;
      const kill = a.kill_switch_active ? '<div class="acct-kill">⚠ Kill switch active</div>' : '';
      return `<div class="acct-card">
        <div class="acct-name">${esc(label)}</div>
        <div class="acct-eq">${fmt$(a.equity ?? 0)}</div>
        <div class="acct-pnl ${pnl>=0?'pill-up':'pill-dn'}">${fmtPct(pnl)}</div>
        ${kill}
      </div>`;
    }).join('');
  }

  // Kill switch banners
  const killWrap = document.getElementById('m-kill-wrap');
  const kills = Object.entries(accounts).filter(([,a]) => a.kill_switch_active);
  killWrap.innerHTML = kills.map(([label]) =>
    `<div class="kill-banner">⚠ ${esc(label.toUpperCase())} kill switch active — drawdown limit hit</div>`
  ).join('');

  // Top actionable signals (non-HOLD)
  const sigs = state.signals || [];
  const actionable = sigs.filter(s => {
    const c = (s.composite||'').toLowerCase();
    return c === 'bullish' || c === 'bearish';
  }).slice(0, 2);
  const topWrap = document.getElementById('m-top-signals-wrap');
  if (actionable.length) {
    topWrap.innerHTML = `<div class="top-signals">` + actionable.map(s => {
      const c = (s.composite||'').toLowerCase();
      const cls = c === 'bullish' ? 'buy' : 'sell';
      const label = c === 'bullish' ? '↑ BUY signal' : '↓ SELL signal';
      const conf = s.confidence != null ? Math.round(s.confidence*100)+'% confident' : '';
      return `<div class="top-sig-chip ${cls}" onclick="mTab('charts');mSetSym('${esc(s.symbol)}')">
        <span class="top-sig-chip-sym">${esc(s.symbol)}</span>
        <span class="top-sig-chip-action">${label}</span>
        <span class="top-sig-chip-conf">${conf}</span>
      </div>`;
    }).join('') + `</div>`;
  } else {
    topWrap.innerHTML = '';
  }

  // Signals tab
  const sigEl = document.getElementById('m-signals');
  const _mSbd = state.sell_by_dates || {};
  const _mEo  = new Set(state.exit_only_symbols || []);
  if (sigs.length) {
    sigEl.innerHTML = sigs.map(s => {
      const comp = (s.composite || 'neutral').toLowerCase();
      const cls = comp === 'bullish' ? 'buy' : comp === 'bearish' ? 'sell' : 'hold';
      const label = comp === 'bullish' ? 'BUY' : comp === 'bearish' ? 'SELL' : 'HOLD';
      const conf = s.confidence != null ? Math.round(s.confidence * 100) + '%' : '—';
      const price = s.price != null ? fmt$(s.price) : '—';
      const sbdStr = _mSbd[s.symbol] || '';
      const dlDays = sbdStr ? Math.ceil((new Date(sbdStr+'T00:00:00') - new Date()) / 86400000) : null;
      const dlBadge = sbdStr ? `<span class="ct-deadline-badge${dlDays<=7?' soon':''}" title="Sell by ${esc(sbdStr)}">DL ${dlDays}d</span>` : '';
      const eoBadge = _mEo.has(s.symbol) ? `<span class="ct-exit-only-badge" style="font-size:9px">EO</span>` : '';
      return `<div class="sig-row">
        <span class="sig-sym" onclick="mTab('charts');mSetSym('${esc(s.symbol)}')">${esc(s.symbol)}</span>
        <span class="sig-price">${price}</span>
        <span class="sig-badge ${cls}">${label}</span>
        ${dlBadge}${eoBadge}
        <span class="sig-conf">${conf}</span>
        <button class="sig-inv-btn" onclick="mQuickInv('${esc(s.symbol)}')" title="Investigate">🔍</button>
      </div>`;
    }).join('');
  } else {
    sigEl.innerHTML = '<div class="empty-state">Waiting for scan…</div>';
  }

  // Open positions
  const positions = state.positions || [];
  const posCard = document.getElementById('m-positions-card');
  const posEl = document.getElementById('m-positions');
  if (positions.length) {
    posCard.style.display = '';
    posEl.innerHTML = positions.map(p => {
      const pnl = (p.pnl_pct || 0);
      return `<div class="pos-row">
        <span class="pos-sym">${esc(p.symbol||'')}</span>
        <span class="pos-qty">${p.quantity||''} shares · ${esc(p.account||'')}</span>
        <span class="pos-val">${fmt$(p.market_value||0)}</span>
        <span class="pos-pnl ${pnl>=0?'pill-up':'pill-dn'}">${fmtPct(pnl)}</span>
      </div>`;
    }).join('');
  } else {
    posCard.style.display = 'none';
  }

  // Pending approvals
  const approvals = Object.entries(state.pending_approvals || {});
  const apprWrap = document.getElementById('m-approvals-wrap');
  if (approvals.length) {
    apprWrap.innerHTML = approvals.map(([id, a]) => {
      const side = (a.side||'buy').toUpperCase();
      return `<div class="appr-card">
        <div class="appr-sym">${side} ${esc(a.symbol||'')}</div>
        <div class="appr-detail">${fmt$(a.dollar_amount||0)} · ${esc((a.risk_level||'medium').toUpperCase())} risk · ${esc(a.account_label||'Default')}</div>
        ${a.reasoning ? `<div class="appr-reasoning">${esc(a.reasoning.slice(0,120))}${a.reasoning.length>120?'…':''}</div>` : ''}
        <div class="appr-actions">
          <button class="appr-btn approve" onclick="mApprove('${esc(id)}')">✓ Approve</button>
          <button class="appr-btn deny" onclick="mDeny('${esc(id)}')">✗ Deny</button>
        </div>
      </div>`;
    }).join('');
    document.getElementById('nav-alerts').dataset.count = approvals.length;
  } else {
    apprWrap.innerHTML = '';
  }

  // Chart pills (watchlist)
  const wl = state.watchlist || sigs.map(s=>s.symbol);
  const pillsEl = document.getElementById('m-chart-pills');
  if (wl.length && pillsEl.children.length !== wl.length) {
    if (!_mSym) _mSym = wl[0];
    pillsEl.innerHTML = wl.map(sym =>
      `<button class="m-pill ${sym===_mSym?'active':''}" onclick="mSetSym('${esc(sym)}')">${esc(sym)}</button>`
    ).join('');
  }

  // Recent trades
  const trades = state.recent_trades || [];
  const tradeEl = document.getElementById('m-trades');
  if (trades.length) {
    tradeEl.innerHTML = trades.slice(0,8).map(t =>
      `<div class="trade-row">
        <span class="trade-side ${t.side}">${(t.side||'').toUpperCase()}</span>
        <span class="trade-sym">${esc(t.symbol||'')}</span>
        <span class="trade-val">${fmt$(t.price||0)}</span>
        <span class="trade-time">${esc(t.time||'')}</span>
      </div>`
    ).join('');
  } else {
    tradeEl.innerHTML = '<div class="empty-state">No trades yet</div>';
  }

  // Alerts
  _allAlerts = state.alert_log || [];
  const navAlerts = document.getElementById('nav-alerts');
  navAlerts.dataset.count = _allAlerts.length > 0 ? _allAlerts.length : '';
  mRenderAlerts();

  // Investigations
  mRenderInv(state.investigations || {});

  // Market session badge
  mUpdateSession(state.market_session || 'closed');
}

function mRenderPriceRail(signals) {
  const rail = document.getElementById('m-price-rail');
  if (!rail || !signals || !signals.length) return;
  const chips = signals.map(s => {
    const change = s.change_pct || 0;
    const color = change > 0 ? 'var(--bull)' : change < 0 ? 'var(--bear)' : 'var(--muted)';
    const arrow = change > 0 ? '▲' : change < 0 ? '▼' : '·';
    return `<span class="m-chip" onclick="mTab('charts');mSetSym('${esc(s.symbol)}')">`
      + `<span class="m-chip-sym">${esc(s.symbol)}</span>`
      + `<span class="m-chip-price" style="color:${color}">${fmt$(s.price)}</span>`
      + `<span style="color:${color};font-size:9px">${arrow}</span>`
      + `</span><span class="m-chip-sep">·</span>`;
  }).join('');
  // Double the chips so the loop is seamless
  rail.innerHTML = `<div class="m-rail-track">${chips}${chips}</div>`;
}

function mRenderReadiness(scorecard) {
  const wrap = document.getElementById('m-readiness-wrap');
  if (!wrap || !scorecard || !scorecard.sample_size) return;

  const s = scorecard;
  wrap.innerHTML = `
    <div class="m-readiness">
      <div style="display:flex; justify-content:space-between; align-items:center;">
        <span class="card-label" style="margin:0">Go-Live Readiness</span>
        <span class="sig-badge ${s.is_ready ? 'buy' : 'hold'}">${s.is_ready ? 'READY' : 'SCANNING'}</span>
      </div>
      <div class="m-readiness-grid">
        <div class="m-goal-box ${s.sample_size.ok ? 'ok' : ''}">
          <div class="m-goal-lbl">Trades</div>
          <div class="m-goal-val">${s.sample_size.val} / ${s.sample_size.goal}</div>
        </div>
        <div class="m-goal-box ${s.profit_factor.ok ? 'ok' : ''}">
          <div class="m-goal-lbl">Profit Factor</div>
          <div class="m-goal-val">${s.profit_factor.val}</div>
        </div>
      </div>
    </div>
  `;
}

// ── Alert filter ──────────────────────────────────────────────────────────
function mAlertClassify(a) {
  const s = (a.subject||'').toUpperCase();
  if (s.includes('KILL')||s.includes('DRAW')) return 'kill';
  if (s.includes('ERROR')) return 'error';
  if (s.includes('APPROV')) return 'approval';
  if (s.includes('INVEST')||s.includes('🟢')||s.includes('🔴')) return 'investigation';
  if (s.includes('BUY')) return 'buy';
  if (s.includes('SELL')) return 'sell';
  return '';
}
function mRenderAlerts() {
  const alertFeed = document.getElementById('m-alert-feed');
  const visible = _alertFilter === 'all' ? _allAlerts : _allAlerts.filter(a => mAlertClassify(a) === _alertFilter);
  // Build lookup: "BUY|AAPL" -> trade_id
  const approvalMap = {};
  for (const [id, info] of Object.entries((_state && _state.pending_approvals) || {})) {
    approvalMap[`${(info.action||'BUY').toUpperCase()}|${(info.symbol||'').toUpperCase()}`] = id;
  }
  if (visible.length) {
    alertFeed.innerHTML = visible.map(a => {
      const cls = mAlertClassify(a);
      const t = new Date(a.time).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
      let actionBtns = '';
      const m = (a.subject||'').match(/Approval needed:\s*(\w+)\s+(\w+)/i);
      if (m) {
        const tid = approvalMap[`${m[1].toUpperCase()}|${m[2].toUpperCase()}`];
        if (tid) {
          actionBtns = `<div class="alert-inline-actions">
            <button class="alert-inline-btn approve" onclick="mApprove('${esc(tid)}')">✓ Approve</button>
            <button class="alert-inline-btn deny" onclick="mDeny('${esc(tid)}')">✗ Deny</button>
          </div>`;
        }
      }
      return `<div class="alert-row ${cls}">
        <span class="alert-t">${t}</span>
        <div><div class="alert-subj">${esc(a.subject)}</div><div class="alert-body">${esc(a.body)}</div>${actionBtns}</div>
      </div>`;
    }).join('');
  } else {
    alertFeed.innerHTML = '<div class="empty-state">No alerts yet</div>';
  }
}
function mAlertFilter(f, btn) {
  _alertFilter = f;
  document.querySelectorAll('.filter-chip').forEach(c => c.classList.remove('active'));
  if (btn) btn.classList.add('active');
  mRenderAlerts();
}

// ── Timeframe ──────────────────────────────────────────────────────────────
function mTf(tf) {
  _mTf = tf;
  document.querySelectorAll('.tf-pill').forEach(p => p.classList.toggle('active', p.textContent === tf));
  if (_mSym) mLoadChart(_mSym);
}

// ── Approvals ──────────────────────────────────────────────────────────────
function _mDecideFeedback(id, decision) {
  document.querySelectorAll(`[onclick*="${id}"]`).forEach(btn => {
    const wrap = btn.closest('.appr-actions, .alert-inline-btns, .appr-row');
    if (wrap) {
      const label = decision === 'approve' ? '✓ Approved' : '✗ Denied';
      const cls   = decision === 'approve' ? 'color:var(--bull)' : 'color:var(--bear)';
      wrap.innerHTML = `<span style="font-size:12px;font-weight:700;${cls}">${label}</span>`;
    }
  });
}
async function mApprove(id) {
  _mDecideFeedback(id, 'approve');
  try {
    await fetch('/api/approve/'+id, {method:'POST',
      headers:window._ARGUS_TOKEN?{'X-Argus-Token':window._ARGUS_TOKEN}:{}});
  } catch(e) {}
}
async function mDeny(id) {
  _mDecideFeedback(id, 'deny');
  try {
    await fetch('/api/deny/'+id, {method:'POST',
      headers:window._ARGUS_TOKEN?{'X-Argus-Token':window._ARGUS_TOKEN}:{}});
  } catch(e) {}
}

// ── Investigate from signals ────────────────────────────────────────────────
async function mQuickInv(sym) {
  await fetch('/api/investigate', {method:'POST',headers:{'Content-Type':'application/json',
    ...(window._ARGUS_TOKEN?{'X-Argus-Token':window._ARGUS_TOKEN}:{})},
    body:JSON.stringify({symbol:sym})});
  mTab('investigate');
}

// ── Chart ──────────────────────────────────────────────────────────────────
function mSetSym(sym) {
  _mSym = sym;
  document.querySelectorAll('#m-chart-pills .m-pill').forEach(p => p.classList.toggle('active', p.textContent === sym));
  mLoadChart(sym);
}

let _mSma = null, _mEma = null;

function mLoadChart(sym) {
  const tfParam = {'1D':'day','1W':'week','1M':'month','3M':'3month'}[_mTf] || 'week';
  fetch('/api/chart/' + sym + '?span=' + tfParam)
    .then(r => r.json())
    .then(d => {
      const area = document.getElementById('m-chart-area');
      if (!_mChart) {
        const w = area.clientWidth || (window.innerWidth - 32);
        const h = area.clientHeight || Math.min(380, Math.floor(window.innerHeight * 0.45));
        _mChart = LightweightCharts.createChart(area, {
          width: w, height: h,
          layout: { background:{color:'#1c2128'}, textColor:'#8b949e' },
          grid: { vertLines:{color:'#30363d'}, horzLines:{color:'#30363d'} },
          crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
          rightPriceScale: { borderColor:'#30363d' },
          timeScale: { borderColor:'#30363d', timeVisible:true },
          handleScroll: true, handleScale: false,
        });
        _mSeries = _mChart.addCandlestickSeries({
          upColor:'#3fb950', downColor:'#f85149',
          borderUpColor:'#3fb950', borderDownColor:'#f85149',
          wickUpColor:'#3fb950', wickDownColor:'#f85149',
        });
        _mSma = _mChart.addLineSeries({ color: 'gold', lineWidth: 1, title: 'SMA 20' });
        _mEma = _mChart.addLineSeries({ color: '#58a6ff', lineWidth: 1, title: 'EMA 50' });
      }
      const candles = (d.candles||[]).sort((a,b)=>a.time-b.time);
      _mSeries.setData(candles);
      
      const smaData = candles.filter(c => c.sma_20).map(c => ({ time: c.time, value: c.sma_20 }));
      const emaData = candles.filter(c => c.ema_50).map(c => ({ time: c.time, value: c.ema_50 }));
      _mSma.setData(smaData);
      _mEma.setData(emaData);

      if (candles.length) {
        const last = candles[candles.length-1];
        document.getElementById('m-ohlc').style.display = 'flex';
        document.getElementById('mo-o').textContent = '$'+last.open.toFixed(2);
        document.getElementById('mo-h').textContent = '$'+last.high.toFixed(2);
        document.getElementById('mo-l').textContent = '$'+last.low.toFixed(2);
        document.getElementById('mo-c').textContent = '$'+last.close.toFixed(2);
      }
      // Double-RAF: first frame applies stable dimensions, second calls fitContent
      requestAnimationFrame(() => requestAnimationFrame(() => {
        if (!_mChart) return;
        const w = area.clientWidth || (window.innerWidth - 32);
        const h = area.clientHeight || Math.min(380, Math.floor(window.innerHeight * 0.45));
        _mChart.applyOptions({width: w, height: h});
        _mChart.timeScale().fitContent();
      }));
    }).catch(()=>{});
}

// ── Investigations ─────────────────────────────────────────────────────────
function mRenderInv(invs) {
  const el = document.getElementById('m-inv-cards');
  const entries = Object.entries(invs);
  if (!entries.length) { el.innerHTML = '<div class="empty-state">No active investigations</div>'; return; }
  el.innerHTML = entries.map(([sym, inv]) => {
    const statusColor = inv.status==='complete' ? 'var(--accent)' : inv.status==='error' ? 'var(--bear)' : 'var(--warn)';
    const verdictHtml = inv.verdict ? `<div class="inv-m-verdict">${esc(inv.verdict)} ${inv.confidence ? '· '+Math.round(inv.confidence*100)+'%' : ''}</div>` : '';
    const summaryHtml = inv.summary ? `<div class="inv-m-summary">${esc(inv.summary.slice(0,200))}${inv.summary.length>200?'…':''}</div>` : '';
    return `<div class="inv-m-card">
      <button class="inv-m-del" onclick="mInvDelete('${esc(sym)}')">✕</button>
      <div class="inv-m-sym">${esc(sym)}</div>
      <div class="inv-m-status" style="color:${statusColor}">${esc(inv.status||'queued')}</div>
      ${verdictHtml}${summaryHtml}
    </div>`;
  }).join('');
}

let _invDdTerm = '';
function mInvSearch(val) {
  _invDdTerm = val;
  const dd = document.getElementById('inv-m-dd');
  if (!val || val.length < 1) { dd.style.display='none'; return; }
  const _msh = window._ARGUS_TOKEN ? {'X-Argus-Token': window._ARGUS_TOKEN} : {};
  fetch('/api/search?q=' + encodeURIComponent(val), {headers: _msh})
    .then(r => r.json()).then(d => {
      if (!d.results || !d.results.length) { dd.style.display='none'; return; }
      dd.innerHTML = d.results.slice(0,5).map(r =>
        `<div class="inv-m-dd-item" onclick="mInvPick('${esc(r.symbol)}')">
          <span class="inv-m-dd-sym">${esc(r.symbol)}</span>
          <span class="inv-m-dd-name">${esc(r.name||'')}</span>
        </div>`
      ).join('');
      dd.style.display = 'block';
    }).catch(()=>{});
}
function mInvPick(sym) {
  document.getElementById('inv-m-input').value = sym;
  document.getElementById('inv-m-dd').style.display = 'none';
}
function mInvDdClose() { document.getElementById('inv-m-dd').style.display='none'; }
async function mInvStart() {
  const sym = document.getElementById('inv-m-input').value.trim().toUpperCase();
  if (!sym) return;
  await fetch('/api/investigate', {method:'POST',headers:{'Content-Type':'application/json',
    ...(window._ARGUS_TOKEN?{'X-Argus-Token':window._ARGUS_TOKEN}:{})},
    body:JSON.stringify({symbol:sym})});
  document.getElementById('inv-m-input').value = '';
}
async function mInvDelete(sym) {
  await fetch('/api/investigate/'+sym,{method:'DELETE',
    headers:window._ARGUS_TOKEN?{'X-Argus-Token':window._ARGUS_TOKEN}:{}});
}
async function mClearAlertsConfirm() {
  if (!confirm('Clear all alerts? This cannot be undone.')) return;
  await fetch('/api/alerts/clear',{method:'POST',
    headers:window._ARGUS_TOKEN?{'X-Argus-Token':window._ARGUS_TOKEN}:{}});
}

// ── SSE + connection status ────────────────────────────────────────────────
function _setConn(state) {
  const dot = document.getElementById('conn-dot');
  const lbl = document.getElementById('conn-label');
  if (dot) dot.className = state === 'online' ? '' : state === 'reconnecting' ? 'reconnecting' : 'offline';
  if (lbl) lbl.textContent = state === 'online' ? 'Live' : state === 'reconnecting' ? 'Reconnecting…' : 'Offline';
}

// ── Mobile market session badge ────────────────────────────────────────────
const _M_SESS_LABELS  = {open:'MARKET OPEN',premarket:'PRE-MARKET',afterhours:'AFTER-HOURS',closed:'CLOSED'};
const _M_SESS_CLASSES = {open:'m-sess-open',premarket:'m-sess-pre',afterhours:'m-sess-after',closed:'m-sess-closed'};
function mUpdateSession(session) {
  const el = document.getElementById('m-session-badge');
  if (!el) return;
  el.textContent = _M_SESS_LABELS[session] || (session||'closed').toUpperCase();
  el.className   = 'm-sess-badge ' + (_M_SESS_CLASSES[session] || 'm-sess-closed');
}

// ── Mobile market open/close countdown ────────────────────────────────────
function mMarketCountdown() {
  const labelEl = document.getElementById('m-mc-label');
  const valEl   = document.getElementById('m-mc-val');
  if (!labelEl || !valEl) return;
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone:'America/New_York', year:'numeric', month:'2-digit', day:'2-digit',
    hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false,
  }).formatToParts(new Date());
  const p = {};
  parts.forEach(({type,value}) => { p[type] = parseInt(value); });
  const h = p.hour % 24, min = p.minute, s = p.second;
  const dow = new Date(p.year, p.month - 1, p.day).getDay();
  const secOfDay = h * 3600 + min * 60 + s;
  const OPEN = 9 * 3600 + 30 * 60, CLOSE = 16 * 3600;
  const isWeekday = dow >= 1 && dow <= 5;
  function fmt(secs) {
    secs = Math.max(0, secs);
    const hh = Math.floor(secs / 3600), mm = Math.floor((secs % 3600) / 60), ss = secs % 60;
    return hh > 0 ? `${hh}h ${String(mm).padStart(2,'0')}m`
                  : `${String(mm).padStart(2,'0')}m ${String(ss).padStart(2,'0')}s`;
  }
  if (!isWeekday) {
    const daysUntilMon = dow === 0 ? 1 : 2;
    labelEl.textContent = 'Opens in';
    valEl.textContent   = fmt(daysUntilMon * 86400 + OPEN - secOfDay);
  } else if (secOfDay < OPEN) {
    labelEl.textContent = 'Opens in';
    valEl.textContent   = fmt(OPEN - secOfDay);
  } else if (secOfDay < CLOSE) {
    labelEl.textContent = 'Closes in';
    valEl.textContent   = fmt(CLOSE - secOfDay);
  } else {
    const daysAhead = dow === 5 ? 3 : 1;
    labelEl.textContent = 'Opens in';
    valEl.textContent   = fmt(daysAhead * 86400 + OPEN - secOfDay);
  }
}
mMarketCountdown();
setInterval(mMarketCountdown, 1000);

function connectSSE() {
  _setConn('reconnecting');
  const tok = window._ARGUS_TOKEN || '';
  const url = '/events' + (tok ? '?token=' + encodeURIComponent(tok) : '');
  const es = new EventSource(url);
  es.onopen = () => _setConn('online');
  es.onmessage = e => { try { mApply(JSON.parse(e.data)); _setConn('online'); } catch {} };
  es.onerror = () => { _setConn('reconnecting'); setTimeout(connectSSE, 5000); es.close(); };
}

// ── Pull-to-refresh ────────────────────────────────────────────────────────
(function() {
  const screen = document.getElementById('screen');
  const ptr = document.getElementById('ptr');
  let startY = 0, pulling = false;
  screen.addEventListener('touchstart', e => {
    if (screen.scrollTop === 0) { startY = e.touches[0].clientY; pulling = true; }
  }, {passive: true});
  screen.addEventListener('touchmove', e => {
    if (!pulling) return;
    const dy = e.touches[0].clientY - startY;
    if (dy > 0 && dy < 80) {
      ptr.style.height = dy + 'px';
      ptr.classList.toggle('ready', dy > 55);
    }
  }, {passive: true});
  screen.addEventListener('touchend', () => {
    if (!pulling) return;
    pulling = false;
    if (ptr.classList.contains('ready')) {
      ptr.textContent = '↻ Refreshing…';
      // Force SSE reconnect for fresh snapshot
      connectSSE();
      setTimeout(() => { ptr.style.height = '0'; ptr.textContent = '↓ Release to refresh'; ptr.classList.remove('ready'); }, 1500);
    } else {
      ptr.style.height = '0';
    }
  }, {passive: true});
})();

// ── Init ──────────────────────────────────────────────────────────────────
connectSSE();

// Resize chart when tab becomes active
window.addEventListener('resize', () => {
  if (_mChart) {
    const area = document.getElementById('m-chart-area');
    const w = area.clientWidth || (window.innerWidth - 32);
    const h = area.clientHeight || Math.min(380, Math.floor(window.innerHeight * 0.45));
    _mChart.applyOptions({width: w, height: h});
    _mChart.timeScale().fitContent();
  }
});
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse, dependencies=[Depends(_require_auth)])
async def index() -> str:
    token_script = f"<script>window._ARGUS_TOKEN={json.dumps(_dashboard_token)};</script>"
    return _HTML.replace("</head>", token_script + "\n</head>", 1)


@app.get("/m", response_class=HTMLResponse, dependencies=[Depends(_require_auth)])
async def mobile() -> str:
    token_script = f"<script>window._ARGUS_TOKEN={json.dumps(_dashboard_token)};</script>"
    return _MOBILE_HTML.replace("</head>", token_script + "\n</head>", 1)


def main(host: str = "127.0.0.1", port: int = 8000, token: str = "") -> None:
    if token:
        _configure_auth(token)
        logger.info("Dashboard API authentication enabled")
    _start_news_poller()
    uvicorn.run(app, host=host, port=port, log_level="warning")
