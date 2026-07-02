"""Argus autopilot — main orchestration loop."""

from __future__ import annotations

import collections
import datetime
import logging
import os
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from argus.agent.decision import DecisionEngine, TradeDecision, get_ai_status as _get_ai_status, get_model_info as _get_model_info
from argus.broker.robinhood import RobinhoodBroker
from argus.config import get_settings
from argus.dashboard.terminal import NullTerminalDashboard, TerminalDashboard
from argus.dashboard import web as web_dashboard
from argus.notifications.notifier import Notifier
from argus.risk.manager import RiskManager
from argus.storage.models import (
    AccountDailyStats,
    DailyStats,
    Signal,
    Trade,
    count_day_trades_last_5_days,
    delete_position,
    get_or_create_account_daily_stats,
    get_or_create_daily_stats,
    get_db_watchlist,
    get_sell_by_dates,
    get_today_day_trades,
    add_to_db_watchlist,
    get_session,
    get_reset_baseline,
    increment_day_trades,
    init_db,
    mark_account_kill_switch,
    upsert_position,
)
from argus.learning.flashcards import FlashcardStore
from argus.strategy.indicators import SignalEngine, SignalResult
from argus.engine.session import get_market_session, is_market_hours

logger = logging.getLogger(__name__)

_UTC = datetime.timezone.utc
_RISK_ORDER = {"low": 0, "medium": 1, "high": 2}

# Symbols to watch for their FIRST bullish signal — fire a high-priority ntfy alert
# when the composite flips to bullish for the first time (new listings, IPO watchlist, etc.)
_PRIORITY_WATCH_SYMBOLS: frozenset[str] = frozenset({"SPCX"})
# Force-sell this many calendar days before the sell_by_date deadline
_SELL_BY_FORCE_DAYS = 7


@dataclass
class AccountContext:
    label: str                    # "agentic" | "default"
    account_number: str
    broker: RobinhoodBroker
    risk: RiskManager
    auto_trade: bool              # False = gate medium/high risk on approval
    pending_approvals: dict = field(default_factory=dict)   # trade_id → approval info


class Autopilot:
    def __init__(self) -> None:
        self._cfg = get_settings()
        self._running = False
        self._paused = False
        self._scan_interval_override: Optional[int] = None
        self._next_scan_at: Optional[datetime.datetime] = None
        self._current_interval: int = self._cfg.scan_interval_seconds
        self._market_session: str = "closed"
        self._current_day: datetime.date = datetime.date.today()

        # Signal debouncing state: symbol -> last evaluated SignalResult
        self._last_evaluated_signals: dict[str, SignalResult] = {}
        # Decisions cache: symbol -> last TradeDecision (to reuse if signal hasn't changed)
        self._last_decisions: dict[str, TradeDecision] = {}
        self._last_signal_map: dict[str, "SignalResult"] = {}
        # Screener candidates: [{symbol, reason, category}] — refreshed daily at open
        self._screener_candidates: list[dict] = []
        self._screener_last_date: datetime.date = datetime.date.min

        logger.info(
            "Argus starting — mode=%s watchlist=%s",
            "PAPER" if self._cfg.paper_trade else "LIVE",
            self._cfg.watchlist,
        )

        init_db(self._cfg.database_url)

        try:
            from argus.storage.models import get_exit_only_symbols
            with get_session() as _s:
                _eos = list(get_exit_only_symbols(_s))
                _sbd = get_sell_by_dates(_s)
            web_dashboard._state["exit_only_symbols"] = _eos
            web_dashboard._state["sell_by_dates"] = _sbd
        except Exception:
            pass

        _shared_broker = RobinhoodBroker(
            username=self._cfg.robinhood_username,
            password=self._cfg.robinhood_password,
            mfa_secret=self._cfg.robinhood_mfa_secret,
            paper=self._cfg.paper_trade,
        )
        self._strategy = SignalEngine(_shared_broker)
        gemini_key = self._cfg.gemini_api_key.get_secret_value() or None
        self._decision = DecisionEngine(
            anthropic_key=self._cfg.anthropic_api_key.get_secret_value(),
            gemini_key=gemini_key,
        )

        def _make_broker(account_number: str) -> RobinhoodBroker:
            return RobinhoodBroker(
                username=self._cfg.robinhood_username,
                password=self._cfg.robinhood_password,
                mfa_secret=self._cfg.robinhood_mfa_secret,
                paper=self._cfg.paper_trade,
                account_number=account_number,
            )

        def _make_risk() -> RiskManager:
            return RiskManager(
                max_position_pct=self._cfg.max_position_pct,
                stop_loss_pct=self._cfg.stop_loss_pct,
                max_positions=self._cfg.max_positions,
                daily_drawdown_limit=self._cfg.daily_drawdown_limit,
            )

        self._accounts: list[AccountContext] = []

        if self._cfg.agentic_account_number:
            self._accounts.append(AccountContext(
                label="agentic",
                account_number=self._cfg.agentic_account_number,
                broker=_make_broker(self._cfg.agentic_account_number),
                risk=_make_risk(),
                auto_trade=True,
            ))

        if self._cfg.default_account_number:
            self._accounts.append(AccountContext(
                label="default",
                account_number=self._cfg.default_account_number,
                broker=_make_broker(self._cfg.default_account_number),
                risk=_make_risk(),
                auto_trade=True,
            ))

        if not self._accounts:
            self._accounts.append(AccountContext(
                label="main",
                account_number="",
                broker=_shared_broker,
                risk=_make_risk(),
                auto_trade=True,
            ))

        self._notifier = Notifier(
            notify_email=self._cfg.notify_email,
            smtp_host=self._cfg.smtp_host,
            smtp_port=self._cfg.smtp_port,
            smtp_user=self._cfg.smtp_user,
            smtp_password=self._cfg.smtp_password,
            twilio_account_sid=self._cfg.twilio_account_sid,
            twilio_auth_token=self._cfg.twilio_auth_token,
            twilio_from=self._cfg.twilio_from,
            twilio_to=self._cfg.twilio_to,
            slack_bot_token=self._cfg.slack_bot_token,
            slack_channel=self._cfg.slack_channel,
            discord_webhook_url=self._cfg.discord_webhook_url,
            ntfy_url=self._cfg.ntfy_url,
        )
        self._terminal = (
            NullTerminalDashboard()
            if os.environ.get("ARGUS_NO_TERMINAL")
            else TerminalDashboard()
        )
        self._flashcards = FlashcardStore()
        self._recent_trades: collections.deque = collections.deque(maxlen=200)
        self._latest_signals: list[dict] = []
        self._first_signal_sent: set[str] = set()  # priority symbols that already fired first-signal alert
        # Cache: label → {"equity": float, "positions": dict} — refreshed each tick
        self._account_cache: dict[str, dict] = {}
        self._last_ai_vote: dict = {}
        self._ticks_since_vote = 999

        for acct in self._accounts:
            logger.info(
                "Account [%s] %s — auto=True, large-trade approval threshold=$%.0f",
                acct.label, acct.account_number or "(default)", self._cfg.large_trade_threshold,
            )

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def run(self) -> None:
        self._running = True
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        # 1. Load watchlist from DB (persistent) or .env (fallback)
        with get_session() as session:
            db_wl = get_db_watchlist(session)
        
        if db_wl:
            logger.info("Loaded persistent watchlist from database: %s", db_wl)
            final_wl = db_wl
        else:
            final_wl = self._cfg.watchlist
            logger.info("Database watchlist empty, seeding from .env: %s", final_wl)
            with get_session() as session:
                for s in final_wl:
                    add_to_db_watchlist(session, s)

        self._position_sync_done = False  # retry in tick loop once session is ready

        web_dashboard.register_autopilot(self)
        web_dashboard.register_notifier(self._notifier)
        web_dashboard.set_watchlist_base(final_wl)
        web_dashboard.prefill_state({
            "paper_trade": self._cfg.paper_trade,
            "equity_goal": self._cfg.equity_goal,
        })

        web_dashboard.register_chart_source(
            lambda sym, span="3month", interval="day": self._strategy.get_annotated_chart(sym, span=span, interval=interval)
        )

        try:
            import robin_stocks.robinhood as _rh

            def _rh_search(q: str) -> list[dict]:
                instruments = _rh.stocks.find_instrument_data(q) or []
                seen: set[str] = set()
                results = []
                for inst in instruments:
                    sym = inst.get("symbol", "")
                    if not sym or sym in seen:
                        continue
                    seen.add(sym)
                    results.append({
                        "symbol": sym,
                        "name": inst.get("simple_name") or inst.get("name") or "",
                    })
                    if len(results) >= 6:
                        break
                return results

            web_dashboard.register_search(_rh_search)
        except Exception:
            pass

        # Register AI investigation function (Claude + Gemini ensemble)
        _anthropic_key = self._cfg.anthropic_api_key.get_secret_value()
        _gemini_key    = self._cfg.gemini_api_key.get_secret_value() if self._cfg.gemini_api_key else ""
        if _anthropic_key:
            def _make_investigate_fn(anthropic_key: str, gemini_key: str):
                import anthropic as _ant
                import json as _json, re as _re
                import concurrent.futures as _cf

                _claude_client = _ant.Anthropic(api_key=anthropic_key, timeout=45.0)
                _gemini_client = None
                if gemini_key:
                    try:
                        from google import genai as _genai
                        _gemini_client = _genai.Client(api_key=gemini_key)
                    except Exception as _e:
                        logger.warning("Gemini investigation init failed: %s", _e)

                def _build_prompt(symbol: str, signal_text: str, news_text: str) -> str:
                    return f"""You are a professional equity analyst. Investigate {symbol} and produce a concise trading verdict.

TECHNICAL SIGNALS:
{signal_text or "No signal data available."}

RECENT NEWS HEADLINES:
{news_text}

Respond ONLY with valid JSON matching this schema exactly:
{{
  "verdict": "string (e.g. 'Bullish — Buy dip', 'Bearish — Avoid', 'Neutral — Watch')",
  "confidence": float (0.0–1.0),
  "summary": "2–3 sentence plain-English analysis",
  "findings": ["key bullish/neutral observation", ...],
  "risks": ["key risk factor", ...],
  "timeframe": "string (e.g. '1–3 days', '1 week')"
}}
Be concise. findings and risks: 2–4 items each. No text outside the JSON."""

                def _parse(text: str) -> dict:
                    text = text.strip()
                    m = _re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
                    if m:
                        text = m.group(1)
                    return _json.loads(text)

                def _ask_claude(prompt: str) -> dict:
                    msg = _claude_client.messages.create(
                        model="claude-sonnet-4-6", max_tokens=700,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    return _parse(msg.content[0].text)

                def _ask_gemini(prompt: str) -> dict:
                    from google.genai import types as _gt
                    resp = _gemini_client.models.generate_content(
                        model="gemini-2.5-flash", contents=prompt,
                        config=_gt.GenerateContentConfig(
                            temperature=0.3, max_output_tokens=700,
                            thinking_config=_gt.ThinkingConfig(thinking_budget=0),
                        ),
                    )
                    return _parse(resp.text)

                def _merge(c: dict, g: dict) -> dict:
                    avg_conf = round((float(c.get("confidence", 0)) + float(g.get("confidence", 0))) / 2, 3)
                    cv, gv = c.get("verdict", ""), g.get("verdict", "")
                    cvl, gvl = cv.lower(), gv.lower()
                    both_bull    = "bull"    in cvl and "bull"    in gvl
                    both_bear    = "bear"    in cvl and "bear"    in gvl
                    both_neutral = "neutral" in cvl and "neutral" in gvl
                    if both_bull or both_bear or both_neutral:
                        verdict = cv  # consensus — no penalty
                    else:
                        verdict = f"Split — {cv} / {gv}"
                        avg_conf = round(avg_conf * 0.8, 3)  # disagreement penalty
                    # Deduplicate findings/risks
                    findings = list(dict.fromkeys(c.get("findings", []) + g.get("findings", [])))[:6]
                    risks    = list(dict.fromkeys(c.get("risks", [])    + g.get("risks", [])))[:6]
                    return {
                        "verdict":    verdict,
                        "confidence": avg_conf,
                        "summary":    f"[Claude] {c.get('summary','')}  [Gemini] {g.get('summary','')}",
                        "findings":   findings,
                        "risks":      risks,
                        "timeframe":  c.get("timeframe") or g.get("timeframe", ""),
                        "models":     "ensemble",
                    }

                def _investigate(symbol: str, signal: dict, headlines: list) -> dict:
                    if signal and isinstance(signal.get("rsi"), (int, float)):
                        signal_text = (
                            f"RSI={signal['rsi']:.1f}, "
                            f"MACD_hist={signal.get('macd_hist', 0):.4f}, "
                            f"Composite={signal.get('composite','N/A')}, "
                            f"Price=${signal.get('price',0):.2f}, "
                            f"SMA20={signal.get('sma_20','N/A')}, "
                            f"AI_decision={signal.get('ai_action','N/A')} "
                            f"(conf={signal.get('ai_confidence',0):.0%}, "
                            f"consensus={'yes' if signal.get('ai_consensus') else 'no'})"
                        )
                    else:
                        signal_text = str(signal or "")

                    clean = [
                        h.get("headline","")[:150].replace("\n"," ").replace("\r"," ")
                        for h in (headlines or [])[:8] if h.get("headline")
                    ]
                    news_text = "\n".join(f"- {h}" for h in clean) or "No recent headlines."
                    prompt = _build_prompt(symbol, signal_text, news_text)

                    if _gemini_client:
                        with _cf.ThreadPoolExecutor(max_workers=2) as ex:
                            fc = ex.submit(_ask_claude, prompt)
                            fg = ex.submit(_ask_gemini, prompt)
                            claude_r, gemini_r = None, None
                            try:
                                claude_r = fc.result(timeout=45)
                            except Exception as e:
                                logger.warning("Claude investigation failed for %s: %s", symbol, e)
                            try:
                                gemini_r = fg.result(timeout=45)
                            except Exception as e:
                                logger.warning("Gemini investigation failed for %s: %s", symbol, e)
                        if claude_r and gemini_r:
                            return _merge(claude_r, gemini_r)
                        return claude_r or gemini_r or (_ for _ in ()).throw(RuntimeError("Both models failed"))
                    return _ask_claude(prompt)

                return _investigate

            web_dashboard.register_investigate(_make_investigate_fn(_anthropic_key, _gemini_key))

        web_thread = threading.Thread(
            target=web_dashboard.main,
            kwargs={"host": self._cfg.web_host, "port": self._cfg.web_port, "token": self._cfg.dashboard_token.get_secret_value()},
            daemon=True,
            name="argus-web",
        )
        web_thread.start()
        logger.info("Web dashboard at http://%s:%d", self._cfg.web_host, self._cfg.web_port)

        # Restore persisted approvals into account queues (survive restarts)
        persisted = web_dashboard.get_persisted_approvals()
        if persisted:
            for acct in self._accounts:
                for trade_id, info in persisted.items():
                    if info.get("account_label") == acct.label and trade_id not in acct.pending_approvals:
                        acct.pending_approvals[trade_id] = info
            logger.info("Restored %d persisted approval(s) into account queues", len(persisted))

        self._terminal.start()
        try:
            first_equity = 0.0
            for acct in self._accounts:
                try:
                    equity = acct.broker.get_portfolio_equity()
                    if not first_equity:
                        first_equity = equity
                    acct.risk.set_session_equity(equity)
                except Exception as exc:
                    logger.error("[%s] Could not fetch initial equity: %s", acct.label, exc)
            self._init_daily_stats(first_equity)
            self._restore_session_state()
            self._update_dashboard()

            while self._running:
                try:
                    # Day-boundary rollover
                    today = datetime.date.today()
                    if today != self._current_day:
                        self._handle_day_rollover(today)
                        self._current_day = today

                    self._market_session = get_market_session()
                    self._check_force_close()
                    self._check_promotions()
                    self._poll_approvals()
                    if not self._paused:
                        self._tick()
                    interval = self._get_interval()
                    self._current_interval = interval
                    self._next_scan_at = datetime.datetime.now(_UTC) + datetime.timedelta(seconds=interval)
                    self._update_dashboard()
                    logger.info("Next scan in %ds [session=%s]", interval, self._market_session)
                    for _ in range(interval):
                        if not self._running:
                            break
                        time.sleep(1)
                except Exception as exc:
                    logger.exception("Unhandled error in main loop: %s", exc)
                    for _ in range(30):
                        if not self._running:
                            break
                        time.sleep(1)
        finally:
            self._terminal.stop()
            for acct in self._accounts:
                acct.broker.logout()

    def _shutdown(self, *_) -> None:
        logger.info("Shutdown signal received — stopping Argus")
        self._running = False

    def _get_interval(self) -> int:
        if self._scan_interval_override is not None:
            return self._scan_interval_override
        return {
            "open":       self._cfg.interval_open,
            "premarket":  self._cfg.interval_premarket,
            "afterhours": self._cfg.interval_afterhours,
            "closed":     self._cfg.interval_closed,
        }.get(self._market_session, self._cfg.scan_interval_seconds)

    def _sync_positions_to_watchlist(self) -> None:
        """Add live brokerage positions to watchlist so they get scanned. Runs once per session."""
        try:
            import robin_stocks.robinhood as _rh
            synced: list[str] = []
            current_wl = set(web_dashboard.get_watchlist() or self._cfg.watchlist)
            holdings: dict = _rh.account.build_holdings() or {}
            for sym in holdings:
                sym = sym.strip().upper()
                if sym and sym not in current_wl:
                    with get_session() as session:
                        add_to_db_watchlist(session, sym)
                    web_dashboard.add_to_runtime_watchlist(sym)
                    current_wl.add(sym)
                    synced.append(sym)
            for item in (_rh.crypto.get_crypto_positions() or []):
                sym = (item.get("currency", {}).get("code") or "").strip().upper()
                qty = float(item.get("quantity", 0) or 0)
                if sym and qty > 0 and sym not in current_wl:
                    with get_session() as session:
                        add_to_db_watchlist(session, sym)
                    web_dashboard.add_to_runtime_watchlist(sym)
                    current_wl.add(sym)
                    synced.append(sym)
            self._position_sync_done = True
            if synced:
                logger.info("Auto-synced open positions to watchlist: %s", synced)
        except Exception as exc:
            logger.debug("Position sync deferred: %s", exc)

    def _refresh_screener(self) -> None:
        try:
            broker = self._accounts[0].broker
            candidates = broker.get_screener_symbols()
            if candidates:
                self._screener_candidates = candidates
                syms = [c["symbol"] for c in candidates]
                logger.info("Screener updated: %d candidates %s", len(candidates), syms)
        except Exception as exc:
            logger.debug("Screener refresh failed: %s", exc)

    def set_scan_interval(self, seconds: Optional[int]) -> None:
        self._scan_interval_override = seconds
        logger.info(
            "Scan interval %s",
            f"overridden to {seconds}s" if seconds else "reset to adaptive"
        )

    # ── Main tick ────────────────────────────────────────────────────────────

    def _tick(self) -> None:
        # Always compute signals so the ticker and dashboard show live prices,
        # even outside market hours.  Trading only executes when the market is open.
        signals = []
        signal_map: dict[str, SignalResult] = {}

        def _compute_signal(symbol: str):
            sig = self._strategy.compute(symbol)
            if sig is not None:
                self._persist_signal(sig)
            return symbol, sig

        import concurrent.futures as _cf
        watchlist = web_dashboard.get_watchlist() or self._cfg.watchlist

        # Refresh screener once per day at market open
        today = datetime.date.today()
        if today != self._screener_last_date and is_market_hours():
            self._refresh_screener()
            self._screener_last_date = today

        # Scan watchlist + screener symbols (screener symbols are not persisted)
        screener_syms = [c["symbol"] for c in self._screener_candidates
                         if c["symbol"] not in set(watchlist)]
        all_syms = list(watchlist) + screener_syms
        if not all_syms:
            return

        with _cf.ThreadPoolExecutor(max_workers=min(len(all_syms), 8), thread_name_prefix="sig") as ex:
            futures = {ex.submit(_compute_signal, sym): sym for sym in all_syms}
            for fut in _cf.as_completed(futures):
                try:
                    sym, sig = fut.result()
                    if sig is not None:
                        signal_map[sym] = sig
                except Exception as exc:
                    logger.error("Signal error for %s: %s", futures[fut], exc)

        # Lazy one-time position sync — runs once after session is proven live
        if not self._position_sync_done and signal_map:
            self._sync_positions_to_watchlist()

        # Build signals list — watchlist first, then screener candidates
        screener_sym_set = {c["symbol"] for c in self._screener_candidates}
        for sym in watchlist:
            if sym in signal_map:
                signals.append(signal_map[sym].to_dict())
        for candidate in self._screener_candidates:
            sym = candidate["symbol"]
            if sym in signal_map and sym not in set(watchlist):
                d = signal_map[sym].to_dict()
                d["screener_reason"] = candidate["reason"]
                d["screener_category"] = candidate["category"]
                signals.append(d)

        # First-signal ntfy for high-conviction watch symbols (new listings, IPO pipeline)
        for _sig_d in signals:
            _sym = _sig_d.get("symbol", "")
            if (_sym in _PRIORITY_WATCH_SYMBOLS
                    and _sym not in self._first_signal_sent
                    and _sig_d.get("composite") == "bullish"):
                self._first_signal_sent.add(_sym)
                _price = _sig_d.get("price", 0)
                _conf  = _sig_d.get("confidence", 0)
                logger.info("First BULLISH signal for priority watch symbol %s @ $%.2f", _sym, _price)
                self._notifier.send(
                    f"[PRIORITY] First BULLISH signal: {_sym}",
                    f"{_sym} — first bullish composite signal detected!\n"
                    f"Price: ${_price:.2f} | Confidence: {_conf:.0%}\n"
                    f"RSI={_sig_d.get('rsi', 'N/A')}, MACD_hist={_sig_d.get('macd_hist', 'N/A')}\n"
                    f"New listing — limited history, confirm before sizing up.",
                )

        if not is_market_hours():
            self._latest_signals = signals
            self._update_dashboard(trading=False)
            return

        # Snapshot positions across ALL accounts before the loop so each account
        # can avoid buying symbols another account already holds.
        all_acct_positions: dict[str, set[str]] = {}
        for _a in self._accounts:
            try:
                all_acct_positions[_a.label] = set(_a.broker.get_open_positions().keys())
            except Exception:
                all_acct_positions[_a.label] = set()

        # Collect AI decisions across all accounts; keep highest-confidence per symbol
        ai_decisions: dict[str, TradeDecision] = {}
        for acct in self._accounts:
            try:
                # Symbols held by any OTHER account — agentic won't mirror default and vice versa
                other_held = set().union(*(pos for lbl, pos in all_acct_positions.items() if lbl != acct.label))
                acct_decisions = self._tick_account(acct, signal_map, other_held)
                for sym, dec in acct_decisions.items():
                    if sym not in ai_decisions or dec.confidence > ai_decisions[sym].confidence:
                        ai_decisions[sym] = dec
            except Exception as exc:
                logger.error("[%s] Account tick error: %s", acct.label, exc)

        # Enrich signals with best AI decision so dashboard + auto-trigger can use them
        enriched = []
        for sig_dict in signals:
            sym = sig_dict.get("symbol", "")
            dec = ai_decisions.get(sym)
            if dec:
                sig_dict = {**sig_dict,
                    "ai_action":     dec.action,
                    "ai_confidence": round(dec.confidence, 3),
                    "ai_consensus":  dec.consensus,
                    "ai_models":     dec.models_used,
                }
            enriched.append(sig_dict)
        self._latest_signals = enriched
        self._last_signal_map = signal_map

        self._update_dashboard(trading=True)

    def _should_decide(self, sig: SignalResult) -> bool:
        """Signal debouncing: skip AI decision if technicals haven't shifted significantly."""
        last = self._last_evaluated_signals.get(sig.symbol)
        if last is None:
            return True

        # Criteria for skipping (must meet ALL to skip):
        # 1. Composite signal (bullish/bearish/neutral) is identical
        # 2. RSI is within 2 points
        # 3. MACD histogram sign hasn't flipped
        # 4. Confidence hasn't changed by more than 5% absolute

        same_composite = sig.composite == last.composite
        rsi_stable     = abs((sig.rsi or 0) - (last.rsi or 0)) <= 2.0
        
        hist_now = sig.macd_hist or 0
        hist_last = last.macd_hist or 0
        macd_stable = (hist_now >= 0) == (hist_last >= 0)

        conf_stable = abs(sig.confidence - last.confidence) <= 0.05

        if same_composite and rsi_stable and macd_stable and conf_stable:
            return False
        return True

    def _tick_account(self, acct: AccountContext, signal_map: dict[str, SignalResult], other_held: set[str] | None = None) -> dict[str, TradeDecision]:
        decisions: dict[str, TradeDecision] = {}
        if acct.risk.kill_switch_active:
            return decisions

        try:
            equity = acct.broker.get_portfolio_equity()
        except Exception as exc:
            logger.error("[%s] Could not fetch equity: %s", acct.label, exc)
            return decisions

        if acct.risk.check_drawdown(equity):
            self._persist_kill_switch(acct)
            self._notifier.send(
                f"[{acct.label.upper()}] KILL SWITCH",
                f"Daily drawdown limit hit. Equity: ${equity:,.2f}",
            )
            return decisions

        open_positions = acct.broker.get_open_positions()
        # Cache so _update_dashboard can reuse without extra API calls
        self._account_cache[acct.label] = {"equity": equity, "positions": open_positions}

        # Stop-loss sweep
        for sym, pos in list(open_positions.items()):
            try:
                current_price = acct.broker.get_price(sym)
                if acct.risk.should_stop_loss(sym, pos["avg_price"], current_price):
                    self._execute_sell(acct, sym, pos["qty"], reason="stop-loss")
                    open_positions.pop(sym, None)
            except Exception as exc:
                logger.error("[%s] Stop-loss check failed for %s: %s", acct.label, sym, exc)

        daily_pnl_pct = self._daily_pnl_pct(acct, equity)

        # Load sell-time constraints once for the entire tick (avoid per-symbol DB round-trips)
        try:
            with get_session() as session:
                from argus.storage.models import get_exit_only_symbols
                _exit_only = get_exit_only_symbols(session)
                _sell_by   = get_sell_by_dates(session)
                _db_day_trades = count_day_trades_last_5_days(session, acct.label)
        except Exception as exc:
            logger.error("[%s] Could not load sell constraints: %s", acct.label, exc)
            _exit_only, _sell_by, _db_day_trades = set(), {}, 0

        for symbol, sig in signal_map.items():
            try:
                # DEBOUNCING CHECK
                if not self._should_decide(sig) and symbol in self._last_decisions:
                    decision = self._last_decisions[symbol]
                    logger.debug("[%s][%s] Signal stable; reusing last AI decision", acct.label, symbol)
                else:
                    decision = self._decision.decide(
                        signal=sig,
                        portfolio_equity=equity,
                        open_positions=open_positions,
                        daily_pnl_pct=daily_pnl_pct,
                        max_positions=self._cfg.max_positions,
                    )
                    # Don't cache flawed decisions — a model error (e.g. billing
                    # outage) would otherwise be replayed until the signal shifts
                    if decision.is_error or decision.partial_error:
                        self._last_evaluated_signals.pop(symbol, None)
                        self._last_decisions.pop(symbol, None)
                    else:
                        self._last_evaluated_signals[symbol] = sig
                        self._last_decisions[symbol] = decision

                if decision.is_error:
                    logger.critical(
                        "[%s][%s] AI decision engine failed — both models errored, holding",
                        acct.label, symbol,
                    )
                    self._notifier.send(
                        f"[{acct.label.upper()}] AI ERROR — {symbol}",
                        f"Both models failed to decide on {symbol}. Holding. Reason: {decision.reasoning[:200]}",
                    )

                logger.info(
                    "[%s][%s] %s → %s (conf %.0f%% risk=%s) | %s",
                    acct.label, symbol,
                    sig.composite.upper(),
                    decision.action,
                    decision.confidence * 100,
                    decision.risk_level,
                    decision.reasoning[:80],
                )

                # ── Deadline exit override ───────────────────────────────────
                sbd_str = _sell_by.get(symbol)
                if sbd_str:
                    sbd = datetime.date.fromisoformat(sbd_str)
                    days_left = (sbd - datetime.date.today()).days
                    if symbol in open_positions and days_left <= _SELL_BY_FORCE_DAYS:
                        logger.warning(
                            "[%s][%s] DEADLINE EXIT — forcing SELL with %d days until %s",
                            acct.label, symbol, days_left, sbd_str,
                        )
                        self._execute_sell(
                            acct, symbol, open_positions[symbol]["qty"],
                            reason=f"Deadline exit — sell by {sbd_str} ({days_left}d remaining)",
                        )
                        open_positions.pop(symbol, None)
                        decisions[symbol] = decision
                        continue
                    if decision.action == "BUY":
                        logger.info(
                            "[%s][%s] BUY blocked — sell_by_date %s (%d days)",
                            acct.label, symbol, sbd_str, days_left,
                        )
                        continue

                if decision.action == "BUY":
                    if symbol in _exit_only:
                        logger.info("[%s][%s] BUY skipped — exit-only symbol", acct.label, symbol)
                        continue
                    if other_held and symbol in other_held:
                        logger.info("[%s][%s] BUY skipped — already held by another account", acct.label, symbol)
                        continue
                    risk_check = acct.risk.approve_buy(symbol, equity, open_positions, _db_day_trades)
                    if not risk_check.allowed:
                        logger.info("[%s][%s] BUY blocked: %s", acct.label, symbol, risk_check.reason)
                        continue
                    self._route_buy(acct, symbol, risk_check.dollar_amount, decision, sig, signal_obj=sig)
                    open_positions = acct.broker.get_open_positions()

                elif decision.action == "SELL" and symbol in open_positions:
                    self._execute_sell(acct, symbol, open_positions[symbol]["qty"], reason=decision.reasoning)
                    open_positions.pop(symbol, None)

                decisions[symbol] = decision

            except Exception as exc:
                logger.error("[%s] Error processing %s: %s", acct.label, symbol, exc)

        self._update_positions_in_db(open_positions, acct)
        return decisions

    def _route_buy(
        self,
        acct: AccountContext,
        symbol: str,
        dollar_amount: float,
        decision: TradeDecision,
        sig: SignalResult,
        signal_obj: Optional[SignalResult] = None,
    ) -> None:
        needs_approval = dollar_amount > self._cfg.large_trade_threshold

        if not needs_approval:
            self._execute_buy(acct, symbol, dollar_amount, decision.reasoning, signal=signal_obj, decision=decision)
            return

        # Don't queue a duplicate if a BUY for this symbol is already pending
        already_pending = any(
            info.get("symbol") == symbol and info.get("action") == "BUY"
            for info in acct.pending_approvals.values()
        )
        if already_pending:
            logger.debug("[%s][%s] BUY already pending approval — skipping duplicate", acct.label, symbol)
            return

        trade_id = str(uuid.uuid4())
        trade_info = {
            "trade_id": trade_id,
            "symbol": symbol,
            "action": "BUY",
            "dollar_amount": dollar_amount,
            "risk_level": decision.risk_level,
            "confidence": decision.confidence,
            "reasoning": decision.reasoning,
            "account_label": acct.label,
            "account_number": acct.account_number,
            "signal": sig.composite,
            "signal_confidence": sig.confidence,
            "price_at_queue": sig.price,
        }
        acct.pending_approvals[trade_id] = {
            **trade_info,
            "_sig": sig,
            "_decision": decision,
            "queued_at": datetime.datetime.now(_UTC).isoformat(),
        }
        web_dashboard.queue_approval(trade_id, trade_info)
        logger.info("[%s][%s] BUY queued for approval (large trade $%.2f) id=%s", acct.label, symbol, dollar_amount, trade_id)
        self._notifier.send(
            f"[{acct.label.upper()}] Approval needed: BUY {symbol}",
            f"LARGE TRADE ${dollar_amount:.2f} (>{self._cfg.large_trade_threshold:.0f}) · {decision.risk_level.upper()} RISK · conf {decision.confidence:.0%}\n{decision.reasoning[:200]}\nApprove at http://{self._cfg.web_host}:{self._cfg.web_port}",
        )

    _APPROVAL_TTL_SECONDS = 1800  # 30 minutes

    _APPROVAL_PRICE_DRIFT_PCT = 0.03   # cancel BUY if price rose >3% since queued

    def _poll_approvals(self) -> None:
        now = datetime.datetime.now(_UTC)
        for acct in self._accounts:
            for trade_id in list(acct.pending_approvals):
                info = acct.pending_approvals[trade_id]
                sym = info.get("symbol", "")

                # Auto-expire stale approvals (30 min TTL)
                queued_at_str = info.get("queued_at")
                if queued_at_str:
                    try:
                        queued_at = datetime.datetime.fromisoformat(queued_at_str)
                        if (now - queued_at).total_seconds() > self._APPROVAL_TTL_SECONDS:
                            acct.pending_approvals.pop(trade_id)
                            web_dashboard.clear_approval(trade_id)
                            logger.warning("[%s] Approval %s expired (>30 min): %s", acct.label, trade_id, sym)
                            continue
                    except Exception:
                        pass

                # Auto-expire if price has drifted too far from when trade was queued
                price_at_queue = info.get("price_at_queue")
                if price_at_queue and price_at_queue > 0 and sym in self._last_signal_map:
                    current_price = self._last_signal_map[sym].price
                    drift = (current_price - price_at_queue) / price_at_queue
                    action = info.get("action", "BUY")
                    # BUY: cancel if price rose too much (worse entry)
                    # SELL: cancel if price fell too much (worse exit)
                    if (action == "BUY" and drift > self._APPROVAL_PRICE_DRIFT_PCT) or \
                       (action == "SELL" and drift < -self._APPROVAL_PRICE_DRIFT_PCT):
                        acct.pending_approvals.pop(trade_id)
                        web_dashboard.clear_approval(trade_id)
                        logger.warning(
                            "[%s] Approval %s cancelled — price drifted %.1f%% (queued=%.2f now=%.2f): %s",
                            acct.label, trade_id, drift * 100, price_at_queue, current_price, sym,
                        )
                        self._notifier.send(
                            f"[{acct.label.upper()}] Approval cancelled: {action} {sym}",
                            f"Price moved {drift:+.1%} since queued (${price_at_queue:.2f} → ${current_price:.2f}). Re-run scan to get fresh signal.",
                        )
                        continue

                decision = web_dashboard.get_approval_decision(trade_id)
                if decision is None:
                    continue
                info = acct.pending_approvals.pop(trade_id)
                web_dashboard.clear_approval(trade_id)
                if decision == "approved":
                    logger.info("[%s] Executing approved trade %s: BUY %s", acct.label, trade_id, info["symbol"])
                    self._execute_buy(
                        acct, info["symbol"], info["dollar_amount"], info["reasoning"],
                        signal=info.get("_sig"), decision=info.get("_decision"),
                    )
                else:
                    logger.info("[%s] Trade %s denied by user", acct.label, trade_id)

    # ── Trade execution ──────────────────────────────────────────────────────

    def _execute_buy(
        self,
        acct: AccountContext,
        symbol: str,
        dollar_amount: float,
        reasoning: str,
        signal: Optional[SignalResult] = None,
        decision: Optional[TradeDecision] = None,
    ) -> None:
        try:
            result = acct.broker.buy(symbol, dollar_amount)
            if not result.filled:
                logger.warning("[%s][%s] Buy order not filled", acct.label, symbol)
                return

            trade_id = result.order_id
            stop_price = acct.risk.stop_loss_price(result.price)
            with get_session() as session:
                upsert_position(session, symbol, result.quantity, result.price, stop_price, acct.label)
                session.add(Trade(
                    symbol=symbol,
                    side="buy",
                    quantity=result.quantity,
                    price=result.price,
                    total_value=result.quantity * result.price,
                    paper=result.paper,
                    order_id=trade_id,
                    reasoning=reasoning[:4000],
                ))
                self._increment_trade_count(session)

            if signal is not None and decision is not None:
                self._flashcards.record_trade(
                    trade_id=trade_id,
                    symbol=symbol,
                    action="BUY",
                    account=acct.label,
                    signal=signal,
                    decision=decision,
                    entry_price=result.price,
                    dollar_amount=dollar_amount,
                )

            self._recent_trades.appendleft({
                "time": datetime.datetime.now(_UTC).strftime("%H:%M:%S"),
                "symbol": symbol,
                "side": "buy",
                "quantity": result.quantity,
                "price": result.price,
                "account": acct.label,
            })
            self._notifier.send(
                f"[{acct.label.upper()}] BUY {symbol}",
                f"Bought {result.quantity:.4f} {symbol} @ ${result.price:.4f} (${result.quantity * result.price:.2f})",
            )
        except Exception as exc:
            logger.critical(
                "[%s] BROKER BUY MAY HAVE FILLED BUT DB WRITE FAILED for %s: %s — check brokerage positions!",
                acct.label, symbol, exc,
            )
            self._notifier.send(
                f"[{acct.label.upper()}] CRITICAL: Buy DB write failed — {symbol}",
                f"Broker order may have filled but local records were not saved. "
                f"Check your brokerage account for {symbol} position. Error: {str(exc)[:200]}",
            )

    def _execute_sell(self, acct: AccountContext, symbol: str, quantity: float, reason: str = "") -> bool:
        """Returns True if the sell was successfully filled."""
        # ── Step 1: broker order ─────────────────────────────────────────────
        try:
            result = acct.broker.sell(symbol, quantity)
        except Exception as exc:
            logger.error("[%s] Execute sell broker call failed for %s: %s", acct.label, symbol, exc)
            return False

        if not result.filled:
            logger.warning("[%s][%s] Sell order not filled", acct.label, symbol)
            return False

        # ── Step 2: flashcard close ──────────────────────────────────────────
        outcome = "stop-loss" if "stop-loss" in reason.lower() else "sell"
        for card in self._flashcards.get_all():
            if card.symbol == symbol and card.account == acct.label and card.exit_price is None:
                self._flashcards.close_trade(card.trade_id, result.price, outcome)
                break

        # ── Step 3: DB write (broker already filled — critical if this fails) ─
        try:
            with get_session() as session:
                delete_position(session, symbol, acct.label)
                session.add(Trade(
                    symbol=symbol,
                    side="sell",
                    quantity=result.quantity,
                    price=result.price,
                    total_value=result.quantity * result.price,
                    paper=result.paper,
                    order_id=result.order_id,
                    reasoning=reason[:4000],
                ))
                self._increment_trade_count(session)
                # Detect day trade: same symbol bought today on this account
                if self._is_day_trade(acct, symbol):
                    acct.risk.record_day_trade()
                    increment_day_trades(session, acct.label)
        except Exception as exc:
            logger.critical(
                "[%s] BROKER SELL MAY HAVE FILLED BUT DB WRITE FAILED for %s: %s — check brokerage positions!",
                acct.label, symbol, exc,
            )
            self._notifier.send(
                f"[{acct.label.upper()}] CRITICAL: Sell DB write failed — {symbol}",
                f"Broker sell order may have filled but local records were not updated. "
                f"Check your brokerage account for {symbol} position. Error: {str(exc)[:200]}",
            )
            return True  # sell did fill at the broker; caller should treat as success

        # ── Step 4: in-memory + notification ────────────────────────────────
        self._recent_trades.appendleft({
            "time": datetime.datetime.now(_UTC).strftime("%H:%M:%S"),
            "symbol": symbol,
            "side": "sell",
            "quantity": result.quantity,
            "price": result.price,
            "account": acct.label,
        })
        self._notifier.send(
            f"[{acct.label.upper()}] SELL {symbol}",
            f"Sold {result.quantity:.4f} {symbol} @ ${result.price:.4f} | {reason[:100]}",
        )
        return True

    def _is_day_trade(self, acct: "AccountContext", symbol: str) -> bool:
        """True if this account bought symbol today — making the same-day sell a day trade."""
        today_start = datetime.datetime.combine(datetime.date.today(), datetime.time.min, tzinfo=_UTC)
        try:
            with get_session() as session:
                count = (
                    session.query(Trade)
                    .filter(
                        Trade.symbol == symbol,
                        Trade.side == "buy",
                        Trade.paper == acct.broker.paper,
                        Trade.created_at >= today_start,
                    )
                    .count()
                )
                return count > 0
        except Exception:
            return False

    # ── Force-close (web dashboard) ──────────────────────────────────────────

    def _check_force_close(self) -> None:
        symbol = web_dashboard.get_force_close_symbol()
        if not symbol:
            return
        for acct in self._accounts:
            positions = acct.broker.get_open_positions()
            if symbol in positions:
                self._execute_sell(acct, symbol, positions[symbol]["qty"], reason="force-close via dashboard")

    def _check_promotions(self) -> None:
        """Sell from Default account and re-buy on Agentic (promote position)."""
        req = web_dashboard.get_promote_request()
        if not req:
            return
        symbol     = req["symbol"]
        from_label = req.get("from_account", "default")
        to_label   = req.get("to_account",   "agentic")

        # Validate known account labels
        valid_labels = {a.label for a in self._accounts}
        if from_label not in valid_labels or to_label not in valid_labels:
            logger.warning("Promote: invalid account labels %s → %s", from_label, to_label)
            return

        from_acct = next((a for a in self._accounts if a.label == from_label), None)
        to_acct   = next((a for a in self._accounts if a.label == to_label),   None)
        if not from_acct or not to_acct:
            return

        positions = from_acct.broker.get_open_positions()
        if symbol not in positions:
            logger.warning("Promote: %s not found in %s positions", symbol, from_label)
            return

        pos          = positions[symbol]
        price        = from_acct.broker.get_price(symbol)
        qty          = pos["qty"]
        dollar_value = qty * price

        logger.info("Promoting %s from %s → %s ($%.2f)", symbol, from_label, to_label, dollar_value)

        # Step 1: sell on source account
        sell_ok = self._execute_sell(from_acct, symbol, qty, reason=f"promote to {to_label}")
        if not sell_ok:
            logger.error("Promote: sell of %s on %s failed — aborting re-buy", symbol, from_label)
            return

        # Step 2: run risk checks on the target account before buying
        try:
            to_equity    = to_acct.broker.get_portfolio_equity()
            to_positions = to_acct.broker.get_open_positions()
        except Exception as exc:
            logger.critical(
                "PROMOTE INCOMPLETE: sold %s on %s but cannot reach %s to re-buy — check positions! (%s)",
                symbol, from_label, to_label, exc,
            )
            self._notifier.send(
                "[PROMOTE] Incomplete — action required",
                f"Sold {symbol} on {from_label} but could not fetch {to_label} state. Re-buy manually.",
            )
            return

        with get_session() as session:
            db_day_trades = count_day_trades_last_5_days(session, to_acct.label)
        risk_check = to_acct.risk.approve_buy(symbol, to_equity, to_positions, db_day_trades)
        if not risk_check.allowed:
            logger.critical(
                "PROMOTE INCOMPLETE: sold %s on %s but re-buy on %s blocked — %s",
                symbol, from_label, to_label, risk_check.reason,
            )
            self._notifier.send(
                "[PROMOTE] Re-buy blocked — action required",
                f"Sold {symbol} on {from_label}. Re-buy on {to_label} blocked: {risk_check.reason}. Re-buy manually.",
            )
            return

        # Step 3: re-buy on target account — cap at what risk check approved
        capped_amount = min(dollar_value, risk_check.dollar_amount)
        self._execute_buy(to_acct, symbol, capped_amount, f"promoted from {from_label}")

    # ── Dashboard state ──────────────────────────────────────────────────────

    def _update_dashboard(self, trading: bool = True) -> None:
        total_equity = 0.0
        total_entry_equity = 0.0
        pos_display: dict = {}
        kill_switch = False

        for acct in self._accounts:
            try:
                cached = self._account_cache.get(acct.label, {})
                eq = cached["equity"] if "equity" in cached else acct.broker.get_portfolio_equity()
                positions_raw = cached.get("positions") or acct.broker.get_open_positions()
                total_equity += eq
                total_entry_equity += acct.risk.session_entry_equity
                kill_switch = kill_switch or acct.risk.kill_switch_active

                for sym, pos in positions_raw.items():
                    try:
                        cur = acct.broker.get_price(sym)
                        pnl_pct = (cur - pos["avg_price"]) / pos["avg_price"] * 100 if pos["avg_price"] else 0
                        key = f"{sym} [{acct.label}]"
                        pos_display[key] = {
                            "quantity": pos["qty"],
                            "entry_price": pos["avg_price"],
                            "current_price": cur,
                            "stop_loss_price": acct.risk.stop_loss_price(pos["avg_price"]),
                            "unrealized_pnl": (cur - pos["avg_price"]) * pos["qty"],
                            "unrealized_pnl_pct": pnl_pct,
                            "account": acct.label,
                        }
                    except Exception:
                        pass
            except Exception:
                pass

        daily_pnl = total_equity - total_entry_equity
        daily_pnl_pct = (daily_pnl / total_entry_equity * 100) if total_entry_equity else 0.0

        day_trades = sum(a.risk.day_trade_count for a in self._accounts)

        accounts_state = {}
        for acct in self._accounts:
            cached = self._account_cache.get(acct.label, {})
            try:
                eq = cached["equity"] if "equity" in cached else acct.broker.get_portfolio_equity()
            except Exception:
                eq = 0.0
            entry_eq = acct.risk.session_entry_equity
            acct_pnl = eq - entry_eq
            acct_pnl_pct = (acct_pnl / entry_eq * 100) if entry_eq else 0.0

            try:
                with next(get_session()) as _sess:
                    reset_baseline = get_reset_baseline(_sess, acct.label)
            except Exception:
                reset_baseline = entry_eq
            since_reset_pnl = eq - reset_baseline if reset_baseline else 0.0
            since_reset_pnl_pct = (since_reset_pnl / reset_baseline * 100) if reset_baseline else 0.0

            acct_positions = {}
            positions_raw = cached.get("positions") or acct.broker.get_open_positions()
            for sym, pos in positions_raw.items():
                try:
                    cur = acct.broker.get_price(sym)
                    pnl_pct = (cur - pos["avg_price"]) / pos["avg_price"] * 100 if pos["avg_price"] else 0
                    acct_positions[sym] = {
                        "quantity": pos["qty"],
                        "entry_price": pos["avg_price"],
                        "current_price": cur,
                        "stop_loss_price": acct.risk.stop_loss_price(pos["avg_price"]),
                        "unrealized_pnl_pct": pnl_pct,
                    }
                except Exception:
                    pass
            accounts_state[acct.label] = {
                "equity": eq,
                "daily_pnl": acct_pnl,
                "daily_pnl_pct": acct_pnl_pct,
                "since_reset_pnl": since_reset_pnl,
                "since_reset_pnl_pct": since_reset_pnl_pct,
                "reset_baseline": reset_baseline,
                "kill_switch": acct.risk.kill_switch_active,
                "day_trades": acct.risk.day_trade_count,
                "auto_trade": acct.auto_trade,
                "positions": acct_positions,
                "trades": [t for t in list(self._recent_trades) if t.get("account") == acct.label][:10],
                "pending_approvals": len(acct.pending_approvals),
                "equity_goal": self._cfg.equity_goal,
            }

        from argus.dashboard.token_tracker import get_summary as _token_summary
        tokens = _token_summary()
        lifetime_cost = tokens.get("lifetime_cost_usd", 0.0)
        
        perf = self._flashcards.performance()
        scorecard = self._flashcards.readiness_scorecard(lifetime_cost=lifetime_cost)
        
        # Periodic AI Vote (every 200 ticks or if we just became statistically ready)
        if (self._ticks_since_vote >= 200) or (scorecard["is_ready"] and not self._last_ai_vote.get("agreed")):
            self._last_ai_vote = self._decision.go_live_vote(perf)
            self._ticks_since_vote = 0
        else:
            self._ticks_since_vote += 1

        recent_cards = self._flashcards.get_recent(20)
        state = {
            "paper_trade": self._cfg.paper_trade,
            "kill_switch": kill_switch,
            "paused": web_dashboard.is_paused(),
            "equity": total_equity,
            "daily_pnl": daily_pnl,
            "daily_pnl_pct": daily_pnl_pct,
            "trade_count": self._today_trade_count(),
            "day_trades": day_trades,
            "positions": pos_display,
            "signals": self._latest_signals[-20:],
            "recent_trades": list(self._recent_trades)[:20],
            "accounts": accounts_state,
            "flashcards": [c.as_dict() for c in recent_cards],
            "flashcard_summary": self._flashcards.summary(),
            "market_session":    self._market_session,
            "scan_interval":     self._current_interval,
            "interval_override": self._scan_interval_override,
            "next_scan_at":      self._next_scan_at.isoformat() if self._next_scan_at else None,
            "token_usage":       tokens,
            "equity_goal":       self._cfg.equity_goal,
            "monthly_api_budget":    self._cfg.monthly_api_budget,
            "large_trade_threshold": self._cfg.large_trade_threshold,
            "performance":       perf,
            "readiness_scorecard": scorecard,
            "ai_vote":           self._last_ai_vote,
            "ai_status":         _get_ai_status(),
            "ai_models":         _get_model_info(),
            "watchlist":         web_dashboard.get_watchlist(),
            "screener":          self._screener_candidates,
        }
        self._terminal.update(state)
        web_dashboard.push_state(state)

    # ── Storage helpers ──────────────────────────────────────────────────────

    def _restore_session_state(self) -> None:
        """Load per-account starting equity, kill switch, and day trade count from today's DB rows."""
        today = datetime.date.today()
        restored_ks: list[str] = []
        with get_session() as session:
            for acct in self._accounts:
                row = get_or_create_account_daily_stats(
                    session, today, acct.label, acct.risk.session_entry_equity
                )
                if row.starting_equity > 0:
                    acct.risk.set_session_equity(row.starting_equity)
                # Explicitly sync kill switch with today's DB state so a stale
                # in-memory True from the previous day is cleared on day rollover.
                if row.kill_switch_triggered:
                    acct.risk._kill_switch = True
                    restored_ks.append(acct.label)
                else:
                    acct.risk._kill_switch = False
                # Seed today's per-account day trade count from DB so restarts don't
                # under-count and mistakenly allow buys that would breach PDT.
                today_dt = get_today_day_trades(session, acct.label)
                if today_dt > acct.risk._day_trade_count:
                    acct.risk._day_trade_count = today_dt
        if restored_ks:
            logger.warning(
                "Kill switch restored for [%s] — drawdown limit was exceeded earlier today",
                ", ".join(restored_ks),
            )

    def _persist_kill_switch(self, acct: AccountContext) -> None:
        try:
            with get_session() as session:
                mark_account_kill_switch(session, datetime.date.today(), acct.label)
        except Exception as exc:
            logger.warning("Could not persist kill switch for [%s]: %s", acct.label, exc)

    def _handle_day_rollover(self, new_day: datetime.date) -> None:
        logger.info("Day rollover — resetting session state for %s", new_day)
        for acct in self._accounts:
            acct.risk.reset_day_trade_count()
            try:
                equity = acct.broker.get_portfolio_equity()
                acct.risk.set_session_equity(equity)
            except Exception as exc:
                logger.error("[%s] Could not fetch equity for day rollover: %s", acct.label, exc)
        # Init daily stats for new day using first account's equity as reference
        first_equity = self._accounts[0].risk.session_entry_equity if self._accounts else 0.0
        self._init_daily_stats(first_equity)
        self._restore_session_state()

    def _init_daily_stats(self, equity: float) -> None:
        today = datetime.date.today()
        with get_session() as session:
            get_or_create_daily_stats(session, today, equity)

    def _increment_trade_count(self, session) -> None:
        today = datetime.date.today()
        stats = session.query(DailyStats).filter_by(date=today).first()
        if stats:
            stats.trade_count = (stats.trade_count or 0) + 1

    def _today_trade_count(self) -> int:
        try:
            with get_session() as session:
                today = datetime.date.today()
                stats = session.query(DailyStats).filter_by(date=today).first()
                return stats.trade_count if stats else 0
        except Exception:
            return 0

    def _daily_pnl_pct(self, acct: AccountContext, equity: float) -> float:
        if acct.risk.session_entry_equity <= 0:
            return 0.0
        return (equity - acct.risk.session_entry_equity) / acct.risk.session_entry_equity * 100

    def _persist_signal(self, sig: SignalResult) -> None:
        try:
            with get_session() as session:
                session.add(Signal(
                    symbol=sig.symbol,
                    rsi=sig.rsi,
                    macd=sig.macd,
                    macd_signal=sig.macd_signal,
                    macd_hist=sig.macd_hist,
                    bb_upper=sig.bb_upper,
                    bb_mid=sig.bb_mid,
                    bb_lower=sig.bb_lower,
                    sma_20=sig.sma_20,
                    ema_50=sig.ema_50,
                    price=sig.price,
                    volume=sig.volume,
                    composite_signal=sig.composite,
                ))
        except Exception as exc:
            logger.debug("Signal persist failed: %s", exc)

    def _update_positions_in_db(self, positions: dict, acct: Optional[AccountContext] = None) -> None:
        risk = acct.risk if acct else self._accounts[0].risk
        label = acct.label if acct else "main"
        with get_session() as session:
            for sym, pos in positions.items():
                upsert_position(
                    session, sym, pos["qty"], pos["avg_price"],
                    risk.stop_loss_price(pos["avg_price"]),
                    label,
                )
