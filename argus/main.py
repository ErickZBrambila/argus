"""Argus autopilot — main orchestration loop."""

from __future__ import annotations

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

from argus.agent.decision import DecisionEngine, TradeDecision
from argus.broker.robinhood import RobinhoodBroker
from argus.config import get_settings
from argus.dashboard.terminal import TerminalDashboard
from argus.dashboard import web as web_dashboard
from argus.notifications.notifier import Notifier
from argus.risk.manager import RiskManager
from argus.storage.models import (
    DailyStats,
    Signal,
    Trade,
    count_day_trades_last_5_days,
    delete_position,
    get_or_create_daily_stats,
    get_session,
    init_db,
    upsert_position,
)
from argus.learning.flashcards import FlashcardStore
from argus.strategy.indicators import SignalEngine, SignalResult

logger = logging.getLogger(__name__)

_UTC = datetime.timezone.utc

# ── Market hours (NYSE / NASDAQ Eastern time) ────────────────────────────────
_MARKET_OPEN_H = 9
_MARKET_OPEN_M = 30
_MARKET_CLOSE_H = 16
_MARKET_CLOSE_M = 0


def _is_market_hours() -> bool:
    try:
        import pytz

        et = pytz.timezone("America/New_York")
        now = datetime.datetime.now(et)
        if now.weekday() >= 5:    # Saturday/Sunday
            return False
        market_open = now.replace(hour=_MARKET_OPEN_H, minute=_MARKET_OPEN_M, second=0, microsecond=0)
        market_close = now.replace(hour=_MARKET_CLOSE_H, minute=_MARKET_CLOSE_M, second=0, microsecond=0)
        return market_open <= now < market_close
    except Exception:
        return True    # fail open (let the loop run; broker will reject outside-hours orders)


_RISK_ORDER = {"low": 0, "medium": 1, "high": 2}


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

        logger.info(
            "Argus starting — mode=%s watchlist=%s",
            "PAPER" if self._cfg.paper_trade else "LIVE",
            self._cfg.watchlist,
        )

        init_db(self._cfg.database_url)

        # Shared signal engine (market data only, no account context)
        # Uses a broker instance just for price/history lookups
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
                auto_trade=False,
            ))

        # Fallback: single account mode (original behaviour)
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
        )
        self._terminal = TerminalDashboard()
        self._flashcards = FlashcardStore()
        self._recent_trades: list[dict] = []
        self._latest_signals: list[dict] = []

        for acct in self._accounts:
            logger.info(
                "Account [%s] %s — auto=%s",
                acct.label, acct.account_number or "(default)", acct.auto_trade,
            )

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def run(self) -> None:
        self._running = True
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        # Register chart data source using first account's broker
        _history_broker = self._accounts[0].broker
        web_dashboard.register_chart_source(
            lambda sym: _history_broker.get_historical_prices(sym, span="month", interval="day")
        )

        # Start web dashboard in background thread
        web_thread = threading.Thread(
            target=web_dashboard.main,
            kwargs={"host": self._cfg.web_host, "port": self._cfg.web_port},
            daemon=True,
            name="argus-web",
        )
        web_thread.start()
        logger.info("Web dashboard at http://%s:%d", self._cfg.web_host, self._cfg.web_port)

        self._terminal.start()
        try:
            for acct in self._accounts:
                try:
                    equity = acct.broker.get_portfolio_equity()
                    acct.risk.set_session_equity(equity)
                except Exception as exc:
                    logger.error("[%s] Could not fetch initial equity: %s", acct.label, exc)
            self._init_daily_stats(self._accounts[0].broker.get_portfolio_equity())
            self._update_dashboard()  # seed web dashboard before first tick

            while self._running:
                try:
                    self._check_force_close()
                    self._poll_approvals()
                    if not self._paused:
                        self._tick()
                    time.sleep(self._cfg.scan_interval_seconds)
                except Exception as exc:
                    logger.exception("Unhandled error in main loop: %s", exc)
                    time.sleep(30)
        finally:
            self._terminal.stop()
            for acct in self._accounts:
                acct.broker.logout()

    def _shutdown(self, *_) -> None:
        logger.info("Shutdown signal received — stopping Argus")
        self._running = False

    # ── Main tick ────────────────────────────────────────────────────────────

    def _tick(self) -> None:
        if not _is_market_hours():
            self._update_dashboard(trading=False)
            return

        # Compute signals once (market data is account-agnostic)
        signals = []
        signal_map: dict[str, SignalResult] = {}
        for symbol in self._cfg.watchlist:
            try:
                sig = self._strategy.compute(symbol)
                if sig is None:
                    continue
                signals.append(sig.to_dict())
                signal_map[symbol] = sig
                self._persist_signal(sig)
            except Exception as exc:
                logger.error("Signal error for %s: %s", symbol, exc)

        self._latest_signals = signals

        # Run each account
        for acct in self._accounts:
            try:
                self._tick_account(acct, signal_map)
            except Exception as exc:
                logger.error("[%s] Account tick error: %s", acct.label, exc)

        self._update_dashboard(trading=True)

    def _tick_account(self, acct: AccountContext, signal_map: dict[str, SignalResult]) -> None:
        if acct.risk.kill_switch_active:
            return

        try:
            equity = acct.broker.get_portfolio_equity()
        except Exception as exc:
            logger.error("[%s] Could not fetch equity: %s", acct.label, exc)
            return

        if acct.risk.check_drawdown(equity):
            self._notifier.send(
                f"[{acct.label.upper()}] KILL SWITCH",
                f"Daily drawdown limit hit. Equity: ${equity:,.2f}",
            )
            return

        open_positions = acct.broker.get_open_positions()

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

        for symbol, sig in signal_map.items():
            try:
                decision = self._decision.decide(
                    signal=sig,
                    portfolio_equity=equity,
                    open_positions=open_positions,
                    daily_pnl_pct=daily_pnl_pct,
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

                if decision.action == "BUY":
                    db_day_trades = count_day_trades_last_5_days(self._get_session_ref())
                    risk_check = acct.risk.approve_buy(symbol, equity, open_positions, db_day_trades)
                    if not risk_check.allowed:
                        logger.info("[%s][%s] BUY blocked: %s", acct.label, symbol, risk_check.reason)
                        continue
                    self._route_buy(acct, symbol, risk_check.dollar_amount, decision, sig, signal_obj=sig)
                    open_positions = acct.broker.get_open_positions()

                elif decision.action == "SELL" and symbol in open_positions:
                    self._execute_sell(acct, symbol, open_positions[symbol]["qty"], reason=decision.reasoning)
                    open_positions.pop(symbol, None)

            except Exception as exc:
                logger.error("[%s] Error processing %s: %s", acct.label, symbol, exc)

        self._update_positions_in_db(open_positions, acct)

    def _route_buy(
        self,
        acct: AccountContext,
        symbol: str,
        dollar_amount: float,
        decision: TradeDecision,
        sig: SignalResult,
        signal_obj: Optional[SignalResult] = None,
    ) -> None:
        """Execute immediately (auto_trade) or queue for approval."""
        needs_approval = (
            not acct.auto_trade
            and _RISK_ORDER.get(decision.risk_level, 1) >= _RISK_ORDER.get(self._cfg.approval_threshold, 1)
        )

        if not needs_approval:
            self._execute_buy(acct, symbol, dollar_amount, decision.reasoning, signal=signal_obj, decision=decision)
            return

        # Queue for dashboard approval
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
        }
        acct.pending_approvals[trade_id] = {**trade_info, "dollar_amount": dollar_amount, "_sig": sig, "_decision": decision}
        web_dashboard.queue_approval(trade_id, trade_info)
        logger.info("[%s][%s] BUY queued for approval (risk=%s) id=%s", acct.label, symbol, decision.risk_level, trade_id)
        self._notifier.send(
            f"[{acct.label.upper()}] Approval needed: BUY {symbol}",
            f"{decision.risk_level.upper()} RISK — ${dollar_amount:.2f} · conf {decision.confidence:.0%}\n{decision.reasoning[:200]}\nApprove at http://{self._cfg.web_host}:{self._cfg.web_port}",
        )

    def _poll_approvals(self) -> None:
        """Check for dashboard approval decisions and execute approved trades."""
        for acct in self._accounts:
            if acct.auto_trade:
                continue
            for trade_id in list(acct.pending_approvals):
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
                upsert_position(session, symbol, result.quantity, result.price, stop_price)
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

            # Create flashcard for learning
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

            self._recent_trades.insert(0, {
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
            logger.error("[%s] Execute buy failed for %s: %s", acct.label, symbol, exc)

    def _execute_sell(self, acct: AccountContext, symbol: str, quantity: float, reason: str = "") -> None:
        try:
            result = acct.broker.sell(symbol, quantity)
            if not result.filled:
                logger.warning("[%s][%s] Sell order not filled", acct.label, symbol)
                return

            outcome = "stop-loss" if "stop-loss" in reason.lower() else "sell"
            # Close any open flashcard for this symbol on this account
            for card in self._flashcards.get_all():
                if card.symbol == symbol and card.account == acct.label and card.exit_price is None:
                    self._flashcards.close_trade(card.trade_id, result.price, outcome)
                    break

            with get_session() as session:
                delete_position(session, symbol)
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

            self._recent_trades.insert(0, {
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
        except Exception as exc:
            logger.error("[%s] Execute sell failed for %s: %s", acct.label, symbol, exc)

    # ── Force-close (web dashboard) ──────────────────────────────────────────

    def _check_force_close(self) -> None:
        symbol = web_dashboard.get_force_close_symbol()
        if not symbol:
            return
        for acct in self._accounts:
            positions = acct.broker.get_open_positions()
            if symbol in positions:
                self._execute_sell(acct, symbol, positions[symbol]["qty"], reason="force-close via dashboard")

    # ── Dashboard state ──────────────────────────────────────────────────────

    def _update_dashboard(self, trading: bool = True) -> None:
        # Aggregate equity and positions across all accounts
        total_equity = 0.0
        total_entry_equity = 0.0
        pos_display: dict = {}
        kill_switch = False

        for acct in self._accounts:
            try:
                eq = acct.broker.get_portfolio_equity()
                total_equity += eq
                total_entry_equity += acct.risk._session_entry_equity
                kill_switch = kill_switch or acct.risk.kill_switch_active

                for sym, pos in acct.broker.get_open_positions().items():
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

        day_trades = sum(a.risk._day_trade_count for a in self._accounts)

        # Per-account breakdown for terminal dashboard
        accounts_state = {}
        for acct in self._accounts:
            try:
                eq = acct.broker.get_portfolio_equity()
            except Exception:
                eq = 0.0
            entry_eq = acct.risk._session_entry_equity
            acct_pnl = eq - entry_eq
            acct_pnl_pct = (acct_pnl / entry_eq * 100) if entry_eq else 0.0
            acct_positions = {}
            for sym, pos in acct.broker.get_open_positions().items():
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
                "kill_switch": acct.risk.kill_switch_active,
                "day_trades": acct.risk._day_trade_count,
                "auto_trade": acct.auto_trade,
                "positions": acct_positions,
                "trades": [t for t in self._recent_trades if t.get("account") == acct.label][:10],
                "pending_approvals": len(acct.pending_approvals),
            }

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
            "recent_trades": self._recent_trades[:20],
            "accounts": accounts_state,
            "flashcards": [c.as_dict() for c in recent_cards],
            "flashcard_summary": self._flashcards.summary(),
        }
        self._terminal.update(state)
        web_dashboard.push_state(state)

    # ── Storage helpers ──────────────────────────────────────────────────────

    def _get_session_ref(self):
        """Return a temporary session for read-only queries."""
        from sqlalchemy.orm import Session as _S
        from argus.storage.models import _SessionLocal
        return _SessionLocal()

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
        if acct.risk._session_entry_equity <= 0:
            return 0.0
        return (equity - acct.risk._session_entry_equity) / acct.risk._session_entry_equity * 100

    def _persist_signal(self, sig) -> None:
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
        with get_session() as session:
            for sym, pos in positions.items():
                upsert_position(
                    session, sym, pos["qty"], pos["avg_price"],
                    risk.stop_loss_price(pos["avg_price"]),
                )


# ── Entry point ──────────────────────────────────────────────────────────────

def _setup_logging() -> None:
    from argus.dashboard.log_buffer import install as _install_log_buffer
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("argus.log", mode="a"),
        ],
    )
    _install_log_buffer()


def main() -> None:
    _setup_logging()
    pilot = Autopilot()
    pilot.run()


if __name__ == "__main__":
    main()
