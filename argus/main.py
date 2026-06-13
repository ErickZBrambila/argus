"""Argus autopilot — main orchestration loop."""

from __future__ import annotations

import datetime
import logging
import os
import signal
import sys
import threading
import time
from typing import Optional

from argus.agent.decision import DecisionEngine
from argus.broker.robinhood import RobinhoodBroker
from argus.config import get_settings
from argus.dashboard.terminal import TerminalDashboard
from argus.dashboard import server as web_dashboard
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
from argus.strategy.indicators import SignalEngine

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

        self._broker = RobinhoodBroker(
            username=self._cfg.robinhood_username,
            password=self._cfg.robinhood_password,
            mfa_secret=self._cfg.robinhood_mfa_secret,
            paper=self._cfg.paper_trade,
        )
        self._strategy = SignalEngine(self._broker)
        self._decision = DecisionEngine(self._cfg.anthropic_api_key.get_secret_value())
        self._risk = RiskManager(
            max_position_pct=self._cfg.max_position_pct,
            stop_loss_pct=self._cfg.stop_loss_pct,
            max_positions=self._cfg.max_positions,
            daily_drawdown_limit=self._cfg.daily_drawdown_limit,
        )
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
        self._recent_trades: list[dict] = []
        self._latest_signals: list[dict] = []

        logger.info(
            "RiskManager: max_pos_pct=%.2f stop_loss=%.2f drawdown_limit=%.2f max_positions=%d",
            self._cfg.max_position_pct,
            self._cfg.stop_loss_pct,
            self._cfg.daily_drawdown_limit,
            self._cfg.max_positions,
        )

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def run(self) -> None:
        self._running = True
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        # Wire live objects into dashboard state for richer endpoints
        web_dashboard.build_app(risk=self._risk, broker=self._broker)

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
            equity = self._broker.get_portfolio_equity()
            self._risk.set_session_equity(equity)
            self._init_daily_stats(equity)

            while self._running:
                try:
                    self._check_force_close()
                    if not self._paused:
                        self._tick()
                    time.sleep(self._cfg.scan_interval_seconds)
                except Exception as exc:
                    logger.exception("Unhandled error in main loop: %s", exc)
                    time.sleep(30)
        finally:
            self._terminal.stop()
            self._broker.logout()

    def _shutdown(self, *_) -> None:
        logger.info("Shutdown signal received — stopping Argus")
        self._running = False

    # ── Main tick ────────────────────────────────────────────────────────────

    def _tick(self) -> None:
        if not _is_market_hours():
            self._update_dashboard(trading=False)
            return

        if self._risk.kill_switch_active:
            self._update_dashboard(trading=False)
            return

        equity = self._broker.get_portfolio_equity()
        if self._risk.check_drawdown(equity):
            self._notifier.send(
                "KILL SWITCH TRIGGERED",
                f"Daily drawdown limit hit. Equity: ${equity:,.2f}",
            )
            self._update_dashboard(trading=False)
            return

        open_positions = self._broker.get_open_positions()

        # Stop-loss sweep
        for sym, pos in list(open_positions.items()):
            try:
                current_price = self._broker.get_price(sym)
                if self._risk.should_stop_loss(sym, pos["avg_price"], current_price):
                    self._execute_sell(sym, pos["qty"], reason="stop-loss")
                    open_positions.pop(sym, None)
            except Exception as exc:
                logger.error("Stop-loss check failed for %s: %s", sym, exc)

        # Check for force-close requests from web dashboard
        self._check_force_close()

        # Scan watchlist
        signals = []
        daily_pnl_pct = self._daily_pnl_pct(equity)

        for symbol in self._cfg.watchlist:
            try:
                signal_result = self._strategy.compute(symbol)
                if signal_result is None:
                    continue

                signals.append(signal_result.to_dict())
                self._persist_signal(signal_result)

                decision = self._decision.decide(
                    signal=signal_result,
                    portfolio_equity=equity,
                    open_positions=open_positions,
                    daily_pnl_pct=daily_pnl_pct,
                )

                logger.info(
                    "[%s] %s → %s (conf %.0f%%) | %s",
                    symbol,
                    signal_result.composite.upper(),
                    decision.action,
                    decision.confidence * 100,
                    decision.reasoning[:80],
                )

                if decision.action == "BUY":
                    db_day_trades = count_day_trades_last_5_days(self._get_session_ref())
                    risk = self._risk.approve_buy(symbol, equity, open_positions, db_day_trades)
                    if risk.allowed:
                        self._execute_buy(symbol, risk.dollar_amount, decision.reasoning)
                        open_positions = self._broker.get_open_positions()
                    else:
                        logger.info("[%s] BUY blocked: %s", symbol, risk.reason)

                elif decision.action == "SELL" and symbol in open_positions:
                    self._execute_sell(symbol, open_positions[symbol]["qty"], reason=decision.reasoning)
                    open_positions.pop(symbol, None)

            except Exception as exc:
                logger.error("Error processing %s: %s", symbol, exc)

        self._latest_signals = signals
        self._update_positions_in_db(open_positions)
        self._update_dashboard(equity=equity, trading=True, positions=open_positions)

    # ── Trade execution ──────────────────────────────────────────────────────

    def _execute_buy(self, symbol: str, dollar_amount: float, reasoning: str) -> None:
        try:
            result = self._broker.buy(symbol, dollar_amount)
            if not result.filled:
                logger.warning("[%s] Buy order not filled", symbol)
                return

            stop_price = self._risk.stop_loss_price(result.price)
            with get_session() as session:
                upsert_position(session, symbol, result.quantity, result.price, stop_price)
                session.add(Trade(
                    symbol=symbol,
                    side="buy",
                    quantity=result.quantity,
                    price=result.price,
                    total_value=result.quantity * result.price,
                    paper=result.paper,
                    order_id=result.order_id,
                    reasoning=reasoning[:4000],
                ))
                self._increment_trade_count(session)

            self._recent_trades.insert(0, {
                "time": datetime.datetime.now(_UTC).strftime("%H:%M:%S"),
                "symbol": symbol,
                "side": "buy",
                "quantity": result.quantity,
                "price": result.price,
            })
            self._notifier.send(
                f"BUY {symbol}",
                f"Bought {result.quantity:.4f} {symbol} @ ${result.price:.4f} (${result.quantity * result.price:.2f})",
            )
        except Exception as exc:
            logger.error("Execute buy failed for %s: %s", symbol, exc)

    def _execute_sell(self, symbol: str, quantity: float, reason: str = "") -> None:
        try:
            result = self._broker.sell(symbol, quantity)
            if not result.filled:
                logger.warning("[%s] Sell order not filled", symbol)
                return

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
            })
            self._notifier.send(
                f"SELL {symbol}",
                f"Sold {result.quantity:.4f} {symbol} @ ${result.price:.4f} | {reason[:100]}",
            )
        except Exception as exc:
            logger.error("Execute sell failed for %s: %s", symbol, exc)

    # ── Force-close (web dashboard) ──────────────────────────────────────────

    def _check_force_close(self) -> None:
        symbol = web_dashboard.get_force_close_symbol()
        if symbol:
            positions = self._broker.get_open_positions()
            if symbol in positions:
                self._execute_sell(symbol, positions[symbol]["qty"], reason="force-close via dashboard")

    # ── Dashboard state ──────────────────────────────────────────────────────

    def _update_dashboard(
        self,
        equity: float = 0.0,
        trading: bool = True,
        positions: Optional[dict] = None,
    ) -> None:
        if equity == 0.0:
            try:
                equity = self._broker.get_portfolio_equity()
            except Exception:
                pass

        daily_pnl = equity - self._risk._session_entry_equity
        daily_pnl_pct = (daily_pnl / self._risk._session_entry_equity * 100) if self._risk._session_entry_equity else 0.0

        pos_display: dict = {}
        if positions:
            for sym, pos in positions.items():
                try:
                    cur = self._broker.get_price(sym)
                    pnl_pct = (cur - pos["avg_price"]) / pos["avg_price"] * 100 if pos["avg_price"] else 0
                    pos_display[sym] = {
                        "quantity": pos["qty"],
                        "entry_price": pos["avg_price"],
                        "current_price": cur,
                        "stop_loss_price": self._risk.stop_loss_price(pos["avg_price"]),
                        "unrealized_pnl": (cur - pos["avg_price"]) * pos["qty"],
                        "unrealized_pnl_pct": pnl_pct,
                    }
                except Exception:
                    pass

        state = {
            "paper_trade": self._cfg.paper_trade,
            "kill_switch": self._risk.kill_switch_active,
            "paused": web_dashboard.is_paused(),
            "equity": equity,
            "daily_pnl": daily_pnl,
            "daily_pnl_pct": daily_pnl_pct,
            "trade_count": self._today_trade_count(),
            "day_trades": self._risk._day_trade_count,
            "positions": pos_display,
            "signals": self._latest_signals[-20:],
            "recent_trades": self._recent_trades[:20],
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

    def _daily_pnl_pct(self, equity: float) -> float:
        if self._risk._session_entry_equity <= 0:
            return 0.0
        return (equity - self._risk._session_entry_equity) / self._risk._session_entry_equity * 100

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

    def _update_positions_in_db(self, positions: dict) -> None:
        with get_session() as session:
            for sym, pos in positions.items():
                upsert_position(
                    session, sym, pos["qty"], pos["avg_price"],
                    self._risk.stop_loss_price(pos["avg_price"]),
                )


# ── Entry point ──────────────────────────────────────────────────────────────

def _setup_logging() -> None:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("argus.log", mode="a"),
        ],
    )


def main() -> None:
    _setup_logging()
    pilot = Autopilot()
    pilot.run()


if __name__ == "__main__":
    main()
