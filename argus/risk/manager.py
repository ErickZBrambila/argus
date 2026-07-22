"""Risk manager: position sizing, stop-loss, PDT tracking, drawdown kill switch."""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# PDT limit: no more than 3 day trades in a rolling 5-business-day window
PDT_LIMIT = 3


@dataclass
class RiskDecision:
    allowed: bool
    reason: str
    dollar_amount: float = 0.0


class RiskManager:
    def __init__(
        self,
        max_position_pct: float = 0.10,
        stop_loss_pct: float = 0.05,
        max_positions: int = 5,
        daily_drawdown_limit: float = -0.05,
        pdt_aware: bool = True,
        max_position_loss_usd: float = 75.0,
    ) -> None:
        self.max_position_pct = max_position_pct
        self.stop_loss_pct = stop_loss_pct
        self.max_positions = max_positions
        self.daily_drawdown_limit = daily_drawdown_limit
        self.pdt_aware = pdt_aware
        self.max_position_loss_usd = max_position_loss_usd

        self._kill_switch: bool = False
        self._day_trade_count: int = 0
        self._session_entry_equity: float = 0.0

    # ── Kill switch ──────────────────────────────────────────────────────────

    @property
    def kill_switch_active(self) -> bool:
        return self._kill_switch

    @property
    def session_entry_equity(self) -> float:
        return self._session_entry_equity

    @property
    def day_trade_count(self) -> int:
        return self._day_trade_count

    def reset_kill_switch(self) -> None:
        self._kill_switch = False
        logger.warning("Kill switch manually reset")

    def check_drawdown(self, current_equity: float) -> bool:
        """Return True if trading should be halted (drawdown exceeded)."""
        if self._session_entry_equity <= 0:
            return False

        drawdown = (current_equity - self._session_entry_equity) / self._session_entry_equity
        if drawdown <= self.daily_drawdown_limit:
            if not self._kill_switch:
                self._kill_switch = True
                logger.critical(
                    "KILL SWITCH TRIGGERED — daily drawdown %.2f%% exceeded limit %.2f%%",
                    drawdown * 100,
                    self.daily_drawdown_limit * 100,
                )
            return True
        return False

    def set_session_equity(self, equity: float) -> None:
        self._session_entry_equity = equity

    # ── Buy approval ─────────────────────────────────────────────────────────

    def approve_buy(
        self,
        symbol: str,
        current_equity: float,
        current_positions: dict,
        from_db_day_trades: int = 0,
    ) -> RiskDecision:
        if self._kill_switch:
            return RiskDecision(False, "Kill switch active — daily drawdown limit hit")

        # Do NOT call check_drawdown here — it would set the kill switch silently
        # with no DB persistence or ntfy alert. The real drawdown guard runs in
        # _tick_account before any symbol processing.

        if symbol in current_positions:
            return RiskDecision(False, f"Already holding {symbol}")

        if len(current_positions) >= self.max_positions:
            return RiskDecision(False, f"Max concurrent positions ({self.max_positions}) reached")

        if self.pdt_aware:
            total_day_trades = from_db_day_trades + self._day_trade_count
            if total_day_trades >= PDT_LIMIT:
                return RiskDecision(
                    False,
                    f"PDT limit: {total_day_trades} day trades in rolling 5-day window (limit {PDT_LIMIT})",
                )

        dollar_amount = current_equity * self.max_position_pct
        if dollar_amount < 1.0:
            return RiskDecision(False, "Insufficient equity for minimum position")

        return RiskDecision(True, "Risk checks passed", dollar_amount)

    # ── Stop-loss check ──────────────────────────────────────────────────────

    def should_stop_loss(self, symbol: str, entry_price: float, current_price: float,
                         position_qty: float = 0.0) -> bool:
        if entry_price <= 0:
            return False
        drop = (current_price - entry_price) / entry_price
        dollar_loss = abs(drop * entry_price * position_qty) if position_qty > 0 else 0.0

        if drop <= -self.stop_loss_pct:
            logger.warning(
                "Stop-loss triggered for %s: entry=%.4f current=%.4f drop=%.2f%%",
                symbol, entry_price, current_price, drop * 100,
            )
            return True

        # Hard dollar loss cap — catches gap-downs that blow past the % stop
        if self.max_position_loss_usd > 0 and dollar_loss >= self.max_position_loss_usd:
            logger.warning(
                "Max-loss cap triggered for %s: entry=%.4f current=%.4f loss=$%.2f (cap=$%.2f)",
                symbol, entry_price, current_price, dollar_loss, self.max_position_loss_usd,
            )
            return True

        return False

    # ── PDT tracking ─────────────────────────────────────────────────────────

    def record_day_trade(self) -> None:
        """Call this when a position is opened AND closed on the same trading day."""
        self._day_trade_count += 1
        logger.info("Day trade recorded. Session count: %d", self._day_trade_count)

    def reset_day_trade_count(self) -> None:
        self._day_trade_count = 0

    # ── Stop-loss price calculator ───────────────────────────────────────────

    def stop_loss_price(self, entry_price: float) -> float:
        return entry_price * (1 - self.stop_loss_pct)

    # ── Position size calculator ─────────────────────────────────────────────

    def position_dollar_size(self, portfolio_equity: float) -> float:
        return max(0.0, portfolio_equity * self.max_position_pct)
