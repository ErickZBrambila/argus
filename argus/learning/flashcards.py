"""Trade decision flashcards for learning.

Every executed trade produces a flashcard explaining what signals fired,
why the AI decided to act, and (once the position closes) what the outcome was.
Flashcards are stored as JSON lines in argus_flashcards.jsonl.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_UTC = timezone.utc


@dataclass
class Flashcard:
    # Identity
    trade_id: str
    symbol: str
    action: str              # BUY | SELL
    account: str             # agentic | default
    timestamp: str           # ISO-8601

    # Signal snapshot
    signal_composite: str    # bullish | bearish | neutral
    signal_confidence: float
    rsi: Optional[float]
    macd_hist: Optional[float]
    bb_position: str         # "above_upper" | "below_lower" | "inside"
    price_vs_sma20: str      # "above" | "below"
    price_vs_ema50: str      # "above" | "below"

    # Decision
    risk_level: str          # low | medium | high
    decision_confidence: float
    reasoning: str

    # Execution
    entry_price: float
    dollar_amount: float

    # Outcome (filled in when position closes)
    exit_price: Optional[float] = None
    pnl_pct: Optional[float] = None
    outcome: Optional[str] = None   # "win" | "loss" | "stop-loss"
    hold_duration_hours: Optional[float] = None

    # Pattern label (set manually or by future ML)
    pattern: str = ""

    def as_dict(self) -> dict:
        return asdict(self)

    def front(self) -> str:
        """The 'question' side — market context at decision time."""
        bb = self.bb_position.replace("_", " ")
        return (
            f"{self.symbol} @ ${self.entry_price:.2f}\n"
            f"Signal: {self.signal_composite.upper()} ({self.signal_confidence:.0%} conf)\n"
            f"RSI: {self.rsi:.1f if self.rsi else 'N/A'} | "
            f"MACD hist: {self.macd_hist:.4f if self.macd_hist else 'N/A'}\n"
            f"BB: {bb} | SMA20: price {self.price_vs_sma20} | EMA50: price {self.price_vs_ema50}"
        )

    def back(self) -> str:
        """The 'answer' side — decision and outcome."""
        outcome_str = ""
        if self.pnl_pct is not None:
            sign = "+" if self.pnl_pct >= 0 else ""
            outcome_str = f"\nOutcome: {self.outcome} | P&L {sign}{self.pnl_pct:.2f}%"
            if self.hold_duration_hours is not None:
                outcome_str += f" | held {self.hold_duration_hours:.1f}h"
        return (
            f"Action: {self.action} ({self.risk_level.upper()} risk, {self.decision_confidence:.0%} conf)\n"
            f"Reasoning: {self.reasoning[:300]}"
            + outcome_str
        )


class FlashcardStore:
    def __init__(self, path: str = "argus_flashcards.jsonl") -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        # In-memory index: trade_id → line offset for quick updates
        self._cards: dict[str, Flashcard] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            with self._path.open() as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                        card = Flashcard(**d)
                        self._cards[card.trade_id] = card
                    except Exception:
                        pass
            logger.info("Loaded %d flashcards from %s", len(self._cards), self._path)
        except Exception as exc:
            logger.warning("Could not load flashcards: %s", exc)

    def _flush(self) -> None:
        try:
            with self._path.open("w") as f:
                for card in self._cards.values():
                    f.write(json.dumps(card.as_dict()) + "\n")
        except Exception as exc:
            logger.warning("Could not save flashcards: %s", exc)

    def record_trade(
        self,
        trade_id: str,
        symbol: str,
        action: str,
        account: str,
        signal,           # SignalResult
        decision,         # TradeDecision
        entry_price: float,
        dollar_amount: float,
    ) -> Flashcard:
        # Determine BB position relative to price
        if signal.bb_lower is not None and signal.bb_upper is not None:
            if signal.price >= signal.bb_upper:
                bb_pos = "above_upper"
            elif signal.price <= signal.bb_lower:
                bb_pos = "below_lower"
            else:
                bb_pos = "inside"
        else:
            bb_pos = "unknown"

        price_vs_sma = "above" if (signal.sma_20 and signal.price > signal.sma_20) else "below"
        price_vs_ema = "above" if (signal.ema_50 and signal.price > signal.ema_50) else "below"

        card = Flashcard(
            trade_id=trade_id,
            symbol=symbol,
            action=action,
            account=account,
            timestamp=datetime.now(_UTC).isoformat(),
            signal_composite=signal.composite,
            signal_confidence=signal.confidence,
            rsi=signal.rsi,
            macd_hist=signal.macd_hist,
            bb_position=bb_pos,
            price_vs_sma20=price_vs_sma,
            price_vs_ema50=price_vs_ema,
            risk_level=decision.risk_level,
            decision_confidence=decision.confidence,
            reasoning=decision.reasoning,
            entry_price=entry_price,
            dollar_amount=dollar_amount,
        )

        with self._lock:
            self._cards[trade_id] = card
            self._flush()

        logger.info("Flashcard created for %s %s [%s]", action, symbol, trade_id)
        return card

    def close_trade(
        self,
        trade_id: str,
        exit_price: float,
        outcome: str,
    ) -> Optional[Flashcard]:
        with self._lock:
            card = self._cards.get(trade_id)
            if card is None:
                return None
            card.exit_price = exit_price
            card.pnl_pct = (exit_price - card.entry_price) / card.entry_price * 100
            card.outcome = outcome
            entry_dt = datetime.fromisoformat(card.timestamp)
            card.hold_duration_hours = (datetime.now(_UTC) - entry_dt).total_seconds() / 3600
            self._flush()
        logger.info("Flashcard closed: %s %s | P&L %.2f%%", card.symbol, outcome, card.pnl_pct)
        return card

    def get_all(self) -> list[Flashcard]:
        with self._lock:
            return list(self._cards.values())

    def get_recent(self, n: int = 10) -> list[Flashcard]:
        with self._lock:
            cards = sorted(self._cards.values(), key=lambda c: c.timestamp, reverse=True)
            return cards[:n]

    def summary(self) -> dict:
        """Win rate, avg P&L, best/worst trade."""
        with self._lock:
            closed = [c for c in self._cards.values() if c.pnl_pct is not None]
        if not closed:
            return {"total": 0, "closed": 0, "win_rate": None, "avg_pnl_pct": None}
        wins = [c for c in closed if (c.pnl_pct or 0) > 0]
        pnls = [c.pnl_pct for c in closed if c.pnl_pct is not None]
        return {
            "total": len(self._cards),
            "closed": len(closed),
            "win_rate": len(wins) / len(closed),
            "avg_pnl_pct": sum(pnls) / len(pnls),
            "best_pnl_pct": max(pnls),
            "worst_pnl_pct": min(pnls),
        }
