"""Price book guard — skips BUY when bid/ask spread is too wide.

Fetches the Level 2 order book from Robinhood once per symbol per minute
and blocks BUY orders when the spread exceeds MAX_SPREAD_PCT (default 0.5%).
A wide spread signals low liquidity or a fast-moving market where slippage
can easily wipe out any edge the AI signal has.

Falls back to (False, "") if the price book is unavailable — never blocks
buying on API failure.
"""

from __future__ import annotations

import datetime
import logging
import threading
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

_MAX_SPREAD_PCT = 0.005   # 0.5% — block BUY above this spread
_CACHE_SECONDS  = 60      # refresh at most once per minute per symbol


@dataclass
class BookSnapshot:
    symbol: str
    fetched_at: datetime.datetime
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    spread_pct: Optional[float] = None


class PriceBookGuard:
    """Thread-safe price book cache with a per-minute TTL."""

    def __init__(self, max_spread_pct: float = _MAX_SPREAD_PCT) -> None:
        self._max_spread_pct = max_spread_pct
        self._cache: dict[str, BookSnapshot] = {}
        self._lock = threading.Lock()

    def _fetch(self, symbol: str) -> BookSnapshot:
        now = datetime.datetime.now(datetime.timezone.utc)
        snap = BookSnapshot(symbol=symbol, fetched_at=now)

        try:
            import robin_stocks.robinhood as rh

            # get_pricebook_by_symbol returns {asks: [{price, quantity}], bids: [...], ...}
            book = rh.stocks.get_pricebook_by_symbol(symbol) or {}
            asks = book.get("asks") or []
            bids = book.get("bids") or []

            if asks and bids:
                best_ask = float(asks[0].get("price", 0))
                best_bid = float(bids[0].get("price", 0))
                if best_ask > 0 and best_bid > 0:
                    snap.best_bid = best_bid
                    snap.best_ask = best_ask
                    mid = (best_bid + best_ask) / 2
                    snap.spread_pct = (best_ask - best_bid) / mid if mid > 0 else None

        except Exception as exc:
            logger.debug("Price book fetch failed for %s: %s", symbol, exc)

        return snap

    def get(self, symbol: str) -> BookSnapshot:
        now = datetime.datetime.now(datetime.timezone.utc)
        with self._lock:
            cached = self._cache.get(symbol)
            if cached and (now - cached.fetched_at).total_seconds() < _CACHE_SECONDS:
                return cached
        snap = self._fetch(symbol)
        with self._lock:
            self._cache[symbol] = snap
        return snap

    def should_block_buy(self, symbol: str) -> tuple[bool, str]:
        """Returns (blocked, reason). Blocks BUY when spread is too wide.
        Crypto is included — Robinhood exposes crypto price books too."""
        snap = self.get(symbol)
        if snap.spread_pct is None:
            return False, ""  # no data — don't block
        if snap.spread_pct > self._max_spread_pct:
            reason = (
                f"bid/ask spread {snap.spread_pct * 100:.2f}% "
                f"(bid ${snap.best_bid:.4f} ask ${snap.best_ask:.4f}) "
                f"exceeds {self._max_spread_pct * 100:.1f}% limit — wide spread indicates low liquidity"
            )
            return True, reason
        return False, ""
