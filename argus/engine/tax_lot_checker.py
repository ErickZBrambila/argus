"""Tax lot checker — defers SELL when a lot is close to long-term threshold.

Fetches open position lots from Robinhood once per symbol per day and checks
whether holding a few more days would convert a short-term gain to long-term.
If the earliest lot is within DEFER_DAYS of the 365-day threshold, SELL is
deferred (stop-loss still fires; this gate only blocks voluntary AI sells).

Falls back to (False, "") silently if lot data is unavailable so it never
blocks selling on API failure.
"""

from __future__ import annotations

import datetime
import logging
import threading
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

_DEFER_DAYS = 14        # defer sell if lot becomes long-term within this many days
_LONG_TERM_DAYS = 365   # IRS threshold for long-term capital gains


@dataclass
class LotInfo:
    symbol: str
    cached_on: datetime.date
    earliest_acquisition: Optional[datetime.date] = None   # oldest open lot
    days_held: Optional[int] = None                        # days since earliest lot
    days_until_long_term: Optional[int] = None             # None if already long-term or unknown


class TaxLotChecker:
    """Thread-safe per-symbol tax lot cache with a daily TTL."""

    def __init__(self, defer_days: int = _DEFER_DAYS) -> None:
        self._defer_days = defer_days
        self._cache: dict[str, LotInfo] = {}
        self._lock = threading.Lock()

    def _fetch(self, symbol: str) -> LotInfo:
        today = datetime.date.today()
        info = LotInfo(symbol=symbol, cached_on=today)

        try:
            import robin_stocks.robinhood as rh

            # get_open_stock_positions() returns a list of position dicts.
            # Each position may have an 'average_buy_price' and 'created_at' field.
            # Robin stocks doesn't expose per-lot data directly, so we use the
            # position's created_at as a proxy for the oldest lot acquisition date.
            positions = rh.account.get_open_stock_positions() or []
            for pos in positions:
                # Match by instrument URL → ticker
                instrument_url = pos.get("instrument", "")
                try:
                    data = rh.stocks.get_instrument_by_url(instrument_url) or {}
                    ticker = data.get("symbol", "").upper()
                except Exception:
                    ticker = ""

                if ticker != symbol.upper():
                    continue

                created_str = pos.get("created_at") or pos.get("updated_at")
                if not created_str:
                    break

                try:
                    # ISO format: "2024-11-15T14:32:01Z"
                    acq_date = datetime.date.fromisoformat(created_str[:10])
                    info.earliest_acquisition = acq_date
                    days_held = (today - acq_date).days
                    info.days_held = days_held
                    if days_held < _LONG_TERM_DAYS:
                        info.days_until_long_term = _LONG_TERM_DAYS - days_held
                    else:
                        info.days_until_long_term = 0  # already long-term
                except (ValueError, TypeError):
                    pass
                break

        except Exception as exc:
            logger.debug("Tax lot fetch failed for %s: %s", symbol, exc)

        return info

    def get(self, symbol: str) -> LotInfo:
        today = datetime.date.today()
        with self._lock:
            cached = self._cache.get(symbol)
            if cached and cached.cached_on == today:
                return cached
        info = self._fetch(symbol)
        with self._lock:
            self._cache[symbol] = info
        return info

    def should_defer_sell(self, symbol: str) -> tuple[bool, str]:
        """Returns (defer, reason). True when selling now would trigger a short-term
        gain that could be avoided by holding a few more days."""
        # Crypto is always short-term regardless; don't interfere with those sells
        _CRYPTO = {"BTC", "ETH", "ETC", "DOGE", "SOL", "LTC", "BCH", "XRP", "ADA", "AVAX"}
        if symbol.upper() in _CRYPTO:
            return False, ""

        info = self.get(symbol)
        if info.days_until_long_term is None:
            return False, ""  # couldn't fetch — don't block
        if info.days_until_long_term == 0:
            return False, ""  # already long-term — sell freely
        if info.days_until_long_term <= self._defer_days:
            reason = (
                f"lot acquired {info.earliest_acquisition} ({info.days_held}d ago) — "
                f"long-term in {info.days_until_long_term}d; deferring to avoid short-term tax"
            )
            return True, reason

        return False, ""
