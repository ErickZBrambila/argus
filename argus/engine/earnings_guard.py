"""Earnings proximity guard — blocks BUY orders when earnings are imminent.

Caches results per symbol per calendar day so the Robinhood API is only
hit once per symbol per day, not on every 90-second scan tick.
"""

from __future__ import annotations

import datetime
import logging
import threading
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

_EARNINGS_BLOCK_DAYS = 5   # block BUY if earnings within this many calendar days


@dataclass
class EarningsInfo:
    symbol: str
    report_date: Optional[datetime.date]   # None if no upcoming earnings found
    days_away: Optional[int]               # None if no upcoming earnings found
    timing: Optional[str]                  # "am" | "pm" | None
    cached_on: datetime.date               # cache key — invalidated on day change


class EarningsGuard:
    """Thread-safe per-symbol earnings cache with a daily TTL."""

    def __init__(self, block_days: int = _EARNINGS_BLOCK_DAYS) -> None:
        self._block_days = block_days
        self._cache: dict[str, EarningsInfo] = {}
        self._lock = threading.Lock()

    def _fetch(self, symbol: str) -> EarningsInfo:
        """Call Robinhood via robin_stocks and return EarningsInfo."""
        today = datetime.date.today()
        try:
            import robin_stocks.robinhood as rh
            earnings = rh.stocks.get_earnings(symbol) or []
            upcoming = []
            for e in earnings:
                report = e.get("report") or {}
                date_str = report.get("date")
                if not date_str:
                    eps = e.get("eps") or {}
                    date_str = eps.get("report_date") or e.get("report", {}).get("date")
                if not date_str:
                    continue
                try:
                    report_date = datetime.date.fromisoformat(date_str[:10])
                except (ValueError, TypeError):
                    continue
                if report_date >= today:
                    upcoming.append((report_date, report.get("timing")))

            if upcoming:
                upcoming.sort(key=lambda x: x[0])
                nearest_date, timing = upcoming[0]
                days_away = (nearest_date - today).days
                return EarningsInfo(
                    symbol=symbol,
                    report_date=nearest_date,
                    days_away=days_away,
                    timing=timing,
                    cached_on=today,
                )
        except Exception as exc:
            logger.debug("Earnings fetch failed for %s: %s", symbol, exc)

        return EarningsInfo(symbol=symbol, report_date=None, days_away=None, timing=None, cached_on=today)

    def get(self, symbol: str) -> EarningsInfo:
        today = datetime.date.today()
        with self._lock:
            cached = self._cache.get(symbol)
            if cached and cached.cached_on == today:
                return cached
        info = self._fetch(symbol)
        with self._lock:
            self._cache[symbol] = info
        return info

    def should_block_buy(self, symbol: str) -> tuple[bool, str]:
        """Returns (blocked, reason). Only blocks equities — crypto always passes."""
        # Crypto symbols don't have earnings reports
        crypto = {"BTC", "ETH", "ETC", "DOGE", "SOL", "LTC", "BCH", "XRP", "ADA", "AVAX"}
        if symbol.upper() in crypto:
            return False, ""

        info = self.get(symbol)
        if info.days_away is not None and info.days_away <= self._block_days:
            timing_str = f" ({info.timing})" if info.timing else ""
            reason = (
                f"earnings in {info.days_away}d on {info.report_date}{timing_str} "
                f"— avoiding pre-earnings event risk"
            )
            return True, reason

        return False, ""
