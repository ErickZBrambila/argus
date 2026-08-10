"""Fundamentals + financials cache for AI decision context.

Fetches PE ratio, P/B, 52-week range, and recent revenue/margin trend
from Robinhood once per symbol per calendar day and injects the result
into the AI decision prompt so Claude and Gemini are valuation-aware.
"""

from __future__ import annotations

import datetime
import logging
import threading
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

_CRYPTO = frozenset({"BTC", "ETH", "ETC", "DOGE", "SOL", "LTC", "BCH", "XRP", "ADA", "AVAX"})


@dataclass
class FundamentalsSnapshot:
    symbol: str
    cached_on: datetime.date

    # Valuation
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    market_cap_b: Optional[float] = None   # billions

    # 52-week position (0.0 = at 52w low, 1.0 = at 52w high)
    week52_position: Optional[float] = None
    week52_low: Optional[float] = None
    week52_high: Optional[float] = None

    # Financials: last two quarters (most-recent first)
    revenue_growth_pct: Optional[float] = None   # QoQ revenue growth %
    net_margin_latest: Optional[float] = None    # most recent quarter net margin %
    net_margin_prev: Optional[float] = None      # prior quarter net margin %


class FundamentalsCache:
    """Thread-safe per-symbol fundamentals cache with a daily TTL."""

    def __init__(self) -> None:
        self._cache: dict[str, FundamentalsSnapshot] = {}
        self._lock = threading.Lock()

    def _fetch(self, symbol: str) -> FundamentalsSnapshot:
        today = datetime.date.today()
        snap = FundamentalsSnapshot(symbol=symbol, cached_on=today)

        if symbol in _CRYPTO:
            return snap  # crypto has no fundamentals

        try:
            import robin_stocks.robinhood as rh

            # ── Fundamentals ──────────────────────────────────────────────────
            raw = rh.stocks.get_fundamentals(symbol, info=None)
            if raw and isinstance(raw, list):
                raw = raw[0]
            if raw and isinstance(raw, dict):
                def _f(key: str) -> Optional[float]:
                    v = raw.get(key)
                    try:
                        return float(v) if v not in (None, "", "None") else None
                    except (TypeError, ValueError):
                        return None

                snap.pe_ratio = _f("pe_ratio")
                snap.pb_ratio = _f("pb_ratio")
                mkt = _f("market_cap")
                snap.market_cap_b = round(mkt / 1e9, 1) if mkt else None
                lo = _f("low_52_weeks")
                hi = _f("high_52_weeks")
                snap.week52_low = lo
                snap.week52_high = hi
                if lo and hi and hi > lo:
                    try:
                        price = _f("open") or _f("low") or lo
                        snap.week52_position = round((price - lo) / (hi - lo), 2)
                    except Exception:
                        pass

        except Exception as exc:
            logger.debug("Fundamentals fetch failed for %s: %s", symbol, exc)

        try:
            import robin_stocks.robinhood as rh

            # ── Financials (revenue / net margin trend) ───────────────────────
            earnings = rh.stocks.get_earnings(symbol) or []
            # robin_stocks earnings don't include revenue — try instruments financials
            # Fall back to quarterly EPS trend as a profitability proxy if full
            # financials aren't available via robin_stocks
            if not earnings:
                return snap

            # Use most recent two entries that have actual EPS to gauge trend
            actuals = [
                e for e in earnings
                if e.get("eps", {}) and e["eps"].get("actual") not in (None, "")
            ]
            if len(actuals) >= 2:
                eps_now  = float(actuals[0]["eps"]["actual"])
                eps_prev = float(actuals[1]["eps"]["actual"])
                if eps_prev != 0:
                    snap.revenue_growth_pct = round((eps_now - eps_prev) / abs(eps_prev) * 100, 1)

        except Exception as exc:
            logger.debug("Financials fetch failed for %s: %s", symbol, exc)

        return snap

    def get(self, symbol: str) -> FundamentalsSnapshot:
        today = datetime.date.today()
        with self._lock:
            cached = self._cache.get(symbol)
            if cached and cached.cached_on == today:
                return cached
        snap = self._fetch(symbol)
        with self._lock:
            self._cache[symbol] = snap
        return snap

    def to_prompt_block(self, symbol: str) -> str:
        """Return a compact text block ready to append to the AI prompt."""
        snap = self.get(symbol)
        if symbol in _CRYPTO:
            return ""

        lines = []
        if snap.pe_ratio is not None:
            lines.append(f"  P/E ratio: {snap.pe_ratio:.1f}")
        if snap.pb_ratio is not None:
            lines.append(f"  P/B ratio: {snap.pb_ratio:.1f}")
        if snap.market_cap_b is not None:
            lines.append(f"  Market cap: ${snap.market_cap_b:.1f}B")
        if snap.week52_position is not None:
            pct = snap.week52_position * 100
            lines.append(
                f"  52-week range: ${snap.week52_low:.2f} – ${snap.week52_high:.2f} "
                f"(currently at {pct:.0f}% of range)"
            )
        if snap.revenue_growth_pct is not None:
            direction = "▲" if snap.revenue_growth_pct >= 0 else "▼"
            lines.append(f"  EPS trend (QoQ): {direction}{abs(snap.revenue_growth_pct):.1f}%")

        if not lines:
            return ""
        return "Fundamentals:\n" + "\n".join(lines)
