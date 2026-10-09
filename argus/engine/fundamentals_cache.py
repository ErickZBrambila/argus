"""Fundamentals + financials cache for AI decision context.

Fetches PE ratio, P/B, 52-week range, and recent revenue/margin trend
from Robinhood once per symbol per calendar day and injects the result
into the AI decision prompt so Claude and Gemini are valuation-aware.
"""

from __future__ import annotations

import datetime
import logging
import threading
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_CRYPTO = frozenset({"BTC", "ETH", "ETC", "DOGE", "SOL", "LTC", "BCH", "XRP", "ADA", "AVAX"})


@dataclass
class FundamentalsSnapshot:
    symbol: str
    cached_on: datetime.date

    # Valuation
    pe_ratio: float | None = None
    pb_ratio: float | None = None
    market_cap_b: float | None = None   # billions

    # 52-week position (0.0 = at 52w low, 1.0 = at 52w high)
    week52_position: float | None = None
    week52_low: float | None = None
    week52_high: float | None = None

    # Financials: last two quarters (most-recent first)
    revenue_growth_pct: float | None = None   # QoQ revenue growth %
    net_margin_latest: float | None = None    # most recent quarter net margin %
    net_margin_prev: float | None = None      # prior quarter net margin %
    gross_profit_b: float | None = None       # gross profit in billions, latest quarter

    # Analyst consensus
    analyst_buy_pct: float | None = None
    analyst_sell_pct: float | None = None
    analyst_target_avg: float | None = None
    analyst_target_upside_pct: float | None = None  # relative to estimated current price

    # Politician disclosures (90-day window, cached with daily TTL)
    politician_trades_summary: str | None = None


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
                def _f(key: str) -> float | None:
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
            from argus.broker.robinhood_mcp import get_quarterly_financials as _qf
            fin = _qf([symbol]).get(symbol, {})
            snap.revenue_growth_pct = fin.get("revenue_growth_pct")
            snap.net_margin_latest  = fin.get("net_margin_latest")
            snap.net_margin_prev    = fin.get("net_margin_prev")
            snap.gross_profit_b     = fin.get("gross_profit_b")
        except Exception as exc:
            logger.debug("MCP financials fetch failed for %s: %s", symbol, exc)

        try:
            from argus.broker.robinhood_mcp import get_analyst_ratings as _ar
            ar = _ar([symbol]).get(symbol, {})
            snap.analyst_buy_pct   = ar.get("buy_pct")
            snap.analyst_sell_pct  = ar.get("sell_pct")
            snap.analyst_target_avg = ar.get("target_avg")
            if snap.analyst_target_avg and snap.week52_low and snap.week52_high and snap.week52_position is not None:
                est_price = snap.week52_low + snap.week52_position * (snap.week52_high - snap.week52_low)
                if est_price > 0:
                    snap.analyst_target_upside_pct = round(
                        (snap.analyst_target_avg / est_price - 1) * 100, 1
                    )
        except Exception as exc:
            logger.debug("MCP analyst ratings fetch failed for %s: %s", symbol, exc)

        try:
            import datetime as _dt

            from argus.broker.robinhood_mcp import get_politician_trades_for as _pt
            cutoff = (_dt.date.today() - _dt.timedelta(days=90)).isoformat()
            trades = [t for t in _pt(symbol) if t.get("transaction_date", "") >= cutoff]
            if trades:
                buys  = sum(1 for t in trades if "buy"  in (t.get("transaction_type") or "").lower())
                sells = sum(1 for t in trades if "sell" in (t.get("transaction_type") or "").lower())
                # Only emit derived counts — raw politician/amount strings are free-text
                # from a third-party feed and must not flow into the AI prompt verbatim.
                parts = []
                if buys:
                    parts.append(f"{buys} buy{'s' if buys > 1 else ''}")
                if sells:
                    parts.append(f"{sells} sell{'s' if sells > 1 else ''}")
                if parts:
                    snap.politician_trades_summary = f"{', '.join(parts)} in last 90 days"
        except Exception as exc:
            logger.debug("MCP politician trades fetch failed for %s: %s", symbol, exc)

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

        if snap.revenue_growth_pct is not None or snap.net_margin_latest is not None:
            rev_str = ""
            if snap.revenue_growth_pct is not None:
                d = "▲" if snap.revenue_growth_pct >= 0 else "▼"
                rev_str = f"revenue {d}{abs(snap.revenue_growth_pct):.1f}%"
            margin_str = ""
            if snap.net_margin_latest is not None:
                margin_str = f"net margin {snap.net_margin_latest:.1f}%"
            parts = [p for p in [rev_str, margin_str] if p]
            lines.append(f"  Financials (QoQ): {' · '.join(parts)}")

        if snap.analyst_buy_pct is not None:
            buy_pct  = round(snap.analyst_buy_pct * 100)
            sell_pct = round((snap.analyst_sell_pct or 0) * 100)
            analyst_str = f"  Analyst: {buy_pct}% Buy · {100 - buy_pct - sell_pct}% Hold · {sell_pct}% Sell"
            if snap.analyst_target_avg:
                analyst_str += f" · avg target ${snap.analyst_target_avg:.2f}"
            if snap.analyst_target_upside_pct is not None:
                sign = "+" if snap.analyst_target_upside_pct >= 0 else ""
                analyst_str += f" ({sign}{snap.analyst_target_upside_pct:.1f}% upside)"
            lines.append(analyst_str)

        if snap.politician_trades_summary:
            lines.append(f"  Politician disclosures (90d): {snap.politician_trades_summary}")

        if not lines:
            return ""
        return "Fundamentals:\n" + "\n".join(lines)
