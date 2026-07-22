"""Technical indicator engine using pandas_ta."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Protocol

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_VALID_SYMBOL_CHARS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.")

# Symbols with limited price history due to recent IPO — use reduced bar minimum
# during the grace period so signals still fire while history accumulates.
_NEW_LISTINGS: dict[str, str] = {
    "SPCX": "2026-06-12",   # SpaceX IPO
}
_NEW_LISTING_GRACE_DAYS = 90
_NEW_LISTING_MIN_BARS = 3   # indicators populate gradually; price/ticker shows immediately


def _validate_symbol(symbol: str) -> str:
    clean = symbol.strip().upper()
    if not clean or not all(c in _VALID_SYMBOL_CHARS for c in clean):
        raise ValueError(f"Invalid symbol: {symbol!r}")
    if len(clean) > 10:
        raise ValueError(f"Symbol too long: {symbol!r}")
    return clean


@dataclass
class SignalResult:
    symbol: str
    price: float
    volume: float

    rsi: Optional[float]
    macd: Optional[float]
    macd_signal: Optional[float]
    macd_hist: Optional[float]
    bb_upper: Optional[float]
    bb_mid: Optional[float]
    bb_lower: Optional[float]
    sma_20: Optional[float]
    ema_50: Optional[float]

    composite: str        # "bullish" | "bearish" | "neutral"
    confidence: float     # 0.0 – 1.0
    change_pct: float = 0.0  # Daily change percentage

    def to_dict(self) -> dict:
        return self.__dict__.copy()


class StrategyProtocol(Protocol):
    def score(
        self,
        price: float,
        rsi: Optional[float],
        macd_hist: Optional[float],
        bb_upper: Optional[float],
        bb_lower: Optional[float],
        sma_20: Optional[float],
        ema_50: Optional[float],
    ) -> tuple[str, float]:
        ...


class DefaultStrategy(StrategyProtocol):
    def score(
        self,
        price: float,
        rsi: Optional[float],
        macd_hist: Optional[float],
        bb_upper: Optional[float],
        bb_lower: Optional[float],
        sma_20: Optional[float],
        ema_50: Optional[float],
    ) -> tuple[str, float]:
        """Simple vote-based composite signal."""
        bullish = 0
        bearish = 0
        total = 0

        if rsi is not None:
            total += 1
            if rsi < 30:
                bullish += 1
            elif rsi > 70:
                bearish += 1

        if macd_hist is not None:
            total += 1
            if macd_hist > 0:
                bullish += 1
            elif macd_hist < 0:
                bearish += 1

        if bb_upper is not None and bb_lower is not None:
            total += 1
            if price <= bb_lower:
                bullish += 1
            elif price >= bb_upper:
                bearish += 1

        if sma_20 is not None:
            total += 1
            if price > sma_20:
                bullish += 1
            elif price < sma_20:
                bearish += 1

        if ema_50 is not None:
            total += 1
            if price > ema_50:
                bullish += 1
            elif price < ema_50:
                bearish += 1

        if total == 0:
            return "neutral", 0.0

        confidence = max(bullish, bearish) / total
        if bullish > bearish:
            return "bullish", confidence
        elif bearish > bullish:
            return "bearish", confidence
        return "neutral", confidence


class SignalEngine:
    def __init__(self, broker, strategy: Optional[StrategyProtocol] = None) -> None:
        self._broker = broker
        self._strategy = strategy or DefaultStrategy()

    def compute(self, symbol: str) -> Optional[SignalResult]:
        symbol = _validate_symbol(symbol)
        try:
            return self._compute(symbol)
        except Exception as exc:
            logger.warning("Signal computation failed for %s: %s", symbol, exc)
            return None

    def get_annotated_chart(self, symbol: str, span: str = "3month", interval: str = "day") -> list[dict]:
        """Fetch historical prices and compute indicators for the full series."""
        try:
            raw = self._broker.get_historical_prices(symbol, span=span, interval=interval)
            if not raw:
                return []
            df = _build_dataframe(raw)
            if df is None or len(df) < 2:
                return raw

            # Add indicators
            df.ta.rsi(length=14, append=True)
            df.ta.sma(length=20, append=True)
            df.ta.ema(length=50, append=True)

            # Replace NaNs with None for JSON compatibility
            df = df.replace({np.nan: None})

            # Convert back to list of dicts, preserving original keys where possible
            results = []
            for i, row in df.iterrows():
                d = raw[i].copy()
                d["rsi"] = row.get("RSI_14")
                d["sma_20"] = row.get("SMA_20")
                d["ema_50"] = row.get("EMA_50")
                results.append(d)
            return results
        except Exception as exc:
            logger.warning("Failed to annotate chart for %s: %s", symbol, exc)
            return []

    def _compute(self, symbol: str) -> Optional[SignalResult]:
        from argus.storage.models import get_session, get_cached_historicals, save_historicals
        
        # 1. Try to load from cache
        with get_session() as session:
            cached = get_cached_historicals(session, symbol)
        
        # 2. Fetch latest (short span) to fill gaps
        raw = self._broker.get_historical_prices(symbol, span="week", interval="day")
        
        # 3. Merge and persist
        if raw:
            with get_session() as session:
                save_historicals(session, symbol, raw)
            # Re-fetch full cached series after update
            with get_session() as session:
                cached = get_cached_historicals(session, symbol)

        # Determine minimum bars — lower threshold for new listings within grace period
        import datetime as _dt
        _min_bars = 50
        _listing_date_str = _NEW_LISTINGS.get(symbol)
        if _listing_date_str:
            try:
                _listing_date = _dt.date.fromisoformat(_listing_date_str)
                if _dt.date.today() <= _listing_date + _dt.timedelta(days=_NEW_LISTING_GRACE_DAYS):
                    _min_bars = _NEW_LISTING_MIN_BARS
            except Exception:
                pass

        # If cache is still too small, fall back to full fetch (skip for new listings — no data exists)
        if len(cached) < _min_bars and _min_bars >= 50:
            raw_full = self._broker.get_historical_prices(symbol, span="3month", interval="day")
            if raw_full:
                with get_session() as session:
                    save_historicals(session, symbol, raw_full)
                with get_session() as session:
                    cached = get_cached_historicals(session, symbol)

        if len(cached) < _min_bars:
            logger.warning("%s: not enough history (%d bars, need %d)", symbol, len(cached), _min_bars)
            return None

        df = _build_dataframe(cached)
        if df is None or len(df) < _min_bars:
            return None

        try:
            import pandas_ta as ta  # noqa: F401
        except ImportError:
            logger.error("pandas_ta not installed; run: pip install pandas-ta")
            return None

        # RSI (14)
        df.ta.rsi(length=14, append=True)
        rsi_col = "RSI_14"

        # MACD (12, 26, 9)
        df.ta.macd(fast=12, slow=26, signal=9, append=True)
        macd_col, macd_sig_col, macd_hist_col = "MACD_12_26_9", "MACDs_12_26_9", "MACDh_12_26_9"

        # Bollinger Bands (20, 2) — pandas_ta appends std twice in the column name
        df.ta.bbands(length=20, std=2, append=True)
        bb_lower_col, bb_mid_col, bb_upper_col = "BBL_20_2.0_2.0", "BBM_20_2.0_2.0", "BBU_20_2.0_2.0"

        # SMA 20, EMA 50
        df.ta.sma(length=20, append=True)
        df.ta.ema(length=50, append=True)
        sma_col, ema_col = "SMA_20", "EMA_50"

        last = df.iloc[-1]

        def _safe(col: str) -> Optional[float]:
            v = last.get(col)
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return None
            return float(v)

        # Price from broker's latest quote for real-time ticker accuracy, 
        # falling back to candle close if quote fails.
        try:
            price = self._broker.get_price(symbol)
        except Exception:
            price = float(last["close"])

        volume = float(last.get("volume", 0) or 0)
        rsi = _safe(rsi_col)
        macd = _safe(macd_col)
        macd_sig = _safe(macd_sig_col)
        macd_hist = _safe(macd_hist_col)
        bb_upper = _safe(bb_upper_col)
        bb_mid = _safe(bb_mid_col)
        bb_lower = _safe(bb_lower_col)
        sma_20 = _safe(sma_col)
        ema_50 = _safe(ema_col)

        # Daily change % (current vs prev close)
        change_pct = 0.0
        if len(df) >= 2:
            prev_close = float(df.iloc[-2]["close"])
            if prev_close > 0:
                change_pct = (price - prev_close) / prev_close * 100

        composite, confidence = self._strategy.score(price, rsi, macd_hist, bb_upper, bb_lower, sma_20, ema_50)

        return SignalResult(
            symbol=symbol,
            price=price,
            volume=volume,
            change_pct=change_pct,
            rsi=rsi,
            macd=macd,
            macd_signal=macd_sig,
            macd_hist=macd_hist,
            bb_upper=bb_upper,
            bb_mid=bb_mid,
            bb_lower=bb_lower,
            sma_20=sma_20,
            ema_50=ema_50,
            composite=composite,
            confidence=confidence,
        )


def _build_dataframe(raw: list[dict]) -> Optional[pd.DataFrame]:
    try:
        df = pd.DataFrame(raw)
        for col in ("open_price", "close_price", "high_price", "low_price"):
            if col in df.columns:
                df[col.replace("_price", "")] = pd.to_numeric(df[col], errors="coerce")
        if "close" not in df.columns and "close_price" in df.columns:
            df["close"] = pd.to_numeric(df["close_price"], errors="coerce")
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df = df.dropna(subset=["close"])
        df = df.reset_index(drop=True)
        return df
    except Exception as exc:
        logger.warning("DataFrame build failed: %s", exc)
        return None
