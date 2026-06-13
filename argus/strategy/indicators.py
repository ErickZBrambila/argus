"""Technical indicator engine using pandas_ta."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_VALID_SYMBOL_CHARS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.")


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

    def to_dict(self) -> dict:
        return self.__dict__.copy()


class SignalEngine:
    def __init__(self, broker) -> None:
        self._broker = broker

    def compute(self, symbol: str) -> Optional[SignalResult]:
        symbol = _validate_symbol(symbol)
        try:
            return self._compute(symbol)
        except Exception as exc:
            logger.warning("Signal computation failed for %s: %s", symbol, exc)
            return None

    def _compute(self, symbol: str) -> Optional[SignalResult]:
        raw = self._broker.get_historical_prices(symbol, span="3month", interval="day")
        if len(raw) < 50:
            logger.warning("%s: not enough history (%d bars)", symbol, len(raw))
            return None

        df = _build_dataframe(raw)
        if df is None or len(df) < 50:
            return None

        try:
            import pandas_ta as ta
        except ImportError:
            logger.error("pandas_ta not installed; run: pip install pandas-ta")
            return None

        # RSI (14)
        df.ta.rsi(length=14, append=True)
        rsi_col = "RSI_14"

        # MACD (12, 26, 9)
        df.ta.macd(fast=12, slow=26, signal=9, append=True)
        macd_col, macd_sig_col, macd_hist_col = "MACD_12_26_9", "MACDs_12_26_9", "MACDh_12_26_9"

        # Bollinger Bands (20, 2)
        df.ta.bbands(length=20, std=2, append=True)
        bb_lower_col, bb_mid_col, bb_upper_col = "BBL_20_2.0", "BBM_20_2.0", "BBU_20_2.0"

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

        composite, confidence = _score(price, rsi, macd_hist, bb_upper, bb_lower, sma_20, ema_50)

        return SignalResult(
            symbol=symbol,
            price=price,
            volume=volume,
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


def _score(
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
