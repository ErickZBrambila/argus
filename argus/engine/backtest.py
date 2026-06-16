"""Walk-forward backtester using DefaultStrategy (indicators only, no LLM)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from argus.strategy.indicators import DefaultStrategy, _build_dataframe, _validate_symbol

logger = logging.getLogger(__name__)

_STRATEGY = DefaultStrategy()
_START_EQUITY = 10_000.0
_TRADE_SIZE = 1_000.0     # dollars per position
_STOP_LOSS_PCT = 0.05     # 5% stop-loss


@dataclass
class BacktestResult:
    symbol: str
    span: str
    start_date: str
    end_date: str
    total_return_pct: float
    win_rate: float
    profit_factor: float
    max_drawdown_pct: float
    trade_count: int
    equity_curve: list[dict] = field(default_factory=list)   # [{date, equity}]
    trades: list[dict] = field(default_factory=list)          # [{date, action, price, pnl}]

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "span": self.span,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "total_return_pct": round(self.total_return_pct, 2),
            "win_rate": round(self.win_rate, 3),
            "profit_factor": round(self.profit_factor, 2),
            "max_drawdown_pct": round(self.max_drawdown_pct, 2),
            "trade_count": self.trade_count,
            "equity_curve": self.equity_curve,
            "trades": self.trades,
        }


class BacktestEngine:
    def __init__(self, broker) -> None:
        self._broker = broker

    def run(self, symbol: str, span: str = "year") -> BacktestResult:
        symbol = _validate_symbol(symbol)
        raw = self._broker.get_historical_prices(symbol, span=span, interval="day")
        if not raw:
            # Try yfinance fallback for any symbol
            raw = self._yf_fallback(symbol, span)
        if len(raw) < 60:
            raise ValueError(f"{symbol}: need ≥60 bars for backtest, got {len(raw)}")

        df = _build_dataframe(raw)
        if df is None or len(df) < 60:
            raise ValueError(f"{symbol}: insufficient clean data")

        try:
            import pandas_ta as ta
        except ImportError:
            raise RuntimeError("pandas_ta not installed")

        # Pre-compute all indicators on the full series to avoid re-computing per bar
        # (no lookahead bias because we only read up to row i on each step)
        df.ta.rsi(length=14, append=True)
        df.ta.macd(fast=12, slow=26, signal=9, append=True)
        df.ta.bbands(length=20, std=2, append=True)
        df.ta.sma(length=20, append=True)
        df.ta.ema(length=50, append=True)
        df = df.replace({np.nan: None})

        cash = _START_EQUITY
        position: dict | None = None   # {shares, entry_price, entry_date}
        trades: list[dict] = []
        equity_curve: list[dict] = []
        peak_equity = _START_EQUITY

        for i in range(50, len(df)):
            row = df.iloc[i]
            price = float(row["close"])
            date = str(raw[i].get("begins_at", i))

            def _get(col: str) -> float | None:
                v = row.get(col)
                return None if v is None else float(v)

            rsi      = _get("RSI_14")
            macd_h   = _get("MACDh_12_26_9")
            bb_upper = _get("BBU_20_2.0")
            bb_lower = _get("BBL_20_2.0")
            sma_20   = _get("SMA_20")
            ema_50   = _get("EMA_50")

            composite, confidence = _STRATEGY.score(
                price, rsi, macd_h, bb_upper, bb_lower, sma_20, ema_50
            )

            # Stop-loss check
            if position and price <= position["entry_price"] * (1 - _STOP_LOSS_PCT):
                proceeds = position["shares"] * price
                pnl = proceeds - _TRADE_SIZE
                cash += proceeds
                trades.append({"date": date, "action": "STOP", "price": price, "pnl": round(pnl, 2)})
                position = None

            # Strategy signals
            if composite == "bullish" and position is None and cash >= _TRADE_SIZE:
                shares = _TRADE_SIZE / price
                cash -= _TRADE_SIZE
                position = {"shares": shares, "entry_price": price, "entry_date": date}
                trades.append({"date": date, "action": "BUY", "price": price, "pnl": 0})

            elif composite == "bearish" and position is not None:
                proceeds = position["shares"] * price
                pnl = proceeds - _TRADE_SIZE
                cash += proceeds
                trades.append({"date": date, "action": "SELL", "price": price, "pnl": round(pnl, 2)})
                position = None

            # Mark-to-market equity
            unrealized = (position["shares"] * price) if position else 0.0
            equity = cash + unrealized
            peak_equity = max(peak_equity, equity)
            equity_curve.append({"date": date, "equity": round(equity, 2)})

        # Close any open position at last price
        if position:
            last_price = float(df.iloc[-1]["close"])
            proceeds = position["shares"] * last_price
            pnl = proceeds - _TRADE_SIZE
            cash += proceeds
            trades.append({"date": equity_curve[-1]["date"], "action": "SELL", "price": last_price, "pnl": round(pnl, 2)})

        # Metrics
        final_equity = cash
        total_return_pct = (final_equity - _START_EQUITY) / _START_EQUITY * 100
        max_drawdown_pct = self._max_drawdown(equity_curve)

        closed = [t for t in trades if t["action"] in ("SELL", "STOP")]
        wins = [t for t in closed if t["pnl"] > 0]
        gross_profit = sum(t["pnl"] for t in closed if t["pnl"] > 0)
        gross_loss = abs(sum(t["pnl"] for t in closed if t["pnl"] < 0))
        win_rate = len(wins) / len(closed) if closed else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0)

        return BacktestResult(
            symbol=symbol,
            span=span,
            start_date=equity_curve[0]["date"] if equity_curve else "",
            end_date=equity_curve[-1]["date"] if equity_curve else "",
            total_return_pct=total_return_pct,
            win_rate=win_rate,
            profit_factor=profit_factor,
            max_drawdown_pct=max_drawdown_pct,
            trade_count=len(closed),
            equity_curve=equity_curve,
            trades=trades,
        )

    @staticmethod
    def _max_drawdown(curve: list[dict]) -> float:
        if not curve:
            return 0.0
        peak = curve[0]["equity"]
        max_dd = 0.0
        for pt in curve:
            eq = pt["equity"]
            peak = max(peak, eq)
            dd = (peak - eq) / peak * 100 if peak > 0 else 0.0
            max_dd = max(max_dd, dd)
        return max_dd

    def _yf_fallback(self, symbol: str, span: str) -> list[dict]:
        try:
            import yfinance as yf
            period_map = {
                "day": "5d", "week": "1mo", "month": "1mo",
                "3month": "3mo", "year": "1y", "5year": "5y",
            }
            period = period_map.get(span, "1y")
            df = yf.download(symbol, period=period, interval="1d", progress=False, auto_adjust=True)
            if df.empty:
                return []
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [c.lower() for c in df.columns]
            result = []
            for ts, row in df.iterrows():
                result.append({
                    "begins_at": str(ts.date()),
                    "open_price": str(row.get("open", 0)),
                    "close_price": str(row.get("close", 0)),
                    "high_price": str(row.get("high", 0)),
                    "low_price": str(row.get("low", 0)),
                    "volume": int(row.get("volume", 0) or 0),
                    "symbol": symbol,
                })
            return result
        except Exception as exc:
            logger.debug("yfinance backtest fallback failed for %s: %s", symbol, exc)
            return []
