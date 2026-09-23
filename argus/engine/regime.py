"""CassandraAgent — deterministic market regime classifier (Argus 2.0 Phase 1).

Runs once each morning (9:20am ET). Log-only: it writes a row to the
``regime_state`` table and emits an INFO line. NO LLM calls, NO trading
decisions, NO consumers yet. Uses VIX, SPY vs its 200-day SMA, and SPY RSI.

Data is pulled from Yahoo Finance (yfinance) — no auth required, matching the
fallback data path already used across the codebase.
"""

from __future__ import annotations

import datetime
import logging

logger = logging.getLogger(__name__)

_UTC = datetime.UTC


class CassandraAgent:
    def run(self) -> None:
        """Classify the current regime and persist a snapshot. Never raises."""
        from argus.metrics import (
            cassandra_duration_seconds,
            cassandra_runs_total,
            set_regime,
        )
        with cassandra_duration_seconds.time():
            try:
                vix, spy_vs_200d, spy_rsi = self._fetch_inputs()
                regime, notes = self._classify(vix, spy_vs_200d, spy_rsi)
                cassandra_runs_total.labels(result="success").inc()
            except Exception as exc:  # pragma: no cover - defensive, log-only agent  # noqa: BLE001
                logger.warning("CassandraAgent failed: %s", exc)
                regime, vix, spy_vs_200d, spy_rsi, notes = "unknown", None, None, None, str(exc)
                cassandra_runs_total.labels(result="failure").inc()

        set_regime(regime)
        self._persist(regime, vix, spy_vs_200d, spy_rsi, notes)
        logger.info(
            "Regime: %s (VIX=%.1f, SPY vs 200d=%.1f%%)",
            regime,
            vix if vix is not None else float("nan"),
            spy_vs_200d if spy_vs_200d is not None else float("nan"),
        )

    def _fetch_inputs(self) -> tuple[float | None, float | None, float | None]:
        import numpy as np
        import pandas as pd  # noqa: F401  (pandas_ta accessor needs pandas imported)
        import pandas_ta  # noqa: F401  (registers the .ta DataFrame accessor)
        import yfinance as yf

        # VIX: latest close of ^VIX
        vix_df = yf.download("^VIX", period="5d", interval="1d", progress=False, auto_adjust=True)
        vix = float(vix_df["Close"].iloc[-1].item()) if not vix_df.empty else None

        # SPY: need ~1y of daily bars for a 200-day SMA and RSI-14
        spy = yf.download("SPY", period="1y", interval="1d", progress=False, auto_adjust=True)
        if spy.empty or "Close" not in spy:
            return vix, None, None
        # Flatten possible MultiIndex columns from yfinance
        close = spy["Close"]
        if hasattr(close, "columns"):
            close = close.iloc[:, 0]
        close = close.dropna()

        price = float(close.iloc[-1])
        sma_200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
        spy_vs_200d = ((price - sma_200) / sma_200 * 100.0) if sma_200 else None

        rsi_series = close.to_frame(name="close").ta.rsi(length=14)
        spy_rsi = None
        if rsi_series is not None and len(rsi_series) and not np.isnan(rsi_series.iloc[-1]):
            spy_rsi = float(rsi_series.iloc[-1])

        return vix, spy_vs_200d, spy_rsi

    def _classify(
        self,
        vix: float | None,
        spy_vs_200d: float | None,
        spy_rsi: float | None,
    ) -> tuple[str, str]:
        """Deterministic regime rules. Returns (regime, notes)."""
        if spy_vs_200d is None or vix is None:
            return "unknown", "insufficient data (VIX or SPY 200d unavailable)"

        # Bull: SPY comfortably above its 200-day SMA with subdued volatility.
        if spy_vs_200d > 1.0 and vix < 20.0:
            regime = "bull"
        # Bear: SPY below its 200-day SMA, or elevated volatility.
        elif spy_vs_200d < -1.0 or vix > 28.0:
            regime = "bear"
        else:
            regime = "neutral"

        notes = f"vix={vix:.1f} spy_vs_200d={spy_vs_200d:.2f}% rsi={spy_rsi}"
        return regime, notes

    def _persist(
        self,
        regime: str,
        vix: float | None,
        spy_vs_200d: float | None,
        spy_rsi: float | None,
        notes: str | None,
    ) -> None:
        from argus.storage.models import RegimeState, get_session

        try:
            with get_session() as session:
                session.add(RegimeState(
                    timestamp=datetime.datetime.now(_UTC),
                    regime=regime,
                    vix=vix,
                    spy_vs_200d_pct=spy_vs_200d,
                    spy_rsi=spy_rsi,
                    notes=notes,
                ))
        except Exception as exc:  # pragma: no cover - log-only agent must not crash  # noqa: BLE001
            logger.warning("CassandraAgent could not persist regime_state: %s", exc)
