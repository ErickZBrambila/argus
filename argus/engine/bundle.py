"""Analysis data containers for Argus 2.0.

Phase 1: pure data structures with NO consumers yet. These are the transport
shapes that later phases (multi-timeframe confluence, regime-aware sizing) will
populate and read. Nothing in the live trading path touches them today.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass


@dataclass
class MultiTFSignal:
    """Daily + weekly technical snapshot for a single symbol.

    All fields are nullable — a timeframe may be unavailable (e.g. not enough
    weekly bars for a recent listing). No logic lives here; it is a container.
    """

    symbol: str

    # Daily timeframe
    daily_rsi: float | None = None
    daily_macd: float | None = None
    daily_bb_position: str | None = None  # "above_upper" | "below_lower" | "inside"
    daily_trend: str | None = None        # "up" | "down" | "flat"

    # Weekly timeframe
    weekly_rsi: float | None = None
    weekly_macd: float | None = None
    weekly_bb_position: str | None = None
    weekly_trend: str | None = None


@dataclass
class AnalysisBundle:
    """Everything an analysis pass knows about one symbol at a point in time.

    Aggregates the multi-timeframe signal with optional sentiment and the
    prevailing market regime. Consumed by nothing in Phase 1.
    """

    symbol: str
    timestamp: datetime.datetime
    multi_tf: MultiTFSignal
    sentiment: str | None = None  # free-form sentiment label, nullable
    regime: str | None = None     # "bull" | "bear" | "neutral" | "unknown"
