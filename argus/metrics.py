"""Prometheus metrics registry for Argus.

Import from here — never define metrics in individual modules to avoid
duplicate-registration errors on reload.
"""

from prometheus_client import Counter, Gauge, Histogram

# ── Counters ──────────────────────────────────────────────────────────────────

cassandra_runs_total = Counter(
    "argus_cassandra_runs_total",
    "CassandraAgent run attempts",
    ["result"],  # "success" | "failure"
)

trades_total = Counter(
    "argus_trades_total",
    "Filled trade orders",
    ["action", "account"],  # action: "buy" | "sell"
)

decisions_total = Counter(
    "argus_decisions_total",
    "AI decisions produced",
    ["decision", "account"],  # decision: "BUY" | "SELL" | "HOLD"
)

api_errors_total = Counter(
    "argus_api_errors_total",
    "External API call failures",
    ["service"],  # "broker" | "yfinance" | "claude" | "gemini"
)

# ── Gauges ────────────────────────────────────────────────────────────────────

# Enum pattern: set 1.0 for current regime, 0.0 for all others.
regime_current = Gauge(
    "argus_regime_current",
    "Current market regime (1 = active)",
    ["regime"],  # "bull" | "bear" | "neutral" | "unknown"
)

daily_pnl_dollars = Gauge(
    "argus_daily_pnl_dollars",
    "Session P&L in USD per account",
    ["account"],
)

portfolio_equity_dollars = Gauge(
    "argus_portfolio_equity_dollars",
    "Total portfolio equity in USD per account",
    ["account"],
)

active_positions_count = Gauge(
    "argus_active_positions_count",
    "Number of open positions per account",
    ["account"],
)

# ── Histograms ────────────────────────────────────────────────────────────────

decision_cycle_seconds = Histogram(
    "argus_decision_cycle_seconds",
    "Duration of a full _tick() scan cycle",
    buckets=[0.5, 1, 2, 5, 10, 30, 60],
)

cassandra_duration_seconds = Histogram(
    "argus_cassandra_duration_seconds",
    "Duration of a CassandraAgent.run() call",
    buckets=[1, 2, 5, 10, 30, 60],
)

# ── Helpers ───────────────────────────────────────────────────────────────────

_REGIMES = ("bull", "bear", "neutral", "unknown")


def set_regime(regime: str) -> None:
    """Update the regime enum gauge. Never raises."""
    try:
        r = regime if regime in _REGIMES else "unknown"
        for label in _REGIMES:
            regime_current.labels(regime=label).set(1.0 if label == r else 0.0)
    except Exception:
        pass
