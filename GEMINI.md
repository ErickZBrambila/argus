# GEMINI.md - Argus Project Context

This document provides essential context and instructions for AI agents working on the Argus codebase.

## Project Overview

Argus is an automated AI trading agent for Robinhood that uses a **Claude + Gemini ensemble** to make trading decisions. It features an adaptive main loop that synchronizes with NYSE market hours, managing two accounts simultaneously: one fully automated ("Agentic") and one requiring human approval for higher-risk trades ("Default").

### Core Technologies
- **Language:** Python 3.11+
- **AI Models:** Claude 3.5 Sonnet / Opus (Anthropic) & Gemini 2.0 / 2.5 Flash (Google)
- **Broker API:** `robin-stocks` (Robinhood)
- **Signal Engine:** `pandas-ta` with a pluggable **Strategy Pattern**
- **Web Dashboard:** FastAPI with SSE for real-time updates and **Advanced Charting** (synced RSI, Volume, SMA/EMA)
- **Terminal UI:** Rich-based dashboard
- **Configuration:** Pydantic Settings with OS Keychain integration (`keyring`)
- **Persistence:** SQLite (SQLAlchemy) for trade history, cached historicals, and a persistent watchlist

## Architecture

- `argus/main.py`: CLI entry point.
- `argus/engine/autopilot.py`: The core orchestration loop and account management.
- `argus/engine/session.py`: NYSE market session detection and adaptive interval logic.
- `argus/agent/decision.py`: The ensemble decision engine and automated **Go-Live Audit**.
- `argus/broker/robinhood.py`: Integration with Robinhood (Live & Paper modes).
- `argus/strategy/indicators.py`: Technical signal computation using pluggable strategies.
- `argus/risk/manager.py`: PDT tracking, drawdown limits, and stop-loss logic.
- `argus/dashboard/`: Web (FastAPI) and Terminal (Rich) UI components.
- `argus/learning/flashcards.py`: Immutable storage of decision reasoning and **Go-Live Readiness** analytics.
- `argus/storage/models.py`: Database models for trades, signals, cached historicals, and the persistent watchlist.

## Key Features

### 1. Go-Live Readiness Scorecard
Argus tracks statistical performance (Sample Size, Profit Factor, Calibration) and lifetime token costs to calculate a readiness score. Transitioning to live trading requires these metrics to be green and both AIs to vote "YES" in a periodic audit.

### 2. Advanced Charting
The dashboard features `lightweight-charts` with synced RSI sub-panes, Volume histograms, SMA-20/EMA-50 overlays, and a timeframe picker (1D, 1W, 1M, 3M, 1Y). It automatically "patches" today's data using live price feeds if the broker history is delayed.

### 3. Efficiency & Resilience
- **Signal Debouncing**: Skips expensive LLM calls if technical signals haven't shifted significantly.
- **Historical Caching**: Caches OHLCV data in SQLite to reduce API latency.
- **Kill Switch**: Automatically halts buying if a -5% daily drawdown is reached.

## Building and Running

### Development Setup
1. **Environment:** Use Python 3.11+. Install dependencies: `pip install -e '.[dev]'`
2. **Secrets:** Run `argus-setup` to store API keys in the OS keychain.
3. **Tests:** Run `pytest tests/` to verify logic.

### Key Commands
- `argus`: Starts the full autopilot loop.
- `argus-web`: Starts the web dashboard (port 8000).
- `argus-setup`: Interactive secret management.
- `argus-service`: Installs as a system service.

## Development Conventions

- **Strategy Pattern:** New trading logic should implement `StrategyProtocol` in `indicators.py`.
- **Secret Management:** Never log or commit secrets. Use `argus/config.py` for all settings.
- **Testing:** Add new test cases to `tests/` for any changes to risk or decision logic.
