# GEMINI.md - Argus Project Context

This document provides essential context and instructions for AI agents working on the Argus codebase.

## Project Overview

Argus is an automated AI trading agent for Robinhood that uses a **Claude + Gemini ensemble** to make trading decisions. It features an adaptive main loop that synchronizes with NYSE market hours, managing two accounts simultaneously: one fully automated ("Agentic") and one requiring human approval for higher-risk trades ("Default").

### Core Technologies
- **Language:** Python 3.11+
- **AI Models:** Claude 3.5 Opus (Anthropic) & Gemini 2.0 Flash (Google)
- **Broker API:** `robin-stocks` (Robinhood)
- **Signal Engine:** `pandas-ta` for technical indicators
- **Web Dashboard:** FastAPI with Server-Sent Events (SSE) for real-time updates
- **Terminal UI:** Rich-based dashboard
- **Configuration:** Pydantic Settings with OS Keychain integration (`keyring`)
- **Persistence:** SQLite (SQLAlchemy) for trade history; JSONL for decision "flashcards"

## Architecture

- `argus/main.py`: The "Autopilot" orchestration loop.
- `argus/agent/decision.py`: The ensemble decision engine logic.
- `argus/broker/robinhood.py`: Integration with Robinhood (Live & Paper modes).
- `argus/strategy/indicators.py`: Computation of technical signals (RSI, MACD, BB, etc.).
- `argus/risk/manager.py`: PDT tracking, drawdown limits, and stop-loss logic.
- `argus/dashboard/`: Web (FastAPI) and Terminal (Rich) UI components.
- `argus/learning/flashcards.py`: Immutable storage of decision reasoning and outcomes.
- `argus/config.py`: Centralized configuration and secret resolution.

## Building and Running

### Development Setup
1. **Environment:** Use Python 3.11+. Install dependencies: `pip install -e .`
2. **Secrets:** Run `argus-setup` to interactively store API keys and passwords in your OS keychain.
3. **Config:** Copy `.env.example` to `.env` and configure your watchlist and intervals.

### Key Commands
- `argus`: Starts the full autopilot loop with terminal UI.
- `argus-setup`: Launches the interactive secret management tool.
- `argus-web`: Starts only the web dashboard (default: port 8000).
- `argus-tmux`: (via alias) Starts/attaches a tmux session with the full stack.
- `argus-mock`: Runs a development server with mock data for UI testing.

### Testing
- TODO: Identify specific test suite command (currently focused on manual validation and paper trading).

## Development Conventions

- **Secret Management:** Never store secrets in `.env` or hardcode them. Use `argus/config.py`'s `_KeychainSource` which prioritizes environment variables > OS keychain > `.env`.
- **Configuration:** All settings should be added to the `Settings` class in `argus/config.py` using Pydantic fields.
- **Main Loop:** The loop in `main.py` uses a tick-based system with adaptive intervals based on the NYSE market session (pre-market, open, after-hours, closed).
- **Ensemble Logic:** Decisions require consensus between Claude and Gemini. Disagreements result in a "HOLD" with high risk.
- **Flashcards:** Every trade *must* be recorded via `FlashcardStore` to ensure reasoning is preserved for performance analysis.
- **Async/Threading:** The UI (FastAPI) runs in a separate thread from the main autopilot loop. Signal computation uses `ThreadPoolExecutor` for parallel processing of watchlist symbols.

## Key Files
- `pyproject.toml`: Dependency management and entry point definitions.
- `README.md`: Comprehensive documentation of architecture and flows.
- `argus_flashcards.jsonl`: Local database of all AI trading decisions.
- `argus.db`: SQLite database for structured trade and account statistics.
- `Dockerfile` & `docker-compose.yml`: Containerization for persistent deployment.
