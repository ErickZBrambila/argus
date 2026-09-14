# Argus 2.0 — Architecture Blueprint

> Status: **Design phase** — implementation begins September 2026.
> Read the Migration Path (Section 7) before touching any code.

## What Opus found in the codebase that shapes this design

- `FlashcardStore.performance()` already returns `by_symbol` win rates — adaptive thresholds are ready to wire in.
- `web_dashboard.get_mcp_candidates()` already has an MCP injection hook in `autopilot._tick()` — formalized "MCP bridge" where agent candidates can be injected today.
- `FundamentalsCache.to_prompt_block()` already produces LLM-ready output — Ledger extends rather than replaces it.
- `get_screener_symbols()` already combines three sources (movers, EDGAR insiders, options flow) — Radar formalizes and widens this.
- `robin_stocks` calls are mostly isolated inside `broker/robinhood.py` — broker interface is clean enough to swap.
- There is no scheduler — the loop is a `time.sleep()` in `autopilot.run()`. Daily sub-agents need cron-style scheduling.

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          ARGUS 2.0 — FULL SYSTEM                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  PRE-MARKET (7:00am–9:25am ET)  — runs once per day                        │
│  ┌─────────────┐  ┌──────────────────┐  ┌───────────────────────────────┐  │
│  │   RADAR     │  │    CASSANDRA     │  │    ORACLE (Options Flow)      │  │
│  │  Market     │  │  Regime Detect   │  │  Scans scan_universe for      │  │
│  │  Scanner    │  │  SPY + VIX +     │  │  unusual call/put activity    │  │
│  │             │  │  breadth         │  │  (yfinance option chains)     │  │
│  │  Outputs:   │  │                  │  │                               │  │
│  │  scan_      │  │  Outputs:        │  │  Outputs:                     │  │
│  │  universe   │  │  RegimeState     │  │  options_signals list         │  │
│  │  (~50 syms) │  │  (bull/bear/     │  │  flagged with call/put ratio  │  │
│  │             │  │  sideways/vol)   │  │                               │  │
│  │  Model:     │  │  + size/thresh   │  │  Model: Python (yfinance,     │  │
│  │  python +   │  │  multipliers     │  │  no LLM needed — ratio math)  │  │
│  │  Haiku      │  │                  │  │                               │  │
│  │  (ranking)  │  │  Model:          │  │                               │  │
│  │             │  │  Python only     │  │                               │  │
│  │             │  │  (deterministic) │  │                               │  │
│  └──────┬──────┘  └────────┬─────────┘  └─────────────┬─────────────────┘  │
│         │                  │                           │                    │
│         └──────────────────┼───────────────────────────┘                    │
│                            │ (written to SQLite: scan_universe, regime)     │
├────────────────────────────┼────────────────────────────────────────────────┤
│                            │                                                │
│  MARKET HOURS (9:30–4pm)   │  — runs every scan tick (90s during open)     │
│                            ▼                                                │
│         ┌──────────────────────────────────────────────────┐               │
│         │            SCAN UNIVERSE (~50 symbols)           │               │
│         │  = watchlist ∪ Radar output ∪ open positions     │               │
│         └──────────────────────┬───────────────────────────┘               │
│                                │ parallel fetch                            │
│                                ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    SPECIALIST AGENTS (parallel)                      │   │
│  │                                                                      │   │
│  │  ┌───────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │   │
│  │  │     ATLAS     │  │    LEDGER    │  │         HERALD           │  │   │
│  │  │  (Technical)  │  │ (Fundament.) │  │    (News/Sentiment)      │  │   │
│  │  │               │  │              │  │                          │  │   │
│  │  │  5min/15min/  │  │  PE, P/B,    │  │  Robinhood news feed     │  │   │
│  │  │  daily/weekly │  │  EPS trend,  │  │  → sentiment score       │  │   │
│  │  │  RSI/MACD/BB/ │  │  revenue     │  │  (-1 to +1)              │  │   │
│  │  │  SMA/EMA/ATR  │  │  growth,     │  │  + key headlines         │  │   │
│  │  │  support/res  │  │  value score │  │  + catalyst flag         │  │   │
│  │  │               │  │              │  │                          │  │   │
│  │  │  Model:       │  │  Model:      │  │  Model:                  │  │   │
│  │  │  pandas_ta    │  │  Haiku       │  │  Haiku                   │  │   │
│  │  │  + Haiku for  │  │  (daily TTL  │  │  (per-symbol once/day    │  │   │
│  │  │  patterns     │  │  cache)      │  │  unless breaking news)   │  │   │
│  │  └──────┬────────┘  └──────┬───────┘  └────────────┬─────────────┘  │   │
│  └─────────┼──────────────────┼────────────────────────┼───────────────┘   │
│            │                  │                        │                   │
│            └──────────────────┼────────────────────────┘                   │
│                               ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    AnalysisBundle per symbol                        │   │
│  │  { regime, technical (multi-TF), fundamental, sentiment, options,   │   │
│  │    portfolio_state, performance_history (from FlashcardStore) }     │   │
│  └────────────────────────────┬─────────────────────────────────────────┘  │
│                               ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         HOUSTON                                      │   │
│  │                (Orchestrator — Final Decision Engine)                │   │
│  │  Claude Sonnet ─────────────── Gemini Flash                         │   │
│  │  (structured bundle JSON)     (same bundle)                         │   │
│  │               └──────── ensemble ────────┘                          │   │
│  │  Output: TradeDecision (BUY/SELL/HOLD + confidence + size_override) │   │
│  └────────────────────────────┬─────────────────────────────────────────┘  │
│                               ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    PORTFOLIO RISK MANAGER 2.0 (PortfolioGuard)       │   │
│  │  ├── Sector concentration (max 40% any sector across both accounts) │   │
│  │  ├── Regime-adjusted sizing (bear → 50% position size)              │   │
│  │  ├── Correlation guard (block r > 0.85 with existing position)      │   │
│  │  ├── PDT tracking (per-account, unchanged from 1.0)                 │   │
│  │  └── Kill switch + all 1.0 guards (unchanged)                       │   │
│  └────────────────────────────┬─────────────────────────────────────────┘  │
│                   ┌───────────┴───────────┐                               │
│                   ▼                       ▼                               │
│         ┌──────────────────┐  ┌────────────────────────────┐             │
│         │  Auto-execute    │  │  Approval Gate             │             │
│         │  (agentic / low  │  │  (default acct, any risk)  │             │
│         │   risk)          │  │  ntfy mobile action URLs   │             │
│         └─────────┬────────┘  └─────────────┬──────────────┘             │
│                   └─────────────┬────────────┘                           │
│                                 ▼                                         │
│         Broker Layer (RobinhoodBroker → MCPBroker in Phase 4)            │
│                                                                           │
│  WEEKLY LEARNING LOOP (Sundays 8pm):                                      │
│  LearnAgent reads FlashcardStore.performance() → Haiku analysis          │
│  → strategy_overrides.json (per-symbol confidence deltas, regime filters) │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Agent Roster

### RADAR — Market Scout
- **Runs**: 7:00am ET Mon–Fri
- **Output**: `ScanUniverse` (~50 symbols scored 0–100) written to SQLite
- **Scoring**: +35 insider buy, +30 options flow, +25 SP500 mover, +20 sector momentum, +10 popular. Bear market ×0.7, volatile ×0.5.
- **Model**: Python (deterministic). Optional Haiku call if >80 candidates: rank to top 50.
- **New file**: `argus/agent/radar.py`

**Key new signal: sector rotation.** Fetch 5-day returns for 9 sector ETFs (XLK, XLF, XLE, XLV, XLY, XLI, XLB, XLU, XLRE). Boost symbols in the top 2 performing sectors. Argus 1.0 is sector-blind — this alone improves candidate quality significantly.

### CASSANDRA — Regime Detector
- **Runs**: 9:20am ET. Re-runs if VIX spikes >5 points intraday.
- **Output**: `RegimeState` (bull/bear/sideways/volatile) with `position_size_mult` and `confidence_threshold_adj`
- **Model**: Pure Python math. No LLM. Cost: $0.
- **New file**: `argus/engine/regime.py`

```
volatile (VIX>30): size=0.3x, conf+0.10, no day trades
bear (SPY<200d SMA, VIX>22): size=0.5x, conf+0.08
bull (SPY>200d SMA, VIX<20, 5d>0): size=1.0x, conf+0.00
sideways (else): size=0.7x, conf+0.04
```

### ATLAS — Technical Analyst
- **Runs**: Per symbol, every tick. Heavily cached.
- **Output**: `MultiTFSignal` — weekly trend, daily (existing), 15min momentum, 5min entry quality, support/resistance
- **Model**: `pandas_ta`. Optional Haiku for chart patterns (Phase 2).
- **Modified file**: `argus/strategy/indicators.py` — add `compute_multi_tf()`, keep `compute()` intact.

### LEDGER — Fundamentals Agent
- **Runs**: Once per symbol per trading day (daily TTL cache).
- **Output**: `FundamentalScore` with forward PE (yfinance), analyst ratings, short float %, `value_score` 0–1, `prompt_block` ready for Houston.
- **Model**: Haiku only for unusual data (negative forward PE, short float >30%).
- **Modified file**: `argus/engine/fundamentals_cache.py`

### HERALD — News/Sentiment Agent
- **Runs**: 9:20am per symbol. 1-hour TTL cache.
- **Output**: `SentimentResult` — score (-1 to +1), `catalyst_detected`, `risk_flag`, top 3 headlines, `prompt_block`
- **Model**: Haiku (~$0.0002/call). Prompt: classify 5 headlines, flag catalysts/risks.
- **New file**: `argus/agent/herald.py`

### ORACLE — Options Flow Agent
- **Runs**: 9:30am open + every 30 minutes.
- **Output**: `OptionsSignal` — call/put ratio, put wall, call ceiling, flow direction
- **Model**: Pure Python math. Extends existing `_check_symbol_options()`.
- **Modified file**: `argus/screener/market_intelligence.py`

### HOUSTON — Orchestrator and Final Decision Engine
- **Runs**: Per symbol when AnalysisBundle has changed (debounced).
- **Model**: Claude Sonnet + Gemini Flash in parallel (same as 1.0 ensemble). No cheaper model here.
- **Key change**: Instead of flat raw-indicator prompt, Houston receives a structured JSON bundle with all specialist conclusions. "Specialized experts brief the judge" pattern.

```json
{
  "symbol": "AAPL",
  "regime": {"state": "bull", "vix": 18.5},
  "technical": {"daily": {"composite": "bullish", "rsi": 58.3}, "weekly": {"trend": "up"}, "15min": {"momentum": "rising"}},
  "fundamental": {"pe": 29.1, "forward_pe": 27.8, "eps_trend": "improving"},
  "sentiment": {"score": 0.42, "catalyst": false, "risk_flag": false},
  "options": {"call_put_ratio": 2.8, "flow_direction": "bullish"},
  "portfolio": {"tech_sector_pct": 0.28},
  "history": {"symbol_win_rate": 0.67, "last_3_trades": ["+3.2%", "+1.1%", "-2.8%"]}
}
```

New output field: `size_override` (0.5–1.5) — Houston can express conviction through sizing.

---

## 3. PortfolioGuard — New Risk Layer

**New file**: `argus/risk/portfolio_guard.py`

Sees BOTH accounts combined. Runs after Houston, before execution:
- Sector concentration: max 40% in any sector across both accounts combined
- Correlation guard: block buy if r > 0.85 with any open position (90-day correlation matrix)
- Regime-adjusted sizing: `final_amount = base × regime.position_size_mult × houston.size_override`
- All 1.0 guards preserved and unchanged (ATR stop, PDT, drawdown kill switch, earnings guard, price book guard, 48h hold, 4h cooldown, tax lot gate)

---

## 4. Learning Loop

**New file**: `argus/learning/reviewer.py`

Runs Sundays 8pm via APScheduler. Reads `FlashcardStore.performance()`. One Haiku call (~$0.005) produces `strategy_overrides.json`:

```json
{
  "symbol_overrides": {
    "TSLA": {"confidence_delta": 0.08, "note": "Win rate 38% — require higher confidence"},
    "AAPL": {"confidence_delta": -0.05, "note": "Win rate 72% — can be less restrictive"},
    "NVDA": {"regime_filter": "bull_only", "note": "3 of 3 losses were in sideways regime"}
  },
  "pattern_overrides": {
    "price_vs_bb_lower_only": {"block": true, "note": "31% win rate — require MACD confirmation"}
  },
  "correlation_matrix": {"NVDA_AMD": 0.91}
}
```

Confidence delta caps at ±0.15 to prevent over-fitting. `regime_filter` enforced in Python before Houston is called.

---

## 5. Technology Changes

### Add
- **APScheduler 3.x**: Replace `time.sleep()` loop with cron-style jobs. Radar at 7am, Cassandra at 9:20am, Oracle every 30min, LearnAgent Sundays 8pm.
- **Anthropic prompt caching**: `cache_control: {"type": "ephemeral"}` on system prompt + regime context. ~60% input token cost reduction for Houston.
- Proactive Robinhood session keep-alive: background thread calling `rh.profiles.load_account_profile()` every 45 minutes. 3 lines of code, eliminates reactive reauth.

### Keep (unchanged)
Python 3.12, FastAPI, SQLite, pandas_ta, yfinance, Anthropic SDK, Gemini, ntfy.sh, Tailscale, httpx, keyring.

### NOT needed
Redis, Kafka, Celery, Docker, LangChain/LangGraph, vector database, real-time news streaming, Bloomberg/Refinitiv.

### Phase 4 (later): MCP Broker
Replace `robin_stocks` with `MCPBroker` behind `USE_MCP_BROKER=true` feature flag. MCP server handles OAuth lifecycle — no more `reauth()`.

---

## 6. Cost Model

| Component | Monthly |
|-----------|---------|
| Claude Sonnet (Houston, ~1,300 calls/day with debouncing, 22 trading days) | $55–$80 |
| Claude Haiku (Ledger + Herald + Radar) | <$1 |
| Gemini Flash (parallel with Sonnet) | $4–$5 |
| Infrastructure (Mac power, free-tier services) | $3 |
| **Total** | **$62–$88/month** |

Current 1.0 cost: ~$15–20/month. 2.0 is 3–5x more — the tradeoff for multi-domain analysis. If over budget, extend debounce RSI threshold from 3pt to 5pt (cuts ~30% of calls) or switch default account to Haiku.

---

## 7. Migration Path (6 Phases — Nothing Breaks)

### Phase 1 (1 week) — Foundation, zero behavior change
1. Add `regime.py` (CassandraAgent). Run at 9:20am, write to SQLite, **log only — don't use for decisions**.
2. Add `bundle.py` dataclasses (`AnalysisBundle`, `MultiTFSignal`). No consumers yet.
3. Add proactive RH keep-alive (3 lines in `broker/robinhood.py`).
4. Add APScheduler dependency. Schedule Radar + Cassandra. Don't change tick logic.
5. Add `regime` field to `Flashcard` (defaults to `"unknown"` for historical records).

### Phase 2 (2 weeks) — Specialist agents, parallel to existing
1. `RadarAgent` in `argus/agent/radar.py`. Wire to 7am job. Output to `scan_universe` SQLite table. Drop-in for `get_screener_symbols()`.
2. `HeraldAgent` in `argus/agent/herald.py`. News at 9:20am. Adds sentiment to AnalysisBundle.
3. `compute_multi_tf()` in `indicators.py`. Keep `compute()` intact.
4. Extend `FundamentalsCache` with yfinance supplemental data.

### Phase 3 (2 weeks) — Houston upgrade (the big switch)
1. Implement `decide_bundle()` in `decision.py`. **Shadow mode**: run in parallel with `decide()`, log both, execute 1.0 only.
2. Shadow for 1 week. Validate Houston's reasoning is sensible.
3. Flip switch on agentic account. Watch 3 days. Then enable on default.
4. Wire `PortfolioGuard`.
5. Enable regime-adjusted sizing. Monitor 1 week.

### Phase 4 (1 week) — Learning loop
1. `LearnAgent.weekly_review()` wired to Sunday 8pm.
2. Houston reads `strategy_overrides.json`. Conservative deltas (max ±0.05 initially).
3. Wire dynamic confidence thresholds.

### Phase 5 (1 week) — Mobile approval action URLs
- ntfy notifications get Approve/Reject action URLs pointing to existing FastAPI endpoints.

### Phase 6 (2+ weeks, after everything stable) — MCP Broker migration
- `MCPBroker` behind feature flag. Paper test 2 weeks before live.

---

## 8. File Map

**New files**:
- `argus/agent/radar.py`
- `argus/agent/herald.py`
- `argus/engine/regime.py`
- `argus/engine/bundle.py`
- `argus/risk/portfolio_guard.py`
- `argus/learning/reviewer.py`
- `argus/broker/mcp_broker.py` (Phase 4)
- `strategy_overrides.json` (auto-generated, gitignore)

**Modified files**:
- `argus/engine/autopilot.py` — APScheduler, PortfolioGuard, AnalysisBundle, regime reads
- `argus/agent/decision.py` — add `decide_bundle()` alongside `decide()`
- `argus/strategy/indicators.py` — add `compute_multi_tf()`
- `argus/engine/fundamentals_cache.py` — yfinance supplemental fields
- `argus/screener/market_intelligence.py` — return `OptionsSignal` dataclasses
- `argus/risk/manager.py` — read regime multiplier for sizing
- `argus/notifications/notifier.py` — add `action_urls` parameter
- `argus/storage/models.py` — add `scan_universe`, `regime_state` tables
- `argus/broker/robinhood.py` — proactive keep-alive thread
- `pyproject.toml` — add `apscheduler>=3.10`

**Untouched** (by design):
- `argus/engine/earnings_guard.py`
- `argus/engine/tax_lot_checker.py`
- `argus/engine/price_book_guard.py`
- `argus/engine/session.py`
- `argus/learning/flashcards.py`
- `argus/dashboard/web.py` (minor portfolio view addition only)
- `argus/config.py`
- `argus/secrets.py`

---

## Key Architectural Decisions

**No message bus**: All agents run in the same Python process. Threading + SQLite is faster and simpler than IPC for a single-machine system.

**No LangChain/LangGraph**: The existing orchestration in `autopilot._tick()` is debuggable Python. Frameworks add abstraction that makes "why did it trade TSLA at 10am Tuesday" harder to answer.

**Debounce preserved**: Primary cost lever. Extended in 2.0 to also debounce on regime state and sentiment catalyst. Net effect: slightly fewer calls in quiet markets, same or more calls when things change.

**Cassandra uses no LLM**: Regime classification is deterministic from 5 numbers (VIX, SPY vs 200d SMA, etc.). Adding an LLM introduces latency, cost, and hallucination risk without improving classification quality.

**Options execution is Phase 2+**: Oracle (signals) is Phase 1 at near-zero cost. Execution requires understanding Greeks, expiry selection, assignment risk — get that wrong and losses are worse than any equity mistake. Do it after everything else is stable.

**Argus stays more autonomous than Bracket22**: Houston makes the final call automatically (unlike Bracket22 where a human reviews). The default account approval gate provides human oversight for large/risky trades. This is the right tradeoff for the two-account setup.
