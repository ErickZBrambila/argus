# Argus 2.0 — Architecture Blueprint

> Status: **Phase 1 complete (Oct 2026). Phase 5 complete. Remaining phases paused — accumulating live trading data before building out specialist agents.**
> Read the Migration Path (Section 7) before touching any code.
>
> **October 2026 update**: Robinhood MCP integration is live. See Section 9 for what this changes.

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
- **Model**: `pandas_ta` for daily/weekly. Optional Haiku for chart patterns (Phase 2).
- **Modified file**: `argus/strategy/indicators.py` — add `compute_multi_tf()`, keep `compute()` intact.
- **MCP note**: The intraday tier (5min/15min) of `compute_multi_tf()` can source directly from Robinhood MCP's `get_equity_technical_indicators` endpoint, which supports `5min`/`15min` intervals and covers RSI, VWAP, MACD, Bollinger, Stochastic, Ichimoku. Avoids a second set of OHLCV fetches + pandas_ta for intraday. SuperTrend and ADX already live via MCP (daily cached).

### LEDGER — Fundamentals Agent
- **Runs**: Once per symbol per trading day (daily TTL cache). **Core data layer already live as of Oct 2026.**
- **Output**: `FundamentalScore` with PE/P/B/52-week range, analyst ratings (buy%/sell%/avg target/upside%), quarterly financials (revenue growth, net margins, gross profit), politician STOCK Act disclosures (90d window), `prompt_block` ready for Houston. Also writes an **OKF document** to `knowledge/symbols/<SYMBOL>.md` so the profile is human-readable and persists across restarts.
- **Model**: Haiku only for unusual flags (negative forward PE, short float >30%). The data itself is MCP-sourced — no yfinance needed for the core data.
- **Modified file**: `argus/engine/fundamentals_cache.py` — **already updated, all MCP fields live**.
- **MCP replaces yfinance for**: analyst ratings, quarterly financials, politician trades. yfinance yfinance supplemental call is dropped.
- **New capabilities from MCP** (not yet wired):
  - `get_sec_filing_facts` — pull key 10-Q data (revenue, cash, debt) per symbol daily.
  - `get_earnings_results` — EPS actual vs. estimate (beat/miss magnitude) as a momentum signal; add to LEDGER output when building the full Houston bundle.

```markdown
---
type: Symbol Profile
title: NVDA — Fundamentals
tags: [equity, fundamentals, semiconductor]
updated_at: 2026-09-21T09:20:00Z
value_score: 0.71
forward_pe: 34.2
short_float_pct: 1.8
---
NVDA trades at a premium (fwd PE 34.2) but EPS trend is improving (+18% YoY).
Short interest is low. Analyst consensus: Buy (28/35 analysts).
No unusual flags. Value score 0.71 — acceptable for momentum entry.
```

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
- **MCP note**: `get_option_chains` and `get_option_quotes` are in the MCP tool list. Oracle can source chains from MCP (official Robinhood data) instead of yfinance. The math logic is unchanged.

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

Runs Sundays 8pm via APScheduler. Reads `FlashcardStore.performance()`. One Haiku call (~$0.005) produces per-symbol OKF documents in `knowledge/overrides/`:

```
knowledge/
├── overrides/
│   ├── index.md          ← summary of all active overrides
│   ├── TSLA.md
│   ├── AAPL.md
│   └── NVDA.md
└── patterns/
    └── bb_lower_only.md
```

Each override is an **Open Knowledge Format (OKF)** document — plain markdown + YAML frontmatter, readable in any editor, auditable in git diff:

```markdown
---
type: Strategy Override
title: TSLA confidence adjustment
tags: [equity, override, learning-loop]
generated_at: 2026-09-21T20:00:00Z
confidence_delta: 0.08
regime_filter: null
---
Win rate 38% over 13 trades — require higher confidence before buying.
Losses concentrated in Q3 earnings windows. No regime filter applied yet
(need more data — re-evaluate after 20 more trades).
```

Houston reads the `knowledge/` directory at startup and on each weekly review cycle. Confidence delta caps at ±0.15 to prevent over-fitting. `regime_filter` enforced in Python before Houston is called.

**Why OKF here:** The override decisions are Argus's accumulated trading knowledge — keeping them as inspectable text files means you can read exactly why the system is cautious on TSLA, audit changes in git history, and override manually if the AI reasoning looks wrong.

---

## 5. Technology Changes

### Add
- **APScheduler 3.x**: Replace `time.sleep()` loop with cron-style jobs. Radar at 7am, Cassandra at 9:20am, Oracle every 30min, LearnAgent Sundays 8pm.
- **Anthropic prompt caching**: `cache_control: {"type": "ephemeral"}` on system prompt + regime context. ~60% input token cost reduction for Houston.
- Proactive Robinhood session keep-alive: background thread calling `rh.profiles.load_account_profile()` every 45 minutes. 3 lines of code, eliminates reactive reauth.
- **Open Knowledge Format (OKF)**: Markdown + YAML frontmatter for two outputs that benefit from being human-readable and auditable — LearnAgent strategy overrides (`knowledge/overrides/`) and LEDGER symbol profiles (`knowledge/symbols/`). No new dependency — plain file writes. Everything in `knowledge/` is gitignored from the main repo but can be version-controlled separately if desired. Spec: [github.com/GoogleCloudPlatform/knowledge-catalog](https://github.com/GoogleCloudPlatform/knowledge-catalog/tree/main/okf)

### Keep (unchanged)
Python 3.12, FastAPI, SQLite, pandas_ta, Anthropic SDK, Gemini, ntfy.sh, Tailscale, httpx, keyring.

### yfinance scope reduced
yfinance is no longer needed for LEDGER (analyst ratings, financials, politician trades now come from MCP). Still used for sector ETF returns in RADAR and options chains in ORACLE (until MCP option chains are wired).

### NOT needed
Redis, Kafka, Celery, Docker, LangChain/LangGraph, vector database, real-time news streaming, Bloomberg/Refinitiv.

### Robinhood MCP (live as of Phase 1)
`argus/broker/robinhood_mcp.py` — HTTP MCP client reading OAuth from macOS keychain. Daily-cached tools: SuperTrend, ADX, analyst ratings, quarterly financials, earnings calendar, politician trades. Realized P&L and crypto cost basis refresh every ~2h. Order placement tools (`place_equity_order`, `place_crypto_order`) available for Phase 3 migration.

### Phase 3: MCPBroker migration (incremental)
Wire `USE_MCP_BROKER=true` flag for order placement. MCP handles OAuth lifecycle — no more `reauth()`. No big-bang phase needed.

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

### Phase 1 — Foundation ✅ COMPLETE (Oct 2026)
1. ✅ `regime.py` (CassandraAgent). Runs at 9:20am, log-only mode.
2. ✅ `bundle.py` dataclasses (`AnalysisBundle`, `MultiTFSignal`).
3. ✅ Proactive RH keep-alive in `broker/robinhood.py`.
4. ✅ APScheduler wired. Radar + Cassandra scheduled.
5. ✅ `regime` field on `Flashcard`.
6. ✅ **Robinhood MCP client** (`broker/robinhood_mcp.py`) — SuperTrend/ADX, analyst ratings, financials, earnings calendar, politician trades, realized P&L, crypto cost basis. All daily-cached.
7. ✅ LEDGER MCP data: analyst ratings, quarterly financials, politician trades already injected into AI prompts.

### Phase 5 — Mobile approval action URLs ✅ COMPLETE (Oct 2026)
- ntfy notifications send high-priority push with Approve/Deny HTTP action buttons.
- Buttons POST to `http://elcuchomacbookpro.tail19e5ce.ts.net:8000/api/approve/{id}` with `X-Argus-Token` auth.
- Approval gate re-enabled in `_route_buy()`. `LARGE_TRADE_THRESHOLD=0` queues all BUYs.

### Phase 2 — Specialist agents (paused — accumulating live data)
1. `RadarAgent` in `argus/agent/radar.py`. Wire to 7am job. Output to `scan_universe` SQLite table. Drop-in for `get_screener_symbols()`.
   - **New input available**: `run_scan` / `get_scans` MCP tools can serve as an additional high-quality candidate source alongside EDGAR/options/movers.
2. `HeraldAgent` in `argus/agent/herald.py`. News at 9:20am. Adds sentiment to AnalysisBundle.
3. `compute_multi_tf()` in `indicators.py`. Intraday (5min/15min) tier sourced from MCP; daily/weekly from pandas_ta.
4. ~~Extend `FundamentalsCache` with yfinance supplemental data~~ — **done via MCP** (see Phase 1 above). Remaining: wire `get_sec_filing_facts` and `get_earnings_results` (beat/miss) into LEDGER output for Houston bundle.

### Phase 3 (2 weeks) — Houston upgrade (the big switch)
1. Implement `decide_bundle()` in `decision.py`. **Shadow mode**: run in parallel with `decide()`, log both, execute 1.0 only.
2. Shadow for 1 week. Validate Houston's reasoning is sensible.
3. Flip switch on agentic account. Watch 3 days. Then enable on default.
4. Wire `PortfolioGuard`.
5. Enable regime-adjusted sizing. Monitor 1 week.
6. Wire `USE_MCP_BROKER=true` flag for order placement via MCP (`place_equity_order`, `place_crypto_order`, `get_equity_positions`). Incremental migration — no big-bang rewrite.

### Phase 4 (1 week) — Learning loop
1. `LearnAgent.weekly_review()` wired to Sunday 8pm.
2. Houston reads `strategy_overrides.json`. Conservative deltas (max ±0.05 initially).
3. Wire dynamic confidence thresholds.

### ~~Phase 6~~ MCPBroker migration — folded into Phase 3
See Phase 3 item 6 above. No longer a separate phase — incremental behind feature flag.

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
- `knowledge/overrides/<SYMBOL>.md` (OKF — auto-generated by LearnAgent, gitignore)
- `knowledge/symbols/<SYMBOL>.md` (OKF — auto-generated by Ledger, gitignore)

**Modified files**:
- `argus/engine/autopilot.py` — APScheduler, PortfolioGuard, AnalysisBundle, regime reads
- `argus/agent/decision.py` — add `decide_bundle()` alongside `decide()`
- `argus/strategy/indicators.py` — add `compute_multi_tf()`
- `argus/engine/fundamentals_cache.py` — ✅ MCP data live; remaining: SEC filings + earnings beat/miss for Houston bundle
- `argus/screener/market_intelligence.py` — return `OptionsSignal` dataclasses
- `argus/risk/manager.py` — read regime multiplier for sizing
- `argus/notifications/notifier.py` — ✅ action buttons live (Approve/Deny via Tailscale)
- `argus/storage/models.py` — add `scan_universe`, `regime_state` tables
- `argus/broker/robinhood.py` — ✅ proactive keep-alive live
- `argus/broker/robinhood_mcp.py` — ✅ new file, MCP client live
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

---

## 9. MCP Impact Assessment (Oct 2026)

### What MCP replaced / made redundant
| Original Plan | MCP Reality |
|---|---|
| yfinance analyst ratings for LEDGER | `get_equity_analyst_ratings` — live, daily cached |
| yfinance quarterly financials for LEDGER | `get_financials` — live, daily cached |
| Politician trades as future Phase 2 enhancement | `get_politician_trades` — live, 90d window, daily cached |
| Phase 5 ntfy action URLs as a separate phase | Done in Phase 1 alongside MCP work |
| Phase 6 MCPBroker as big-bang rewrite | Incremental — order placement tools ready, migrate in Phase 3 behind flag |
| Haiku call for LEDGER unusual flag detection | Still applicable, but the data source is MCP not yfinance |

### What MCP simplified
| Agent | Simplification |
|---|---|
| ATLAS multi-TF | Intraday (5min/15min) tier sources from MCP instead of extra OHLCV + pandas_ta run |
| ORACLE | Option chains available from `get_option_chains` / `get_option_quotes` (official data, no yfinance) |

### What MCP leaves unaffected
RADAR, CASSANDRA (uses no external data), PortfolioGuard, LearnAgent/OKF loop, Houston bundle format, APScheduler, prompt caching. These proceed as designed when we resume.

### New capabilities MCP unlocks (not yet wired)
- **`get_sec_filing_facts`**: Pull key 10-Q data (revenue, cash, debt) per symbol — add to LEDGER Houston bundle.
- **`get_earnings_results`**: EPS beat/miss magnitude — momentum signal not in the current design. Add to LEDGER.
- **`run_scan` / `get_scans`**: Robinhood server-side screener — use as an additional candidate source in RADAR alongside EDGAR/options/movers.
