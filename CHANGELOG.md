# Changelog

All notable changes to Argus are documented here.
Versioning follows [Semantic Versioning](https://semver.org): `MAJOR.MINOR.PATCH`
- **MAJOR** — breaking changes to config, API, or data formats
- **MINOR** — new features, backwards-compatible
- **PATCH** — bug fixes, documentation, refactors

---

## [0.5.5] — 2026-07-13

### Added — Market Discovery & MCP Bridge

- **Expanded native screener** (`broker/robinhood.py`) — `get_screener_symbols()` now pulls Robinhood's 100 Most Popular (`rh.markets.get_top_100`), top 20 overall movers (`rh.markets.get_top_movers`), and upcoming-earnings tag stocks (`rh.markets.get_all_stocks_from_market_tag`). These are merged with the existing S&P 500 mover scan. Combined, the daily screener universe grows from ~10 to ~100 symbols before deduplication.
- **MCP Bridge API** (`dashboard/web.py`) — Three new endpoints bridge Claude Code's Robinhood MCP tool access into Argus:
  - `POST /api/inject-candidates` — accepts `[{symbol, reason, category, ttl_hours}]`; merges into the in-memory candidate queue; authenticated.
  - `GET /api/mcp-candidates` — returns all non-expired queued candidates; unauthenticated.
  - `DELETE /api/mcp-candidates` — clears the queue; authenticated.
- **TTL-aware candidate queue** — each injected candidate carries a `ttl_hours` value (default 8h). `get_mcp_candidates()` prunes expired entries on every read. Earnings catalysts typically get 48–96h; popular/movers get 24h.
- **Autopilot MCP integration** (`engine/autopilot.py`) — `_tick_session()` now calls `web_dashboard.get_mcp_candidates()` on every scan cycle and merges the live queue into the signal computation pool alongside the watchlist and daily screener. Logged when MCP symbols enter the universe.

### Added — Risk Guardrails

- **`MIN_CONFIDENCE`** (config) — minimum ensemble confidence (default `0.65`) to execute a BUY. Decisions below this threshold are held regardless of signal direction.
- **`MAX_POSITION_LOSS_USD`** (config) — hard dollar loss cap per position (default `$75`). Triggers stop-loss immediately if unrealized loss reaches this amount, regardless of `STOP_LOSS_PCT`. Prevents gap-down events from exceeding a fixed dollar risk.

### Fixed

- **Stop-loss persistence on paper positions** — `stop_loss` field now stored and restored through `_paper_save`/`_paper_load` cycle; new paper positions initialized with the computed stop price.
- **Charts showing data 2 days behind** — switched yfinance calls from `period=` to explicit `start`/`end` date ranges; `_yf_chart_fallback()` appends today's candle if missing.

### Changed

- Agentic account bypasses large-trade approval gate — all BUY actions on the agentic account auto-execute regardless of dollar amount.
- Dashboard Alerts tab now shows Promote actions with inline Approve/Deny buttons.
- All chart instances have `handleScroll` and `handleScale` set to `false` — zoom/pan disabled.

---

## [0.5.4] — 2026-06-15

### Added — Intelligence & Readiness
- **Go-Live Readiness Scorecard** — Automated tracking of Sample Size, Profit Factor, and AI Calibration.
- **AI Ensemble Audit** — Periodic "Risk Committee" vote where Claude and Gemini must independently approve going live.
- **Lifetime Cost Tracking** — `total_token_usage.json` persists project-wide API costs to calculate true paper ROI.
- **Signal Debouncing** — Skips LLM calls if technical signals haven't significantly changed, saving tokens.

### Added — UI & Charting
- **Advanced Charting** — Added Volume histogram, Synced RSI sub-pane, and SMA-20/EMA-50 overlays.
- **Timeframe Picker** — Switch between 1D, 1W, 1M, 3M, and 1Y views directly on the dashboard.
- **Live Price Patching** — Automatically detects delayed broker history and patches today's candle using live feeds.
- **Persistent Watchlist** — Watchlist is now stored in SQLite and persists across sessions.
- **Visual Feedback** — Table cells now "flash" green/red on real-time price and P&L updates.
- **Ticker Color Logic** — Ticker symbols now correctly turn red/green based on daily price change.

### Changed — Architecture refactor
- **Monolith Decomposition** — Split `main.py` into `argus/engine/autopilot.py` and `argus/engine/session.py`.
- **Strategy Pattern** — Refactored signal engine to use `StrategyProtocol` for pluggable trading logic.
- **Historical Caching** — Implemented SQLite-backed caching for OHLCV data to reduce API latency.
- **Dynamic AI Configuration** — Claude and Gemini models are now configurable via `.env` / Pydantic settings.

### Added — Quality Assurance
- **Test Suite** — Introduced `pytest` with unit tests for Risk Management, Decision Engine, and Signal logic.

---

## [0.5.3] — 2026-06-14


### Fixed — Reliability
- **Parallel signal computation** — watchlist symbols now computed concurrently via `ThreadPoolExecutor(max_workers=8)` in `_tick()`; scan time scales with I/O latency of one symbol instead of N×latency
- **Order fill polling** — `_live_buy()` and `_live_sell()` now poll `get_stock_order_info()` / `get_crypto_order_info()` every 2s (up to 30s) when the order state is not immediately `filled`; live trades no longer silently drop with `filled=False`
- **Flashcard atomic writes** — `_flush()` now writes to a temp file in the same directory then `os.replace()`s it into place; corrupt-on-crash risk eliminated
- **Approval TTL** — pending approvals on the Default account are auto-denied after 30 minutes; stale approvals no longer accumulate indefinitely
- **`max_positions` in AI prompt** — `_build_prompt()` now uses the configured `MAX_POSITIONS` value (was hardcoded to 5)
- **AI error alerting** — when both models fail (error-HOLD), logs at CRITICAL and sends a notification; previously silent
- **Log timestamps UTC** — `log_buffer.py` now emits `HH:MM:SSZ` (UTC) instead of local time
- **Broker call deduplication** — `_update_dashboard()` now reads equity and positions from `_account_cache` populated during the tick; eliminates 2× redundant API calls per account per scan cycle

### Added — Security
- **Dashboard API authentication** — set `DASHBOARD_TOKEN=<secret>` in `.env`; all mutating endpoints (`/api/pause`, `/api/resume`, `/api/close`, `/api/promote`, `/api/approve`, `/api/deny`, `/api/scan-interval`) then require `X-Argus-Token: <secret>` header; token is injected into the served HTML at page load so the browser attaches it automatically; read-only endpoints and SSE remain unauthenticated

### Removed
- **`argus/dashboard/server.py`** — dead code never imported by main; superseded by `web.py` since v0.3.0

---

## [0.5.2] — 2026-06-14

### Fixed — Architecture
- **PDT tracking now functional** — `_execute_sell` detects same-day trades, calls `record_day_trade()`, and persists to `DailyStats.day_trades`; PDT guard reads real data from DB across restarts
- **Position table multi-account safe** — added `account_label` column with `UNIQUE(symbol, account_label)`; `_apply_migrations()` rebuilds existing tables at startup; two accounts can now hold the same symbol without DB corruption
- **SQLite WAL mode** — enabled at startup to prevent read/write lock contention between main loop and FastAPI thread
- **Market session fail-closed** — exception in `get_market_session()` now returns `"closed"` (was `"open"`, causing unintended scan + trade attempts)
- **Market holidays** — `pandas-market-calendars` (already a dep) is now used to detect NYSE holidays; agent no longer scans on closed market days
- **Overnight day rollover** — main loop detects midnight boundary, resets day-trade counter and refreshes session equity so drawdown baseline stays correct for multi-day continuous runs
- **Promote crash fixed** — removed broken `position_size()` call; `_check_promotions` now passes `dollar_value` directly to `_execute_buy` with correct argument order; added `approve_buy()` risk check before re-buy; added error boundary with CRITICAL alert + notification if sell succeeds but re-buy fails; validates account labels
- **`_recent_trades` bounded** — changed to `collections.deque(maxlen=200)` (was growing unbounded, serialized in full every SSE push)
- **DB session leak eliminated** — `_get_session_ref()` removed; all queries use `get_session()` context manager

### Fixed — Security
- **CORS removed** — `CORSMiddleware` with `allow_origins=["*"]` deleted; frontend is same-origin as API so CORS is not needed and was a CSRF vector
- **XSS hardened** — all AI reasoning, symbol names, actions, risk levels, and account labels now wrapped in `escHtml()` before `innerHTML` injection; crafted model output can no longer execute in the browser
- **Thread-safe SSE** — `asyncio.Queue` (not thread-safe across threads) replaced with `stdlib.queue.Queue` for cross-thread state pushes; SSE switched to per-subscriber `asyncio.Queue` pattern — multiple browser tabs now all receive live updates
- **Symbol + account label validation** — added to `GET /api/chart/{symbol}` and `POST /api/promote/{symbol}`; scan-interval capped at 3600s
- **`argus-web` default host** — changed from `0.0.0.0` to `127.0.0.1`
- **Docker hardened** — non-root `USER argus` added to Dockerfile; port changed to `127.0.0.1:8000:8000` (was binding all host interfaces)
- **File permissions** — `argus.log` and `argus_flashcards.jsonl` now `chmod 0600` after write

---

## [0.5.1] — 2026-06-14

### Fixed
- **Session persistence across restarts** — daily P&L baseline and kill switch now survive process restarts
  - New `account_daily_stats` table stores per-account `starting_equity` and `kill_switch_triggered`
  - On startup, `_restore_session_state()` loads today's row: drawdown baseline is the real start-of-day equity, not whatever equity happens to be at restart time
  - Kill switch state persists: if the -5% drawdown limit was hit before a restart, the restarted process cannot trade until the next day
  - On kill switch fire, `_persist_kill_switch()` writes to DB immediately

---

## [0.5.0] — 2026-06-13

### Added
- **Performance tab** — second dashboard tab with full analytics:
  - 6 stat cards: win rate, avg P&L per trade, current streak, avg hold time, best trade, worst trade
  - By-symbol breakdown: trades, wins, win rate, avg P&L per symbol
  - AI Confidence Accuracy: win rate segmented by confidence bucket (≥70%, 50–69%, <50%)
- **Promote to Agentic** — "Promote ↑" button on Default account positions; sells on Default and re-buys same dollar amount on Agentic, atomically queued via `POST /api/promote/{symbol}`
- **$25K PDT Goal tracker** — progress bar per account in web (gradient fill, turns green at goal) and terminal (ASCII block bar); configurable via `EQUITY_GOAL` env var
- **Tab navigation** — Dashboard / Performance tabs in web header; clean `switchTab()` JS, no page reload
- **`FlashcardStore.performance()`** — full analytics engine: streak detection, per-symbol stats, confidence accuracy buckets, best/worst trade, avg hold duration

### Changed
- `flashcard_summary` extended; `performance` key added to state dict on every dashboard push

---

## [0.4.2] — 2026-06-13

### Added
- **Docker support** — `Dockerfile`, `docker-compose.yml`, `.dockerignore`
  - Source in `/app`; runtime data (SQLite, flashcards) written to `/data` volume
  - Secrets injected from host environment variables — never baked into the image
  - `WEB_HOST=0.0.0.0` and `ARGUS_NO_TERMINAL=1` set automatically in container
  - Healthcheck on `/api/status`; `restart: unless-stopped` for auto-start on boot
- **Headless mode** (`ARGUS_NO_TERMINAL=1`) — `NullTerminalDashboard` no-op replaces Rich terminal UI when running without a TTY
- **`docs/mac-mini-setup.md`** — step-by-step checklist for migrating to Mac Mini + Docker + Tailscale

---

## [0.4.1] — 2026-06-13

### Added
- **Branding** — neon hexagon-eye logo, favicon (16×16, 32×32), Apple touch icon (180×180), and GitHub banner; FastAPI serves assets via `/static`
- **`argus-restart` alias** — full clean restart in one command: kills process + destroys tmux session + starts fresh

### Fixed
- **Terminal countdown frozen** — Rich `Live` was repainting the same panel object; wrapped in `_LiveRenderable` so `_render()` is called on every 1 s refresh and the countdown ticks correctly

### Changed
- **Web UI polish pass** — typography hierarchy, card spacing, semi-transparent badge borders, tabular-nums on all dollar values, button hover glow + press animation, log panel near-black background, flashcard indicator grid, modal backdrop blur

---

## [0.4.0] — 2026-06-13

### Added
- **Adaptive scan intervals** — interval automatically adjusts by market session:
  - Market open (9:30 AM–4:00 PM ET): 90 sec
  - Pre-market / after-hours: 180 sec
  - Closed / weekends: 300 sec (crypto-only)
  - Configurable via `INTERVAL_OPEN`, `INTERVAL_PREMARKET`, `INTERVAL_AFTERHOURS`, `INTERVAL_CLOSED`
- **Manual interval override** — dropdown in web Controls card to override adaptive logic for the current session; resets on restart
- **Countdown timer** — live countdown to next scan in web dashboard header, ticking every second without polling
- **Market session badge** — MARKET OPEN / PRE-MARKET / AFTER-HOURS / CLOSED badge in header (web + terminal)
- **Token usage monitor** — per-model daily tracker (resets midnight):
  - Claude: input / output / cache-read tokens + estimated cost (Opus pricing)
  - Gemini: input / output tokens + estimated cost (Flash pricing)
  - Token Usage Today card in web dashboard
  - Cost summary line in terminal header
- **Live log tail** — Agent Log card in web dashboard (filter by level, auto-scroll) and compact log panel in terminal dashboard; backed by in-memory ring buffer (500 entries)
- `GET /api/scan-interval` and `POST /api/scan-interval` endpoints

### Changed
- Main loop sleep is now 1-second chunks for clean shutdown and countdown accuracy

---

## [0.3.0] — 2026-06-13

### Added
- **Claude + Gemini ensemble decision engine** — both models vote in parallel via `ThreadPoolExecutor`; consensus required to execute, disagreement forces HOLD
- Consensus rules: agree → execute (avg confidence); one HOLD → defer; BUY vs SELL contradiction → hard HOLD, risk=high
- `models_used` and `consensus` fields on `TradeDecision`
- `GEMINI_API_KEY` added to keychain secrets and `argus-setup` wizard
- Graceful degradation: runs Claude-only if Gemini key absent or init fails
- `google-genai` dependency (Gemini 2.0 Flash)

### Changed
- `classify_risk()` now accepts `consensus: bool`; disagreement always returns `"high"` regardless of confidence

---

## [0.2.0] — 2026-06-13

### Added
- **Dual-account trading** — Agentic (auto) + Default (approval-gated) accounts
- **Approval queue** — medium/high risk trades on Default account queue in web dashboard with Approve/Deny buttons
- **Decision flashcards** — every trade captured to `argus_flashcards.jsonl` with signal snapshot + AI reasoning; click-to-expand in web dashboard
- **Price chart** — candlestick / line toggle, linear regression prediction line, trade markers (buy/sell arrows on chart)
- **Per-account panels** in web dashboard (cyan = Agentic, purple = Default) and terminal (side-by-side)
- **Hide values toggle** — blur dollar amounts in web dashboard (hidden by default, 🙈/👁 button)
- `dev_mock.py` — fake-data development server (no broker connection needed)
- `argus-tmux`, `argus-start`, `argus-bg`, `argus-stop`, `argus-status`, `argus-log`, `argus-open`, `argus-mock`, `argus-setup` shell aliases

### Changed
- Risk classification: `low` / `medium` / `high` on every `TradeDecision`
- `APPROVAL_THRESHOLD` config field controls which risk levels require approval on the Default account

---

## [0.1.0] — 2026-06-13

### Added
- Initial release
- Robinhood broker integration via `robin_stocks` with OS keychain secrets
- Technical signal engine (RSI, MACD, Bollinger Bands, SMA-20, EMA-50)
- Claude Opus decision engine with adaptive thinking
- Risk manager (PDT tracking, drawdown kill switch, stop-loss, position sizing)
- FastAPI web dashboard with SSE real-time updates
- Rich terminal dashboard
- SQLite trade/signal/position/stats persistence
- Email, SMS (Twilio), and Slack notifications
- Paper trading mode (`PAPER_TRADE=true`)
