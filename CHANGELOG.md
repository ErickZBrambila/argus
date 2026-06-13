# Changelog

All notable changes to Argus are documented here.
Versioning follows [Semantic Versioning](https://semver.org): `MAJOR.MINOR.PATCH`
- **MAJOR** — breaking changes to config, API, or data formats
- **MINOR** — new features, backwards-compatible
- **PATCH** — bug fixes, documentation, refactors

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
