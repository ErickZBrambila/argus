# Argus — Technical Documentation

## 1. What is Argus?

Argus is an automated AI trading agent for Robinhood that uses a Claude + Gemini ensemble to make BUY/SELL/HOLD decisions from technical indicator signals. It manages two Robinhood accounts simultaneously — one fully automated and one requiring human approval for higher-risk trades — and surfaces everything through a real-time web dashboard and a Rich terminal UI.

---

## 2. Architecture Overview

```mermaid
graph TD
    RH["Robinhood API\n(robin_stocks)"]
    SE["Signal Engine\n(pandas_ta indicators)"]
    DE["Decision Engine\n(Claude + Gemini ensemble)"]
    RM["Risk Manager\n(PDT, drawdown, sizing)"]
    AUT["Agentic Account\nauto_trade=True"]
    DEF["Default Account\napproval queue"]
    WEB["Web Dashboard\n(FastAPI + SSE)"]
    TERM["Terminal Dashboard\n(Rich)"]
    FC["Flashcard Store\n(argus_flashcards.jsonl)"]
    LOG["Log Buffer\n(in-memory ring)"]
    DB["SQLite DB\n(argus.db)"]

    RH -->|"price/history"| SE
    SE -->|"SignalResult"| DE
    DE -->|"TradeDecision"| RM
    RM -->|"approved + dollar_amount"| AUT
    RM -->|"needs approval?"| DEF
    AUT -->|"order"| RH
    DEF -->|"user approves"| RH
    AUT -->|"fill result"| FC
    AUT -->|"fill result"| DB
    DEF -->|"fill result"| FC
    DEF -->|"fill result"| DB
    AUT -->|"state"| WEB
    DEF -->|"state"| WEB
    WEB -->|"SSE push"| TERM
    LOG -->|"last 100 lines"| WEB
    LOG -->|"last 12 lines"| TERM
```

The main loop (`Autopilot`) runs on a configurable interval (default 5 min). On each tick it computes signals once (market data is account-agnostic), then runs `_tick_account` independently for each account.

---

## 3. Trade Decision Flow

```mermaid
sequenceDiagram
    participant ML as Main Loop
    participant SE as Signal Engine
    participant DE as Decision Engine
    participant C as Claude
    participant G as Gemini
    participant RM as Risk Manager
    participant B as Broker
    participant FC as Flashcards

    ML->>SE: compute(symbol)
    SE-->>ML: SignalResult (RSI, MACD, BB, SMA, EMA, composite, confidence)
    ML->>DE: decide(signal, equity, positions, daily_pnl_pct)
    DE->>C: decide(symbol, prompt) [thread 1]
    DE->>G: decide(symbol, prompt) [thread 2]
    C-->>DE: TradeDecision (action, confidence, reasoning)
    G-->>DE: TradeDecision (action, confidence, reasoning)
    DE->>DE: _consensus() → merged TradeDecision
    DE->>DE: classify_risk(signal_conf, decision_conf, consensus)
    DE-->>ML: TradeDecision (action, risk_level, confidence)
    ML->>RM: approve_buy(symbol, equity, positions, day_trades)
    RM-->>ML: RiskDecision (allowed, dollar_amount)
    alt auto_trade OR risk below threshold
        ML->>B: buy(symbol, dollar_amount)
        B-->>ML: OrderResult (filled, price, qty)
        ML->>FC: record_trade(trade_id, signal, decision, entry_price)
    else needs approval
        ML->>DE: queue_approval(trade_id, trade_info)
        DE-->>FC: yellow approval card shown
        FC-->>DE: approve / deny
        DE-->>ML: get_approval_decision()
        ML->>B: buy(symbol, dollar_amount)
        ML->>FC: record_trade(...)
    end
    Note over B,FC: On SELL or stop-loss: FC.close_trade(exit_price, outcome)
```

---

## 4. Ensemble AI Logic

Both models receive an identical prompt containing: symbol, current price, RSI, MACD, Bollinger Bands, SMA-20, EMA-50, portfolio equity, open position count, daily P&L, and whether the symbol is already held. They run in parallel via `ThreadPoolExecutor(max_workers=2)` with a 30-second timeout per model.

**Models:**
- Claude: `claude-opus-4-8` with `thinking: {"type": "adaptive"}` (extended thinking enabled)
- Gemini: `gemini-2.0-flash` at `temperature=0.2`

Each model returns strict JSON: `{"action": "BUY"|"SELL"|"HOLD", "confidence": 0.0-1.0, "reasoning": "..."}`.

```mermaid
flowchart LR
    C([Claude vote]) --> CC{Same action?}
    G([Gemini vote]) --> CC
    CC -- "Both BUY or both SELL" --> AGR["Execute\nconfidence = avg(C, G)\nconsensus = True"]
    CC -- "One HOLD\none directional" --> CON["HOLD\nconfidence = min(C, G)\nconsensus = False"]
    CC -- "BUY vs SELL\ncontradiction" --> HARD["Hard HOLD\nconfidence = 0.0\nconsensus = False\nrisk = high"]
```

If `GEMINI_API_KEY` is not set, Argus runs Claude solo with no confidence penalty. If Gemini initialization fails at startup, it degrades gracefully to Claude-only mode.

The combined reasoning string from both models is stored in the flashcard and shown in the web dashboard's decision log.

---

## 5. Risk Classification

After ensemble consensus, `classify_risk()` maps signal and decision confidence into a risk tier:

| `consensus` | `signal_confidence` | `decision_confidence` | Risk Level |
|-------------|--------------------|-----------------------|------------|
| `False` | any | any | **high** |
| `True` | ≥ 0.7 | ≥ 0.7 | **low** |
| `True` | ≥ 0.4 OR decision ≥ 0.5 | — | **medium** |
| `True` | < 0.4 | < 0.5 | **high** |

```mermaid
flowchart TD
    START([classify_risk]) --> CHK_CON{consensus?}
    CHK_CON -- "False\n(models disagreed)" --> HIGH([high])
    CHK_CON -- "True" --> CHK_CONF{"signal_conf ≥ 0.7\nAND decision_conf ≥ 0.7?"}
    CHK_CONF -- Yes --> LOW([low])
    CHK_CONF -- No --> CHK_MED{"signal_conf ≥ 0.4\nOR decision_conf ≥ 0.5?"}
    CHK_MED -- Yes --> MED([medium])
    CHK_MED -- No --> HIGH2([high])
```

On the **Default account**, trades with risk ≥ `APPROVAL_THRESHOLD` (default: `medium`) go to the approval queue instead of executing immediately. On the **Agentic account**, all three risk levels auto-execute.

---

## 6. Account Setup

Argus manages two accounts simultaneously. Each account has its own `RobinhoodBroker` instance, its own `RiskManager` (separate drawdown/PDT tracking), and its own pending approvals dict.

| Account | Variable | Mode |
|---------|----------|------|
| Agentic | `AGENTIC_ACCOUNT_NUMBER` | Fully automated (`auto_trade=True`) |
| Default | `DEFAULT_ACCOUNT_NUMBER` | Approval required for `medium`+ risk |

```mermaid
graph LR
    SIG[SignalResult] --> AUT_CHK{Account:\nAgentic?}
    AUT_CHK -- Yes\nauto_trade=True --> EXEC["Execute immediately\n(all risk levels)"]
    AUT_CHK -- No\nDefault account --> RISK_CHK{"risk_level ≥\nAPPROVAL_THRESHOLD?"}
    RISK_CHK -- "No (low risk)" --> EXEC2["Execute immediately"]
    RISK_CHK -- "Yes (medium/high)" --> QUEUE["Queue for approval\nin web dashboard"]
    QUEUE --> USER{User decision}
    USER -- Approve --> EXEC3[Execute trade]
    USER -- Deny --> DROP[Discard trade]
```

Signals and price data are computed once per tick using a shared broker instance, then the resulting `SignalResult` is fed to both account ticks independently.

---

## 7. Configuration Reference

Non-secret settings live in `.env`. Secrets use the OS keychain (see §8).

### Trading Mode

| Variable | Default | Description |
|----------|---------|-------------|
| `PAPER_TRADE` | `true` | `true` = simulated orders, `false` = real money |
| `APPROVAL_THRESHOLD` | `medium` | Risk level requiring human approval on Default account (`medium` or `high`) |

### Accounts

| Variable | Default | Description |
|----------|---------|-------------|
| `ROBINHOOD_USERNAME` | — | Robinhood login email (required) |
| `AGENTIC_ACCOUNT_NUMBER` | — | Robinhood account number for the auto-trade account |
| `DEFAULT_ACCOUNT_NUMBER` | — | Robinhood account number for the approval-gated account |

### Risk Guardrails

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_POSITION_PCT` | `0.10` | Max fraction of equity in a single position (must be 0–0.25) |
| `STOP_LOSS_PCT` | `0.05` | Hard stop-loss trigger; position closed if price drops this % from entry |
| `MAX_POSITIONS` | `5` | Max concurrent open positions per account |
| `DAILY_DRAWDOWN_LIMIT` | `-0.05` | Kill switch threshold; stops all trading if session equity drops this % (must be negative) |

### Watchlist

| Variable | Default | Description |
|----------|---------|-------------|
| `WATCHLIST` | `AAPL,TSLA,NVDA,BTC,ETH` | Comma-separated list of symbols to scan on each tick |

### Scan Loop

| Variable | Default | Description |
|----------|---------|-------------|
| `SCAN_INTERVAL_SECONDS` | `300` | Seconds between ticks (minimum 30 to avoid rate-limiting) |

### Web Dashboard

| Variable | Default | Description |
|----------|---------|-------------|
| `WEB_HOST` | `127.0.0.1` | Bind address; use `0.0.0.0` only behind an auth proxy |
| `WEB_PORT` | `8000` | Port for the FastAPI web server |
| `DATABASE_URL` | `sqlite:///argus.db` | SQLAlchemy connection string for trade history |

### Notifications

| Variable | Default | Description |
|----------|---------|-------------|
| `NOTIFY_EMAIL` | — | Recipient address for email alerts |
| `SMTP_HOST` | `smtp.gmail.com` | SMTP server host |
| `SMTP_PORT` | `587` | SMTP port |
| `SMTP_USER` | — | SMTP login (sender address) |
| `TWILIO_ACCOUNT_SID` | — | Twilio Account SID for SMS alerts |
| `TWILIO_FROM` | — | Twilio source phone number |
| `TWILIO_TO` | — | Destination phone number for SMS |
| `SLACK_CHANNEL` | `#argus-alerts` | Slack channel for trade notifications |

---

## 8. Secrets & Keychain

Secrets are never stored in `.env` or committed to source control. Argus uses the OS keychain (macOS Keychain, Windows Credential Manager, or Linux Secret Service via `keyring`). On startup, `config.py` loads secrets through `_KeychainSource` before checking `.env`.

**Priority order (highest → lowest):** environment variable → OS keychain → `.env` → default

Run `argus-setup` once after install to populate the keychain interactively. Re-run any time to rotate a key.

```
argus-setup           # store / update all secrets (interactive prompts)
argus-setup --show    # display which secrets are stored (values masked)
argus-setup --clear   # remove all Argus secrets from keychain
```

| Key | Required | Description |
|-----|----------|-------------|
| `ANTHROPIC_API_KEY` | **Required** | Anthropic API key for Claude |
| `GEMINI_API_KEY` | Optional | Google Gemini API key; omit to run Claude-only |
| `ROBINHOOD_PASSWORD` | **Required** | Robinhood account password |
| `ROBINHOOD_MFA_SECRET` | Optional | TOTP secret for Robinhood 2FA (base32 string from the authenticator setup page) |
| `SMTP_PASSWORD` | Optional | Gmail app password or SMTP credential |
| `TWILIO_AUTH_TOKEN` | Optional | Twilio auth token for SMS notifications |
| `SLACK_BOT_TOKEN` | Optional | Slack bot token (`xoxb-...`) for channel notifications |

> ⚠️ MFA sessions are never persisted to disk (`store_session=False`). The TOTP secret is cleared from memory immediately after generating the login code.

---

## 9. CLI Commands / Aliases

These shell functions are defined in `~/.zshrc` and require `ARGUS_DIR` to point to the project root.

| Alias / Function | Description |
|-----------------|-------------|
| `argus-tmux` | **Start here.** Creates (or re-attaches to) a `tmux` session named `argus` running the full terminal UI + web dashboard. Re-run from any terminal to re-attach. |
| `argus-start` | Foreground start with terminal UI. Exits when the terminal closes. |
| `argus-bg` | Background start (no terminal UI). Logs to `~/argus.log`. Writes PID to `~/argus.pid`. |
| `argus-stop` | Graceful shutdown. Kills by PID file, port 8000 process, and process name — works regardless of how Argus was started. |
| `argus-status` | Checks if Argus is running by polling `/api/status`. Prints equity and P&L if up, or a start instruction if down. |
| `argus-log` | `tail -f ~/argus.log` — follows the live log file for `argus-bg` and `argus-tmux` runs. |
| `argus-open` | Opens `http://127.0.0.1:8000` in the default browser. |
| `argus-mock` | Runs `dev_mock.py` — a fake-data UI server for development. No Robinhood connection required. |
| `argus-setup` | Launches the interactive keychain setup wizard to store or rotate secrets. |

---

## 10. Web Dashboard

The dashboard is a single-page app served by FastAPI at `http://127.0.0.1:8000`. State is pushed from the main loop to the browser via Server-Sent Events (`/events`) every tick, so all panels update live without polling.

### Account Panels

Two side-by-side panels, one per account. **Agentic** uses cyan borders; **Default** uses purple (magenta). Each panel shows: equity, daily P&L (absolute + percent), day trade count, mode (`AUTO` vs `APPROVAL`), pending approval count, open positions (entry/current price, unrealized P&L %), and recent trades (time, side, price).

### Price Chart

Interactive candlestick chart for any watchlisted symbol. Pulls one month of daily OHLCV data from Robinhood via `/api/chart/{symbol}`. Features:

- **Candlestick / Line toggle** — switch between chart styles
- **Linear regression prediction line** — overlaid trend projection
- **Trade markers** — buy/sell executions plotted directly on the chart

### Pending Approvals

Yellow cards appear for every Default-account trade queued for approval. Each card shows symbol, dollar amount, risk level, AI confidence, signal direction, and the full AI reasoning. Buttons: **Approve** (POST `/api/approve/{trade_id}`) or **Deny** (POST `/api/deny/{trade_id}`). The main loop polls for decisions every tick and executes approved trades immediately.

### Open Positions Table

Lists all open positions across both accounts with: symbol, account label, quantity, entry price, current price, stop-loss price, and unrealized P&L %.

### Signals Table

Shows the latest `SignalResult` for every watchlisted symbol: price, RSI, MACD histogram, composite direction (BULLISH / BEARISH / NEUTRAL, color-coded), and confidence.

### Recent Trades

Chronological list of executed buy/sell orders from the current session: time, symbol, account, side, quantity, and fill price.

### Decision Flashcards

A grid of cards from `argus_flashcards.jsonl`. Each card shows the market context at decision time on the front (symbol, signal, RSI, MACD, BB position). Click any card to expand the full AI reasoning, risk level, entry price, and (if the trade is closed) exit price, P&L%, and hold duration.

### Agent Log

Real-time log stream (last 100 entries). Color-coded by level: INFO (white), WARNING (yellow), ERROR (red). The log automatically scrolls to the latest entry. Filterable by level in the UI.

---

## 11. Flashcard Learning System

Every executed trade produces a flashcard. Cards are stored in `argus_flashcards.jsonl` (one JSON object per line) in the project root. The store is loaded into memory at startup and flushed to disk on every write.

**On trade entry (`FlashcardStore.record_trade`)**, the card captures:

| Field | What it records |
|-------|----------------|
| `signal_composite` | `bullish` / `bearish` / `neutral` at decision time |
| `signal_confidence` | Fraction of indicators in agreement (0–1) |
| `rsi` | RSI-14 value |
| `macd_hist` | MACD histogram value (sign indicates momentum direction) |
| `bb_position` | `above_upper` / `below_lower` / `inside` |
| `price_vs_sma20` | `above` or `below` the 20-day simple moving average |
| `price_vs_ema50` | `above` or `below` the 50-day exponential moving average |
| `risk_level` | `low` / `medium` / `high` from the ensemble |
| `decision_confidence` | Averaged AI confidence (0–1) |
| `reasoning` | Full concatenated Claude + Gemini reasoning |
| `entry_price` | Actual fill price |
| `dollar_amount` | Position size in dollars |

**On trade close (`FlashcardStore.close_trade`)**, the card is updated with:

| Field | What it records |
|-------|----------------|
| `exit_price` | Fill price on sell |
| `pnl_pct` | `(exit - entry) / entry * 100` |
| `outcome` | `win`, `loss`, or `stop-loss` |
| `hold_duration_hours` | Time from entry to exit |

**How to use them:**

`FlashcardStore.summary()` returns aggregate stats (win rate, average P&L %, best and worst trade) visible in the web dashboard flashcard panel. For deeper analysis, load the JSONL file directly:

```python
import json
from pathlib import Path

cards = [json.loads(l) for l in Path("argus_flashcards.jsonl").read_text().splitlines() if l]
closed = [c for c in cards if c["pnl_pct"] is not None]

# Example: which RSI ranges correlate with wins?
wins = [c for c in closed if c["pnl_pct"] > 0]
```

Look for patterns in: which `bb_position` + `signal_composite` combinations win most, whether high `decision_confidence` actually predicts positive P&L, and which symbols produce the best outcomes.

---

## 12. Paper Mode vs Live

### Paper Mode (`PAPER_TRADE=true`)

- Each account broker starts with a simulated `$10,000` cash balance (separate per broker instance).
- `buy()` deducts from the simulated balance and records a synthetic position; `sell()` returns proceeds to the balance.
- Price data is still fetched live from Robinhood for realistic signal computation.
- Orders never reach Robinhood's order management system.
- All other features (flashcards, signals, approvals, notifications, database) work identically.

### Live Mode (`PAPER_TRADE=false`)

- Each account broker authenticates to Robinhood at startup using the real account credentials.
- `buy()` calls `rh.orders.order_buy_fractional_by_quantity` (equities) or `order_buy_crypto_by_quantity` (crypto).
- Fractional shares are supported, so any dollar amount works regardless of share price.
- Sessions are never cached to disk (`store_session=False`). The TOTP code is computed fresh each startup and the MFA secret is cleared from memory immediately after.

### Go-Live Checklist

Before setting `PAPER_TRADE=false`, verify all of the following in paper mode:

- [ ] At least 5 full trading days completed without crashes
- [ ] Win rate ≥ 50% across ≥ 10 closed trades (`argus_flashcards.jsonl`)
- [ ] Kill switch has triggered and recovered correctly at least once
- [ ] Approval flow on Default account tested end-to-end (approve and deny)
- [ ] Stop-loss sweep confirmed to close positions automatically
- [ ] `argus-status` shows correct equity and P&L after each session
- [ ] Notifications (email / SMS / Slack) delivering correctly
- [ ] `DAILY_DRAWDOWN_LIMIT` set conservatively (e.g., `-0.02` for first live week)

> ⚠️ Start with the Agentic account (smaller balance) for the first live week before trusting the Default account's approval flow with larger positions.

---

## 13. Directory Structure

```
argus/
├── .env                          # Non-secret config (watchlist, ports, risk params)
├── pyproject.toml                # Package definition and dependencies
├── argus.db                      # SQLite trade history (created at runtime)
├── argus.log                     # Rolling log file (created at runtime)
├── argus_flashcards.jsonl        # Trade decision flashcards (created at runtime)
│
└── argus/                        # Main package
    ├── main.py                   # Autopilot orchestration loop; AccountContext dataclass
    ├── config.py                 # Pydantic settings; keychain source; priority chain
    ├── secrets.py                # OS keychain read/write via `keyring`
    ├── setup_secrets.py          # `argus-setup` interactive CLI
    ├── install_service.py        # `argus-service` macOS launchd / systemd installer
    │
    ├── agent/
    │   └── decision.py           # DecisionEngine; _ClaudeEngine; _GeminiEngine; classify_risk()
    │
    ├── broker/
    │   └── robinhood.py          # RobinhoodBroker; paper simulation; live order execution
    │
    ├── strategy/
    │   └── indicators.py         # SignalEngine; SignalResult; pandas_ta indicator computation
    │
    ├── risk/
    │   └── manager.py            # RiskManager; PDT tracking; drawdown kill switch; stop-loss
    │
    ├── dashboard/
    │   ├── web.py                # FastAPI app; SSE stream; approval queue; REST endpoints; HTML UI
    │   ├── terminal.py           # Rich terminal dashboard; per-account panels; signals table
    │   ├── log_buffer.py         # In-memory log ring buffer; logging handler installer
    │   └── server.py             # Standalone web-only entry point (no main loop)
    │
    ├── learning/
    │   └── flashcards.py         # FlashcardStore; Flashcard dataclass; JSONL persistence
    │
    ├── notifications/
    │   └── notifier.py           # Notifier; email (aiosmtplib); SMS (Twilio); Slack
    │
    └── storage/
        └── models.py             # SQLAlchemy models: Trade, Signal, Position, DailyStats
```
