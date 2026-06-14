"""Dev mock — runs the web dashboard with realistic fake data, no broker needed."""

import threading
import time
import random
import datetime
import math
import uvicorn
from argus.dashboard import web as dash


# ── Fake OHLCV candle generator ───────────────────────────────────────────────

def _fake_candles(symbol: str) -> list[dict]:
    """Generate 60 days of fake daily OHLCV bars."""
    bases = {"AAPL": 189, "TSLA": 248, "NVDA": 875, "BTC": 67400, "ETH": 3540}
    base = bases.get(symbol, 100)
    candles = []
    now = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    price = base
    rng = random.Random(hash(symbol))
    for i in range(60, 0, -1):
        ts = int((now - datetime.timedelta(days=i)).timestamp())
        change = rng.uniform(-0.025, 0.025)
        price = price * (1 + change)
        hi = price * rng.uniform(1.002, 1.018)
        lo = price * rng.uniform(0.982, 0.998)
        op = rng.uniform(lo, hi)
        candles.append({"time": ts, "open": round(op, 2), "high": round(hi, 2),
                        "low": round(lo, 2), "close": round(price, 2)})
    return candles


dash.register_chart_source(_fake_candles)

# ── Symbol search (name → ticker) ─────────────────────────────────────────────

_SYMBOL_DB = [
    {"symbol": "AAPL",  "name": "Apple Inc."},
    {"symbol": "TSLA",  "name": "Tesla, Inc."},
    {"symbol": "NVDA",  "name": "NVIDIA Corporation"},
    {"symbol": "MSFT",  "name": "Microsoft Corporation"},
    {"symbol": "AMZN",  "name": "Amazon.com, Inc."},
    {"symbol": "GOOGL", "name": "Alphabet Inc. (Google)"},
    {"symbol": "GOOG",  "name": "Alphabet Inc. Class C"},
    {"symbol": "META",  "name": "Meta Platforms (Facebook)"},
    {"symbol": "NFLX",  "name": "Netflix, Inc."},
    {"symbol": "AMD",   "name": "Advanced Micro Devices"},
    {"symbol": "INTC",  "name": "Intel Corporation"},
    {"symbol": "JPM",   "name": "JPMorgan Chase & Co."},
    {"symbol": "BAC",   "name": "Bank of America"},
    {"symbol": "GS",    "name": "Goldman Sachs"},
    {"symbol": "COIN",  "name": "Coinbase Global, Inc."},
    {"symbol": "PLTR",  "name": "Palantir Technologies"},
    {"symbol": "RIVN",  "name": "Rivian Automotive"},
    {"symbol": "UBER",  "name": "Uber Technologies"},
    {"symbol": "LYFT",  "name": "Lyft, Inc."},
    {"symbol": "ROKU",  "name": "Roku, Inc."},
    {"symbol": "SOFI",  "name": "SoFi Technologies"},
    {"symbol": "SPY",   "name": "SPDR S&P 500 ETF"},
    {"symbol": "QQQ",   "name": "Invesco QQQ Trust (Nasdaq-100)"},
    {"symbol": "DIS",   "name": "The Walt Disney Company"},
    {"symbol": "NKLA",  "name": "Nikola Corporation"},
    {"symbol": "SHOP",  "name": "Shopify Inc."},
    {"symbol": "SQ",    "name": "Block, Inc. (Square)"},
    {"symbol": "PYPL",  "name": "PayPal Holdings"},
    {"symbol": "V",     "name": "Visa Inc."},
    {"symbol": "MA",    "name": "Mastercard Incorporated"},
    {"symbol": "BTC",   "name": "Bitcoin"},
    {"symbol": "ETH",   "name": "Ethereum"},
    {"symbol": "SOL",   "name": "Solana"},
    {"symbol": "DOGE",  "name": "Dogecoin"},
    {"symbol": "XRP",   "name": "XRP (Ripple)"},
    {"symbol": "ADA",   "name": "Cardano"},
]

def _mock_search(query: str) -> list[dict]:
    q = query.lower().strip()
    results = []
    for item in _SYMBOL_DB:
        if q in item["symbol"].lower() or q in item["name"].lower():
            results.append(item)
        if len(results) >= 6:
            break
    return results

dash.register_search(_mock_search)

# ── Fake data generators ──────────────────────────────────────────────────────

SYMBOLS = ["AAPL", "TSLA", "NVDA", "BTC", "ETH"]
dash.set_watchlist_base(SYMBOLS)
_t = 0  # tick counter

def _price(sym, tick):
    bases = {"AAPL": 189, "TSLA": 248, "NVDA": 875, "BTC": 67400, "ETH": 3540}
    base = bases[sym]
    wave = math.sin(tick * 0.3 + hash(sym) % 10) * base * 0.02
    noise = random.uniform(-base * 0.005, base * 0.005)
    return round(base + wave + noise, 2)

def _signals(tick):
    out = []
    for sym in SYMBOLS:
        p = _price(sym, tick)
        rsi = 45 + math.sin(tick * 0.2 + hash(sym)) * 20 + random.uniform(-3, 3)
        macd_h = math.sin(tick * 0.15 + hash(sym) % 5) * 0.8
        comp = "bullish" if rsi < 45 else "bearish" if rsi > 65 else "neutral"
        conf = abs(rsi - 50) / 50
        out.append({"symbol": sym, "price": p, "rsi": round(rsi, 1),
                    "macd_hist": round(macd_h, 4), "composite": comp,
                    "confidence": round(conf, 2), "volume": random.randint(1_000_000, 50_000_000)})
    return out

def _positions(tick):
    return {
        "NVDA [agentic]": {
            "quantity": 0.5714, "entry_price": 861.50,
            "current_price": _price("NVDA", tick),
            "stop_loss_price": 818.43,
            "unrealized_pnl": (_price("NVDA", tick) - 861.50) * 0.5714,
            "unrealized_pnl_pct": round((_price("NVDA", tick) - 861.50) / 861.50 * 100, 2),
            "account": "agentic",
        },
        "AAPL [default]": {
            "quantity": 5.2300, "entry_price": 184.20,
            "current_price": _price("AAPL", tick),
            "stop_loss_price": 174.99,
            "unrealized_pnl": (_price("AAPL", tick) - 184.20) * 5.23,
            "unrealized_pnl_pct": round((_price("AAPL", tick) - 184.20) / 184.20 * 100, 2),
            "account": "default",
        },
    }

def _accounts(tick):
    agen_eq = 500 + math.sin(tick * 0.1) * 12 + random.uniform(-2, 2)
    def_eq  = 15700 + math.sin(tick * 0.08 + 1) * 180 + random.uniform(-10, 10)
    return {
        "agentic": {
            "equity": round(agen_eq, 2),
            "daily_pnl": round(agen_eq - 500, 2),
            "daily_pnl_pct": round((agen_eq - 500) / 500 * 100, 2),
            "kill_switch": False, "day_trades": 1, "auto_trade": True,
            "pending_approvals": 0,
            "positions": {
                "NVDA": {
                    "quantity": 0.5714, "entry_price": 861.50,
                    "current_price": _price("NVDA", tick),
                    "stop_loss_price": 818.43,
                    "unrealized_pnl_pct": round((_price("NVDA", tick) - 861.50) / 861.50 * 100, 2),
                }
            },
            "trades": [
                {"time": "09:31:22", "symbol": "NVDA", "side": "buy",  "price": 861.50, "quantity": 0.5714, "account": "agentic"},
                {"time": "09:45:10", "symbol": "TSLA", "side": "sell", "price": 251.80, "quantity": 1.2000, "account": "agentic"},
            ],
        },
        "default": {
            "equity": round(def_eq, 2),
            "daily_pnl": round(def_eq - 15700, 2),
            "daily_pnl_pct": round((def_eq - 15700) / 15700 * 100, 2),
            "kill_switch": False, "day_trades": 0, "auto_trade": False,
            "pending_approvals": 1 if tick % 20 < 8 else 0,
            "positions": {
                "AAPL": {
                    "quantity": 5.23, "entry_price": 184.20,
                    "current_price": _price("AAPL", tick),
                    "stop_loss_price": 174.99,
                    "unrealized_pnl_pct": round((_price("AAPL", tick) - 184.20) / 184.20 * 100, 2),
                }
            },
            "trades": [
                {"time": "10:02:44", "symbol": "AAPL", "side": "buy",  "price": 184.20, "quantity": 5.2300, "account": "default"},
            ],
        },
    }

def _recent_trades(tick):
    return [
        {"time": "10:02:44", "symbol": "AAPL",  "side": "buy",  "price": 184.20, "quantity": 5.2300, "account": "default"},
        {"time": "09:45:10", "symbol": "TSLA",  "side": "sell", "price": 251.80, "quantity": 1.2000, "account": "agentic"},
        {"time": "09:31:22", "symbol": "NVDA",  "side": "buy",  "price": 861.50, "quantity": 0.5714, "account": "agentic"},
        {"time": "09:15:05", "symbol": "ETH",   "side": "sell", "price": 3498.00,"quantity": 0.3000, "account": "agentic"},
        {"time": "09:00:01", "symbol": "BTC",   "side": "buy",  "price": 66800.0,"quantity": 0.0012, "account": "default"},
    ]

def _flashcards():
    return [
        {
            "trade_id": "abc-001", "symbol": "NVDA", "action": "BUY", "account": "agentic",
            "timestamp": "2026-06-13T09:31:22+00:00",
            "signal_composite": "bullish", "signal_confidence": 0.82,
            "rsi": 38.4, "macd_hist": 0.6231, "bb_position": "below_lower",
            "price_vs_sma20": "below", "price_vs_ema50": "above",
            "risk_level": "low", "decision_confidence": 0.78,
            "reasoning": "RSI oversold at 38 combined with positive MACD histogram and price breaking below lower Bollinger Band suggests a mean-reversion entry. EMA50 still bullish.",
            "entry_price": 861.50, "dollar_amount": 50.00,
            "exit_price": None, "pnl_pct": None, "outcome": None,
        },
        {
            "trade_id": "abc-002", "symbol": "TSLA", "action": "SELL", "account": "agentic",
            "timestamp": "2026-06-13T09:45:10+00:00",
            "signal_composite": "bearish", "signal_confidence": 0.71,
            "rsi": 72.1, "macd_hist": -0.3412, "bb_position": "above_upper",
            "price_vs_sma20": "above", "price_vs_ema50": "above",
            "risk_level": "medium", "decision_confidence": 0.65,
            "reasoning": "RSI overbought at 72, price above upper Bollinger Band, MACD histogram turning negative. Taking profit on extended move.",
            "entry_price": 238.40, "dollar_amount": 50.00,
            "exit_price": 251.80, "pnl_pct": 5.62, "outcome": "win",
            "hold_duration_hours": 2.3,
        },
        {
            "trade_id": "abc-003", "symbol": "ETH", "action": "SELL", "account": "agentic",
            "timestamp": "2026-06-13T09:15:05+00:00",
            "signal_composite": "bearish", "signal_confidence": 0.55,
            "rsi": 68.0, "macd_hist": -0.1100, "bb_position": "inside",
            "price_vs_sma20": "above", "price_vs_ema50": "below",
            "risk_level": "high", "decision_confidence": 0.48,
            "reasoning": "Mixed signals — RSI mildly elevated, MACD slightly negative. Stop-loss triggered as position dropped below threshold.",
            "entry_price": 3580.00, "dollar_amount": 50.00,
            "exit_price": 3498.00, "pnl_pct": -2.29, "outcome": "stop-loss",
            "hold_duration_hours": 5.1,
        },
    ]

def _token_usage(tick):
    # Simulate accumulating token usage across the trading day
    calls = 4 + tick  # one ensemble call per scan tick
    claude_input  = calls * 1_240
    claude_output = calls * 187
    claude_cache  = calls * 3_800
    gemini_input  = calls * 1_190
    gemini_output = calls * 143
    claude_cost = (
        claude_input  * 15.00 / 1_000_000
        + claude_output * 75.00 / 1_000_000
        + claude_cache  *  1.50 / 1_000_000
    )
    gemini_cost = gemini_input * 0.10 / 1_000_000 + gemini_output * 0.40 / 1_000_000
    import datetime as _dt
    return {
        "date": _dt.date.today().isoformat(),
        "claude": {
            "calls": calls,
            "input_tokens":      claude_input,
            "output_tokens":     claude_output,
            "cache_read_tokens": claude_cache,
            "cost_usd":          round(claude_cost, 4),
        },
        "gemini": {
            "calls":         calls,
            "input_tokens":  gemini_input,
            "output_tokens": gemini_output,
            "cost_usd":      round(gemini_cost, 4),
        },
        "total_calls":    calls * 2,
        "total_cost_usd": round(claude_cost + gemini_cost, 4),
    }


def _pending_approval(tick):
    if tick % 20 < 8:
        return {
            "trade-pending-001": {
                "trade_id": "trade-pending-001",
                "symbol": "BTC", "action": "BUY",
                "dollar_amount": 1570.00, "risk_level": "medium",
                "confidence": 0.58,
                "reasoning": "MACD histogram turning positive after extended bearish period. RSI at 42 with room to run. Position sizing within limits.",
                "account_label": "default", "account_number": "XXXXXXXXX",
                "signal": "bullish", "signal_confidence": 0.61,
                "queued_at": datetime.datetime.utcnow().isoformat(),
            }
        }
    return {}

# ── Push loop ─────────────────────────────────────────────────────────────────

def push_loop():
    global _t
    time.sleep(0.5)  # let server start
    while True:
        sigs = _signals(_t)
        accts = _accounts(_t)
        total_eq = sum(a["equity"] for a in accts.values())
        total_pnl = sum(a["daily_pnl"] for a in accts.values())
        approvals = _pending_approval(_t)

        state = {
            "paper_trade": True,
            "kill_switch": False,
            "paused": False,
            "watchlist": dash.get_watchlist(),
            "equity": round(total_eq, 2),
            "daily_pnl": round(total_pnl, 2),
            "daily_pnl_pct": round(total_pnl / (total_eq - total_pnl) * 100, 2) if total_eq else 0,
            "trade_count": 5,
            "day_trades": 1,
            "positions": _positions(_t),
            "signals": sigs,
            "recent_trades": _recent_trades(_t),
            "accounts": accts,
            "pending_approvals": approvals,
            "flashcards": _flashcards(),
            "flashcard_summary": {
                "total": 3, "closed": 2, "win_rate": 0.5,
                "avg_pnl_pct": 1.67, "best_pnl_pct": 5.62, "worst_pnl_pct": -2.29,
            },
            "token_usage": _token_usage(_t),
        }
        dash.push_state(state)
        # Also wire pending approvals directly
        import argus.dashboard.web as _w
        import threading as _th
        with _w._approval_lock:
            _w._pending_approvals.clear()
            _w._pending_approvals.update(approvals)

        _t += 1
        time.sleep(3)

if __name__ == "__main__":
    threading.Thread(target=push_loop, daemon=True).start()
    print("Mock dashboard → http://127.0.0.1:8000")
    uvicorn.run(dash.app, host="127.0.0.1", port=8000, log_level="warning")
