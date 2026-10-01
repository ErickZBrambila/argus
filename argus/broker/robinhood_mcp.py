"""Lightweight client for the Robinhood MCP HTTP server.

Reads the OAuth token stored by Claude Code in the macOS keychain and calls
the MCP JSON-RPC endpoint directly, so Argus can pull official realized P&L
without going through the Claude CLI.

Token is read once and cached in memory; a failed refresh falls through
gracefully rather than crashing the agent loop.
"""

from __future__ import annotations

import json
import logging
import subprocess
import threading
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

_MCP_URL = "https://agent.robinhood.com/mcp/trading"
_KEYCHAIN_SERVICE = "Claude Code-credentials"

# In-memory token cache: {"access_token": str, "refresh_token": str, "expires_at": int, "client_id": str}
_token_cache: dict[str, Any] = {}
_token_lock = threading.Lock()


def _load_token_from_keychain() -> dict[str, Any]:
    """Read the Robinhood MCP OAuth token from the macOS keychain."""
    try:
        raw = subprocess.check_output(
            ["security", "find-generic-password", "-s", _KEYCHAIN_SERVICE, "-w"],
            stderr=subprocess.DEVNULL,
            timeout=5,
        ).decode().strip()
        data = json.loads(raw)
        oauth = data.get("mcpOAuth", {})
        for key, val in oauth.items():
            if "robinhood" in key.lower() and isinstance(val, dict):
                return val
    except Exception as exc:
        logger.warning("robinhood_mcp: could not read token from keychain: %s", exc)
    return {}


def _get_access_token() -> str | None:
    """Return a valid access token, refreshing if needed."""
    global _token_cache
    with _token_lock:
        if not _token_cache:
            _token_cache = _load_token_from_keychain()
        if not _token_cache:
            return None
        expires_at_ms = _token_cache.get("expiresAt", 0)
        now_ms = int(time.time() * 1000)
        # Refresh 5 minutes before expiry
        if expires_at_ms and (expires_at_ms - now_ms) < 5 * 60 * 1000:
            _refresh_token()
        return _token_cache.get("accessToken")


def _refresh_token() -> None:
    """Attempt to refresh the OAuth access token. Must be called while holding _token_lock."""
    refresh = _token_cache.get("refreshToken")
    client_id = _token_cache.get("clientId")
    discovery = _token_cache.get("discoveryState", {})
    token_endpoint = discovery.get("tokenEndpoint") if isinstance(discovery, dict) else None

    if not (refresh and client_id and token_endpoint):
        logger.warning("robinhood_mcp: missing refresh credentials, cannot refresh token")
        return

    try:
        resp = requests.post(
            token_endpoint,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh,
                "client_id": client_id,
            },
            timeout=10,
        )
        resp.raise_for_status()
        new_data = resp.json()
        _token_cache["accessToken"] = new_data["access_token"]
        if "refresh_token" in new_data:
            _token_cache["refreshToken"] = new_data["refresh_token"]
        expires_in = new_data.get("expires_in", 3600)
        _token_cache["expiresAt"] = int(time.time() * 1000) + expires_in * 1000
        logger.info("robinhood_mcp: token refreshed successfully")
    except Exception as exc:
        logger.warning("robinhood_mcp: token refresh failed: %s", exc)


def _call_tool(tool_name: str, arguments: dict[str, Any]) -> Any:
    """Make a single MCP tools/call request and return the parsed result dict."""
    token = _get_access_token()
    if not token:
        raise RuntimeError("No Robinhood MCP access token available")

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments},
    }
    resp = requests.post(
        _MCP_URL,
        json=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
        timeout=15,
    )
    resp.raise_for_status()

    # Response is SSE: "event: message\ndata: {...}\n\n"
    text = resp.text.strip()
    for line in text.splitlines():
        if line.startswith("data:"):
            body = json.loads(line[5:].strip())
            result = body.get("result", {})
            content = result.get("content", [])
            if content:
                return json.loads(content[0]["text"])
            return result.get("structuredContent", {})

    raise ValueError(f"Unexpected MCP response format: {text[:200]}")


def get_technical_indicator(
    symbol: str,
    indicator_type: str,
    interval: str,
    start_time: str,
    *,
    output: str = "latest",
    **kwargs: Any,
) -> dict[str, Any]:
    """Return the latest data-point dict for one technical indicator, or {} on failure.

    Navigates: result["data"]["indicators"][0]["series"][-1]
    """
    args: dict[str, Any] = {
        "symbol": symbol,
        "type": indicator_type,
        "interval": interval,
        "start_time": start_time,
        "output": output,
    }
    args.update(kwargs)
    result = _call_tool("get_equity_technical_indicators", args)
    indicators = result.get("data", result).get("indicators", [])
    if not indicators:
        return {}
    series = indicators[0].get("series", [])
    if not series:
        return {}
    return series[-1] if isinstance(series[-1], dict) else {}


def get_analyst_ratings(symbols: list[str]) -> dict[str, dict[str, Any]]:
    """Return {SYMBOL: {buy_pct, hold_pct, sell_pct, target_avg, target_high, target_low}}."""
    result = _call_tool("get_equity_analyst_ratings", {"symbols": symbols})
    items = result.get("data", {}).get("results", [])
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        sym = item.get("symbol", "")
        ratings = item.get("ratings") or {}
        if not sym or not ratings:
            continue
        n_buy  = int(ratings.get("num_buy_ratings",  0) or 0)
        n_hold = int(ratings.get("num_hold_ratings", 0) or 0)
        n_sell = int(ratings.get("num_sell_ratings", 0) or 0)
        total  = n_buy + n_hold + n_sell

        def _pct(n: int, _t: int = total) -> float | None:
            return round(n / _t, 3) if _t else None

        def _price(key: str, _r: dict = ratings) -> float | None:
            v = _r.get(key)
            try:
                return float(v) if v not in (None, "", "0", "0.0000") else None
            except (TypeError, ValueError):
                return None

        out[sym] = {
            "buy_pct":    _pct(n_buy),
            "hold_pct":   _pct(n_hold),
            "sell_pct":   _pct(n_sell),
            "target_avg": _price("mean_price_target"),
            "target_high": _price("high_price_target"),
            "target_low":  _price("low_price_target"),
        }
    return out


def get_quarterly_financials(symbols: list[str], limit: int = 4) -> dict[str, dict[str, Any]]:
    """Return {SYMBOL: {revenue_growth_pct, net_margin_latest, net_margin_prev, gross_profit_b}}."""
    result = _call_tool("get_financials", {"symbols": symbols, "period": "quarterly", "limit": limit})
    items = result.get("data", {}).get("results", [])
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        sym = item.get("symbol", "")
        periods = item.get("financials") or []
        if not sym or not periods:
            continue

        def _f(p: dict, key: str) -> float | None:
            v = p.get(key)
            try:
                return float(v) if v not in (None, "") else None
            except (TypeError, ValueError):
                return None

        rev0 = _f(periods[0], "revenue")
        rev1 = _f(periods[1], "revenue") if len(periods) > 1 else None
        rev_growth = None
        if rev0 and rev1 and rev1 != 0:
            rev_growth = round((rev0 - rev1) / abs(rev1) * 100, 1)

        gp = _f(periods[0], "gross_profit")
        out[sym] = {
            "revenue_growth_pct": rev_growth,
            "net_margin_latest":  _f(periods[0], "net_margin"),
            "net_margin_prev":    _f(periods[1], "net_margin") if len(periods) > 1 else None,
            "gross_profit_b":     round(gp / 1e9, 2) if gp else None,
        }
    return out


def get_earnings_upcoming(days_forward: int = 10) -> list[dict[str, Any]]:
    """Return upcoming high-cap earnings events as {symbol, report_date, eps_estimate, timing}."""
    result = _call_tool("get_earnings_calendar", {"days": days_forward, "filter": "high_market_cap"})
    items = result.get("data", result)
    if isinstance(items, dict):
        items = items.get("results", items.get("earnings", []))
    out = []
    for item in (items if isinstance(items, list) else []):
        sym = item.get("symbol") or item.get("ticker", "")
        date_str = (
            item.get("report_date")
            or item.get("date")
            or (item.get("report") or {}).get("date", "")
        )
        if not sym or not date_str:
            continue
        out.append({
            "symbol":       sym.upper(),
            "report_date":  date_str[:10],
            "eps_estimate": item.get("eps_estimate") or (item.get("eps") or {}).get("estimate"),
            "timing":       item.get("timing") or (item.get("report") or {}).get("timing"),
        })
    return out


def get_crypto_cost_basis(rhs_account_number: str) -> dict[str, float]:
    """Return {currency_code: cost_basis_usd} from official Robinhood crypto positions."""
    result = _call_tool("get_crypto_positions", {"rhs_account_number": rhs_account_number})
    items = result.get("data", {}).get("results", [])
    out: dict[str, float] = {}
    for item in items:
        code = (item.get("currency") or {}).get("code", "")
        cost_bases = item.get("cost_bases") or []
        if not code or not cost_bases:
            continue
        try:
            cost = float(cost_bases[0].get("direct_cost_basis", 0) or 0)
        except (TypeError, ValueError):
            cost = 0.0
        if cost > 0:
            out[code.upper()] = cost
    return out


def get_politician_trades_for(symbol: str) -> list[dict[str, Any]]:
    """Return recent STOCK Act disclosures for a symbol as normalized dicts."""
    result = _call_tool("get_politician_trades", {"equity_symbol": symbol.upper()})
    items = result.get("data", result)
    if isinstance(items, dict):
        items = items.get("results", items.get("trades", []))
    out = []
    for item in (items if isinstance(items, list) else []):
        out.append({
            "politician":        item.get("politician_name") or item.get("name", ""),
            "transaction_type":  item.get("transaction_type") or item.get("type", ""),
            "amount_range":      item.get("amount_range") or item.get("amount", ""),
            "transaction_date":  item.get("transaction_date") or item.get("date", ""),
        })
    return out


def get_realized_pnl(account_number: str, span: str = "3month") -> dict[str, Any]:
    """Fetch Robinhood's official realized P&L for one account.

    Returns a dict with at least:
      total_returns: float  (negative = loss)
      total_rate:    float  (as a decimal, e.g. -0.055 = -5.5%)
      window:        str

    Raises on network or auth failure — caller should catch and fall back.
    """
    result = _call_tool("get_realized_pnl", {"account_number": account_number, "span": span})
    data = result.get("data", result)
    return {
        "total_returns": float(data.get("total_returns", 0) or 0),
        "total_rate": float(data.get("total_rate_of_return", 0) or 0),
        "window": data.get("window", span),
    }
