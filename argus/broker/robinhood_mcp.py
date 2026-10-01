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
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

_MCP_URL = "https://agent.robinhood.com/mcp/trading"
_KEYCHAIN_SERVICE = "Claude Code-credentials"

# In-memory token cache: {"access_token": str, "refresh_token": str, "expires_at": int, "client_id": str}
_token_cache: dict[str, Any] = {}


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
    """Attempt to refresh the OAuth access token using the refresh token."""
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
