"""Thread-safe token and cost tracker for Claude + Gemini API calls.

Resets daily at midnight. Persists to a JSON file so restarts within the
same day accumulate correctly. Costs are estimates based on public pricing.
"""

from __future__ import annotations

import datetime
import json
import logging
import pathlib
import threading
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Claude Sonnet 4.6 pricing (per token)
_CLAUDE_INPUT_COST  =  3.00 / 1_000_000
_CLAUDE_OUTPUT_COST = 15.00 / 1_000_000
_CLAUDE_CACHE_READ  =  0.30 / 1_000_000

# Gemini 2.5 Flash pricing (per token, thinking disabled)
_GEMINI_INPUT_COST  = 0.15 / 1_000_000
_GEMINI_OUTPUT_COST = 0.60 / 1_000_000

_PERSIST_PATH = pathlib.Path(__file__).parent.parent.parent / "token_usage.json"
_TOTAL_PERSIST_PATH = pathlib.Path(__file__).parent.parent.parent / "total_token_usage.json"


@dataclass
class _ModelStats:
    calls:             int   = 0
    input_tokens:      int   = 0
    output_tokens:     int   = 0
    cache_read_tokens: int   = 0
    cost_usd:          float = 0.0


class TokenTracker:
    def __init__(self) -> None:
        self._lock   = threading.Lock()
        self._day    = datetime.date.today()
        self._claude = _ModelStats()
        self._gemini = _ModelStats()
        self._total_cost_usd = 0.0
        self._load()

    def _load(self) -> None:
        try:
            if _PERSIST_PATH.exists():
                data = json.loads(_PERSIST_PATH.read_text())
                if data.get("date") == self._day.isoformat():
                    c = data.get("claude", {})
                    g = data.get("gemini", {})
                    self._claude = _ModelStats(
                        calls=c.get("calls", 0),
                        input_tokens=c.get("input_tokens", 0),
                        output_tokens=c.get("output_tokens", 0),
                        cache_read_tokens=c.get("cache_read_tokens", 0),
                        cost_usd=c.get("cost_usd", 0.0),
                    )
                    self._gemini = _ModelStats(
                        calls=g.get("calls", 0),
                        input_tokens=g.get("input_tokens", 0),
                        output_tokens=g.get("output_tokens", 0),
                        cost_usd=g.get("cost_usd", 0.0),
                    )
            
            if _TOTAL_PERSIST_PATH.exists():
                total_data = json.loads(_TOTAL_PERSIST_PATH.read_text())
                self._total_cost_usd = total_data.get("total_cost_usd", 0.0)
        except Exception as exc:
            logger.debug("Token tracker load failed: %s", exc)

    def _save(self) -> None:
        try:
            _PERSIST_PATH.write_text(json.dumps({
                "date": self._day.isoformat(),
                "claude": {
                    "calls": self._claude.calls,
                    "input_tokens": self._claude.input_tokens,
                    "output_tokens": self._claude.output_tokens,
                    "cache_read_tokens": self._claude.cache_read_tokens,
                    "cost_usd": round(self._claude.cost_usd, 6),
                },
                "gemini": {
                    "calls": self._gemini.calls,
                    "input_tokens": self._gemini.input_tokens,
                    "output_tokens": self._gemini.output_tokens,
                    "cost_usd": round(self._gemini.cost_usd, 6),
                },
            }))
            _TOTAL_PERSIST_PATH.write_text(json.dumps({
                "total_cost_usd": round(self._total_cost_usd, 6),
            }))
        except Exception as exc:
            logger.debug("Token tracker save failed: %s", exc)

    def _maybe_reset(self) -> None:
        today = datetime.date.today()
        if today != self._day:
            self._day    = today
            self._claude = _ModelStats()
            self._gemini = _ModelStats()

    def record_claude(self, input_tokens: int, output_tokens: int, cache_read_tokens: int = 0) -> None:
        cost = (
            input_tokens        * _CLAUDE_INPUT_COST
            + output_tokens     * _CLAUDE_OUTPUT_COST
            + cache_read_tokens * _CLAUDE_CACHE_READ
        )
        with self._lock:
            self._maybe_reset()
            self._claude.calls             += 1
            self._claude.input_tokens      += input_tokens
            self._claude.output_tokens     += output_tokens
            self._claude.cache_read_tokens += cache_read_tokens
            self._claude.cost_usd          += cost
            self._total_cost_usd           += cost
            self._save()

    def record_gemini(self, input_tokens: int, output_tokens: int) -> None:
        cost = input_tokens * _GEMINI_INPUT_COST + output_tokens * _GEMINI_OUTPUT_COST
        with self._lock:
            self._maybe_reset()
            self._gemini.calls         += 1
            self._gemini.input_tokens  += input_tokens
            self._gemini.output_tokens += output_tokens
            self._gemini.cost_usd      += cost
            self._total_cost_usd       += cost
            self._save()

    def get_summary(self) -> dict:
        with self._lock:
            self._maybe_reset()
            return {
                "date": self._day.isoformat(),
                "claude": {
                    "calls":             self._claude.calls,
                    "input_tokens":      self._claude.input_tokens,
                    "output_tokens":     self._claude.output_tokens,
                    "cache_read_tokens": self._claude.cache_read_tokens,
                    "cost_usd":          round(self._claude.cost_usd, 4),
                },
                "gemini": {
                    "calls":         self._gemini.calls,
                    "input_tokens":  self._gemini.input_tokens,
                    "output_tokens": self._gemini.output_tokens,
                    "cost_usd":      round(self._gemini.cost_usd, 4),
                },
                "total_calls":    self._claude.calls + self._gemini.calls,
                "total_cost_usd": round(self._claude.cost_usd + self._gemini.cost_usd, 4),
                "lifetime_cost_usd": round(self._total_cost_usd, 4),
            }


# Module-level singleton
_tracker = TokenTracker()


def record_claude(input_tokens: int, output_tokens: int, cache_read_tokens: int = 0) -> None:
    _tracker.record_claude(input_tokens, output_tokens, cache_read_tokens)


def record_gemini(input_tokens: int, output_tokens: int) -> None:
    _tracker.record_gemini(input_tokens, output_tokens)


def get_summary() -> dict:
    return _tracker.get_summary()
