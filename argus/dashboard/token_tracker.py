"""Thread-safe token and cost tracker for Claude + Gemini API calls.

Resets daily at midnight. Costs are estimates based on public pricing.
"""

from __future__ import annotations

import datetime
import threading
from dataclasses import dataclass, field

# Claude Opus 4.8 pricing (per token)
_CLAUDE_INPUT_COST  = 15.00 / 1_000_000
_CLAUDE_OUTPUT_COST = 75.00 / 1_000_000
_CLAUDE_CACHE_READ  =  1.50 / 1_000_000

# Gemini 2.0 Flash pricing (per token)
_GEMINI_INPUT_COST  = 0.10 / 1_000_000
_GEMINI_OUTPUT_COST = 0.40 / 1_000_000


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

    def _maybe_reset(self) -> None:
        today = datetime.date.today()
        if today != self._day:
            self._day    = today
            self._claude = _ModelStats()
            self._gemini = _ModelStats()

    def record_claude(
        self,
        input_tokens: int,
        output_tokens: int,
        cache_read_tokens: int = 0,
    ) -> None:
        cost = (
            input_tokens      * _CLAUDE_INPUT_COST
            + output_tokens   * _CLAUDE_OUTPUT_COST
            + cache_read_tokens * _CLAUDE_CACHE_READ
        )
        with self._lock:
            self._maybe_reset()
            self._claude.calls             += 1
            self._claude.input_tokens      += input_tokens
            self._claude.output_tokens     += output_tokens
            self._claude.cache_read_tokens += cache_read_tokens
            self._claude.cost_usd          += cost

    def record_gemini(self, input_tokens: int, output_tokens: int) -> None:
        cost = input_tokens * _GEMINI_INPUT_COST + output_tokens * _GEMINI_OUTPUT_COST
        with self._lock:
            self._maybe_reset()
            self._gemini.calls         += 1
            self._gemini.input_tokens  += input_tokens
            self._gemini.output_tokens += output_tokens
            self._gemini.cost_usd      += cost

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
                    "calls":        self._gemini.calls,
                    "input_tokens": self._gemini.input_tokens,
                    "output_tokens":self._gemini.output_tokens,
                    "cost_usd":     round(self._gemini.cost_usd, 4),
                },
                "total_calls":    self._claude.calls + self._gemini.calls,
                "total_cost_usd": round(self._claude.cost_usd + self._gemini.cost_usd, 4),
            }


# Module-level singleton
_tracker = TokenTracker()


def record_claude(input_tokens: int, output_tokens: int, cache_read_tokens: int = 0) -> None:
    _tracker.record_claude(input_tokens, output_tokens, cache_read_tokens)


def record_gemini(input_tokens: int, output_tokens: int) -> None:
    _tracker.record_gemini(input_tokens, output_tokens)


def get_summary() -> dict:
    return _tracker.get_summary()
