"""Claude LLM decision layer.

Uses claude-opus-4-8 with adaptive thinking to make final trade decisions
based on technical indicator signals and portfolio context.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Optional

import anthropic

from argus.strategy.indicators import SignalResult

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are Argus, an AI trading agent. You receive technical analysis signals
and portfolio context, then decide whether to BUY, SELL, or HOLD a position.

Rules you must follow:
- Only output valid JSON in the exact schema specified.
- Never recommend risking more than the caller's position sizing limits.
- Be conservative — when in doubt, HOLD.
- Factor in the composite technical signal, individual indicator readings, and
  overall portfolio health.
- Briefly explain your reasoning in the "reasoning" field (2-3 sentences max).

Output schema (strict JSON, no markdown fences):
{
  "action": "BUY" | "SELL" | "HOLD",
  "confidence": 0.0-1.0,
  "reasoning": "string"
}"""


@dataclass
class TradeDecision:
    symbol: str
    action: str          # "BUY" | "SELL" | "HOLD"
    confidence: float
    reasoning: str
    raw_response: str = ""


class DecisionEngine:
    def __init__(self, api_key: str) -> None:
        self._client = anthropic.Anthropic(api_key=api_key)

    def decide(
        self,
        signal: SignalResult,
        portfolio_equity: float,
        open_positions: dict,
        daily_pnl_pct: float = 0.0,
    ) -> TradeDecision:
        prompt = _build_prompt(signal, portfolio_equity, open_positions, daily_pnl_pct)
        try:
            return self._call_claude(signal.symbol, prompt)
        except Exception as exc:
            logger.error("Claude decision failed for %s: %s", signal.symbol, exc)
            return TradeDecision(
                symbol=signal.symbol,
                action="HOLD",
                confidence=0.0,
                reasoning=f"Decision engine error: {exc}",
            )

    def _call_claude(self, symbol: str, prompt: str) -> TradeDecision:
        with self._client.messages.stream(
            model="claude-opus-4-8",
            max_tokens=512,
            thinking={"type": "adaptive"},
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            message = stream.get_final_message()

        raw = ""
        for block in message.content:
            if block.type == "text":
                raw = block.text
                break

        return _parse_response(symbol, raw)


def _build_prompt(
    signal: SignalResult,
    portfolio_equity: float,
    open_positions: dict,
    daily_pnl_pct: float,
) -> str:
    holding = signal.symbol in open_positions
    pos_info = ""
    if holding:
        pos = open_positions[signal.symbol]
        pos_info = (
            f"\nCurrent position: {pos.get('qty', 0):.4f} units @ avg ${pos.get('avg_price', 0):.2f}"
        )

    rsi_str = f"{signal.rsi:.1f}" if signal.rsi is not None else "N/A"
    macd_str = f"{signal.macd:.4f}" if signal.macd is not None else "N/A"
    macd_hist_str = f"{signal.macd_hist:.4f}" if signal.macd_hist is not None else "N/A"
    bb_str = (
        f"lower={signal.bb_lower:.2f} mid={signal.bb_mid:.2f} upper={signal.bb_upper:.2f}"
        if signal.bb_lower is not None
        else "N/A"
    )
    sma_str = f"{signal.sma_20:.2f}" if signal.sma_20 is not None else "N/A"
    ema_str = f"{signal.ema_50:.2f}" if signal.ema_50 is not None else "N/A"

    return f"""Symbol: {signal.symbol}
Current price: ${signal.price:.4f}
Composite signal: {signal.composite} (confidence {signal.confidence:.0%})

Technical indicators:
  RSI (14): {rsi_str}
  MACD (12/26/9): {macd_str} | histogram: {macd_hist_str}
  Bollinger Bands: {bb_str}
  SMA-20: {sma_str}
  EMA-50: {ema_str}

Portfolio context:
  Equity: ${portfolio_equity:,.2f}
  Open positions: {len(open_positions)} / 5 max
  Daily P&L: {daily_pnl_pct:+.2f}%
  Currently holding {signal.symbol}: {'YES' + pos_info if holding else 'NO'}

Should I BUY, SELL, or HOLD {signal.symbol} right now?"""


def _parse_response(symbol: str, raw: str) -> TradeDecision:
    try:
        data = json.loads(raw.strip())
        action = str(data.get("action", "HOLD")).upper()
        if action not in ("BUY", "SELL", "HOLD"):
            action = "HOLD"
        confidence = float(data.get("confidence", 0.0))
        confidence = max(0.0, min(1.0, confidence))
        reasoning = str(data.get("reasoning", ""))[:4000]
        return TradeDecision(symbol=symbol, action=action, confidence=confidence, reasoning=reasoning, raw_response=raw)
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        logger.warning("Failed to parse Claude response for %s: %s | raw=%r", symbol, exc, raw[:200])
        return TradeDecision(
            symbol=symbol,
            action="HOLD",
            confidence=0.0,
            reasoning="Could not parse AI response.",
            raw_response=raw,
        )
