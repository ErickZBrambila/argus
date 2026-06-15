"""AI decision layer — Claude + Gemini ensemble.

Both models receive the same signal snapshot and vote independently.
Consensus rules:
  - Both agree on BUY or SELL  → execute, average confidence
  - One says HOLD, other BUY/SELL → defer to HOLD (conservative)
  - Claude BUY vs Gemini SELL (or vice versa) → HOLD (contradiction = high risk)

If only one model is configured, it runs solo with no penalty.
Risk classification accounts for agreement: disagreement always escalates risk.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Optional

import anthropic

from argus.strategy.indicators import SignalResult

logger = logging.getLogger(__name__)

# ── AI status tracking ────────────────────────────────────────────────────────
# green = last call ok | yellow = billing/quota | red = auth/config error | gray = never called
_ai_status: dict = {"claude": "gray", "gemini": "gray"}


def get_ai_status() -> dict:
    return dict(_ai_status)


def _classify_error(exc: Exception) -> str:
    msg = str(exc).lower()
    if "401" in msg or ("invalid" in msg and "key" in msg):
        return "red"
    if "402" in msg or "credit" in msg or "balance" in msg or "billing" in msg:
        return "yellow"
    if "429" in msg or "503" in msg or "quota" in msg or "rate" in msg or "resource_exhausted" in msg or "unavailable" in msg:
        return "yellow"
    return "red"

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
    action: str           # "BUY" | "SELL" | "HOLD"
    confidence: float
    reasoning: str
    raw_response: str = ""
    risk_level: str = "medium"    # "low" | "medium" | "high"
    models_used: str = "claude"   # "claude" | "gemini" | "ensemble"
    consensus: bool = True        # False when models disagreed → HOLD
    is_error: bool = False        # True when both models failed — caller should alert


def classify_risk(signal_confidence: float, decision_confidence: float, consensus: bool = True) -> str:
    if not consensus:
        return "high"
    if signal_confidence >= 0.7 and decision_confidence >= 0.7:
        return "low"
    if signal_confidence >= 0.4 or decision_confidence >= 0.5:
        return "medium"
    return "high"


CLAUDE_MODEL = "claude-sonnet-4-6"
GEMINI_MODEL = "gemini-2.5-flash"


def get_model_info() -> dict:
    return {"claude": CLAUDE_MODEL, "gemini": GEMINI_MODEL}


# ── Claude ────────────────────────────────────────────────────────────────────

class _ClaudeEngine:
    def __init__(self, api_key: str) -> None:
        self._client = anthropic.Anthropic(api_key=api_key)

    def decide(self, symbol: str, prompt: str) -> TradeDecision:
        try:
            with self._client.messages.stream(
                model=CLAUDE_MODEL,
                max_tokens=1024,
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

            try:
                from argus.dashboard.token_tracker import record_claude
                u = message.usage
                record_claude(u.input_tokens, u.output_tokens,
                              getattr(u, "cache_read_input_tokens", 0))
            except Exception:
                pass

            d = _parse_response(symbol, raw)
            d.models_used = "claude"
            _ai_status["claude"] = "green"
            return d
        except Exception as exc:
            logger.error("Claude decision failed for %s: %s", symbol, exc)
            _ai_status["claude"] = _classify_error(exc)
            return _error_hold(symbol, f"Claude error: {exc}", "claude")


# ── Gemini ────────────────────────────────────────────────────────────────────

class _GeminiEngine:
    def __init__(self, api_key: str) -> None:
        from google import genai
        from google.genai import types as _gt
        self._client = genai.Client(api_key=api_key)
        self._config = _gt.GenerateContentConfig(
            system_instruction=_SYSTEM_PROMPT,
            temperature=0.2,
            max_output_tokens=1024,
            thinking_config=_gt.ThinkingConfig(thinking_budget=0),
        )

    def decide(self, symbol: str, prompt: str) -> TradeDecision:
        try:
            response = self._client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=self._config,
            )
            raw = response.text.strip()
            # Strip markdown fences if Gemini wraps its JSON
            if raw.startswith("```"):
                raw = "\n".join(raw.split("\n")[1:])
                raw = raw.rstrip("`").strip()
            try:
                from argus.dashboard.token_tracker import record_gemini
                m = response.usage_metadata
                record_gemini(
                    getattr(m, "prompt_token_count", 0),
                    getattr(m, "candidates_token_count", 0),
                )
            except Exception:
                pass

            d = _parse_response(symbol, raw)
            d.models_used = "gemini"
            _ai_status["gemini"] = "green"
            return d
        except Exception as exc:
            logger.error("Gemini decision failed for %s: %s", symbol, exc)
            _ai_status["gemini"] = _classify_error(exc)
            return _error_hold(symbol, f"Gemini error: {exc}", "gemini")


# ── Ensemble ──────────────────────────────────────────────────────────────────

def _consensus(claude: TradeDecision, gemini: TradeDecision, symbol: str) -> TradeDecision:
    ca, ga = claude.action, gemini.action

    if ca == ga:
        # Full agreement
        avg_conf = round((claude.confidence + gemini.confidence) / 2, 3)
        return TradeDecision(
            symbol=symbol,
            action=ca,
            confidence=avg_conf,
            reasoning=f"[Claude] {claude.reasoning}  [Gemini] {gemini.reasoning}",
            models_used="ensemble",
            consensus=True,
        )

    # One HOLD, one directional → conservative HOLD
    if ca == "HOLD" or ga == "HOLD":
        dissenter = gemini if ca == "HOLD" else claude
        logger.info(
            "Ensemble: models split on %s (Claude=%s, Gemini=%s) — holding",
            symbol, ca, ga,
        )
        return TradeDecision(
            symbol=symbol,
            action="HOLD",
            confidence=min(claude.confidence, gemini.confidence),
            reasoning=(
                f"Models disagreed — holding. "
                f"[Claude] {claude.reasoning}  [Gemini] {gemini.reasoning}"
            ),
            models_used="ensemble",
            consensus=False,
        )

    # Direct contradiction (BUY vs SELL) → hard HOLD, high risk
    logger.warning(
        "Ensemble: contradiction on %s (Claude=%s, Gemini=%s) — hard hold",
        symbol, ca, ga,
    )
    return TradeDecision(
        symbol=symbol,
        action="HOLD",
        confidence=0.0,
        reasoning=(
            f"Contradiction: Claude says {ca}, Gemini says {ga}. "
            f"[Claude] {claude.reasoning}  [Gemini] {gemini.reasoning}"
        ),
        models_used="ensemble",
        consensus=False,
    )


# ── Public engine ─────────────────────────────────────────────────────────────

class DecisionEngine:
    """Ensemble decision engine. Uses both Claude and Gemini when Gemini key is set."""

    def __init__(self, anthropic_key: str, gemini_key: Optional[str] = None) -> None:
        self._claude = _ClaudeEngine(anthropic_key)
        self._gemini: Optional[_GeminiEngine] = None
        if gemini_key:
            try:
                self._gemini = _GeminiEngine(gemini_key)
                logger.info("Ensemble mode: Claude + Gemini")
            except Exception as exc:
                logger.warning("Gemini init failed, running Claude-only: %s", exc)

    def decide(
        self,
        signal: SignalResult,
        portfolio_equity: float,
        open_positions: dict,
        daily_pnl_pct: float = 0.0,
        max_positions: int = 5,
    ) -> TradeDecision:
        prompt = _build_prompt(signal, portfolio_equity, open_positions, daily_pnl_pct, max_positions)
        try:
            if self._gemini:
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                    f_claude = ex.submit(self._claude.decide, signal.symbol, prompt)
                    f_gemini = ex.submit(self._gemini.decide, signal.symbol, prompt)
                    claude_d = f_claude.result(timeout=30)
                    gemini_d = f_gemini.result(timeout=30)
                decision = _consensus(claude_d, gemini_d, signal.symbol)
            else:
                decision = self._claude.decide(signal.symbol, prompt)

            decision.risk_level = classify_risk(
                signal.confidence, decision.confidence, decision.consensus
            )
            return decision
        except Exception as exc:
            logger.error("DecisionEngine failed for %s: %s", signal.symbol, exc)
            return _error_hold(signal.symbol, str(exc))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_prompt(
    signal: SignalResult,
    portfolio_equity: float,
    open_positions: dict,
    daily_pnl_pct: float,
    max_positions: int = 5,
) -> str:
    holding = signal.symbol in open_positions
    pos_info = ""
    if holding:
        pos = open_positions[signal.symbol]
        pos_info = (
            f"\nCurrent position: {pos.get('qty', 0):.4f} units @ avg ${pos.get('avg_price', 0):.2f}"
        )

    rsi_str      = f"{signal.rsi:.1f}"      if signal.rsi      is not None else "N/A"
    macd_str     = f"{signal.macd:.4f}"     if signal.macd     is not None else "N/A"
    macd_hist_str= f"{signal.macd_hist:.4f}"if signal.macd_hist is not None else "N/A"
    bb_str = (
        f"lower={signal.bb_lower:.2f} mid={signal.bb_mid:.2f} upper={signal.bb_upper:.2f}"
        if signal.bb_lower is not None else "N/A"
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
  Open positions: {len(open_positions)} / {max_positions} max
  Daily P&L: {daily_pnl_pct:+.2f}%
  Currently holding {signal.symbol}: {'YES' + pos_info if holding else 'NO'}

Should I BUY, SELL, or HOLD {signal.symbol} right now?"""


def _parse_response(symbol: str, raw: str) -> TradeDecision:
    try:
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = "\n".join(cleaned.split("\n")[1:]).rstrip("`").strip()
        data = json.loads(cleaned)
        action = str(data.get("action", "HOLD")).upper()
        if action not in ("BUY", "SELL", "HOLD"):
            action = "HOLD"
        confidence = float(data.get("confidence", 0.0))
        confidence = max(0.0, min(1.0, confidence))
        reasoning  = str(data.get("reasoning", ""))[:4000]
        return TradeDecision(
            symbol=symbol, action=action, confidence=confidence,
            reasoning=reasoning, raw_response=raw,
        )
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        logger.warning("Failed to parse response for %s: %s | raw=%r", symbol, exc, raw[:200])
        return _error_hold(symbol, "Could not parse AI response.")


def _error_hold(symbol: str, reason: str, model: str = "unknown") -> TradeDecision:
    return TradeDecision(
        symbol=symbol, action="HOLD", confidence=0.0,
        reasoning=reason, risk_level="high", models_used=model, is_error=True,
    )
