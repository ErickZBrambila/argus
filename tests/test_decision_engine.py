import pytest
from argus.agent.decision import TradeDecision, _consensus, classify_risk

def test_consensus_agreement():
    claude = TradeDecision(symbol="AAPL", action="BUY", confidence=0.8, reasoning="c")
    gemini = TradeDecision(symbol="AAPL", action="BUY", confidence=0.6, reasoning="g")
    
    decision = _consensus(claude, gemini, "AAPL")
    assert decision.action == "BUY"
    assert decision.confidence == 0.7
    assert decision.consensus is True

def test_consensus_disagreement_hold():
    # Majority rules: directional vote wins over HOLD, at high risk
    claude = TradeDecision(symbol="AAPL", action="BUY", confidence=0.8, reasoning="c")
    gemini = TradeDecision(symbol="AAPL", action="HOLD", confidence=0.6, reasoning="g")

    decision = _consensus(claude, gemini, "AAPL")
    assert decision.action == "BUY"
    assert decision.confidence == 0.8  # directional model's confidence
    assert decision.consensus is False  # still flagged as split → high risk

def test_consensus_contradiction():
    claude = TradeDecision(symbol="AAPL", action="BUY", confidence=0.8, reasoning="c")
    gemini = TradeDecision(symbol="AAPL", action="SELL", confidence=0.6, reasoning="g")
    
    decision = _consensus(claude, gemini, "AAPL")
    assert decision.action == "HOLD"
    assert decision.confidence == 0.0
    assert decision.consensus is False

def test_classify_risk():
    assert classify_risk(0.8, 0.8, True) == "low"
    assert classify_risk(0.8, 0.6, True) == "medium"
    assert classify_risk(0.6, 0.8, True) == "medium"
    assert classify_risk(0.4, 0.5, True) == "medium"
    assert classify_risk(0.3, 0.4, True) == "high"
    assert classify_risk(0.8, 0.8, False) == "high"
