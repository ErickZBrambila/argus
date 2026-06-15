import pytest
from argus.risk.manager import RiskManager

def test_risk_manager_kill_switch():
    rm = RiskManager(daily_drawdown_limit=-0.05)
    rm.set_session_equity(10000)
    
    assert rm.check_drawdown(10000) is False
    assert rm.kill_switch_active is False
    
    assert rm.check_drawdown(9500) is True  # exact limit triggers kill switch
    assert rm.check_drawdown(9499) is True  # below limit
    assert rm.kill_switch_active is True

def test_risk_manager_approve_buy():
    rm = RiskManager(max_position_pct=0.10, max_positions=5, pdt_aware=True)
    rm.set_session_equity(10000)
    
    decision = rm.approve_buy("AAPL", 10000, {})
    assert decision.allowed is True
    assert decision.dollar_amount == 1000.0
    
    # already holding
    decision = rm.approve_buy("AAPL", 10000, {"AAPL": {}})
    assert decision.allowed is False
    assert "Already holding" in decision.reason
    
    # max positions
    decision = rm.approve_buy("TSLA", 10000, {"1": {}, "2": {}, "3": {}, "4": {}, "5": {}})
    assert decision.allowed is False
    
    # pdt
    rm._day_trade_count = 3
    decision = rm.approve_buy("MSFT", 10000, {})
    assert decision.allowed is False
    assert "PDT" in decision.reason
