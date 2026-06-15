import pandas as pd
import pytest
from argus.strategy.indicators import SignalEngine, SignalResult, _build_dataframe, DefaultStrategy

def test_build_dataframe():
    raw_data = [
        {"open_price": "100", "high_price": "105", "low_price": "95", "close_price": "102", "volume": "1000"},
        {"open_price": "102", "high_price": "103", "low_price": "100", "close_price": "101", "volume": "1200"},
    ]
    df = _build_dataframe(raw_data)
    
    assert df is not None
    assert "close" in df.columns
    assert len(df) == 2
    assert df.iloc[0]["close"] == 102.0
    assert df.iloc[1]["volume"] == 1200.0

def test_default_strategy_score():
    strategy = DefaultStrategy()
    
    # Perfect bullish setup
    composite, conf = strategy.score(
        price=105.0,
        rsi=25.0,          # Bullish (< 30)
        macd_hist=0.5,     # Bullish (> 0)
        bb_upper=110.0,
        bb_lower=106.0,    # Bullish (price <= bb_lower, wait price=105 <= 106)
        sma_20=100.0,      # Bullish (price > sma)
        ema_50=95.0        # Bullish (price > ema)
    )
    assert composite == "bullish"
    assert conf == 1.0
    
    # Neutral/mixed setup
    composite, conf = strategy.score(
        price=105.0,
        rsi=50.0,          # Neutral
        macd_hist=0.5,     # Bullish
        bb_upper=110.0,
        bb_lower=100.0,    # Neutral
        sma_20=110.0,      # Bearish
        ema_50=95.0        # Bullish
    )
    # 2 bullish, 1 bearish, 2 neutral (out of 5 evaluated indicators)
    # bullish > bearish -> bullish, conf = 2 / 5 = 0.4
    assert composite == "bullish"
    assert conf == 0.4
