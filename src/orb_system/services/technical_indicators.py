import pandas as pd
import numpy as np
from typing import List, Tuple, Dict
from src.orb_system.models.data_models import SignalType

class TechnicalIndicators:
    """Calculate technical indicators for ORB system using pure pandas/numpy (dependency-lite)"""
    
    @staticmethod
    def calculate_rsi(prices: List[float], period: int = 14) -> float:
        """Calculate RSI using pure pandas"""
        if len(prices) < period + 1:
            return 50.0
        
        series = pd.Series(prices)
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        val = rsi.iloc[-1]
        return float(val) if not np.isnan(val) else 50.0
    
    @staticmethod
    def calculate_macd(prices: List[float], 
                       fast: int = 12, 
                       slow: int = 26, 
                       signal: int = 9) -> Tuple[float, float, float]:
        """Calculate MACD using pure pandas"""
        if len(prices) < slow:
            return 0.0, 0.0, 0.0
        
        series = pd.Series(prices)
        exp1 = series.ewm(span=fast, adjust=False).mean()
        exp2 = series.ewm(span=slow, adjust=False).mean()
        macd = exp1 - exp2
        signal_line = macd.ewm(span=signal, adjust=False).mean()
        hist = macd - signal_line
        
        return float(macd.iloc[-1]), float(signal_line.iloc[-1]), float(hist.iloc[-1])
    
    @staticmethod
    def calculate_ema(prices: List[float], period: int) -> float:
        """Calculate EMA using pure pandas"""
        if len(prices) < period:
            return float(np.mean(prices)) if prices else 0.0
        
        series = pd.Series(prices)
        ema = series.ewm(span=period, adjust=False).mean()
        return float(ema.iloc[-1])
    
    @staticmethod
    def calculate_atr(high: List[float], 
                      low: List[float], 
                      close: List[float], 
                      period: int = 14) -> float:
        """Calculate ATR using pure pandas"""
        if len(close) < period:
            return 0.0
        
        df = pd.DataFrame({'high': high, 'low': low, 'close': close})
        df['tr'] = np.maximum(df['high'] - df['low'], 
                             np.maximum(abs(df['high'] - df['close'].shift(1)), 
                                      abs(df['low'] - df['close'].shift(1))))
        atr = df['tr'].rolling(window=period).mean()
        
        val = atr.iloc[-1]
        return float(val) if not np.isnan(val) else 0.0
    
    @staticmethod
    def calculate_vwap(df: pd.DataFrame) -> pd.Series:
        """Calculate VWAP"""
        q = df.volume
        p = (df.high + df.low + df.close) / 3
        return (p * q).cumsum() / q.cumsum()
    
    @staticmethod
    def calculate_trend_strength(prices: List[float], 
                                  period: int = 20) -> float:
        """Calculate trend strength (normalized slope)"""
        if len(prices) < period:
            return 0.0
        
        recent_prices = prices[-period:]
        x = np.arange(len(recent_prices))
        y = np.array(recent_prices)
        
        coeffs = np.polyfit(x, y, 1)
        slope = coeffs[0]
        avg_price = np.mean(recent_prices)
        normalized_slope = (slope / avg_price) * 100
        
        return float(normalized_slope)
    
    @staticmethod
    def score_rsi(rsi: float, signal_type: SignalType) -> int:
        if signal_type == SignalType.BREAKOUT:
            if rsi > 70: return 0
            elif 60 <= rsi <= 70: return 25
            elif 50 <= rsi < 60: return 15
            else: return 5
        else: # BREAKDOWN
            if rsi < 30: return 0
            elif 30 <= rsi <= 40: return 25
            elif 40 <= rsi < 50: return 15
            else: return 5
    
    @staticmethod
    def score_macd(macd_value: float, signal_type: SignalType) -> int:
        if signal_type == SignalType.BREAKOUT:
            if macd_value > 0: return 25
            elif macd_value > -0.05: return 10
            else: return 0
        else: # BREAKDOWN
            if macd_value < 0: return 25
            elif macd_value < 0.05: return 10
            else: return 0
            
    @staticmethod
    def score_trend(trend_strength: float, signal_type: SignalType) -> int:
        if signal_type == SignalType.BREAKOUT:
            return 25 if trend_strength > 0.1 else (10 if trend_strength > 0 else 0)
        else: # BREAKDOWN
            return 25 if trend_strength < -0.1 else (10 if trend_strength < 0 else 0)

    @staticmethod
    def score_volume(volume_ratio: float) -> int:
        if volume_ratio > 2.0: return 25
        elif volume_ratio > 1.2: return 15
        else: return 0
