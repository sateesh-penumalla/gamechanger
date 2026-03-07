import pandas as pd
import numpy as np
from loguru import logger
from typing import List, Dict, Optional, Any

class SqueezeHunterAgent:
    """
    Agent designed to capture extreme mean-reversion moves (Short Squeezes / Rebounds).
    Target: High-probability 0.75% gains from capitulation.
    """
    def __init__(self, data_client: Any):
        self.data_client = data_client
        logger.info("Squeeze Hunter Agent Initialized - Watching for Capitulation...")

    def calculate_rsi(self, series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    def detect_rebound(self, symbol: str, data: pd.DataFrame) -> Dict:
        """
        Detects Oversold + Volume Blow-off + Hammer Candle logic.
        """
        if data is None or len(data) < 20:
            return {"signal": False}

        last_candle = data.iloc[-1]
        prev_candle = data.iloc[-2]

        # 1. RSI Condition (Extreme Oversold)
        rsi = self.calculate_rsi(data['Close']).iloc[-1]
        is_oversold = rsi < 25

        # 2. VWAP Deviation (Z-Score)
        v = data['Volume']
        p = data['Close']
        vwap = (p * v).cumsum() / v.cumsum()
        vwap_val = vwap.iloc[-1]
        std_dev = data['Close'].rolling(window=20).std().iloc[-1]
        
        # Is price more than 2 SD below VWAP?
        z_score = (last_candle['Close'] - vwap_val) / std_dev if std_dev > 0 else 0
        is_extended = z_score < -2.0

        # 3. Volume Capitulation (The Blow-off)
        avg_vol = data['Volume'].tail(14).mean()
        high_vol = last_candle['Volume'] > (avg_vol * 1.8)

        # 4. Pattern Check: Hammer (Lower Tail is 2x the Body)
        body = abs(last_candle['Open'] - last_candle['Close'])
        lower_tail = min(last_candle['Open'], last_candle['Close']) - last_candle['Low']
        is_hammer = lower_tail > (body * 2.0) if body > 0 else False
        
        # Bullish Engulfing
        is_engulfing = (last_candle['Close'] > last_candle['Open']) and \
                       (prev_candle['Close'] < prev_candle['Open']) and \
                       (last_candle['Close'] > prev_candle['Open']) and \
                       (last_candle['Open'] < prev_candle['Close'])

        if is_oversold and (is_hammer or is_engulfing) and high_vol and is_extended:
            return {
                "signal": True,
                "type": "REBOUND",
                "reason": f"Squeeze: RSI {rsi:.2f} | Z-Score {z_score:.2f} | Pattern: {'Hammer' if is_hammer else 'Engulfing'}",
                "confidence": 75,
                "entry": last_candle['Close'],
                "target": round(last_candle['Close'] * 1.0075, 2), # Strict 0.75%
                "sl": round(last_candle['Close'] * 0.99, 2) # 1% SL
            }

        return {"signal": False}

    def scan_for_rebounds(self, symbols: List[str]) -> List[Dict]:
        candidates = []
        for symbol in symbols:
            try:
                data = self.data_client.fetch_realtime_data(symbol, period="1d", interval="5m")
                if data is None or data.empty:
                    continue
                
                res = self.detect_rebound(symbol, data)
                if res["signal"]:
                    candidates.append({
                        "symbol": symbol,
                        "type": "REBOUND",
                        "reason": res["reason"],
                        "confidence": res["confidence"],
                        "entry": res["entry"],
                        "sl": res["sl"],
                        "target": res["target"]
                    })
            except Exception as e:
                logger.error(f"Squeeze Scan Error for {symbol}: {e}")
        return candidates
