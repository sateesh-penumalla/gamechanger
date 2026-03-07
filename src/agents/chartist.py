from loguru import logger
import pandas as pd
import numpy as np
from typing import Dict, Optional
from src.db.schema import get_ist_now

class ChartistAgent:
    """Technical analysis agent using pure pandas (dependency-lite)"""
    
    def __init__(self):
        logger.info("Chartist Agent Initialized (Dependency-Lite)")

    def _calculate_indicators_lite(self, data: pd.DataFrame) -> pd.DataFrame:
        """Internal helper to calculate Indicators without pandas_ta"""
        df = data.copy()
        
        # 1. VWAP
        q = df.volume
        p = (df.high + df.low + df.close) / 3
        df['VWAP'] = (p * q).cumsum() / q.cumsum()
        
        # 2. RSI (14)
        delta = df.close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI_14'] = 100 - (100 / (1 + rs))
        
        # 3. Supertrend (10, 3)
        # Simplified Supertrend implementation
        period = 10
        multiplier = 3
        
        df['tr'] = np.maximum(df['high'] - df['low'], 
                             np.maximum(abs(df['high'] - df['close'].shift(1)), 
                                      abs(df['low'] - df['close'].shift(1))))
        df['atr'] = df['tr'].rolling(window=period).mean()
        
        df['upper_band'] = ((df.high + df.low) / 2) + (multiplier * df['atr'])
        df['lower_band'] = ((df.high + df.low) / 2) - (multiplier * df['atr'])
        
        df['st'] = 0.0
        df['st_dir'] = 1 # 1 for up, -1 for down
        
        for i in range(period, len(df)):
            if df.close.iloc[i] > df.upper_band.iloc[i-1]:
                df.at[df.index[i], 'st_dir'] = 1
            elif df.close.iloc[i] < df.lower_band.iloc[i-1]:
                df.at[df.index[i], 'st_dir'] = -1
            else:
                df.at[df.index[i], 'st_dir'] = df.st_dir.iloc[i-1]
                
            if df.st_dir.iloc[i] == 1:
                df.at[df.index[i], 'st'] = df.lower_band.iloc[i]
            else:
                df.at[df.index[i], 'st'] = df.upper_band.iloc[i]
                
        return df

    def analyze_technical_setup(self, data: pd.DataFrame) -> Dict:
        if data.empty or len(data) < 20:
            return {"conviction": 0, "reason": "Insufficient data"}

        # Calculate indicators
        data = self._calculate_indicators_lite(data)
        current = data.iloc[-1]
        
        rsi_val = current['RSI_14'] if not np.isnan(current['RSI_14']) else 50.0
        vwap_val = current['VWAP']
        close_price = current['close']
        supertrend_dir = current['st_dir'] # 1 for up, -1 for down

        # Weighted Conviction Logic
        conviction_score = 0
        sentiment = "Neutral"
        
        if close_price > vwap_val: conviction_score += 30
        if supertrend_dir == 1: conviction_score += 30
        if 55 <= rsi_val <= 75: conviction_score += 20
        elif 45 <= rsi_val < 55: conviction_score += 10
            
        if conviction_score >= 50: sentiment = "Bullish"
        elif conviction_score <= 20: sentiment = "Neutral"

        return {
            "sentiment": sentiment,
            "conviction": conviction_score,
            "indicators": {
                "rsi": round(rsi_val, 2),
                "at_vwap": bool(close_price > vwap_val),
                "supertrend": "Up" if supertrend_dir == 1 else "Down",
                "vwap": round(vwap_val, 2)
            }
        }

    def get_execution_levels(self, data: pd.DataFrame) -> Optional[Dict]:
        latest_date = data.index.date[-1] if hasattr(data.index, 'date') else pd.to_datetime(data.index).date[-1]
        
        if latest_date != get_ist_now().date():
            return None
            
        today_data = data[data.index.date == latest_date] if hasattr(data.index, 'date') else data[pd.to_datetime(data.index).date == latest_date]
        
        if len(today_data) < 3: return None

        opening_range = today_data.iloc[:3]
        orb_high = opening_range['high'].max()
        orb_low = opening_range['low'].min()
        
        entry = orb_high * 1.001
        sl = orb_low * 0.999 
        
        risk_pct = ((entry - sl) / entry) * 100
        if risk_pct > 1.5:
            sl = entry * 0.985 
            risk_pct = 1.5

        risk_amount = entry - sl
        t1 = entry + risk_amount
        t2 = entry + (risk_amount * 2)

        return {
            "orb_high": round(orb_high, 2),
            "orb_low": round(orb_low, 2),
            "entry": round(entry, 2),
            "stop_loss": round(sl, 2),
            "target_1": round(t1, 2),
            "target_2": round(t2, 2),
            "risk_pct": round(risk_pct, 2)
        }

    def get_swing_levels(self, data: pd.DataFrame, entry_price: float, intraday_sl: float) -> Dict:
        if data.empty or len(data) < 20:
             return {"target_3": round(entry_price * 1.05, 2), "trailing_sl": intraday_sl}

        # Calculate EMAs manually
        data['EMA_9'] = data.close.ewm(span=9, adjust=False).mean()
        data['EMA_20'] = data.close.ewm(span=20, adjust=False).mean()
        
        current = data.iloc[-1]
        ema9 = current['EMA_9']
        ema20 = current['EMA_20']
        
        trailing_sl = max(ema9, ema20) if max(ema9, ema20) < entry_price else min(ema9, ema20)
        
        risk_amount = entry_price - intraday_sl
        t3_rr = entry_price + (risk_amount * 3)
        t3_pct = entry_price * 1.05
        t3 = max(t3_rr, t3_pct)

        return {
            "target_3": round(t3, 2),
            "ema_9": round(ema9, 2),
            "ema_20": round(ema20, 2),
            "suggested_trailing_sl": round(trailing_sl, 2)
        }
