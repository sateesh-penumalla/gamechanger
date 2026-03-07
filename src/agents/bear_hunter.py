import numpy as np
import pandas as pd
from loguru import logger
from typing import List, Dict, Optional, Any
from scipy.stats import linregress

class BearHunterAgent:
    """
    Specialized agent for detecting "Panic Selling" and "Bearish Flag" continuation patterns.
    Designed for high-accuracy short-sell opportunities.
    """
    def __init__(self, data_client: Any):
        self.data_client = data_client
        logger.info("Bear Hunter Agent Initialized - Hunting for Panic...")

    def detect_panic(self, symbol: str, data: pd.DataFrame) -> Dict:
        """
        Detects "The Waterfall" and "The Dam Break" signals.
        """
        if data is None or len(data) < 15:
            return {"panic_found": False}

        # 1. VELOCITY CHECK: The Waterfall (%)
        # Drop in last 15 mins (assuming 5m candles or 1m pulse)
        # Using 3 candles for the "Waterfall" expansion rule
        last_3 = data.tail(3)
        
        is_waterfall = (
            (last_3.iloc[0]['Close'] < last_3.iloc[0]['Open']) and
            (last_3.iloc[1]['Close'] < last_3.iloc[1]['Open']) and
            (last_3.iloc[2]['Close'] < last_3.iloc[2]['Open']) and
            (abs(last_3.iloc[2]['Close'] - last_3.iloc[2]['Open']) > abs(last_3.iloc[1]['Close'] - last_3.iloc[1]['Open'])) # Expansion
        )

        drop_15m = (data['Close'].iloc[-1] - data['Open'].iloc[-3]) / data['Open'].iloc[-3]
        is_crashing = drop_15m < -0.015 # > 1.5% drop

        # 2. VOLUME ACCELERATION
        avg_vol = data['Volume'].tail(10).mean()
        current_vol = data['Volume'].iloc[-1]
        vol_spike = current_vol > (avg_vol * 2.0)

        # 3. DAM BREAK: Below VWAP + Level Break
        # Simple VWAP calculation
        v = data['Volume']
        p = data['Close']
        vwap = (p * v).cumsum() / v.cumsum()
        below_vwap = p.iloc[-1] < vwap.iloc[-1]

        if (is_crashing or is_waterfall) and vol_spike and below_vwap:
            return {
                "panic_found": True,
                "type": "WATERFALL" if is_waterfall else "VELOCITY_DROP",
                "velocity": round(drop_15m * 100, 2),
                "vol_ratio": round(current_vol / avg_vol, 2)
            }
        
        return {"panic_found": False}

    def analyze_bear_flag(self, symbol: str, data: pd.DataFrame) -> Dict:
        """
        Confirms panic selling continuation using Bearish Flag geometry.
        """
        if len(data) < 15:
            return {"signal": False}

        # --- STEP 1: POLE (The Panic Pulse) ---
        pole_start = data['High'].iloc[-15]
        pole_end = data['Low'].iloc[-5]
        pole_drop = (pole_end - pole_start) / pole_start
        
        if pole_drop > -0.012: # Threshold 1.2% for the pole
            return {"signal": False, "reason": "Pole not steep enough"}

        # --- STEP 2: FLAG (The Counter-Trend Drift) ---
        flag_data = data.tail(5)
        flag_prices = flag_data['Close']
        flag_vols = flag_data['Volume']
        
        # Calculate Slope (Expect slight positive drift)
        slope, intercept, r_val, p_val, std_err = linregress(range(5), flag_prices)
        
        avg_pole_vol = data['Volume'].iloc[-15:-5].mean()
        avg_flag_vol = flag_vols.mean()
        
        is_drifting_up = slope > 0
        is_vol_drying = avg_flag_vol < (avg_pole_vol * 0.8) # 20% drop in conviction

        # --- STEP 3: FIBONACCI RETRACEMENT (Max 50%) ---
        bounce = flag_prices.max() - pole_end
        pole_height = pole_start - pole_end
        valid_retrace = (bounce / pole_height) < 0.50

        if is_drifting_up and is_vol_drying and valid_retrace:
            return {
                "signal": True,
                "pattern": "BEAR_FLAG",
                "slope": round(slope, 4),
                "stop_loss": round(flag_prices.max(), 2),
                "target": round(pole_end - pole_height, 2)
            }
        
        return {"signal": False}

    def scan_for_shorts(self, symbols: List[str]) -> List[Dict]:
        """
        Main entry point for the Shadow Short Scanner.
        """
        short_candidates = []
        for symbol in symbols:
            try:
                data = self.data_client.fetch_realtime_data(symbol, period="1d", interval="5m")
                if data is None or data.empty:
                    continue

                panic = self.detect_panic(symbol, data)
                if panic["panic_found"]:
                    # Immediately check for flag persistence
                    flag = self.analyze_bear_flag(symbol, data)
                    if flag["signal"]:
                        short_candidates.append({
                            "symbol": symbol,
                            "type": "SHORT",
                            "panic_stats": panic,
                            "pattern": flag,
                            "entry": data['Close'].iloc[-1],
                            "sl": flag["stop_loss"],
                            "target": flag["target"],
                            "confidence": 75 # Standard for verified technical panic
                        })
            except Exception as e:
                logger.error(f"Error scanning shorts for {symbol}: {e}")
        
        return short_candidates
