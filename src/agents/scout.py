from loguru import logger
import pandas as pd
from typing import List, Dict, Any
from src.db.schema import Ticker
from sqlalchemy.orm import Session

class ScoutAgent:
    def __init__(self, data_client: Any, db_session: Session = None, sector_agent: Any = None):
        self.data_client = data_client
        self.db = db_session
        self.sector_agent = sector_agent
        logger.info("Scout Agent Initialized with Points-Based Logic")

    def check_safety_status(self, symbol: str) -> dict:
        """
        Active Defense: Checks if a held position has breached safety guards.
        Returns {'is_panic': bool, 'reason': str}
        """
        try:
            df = self.client.fetch_realtime_data(symbol, period="1d", interval="1m")
            if df is None or df.empty: return {"is_panic": False, "reason": ""}
            
            # Recalculate basic safety metrics
            curr = df.iloc[-1]
            
            # 1. RSI Check
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss.replace(0, np.nan)
            rsi = 100 - (100 / (1 + rs))
            curr_rsi = rsi.iloc[-1]
            
            if curr_rsi < 35:
                return {"is_panic": True, "reason": f"RSI Collapse ({curr_rsi:.1f})"}
                
            # 2. Stochastics Check
            low_min = df['Low'].rolling(window=14).min()
            high_max = df['High'].rolling(window=14).max()
            k_line = (100 * (df['Close'] - low_min) / (high_max - low_min)).fillna(50)
            curr_k = k_line.iloc[-1]
            
            if curr_k < 5:
                 return {"is_panic": True, "reason": f"Stoch Dead ({curr_k:.1f})"}
            
            # 3. Expansion Check
            vwap = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
            curr_vwap = vwap.iloc[-1]
            dist = (curr['Close'] - curr_vwap) / curr_vwap * 100
            
            if dist < -1.5:
                return {"is_panic": True, "reason": f"Expansion Fail ({dist:.2f}%)"}
                
            return {"is_panic": False, "reason": ""}
            
        except Exception as e:
            logger.error(f"Safety check error for {symbol}: {e}")
            return {"is_panic": False, "reason": "Error"} # Fail safe

    def get_score_history(self, symbol: str, limit: int = 5) -> List[float]:
        """Queries ScanLog for recent confidence scores of a symbol."""
        if not self.db:
            return []
        from src.db.schema import ScanLog
        try:
            logs = self.db.query(ScanLog).filter(ScanLog.symbol == symbol)\
                .order_by(ScanLog.timestamp.desc()).limit(limit).all()
            return [l.scout_score for l in reversed(logs)] # Chronological order
        except Exception as e:
            logger.error(f"Error fetching score history for {symbol}: {e}")
            return []

    def scan_for_setups(self, symbols: List[str]) -> List[Dict]:
        """
        Scans symbols using a Points-Based System + VWAP Slingshot logic.
        """
        interesting_stocks = []
        bulk_data = self.data_client.get_bulk_quotes(symbols)
        
        candidates = []
        for symbol in symbols:
            # Oracle Veto
            if self.db:
                ticker = self.db.query(Ticker).filter(Ticker.symbol == symbol).first()
                if not ticker or ticker.oracle_status != "SNIPER":
                    continue
            
            quote = bulk_data.get(symbol)
            if not quote: continue
                
            day_change = abs(quote.get('net_change_percentage', 0))
            volume = quote.get('volume', 0)
            
            if day_change > 0.01 or volume > 1000:
                candidates.append(symbol)

        for symbol in candidates:
            data = self.data_client.fetch_realtime_data(symbol, period="1d", interval="1m")
            if data is None or len(data) < 20:
                continue
            
            current_candle = data.iloc[-1]
            close_price = current_candle['Close']
            
            # --- 1. SLINGSHOT INDICATORS ---
            df = data.copy()
            # VWAP
            df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
            # Bollinger Squeeze
            df['MA20'] = df['Close'].rolling(window=20).mean()
            df['STD20'] = df['Close'].rolling(window=20).std()
            df['BW'] = (df['STD20'] * 4) / df['MA20']
            df['BW_SMA'] = df['BW'].rolling(20).mean()
            # RSI
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss.replace(0, np.nan)
            df['RSI'] = 100 - (100 / (1 + rs))
            
            curr = df.iloc[-1]
            vwap_dist = (curr['Close'] - curr['VWAP']) / curr['VWAP'] * 100
            is_squeeze = curr['BW'] < curr['BW_SMA']
            rsi_val = curr['RSI']

            # --- 2. POINTS SCORING (Max 100) ---
            score = 0
            signals = []
            
            # --- DEEP DIVE INDICATORS ---
            # 1. MACD (12, 26, 9)
            exp12 = df['Close'].ewm(span=12, adjust=False).mean()
            exp26 = df['Close'].ewm(span=26, adjust=False).mean()
            macd = exp12 - exp26
            signal_line = macd.ewm(span=9, adjust=False).mean()
            macd_hist = macd - signal_line
            curr_hist = macd_hist.iloc[-1]
            prev_hist = macd_hist.iloc[-2]
            
            # 2. Stochastics (14, 3, 3)
            low_min = df['Low'].rolling(window=14).min()
            high_max = df['High'].rolling(window=14).max()
            k_line = (100 * (df['Close'] - low_min) / (high_max - low_min)).fillna(50)
            curr_k = k_line.iloc[-1]
            
            # 3. EMA Slope (Trend Strength)
            ma20 = df['Close'].rolling(window=20).mean()
            ema_slope = ma20.diff().iloc[-1]
            
            # SLINGSHOT LOGIC (Anchor)
            slingshot_pts = 0
            if -1.2 <= vwap_dist <= -0.3:
                slingshot_pts += 30
                signals.append(f"VWAP Slingshot Zone ({vwap_dist:.2f}%)")
            
            if is_squeeze:
                slingshot_pts += 30
                signals.append("Bollinger Squeeze")
                
            # Volume Pulse (Rising over 3 bars)
            avg_min_vol = df['Volume'].mean()
            vol_ratio = curr['Volume'] / avg_min_vol if avg_min_vol > 0 else 0
            recent_vols = df['Volume'].tail(3)
            is_vol_rising = len(recent_vols) >= 3 and recent_vols.iloc[0] < recent_vols.iloc[1] < recent_vols.iloc[2]
            
            if vol_ratio > 1.2 or is_vol_rising:
                slingshot_pts += 20
                signals.append("Volume Pulse Detected")
                
            if 40 <= rsi_val <= 55:
                slingshot_pts += 20
                signals.append(f"RSI Launch Zone ({rsi_val:.1f})")
            
            # Final Score Mix
            score = slingshot_pts
            
            # --- 3. GOLDEN GUARDS (Deep Dive Filters) ---
            # Guard 1: Red Expansion (MACD)
            # If Histogram is Negative AND Lower than previous (Expanding Down), it's a crash
            if curr_hist < 0 and curr_hist < prev_hist:
                score -= 50
                signals.append(f"SAFETY: MACD Red Expansion ({curr_hist:.3f})")

            # Guard 2: Dead Floor (Stochastics)
            if curr_k < 15:
                score -= 30
                signals.append(f"SAFETY: Stoch Dead Floor ({curr_k:.1f})")

            # Guard 3: RSI Safety Floor (Raised to 40)
            if rsi_val < 40:
                score -= 40
                signals.append(f"SAFETY: RSI Collapse ({rsi_val:.1f})")
            
            # Guard 4: Slope of Death
            if ema_slope < -0.1:
                score -= 20
                signals.append(f"SAFETY: Falling Knife Slope ({ema_slope:.2f})")
            
            # Expansion Limit: If price too far below VWAP
            if vwap_dist < -1.5:
                score -= 50
                signals.append("SAFETY: Expansion Limit Broken (Falling Knife)")
            
            # Time Guard
            curr_ist = curr.name + timedelta(hours=5, minutes=30)
            if curr_ist.hour == 9 and curr_ist.minute < 45:
                score -= 20
                signals.append("SAFETY: Opening Range Noise (Pre-09:45)")

            # Fallback to standard trend logic if not in slingshot zone but bullish
            if score < 40 and rsi_val >= 40 and curr_k > 20:
                # Classic Breakout Logic
                if curr['Close'] > curr['VWAP']:
                    score += 20
                    signals.append("Above VWAP Trend")
                if curr['Open'] == curr['Low']:
                    score += 20
                    signals.append("Open=Low Drive")
                if abs(curr['Close'] - df.iloc[0]['Open'])/df.iloc[0]['Open']*100 > 1.0:
                    score += 20
                    signals.append("Strong Momentum")

            res = {
                "symbol": symbol,
                "score": min(score, 100),
                "signals": signals,
                "price": close_price,
                "is_slingshot": 1 if slingshot_pts >= 80 else 0,
                "vwap_dist": vwap_dist,
                "rsi": rsi_val
            }
            interesting_stocks.append(res)
            
            if score >= 70:
                logger.info(f"SLINGSHOT SCOUT: {symbol:<12} | Score: {score} | Zone: {vwap_dist:.2f}% | {', '.join(signals)}")
                
        return interesting_stocks
