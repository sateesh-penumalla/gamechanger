import os
import sys
import pandas as pd
import numpy as np
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.dhan_client import DhanDataClient

def calculate_slingshot_score(df_slice):
    if len(df_slice) < 20:
        return 0, [], {}
    
    df = df_slice.copy()
    df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['STD20'] = df['Close'].rolling(window=20).std()
    df['BW'] = (df['STD20'] * 4) / df['MA20'].replace(0, np.nan)
    df['BW_SMA'] = df['BW'].rolling(20).mean()
    
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI_Slope'] = df['RSI'] - df['RSI'].shift(1)
    
    # --- CORE METRICS ---
    curr = df.iloc[-1]
    vwap_dist = (curr['Close'] - curr['VWAP']) / curr['VWAP'] * 100
    is_squeeze = curr['BW'] < curr['BW_SMA']
    rsi_val = curr['RSI']
    
    # --- DEEP DIVE INDICATORS (Scout Fusion) ---
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
    
    # 3. EMA Slope
    ma20 = df['Close'].rolling(window=20).mean()
    ema_slope = ma20.diff().iloc[-1]
    
    score = 0
    signals = []
    
    # --- SLINGSHOT ANCHORS ---
    if -1.2 <= vwap_dist <= -0.3:
        score += 30
    if is_squeeze:
        score += 30
        
    avg_min_vol = df['Volume'].mean()
    vol_ratio = curr['Volume'] / avg_min_vol if avg_min_vol > 0 else 0
    if vol_ratio > 1.2:
        score += 20
        
    if 40 <= rsi_val <= 55:
        score += 20
        
    # --- GOLDEN GUARDS (Safety Vetoes) ---
    safety_veto = False
    
    # Guard 1: Red Expansion (MACD Histogram widening down)
    if curr_hist < 0 and curr_hist < prev_hist:
        score -= 50
        signals.append(f"VETO: MACD Red Expansion ({curr_hist:.3f})")
        safety_veto = True

    # Guard 2: Dead Floor (Stochastics Pinned)
    if curr_k < 15:
        score -= 30
        signals.append(f"VETO: Stoch Dead Floor ({curr_k:.1f})")
        safety_veto = True

    # Guard 3: Slope of Death (Falling Knife)
    if ema_slope < -0.1:
        score -= 20
        signals.append(f"VETO: Knife Slope ({ema_slope:.2f})")
        safety_veto = True

    # Baseline Grounds
    if rsi_val < 35: score -= 40
    if vwap_dist < -1.5: score -= 50
        
    curr_ist = df.index[-1]
    if curr_ist.hour == 9 and curr_ist.minute < 45:
        score -= 20

    metrics = {
        "vwap_dist": vwap_dist,
        "is_squeeze": is_squeeze,
        "rsi": rsi_val,
        "rsi_slope": curr['RSI_Slope'],
        "vol_ratio": vol_ratio,
        "macd_hist": curr_hist,
        "stoch_k": curr_k,
        "ema_slope": ema_slope,
        "safety_veto": safety_veto
    }
    
    return score, signals, metrics

def simulate_apex_session(symbol):
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    data_client = DhanDataClient(cid, token)
    
    df = data_client.fetch_realtime_data(symbol, period="1d", interval="1m")
    if df is None or df.empty: return

    print("\n" + "="*145)
    print(f"APEX SNIPER SIMULATION: {symbol} (Trailing SL + Time Decay)")
    print("="*145)
    print(f"{'IST Time':<10} | {'Price':<8} | {'Score':<6} | {'Status':<10} | {'PnL%':<7} | {'TSL_Lvl':<8} | {'Signal'}")
    print("-" * 145)

    in_position = False
    entry_price = 0
    entry_time = None
    peak_price = 0
    tsl_price = 0
    
    last_exit_time = None
    
    for i in range(20, len(df)):
        df_slice = df.iloc[:i+1]
        score, _, metrics = calculate_slingshot_score(df_slice)
        
        curr_row = df.iloc[i]
        curr_price = curr_row['Close']
        ist_time = df.index[i]
        ist_time_str = ist_time.strftime('%H:%M')
        
        signal_out = ""
        status = "Watching"
        pnl_str = "-"
        tsl_str = "-"
        
        # Check Cool-down (15 mins)
        in_cooldown = False
        if last_exit_time and (ist_time - last_exit_time).total_seconds() / 60 < 15:
            in_cooldown = True
            status = "COOLDOWN"

        # Entry Logic
        if not in_position and not in_cooldown and score >= 80:
            in_position = True
            entry_price = curr_price
            entry_time = ist_time
            peak_price = curr_price
            tsl_price = curr_price * 0.995 # Initial hard stop at -0.5%
            status = "ENTRY"
            reached_target_1 = False
            reached_breakout = False
            peak_momentum = 0
            signal_out = f"🚀 BUY @ {entry_price:.2f}"
            
        elif in_position:
            status = "HOLDING"
            profit_pct = (curr_price - entry_price) / entry_price * 100
            pnl_str = f"{profit_pct:+.2f}%"
            
            # --- TIERED PULSE TRAILING ---
            if not reached_target_1 and profit_pct >= 1.0:
                # Steady Hand Check
                rsi_v = metrics['rsi_slope']
                vol_r = metrics['vol_ratio']
                if rsi_v < 5.5 and vol_r < 2.5:
                    reached_target_1 = True
                    status = "STEADY"
                else:
                    in_position = False
                    status = "EXIT (TP)"
                    last_exit_time = ist_time
                    signal_out = f"💰 EXHAUSTION TP! {profit_pct:+.2f}% @ {curr_price:.2f}"
            
            # Trailing Logic (only if steady hand)
            if in_position and reached_target_1:
                if curr_price > peak_price:
                    peak_price = curr_price
                
                # Momentum Memory: Track peak slope during runaway
                curr_v = metrics['rsi_slope']
                if curr_v > peak_momentum:
                    peak_momentum = curr_v
                
                # Gap Selection: 0.5% for core (1-2%), 1.0% for breakout (>2%) with Memory
                if profit_pct > 2.0 and peak_momentum > 4.0:
                    reached_breakout = True
                
                if reached_breakout:
                    gap_pct = 1.0 # Breather room for verified trend runners
                    mode_str = "🚀 BREAKOUT"
                else:
                    gap_pct = 0.5 # Tight safety for steady climbers
                    mode_str = "📈 TRAILING"
                
                new_tsl = peak_price * (1 - gap_pct/100)
                if new_tsl > tsl_price:
                    tsl_price = new_tsl
                    status = mode_str
                    signal_out = f"✨ TIERED TSL | Gap {gap_pct}% | TSL: {tsl_price:.2f} | PeakV: {peak_momentum:.1f}"
            
            tsl_str = f"{tsl_price:.2f}"
            
            # EXIT CONDITIONS
            if in_position and curr_price <= tsl_price:
                in_position = False
                status = "EXIT (ST)"
                last_exit_time = ist_time
                exit_type = "💰 APEX PROFIT" if profit_pct > 0 else "📉 STOP LOSS"
                signal_out = f"{exit_type}! {profit_pct:+.2f}% @ {curr_price:.2f}"

        if score >= 40 or in_position or status.startswith("EXIT") or status == "ENTRY":
            print(f"{ist_time_str:<10} | {curr_price:<8.2f} | {score:<6} | {status:<10} | {pnl_str:<7} | {tsl_str:<8} | {signal_out}")

    print("="*145)

if __name__ == "__main__":
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else "PFOCUS"
    simulate_apex_session(symbol)
