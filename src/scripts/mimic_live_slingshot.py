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
    
    # Calculate indicators on the slice
    df = df_slice.copy()
    df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['STD20'] = df['Close'].rolling(window=20).std()
    df['BW'] = (df['STD20'] * 4) / df['MA20'].replace(0, np.nan)
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
    
    score = 0
    signals = []
    
    # Slingshot Components
    if -1.2 <= vwap_dist <= -0.3:
        score += 30
        signals.append(f"VWAP_Zone({vwap_dist:.2f}%)")
    
    if is_squeeze:
        score += 30
        signals.append("Squeeze")
        
    # Volume Pulse
    avg_min_vol = df['Volume'].mean()
    vol_ratio = curr['Volume'] / avg_min_vol if avg_min_vol > 0 else 0
    recent_vols = df['Volume'].tail(3)
    is_vol_rising = False
    if len(recent_vols) >= 3:
        is_vol_rising = recent_vols.iloc[0] < recent_vols.iloc[1] < recent_vols.iloc[2]
    
    if vol_ratio > 1.2 or is_vol_rising:
        score += 20
        signals.append(f"Vol_Pulse({vol_ratio:.1f}x)")
        
    if 40 <= rsi_val <= 55:
        score += 20
        signals.append(f"RSI_Launch({rsi_val:.1f})")
        
    # SAFETY GUARDS (The Collapse Patterns)
    # 1. RSI Floor: If RSI < 35, it's a collapse, not a slingshot
    if rsi_val < 35:
        score -= 40
        signals.append("SAFETY:RSI<35")
    
    # 2. Expansion Limit: Falling Knife if > 1.5% below VWAP
    if vwap_dist < -1.5:
        score -= 50
        signals.append("SAFETY:FallingKnife")
        
    # 3. Time Guard: Avoid noise before 09:45 IST
    curr_ist = df.index[-1] + timedelta(hours=5, minutes=30)
    if curr_ist.hour == 9 and curr_ist.minute < 45:
        score -= 20
        signals.append("SAFETY:MorningNoise")

    metrics = {
        "vwap_dist": vwap_dist,
        "is_squeeze": is_squeeze,
        "rsi": rsi_val,
        "vol_ratio": vol_ratio
    }
    
    return score, signals, metrics

def simulate_live_session(symbol):
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    data_client = DhanDataClient(cid, token)
    
    logger.info(f"Fetching today's data for {symbol}...")
    df = data_client.fetch_realtime_data(symbol, period="1d", interval="1m")
    
    if df is None or df.empty:
        logger.error(f"No data found for {symbol}")
        return

    print("\n" + "="*145)
    print(f"MINUTE-BY-MINUTE SIMULATION: {symbol}")
    print("="*145)
    print(f"{'IST Time':<10} | {'Price':<8} | {'Score':<6} | {'Status':<10} | {'Metrics':<40} | {'Signal'}")
    print("-" * 145)

    in_position = False
    entry_price = 0
    max_score_so_far = 0
    score_history = []
    
    # Start simulation after 20 mins to allow indicators to warm up
    for i in range(20, len(df)):
        # Simulate "Live" slice
        df_slice = df.iloc[:i+1]
        score, signals, metrics = calculate_slingshot_score(df_slice)
        score_history.append(score)
        
        curr_row = df.iloc[i]
        curr_price = curr_row['Close']
        ist_time = (df.index[i] + timedelta(hours=5, minutes=30)).strftime('%H:%M')
        
        # Calculate Momentum (Velocity)
        momentum = 0
        if len(score_history) >= 3:
            momentum = score_history[-1] - score_history[-3]
        
        signal_out = ""
        status = "Watching"
        
        # Entry Logic
        if not in_position and score >= 80:
            in_position = True
            entry_price = curr_price
            status = "ENTRY"
            signal_out = f"🚀 BUY @ {entry_price:.2f}"
            
        # Exit Logic
        if in_position:
            status = "HOLDING"
            profit_pct = (curr_price - entry_price) / entry_price * 100
            if profit_pct >= 1.0:
                in_position = False
                status = "EXIT"
                signal_out = f"💰 PROFIT! +{profit_pct:.2f}% @ {curr_price:.2f}"
            elif profit_pct <= -0.5:
                in_position = False
                status = "EXIT (SL)"
                signal_out = f"📉 STOP LOSS. {profit_pct:.2f}% @ {curr_price:.2f}"

        metric_str = f"VWAP:{metrics['vwap_dist']:.2f}% | Sqz:{str(metrics['is_squeeze'])[0]} | RSI:{metrics['rsi']:.1f} | Mom:{momentum:+d}"
        
        # Print every minute or when a signal occurs
        # To keep output concise, we skip if score is low and no position
        if score >= 40 or in_position or status in ["ENTRY", "EXIT"]:
            print(f"{ist_time:<10} | {curr_price:<8.2f} | {score:<6} | {status:<10} | {metric_str:<40} | {signal_out}")

    print("="*145)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        simulate_live_session(sys.argv[1])
    else:
        # Default to a winning stock from today's analysis
        simulate_live_session("KARURVYSYA")
