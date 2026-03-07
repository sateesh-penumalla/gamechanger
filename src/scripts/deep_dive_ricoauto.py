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
from src.agents.scout import ScoutAgent

def calculate_vwap(df):
    v = df['Volume'].values
    p = df['Close'].values
    return df.assign(VWAP=(p * v).cumsum() / v.cumsum())

def deep_dive_ricoauto():
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    
    if not (cid and token):
        logger.error("Dhan credentials missing!")
        return

    data_client = DhanDataClient(cid, token)
    symbol = "RICOAUTO"
    
    logger.info(f"Fetching deep dive data for {symbol}...")
    
    # 1. Fetch raw data (Check last 2 days to ensure we have enough context for indicators)
    raw_data = data_client.fetch_realtime_data(symbol, period="1d", interval="1m")
    
    if raw_data is None or raw_data.empty:
        logger.error("No data fetched.")
        return
    
    df = raw_data.copy()
    
    # 2. Indicators
    # VWAP
    df = calculate_vwap(df)
    
    # EMAs
    df['EMA9'] = df['Close'].ewm(span=9, adjust=False).mean()
    df['EMA21'] = df['Close'].ewm(span=21, adjust=False).mean()
    
    # MA 20
    df['MA20'] = df['Close'].rolling(window=20).mean()
    
    # Volatility Squeeze (Bollinger Width)
    df['STD20'] = df['Close'].rolling(window=20).std()
    df['BW'] = (df['STD20'] * 4) / df['MA20']
    
    # Volume Average (10 min rolling)
    df['AvgVol10'] = df['Volume'].rolling(window=10).mean()
    df['VolRatio'] = df['Volume'] / df['AvgVol10']
    
    # Relative Momentum (vs Open)
    day_open = df.iloc[0]['Open']
    df['DayChange'] = (df['Close'] - day_open) / day_open * 100

    # 3. Points Simulation (Simplified Scout Agent)
    print("\n" + "="*95)
    print(f"POINTS SIMULATION: {symbol}")
    print("="*95)
    print(f"{'Time':<20} {'Close':<8} {'Vol':<8} {'Ratio':<6} {'V-Dist':<8} {'EMA-S':<8} {'Score':<6}")
    print("-" * 95)
    
    for i in range(21, len(df)):
        row = df.iloc[i]
        prev_row = df.iloc[i-1]
        
        score = 0
        signals = []
        
        # Volume
        if row['VolRatio'] > 3.0: score += 20
        elif row['VolRatio'] > 1.3: score += 10
        
        # VWAP Distance
        v_dist = (row['Close'] - row['VWAP']) / row['VWAP'] * 100
        if row['Close'] > row['VWAP']: score += 10
        
        # EMA Cross (9 over 21)
        ema_sig = "None"
        if row['EMA9'] > row['EMA21']:
            score += 10
            if prev_row['EMA9'] <= prev_row['EMA21']:
                score += 10 # Crossover bonus
                ema_sig = "CROSS"
            else:
                ema_sig = "ABOVE"

        # Momentum
        if 0.3 <= abs(row['DayChange']) <= 1.0: score += 20
        elif abs(row['DayChange']) > 1.0: score += 10

        # Output range around 9:55 IST (04:25 UTC)
        time_str = df.index[i].strftime('%H:%M')
        if "03:5" in time_str or "04:" in time_str:
            print(f"{df.index[i]} | {row['Close']:<8.2f} | {int(row['Volume']):<8} | {row['VolRatio']:<6.1f} | {v_dist:<8.2f}% | {ema_sig:<8} | {score:<6}")

if __name__ == "__main__":
    deep_dive_ricoauto()
