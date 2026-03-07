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

def calculate_advanced_indicators(df):
    if df is None or df.empty:
        return None
    
    df = df.copy()
    
    # Core
    v = df['Volume'].values
    p = df['Close'].values
    df['VWAP'] = (p * v).cumsum() / v.cumsum()
    df['EMA9'] = df['Close'].ewm(span=9, adjust=False).mean()
    df['EMA21'] = df['Close'].ewm(span=21, adjust=False).mean()
    
    # Squeeze
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['STD20'] = df['Close'].rolling(window=20).std()
    df['BW'] = (df['STD20'] * 4) / df['MA20'].replace(0, np.nan)
    df['BW_SMA'] = df['BW'].rolling(20).mean()
    
    # Volume Trend
    df['AvgVol10'] = df['Volume'].rolling(window=10).mean()
    df['VolRatio'] = df['Volume'] / df['AvgVol10'].replace(0, np.nan)
    
    # Momentum
    df['RSI'] = 100 - (100 / (1 + (df['Close'].diff().apply(lambda x: x if x > 0 else 0).rolling(14).mean() / 
                                   df['Close'].diff().apply(lambda x: abs(x) if x < 0 else 0).rolling(14).mean().replace(0, np.nan))))

    return df

def analyze_precision_timings():
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    data_client = DhanDataClient(cid, token)

    # User Timings (IST) -> Convert to UTC and analyze the 10 mins BEFORE
    # Note: 03:45 UTC is 09:15 IST
    targets = [
        {"symbol": "RICOAUTO", "times": ["09:52"]},
        {"symbol": "NYKAA", "times": ["09:43", "11:11", "14:04"]},
        {"symbol": "VEDL", "times": ["10:50", "14:14"]},
        {"symbol": "SHRIRAMFIN", "times": ["11:11", "13:50"]},
        {"symbol": "HINDALCO", "times": ["10:30"]},
        {"symbol": "KARURVYSYA", "times": ["10:57"]}
    ]

    print("\n" + "="*140)
    print(f"{'Symbol':<12} | {'IST Time':<10} | {'Price':<8} | {'VolRatio':<8} | {'EMA-Diff%':<10} | {'VWAP-Dist%':<10} | {'BW-Squeeze':<10} | {'RSI':<6}")
    print("-" * 140)

    for target in targets:
        symbol = target["symbol"]
        df = data_client.fetch_realtime_data(symbol, period="1d", interval="1m")
        if df is None or df.empty:
            continue
            
        df = calculate_advanced_indicators(df)
        
        for ist_time in target["times"]:
            # Convert IST string to actual index timestamp
            # We assume "today" (last row's date)
            last_date = df.index[-1].strftime('%Y-%m-%d')
            ist_dt = datetime.strptime(f"{last_date} {ist_time}", '%Y-%m-%d %H:%M')
            utc_dt = ist_dt - timedelta(hours=5, minutes=30)
            
            # Find the closest match in index
            try:
                # Find index where time matches
                matches = df.index[df.index.strftime('%H:%M') == utc_dt.strftime('%H:%M')]
                if len(matches) == 0: continue
                idx = matches[-1] # Take the most recent if multiple (though 1m should be unique)
                row = df.loc[idx]
                
                # Pre-metrics calculation (Avg of 3 mins before)
                pre_df = df.truncate(after=idx).tail(5)
                avg_vol_ratio = pre_df['VolRatio'].mean()
                ema_diff = (row['EMA9'] - row['EMA21']) / row['EMA21'] * 100
                vwap_dist = (row['Close'] - row['VWAP']) / row['VWAP'] * 100
                squeeze = row['BW'] < row['BW_SMA']
                
                print(f"{symbol:<12} | {ist_time:<10} | {row['Close']:<8.2f} | {avg_vol_ratio:<8.2f} | {ema_diff:<10.2f}% | {vwap_dist:<10.2f}% | {str(squeeze):<10} | {row['RSI']:<6.1f}")
            except Exception as e:
                logger.error(f"Error analyzing {symbol} at {ist_time}: {e}")

if __name__ == "__main__":
    analyze_precision_timings()
