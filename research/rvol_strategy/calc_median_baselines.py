import pandas as pd
from sqlalchemy import create_engine, text
import json
import os

db_url = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"
engine = create_engine(db_url)

# Strategy: 
# 1. Fetch 1-min data (volume) for last 15 days
# 2. Filter out open (Before 9:30) and close (After 15:15) to get "Normal Business"
# 3. Calculate rolling 5m volume and take the MEDIAN per symbol.

query = """
SELECT 
    symbol, 
    timestamp,
    volume
FROM intraday_ticks
WHERE timestamp >= DATE_SUB('2026-03-12', INTERVAL 15 DAY)
  AND timestamp < '2026-03-12'
  AND TIME(timestamp) >= '09:30:00'
  AND TIME(timestamp) <= '15:15:00'
"""

print("Fetching 1-minute data (regime hours) from database...")
try:
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    if df.empty:
        print("No data found in intraday_ticks for the specified range.")
        exit()

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Calculate 5-minute rolling sums per symbol per day
    # Note: Using resample or rolling on a per-symbol basis
    median_profiles = {}
    
    print("Calculating Median 5m volumes...")
    for symbol, group in df.groupby('symbol'):
        # For each symbol, resample to 5m to get typical window sizes
        # We group by date as well so we don't bridge overnight gaps in a 5m sum
        group = group.sort_values('timestamp')
        group['date'] = group['timestamp'].dt.date
        
        day_vols = []
        for date, day_group in group.groupby('date'):
            day_group = day_group.set_index('timestamp')
            # 5-min volume windows
            v5 = day_group['volume'].resample('5min').sum()
            day_vols.extend(v5.tolist())
        
        # Calculate median of all valid 5m windows for this stock
        if day_vols:
            median_profiles[symbol] = float(pd.Series(day_vols).median())

    # Save to file
    with open("/tmp/vol_median_5m.json", "w") as f:
        json.dump(median_profiles, f)
        
    print(f"Median profiles generated for {len(median_profiles)} stocks.")
    
    # Quick Comparison for RPOWER
    if 'RPOWER' in median_profiles:
        median_val = median_profiles['RPOWER']
        # Contrast with previous Mean ADV which was 488,059
        print(f"\n--- RPOWER Comparison ---")
        print(f"Previous Mean 5m: 488,059")
        print(f"New Median 5m:   {median_val:,.0f}")
        print(f"Entry Threshold (500%): {median_val * 5:,.0f}")

except Exception as e:
    print(f"Error: {e}")
