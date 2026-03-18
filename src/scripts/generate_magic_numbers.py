
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm
import json
import os

DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"
engine = create_engine(DATABASE_URL)

def calculate_all_magic_numbers():
    # 1. Get all symbols from the historical table
    query_symbols = "SELECT symbol, COUNT(*) as count FROM historical_intraday_ticks GROUP BY symbol"
    df_symbols = pd.read_sql(query_symbols, engine)
    symbols = df_symbols['symbol'].tolist()
    
    magic_volumes = {}
    threshold = 0.03 # 3% Target Move
    
    print(f"🚀 Calculating Magic Numbers for {len(symbols)} tickers...")
    
    for symbol in tqdm(symbols):
        try:
            # Query the last 6 months of data for the symbol
            query = f"SELECT `close`, volume FROM historical_intraday_ticks WHERE symbol = '{symbol}' ORDER BY timestamp ASC"
            df = pd.read_sql(query, engine)
            
            if len(df) < 1000:
                # Not enough data to be statistically significant
                continue
                
            closes = df['close'].values
            volumes = df['volume'].values
            
            # Identify 3% moves and capture peak volume during those moves
            move_peak_volumes = []
            i = 0
            n = len(df)
            while i < n - 60: # Look at windows up to 60 mins (1 hour)
                start_p = closes[i]
                found = False
                # Look ahead for a 3% move
                for j in range(i + 1, min(i + 61, n)):
                    change = (closes[j] - start_p) / start_p
                    if abs(change) >= threshold:
                        # Found a 3% move, find the max volume candle in this move
                        peak_v = np.max(volumes[i:j+1])
                        move_peak_volumes.append(float(peak_v))
                        i = j # Move pointer to end of move
                        found = True
                        break
                if not found:
                    i += 1
            
            if move_peak_volumes:
                # The Magic Number is the 25th percentile of these peak volumes
                # This represents a "minimum threshold of conviction"
                magic_val = int(np.percentile(move_peak_volumes, 25))
                
                # Guard against extremely low numbers (bad data)
                if magic_val > 100:
                    magic_volumes[symbol] = {
                        "magic_number": magic_val,
                        "sample_count": len(move_peak_volumes),
                        "avg_volume": int(np.mean(volumes))
                    }
        except Exception as e:
            # Print error but continue with other symbols
            print(f"Error processing {symbol}: {e}")

    # Save to JSON
    with open('magic_volume.json', 'w') as f:
        json.dump(magic_volumes, f, indent=4)
    
    print(f"\n✅ Created magic_volume.json with {len(magic_volumes)} symbols.")

if __name__ == "__main__":
    calculate_all_magic_numbers()
