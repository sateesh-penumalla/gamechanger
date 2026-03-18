
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import json
import os
from tqdm import tqdm

DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"
engine = create_engine(DATABASE_URL)

def update_magic_json():
    # 1. Load existing magic_volume.json
    json_path = 'magic_volume.json'
    if not os.path.exists(json_path):
        print("magic_volume.json not found.")
        return
        
    with open(json_path, 'r') as f:
        magic_data = json.load(f)
    
    symbols = list(magic_data.keys())
    print(f"🚀 Adding High Conviction (99.9th Percentile) field for {len(symbols)} symbols...")
    
    for symbol in tqdm(symbols):
        try:
            # Get 99.9th percentile volume for this symbol
            query = f"SELECT volume FROM historical_intraday_ticks WHERE symbol = '{symbol}'"
            df = pd.read_sql(query, engine)
            if not df.empty:
                # 99.9th percentile is the one that only happens 0.1% of the time (Extreme institutional)
                perfection_vol = int(np.percentile(df['volume'], 99.9))
                
                # Add the new field
                magic_data[symbol]['high_conviction_magic'] = perfection_vol
        except Exception as e:
            print(f"Error on {symbol}: {e}")

    # 2. Save back to json
    with open(json_path, 'w') as f:
        json.dump(magic_data, f, indent=4)
        
    print(f"\n✅ Updated magic_volume.json with 'high_conviction_magic' field.")

if __name__ == "__main__":
    update_magic_json()
