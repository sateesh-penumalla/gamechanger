
import os
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"
EXPORT_ROOT = "/Users/sateeshbabu/fractionalcto/GameChanger copy/data/historical_ticks"
engine = create_engine(DATABASE_URL)

def export_symbol_date(symbol, date_str):
    try:
        # Create folder for symbol
        symbol_path = os.path.join(EXPORT_ROOT, symbol)
        os.makedirs(symbol_path, exist_ok=True)
        
        # File path
        file_path = os.path.join(symbol_path, f"{date_str}.csv")
        
        # Fetch data
        query = f"""
            SELECT * FROM historical_intraday_ticks 
            WHERE symbol = '{symbol}' 
            AND DATE(timestamp) = '{date_str}'
            ORDER BY timestamp
        """
        df = pd.read_sql(query, engine)
        
        if not df.empty:
            df.to_csv(file_path, index=False)
            return True
        return False
    except Exception as e:
        print(f"Error exporting {symbol} for {date_str}: {e}")
        return False

def export_symbol(symbol, dates):
    try:
        symbol_path = os.path.join(EXPORT_ROOT, symbol)
        os.makedirs(symbol_path, exist_ok=True)
        
        # Fetch all data for this symbol once
        query = f"SELECT * FROM historical_intraday_ticks WHERE symbol = '{symbol}' ORDER BY timestamp"
        df_all = pd.read_sql(query, engine)
        
        if df_all.empty: return 0
        
        df_all['date'] = pd.to_datetime(df_all['timestamp']).dt.date
        
        count = 0
        for date_val, group in df_all.groupby('date'):
            file_path = os.path.join(symbol_path, f"{date_val}.csv")
            group.drop(columns=['date']).to_csv(file_path, index=False)
            count += 1
        return count
    except Exception as e:
        print(f"Error exporting {symbol}: {e}")
        return 0

def main():
    print("🚀 Initializing Optimized Historical Tick Export...")
    
    # 1. Get all symbols
    query_symbols = "SELECT symbol FROM historical_intraday_ticks GROUP BY symbol"
    symbols = pd.read_sql(query_symbols, engine)['symbol'].tolist()
    
    print(f"📦 Found {len(symbols)} symbols to process.")
    
    # 2. Parallel Export by Symbol
    total_files = 0
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(export_symbol, s, []): s for s in symbols}
        for future in tqdm(as_completed(futures), total=len(symbols)):
            total_files += future.result()
            
    print(f"\n✅ Export Complete!")
    print(f"📂 Total Files Created: {total_files}")
    print(f"📍 Location: {EXPORT_ROOT}")

if __name__ == "__main__":
    main()
