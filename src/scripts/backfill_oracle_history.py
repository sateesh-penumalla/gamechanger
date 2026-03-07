import os
import pandas as pd
import pandas as pd
import numpy as np
from src.data.yfinance_client import YahooFinanceData
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger
from datetime import timedelta
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
engine = create_engine(DB_URL)

def clean_val(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return float(val)

def fetch_weekly_data(symbol: str):
    """Fetches 2 years of weekly data from Dhan (via Yahoo Wrapper)."""
    try:
        data_client = YahooFinanceData()
        data = data_client.fetch_realtime_data(symbol, period="2y", interval="1wk")
        if data is None or data.empty:
            return None
            
        # Cleanup column names
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        
        # Calculate Indicators manually
        data['SMA_20'] = data['Close'].rolling(window=20).mean()
        
        delta = data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss.replace(0, np.nan)
        data['RSI_14'] = 100 - (100 / (1 + rs))
            
        return data[['SMA_20', 'RSI_14', 'Close']]
    except Exception as e:
        logger.error(f"Error for {symbol}: {e}")
        return None

def backfill_oracle():
    with engine.connect() as conn:
        symbols = [r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM history_testing"))]
    
    logger.info(f"Backfilling Oracle data for {len(symbols)} symbols in parallel...")
    
    def worker(symbol):
        try:
            weekly_df = fetch_weekly_data(symbol)
            if weekly_df is None or weekly_df.empty:
                return symbol, None
                
            with engine.connect() as conn:
                dates = [r[0] for r in conn.execute(text("SELECT trade_date FROM history_testing WHERE symbol = :s"), {"s": symbol})]
            
            updates = []
            for d in dates:
                past = weekly_df[weekly_df.index.date < d]
                if past.empty: continue
                    
                last = past.iloc[-1]
                sma_val = last.iloc[0]
                rsi_val = last.iloc[1]
                price_val = last.iloc[2]
                
                is_bullish = False
                if pd.notnull(price_val) and pd.notnull(sma_val) and pd.notnull(rsi_val):
                    is_bullish = float(price_val) > float(sma_val) and float(rsi_val) > 60
                
                status = "SNIPER" if is_bullish else "FILTERED"
                updates.append({
                    "symbol": symbol,
                    "date": d,
                    "rsi": clean_val(rsi_val),
                    "sma": clean_val(sma_val),
                    "status": status
                })
            return symbol, updates
        except Exception as e:
            logger.error(f"Worker failed for {symbol}: {e}")
            return symbol, None

    # Parallelize downloads
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(worker, s): s for s in symbols}
        
        for future in as_completed(futures):
            symbol, results = future.result()
            if results:
                # Batch update for the symbol
                with engine.connect() as conn:
                    for upd in results:
                        conn.execute(text("""
                            UPDATE history_testing 
                            SET weekly_rsi = :rsi, weekly_sma = :sma, oracle_status = :status
                            WHERE symbol = :symbol AND trade_date = :date
                        """), upd)
                    conn.commit()
                logger.info(f"✅ Updated {symbol} ({len(results)} dates)")
            else:
                logger.warning(f"❌ Skipping {symbol} (No data)")

if __name__ == "__main__":
    backfill_oracle()
