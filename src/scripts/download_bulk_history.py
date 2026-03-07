import os
import sys
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.dhan_client import DhanDataClient

def download_history():
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    
    if not cid or not token:
        logger.error("DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN not found in .env")
        return

    data_client = DhanDataClient(cid, token)
    
    # Source directory to get symbols
    cache_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'sniper_cache'))
    # Target directory for history
    history_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'history'))
    os.makedirs(history_dir, exist_ok=True)
    
    # Get symbols from filenames (e.g., RELIANCE_1d_1m.csv)
    if not os.path.exists(cache_dir):
        logger.error(f"Cache directory not found: {cache_dir}")
        return
        
    symbols = [f.split('_')[0] for f in os.listdir(cache_dir) if f.endswith('.csv')]
    logger.info(f"Found {len(symbols)} symbols in cache. Starting 2-month download...")
    
    # Dates for last 6 months (approx 180 days)
    to_date = datetime.now()
    from_date = to_date - timedelta(days=180)
    
    # Dhan typically allows 30 days of 1m data per request. 
    # Generating 30-day chunks for the 180-day window
    chunks = []
    current_start = from_date
    while current_start < to_date:
        current_end = min(current_start + timedelta(days=30), to_date)
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    
    logger.info(f"Generated {len(chunks)} chunks for 6-month download.")
    
    for symbol in symbols:
        try:
            sec_id = data_client.get_security_id(symbol)
            if not sec_id:
                logger.warning(f"Could not find security ID for {symbol}")
                continue
                
            all_dfs = []
            for start, end in chunks:
                from_str = start.strftime("%Y-%m-%d")
                to_str = end.strftime("%Y-%m-%d")
                
                logger.info(f"Fetching {symbol} | {from_str} to {to_str}...")
                
                # Determine Segment/Instr (Same as DhanDataClient)
                segment = "NSE_EQ"
                instr = "EQUITY"
                if symbol.upper() in ["NIFTY", "BANKNIFTY", "NSEI", "NSEBANK"] or (sec_id < 1000):
                    segment = "IDX_I"
                    instr = "INDEX"
                
                # Minute data typically has limits. We'll use intraday_minute_data
                # Note: DhanDataClient.fetch_realtime_data handles 7 days normally.
                # We'll call the underlying dhan library directly if needed, 
                # but let's try to adapt the client logic.
                
                # Fetching 1m data
                response = data_client.dhan.intraday_minute_data(
                    str(sec_id),
                    segment,
                    instr,
                    from_str,
                    to_str,
                    interval=1
                )
                
                if response and response.get('status') == 'success' and response.get('data'):
                    df_chunk = pd.DataFrame(response['data'])
                    all_dfs.append(df_chunk)
                    # Respect rate limits
                    time.sleep(0.5)
                else:
                    logger.warning(f"No data for {symbol} in chunk {from_str} to {to_str}")
            
            if all_dfs:
                df = pd.concat(all_dfs)
                # Cleanup and Format
                df = df.rename(columns={
                    'open': 'Open', 'high': 'High', 'low': 'Low', 
                    'close': 'Close', 'volume': 'Volume', 
                    'start_Time': 'Datetime', 'timestamp': 'Datetime'
                })
                
                if 'Datetime' not in df.columns:
                    logger.warning(f"Could not find Datetime column in {symbol} response. Columns: {df.columns.tolist()}")
                    continue

                # Handle Datetime
                df['Datetime'] = pd.to_datetime(df['Datetime'], unit='s')
                df['Datetime'] = df['Datetime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
                df.set_index('Datetime', inplace=True)
                df = df.sort_index().drop_duplicates()
                
                filepath = os.path.join(history_dir, f"{symbol}_6m_1m.csv")
                df.to_csv(filepath)
                logger.info(f"✅ Successfully saved {len(df)} candles for {symbol}")
            else:
                logger.warning(f"❌ Failed to fetch any data for {symbol}")
                
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            
    logger.info("Bulk download complete.")

if __name__ == "__main__":
    download_history()
