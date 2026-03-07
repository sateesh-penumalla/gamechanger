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

def backfill_missing():
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    
    if not cid or not token:
        logger.error("DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN not found in .env")
        return

    data_client = DhanDataClient(cid, token)
    
    # Target directory for history
    history_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'history'))
    os.makedirs(history_dir, exist_ok=True)
    
    # 1. Get Symbols from sec_list.csv (The 287 stocks)
    try:
        sec_list_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sec_list.csv'))
        df_sec = pd.read_csv(sec_list_path)
        symbols = df_sec['Symbol'].astype(str).str.strip().tolist()
        logger.info(f"Identified {len(symbols)} target symbols from sec_list.csv")
    except Exception as e:
        logger.error(f"Failed to read sec_list.csv: {e}")
        return

    # 2. Identify Missing Symbols (e.g., those without a HINDUNILVR.csv in ANY date folder)
    # Simple check: Does at least one date folder have this symbol?
    existing_symbols = set()
    for d in os.listdir(history_dir):
        dp = os.path.join(history_dir, d)
        if os.path.isdir(dp):
            for f in os.listdir(dp):
                if f.endswith('.csv'):
                    existing_symbols.add(f.replace('.csv', ''))

    missing_symbols = [s for s in symbols if s not in existing_symbols]
    logger.info(f"Found {len(missing_symbols)} symbols currently missing in history date folders.")

    if not missing_symbols:
        logger.info("Nothing to download. All symbols are present.")
        return

    # Dates for last 6 months
    to_date = datetime.now()
    from_date = to_date - timedelta(days=180)
    
    # Dhan typically allows 30 days of 1m data per request.
    chunks = []
    current_start = from_date
    while current_start < to_date:
        current_end = min(current_start + timedelta(days=30), to_date)
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    
    logger.info(f"Generating {len(chunks)} chunks for 6-month download for {len(missing_symbols)} symbols.")
    
    for symbol in missing_symbols:
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
                
                segment = "NSE_EQ"
                instr = "EQUITY"
                # Check for index
                if symbol.upper() in ["NIFTY", "BANKNIFTY", "NSEI", "NSEBANK"]:
                    segment = "IDX_I"
                    instr = "INDEX"
                
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
                    time.sleep(0.4) # Slightly faster rate limit handling
                else:
                    logger.debug(f"No data for {symbol} in chunk {from_str} to {to_str}")
            
            if all_dfs:
                df = pd.concat(all_dfs)
                # Cleanup and Format
                df = df.rename(columns={
                    'open': 'Open', 'high': 'High', 'low': 'Low', 
                    'close': 'Close', 'volume': 'Volume', 
                    'start_Time': 'Datetime', 'timestamp': 'Datetime'
                })
                
                # Handle Datetime
                # Dhan returns epoch in seconds (start_Time)
                df['Datetime'] = pd.to_datetime(df['Datetime'], unit='s')
                df['Datetime'] = df['Datetime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
                df.set_index('Datetime', inplace=True)
                df = df.sort_index().drop_duplicates()
                
                filepath = os.path.join(history_dir, f"{symbol}_6m_1m.csv")
                df.to_csv(filepath)
                logger.success(f"✅ Saved {len(df)} candles for {symbol}")
            else:
                logger.warning(f"❌ No historical data found for {symbol} in last 6 months via Dhan API.")
                
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            
    logger.info("Backfill acquisition complete.")

if __name__ == "__main__":
    backfill_missing()
