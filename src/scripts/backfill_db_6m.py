
import os
import sys
import pandas as pd
import numpy as np
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert
from tqdm import tqdm

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.dhan_client import DhanDataClient
from src.db.schema import HistoricalIntradayTick, Base

def backfill_db():
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    db_url = os.getenv("DATABASE_URL")
    
    if not all([cid, token, db_url]):
        logger.error("Required env vars (DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, DATABASE_URL) not found.")
        return

    data_client = DhanDataClient(cid, token)
    engine = create_engine(db_url)
    
    # 1. Get Symbols from sec_list.csv
    try:
        sec_list_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sec_list.csv'))
        df_sec = pd.read_csv(sec_list_path)
        symbols = df_sec['Symbol'].astype(str).str.strip().tolist()
        logger.info(f"Targeting {len(symbols)} symbols for 6-month backfill.")
    except Exception as e:
        logger.error(f"Failed to read sec_list.csv: {e}")
        return

    # 2. Time Chunks (180 days in 30-day blocks)
    to_date = datetime.now()
    from_date = to_date - timedelta(days=180)
    
    chunks = []
    curr = from_date
    while curr < to_date:
        end = min(curr + timedelta(days=30), to_date)
        chunks.append((curr, end))
        curr = end + timedelta(days=1)
    
    logger.info(f"Downloading {len(chunks)} chunks for each symbol.")

    for symbol in tqdm(symbols, desc="Symbols"):
        try:
            sec_id = data_client.get_security_id(symbol)
            if not sec_id:
                logger.debug(f"Symbol {symbol} not found in Dhan Master.")
                continue
            
            # Metadata for Dhan API
            segment = "NSE_EQ"
            instr = "EQUITY"
            if symbol.upper() in ["NIFTY", "BANKNIFTY", "NSEI", "NSEBANK"]:
                segment = "IDX_I"
                instr = "INDEX"
                
            for start, end in chunks:
                from_str = start.strftime("%Y-%m-%d")
                to_str = end.strftime("%Y-%m-%d")
                
                # Fetch
                response = data_client.dhan.intraday_minute_data(
                    str(sec_id), segment, instr, from_str, to_str, interval=1
                )
                
                if response and response.get('status') == 'success' and response.get('data'):
                    df = pd.DataFrame(response['data'])
                    
                    if df.empty:
                        continue

                    # Determine correct time column
                    time_col = None
                    if 'start_Time' in df.columns:
                        time_col = 'start_Time'
                    elif 'timestamp' in df.columns:
                        time_col = 'timestamp'
                    
                    if not time_col:
                        logger.warning(f"No time column found for {symbol}. Columns: {df.columns.tolist()}")
                        continue
                        
                    # Transform to DB schema
                    df['symbol'] = symbol
                    df['timestamp'] = pd.to_datetime(df[time_col], unit='s')
                    # Adjust to IST
                    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
                    
                    # Select and rename columns correctly
                    cols_to_keep = {
                        'symbol': 'symbol',
                        'timestamp': 'timestamp',
                        'open': 'open',
                        'high': 'high',
                        'low': 'low',
                        'close': 'close',
                        'volume': 'volume'
                    }
                    df_to_db = df[list(cols_to_keep.keys())].rename(columns=cols_to_keep)
                    df_to_db['source'] = 'DHAN_HISTORY'
                    
                    # Upsert into MySQL
                    records = df_to_db.to_dict(orient='records')
                    if records:
                        stmt = insert(HistoricalIntradayTick).values(records)
                        # On conflict, update OHLCV
                        upsert_stmt = stmt.on_duplicate_key_update(
                            open=stmt.inserted.open,
                            high=stmt.inserted.high,
                            low=stmt.inserted.low,
                            close=stmt.inserted.close,
                            volume=stmt.inserted.volume,
                            source=stmt.inserted.source
                        )
                        
                        with engine.begin() as conn:
                            conn.execute(upsert_stmt)
                    
                    logger.debug(f"Inserted/Updated {len(df_to_db)} rows for {symbol} ({from_str} to {to_str})")
                
                # Respect rate limit
                time.sleep(1.4) 
                
        except Exception as e:
            logger.error(f"Error backfilling {symbol}: {e}")
            time.sleep(5) # Pause on error

    logger.info("Database backfill complete.")

if __name__ == "__main__":
    backfill_db()
