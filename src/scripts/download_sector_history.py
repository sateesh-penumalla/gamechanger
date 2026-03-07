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

def download_sector_history():
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    
    if not cid or not token:
        logger.error("DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN not found in .env")
        return

    data_client = DhanDataClient(cid, token)
    
    # Target directory for history
    sector_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'history_sectors', 'indices'))
    os.makedirs(sector_dir, exist_ok=True)
    
    # Mapping: DB Sector -> Dhan/NSE Index Name
    # We focus on the major tradable indices first as they have the most liquidity and relevance.
    sector_map = {
        'NIFTY_BANK': 'NIFTY BANK',
        'NIFTY_IT': 'NIFTY IT',
        'NIFTY_AUTO': 'NIFTY AUTO',
        'NIFTY_PHARMA': 'NIFTY PHARMA',
        'NIFTY_FMCG': 'NIFTY FMCG',
        'NIFTY_METAL': 'NIFTY METAL',
        'NIFTY_REALTY': 'NIFTY REALTY',
        'NIFTY_ENERGY': 'NIFTY ENERGY',
        'NIFTY_INFRA': 'NIFTY INFRA',
        'NIFTY_FINANCIAL': 'NIFTY FIN SERVICE',
        'NIFTY_CONSUMER_DURABLES': 'NIFTY CONSR DURBL', 
        'NIFTY_OIL_GAS': 'NIFTY OIL AND GAS', 
        'NIFTY_HEALTHCARE': 'NIFTY HEALTHCARE', 
        'NIFTY_PSE': 'NIFTY PSE',
        'NIFTY_MEDIA': 'NIFTY MEDIA',
        # Broad Market indices for general trend
        'NIFTY_50': 'NIFTY 50', # Use NIFTY 50 as proxies for unknown/broad sectors
    }

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
    
    logger.info(f"Generated {len(chunks)} chunks for 6-month download for {len(sector_map)} indices.")
    
    for db_sector, index_name in sector_map.items():
        try:
            # For indices, Dhan usually treats them as equity/index segment
            # We try to resolve the security ID directly using the index name
            sec_id = data_client.get_security_id(index_name)
            
            # Fallback attempts for common variations
            if not sec_id and index_name == 'NIFTY BANK': sec_id = data_client.get_security_id('BANKNIFTY')
            if not sec_id and index_name == 'NIFTY 50': sec_id = data_client.get_security_id('NIFTY')
            if not sec_id and index_name == 'NIFTY FIN SERVICE': sec_id = data_client.get_security_id('FINNIFTY')

            if not sec_id:
                logger.warning(f"Could not find security ID for sector index: {index_name} ({db_sector})")
                continue
                
            all_dfs = []
            for_db_sector = db_sector # Using DB sector name for filename consistency
            
            for start, end in chunks:
                from_str = start.strftime("%Y-%m-%d")
                to_str = end.strftime("%Y-%m-%d")
                
                logger.info(f"Fetching {index_name} ({db_sector}) | {from_str} to {to_str}...")
                
                # Try fetching as Index
                segment = "IDX_I" # Default for indices on Dhan
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
                    time.sleep(0.4) 
                else:
                    logger.debug(f"No data for {index_name} in chunk {from_str} to {to_str}")
            
            if all_dfs:
                df = pd.concat(all_dfs)
                # Cleanup and Format
                df = df.rename(columns={
                    'open': 'Open', 'high': 'High', 'low': 'Low', 
                    'close': 'Close', 'volume': 'Volume', 
                    'start_Time': 'Datetime', 'timestamp': 'Datetime'
                })
                
                # Handle Datetime
                df['Datetime'] = pd.to_datetime(df['Datetime'], unit='s')
                df['Datetime'] = df['Datetime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
                df.set_index('Datetime', inplace=True)
                df = df.sort_index().drop_duplicates()
                
                # Save using DB Sector Name (e.g., NIFTY_BANK_6m_1m.csv)
                filepath = os.path.join(sector_dir, f"{for_db_sector}_6m_1m.csv")
                df.to_csv(filepath)
                logger.success(f"✅ Saved {len(df)} candles for {index_name} -> {for_db_sector}")
            else:
                logger.warning(f"❌ No historical data found for {index_name} in last 6 months via Dhan API.")
                
        except Exception as e:
            logger.error(f"Error processing {db_sector}: {e}")
            
    logger.info("Sector history acquisition complete.")

if __name__ == "__main__":
    download_sector_history()
