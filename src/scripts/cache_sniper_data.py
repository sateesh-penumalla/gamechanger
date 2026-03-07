import os
import sys
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.dhan_client import DhanDataClient
from src.db.schema import Ticker
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def cache_data():
    load_dotenv()
    cid = os.getenv("DHAN_CLIENT_ID")
    token = os.getenv("DHAN_ACCESS_TOKEN")
    data_client = DhanDataClient(cid, token)
    
    # Setup DB
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Create cache directory
    cache_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'sniper_cache'))
    os.makedirs(cache_dir, exist_ok=True)
    
    # Get Sniper Stocks
    snipers = session.query(Ticker).filter(Ticker.oracle_status == 'SNIPER').all()
    symbols = [s.symbol for s in snipers]
    session.close()
    
    logger.info(f"Caching data for {len(symbols)} sniper stocks...")
    
    for symbol in symbols:
        try:
            # Fetch for today (1d, 1m)
            df = data_client.fetch_realtime_data(symbol, period="1d", interval="1m")
            if df is not None and not df.empty:
                start_ist = df.index[0].strftime('%H:%M')
                end_ist = df.index[-1].strftime('%H:%M')
                date_str = df.index[0].strftime('%Y-%m-%d')
                
                filepath = os.path.join(cache_dir, f"{symbol}_1d_1m.csv")
                df.to_csv(filepath)
                logger.info(f"Cached {symbol} ({date_str}) | Session: {start_ist} - {end_ist} IST")
            else:
                logger.warning(f"No data for {symbol}")
        except Exception as e:
            logger.error(f"Error caching {symbol}: {e}")

if __name__ == "__main__":
    cache_data()
