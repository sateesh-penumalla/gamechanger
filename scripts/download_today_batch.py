import os
import sys
import argparse
from datetime import datetime, timedelta
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()

from scripts.tick_downloader import TickDownloader
from src.db.schema import Ticker
from src.utils.storage_manager import StorageManager

def get_symbols_from_db():
    """Fetches all symbols from the tickers table."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not set in environment.")
        return []
    
    try:
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        tickers = session.query(Ticker).all()
        symbols = [t.symbol.replace(".NS", "").replace("NSE:", "").strip() for t in tickers]
        session.close()
        return sorted(list(set(symbols)))
    except Exception as e:
        logger.error(f"Error fetching symbols from DB: {e}")
        return []

def main():
    parser = argparse.ArgumentParser(description="Batch download today's tick data for symbols in DB.")
    parser.add_argument("--days", type=int, default=0, help="Number of days to download (default 0 for today only).")
    parser.add_argument("--base-path", type=str, default="data/historical_ticks", help="Path where symbols are stored.")
    parser.add_argument("--exchange", type=str, default="NSE", choices=["NSE", "BSE"], help="Exchange to download from (NSE or BSE).")
    
    args = parser.parse_args()
    
    # 1. Fetch symbols from DB instead of local directory
    base_symbols = get_symbols_from_db()
    if not base_symbols:
        logger.error("No symbols found in database to download.")
        return

    # Map symbols based on exchange
    if args.exchange == "BSE":
        download_list = [f"{s}_BSE" if not s.endswith("_BSE") else s for s in base_symbols]
    else:
        # Defaults to NSE symbols
        download_list = [s.replace("_BSE", "") for s in base_symbols]
    
    download_list = sorted(list(set(download_list)))

    logger.info(f"Found {len(download_list)} symbols in DB for {args.exchange}. Starting batch download...")
    
    downloader = TickDownloader()
    storage = StorageManager(base_path=args.base_path)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=args.days)

    for symbol in download_list:
        try:
            # 2. Skip if file already exists for today (or the requested range)
            # TickDownloader.download_range already has some skip logic, 
            # but we can also double check here to provide clearer logs.
            
            # For the range case, we check only the target date (today if days=0)
            target_date = end_date
            file_path = storage.get_tick_file_path(symbol, target_date)
            
            if os.path.exists(file_path):
                logger.info(f"Skipping {symbol} - File already exists for {target_date.date()}")
                continue
                
            logger.info(f"Processing {symbol}...")
            downloader.download_range(symbol, start_date, end_date)
        except Exception as e:
            logger.error(f"Failed to download {symbol}: {e}")

    logger.success("Batch download completed.")

if __name__ == "__main__":
    main()
