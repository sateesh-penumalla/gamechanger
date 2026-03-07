
import os
import sys
import time
from datetime import datetime, timedelta
from loguru import logger
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.tick_downloader import TickDownloader

load_dotenv()

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.db.schema import DailyFocus, ORBSignal

# Heavyweights - NSE Equity
HEAVYWEIGHTS = [
    "RELIANCE","SBIN", "TCS", "HDFCBANK", "ICICIBANK", "INFY", 
    "BHARTIARTL","LICI", "HINDUNILVR", "ITC", 
    "LT", "BAJFINANCE", "HCLTECH", "MARUTI", "SUNPHARMA", 
    "ADANIENT", "TITAN", "ONGC", "TATAMOTORS", "NTPC"
]

def get_dynamic_symbols():
    """Fetch active stocks from DB to prioritize."""
    engine = create_engine(os.getenv('DATABASE_URL'))
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        # 1. Stocks from Signal History
        signal_stocks = [s.symbol for s in session.query(ORBSignal).all()]
        # 2. Stocks from Daily Focus
        focus_stocks = [s.symbol for s in session.query(DailyFocus).all()]
        
        # Combine and Clean
        combined = list(set(signal_stocks + focus_stocks))
        clean_symbols = []
        for s in combined:
            # Strip standard prefixes/suffixes
            clean = s.replace(".NS", "").replace("NSE:", "").strip()
            if clean:
                clean_symbols.append(clean)
        
        return clean_symbols
    except Exception as e:
        logger.error(f"Error fetching dynamic symbols: {e}")
        return []
    finally:
        session.close()

def download_orb_set(days=30):
    print(f"--- Starting Dynamic Bulk History Download for {days} days ---")
    
    # 1. Get Focus/Signal Stocks
    dynamic_symbols = get_dynamic_symbols()
    print(f"Found {len(dynamic_symbols)} symbols from DB history.")
    
    # 2. Combine with Heavyweights
    # Use ordered set logic to maintain priority
    final_list = []
    seen = set()
    
    for s in dynamic_symbols + HEAVYWEIGHTS:
        if s not in seen and len(final_list) < 50:
            final_list.append(s)
            seen.add(s)
            
    print(f"Final 50-Stock List: {final_list}")
    
    downloader = TickDownloader()
    
    # We download the specified number of days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    for symbol in final_list:
        try:
            downloader.download_range(symbol, start_date, end_date)
            # 5-second sleep after each symbol as requested
            time.sleep(5)
        except Exception as e:
            logger.error(f"Failed to download {symbol}: {e}")

if __name__ == "__main__":
    # Default to 30 days
    import sys
    days_to_download = 30
    if len(sys.argv) > 1:
        try:
            days_to_download = int(sys.argv[1])
        except ValueError:
            print("Usage: python3 scripts/download_orb_history.py [number_of_days]")
            sys.exit(1)
            
    download_orb_set(days=days_to_download)
