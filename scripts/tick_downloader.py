
import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta, time as dt_time
from loguru import logger
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data.truedata_client import TrueDataClient
from src.utils.storage_manager import StorageManager

load_dotenv()

class TickDownloader:
    """
    Downloads historical tick data from TrueData and stores in Parquet.
    This data can be used for high-fidelity order flow simulations.
    """
    
    def __init__(self):
        self.client = TrueDataClient()
        self.storage = StorageManager(base_path="data/historical_ticks")
        
    def download_range(self, symbol: str, start_date: datetime, end_date: datetime) -> bool:
        """
        Downloads ticks day-by-day to avoid memory/request limits.
        """
        if not self.client.access_token:
            if not self.client.login():
                logger.error("Failed to login to TrueData")
                return False

        current_date = start_date
        while current_date <= end_date:
            # 1. Skip if file already exists (saves quota), unless it's today's date
            # Topping up today's data is allowed.
            file_path = self.storage.get_tick_file_path(symbol, current_date)
            if os.path.exists(file_path) and current_date.date() < datetime.now().date():
                logger.debug(f"Skipping {symbol} for {current_date.date()} (File already exists)")
                current_date += timedelta(days=1)
                continue

            # Indian Market Hours: 09:15:00 to 15:30:00
            day_start = datetime.combine(current_date, dt_time(9, 15))
            day_end = datetime.combine(current_date, dt_time(15, 30))
            
            logger.info(f"Downloading Ticks for {symbol} on {current_date.date()}...")
            
            try:
                df = self.client.fetch_ticks_range(symbol, day_start, day_end, bidask=True)
                
                if df is not None:
                    if not df.empty:
                        # Convert index back to column
                        df_reset = df.reset_index()
                        rename_map = {
                            'Datetime': 'timestamp',
                            'Last': 'ltp',
                            'Qty': 'trade_qty',
                            'Bid': 'bid_price',
                            'BidQty': 'bid_qty',
                            'Ask': 'ask_price',
                            'AskQty': 'ask_qty'
                        }
                        df_reset.rename(columns=rename_map, inplace=True)
                        
                        ticks = df_reset.to_dict('records')
                        self.storage.save_ticks_parquet(symbol, ticks, date=current_date)
                        logger.success(f"Successfully archived {len(ticks)} ticks for {symbol}")
                else:
                    logger.warning(f"Failed to fetch data for {symbol} on {current_date.date()} - continuing...")
            except Exception as e:
                logger.error(f"Error fetching ticks for {symbol} on {current_date.date()}: {e}")
            
            # Wait 1 second after each day as requested
            time.sleep(1)
            
            current_date += timedelta(days=1)
        return True

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Download tick data for a symbol.")
    parser.add_argument("--symbol", type=str, default="ONMOBILE", help="The stock symbol to download.")
    parser.add_argument("--days", type=int, default=1, help="Number of days of historical data to download (0 for today only).")
    
    args = parser.parse_args()
    
    downloader = TickDownloader()
    symbol = args.symbol
    end_date = datetime.now()
    start_date = end_date - timedelta(days=args.days)
    
    logger.info(f"Starting download for {symbol} for the last {args.days} days (from {start_date.date()} to {end_date.date()})")
    downloader.download_range(symbol, start_date, end_date)

if __name__ == "__main__":
    main()
