import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from loguru import logger
from typing import List, Dict, Any

class StorageManager:
    """
    Handles file-based storage for high-frequency tick data using Parquet.
    """
    
    def __init__(self, base_path: str = "data/ticks"):
        self.base_path = base_path
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path, exist_ok=True)
            logger.info(f"Created base directory for ticks: {self.base_path}")

    def save_ticks_parquet(self, symbol: str, ticks: List[Dict[str, Any]], date: datetime = None):
        """
        Appends a batch of ticks to a daily Parquet dataset using O(1) batch writing.
        Instead of rewriting the whole file, it writes a new unique shard in a daily folder.
        """
        if not ticks:
            return
        
        target_date = date or datetime.now()

        try:
            # 1. Prepare directory structure
            date_str = target_date.strftime("%Y-%m-%d")
            symbol_dir = os.path.join(self.base_path, symbol)
            
            # Use a folder named .parquet to trick pd.read_parquet into reading it as a unified dataset
            day_dir_path = os.path.join(symbol_dir, f"{date_str}.parquet")
            
            # Handle migration from legacy single-file to directory-partitioned
            if os.path.exists(day_dir_path) and os.path.isfile(day_dir_path):
                temp_legacy = day_dir_path + ".legacy"
                os.rename(day_dir_path, temp_legacy)
                os.makedirs(day_dir_path, exist_ok=True)
                os.rename(temp_legacy, os.path.join(day_dir_path, "legacy_migration.parquet"))
            else:
                os.makedirs(day_dir_path, exist_ok=True)
            
            # 2. Generate unique batch filename (Timestamp allows instant append without conflict)
            batch_id = int(datetime.now().timestamp() * 1000)
            batch_file = os.path.join(day_dir_path, f"batch_{batch_id}.parquet")
            
            # 3. Convert to DataFrame and enforce types
            df = pd.DataFrame(ticks)
            type_map = {
                'ltp': 'float64',
                'imbalance': 'float64',
                'bid_qty': 'float64',
                'ask_qty': 'float64',
                'buy_vol': 'float64',
                'sell_vol': 'float64',
                'total_bid_qty': 'float64',
                'total_ask_qty': 'float64',
                'vol_surge': 'float64',
                'vqs_score': 'float64',
                'vwap': 'float64'
            }
            
            for col, dtype in type_map.items():
                if col in df.columns:
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
                    except:
                        pass

            if 'timestamp' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'])

            # 4. Write shard (Instant operation)
            df.to_parquet(batch_file, compression='snappy', index=False)
            logger.debug(f"Saved {len(ticks)} ticks to batch: {os.path.basename(batch_file)}")
            
        except Exception as e:
            logger.error(f"Error saving ticks to Parquet for {symbol}: {e}")

    def get_tick_file_path(self, symbol: str, date: datetime = None) -> str:
        """Helper to resolve the directory path for the daily tick dataset."""
        date = date or datetime.now()
        date_str = date.strftime("%Y-%m-%d")
        return os.path.join(self.base_path, symbol, f"{date_str}.parquet")
