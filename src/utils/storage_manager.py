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
        Appends a batch of ticks to a daily Parquet file for the given symbol.
        """
        if not ticks:
            return
        
        target_date = date or datetime.now()

        try:
            # 1. Prepare directory and file path
            date_str = target_date.strftime("%Y-%m-%d")
            symbol_dir = os.path.join(self.base_path, symbol)
            os.makedirs(symbol_dir, exist_ok=True)
            
            file_path = os.path.join(symbol_dir, f"{date_str}.parquet")
            
            # 2. Convert to DataFrame
            df = pd.DataFrame(ticks)
            
            # 3. Enforce Strict Types to avoid schema mismatch errors
            # pyarrow/parquet can fail if one batch has int and another has float for the same column
            type_map = {
                'ltp': 'float64',
                'imbalance': 'float64',
                'bid_qty': 'int64',
                'ask_qty': 'int64',
                'buy_vol': 'float64',
                'sell_vol': 'float64'
            }
            
            for col, dtype in type_map.items():
                if col in df.columns:
                    df[col] = df[col].astype(dtype)

            # Ensure timestamp is datetime object
            if 'timestamp' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'])

            # 4. Write/Append to Parquet
            table = pa.Table.from_pandas(df)
            
            if os.path.exists(file_path):
                try:
                    existing_table = pq.read_table(file_path)
                    combined_table = pa.concat_tables([existing_table, table])
                    pq.write_table(combined_table, file_path, compression='snappy')
                except Exception as schema_err:
                    logger.warning(f"Schema mismatch for {symbol}. Overwriting/Fixing: {schema_err}")
                    # If append fails due to schema, we might need to recreate the file with the new expanded schema
                    pq.write_table(table, file_path, compression='snappy')
            else:
                pq.write_table(table, file_path, compression='snappy')
                
            logger.debug(f"Saved {len(ticks)} ticks to {file_path}")
            
        except Exception as e:
            logger.error(f"Error saving ticks to Parquet for {symbol}: {e}")

    def get_tick_file_path(self, symbol: str, date: datetime = None) -> str:
        """Helper to resolve file path."""
        date = date or datetime.now()
        date_str = date.strftime("%Y-%m-%d")
        return os.path.join(self.base_path, symbol, f"{date_str}.parquet")
