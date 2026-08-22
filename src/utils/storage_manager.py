import os
import itertools
import threading
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from loguru import logger
from typing import List, Dict, Any

IST = "Asia/Kolkata"

# Scalar columns are pinned so shards written at different times cannot drift
# between int64 and double. A mismatch makes pyarrow refuse to read the whole
# day directory, not just the offending shard.
TYPE_MAP = {
    'ltp': 'float64', 'vwap': 'float64', 'imbalance': 'float64',
    'l1_imbalance': 'float64', 'vqs_score': 'float64', 'vol_surge': 'float64',
    'bid_qty': 'float64', 'ask_qty': 'float64',
    'total_bid_qty': 'float64', 'total_ask_qty': 'float64',
    'bid_pct': 'float64', 'ask_pct': 'float64',
    'buy_vol': 'float64', 'sell_vol': 'float64',
    'volume': 'int64', 'current_bar_volume': 'float64',
    'oi': 'float64', 'pcr_oi': 'float64', 'pcr_vol': 'float64',
    'max_pain': 'float64',
}


def _one_to_ist(v):
    """Convert a single value to a tz-naive IST Timestamp."""
    if v is None:
        return pd.NaT
    try:
        ts = pd.Timestamp(v)
    except Exception:
        return pd.NaT
    if ts is pd.NaT:
        return pd.NaT
    if ts.tzinfo is not None:
        ts = ts.tz_convert(IST).tz_localize(None)
    return ts


def normalize_ist(series: pd.Series) -> pd.Series:
    """Coerce a timestamp column to tz-naive IST wall clock.

    Packets are published as naive IST, but the archive also holds shards
    written while _publish_depth_packet emitted tz-aware (+05:30) values.
    Neither pandas fast path handles the mix safely:

      pd.to_datetime(mixed, errors='coerce')  -> silently NaTs the aware rows
      pd.to_datetime(mixed, format='mixed')   -> raises "Mixed timezones detected"

    and pd.read_parquet over a mixed day directory converts the aware half to
    UTC and drops the tz, shifting it back 5.5 hours with no error. So the
    vectorised path is used only when it is provably lossless, and anything
    mixed falls back to element-wise conversion.
    """
    if not isinstance(series, pd.Series):
        series = pd.Series(list(series))

    try:
        s = pd.to_datetime(series, errors="coerce")
    except (ValueError, TypeError):
        s = None

    if s is not None:
        tz = getattr(s.dtype, "tz", None)
        if tz is not None:
            return s.dt.tz_convert(IST).dt.tz_localize(None)
        # Rows that were present but became NaT are the tz-aware ones pandas
        # dropped — that means the column is mixed, so take the slow path.
        if s.dtype != object and not (s.isna() & series.notna()).any():
            return s

    return pd.Series(
        [_one_to_ist(v) for v in series], index=series.index
    ).astype("datetime64[ns]")


class StorageManager:
    """
    Handles file-based storage for high-frequency tick data using Parquet.
    """

    _seq = itertools.count()
    _seq_lock = threading.Lock()

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

        try:
            df = pd.DataFrame(ticks)

            # 1. Enforce dtypes so shards cannot drift apart
            for col, dtype in TYPE_MAP.items():
                if col in df.columns:
                    try:
                        num = pd.to_numeric(df[col], errors='coerce')
                        df[col] = num.fillna(0).astype(dtype) if dtype == 'int64' else num.astype(dtype)
                    except Exception:
                        pass

            # 2. Normalize timestamps to naive IST
            if 'timestamp' in df.columns:
                df['timestamp'] = normalize_ist(df['timestamp'])
                df = df.dropna(subset=['timestamp'])
            if df.empty:
                return

            # 3. Partition by the PACKET's own date, not wall clock.
            #    A flush that straddles midnight (or replays buffered packets)
            #    used to file everything under datetime.now(), which is how
            #    2026-03-21 packets ended up inside 2026-03-20.parquet/.
            if date is not None:
                groups = [(date.strftime("%Y-%m-%d"), df)]
            else:
                groups = [(str(d), g) for d, g in df.groupby(df['timestamp'].dt.date)]

            for date_str, group in groups:
                self._write_shard(symbol, date_str, group)

        except Exception as e:
            logger.error(f"Error saving ticks to Parquet for {symbol}: {e}")

    def _write_shard(self, symbol: str, date_str: str, df: pd.DataFrame):
        """Write one shard atomically into SYMBOL/DATE.parquet/."""
        symbol_dir = os.path.join(self.base_path, symbol)

        # A folder named *.parquet so pd.read_parquet treats it as one dataset
        day_dir_path = os.path.join(symbol_dir, f"{date_str}.parquet")

        # Handle migration from legacy single-file to directory-partitioned
        if os.path.exists(day_dir_path) and os.path.isfile(day_dir_path):
            temp_legacy = day_dir_path + ".legacy"
            os.rename(day_dir_path, temp_legacy)
            os.makedirs(day_dir_path, exist_ok=True)
            os.rename(temp_legacy, os.path.join(day_dir_path, "legacy_migration.parquet"))
        else:
            os.makedirs(day_dir_path, exist_ok=True)

        with StorageManager._seq_lock:
            seq = next(StorageManager._seq)
        batch_id = f"{int(datetime.now().timestamp() * 1000)}_{seq:06d}"
        batch_file = os.path.join(day_dir_path, f"batch_{batch_id}.parquet")

        # Write to a temp name then rename. A crash mid-write would otherwise
        # leave a truncated shard that poisons reads of the entire day.
        tmp_file = batch_file + ".tmp"
        df.to_parquet(tmp_file, compression='snappy', index=False)
        os.replace(tmp_file, batch_file)
        logger.debug(f"Saved {len(df)} ticks to batch: {os.path.basename(batch_file)}")

    def get_tick_file_path(self, symbol: str, date: datetime = None) -> str:
        """Helper to resolve the directory path for the daily tick dataset."""
        date = date or datetime.now()
        date_str = date.strftime("%Y-%m-%d")
        return os.path.join(self.base_path, symbol, f"{date_str}.parquet")
