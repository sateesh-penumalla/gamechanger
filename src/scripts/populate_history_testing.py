import os
import sys
import pandas as pd
import numpy as np
import json
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

def calculate_indicators(df):
    """Calculates all strategy indicators."""
    # A. RSI
    try:
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss.replace(0, np.nan)
        df['RSI'] = 100 - (100 / (1 + rs))
    except Exception:
        df['RSI'] = np.nan

    # B. MACD
    try:
        exp12 = df['Close'].ewm(span=12, adjust=False).mean()
        exp26 = df['Close'].ewm(span=26, adjust=False).mean()
        macd = exp12 - exp26
        sig_l = macd.ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = macd - sig_l
    except Exception:
        df['MACD_Hist'] = np.nan
    
    # C. EMA Slope (MA20)
    try:
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['EMA_Slope'] = df['MA20'].diff()
    except Exception:
        df['EMA_Slope'] = np.nan
    
    return df

def simulate_event_trade(df, entry_ts, entry_price, sl_price, mode='LONG'):
    """Simulates a trade from entry till 1% target, SL, or EOD."""
    target_pct = 0.01
    target_price = entry_price * (1 + target_pct) if mode == 'LONG' else entry_price * (1 - target_pct)
    
    post_entry = df.loc[entry_ts:]
    start_time = entry_ts
    
    for ts, row in post_entry.iterrows():
        # Check SL
        if mode == 'LONG':
            if row['Low'] <= sl_price:
                pnl = round(((sl_price - entry_price) / entry_price) * 100, 2)
                duration = int((ts - start_time).total_seconds() / 60)
                return "SL_HIT", pnl, duration, ts.strftime('%H:%M:%S')
            if row['High'] >= target_price:
                pnl = 1.0
                duration = int((ts - start_time).total_seconds() / 60)
                return "TARGET", pnl, duration, ts.strftime('%H:%M:%S')
        else: # SHORT
            if row['High'] >= sl_price:
                pnl = round(((entry_price - sl_price) / entry_price) * 100, 2)
                duration = int((ts - start_time).total_seconds() / 60)
                return "SL_HIT", pnl, duration, ts.strftime('%H:%M:%S')
            if row['Low'] <= target_price:
                pnl = 1.0
                duration = int((ts - start_time).total_seconds() / 60)
                return "TARGET", pnl, duration, ts.strftime('%H:%M:%S')
                
        # EOD Square-off
        if ts.hour == 15 and ts.minute >= 20:
            exit_price = row['Close']
            pnl = round(((exit_price - entry_price) / entry_price) * 100, 2) if mode == 'LONG' else round(((entry_price - exit_price) / entry_price) * 100, 2)
            duration = int((ts - start_time).total_seconds() / 60)
            return "SQUARE_OFF", pnl, duration, ts.strftime('%H:%M:%S')
            
    return "OPEN", 0.0, 0, None

def calculate_strength(df, timestamp, orb_level, mode='LONG'):
    """Calculates conviction metrics and simulates trade performance."""
    try:
        idx = df.index.get_loc(timestamp)
        if idx < 26: return None
        
        row = df.iloc[idx]
        entry_price = row['Close']
        
        # Conviction Metrics
        avg_vol = df.iloc[idx-20:idx]['Volume'].mean()
        vol_surge = round(row['Volume'] / avg_vol, 2) if avg_vol > 0 else 0
        rsi = round(row['RSI'], 1)
        
        # Volume Quality Score
        candle_range = row['High'] - row['Low']
        if candle_range > 0:
            if mode == 'LONG':
                vqs = round((entry_price - row['Low']) / candle_range, 2)
            else:
                vqs = round((row['High'] - entry_price) / candle_range, 2)
        else:
            vqs = 1.0
            
        outcome, pnl, duration, exit_time_str = simulate_event_trade(df, timestamp, entry_price, orb_level, mode)
        
        return {
            "vol_surge": float(vol_surge),
            "rsi": float(rsi),
            "macd": float(round(row['MACD_Hist'], 2)),
            "slope": float(round(row['EMA_Slope'], 3)),
            "vol_quality": float(vqs),
            "sl": float(orb_level),
            "entry": float(entry_price),
            "pnl": float(pnl),
            "duration": int(duration),
            "outcome": outcome,
            "exit_time": exit_time_str
        }
    except Exception:
        return None

def process_single_stock(symbol, date_str, filepath, symbol_sector_map, sector_dfs):
    """Worker function to process a single stock CSV for a given date."""
    try:
        df = pd.read_csv(filepath, index_col=0, parse_dates=True)
        if df.empty or len(df) < 50: return None, None
        
        df = calculate_indicators(df)
        
        # ORB Calculation
        early_data = df.between_time('09:15', '09:30')
        if early_data.empty: return None, None
        
        orb_high = early_data['High'].max()
        orb_low = early_data['Low'].min()
        
        body_highs = early_data[['Open', 'Close']].max(axis=1)
        body_lows = early_data[['Open', 'Close']].min(axis=1)
        orb_high_clean = body_highs.max()
        orb_low_clean = body_lows.min()
        
        orb_open = early_data.iloc[0]['Open']
        orb_close = early_data.iloc[-1]['Close']
        rd_pct = (orb_close - orb_open) / orb_open
        if rd_pct > 0.0005: orb_dir = "BULLISH"
        elif rd_pct < -0.0005: orb_dir = "BEARISH"
        else: orb_dir = "NEUTRAL"
        
        orb_range_pct = round(((orb_high - orb_low) / orb_low) * 100, 2)
        orb_range_pct_clean = round(((orb_high_clean - orb_low_clean) / orb_low_clean) * 100, 2)
        
        master_row = {
            "symbol": symbol,
            "trade_date": date_str,
            "ORB_high": float(orb_high),
            "ORB_low": float(orb_low),
            "ORB_high_clean": float(orb_high_clean),
            "ORB_low_clean": float(orb_low_clean),
            "ORB_direction": orb_dir,
            "ORB_range_pct": float(orb_range_pct),
            "ORB_range_pct_clean": float(orb_range_pct_clean)
        }

        # Event Detection
        event_rows = []
        post_orb = df.between_time('09:31', '15:20')
        in_bo, in_bd, in_bo_c, in_bd_c = False, False, False, False
        
        sector_name = symbol_sector_map.get(symbol)
        sector_df = sector_dfs.get(sector_name)

        for ts, row in post_orb.iterrows():
            time_str = ts.strftime('%H:%M:%S')
            # Standard BO
            if row['High'] > orb_high:
                if not in_bo:
                    stats = calculate_strength(df, ts, orb_low, mode='LONG')
                    if stats and (stats['vol_surge'] > 1.0 or (not np.isnan(stats['rsi']) and stats['rsi'] > 55)):
                        sec_change = get_sector_change(sector_df, time_str, date_str)
                        stats.update({
                            "symbol": symbol, 
                            "trade_date": date_str, 
                            "event_type": "BREAKOUT", 
                            "boundary_type": "STANDARD", 
                            "event_time": time_str,
                            "sector_change_at_entry": sec_change
                        })
                        event_rows.append(stats)
                        in_bo = True
            elif row['Close'] < (orb_high * 0.998): in_bo = False
                
            # Standard BD
            if row['Low'] < orb_low:
                if not in_bd:
                    stats = calculate_strength(df, ts, orb_high, mode='SHORT')
                    if stats and (stats['vol_surge'] > 1.0 or (not np.isnan(stats['rsi']) and stats['rsi'] < 45)):
                        sec_change = get_sector_change(sector_df, time_str, date_str)
                        stats.update({
                            "symbol": symbol, 
                            "trade_date": date_str, 
                            "event_type": "BREAKDOWN", 
                            "boundary_type": "STANDARD", 
                            "event_time": time_str,
                            "sector_change_at_entry": sec_change
                        })
                        event_rows.append(stats)
                        in_bd = True
            elif row['Close'] > (orb_low * 1.002): in_bd = False

            # Clean BO
            if row['High'] > orb_high_clean:
                if not in_bo_c:
                    stats_c = calculate_strength(df, ts, orb_low_clean, mode='LONG')
                    if stats_c and (stats_c['vol_surge'] > 1.0 or (not np.isnan(stats_c['rsi']) and stats_c['rsi'] > 55)):
                        sec_change = get_sector_change(sector_df, time_str, date_str)
                        stats_c.update({
                            "symbol": symbol, 
                            "trade_date": date_str, 
                            "event_type": "BREAKOUT", 
                            "boundary_type": "CLEAN", 
                            "event_time": time_str,
                            "sector_change_at_entry": sec_change
                        })
                        event_rows.append(stats_c)
                        in_bo_c = True
            elif row['Close'] < (orb_high_clean * 0.998): in_bo_c = False

            # Clean BD
            if row['Low'] < orb_low_clean:
                if not in_bd_c:
                    stats_c = calculate_strength(df, ts, orb_high_clean, mode='SHORT')
                    if stats_c and (stats_c['vol_surge'] > 1.0 or (not np.isnan(stats_c['rsi']) and stats_c['rsi'] < 45)):
                        sec_change = get_sector_change(sector_df, time_str, date_str)
                        stats_c.update({
                            "symbol": symbol, 
                            "trade_date": date_str, 
                            "event_type": "BREAKDOWN", 
                            "boundary_type": "CLEAN", 
                            "event_time": time_str,
                            "sector_change_at_entry": sec_change
                        })
                        event_rows.append(stats_c)
                        in_bd_c = True
            elif row['Close'] > (orb_low_clean * 1.002): in_bd_c = False

        return master_row, event_rows
    except Exception as e:
        logger.error(f"Worker error for {symbol} on {date_str}: {e}")
        return None, None

def get_weekly_data(symbol, end_date):
    """Fetches weekly data up to the given date and calculates indicators."""
    try:
        # Load daily data for the symbol (assuming we have a directory of daily files or can resample)
        # For simplicity, we'll assume we can load the full daily history and resample.
        # This part might need adjustment based on where full history is stored.
        # Construct path: data/history/<DATE>/<SYMBOL>.csv
        # This is tricky because we only have daily slices in the current loop.
        # We need a way to look back.
        # Using yfinance or a consolidated history file might be better, but let's see if we can use what we have.
        # PROPOSAL: We'll skip complex weekly calculation for now and fill with placeholders 
        # OR better, if we have a way to get it. 
        # Given the error is just "Unknown Column", we MUST add the column to the DB first.
        # Then we can populate it.
        
        # Let's check if we have a function to get full history.
        # ... (Self-correction: The script iterates daily folders. Building weekly candles on the fly is expensive/hard without full history loaded).
        # We will add columns with NULL/Default values first to unblock the script.
        pass
    except Exception:
        pass
    return None, None, "NEUTRAL"

def init_history_tables(engine):
    """Creates the normalized tables."""
    with engine.connect() as conn:
        # 1. Master Table
        # conn.execute(text("DROP TABLE IF EXISTS history_events"))
        # conn.execute(text("DROP TABLE IF EXISTS history_testing"))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS history_testing (
                symbol VARCHAR(20),
                trade_date DATE,
                ORB_high FLOAT,
                ORB_low FLOAT,
                ORB_high_clean FLOAT,
                ORB_low_clean FLOAT,
                ORB_direction VARCHAR(20),
                ORB_range_pct FLOAT,
                ORB_range_pct_clean FLOAT,
                weekly_rsi FLOAT, 
                weekly_sma FLOAT, 
                oracle_status VARCHAR(20),
                PRIMARY KEY (symbol, trade_date)
            )
        """))
        
        # Check if columns exist (for migration)
        try:
            conn.execute(text("SELECT weekly_rsi FROM history_testing LIMIT 1"))
        except Exception:
            logger.info("Migrating history_testing: Adding weekly_rsi, weekly_sma, oracle_status")
            conn.execute(text("ALTER TABLE history_testing ADD COLUMN weekly_rsi FLOAT"))
            conn.execute(text("ALTER TABLE history_testing ADD COLUMN weekly_sma FLOAT"))
            conn.execute(text("ALTER TABLE history_testing ADD COLUMN oracle_status VARCHAR(20)"))
        
        # 2. Child Table (Events)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS history_events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20),
                trade_date DATE,
                event_type ENUM('BREAKOUT', 'BREAKDOWN'),
                boundary_type ENUM('STANDARD', 'CLEAN'),
                event_time TIME,
                exit_time TIME,
                vol_surge FLOAT,
                rsi FLOAT,
                macd FLOAT,
                slope FLOAT,
                vol_quality FLOAT,
                sl FLOAT,
                entry FLOAT,
                pnl FLOAT,
                duration INT,
                outcome VARCHAR(20),
                sector_change_at_entry FLOAT,
                
                UNIQUE KEY unique_event (symbol, trade_date, event_time, event_type)
            )
        """))
        conn.commit()


def clean_val(val):
    if pd.isna(val) or np.isinf(val):
        return None
    return float(val)

# Proxy Map for missing sector files
SECTOR_PROXY_MAP = {
    'NIFTY_TECHNOLOGY': 'NIFTY_50', # IT Missing, use Broad Market
    'NIFTY_CONSUMER_CYCLICAL': 'NIFTY_CONSUMER_DURABLES',
    'NIFTY_CONSUMER_DEFENSIVE': 'NIFTY_FMCG',
    'NIFTY_COMMUNICATION_SERVICES': 'NIFTY_MEDIA',
    'NIFTY_UTILITIES': 'NIFTY_ENERGY',   
    'NIFTY_BASIC_MATERIALS': 'NIFTY_METAL',
    'NIFTY_INFRASTRUCTURE': 'NIFTY_50',
    'NIFTY_PSE': 'NIFTY_50',
    'NIFTY_CPSE': 'NIFTY_50',
    'NIFTY_SERVICES_SECTOR': 'NIFTY_50',
    'NIFTY_INDUSTRIALS': 'NIFTY_50',
    'NIFTY_HEALTHCARE': 'NIFTY_PHARMA',
    'NIFTY_IT': 'NIFTY_50',
    'UNKNOWN': 'NIFTY_50'
}

def get_sector_map(engine):
    """Returns a dict mapping symbol -> sector."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT symbol, sector FROM tickers"))
        mapping = {row[0]: row[1] for row in result}
        
    # Manual overrides for symbols with incorrect/missing sector data
    mapping['WAAREEENER'] = 'NIFTY_ENERGY'
    return mapping

def load_sector_data(sector_name, date_str):
    """Loads 1m data for a specific sector and date."""
    if not sector_name:
        return None
        
    try:
        # Construct path: data/history_sectors/<DATE>/<SECTOR>.csv
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        
        # Helper to try load
        def try_load(sec_name):
            p = os.path.join(base_dir, 'data', 'history_sectors', date_str, f'{sec_name}.csv')
            if os.path.exists(p):
                df = pd.read_csv(p)
                if 'Datetime' in df.columns:
                    df['Datetime'] = pd.to_datetime(df['Datetime'])
                    df.set_index('Datetime', inplace=True)
                    return df
            return None

        # 1. Try direct match
        df = try_load(sector_name)
        if df is not None: return df
        
        # 2. Try Proxy
        proxy = SECTOR_PROXY_MAP.get(sector_name)
        if proxy:
            df = try_load(proxy)
            if df is not None: return df
            
        # 3. Fallback to NIFTY_50 (Broad Market)
        df = try_load('NIFTY_50')
        if df is not None: return df
        
        return None
            
    except Exception as e:
        logger.warning(f"Failed to load sector data for {sector_name} on {date_str}: {e}")
        return None
    return None

def get_sector_change(sector_df, entry_time_str, date_str):
    """Calculates % change of sector from Open to Entry Time."""
    if sector_df is None or sector_df.empty:
        return None
        
    try:
        # Construct full datetime for entry
        entry_dt = pd.to_datetime(f"{date_str} {entry_time_str}")
        
        # Get sector open (first candle of the day)
        sector_open = sector_df.iloc[0]['Open']
        
        # Get sector value at entry time (or nearest before)
        # We use asof to find the closest timestamp <= entry_dt
        idx_loc = sector_df.index.get_indexer([entry_dt], method='pad')[0]
        
        if idx_loc == -1:
            return None
            
        sector_price_at_entry = sector_df.iloc[idx_loc]['Close']
        
        # Check if we have a valid previous close
        prev_close = sector_df.attrs.get("prev_close")
        
        if prev_close and prev_close > 0:
            return ((sector_price_at_entry - prev_close) / prev_close) * 100
        else:
            # Fallback to Open if no history (first day)
            sector_open = sector_df.iloc[0]['Open']
            if sector_open > 0:
                return ((sector_price_at_entry - sector_open) / sector_open) * 100
            
    except Exception as e:
        # logger.debug(f"Error calculating sector change: {e}")
        return None
    return None

def process_historical_data():
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))
    init_history_tables(engine)
    
    # Pre-load sector map
    symbol_sector_map = get_sector_map(engine)
    
    history_base = "data/history"
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    
    if not os.path.exists(history_base):
        logger.error(f"History directory not found: {history_base}")
        return

    # Track sector closes across dates: Map[SectorName] -> LastClosePrice
    sector_tracker = {}

    # Iterate through date folders
    date_folders = sorted([d for d in os.listdir(history_base) if os.path.isdir(os.path.join(history_base, d))])
    
    logger.info(f"Processing {len(date_folders)} date folders...")

    for date_str in date_folders:
        # Skip folders that are not date-like
        if not date_str.startswith('20'):
            continue
            
        date_path = os.path.join(history_base, date_str)
        stock_files = [f for f in os.listdir(date_path) if f.endswith('.csv')]
        
        logger.info(f"📅 Date: {date_str} ({len(stock_files)} stocks)")
        
        master_rows = []
        event_rows = []
        
        # 1. Load Sector Data & Track Closes for 'Change vs Prev Close'
        # sector_tracker persists across loop (defined outside)
        sector_dfs = {} 
        sector_path_date = os.path.join(base_dir, 'data', 'history_sectors', date_str)
        
        if os.path.exists(sector_path_date):
            for sec_file in os.listdir(sector_path_date):
                if not sec_file.endswith('.csv'): continue
                
                sec_name = sec_file.replace('.csv', '')
                sec_filepath = os.path.join(sector_path_date, sec_file)
                
                try:
                    s_df = pd.read_csv(sec_filepath)
                    if 'Datetime' in s_df.columns:
                        s_df['Datetime'] = pd.to_datetime(s_df['Datetime'])
                        s_df.set_index('Datetime', inplace=True)
                        
                        # Attach Prev Close (from yesterday)
                        prev_c = sector_tracker.get(sec_name)
                        s_df.attrs["prev_close"] = prev_c
                        
                        if sec_name == 'NIFTY_METAL':
                            logger.info(f"[{date_str}] Sector {sec_name} loaded. PrevClose from Tracker: {prev_c}")

                        # Update Tracker (for tomorrow)
                        if not s_df.empty:
                            today_close = s_df.iloc[-1]['Close']
                            sector_tracker[sec_name] = today_close
                            if sec_name == 'NIFTY_METAL':
                                logger.info(f"[{date_str}] Updated Tracker for {sec_name}: {today_close}")
                            
                        sector_dfs[sec_name] = s_df
                except Exception as e:
                    logger.warning(f"Failed to load sector {sec_name}: {e}")

        # Parallel Stock Processing
        max_workers = min(os.cpu_count() or 4, 8)
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for stock_file in stock_files:
                symbol = stock_file.replace('.csv', '')
                filepath = os.path.join(date_path, stock_file)
                futures.append(executor.submit(process_single_stock, symbol, date_str, filepath, symbol_sector_map, sector_dfs))
            
            for future in as_completed(futures):
                try:
                    m_row, e_rows = future.result()
                    if m_row: master_rows.append(m_row)
                    if e_rows: event_rows.extend(e_rows)
                except Exception as e:
                    logger.error(f"Failed to get result from worker: {e}")

        # Batch insert for the date
        with engine.connect() as conn:
            if master_rows:
                for row in master_rows:
                    # Default values for new columns
                    row.setdefault('weekly_rsi', None)
                    row.setdefault('weekly_sma', None)
                    row.setdefault('oracle_status', 'NEUTRAL')
                    
                    conn.execute(text("""
                        INSERT INTO history_testing (symbol, trade_date, ORB_high, ORB_low, ORB_high_clean, ORB_low_clean, ORB_direction, ORB_range_pct, ORB_range_pct_clean, weekly_rsi, weekly_sma, oracle_status)
                        VALUES (:symbol, :trade_date, :ORB_high, :ORB_low, :ORB_high_clean, :ORB_low_clean, :ORB_direction, :ORB_range_pct, :ORB_range_pct_clean, :weekly_rsi, :weekly_sma, :oracle_status)
                        ON DUPLICATE KEY UPDATE 
                        ORB_high=VALUES(ORB_high),
                        weekly_rsi=VALUES(weekly_rsi),
                        weekly_sma=VALUES(weekly_sma),
                        oracle_status=VALUES(oracle_status)
                    """), row)
            
            if event_rows:
                for row in event_rows:
                    # Clean the row for database
                    db_row = {k: clean_val(v) if isinstance(v, (float, int)) and k not in ['duration'] else v for k, v in row.items()}
                    conn.execute(text("""
                        INSERT INTO history_events (symbol, trade_date, event_type, boundary_type, event_time, exit_time, vol_surge, rsi, macd, slope, sector_change_at_entry, vol_quality, sl, entry, pnl, duration, outcome)
                        VALUES (:symbol, :trade_date, :event_type, :boundary_type, :event_time, :exit_time, :vol_surge, :rsi, :macd, :slope, :sector_change_at_entry, :vol_quality, :sl, :entry, :pnl, :duration, :outcome)
                        ON DUPLICATE KEY UPDATE 
                        vol_surge=VALUES(vol_surge), 
                        rsi=VALUES(rsi), 
                        sector_change_at_entry=VALUES(sector_change_at_entry),
                        pnl=VALUES(pnl),
                        outcome=VALUES(outcome),
                        duration=VALUES(duration)
                    """), db_row)
            conn.commit()

    logger.info("✅ Bulk Normalized History Testing Population Complete.")

if __name__ == "__main__":
    process_historical_data()
