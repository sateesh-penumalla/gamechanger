import os
import sys
import pandas as pd
import numpy as np
import json
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, time

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.db.schema import Ticker, init_db

def migrate_db(engine):
    """Updates ORB columns to support multi-event JSON storage."""
    with engine.connect() as conn:
        cols_to_add = [
            ("ORB_high", "FLOAT"),
            ("ORB_low", "FLOAT"),
            ("ORB_high_clean", "FLOAT"),
            ("ORB_low_clean", "FLOAT"),
            ("ORB_direction", "VARCHAR(20)"),
            ("ORB_range_pct", "FLOAT"),
            ("breakout_events", "JSON"),
            ("breakdown_events", "JSON")
        ]
        
        for col_name, col_type in cols_to_add:
            try:
                conn.execute(text(f"ALTER TABLE tickers ADD COLUMN {col_name} {col_type}"))
            except Exception: pass

        for old_col in ["breakout_time", "breakdown_time"]:
            try:
                conn.execute(text(f"ALTER TABLE tickers DROP COLUMN {old_col}"))
            except Exception: pass
        
        conn.commit()

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
                return "SL_HIT", pnl, duration
            if row['High'] >= target_price:
                pnl = 1.0
                duration = int((ts - start_time).total_seconds() / 60)
                return "TARGET", pnl, duration
        else: # SHORT
            if row['High'] >= sl_price:
                pnl = round(((entry_price - sl_price) / entry_price) * 100, 2)
                duration = int((ts - start_time).total_seconds() / 60)
                return "SL_HIT", pnl, duration
            if row['Low'] <= target_price:
                pnl = 1.0
                duration = int((ts - start_time).total_seconds() / 60)
                return "TARGET", pnl, duration
                
        # EOD Square-off
        if ts.hour == 15 and ts.minute >= 20:
            exit_price = row['Close']
            pnl = round(((exit_price - entry_price) / entry_price) * 100, 2) if mode == 'LONG' else round(((entry_price - exit_price) / entry_price) * 100, 2)
            duration = int((ts - start_time).total_seconds() / 60)
            return "SQUARE_OFF", pnl, duration
            
    return "OPEN", 0.0, 0

def calculate_strength(df, timestamp, orb_level, mode='LONG'):
    """Calculates conviction metrics and simulates trade performance."""
    idx = df.index.get_loc(timestamp)
    if idx < 26: return {"vol_surge": 0, "rsi": 50, "sl": orb_level, "pnl": 0, "outcome": "SKIP"}
    
    row = df.iloc[idx]
    entry_price = row['Close']
    
    # Conviction Metrics
    avg_vol = df.iloc[idx-20:idx]['Volume'].mean()
    vol_surge = round(row['Volume'] / avg_vol, 2) if avg_vol > 0 else 0
    rsi = round(row['RSI'], 1)
    
    # Volume Quality Score (Closing Strength)
    # Long: Close near High is good. Short: Close near Low is good.
    candle_range = row['High'] - row['Low']
    if candle_range > 0:
        if mode == 'LONG':
            vqs = round((entry_price - row['Low']) / candle_range, 2)
        else:
            vqs = round((row['High'] - entry_price) / candle_range, 2)
    else:
        vqs = 1.0 # Limit case
        
    # TRADE SIMULATION (1% Target vs SL)
    outcome, pnl, duration = simulate_event_trade(df, timestamp, entry_price, orb_level, mode)
    
    return {
        "vol_surge": vol_surge,
        "rsi": rsi,
        "macd": round(row['MACD_Hist'], 2),
        "slope": round(row['EMA_Slope'], 3),
        "vol_quality": float(vqs),
        "sl": float(orb_level),
        "entry": float(entry_price),
        "pnl": pnl,
        "duration": duration,
        "outcome": outcome
    }

def calculate_orb():
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))
    migrate_db(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    snipers = session.query(Ticker).filter(Ticker.oracle_status.in_(['UP_SNIPER', 'DOWN_SNIPER'])).all()
    cache_dir = "data/sniper_cache"
    
    print(f"\n🚀 MULTI-INDICATOR ORB ANALYSIS FOR {len(snipers)} STOCKS")
    print("="*120)
    print(f"{'Symbol':<15} | {'High':<8} | {'Low':<8} | {'Breakouts':<12} | {'Breakdowns':<12} | {'Conviction'}")
    print("-" * 120)

    for ticker_obj in snipers:
        symbol = ticker_obj.symbol
        filepath = os.path.join(cache_dir, f"{symbol}_1d_1m.csv")
        if not os.path.exists(filepath): continue
            
        df = pd.read_csv(filepath, index_col=0, parse_dates=True)
        if df.empty: continue
            
        # Normalization (IST)
        if df.index.tz is None:
            first_hour = df.index[0].hour
            if first_hour < 6: df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
            else: df.index = df.index.tz_localize('Asia/Kolkata', ambiguous='infer')
        
        # --- CALCULATE STRENGTH INDICATORS ---
        # A. RSI
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss.replace(0, np.nan)
        df['RSI'] = 100 - (100 / (1 + rs))

        # B. MACD
        exp12 = df['Close'].ewm(span=12, adjust=False).mean()
        exp26 = df['Close'].ewm(span=26, adjust=False).mean()
        macd = exp12 - exp26
        sig_l = macd.ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = macd - sig_l
        
        # C. EMA Slope (MA20)
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['EMA_Slope'] = df['MA20'].diff()

        # 1. Define Opening Range (09:15 - 09:30)
        early_data = df.between_time('09:15', '09:30')
        if early_data.empty: continue
        orb_high = early_data['High'].max()
        orb_low = early_data['Low'].min()
        
        # CLEAN ORB: Highest and Lowest of (Open, Close) to ignore wicks
        body_highs = early_data[['Open', 'Close']].max(axis=1)
        body_lows = early_data[['Open', 'Close']].min(axis=1)
        orb_high_clean = body_highs.max()
        orb_low_clean = body_lows.min()
        
        # RANGE DIRECTION (Sentiment)
        orb_open = early_data.iloc[0]['Open']
        orb_close = early_data.iloc[-1]['Close']
        rd_pct = (orb_close - orb_open) / orb_open
        if rd_pct > 0.0005: orb_dir = "BULLISH"
        elif rd_pct < -0.0005: orb_dir = "BEARISH"
        else: orb_dir = "NEUTRAL"
        
        orb_range_pct = round(((orb_high - orb_low) / orb_low) * 100, 2)
        
        # 2. Multi-Event Tracking (Dual Track: Standard & Clean)
        post_orb = df.between_time('09:31', '15:20')
        breakout_events = []
        breakdown_events = []
        
        # Tracking states for Standard
        in_bo = False
        in_bd = False
        
        # Tracking states for Clean
        in_bo_c = False
        in_bd_c = False
        
        for ts, row in post_orb.iterrows():
            time_str = ts.strftime('%H:%M')
            
            # --- STANDARD DETECTION ---
            if row['High'] > orb_high:
                if not in_bo:
                    stats = calculate_strength(df, ts, orb_low, mode='LONG')
                    if stats['vol_surge'] > 1.0 or stats['rsi'] > 55:
                        stats['time'] = time_str
                        stats['boundary_type'] = 'STANDARD'
                        breakout_events.append(stats)
                        in_bo = True
            elif row['Close'] < (orb_high * 0.998): in_bo = False
                
            if row['Low'] < orb_low:
                if not in_bd:
                    stats = calculate_strength(df, ts, orb_high, mode='SHORT')
                    if stats['vol_surge'] > 1.0 or stats['rsi'] < 45:
                        stats['time'] = time_str
                        stats['boundary_type'] = 'STANDARD'
                        breakdown_events.append(stats)
                        in_bd = True
            elif row['Close'] > (orb_low * 1.002): in_bd = False

            # --- CLEAN (BODY) DETECTION ---
            if row['High'] > orb_high_clean:
                if not in_bo_c:
                    stats_c = calculate_strength(df, ts, orb_low_clean, mode='LONG')
                    if stats_c['vol_surge'] > 1.0 or stats_c['rsi'] > 55:
                        stats_c['time'] = time_str
                        stats_c['boundary_type'] = 'CLEAN'
                        breakout_events.append(stats_c)
                        in_bo_c = True
            elif row['Close'] < (orb_high_clean * 0.998): in_bo_c = False

            if row['Low'] < orb_low_clean:
                if not in_bd_c:
                    stats_c = calculate_strength(df, ts, orb_high_clean, mode='SHORT')
                    if stats_c['vol_surge'] > 1.0 or stats_c['rsi'] < 45:
                        stats_c['time'] = time_str
                        stats_c['boundary_type'] = 'CLEAN'
                        breakdown_events.append(stats_c)
                        in_bd_c = True
            elif row['Close'] > (orb_low_clean * 1.002): in_bd_c = False

        ticker_obj.ORB_high = float(orb_high)
        ticker_obj.ORB_low = float(orb_low)
        ticker_obj.ORB_high_clean = float(orb_high_clean)
        ticker_obj.ORB_low_clean = float(orb_low_clean)
        ticker_obj.ORB_direction = orb_dir
        ticker_obj.ORB_range_pct = float(orb_range_pct)
        ticker_obj.breakout_events = breakout_events
        ticker_obj.breakdown_events = breakdown_events
        
        conviction = "NONE"
        if breakout_events:
            best_bo = max(breakout_events, key=lambda x: x['vol_surge'])
            conviction = f"BO {best_bo['time']} ({best_bo['vol_surge']}x)"
        
        print(f"{symbol:<15} | {orb_high:<8.2f} | {orb_low:<8.2f} | {len(breakout_events):<12} | {len(breakdown_events):<12} | {conviction}")
        
    session.commit()
    session.close()
    print("="*120)
    print("✅ Multi-Indicator ORB Analysis Complete.")

if __name__ == "__main__":
    calculate_orb()
