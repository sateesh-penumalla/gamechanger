import os
import sys
import pandas as pd
import numpy as np
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.dhan_client import DhanDataClient

def calculate_indicators(df):
    df = df.copy()
    df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['STD20'] = df['Close'].rolling(window=20).std()
    df['BW'] = (df['STD20'] * 4) / df['MA20'].replace(0, np.nan)
    df['BW_SMA'] = df['BW'].rolling(20).mean()
    
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI_Slope'] = df['RSI'] - df['RSI'].shift(1)
    
    df['VWAP_D'] = (df['Close'] - df['VWAP']) / df['VWAP'] * 100
    
    # --- GOLDEN GUARDS (Scout Fusion) ---
    exp12 = df['Close'].ewm(span=12, adjust=False).mean()
    exp26 = df['Close'].ewm(span=26, adjust=False).mean()
    macd = exp12 - exp26
    sig_l = macd.ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = macd - sig_l
    
    low_min = df['Low'].rolling(window=14).min()
    high_max = df['High'].rolling(window=14).max()
    df['Stoch_K'] = (100 * (df['Close'] - low_min) / (high_max - low_min)).fillna(50)
    
    df['EMA_Slope'] = df['MA20'].diff()
    
    df['Score'] = 0
    
    # Vectorized score calculation (Base)
    vwap_mask = (df['VWAP_D'] >= -1.2) & (df['VWAP_D'] <= -0.3)
    df.loc[vwap_mask, 'Score'] += 30
    
    sqz_mask = df['BW'] < df['BW_SMA']
    df.loc[sqz_mask, 'Score'] += 30
    
    avg_vol = df['Volume'].mean()
    df.loc[df['Volume'] > (avg_vol * 1.2), 'Score'] += 20
    
    rsi_mask = (df['RSI'] >= 40) & (df['RSI'] <= 55)
    df.loc[rsi_mask, 'Score'] += 20
    
    # vwap_dist and rsi are calculated above or in calculate_indicators
    
    # --- GOLDEN GUARDS (Safety Vetoes) ---
    # Guard 1: Red Expansion (MACD Histogram widening down)
    macd_prev = df['MACD_Hist'].shift(1)
    macd_veto = (df['MACD_Hist'] < 0) & (df['MACD_Hist'] < macd_prev)
    df.loc[macd_veto, 'Score'] -= 50
    
    # Guard 2: Dead Floor (Stochastics Pinned)
    df.loc[df['Stoch_K'] < 15, 'Score'] -= 30
    
    # Guard 3: Slope of Death (Falling Knife)
    df.loc[df['EMA_Slope'] < -0.1, 'Score'] -= 20

    # Baseline Grounds
    df.loc[df['RSI'] < 35, 'Score'] -= 40
    df.loc[df['VWAP_D'] < -1.5, 'Score'] -= 50
    
    return df

def run_simulation(symbol, mode="PASSIVE"):
    cache_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'sniper_cache'))
    filepath = os.path.join(cache_dir, f"{symbol}_1d_1m.csv")
    
    if not os.path.exists(filepath): return []
    df = pd.read_csv(filepath, index_col=0, parse_dates=True)
    if df.empty: return []
    
    df = calculate_indicators(df)
    trades = []
    in_position = False
    entry_price = 0
    entry_time = None
    peak_price = 0
    tsl_price = 0
    reached_target_1 = False
    last_exit_time = None
    
    for i in range(20, len(df)):
        curr = df.iloc[i]
        score = curr['Score']
        curr_price = curr['Close']
        ist_time = df.index[i]
        
        # Cool-down logic
        in_cooldown = False
        if last_exit_time and (ist_time - last_exit_time).total_seconds() / 60 < 15:
            in_cooldown = True

        if not in_position and not in_cooldown and score >= 80:
            in_position = True
            entry_price = curr_price
            entry_time = ist_time
            peak_price = curr_price
            tsl_price = entry_price * 0.995 # Hard Stop
            reached_target_1 = False
            reached_breakout = False
            peak_momentum = 0
            
        elif in_position:
            profit_pct = (curr_price - entry_price) / entry_price * 100
            
            if mode == "PASSIVE":
                if profit_pct >= 1.0:
                    trades.append(profit_pct)
                    in_position = False
                    last_exit_time = ist_time
                elif profit_pct <= -0.5:
                    trades.append(profit_pct)
                    in_position = False
                    last_exit_time = ist_time
            else: # APEX MODE
                if not reached_target_1 and profit_pct >= 1.0:
                    # Steady Hand Check
                    rsi_slope = curr['RSI_Slope']
                    vol_r = curr['Volume'] / df['Volume'].rolling(20).mean().iloc[i]
                    if rsi_slope < 5.5 and vol_r < 2.5:
                        reached_target_1 = True
                    else:
                        trades.append(profit_pct)
                        in_position = False
                        last_exit_time = ist_time
                
                # TIERED PULSE TRAILING with BREAKOUT MEMORY
                if in_position and reached_target_1:
                    if curr_price > peak_price:
                        peak_price = curr_price
                    
                    # Momentum Memory: Track peak slope during runaway
                    curr_v = curr['RSI_Slope']
                    if curr_v > peak_momentum:
                        peak_momentum = curr_v
                    
                    # Breakout Memory: Lock in the 1% buffer once crossed 2%
                    if profit_pct > 2.0 and peak_momentum > 4.0:
                        reached_breakout = True
                    
                    # Gap Selection
                    gap_pct = 1.0 if reached_breakout else 0.5
                    
                    new_tsl = peak_price * (1 - gap_pct/100)
                    if new_tsl > tsl_price:
                        tsl_price = new_tsl

                if in_position and curr_price <= tsl_price:
                    trades.append(profit_pct)
                    in_position = False
                    last_exit_time = ist_time
    return trades

def main():
    load_dotenv()
    # No need for Dhan client as we use cached CSVs
    
    from src.db.schema import Ticker
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(os.getenv("DATABASE_URL"))
    Session = sessionmaker(bind=engine)
    session = Session()
    snipers = session.query(Ticker).filter(Ticker.oracle_status == 'SNIPER').all()
    symbols = [s.symbol for s in snipers]
    session.close()

    print(f"\n📈 APEX VS PASSIVE NET ROI COMPARISON (41 SNIPER STOCKS)")
    print("="*100)
    
    TAX = 0.05
    stats = {"PASSIVE": {"pnl": 0, "count": 0}, "APEX": {"pnl": 0, "count": 0}}
    
    for s in symbols:
        for mode in ["PASSIVE", "APEX"]:
            trades = run_simulation(s, mode=mode)
            for t in trades:
                stats[mode]["count"] += 1
                stats[mode]["pnl"] += (t - TAX)

    print(f"{'STRATEGY':<10} | {'TRADES':<6} | {'NET PNL':<10}")
    print("-" * 35)
    for mode, s in stats.items():
        print(f"{mode:<10} | {s['count']:<6} | {s['pnl']:>8.2f}%")
    
    improvement = stats["APEX"]["pnl"] - stats["PASSIVE"]["pnl"]
    print("\n" + "="*35)
    print(f"APEX EDGE: {improvement:+.2f}%")

if __name__ == "__main__":
    main()
