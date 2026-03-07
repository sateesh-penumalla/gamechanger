import os
import sys
import pandas as pd
import numpy as np
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.dhan_client import DhanDataClient
from src.db.schema import Ticker

def calculate_indicators(df, use_guards=True):
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
    
    if use_guards:
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
    df.loc[(df['VWAP_D'] >= -1.2) & (df['VWAP_D'] <= -0.3), 'Score'] += 30
    df.loc[df['BW'] < df['BW_SMA'], 'Score'] += 30
    avg_vol = df['Volume'].mean()
    df.loc[df['Volume'] > (avg_vol * 1.2), 'Score'] += 20
    df.loc[(df['RSI'] >= 40) & (df['RSI'] <= 55), 'Score'] += 20
    
    if use_guards:
        macd_prev = df['MACD_Hist'].shift(1)
        df.loc[(df['MACD_Hist'] < 0) & (df['MACD_Hist'] < macd_prev), 'Score'] -= 50
        df.loc[df['Stoch_K'] < 15, 'Score'] -= 30
        df.loc[df['EMA_Slope'] < -0.1, 'Score'] -= 20

    df.loc[df['RSI'] < 35, 'Score'] -= 40
    df.loc[df['VWAP_D'] < -1.5, 'Score'] -= 50
    return df

def run_simulation(symbol, use_guards=True):
    cache_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'sniper_cache'))
    filepath = os.path.join(cache_dir, f"{symbol}_1d_1m.csv")
    if not os.path.exists(filepath): return []
    
    df = pd.read_csv(filepath, index_col=0, parse_dates=True)
    df = calculate_indicators(df, use_guards=use_guards)
    
    trades = []
    in_position = False
    entry_price = 0
    entry_time = None
    peak_price = 0
    tsl_price = 0
    reached_target_1 = False
    last_exit_time = None
    peak_momentum = 0
    reached_breakout = False
    
    for i in range(20, len(df)):
        curr = df.iloc[i]
        score = curr['Score']
        curr_price = curr['Close']
        ist_time = df.index[i]
        
        in_cooldown = False
        if last_exit_time and (ist_time - last_exit_time).total_seconds() / 60 < 15:
            in_cooldown = True

        if not in_position and not in_cooldown and score >= 80:
            in_position = True
            entry_price = curr_price
            entry_time = ist_time
            peak_price = curr_price
            tsl_price = entry_price * 0.995
            reached_target_1 = False
            reached_breakout = False
            peak_momentum = 0
            
        elif in_position:
            profit_pct = (curr_price - entry_price) / entry_price * 100
            
            if not reached_target_1 and profit_pct >= 1.0:
                rsi_slope = curr['RSI_Slope']
                vol_r = curr['Volume'] / df['Volume'].rolling(20).mean().iloc[i]
                if rsi_slope < 5.5 and vol_r < 2.5:
                    reached_target_1 = True
                else:
                    trades.append({'symbol': symbol, 'entry': entry_time, 'exit': ist_time, 'pnl': profit_pct})
                    in_position = False
                    last_exit_time = ist_time
            
            if in_position and reached_target_1:
                if curr_price > peak_price: peak_price = curr_price
                if curr['RSI_Slope'] > peak_momentum: peak_momentum = curr['RSI_Slope']
                if profit_pct > 2.0 and peak_momentum > 4.0: reached_breakout = True
                
                gap_pct = 1.0 if reached_breakout else 0.5
                new_tsl = peak_price * (1 - gap_pct/100)
                if new_tsl > tsl_price: tsl_price = new_tsl

            if in_position and curr_price <= tsl_price:
                trades.append({'symbol': symbol, 'entry': entry_time, 'exit': ist_time, 'pnl': profit_pct})
                in_position = False
                last_exit_time = ist_time
    return trades

def audit():
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))
    Session = sessionmaker(bind=engine)
    session = Session()
    snipers = session.query(Ticker).filter(Ticker.oracle_status == 'SNIPER').all()
    symbols = [s.symbol for s in snipers]
    session.close()

    print("\n🕵️ AUDITING FILTERED TRADES: APEX vs GOLDEN FUSION")
    print("="*60)
    
    all_apex_trades = []
    all_fusion_trades = []
    
    for s in symbols:
        all_apex_trades.extend(run_simulation(s, use_guards=False))
        all_fusion_trades.extend(run_simulation(s, use_guards=True))
    
    # Identify removed trades
    # We use (symbol, entry_time) as a unique trade identifier
    fusion_trade_ids = set((t['symbol'], t['entry']) for t in all_fusion_trades)
    removed_trades = [t for t in all_apex_trades if (t['symbol'], t['entry']) not in fusion_trade_ids]
    
    print(f"Total Apex Trades (No Guards): {len(all_apex_trades)}")
    print(f"Total Fusion Trades (Guarded): {len(all_fusion_trades)}")
    print(f"Total Removed Trades        : {len(removed_trades)}")
    print("-" * 60)
    
    cat = {"Winners (>0.5%)": 0, "Fake-Outs (Losses)": 0, "Noise (BE)": 0}
    cat_v = {"Winners (>0.5%)": 0.0, "Fake-Outs (Losses)": 0.0, "Noise (BE)": 0.0}
    
    print(f"{'Symbol':<12} | {'Time':<10} | {'PnL%':<7} | {'Category'}")
    print("-" * 60)
    
    for t in sorted(removed_trades, key=lambda x: x['pnl']):
        pnl = t['pnl']
        time_str = t['entry'].strftime('%H:%M')
        
        category = "Noise (BE)"
        if pnl > 0.5: category = "Winners (>0.5%)"
        elif pnl < -0.1: category = "Fake-Outs (Losses)"
        
        cat[category] += 1
        cat_v[category] += pnl
        
        print(f"{t['symbol']:<12} | {time_str:<10} | {pnl:>+6.2f}% | {category}")

    print("\n" + "="*60)
    print("SUMMARY OF REMOVED TRADES")
    print("-" * 60)
    for c, count in cat.items():
        avg_pnl = cat_v[c] / count if count > 0 else 0
        print(f"{c:<18}: {count:<3} trades | Net PnL: {cat_v[c]:>+7.2f}% | Avg: {avg_pnl:>+6.2f}%")
    
    total_saved_pnl = cat_v["Fake-Outs (Losses)"] + cat_v["Noise (BE)"]
    print("-" * 60)
    print(f"TOTAL LOSSES PREVENTED: {abs(total_saved_pnl):.2f}%")
    print(f"SURVIVING ROI IMPACT  : {cat_v['Winners (>0.5%)']:+.2f}%")

if __name__ == "__main__":
    audit()
