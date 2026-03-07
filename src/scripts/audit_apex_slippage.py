import os
import sys
import pandas as pd
import numpy as np
from datetime import timedelta
from dotenv import load_dotenv

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

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
    df['Score'] = 0
    df.loc[(df['VWAP_D'] >= -1.2) & (df['VWAP_D'] <= -0.3), 'Score'] += 30
    df.loc[df['BW'] < df['BW_SMA'], 'Score'] += 30
    avg_vol = df['Volume'].mean()
    df.loc[df['Volume'] > (avg_vol * 1.2), 'Score'] += 20
    df.loc[(df['RSI'] >= 40) & (df['RSI'] <= 55), 'Score'] += 20
    df.loc[df['RSI'] < 35, 'Score'] -= 40
    df.loc[df['VWAP_D'] < -1.5, 'Score'] -= 50
    return df

def audit_slippage(symbol):
    cache_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'sniper_cache'))
    filepath = os.path.join(cache_dir, f"{symbol}_1d_1m.csv")
    if not os.path.exists(filepath): return []
    df = pd.read_csv(filepath, index_col=0, parse_dates=True)
    if df.empty: return []
    df = calculate_indicators(df)
    
    audit_log = []
    in_position = False
    entry_price = 0
    peak_price = 0
    tsl_price = 0
    is_trailing = False
    last_exit_time = None
    
    for i in range(20, len(df)):
        curr = df.iloc[i]
        score = curr['Score']
        curr_price = curr['Close']
        ist_time = df.index[i] + timedelta(hours=5, minutes=30)
        
        # Cool-down
        if last_exit_time and (ist_time - last_exit_time).total_seconds() / 60 < 15:
            continue

        if not in_position and score >= 80:
            in_position = True
            entry_price = curr_price
            peak_price = curr_price
            tsl_price = entry_price * 0.995 # Hard Stop
            is_trailing = False
            
        elif in_position:
            profit_pct = (curr_price - entry_price) / entry_price * 100
            
            if not is_trailing and profit_pct >= 1.0:
                rsi_slope = curr['RSI_Slope']
                vol_r = curr['Volume'] / df['Volume'].rolling(20).mean().iloc[i]
                
                if rsi_slope < 5.5 and vol_r < 2.5:
                    is_trailing = True
                    tsl_price = entry_price * 1.005 # Floor at 0.5%
                else:
                    # Direct Exit at 1%
                    audit_log.append({"symbol": symbol, "type": "DIRECT_TP", "pnl": profit_pct, "giveback": 0})
                    in_position = False
                    last_exit_time = ist_time
                    
            elif is_trailing:
                if curr_price > peak_price:
                    peak_price = curr_price
                    new_tsl = peak_price * 0.995
                    if new_tsl > tsl_price: tsl_price = new_tsl
                    
                if curr_price <= tsl_price:
                    # Giveback calculation: Peak PnL vs Final PnL
                    peak_pnl = (peak_price - entry_price) / entry_price * 100
                    giveback = peak_pnl - profit_pct
                    audit_log.append({"symbol": symbol, "type": "TSL_EXIT", "pnl": profit_pct, "peak_pnl": peak_pnl, "giveback": giveback})
                    in_position = False
                    last_exit_time = ist_time
            
            # Hard stop if not trailing
            elif profit_pct <= -0.5:
                in_position = False
                last_exit_time = ist_time

    return audit_log

def main():
    load_dotenv()
    from src.db.schema import Ticker
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(os.getenv("DATABASE_URL"))
    Session = sessionmaker(bind=engine)
    session = Session()
    snipers = session.query(Ticker).filter(Ticker.oracle_status == 'SNIPER').all()
    symbols = [s.symbol for s in snipers]
    session.close()

    logs = []
    for s in symbols:
        logs.extend(audit_slippage(s))

    df = pd.DataFrame(logs)
    if df.empty:
        print("No Apex-milestone trades found.")
        return

    tsl_trades = df[df['type'] == 'TSL_EXIT']
    direct_tp = df[df['type'] == 'DIRECT_TP']

    print("\n🔍 APEX MILESTONE AUDIT (Trades that passed 1% mark)")
    print("="*80)
    print(f"Total Milestone Trades: {len(df)}")
    print(f"Direct Take Profits (at ~1%): {len(direct_tp)}")
    print(f"Holding for Runaway (TSL Exits): {len(tsl_trades)}")
    print("-" * 80)
    
    if not tsl_trades.empty:
        # Categorize TSL Exits
        winners = tsl_trades[tsl_trades['pnl'] > 1.0] # These ended higher than Passive target!
        reverters = tsl_trades[tsl_trades['pnl'] <= 1.0] # These gave back profit
        
        print(f"TSL Success (Ended > 1%): {len(winners)}")
        print(f"TSL Slippage (Ended <= 1%): {len(reverters)}  <-- These hit SL after 1%")
        print(f"\nAverage Giveback on Slippage: {reverters['giveback'].mean():.2f}%")
        print(f"Total Profit 'Sacrificed' in Reverters: {reverters['giveback'].sum():.2f}%")
        print(f"Total Profit 'Gained' in Extra Run: {(winners['pnl'] - 1.0).sum():.2f}%")
        print("-" * 80)
        print("\n📜 LIST OF REVERTER TRADES (Hit 1% then slipped):")
        print(reverters[['symbol', 'peak_pnl', 'pnl', 'giveback']].to_string(index=False))
        print("-" * 80)
        print(f"NET APEX GAIN FROM HOLDING: {((winners['pnl'] - 1.0).sum() - reverters['giveback'].sum()):+.2f}%")

if __name__ == "__main__":
    main()
