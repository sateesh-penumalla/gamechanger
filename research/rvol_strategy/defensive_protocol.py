
import pandas as pd
import glob
import os
import json
import numpy as np
from datetime import datetime, time
from concurrent.futures import ProcessPoolExecutor, as_completed

# Configuration
target_dates = ["2026-03-09", "2026-03-10", "2026-03-12", "2026-03-16"]
data_dir = "/Users/sateeshbabu/fractionalcto/GameChanger copy/data/ticks"
magic_json_path = "/Users/sateeshbabu/fractionalcto/GameChanger copy/magic_volume.json"

def run_elite_sim(symbol, file_path, target_date, v1m_limit, v3m_limit):
    try:
        df = pd.read_parquet(file_path)
        if df.empty: return []
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # 1-min bars
        mdf = df.resample('1min', on='timestamp').agg({
            'ltp': ['first', 'max', 'min', 'last'], 
            'volume': ['min', 'max'],
            'vwap': 'last'
        })
        mdf.columns = ['open', 'high', 'low', 'close', 'v_min', 'v_max', 'vwap']
        mdf = mdf.dropna()
        mdf['vol'] = mdf['v_max'] - mdf['v_min']
        mdf['vol_3m'] = mdf['vol'].rolling(3).sum()
        mdf['vwap_dist'] = ((mdf['close'] / mdf['vwap']) - 1) * 100
        
        all_trades = []
        active = None
        
        for idx, row in mdf.iterrows():
            if idx.time() < time(9, 17): continue
            
            if active:
                pnl = (row['high'] - active['p']) / active['p'] * 100 if active['side'] == 'LONG' else (active['p'] - row['low']) / active['p'] * 100
                loss = (active['p'] - row['low']) / active['p'] * 100 if active['side'] == 'LONG' else (row['high'] - active['p']) / active['p'] * 100
                
                if pnl > active['max_p']: active['max_p'] = pnl
                
                # 100% Win Strategy Exits:
                if pnl >= 1.0:
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'PnL': 1.0, 'Result': '✅ WIN'})
                    active = None
                elif active['max_p'] >= 0.5 and pnl <= 0.05: # PROTECTIVE EXIT
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'PnL': pnl, 'Result': '🛡️ PROTECTED'})
                    active = None
                elif loss >= 2.0:
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'PnL': -2.0, 'Result': '❌ LOSS'})
                    active = None
                elif idx.hour == 15 and idx.minute >= 25:
                    active = None
                continue

            # Signal
            hit_1m = row['vol'] >= v1m_limit
            hit_3m = row['vol_3m'] >= v3m_limit
            
            if hit_1m or hit_3m:
                move = (row['close'] - row['open']) / row['open'] * 100
                if abs(move) >= 0.15:
                    # VWAP GUARD (The Holy Grail Secret)
                    # Don't chase if already far from VWAP
                    vwap_ok = (0.1 < row['vwap_dist'] < 0.8) if move > 0 else (-0.8 < row['vwap_dist'] < -0.1)
                    
                    if vwap_ok:
                        all_recent = mdf.loc[:idx].tail(5)
                        efficiency = move / ((all_recent['vol'].sum() / 1000000.0) + 0.001)
                        
                        if abs(efficiency) > 0.5:
                            active = {'side': 'LONG' if move > 0 else 'SHORT', 'p': row['close'], 'max_p': 0}
        return all_trades
    except: return []

if __name__ == "__main__":
    with open(magic_json_path, "r") as f:
        magic_data = json.load(f)

    task_list = []
    symbol_dirs = glob.glob(os.path.join(data_dir, "*"))
    for d in target_dates:
        for sdir in symbol_dirs:
            symbol = os.path.basename(sdir)
            p = os.path.join(sdir, f"{d}.parquet")
            if os.path.exists(p) and symbol in magic_data:
                # Use a specific percentile simulation (approx 99th)
                v1 = magic_data[symbol]['high_conviction_magic'] * 0.9 # Slightly below 99.3
                v3 = magic_data[symbol]['high_conviction_magic_3m'] * 0.9
                task_list.append((symbol, p, d, v1, v3))

    print(f"Running Defensive '100% Win' Protocol Simulation...")
    results = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_elite_sim, *t): t for t in task_list}
        for future in as_completed(futures):
            res = future.result()
            if res: results.extend(res)

    if results:
        df = pd.DataFrame(results)
        print(f"\nReport for 4 days:")
        print(f"Total Trades: {len(df)} (Avg: {len(df)/4:.1f}/day)")
        win_rate = len(df[df['Result'].isin(['✅ WIN', '🛡️ PROTECTED'])]) / len(df) * 100
        print(f"Win Rate: {win_rate:.1f}%")
        print(f"Losses: {len(df[df['Result'] == '❌ LOSS'])}")
        print(f"Profitable/BE: {len(df[df['Result'] != '❌ LOSS'])}")
        print("\n--- SAMPLE TRADES ---")
        print(df.head(10).to_string(index=False))
    else:
        print("No trades found.")
