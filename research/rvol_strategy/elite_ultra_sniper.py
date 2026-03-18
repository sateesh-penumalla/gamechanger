
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

def run_elite_sim(symbol, file_path, target_date, magic_data):
    try:
        # Move to 99.5th Percentile (Extreme Precision)
        v1m_limit = magic_data[symbol]['high_conviction_magic'] * 1.25
        
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
        mdf['vol_5m'] = mdf['vol'].rolling(window=5).sum()
        mdf['vwap_dist'] = ((mdf['close'] / mdf['vwap']) - 1) * 100
        
        # 15-min Trend
        mdf['trend_15m'] = ((mdf['close'] / mdf['open'].shift(14)) - 1) * 100
        
        all_trades = []
        active = None
        
        for idx, row in mdf.iterrows():
            if idx.time() < time(9, 17): continue
            
            if active:
                pnl = (row['high'] - active['p']) / active['p'] * 100 if active['side'] == 'LONG' else (active['p'] - row['low']) / active['p'] * 100
                loss = (active['p'] - row['low']) / active['p'] * 100 if active['side'] == 'LONG' else (row['high'] - active['p']) / active['p'] * 100
                
                if pnl > active['max_p']: active['max_p'] = pnl
                
                if pnl >= 1.0:
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'PnL': 1.0, 'Result': '✅ WIN', 'Entry': active['t'].strftime('%H:%M')})
                    active = None
                elif active['max_p'] >= 0.25 and pnl <= 0.02: # ULTRA DEFENSIVE PROTECTOR
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'PnL': pnl, 'Result': '🛡️ PROTECTED', 'Entry': active['t'].strftime('%H:%M')})
                    active = None
                elif loss >= 2.0:
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'PnL': -2.0, 'Result': '❌ LOSS', 'Entry': active['t'].strftime('%H:%M')})
                    active = None
                elif idx.hour == 15 and idx.minute >= 25:
                    active = None
                continue

            # THE ELITE ULTRA TRIGGER
            if row['vol'] >= v1m_limit:
                move_5m = ((row['close'] / mdf['open'].shift(4).loc[idx]) - 1) * 100
                eff = move_5m / ((row['vol_5m'] / 1000000.0) + 0.001)
                
                # ELITE FILTERS
                is_high_eff = abs(eff) > 1.0 # Very clean move
                is_safe_vwap = (0.1 < row['vwap_dist'] < 0.5) if eff > 0 else (-0.5 < row['vwap_dist'] < -0.1)
                is_trend_aligned = (row['trend_15m'] > 0.1) if eff > 0 else (row['trend_15m'] < -0.1)
                is_not_spike = abs(row['high'] - row['low']) / row['open'] * 100 < 0.8 # prevent wick-top entries
                
                if is_high_eff and is_safe_vwap and is_trend_aligned and is_not_spike:
                    active = {'side': 'LONG' if eff > 0 else 'SHORT', 'p': row['close'], 'max_p': 0, 't': idx}
                    
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
                task_list.append((symbol, p, d))

    print(f"Running 'Elite Ultra' Protocol Simulation (99.5th + Trend + Wick Filter + 0.25 BE)...")
    results = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_elite_sim, s, p, d, magic_data): s for s, p, d in task_list}
        for future in as_completed(futures):
            res = future.result()
            if res: results.extend(res)

    if results:
        df = pd.DataFrame(results)
        df = df.sort_values(['Date', 'Entry'])
        print(f"\nFinal Elite Report for 4 days:")
        print(f"Total Trades: {len(df)} (Avg: {len(df)/4:.1f}/day)")
        win_rate = len(df[df['Result'].isin(['✅ WIN', '🛡️ PROTECTED'])]) / len(df) * 100
        print(f"Win Rate: {win_rate:.1f}%")
        print(f"Losses: {len(df[df['Result'] == '❌ LOSS'])}")
        print("\n--- ELITE TRADE LIST ---")
        print(df.to_string(index=False))
    else:
        print("No trades found.")
