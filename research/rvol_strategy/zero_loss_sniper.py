
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
        # Move to 99.7th Percentile (Absolute Top Tier)
        v1m_limit = magic_data[symbol]['high_conviction_magic'] * 1.5
        v3m_limit = magic_data[symbol]['high_conviction_magic_3m'] * 1.5
        
        df = pd.read_parquet(file_path)
        if df.empty: return []
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # 1-min bars
        mdf = df.resample('1min', on='timestamp').agg({
            'ltp': ['first', 'max', 'min', 'last'], 
            'volume': ['min', 'max'],
            'buy_vol': 'last', 'sell_vol': 'last',
            'vwap': 'last', 'bid_pct': 'last'
        })
        mdf.columns = ['open', 'high', 'low', 'close', 'v_min', 'v_max', 'buy_session', 'sell_session', 'vwap', 'bid_pct']
        mdf = mdf.dropna()
        mdf['vol'] = mdf['v_max'] - mdf['v_min']
        mdf['vol_5m'] = mdf['vol'].rolling(window=5).sum()
        mdf['vwap_dist'] = ((mdf['close'] / mdf['vwap']) - 1) * 100
        
        # Aggression
        mdf['buy_d'] = mdf['buy_session'].diff().fillna(0)
        mdf['sell_d'] = mdf['sell_session'].diff().fillna(0)
        mdf['agg'] = mdf['buy_d'].rolling(5).sum() / (mdf['sell_d'].rolling(5).sum() + 1)
        
        all_trades = []
        active = None
        
        for idx, row in mdf.iterrows():
            if idx.time() < time(9, 17): continue
            
            if active:
                pnl = (row['high'] - active['p']) / active['p'] * 100 if active['side'] == 'LONG' else (active['p'] - row['low']) / active['p'] * 100
                loss = (active['p'] - row['low']) / active['p'] * 100 if active['side'] == 'LONG' else (row['high'] - active['p']) / active['p'] * 100
                
                if pnl > active['max_p']: active['max_p'] = pnl
                
                if pnl >= 1.0:
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'PnL': 1.0, 'Result': '✅ WIN'})
                    active = None
                elif active['max_p'] >= 0.3 and pnl <= 0.02: # "Zero-Loss" Defensive Pivot
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'PnL': pnl, 'Result': '🛡️ PROTECTED'})
                    active = None
                elif loss >= 2.0:
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'PnL': -2.0, 'Result': '❌ LOSS'})
                    active = None
                elif idx.hour == 15 and idx.minute >= 25:
                    active = None
                continue

            # ZERO-LOSS SNIPER TRIGGER
            if row['vol'] >= v1m_limit:
                move_5m = ((row['close'] / mdf['open'].shift(4).loc[idx]) - 1) * 100
                eff = move_5m / ((row['vol_5m'] / 1000000.0) + 0.001)
                
                # THE 100% QUALITY FILTER
                is_elite_eff = abs(eff) > 1.5
                is_elite_agg = (row['agg'] > 15.0) or (row['agg'] < 0.03)
                is_bid_confirmed = (row['bid_pct'] > 65) if eff > 0 else (row['bid_pct'] < 35)
                # VWAP Magnet (Strict +/- 0.05% to 0.4%)
                is_near_vwap = (0.05 < row['vwap_dist'] < 0.4) if eff > 0 else (-0.4 < row['vwap_dist'] < -0.05)
                
                if is_elite_eff and is_elite_agg and is_bid_confirmed and is_near_vwap:
                    active = {'side': 'LONG' if eff > 0 else 'SHORT', 'p': row['close'], 'max_p': 0}
                    
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

    print(f"Running Zero-Loss Sniper Simulation (99.7th + BidPct + 0.3 BE)...")
    results = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_elite_sim, s, p, d, magic_data): s for s, p, d in task_list}
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
        print("\n--- SAMPLE TRADES ---")
        print(df.to_string(index=False))
    else:
        print("No trades found.")
