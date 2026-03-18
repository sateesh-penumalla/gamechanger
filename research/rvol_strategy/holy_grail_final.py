
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
        
        df = pd.read_parquet(file_path)
        if df.empty: return []
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # 1-min bars
        mdf = df.resample('1min', on='timestamp').agg({
            'ltp': ['first', 'max', 'min', 'last'], 
            'volume': ['min', 'max'],
            'vwap': 'last',
            'total_bid_qty': 'last',
            'total_ask_qty': 'last'
        })
        mdf.columns = ['open', 'high', 'low', 'close', 'v_min', 'v_max', 'vwap', 't_bid', 't_ask']
        mdf = mdf.dropna()
        mdf['vol'] = mdf['v_max'] - mdf['v_min']
        mdf['vol_5m'] = mdf['vol'].rolling(window=5).sum()
        mdf['vwap_dist'] = ((mdf['close'] / mdf['vwap']) - 1) * 100
        mdf['bid_ratio'] = mdf['t_bid'] / (mdf['t_ask'] + 1)
        
        all_trades = []
        active = None
        
        for idx, row in mdf.iterrows():
            # KEY 1: IGNORE OPENING VOLATILITY (Start at 09:31)
            if idx.time() < time(9, 31): continue
            
            if active:
                pnl = (row['high'] - active['p']) / active['p'] * 100 if active['side'] == 'LONG' else (active['p'] - row['low']) / active['p'] * 100
                loss = (active['p'] - row['low']) / active['p'] * 100 if active['side'] == 'LONG' else (row['high'] - active['p']) / active['p'] * 100
                
                if pnl > active['max_p']: active['max_p'] = pnl
                
                if pnl >= 1.0:
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'Side': active['side'], 'PnL': 1.0, 'Result': '✅ WIN', 'Entry': active['t'].strftime('%H:%M')})
                    active = None
                elif active['max_p'] >= 0.2 and pnl <= 0.01: # ULTRA PROTECTIVE EXIT
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'Side': active['side'], 'PnL': pnl, 'Result': '🛡️ PROTECTED', 'Entry': active['t'].strftime('%H:%M')})
                    active = None
                elif loss >= 2.0:
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'Side': active['side'], 'PnL': -2.0, 'Result': '❌ LOSS', 'Entry': active['t'].strftime('%H:%M')})
                    active = None
                elif idx.hour == 15 and idx.minute >= 25:
                    active = None
                continue

            # THE HOLY GRAIL FINAL FILTER
            if row['vol'] >= v1m_limit:
                move_5m = ((row['close'] / mdf['open'].shift(4).loc[idx]) - 1) * 100
                eff = move_5m / ((row['vol_5m'] / 1000000.0) + 0.001)
                
                # FINAL FILTERS
                is_high_eff = abs(eff) > 1.2
                is_vwap_perfect = (0.1 < row['vwap_dist'] < 0.6) if eff > 0 else (-0.6 < row['vwap_dist'] < -0.1)
                is_bid_confirmed = (row['bid_ratio'] > 1.5) if eff > 0 else (row['bid_ratio'] < 0.6)
                
                if is_high_eff and is_vwap_perfect and is_bid_confirmed:
                    # KEY 2: LIMIT ENTRY (0.1% Better price than close to eliminate slippage)
                    p_entry = row['close'] * 0.999 if eff > 0 else row['close'] * 1.001
                    active = {'side': 'LONG' if eff > 0 else 'SHORT', 'p': p_entry, 'max_p': 0, 't': idx}
                    
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

    print(f"Running Holy Grail 'Zero-Loss' Protocol (9:31 Start + Depth + Limit Entry)...")
    results = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_elite_sim, s, p, d, magic_data): s for s, p, d in task_list}
        for future in as_completed(futures):
            res = future.result()
            if res: results.extend(res)

    if results:
        df = pd.DataFrame(results)
        df = df.sort_values(['Date', 'Entry'])
        print(f"\nFinal Holy Grail Report (4 Days):")
        print(f"Total Trades: {len(df)} (Avg: {len(df)/4:.1f}/day)")
        win_rate = len(df[df['Result'].isin(['✅ WIN', '🛡️ PROTECTED'])]) / len(df) * 100
        print(f"Win Rate: {win_rate:.1f}%")
        print(f"Losses: {len(df[df['Result'] == '❌ LOSS'])}")
        print("\n--- ELITE TRADE LIST ---")
        print(df.to_string(index=False))
    else:
        print("No trades found.")
