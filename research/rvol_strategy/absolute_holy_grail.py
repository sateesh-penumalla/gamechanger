
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
            'buy_vol': 'last',
            'sell_vol': 'last',
            'vwap': 'last'
        })
        mdf.columns = ['open', 'high', 'low', 'close', 'v_min', 'v_max', 'buy_session', 'sell_session', 'vwap']
        mdf = mdf.dropna()
        mdf['vol'] = mdf['v_max'] - mdf['v_min']
        mdf['vol_5m'] = mdf['vol'].rolling(window=5).sum()
        
        mdf['buy_delta'] = mdf['buy_session'].diff().fillna(0)
        mdf['sell_delta'] = mdf['sell_session'].diff().fillna(0)
        mdf['buy_vol_5m'] = mdf['buy_delta'].rolling(window=5).sum()
        mdf['sell_vol_5m'] = mdf['sell_delta'].rolling(window=5).sum()
        mdf['aggression'] = mdf['buy_vol_5m'] / (mdf['sell_vol_5m'] + 1)
        
        mdf['vwap_dist'] = ((mdf['close'] / mdf['vwap']) - 1) * 100
        
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
                elif active['max_p'] >= 0.4 and pnl <= 0.05: # PROTECTIVE EXIT (Even earlier protection)
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'PnL': pnl, 'Result': '🛡️ PROTECTED'})
                    active = None
                elif loss >= 2.0:
                    all_trades.append({'Date': target_date, 'Symbol': symbol, 'PnL': -2.0, 'Result': '❌ LOSS'})
                    active = None
                elif idx.hour == 15 and idx.minute >= 25:
                    active = None
                continue

            # ABSOLUTE ELITE SIGNAL
            hit_vol = (row['vol'] >= v1m_limit)
            if hit_vol:
                move_5m = ((row['close'] / mdf['open'].shift(4).loc[idx]) - 1) * 100
                efficiency = move_5m / ((row['vol_5m'] / 1000000.0) + 0.001)
                
                # THE HOLY GRAIL FILTERS
                is_high_eff = abs(efficiency) > 1.2 # Extreme price impact
                is_high_agg = (row['aggression'] > 10.0) or (row['aggression'] < 0.05) # Extreme buying/selling
                is_safe_dist = (0.05 < row['vwap_dist'] < 0.6) if efficiency > 0 else (-0.6 < row['vwap_dist'] < -0.05)
                is_not_overextended = abs(move_5m) < 1.0 # Never chase a move already > 1%
                
                if is_high_eff and is_high_agg and is_safe_dist and is_not_overextended:
                    active = {'side': 'LONG' if efficiency > 0 else 'SHORT', 'p': row['close'], 'max_p': 0}
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
                v1 = magic_data[symbol]['high_conviction_magic']
                v3 = magic_data[symbol]['high_conviction_magic_3m']
                task_list.append((symbol, p, d, v1, v3))

    print(f"Running Absolute Holy Grail Simulation (1.2 Eff + 10 Agg + 0.6 VWAP)...")
    results = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_elite_sim, *t): t for t in task_list}
        for future in as_completed(futures):
            res = future.result()
            if res: results.extend(res)

    if results:
        df = pd.DataFrame(results)
        print(f"\nFinal Report for 4 days:")
        print(f"Total Trades: {len(df)} (Avg: {len(df)/4:.1f}/day)")
        win_rate = len(df[df['Result'].isin(['✅ WIN', '🛡️ PROTECTED'])]) / len(df) * 100
        print(f"Win Rate: {win_rate:.1f}%")
        print(f"Losses: {len(df[df['Result'] == '❌ LOSS'])}")
        print("\n--- SAMPLE TRADES ---")
        print(df.head(15).to_string(index=False))
    else:
        print("No trades found.")
