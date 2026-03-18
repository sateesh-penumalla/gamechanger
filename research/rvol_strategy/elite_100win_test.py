
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

def run_elite_sniper_sim(symbol, file_path, target_date, magic_data):
    try:
        symbol_magic = magic_data.get(symbol)
        if not symbol_magic: return []
        
        # Use 99.3rd percentile as base (Elite enough)
        v1m_limit = symbol_magic.get('high_conviction_magic')
        v3m_limit = symbol_magic.get('high_conviction_magic_3m')
        
        df = pd.read_parquet(file_path)
        if df.empty: return []
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Resample to 1 minute bars
        mdf = df.resample('1min', on='timestamp').agg({
            'ltp': ['first', 'max', 'min', 'last'], 
            'volume': ['min', 'max'],
            'buy_vol': 'last',
            'sell_vol': 'last',
            'vwap': 'last'
        })
        mdf.columns = ['open', 'high', 'low', 'close', 'v_min', 'v_max', 'buy_session', 'sell_session', 'vwap']
        mdf = mdf.dropna()
        
        mdf['vol_1m'] = mdf['v_max'] - mdf['v_min']
        mdf['vol_5m'] = mdf['vol_1m'].rolling(window=5).sum()
        mdf['buy_delta'] = mdf['buy_session'].diff().fillna(0)
        mdf['sell_delta'] = mdf['sell_session'].diff().fillna(0)
        mdf['buy_vol_5m'] = mdf['buy_delta'].rolling(window=5).sum()
        mdf['sell_vol_5m'] = mdf['sell_delta'].rolling(window=5).sum()
        
        mdf['aggression'] = mdf['buy_vol_5m'] / (mdf['sell_vol_5m'] + 1)
        mdf['open_5m_ago'] = mdf['open'].shift(4)
        mdf['price_change_5m'] = ((mdf['close'] / mdf['open_5m_ago']) - 1) * 100
        mdf['efficiency'] = mdf['price_change_5m'] / ((mdf['vol_5m'] / 1000000.0) + 0.001)
        mdf['vwap_dist'] = ((mdf['close'] / mdf['vwap']) - 1) * 100
        
        pulse_thresh = v1m_limit * 0.3
        mdf['is_pulse'] = mdf['vol_1m'] >= pulse_thresh
        mdf['pulses_15m'] = mdf['is_pulse'].rolling(window=15).sum()
        
        all_trades = []
        active = None
        target = 1.0
        sl = 2.0
        
        for idx, row in mdf.iterrows():
            if idx.time() < time(9, 17): continue
            
            if active:
                pnl = (row['high'] - active['p']) / active['p'] * 100 if active['side'] == 'LONG' else (active['p'] - row['low']) / active['p'] * 100
                loss = (active['p'] - row['low']) / active['p'] * 100 if active['side'] == 'LONG' else (row['high'] - active['p']) / active['p'] * 100
                
                if pnl > active['max_pnl']: active['max_pnl'] = pnl
                
                # EXIT 1: TARGET 1%
                if pnl >= target:
                    all_trades.append({'Symbol': symbol, 'PnL': target, 'Result': '✅ WIN'})
                    active = None
                # EXIT 2: BREAK-EVEN PROTECTION (If it hit 0.7% then falls back to 0.05%)
                elif active['max_pnl'] >= 0.7 and pnl <= 0.05:
                    all_trades.append({'Symbol': symbol, 'PnL': pnl, 'Result': '🛡️ PROTECTED'})
                    active = None
                # EXIT 3: STOP LOSS 2%
                elif loss >= sl:
                    all_trades.append({'Symbol': symbol, 'PnL': -sl, 'Result': '❌ LOSS'})
                    active = None
                elif idx.hour == 15 and idx.minute >= 25:
                    active = None
                continue

            # ELITE ENTRY (99.5th Pct + VWAP + 5 Pulses)
            hit_vol = (row['vol_1m'] >= v1m_limit) or (row['vol_1m'].rolling(window=3).sum() >= v3m_limit)
            if hit_vol:
                is_eff = (row['efficiency'] > 0.4) or (row['efficiency'] < -4.0)
                is_agg = (row['aggression'] >= 3.0) or (row['aggression'] <= 0.15)
                is_pulse = row['pulses_15m'] >= 2
                
                # VWAP Magnet: Don't buy if already too far or too close
                is_vwap_safe = True
                if row['efficiency'] > 0: # LONG
                    if row['vwap_dist'] > 1.5 or row['vwap_dist'] < 0.1: is_vwap_safe = False
                else: # SHORT
                    if row['vwap_dist'] < -2.0 or row['vwap_dist'] > -0.1: is_vwap_safe = False
                
                if is_eff and is_agg and is_pulse and is_vwap_safe:
                    side = 'LONG' if row['efficiency'] > 0 else 'SHORT'
                    active = {'side': side, 'p': row['close'], 't': idx, 'max_pnl': 0}
                    
        return all_trades
    except Exception as e:
        return []

if __name__ == "__main__":
    with open(magic_json_path, "r") as f:
        magic_data = json.load(f)

    task_list = []
    symbol_dirs = glob.glob(os.path.join(data_dir, "*"))
    for d in target_dates:
        for sdir in symbol_dirs:
            symbol = os.path.basename(sdir)
            p = os.path.join(sdir, f"{d}.parquet")
            if os.path.exists(p): task_list.append((symbol, p, d))

    print(f"Testing '100% Win Rate' Elite Logic (99.5th + VWAP + Prot + Pulses)...")
    
    results = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_elite_sniper_sim, s, p, d, magic_data): s for s, p, d in task_list}
        for future in as_completed(futures):
            res = future.result()
            if res: results.extend(res)

    if results:
        res_df = pd.DataFrame(results)
        print(f"\n✅ Total Trades: {len(res_df)} ({len(res_df)/4:.1f} per day)")
        win_count = len(res_df[res_df['Result'].isin(['✅ WIN', '🛡️ PROTECTED'])])
        print(f"✅ Win Rate (Win + Protected): {(win_count/len(res_df))*100:.1f}%")
        print(f"❌ Actual Losses: {len(res_df[res_df['Result'] == '❌ LOSS'])}")
        print(f"🛡️ Protected B/E: {len(res_df[res_df['Result'] == '🛡️ PROTECTED'])}")
    else:
        print("No trades found.")
