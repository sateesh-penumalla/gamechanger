
import pandas as pd
import glob
import os
import json
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

# Configuration
target_dates = ["2026-03-09", "2026-03-10", "2026-03-12", "2026-03-16"]
data_dir = "/Users/sateeshbabu/fractionalcto/GameChanger copy/data/ticks"
magic_json_path = "/Users/sateeshbabu/fractionalcto/GameChanger copy/magic_volume.json"

def run_sniper_sim(symbol, file_path, target_date, magic_data):
    try:
        symbol_magic = magic_data.get(symbol)
        if not symbol_magic: return []
        
        v1m_limit = symbol_magic.get('high_conviction_magic')
        v3m_limit = symbol_magic.get('high_conviction_magic_3m')
        if not v1m_limit or not v3m_limit: return []

        df = pd.read_parquet(file_path)
        if df.empty: return []
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Resample to 1 minute bars
        mdf = df.resample('1min', on='timestamp').agg({
            'ltp': ['first', 'max', 'min', 'last'], 
            'volume': ['min', 'max'],
            'buy_vol': 'last',
            'sell_vol': 'last'
        })
        mdf.columns = ['open', 'high', 'low', 'close', 'v_min', 'v_max', 'buy_session', 'sell_session']
        mdf = mdf.dropna()
        
        mdf['vol_1m'] = mdf['v_max'] - mdf['v_min']
        mdf['vol_3m'] = mdf['vol_1m'].rolling(window=3).sum()
        mdf['vol_5m'] = mdf['vol_1m'].rolling(window=5).sum()
        
        # Correctly calc buy/sell deltas for windows
        mdf['buy_delta'] = mdf['buy_session'].diff().fillna(0)
        mdf['sell_delta'] = mdf['sell_session'].diff().fillna(0)
        mdf['buy_vol_5m'] = mdf['buy_delta'].rolling(window=5).sum()
        mdf['sell_vol_5m'] = mdf['sell_delta'].rolling(window=5).sum()
        
        # Efficiency and Aggression
        mdf['aggression'] = mdf['buy_vol_5m'] / (mdf['sell_vol_5m'] + 1)
        mdf['price_change_5m'] = ((mdf['close'] / mdf['open'].shift(4)) - 1) * 100
        mdf['efficiency'] = mdf['price_change_5m'] / ((mdf['vol_5m'] / 1000000.0) + 0.001)
        
        # Pulses
        pulse_thresh = v1m_limit * 0.3
        mdf['is_pulse'] = mdf['vol_1m'] >= pulse_thresh
        mdf['pulses_15m'] = mdf['is_pulse'].rolling(window=15).sum()
        
        closes = mdf['close'].values
        opens = mdf['open'].values
        highs = mdf['high'].values
        lows = mdf['low'].values
        v1m = mdf['vol_1m'].values
        v3m = mdf['vol_3m'].values
        eff = mdf['efficiency'].values
        agg = mdf['aggression'].values
        pulses = mdf['pulses_15m'].values
        times = mdf.index
        
        all_trades = []
        active = None
        target = 1.0
        sl = 2.0
        
        for k in range(15, len(mdf)): # Start after 15m for pulses
            idx = times[k]
            if idx.hour == 9 and idx.minute <= 16: continue
            
            if active:
                pnl = (highs[k] - active['p']) / active['p'] * 100 if active['side'] == 'LONG' else (active['p'] - lows[k]) / active['p'] * 100
                loss = (active['p'] - lows[k]) / active['p'] * 100 if active['side'] == 'LONG' else (highs[k] - active['p']) / active['p'] * 100
                
                if pnl >= target: 
                    all_trades.append({'Symbol': symbol, 'Date': target_date, 'Side': active['side'], 'Entry Time': active['t'].strftime('%H:%M %p'), 'PnL': f"+{target}%", 'Result': '✅ Target Hit'})
                    active = None
                elif loss >= sl: 
                    all_trades.append({'Symbol': symbol, 'Date': target_date, 'Side': active['side'], 'Entry Time': active['t'].strftime('%H:%M %p'), 'PnL': f"-{sl}%", 'Result': '❌ Stop Loss Hit'})
                    active = None
                elif idx.hour == 15 and idx.minute >= 20: 
                    active = None
                continue

            hit_1m = v1m[k] >= v1m_limit
            hit_3m = v3m[k] >= v3m_limit
            
            if hit_1m or hit_3m:
                # APPLY HOLY GRAIL ALPHA FILTERS
                is_eff = (eff[k] > 0.6) or (eff[k] < -5.0)
                is_agg = (agg[k] >= 4.0 and agg[k] <= 35.0) or (agg[k] <= 0.10)
                is_pulse = pulses[k] >= 3
                
                if is_eff and is_agg and is_pulse:
                    side = 'LONG' if eff[k] > 0 else 'SHORT'
                    active = {'side': side, 'p': closes[k], 't': idx}
                    
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

    print(f"Running Alpha-Filtered Backtest (98.6th Pct + Efficiency + Aggression + Pulses)...")
    
    all_trade_logs = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_sniper_sim, symbol, p, d, magic_data): (symbol, p, d) for symbol, p, d in task_list}
        for future in as_completed(futures):
            res = future.result()
            if res: all_trade_logs.extend(res)

    if all_trade_logs:
        results_df = pd.DataFrame(all_trade_logs)
        print("\n--- Final High-Conviction Results ---")
        print(results_df.to_string(index=False))
        total = len(results_df)
        wins = len(results_df[results_df['Result'] == '✅ Target Hit'])
        wr = (wins / total) * 100 if total > 0 else 0
        print(f"\nFinal Win Rate: {wr:.2f}% | Total Trades: {total}")
    else:
        print("No high-conviction trades found.")
