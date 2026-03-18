
import pandas as pd
import glob
import os
import json
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

# Configuration
target_dates = ["2026-03-09", "2026-03-10", "2026-03-12"]
data_dir = "/Users/sateeshbabu/fractionalcto/GameChanger copy/data/ticks"
magic_json_path = "/Users/sateeshbabu/fractionalcto/GameChanger copy/magic_volume.json"
output_report = "/Users/sateeshbabu/fractionalcto/GameChanger copy/research/rvol_strategy/holy_grail_replication_report.csv"

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
        
        # Resample to 1 minute bars to match accumulation_sniper_fixed.py
        mdf = df.resample('1min', on='timestamp').agg({'ltp': ['first', 'max', 'min', 'last'], 'volume': ['min', 'max']})
        mdf.columns = ['open', 'high', 'low', 'close', 'v_min', 'v_max']
        mdf = mdf.dropna()
        mdf['vol_1m'] = mdf['v_max'] - mdf['v_min']
        mdf['vol_3m'] = mdf['vol_1m'].rolling(window=3).sum()
        
        closes = mdf['close'].values
        opens = mdf['open'].values
        highs = mdf['high'].values
        lows = mdf['low'].values
        v1m = mdf['vol_1m'].values
        v3m = mdf['vol_3m'].values
        times = mdf.index
        
        all_trades = []
        active = None
        target = 1.0
        sl = 2.0
        
        for k in range(3, len(mdf)):
            idx = times[k]
            
            # Match 9:16 guard
            if idx.hour == 9 and idx.minute <= 16:
                continue
                
            if active:
                pnl = (highs[k] - active['p']) / active['p'] * 100 if active['side'] == 'LONG' else (active['p'] - lows[k]) / active['p'] * 100
                loss = (active['p'] - lows[k]) / active['p'] * 100 if active['side'] == 'LONG' else (highs[k] - active['p']) / active['p'] * 100
                
                if pnl >= target: 
                    all_trades.append({
                        'Symbol': symbol, 'Date': target_date, 'Side': active['side'], 
                        'Entry Time': idx.strftime('%H:%M %p'), 'Entry Price': active['p'], 
                        'Exit Price': highs[k] if active['side']=='LONG' else lows[k], 
                        'PnL': f"+{target}%", 'Result': '✅ Target Hit'
                    })
                    active = None
                elif loss >= sl: 
                    all_trades.append({
                        'Symbol': symbol, 'Date': target_date, 'Side': active['side'], 
                        'Entry Time': idx.strftime('%H:%M %p'), 'Entry Price': active['p'], 
                        'Exit Price': lows[k] if active['side']=='LONG' else highs[k], 
                        'PnL': f"-{sl}%", 'Result': '❌ Stop Loss Hit'
                    })
                    active = None
                elif idx.hour == 15 and idx.minute >= 20: 
                    final_pnl = (closes[k] - active['p']) / active['p'] * 100 if active['side'] == 'LONG' else (active['p'] - closes[k]) / active['p'] * 100
                    all_trades.append({
                        'Symbol': symbol, 'Date': target_date, 'Side': active['side'], 
                        'Entry Time': idx.strftime('%H:%M %p'), 'Entry Price': active['p'], 
                        'Exit Price': closes[k], 
                        'PnL': f"{final_pnl:.2f}%", 'Result': '⏹️ EOD Exit'
                    })
                    active = None
                continue

            hit_1m = v1m[k] >= v1m_limit
            hit_3m = v3m[k] >= v3m_limit

            if hit_1m or hit_3m:
                if hit_1m:
                    move = (closes[k] - opens[k]) / opens[k] * 100
                else:
                    move = (closes[k] - opens[k-2]) / opens[k-2] * 100
                    
                if abs(move) >= 0.15:
                    active = {'side': 'LONG' if move > 0 else 'SHORT', 'p': closes[k], 't': idx}
        return all_trades
    except Exception as e:
        return []

if __name__ == "__main__":
    # Load Magic Numbers
    with open(magic_json_path, "r") as f:
        magic_data = json.load(f)

    task_list = []
    symbol_dirs = glob.glob(os.path.join(data_dir, "*"))
    for d in target_dates:
        for sdir in symbol_dirs:
            symbol = os.path.basename(sdir)
            p = os.path.join(sdir, f"{d}.parquet")
            if os.path.exists(p): task_list.append((symbol, p, d))

    print(f"Running Final Replication Backtest...")
    
    all_trade_logs = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_sniper_sim, symbol, p, d, magic_data): (symbol, p, d) for symbol, p, d in task_list}
        for future in as_completed(futures):
            res = future.result()
            if res: all_trade_logs.extend(res)

    if all_trade_logs:
        results_df = pd.DataFrame(all_trade_logs)
        results_df.to_csv(output_report, index=False)
        print("\n--- Backtest Results (Mar 9, 10, 12) ---")
        print(results_df.to_string(index=False))
        
        total = len(results_df)
        wins = len(results_df[results_df['Result'] == '✅ Target Hit'])
        wr = (wins / total) * 100 if total > 0 else 0
        print(f"\nFinal Win Rate: {wr:.2f}% | Total Trades: {total} | Avg: {total/3:.1f}/day")
    else:
        print("No sniper signals found.")
