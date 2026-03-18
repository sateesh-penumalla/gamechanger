
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
output_csv = "/Users/sateeshbabu/fractionalcto/GameChanger copy/orc_sniper_final_trades.csv"

def run_sniper_backtest(symbol, file_path, target_date, magic_data):
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
        
        # Resample to 1-minute bars to match orc_sniper.py logic
        mdf = df.resample('1min', on='timestamp').agg({
            'ltp': ['first', 'max', 'min', 'last'], 
            'volume': ['min', 'max'],
            'buy_vol': 'last',
            'sell_vol': 'last'
        })
        mdf.columns = ['open', 'high', 'low', 'close', 'v_min', 'v_max', 'buy_session', 'sell_session']
        mdf = mdf.dropna()
        
        # Volume Deltas
        mdf['vol_1m'] = mdf['v_max'] - mdf['v_min']
        mdf['buy_delta'] = mdf['buy_session'].diff().fillna(0)
        mdf['sell_delta'] = mdf['sell_session'].diff().fillna(0)
        
        # 5-Bar Rolling Metrics (Matches sum(list(state['prev_1m_vols'])[-4:]) + current)
        mdf['vol_5m'] = mdf['vol_1m'].rolling(window=5).sum()
        mdf['buy_vol_5m'] = mdf['buy_delta'].rolling(window=5).sum()
        mdf['sell_vol_5m'] = mdf['sell_delta'].rolling(window=5).sum()
        
        # Aggression and Efficiency
        mdf['aggression'] = mdf['buy_vol_5m'] / (mdf['sell_vol_5m'] + 1)
        
        # Efficiency uses current LTP vs Open of 4 bars ago (prev_opens[-4])
        # k=4: open_5m_ago = opens[0]. This matches rolling window shifts.
        mdf['open_5m_ago'] = mdf['open'].shift(4)
        mdf['price_change_5m'] = ((mdf['close'] / mdf['open_5m_ago']) - 1) * 100
        mdf['efficiency'] = mdf['price_change_5m'] / ((mdf['vol_5m'] / 1000000.0) + 0.001)
        
        # 3-Minute Rolling Volume
        mdf['vol_3m'] = mdf['vol_1m'].rolling(window=3).sum()
        
        # Pulse Logic (Bar > 30% of v1m_limit)
        pulse_thresh = v1m_limit * 0.3
        mdf['is_pulse'] = mdf['vol_1m'] >= pulse_thresh
        mdf['pulses_15m'] = mdf['is_pulse'].rolling(window=15).sum()
        
        closes = mdf['close'].values
        highs = mdf['high'].values
        lows = mdf['low'].values
        v1m = mdf['vol_1m'].values
        v3m = mdf['vol_3m'].values
        eff = mdf['efficiency'].values
        agg = mdf['aggression'].values
        puls = mdf['pulses_15m'].values
        times = mdf.index
        
        all_trades = []
        active = None
        target = 1.0
        sl = 2.0
        
        for k in range(15, len(mdf)):
            idx = times[k]
            
            # Guard: After 09:16 AM (Starts from 09:17)
            if idx.time() < time(9, 17):
                continue
            
            if active:
                pnl = (highs[k] - active['p']) / active['p'] * 100 if active['side'] == 'LONG' else (active['p'] - lows[k]) / active['p'] * 100
                loss = (active['p'] - lows[k]) / active['p'] * 100 if active['side'] == 'LONG' else (highs[k] - active['p']) / active['p'] * 100
                
                if pnl >= target:
                    all_trades.append({
                        'Symbol': symbol, 'Date': target_date, 'Side': active['side'],
                        'Entry Time': active['t'].strftime('%H:%M'), 'Entry Price': round(active['p'], 2),
                        'Exit Time': idx.strftime('%H:%M'), 'Exit Price': round(highs[k] if active['side']=='LONG' else lows[k], 2),
                        'PnL': f"+{target}%", 'Reason': 'TARGET_HIT'
                    })
                    active = None
                elif loss >= sl:
                    all_trades.append({
                        'Symbol': symbol, 'Date': target_date, 'Side': active['side'],
                        'Entry Time': active['t'].strftime('%H:%M'), 'Entry Price': round(active['p'], 2),
                        'Exit Time': idx.strftime('%H:%M'), 'Exit Price': round(lows[k] if active['side']=='LONG' else highs[k], 2),
                        'PnL': f"-{sl}%", 'Reason': 'STOP_LOSS'
                    })
                    active = None
                elif idx.hour == 15 and idx.minute >= 25: # Square off
                    final_pnl = (closes[k] - active['p']) / active['p'] * 100 if active['side'] == 'LONG' else (active['p'] - closes[k]) / active['p'] * 100
                    all_trades.append({
                        'Symbol': symbol, 'Date': target_date, 'Side': active['side'],
                        'Entry Time': active['t'].strftime('%H:%M'), 'Entry Price': round(active['p'], 2),
                        'Exit Time': idx.strftime('%H:%M'), 'Exit Price': round(closes[k], 2),
                        'PnL': f"{final_pnl:.2f}%", 'Reason': 'EOD'
                    })
                    active = None
                continue

            # Signal Check (Matches orc_sniper.py EXACTLY)
            hit_1m = v1m[k] >= v1m_limit
            hit_3m = v3m[k] >= v3m_limit
            
            if hit_1m or hit_3m:
                is_eff = (eff[k] > 0.6) or (eff[k] < -5.0)
                is_agg = (agg[k] >= 4.0 and agg[k] <= 35.0) or (agg[k] <= 0.10)
                is_pulse = puls[k] >= 3
                
                if is_eff and is_agg and is_pulse:
                    side = "LONG" if eff[k] > 0 else "SHORT"
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

    print(f"Generating Final Trade Report for Orc Sniper Logic...")
    
    results = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_sniper_backtest, symbol, p, d, magic_data): (symbol, p, d) for symbol, p, d in task_list}
        for future in as_completed(futures):
            res = future.result()
            if res: results.extend(res)

    if results:
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values(['Date', 'Entry Time'])
        results_df.to_csv(output_csv, index=False)
        print(f"\n✅ Total Trades Generated: {len(results_df)}")
        print(f"✅ CSV Saved to: {output_csv}")
        
        # Print first few for verification
        print("\n--- SAMPLE TRADES ---")
        print(results_df.head(15).to_string(index=False))
        
        wins = len(results_df[results_df['Reason'] == 'TARGET_HIT'])
        loss = len(results_df[results_df['Reason'] == 'STOP_LOSS'])
        print(f"\nFinal Statistics:")
        print(f"Win Rate: {(wins/len(results_df))*100:.1f}%")
        print(f"Avg Trades/Day: {len(results_df)/len(target_dates):.1f}")
    else:
        print("No trades found.")
