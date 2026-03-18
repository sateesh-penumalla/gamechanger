
import pandas as pd
import numpy as np
import os
import json
from glob import glob
from multiprocessing import Pool, cpu_count
from datetime import datetime

# Path to the data
DATA_ROOT = "./data/ticks"
MAGIC_JSON = "magic_volume.json"

def run_backtest_on_parquet(symbol, dates, high_conviction_threshold):
    all_trades = []
    
    for date_str in dates:
        symbol_date_dir = os.path.join(DATA_ROOT, symbol, f"{date_str}.parquet")
        if not os.path.exists(symbol_date_dir):
            continue
            
        parquet_files = glob(os.path.join(symbol_date_dir, "*.parquet"))
        if not parquet_files:
            continue
            
        try:
            # Load all batches for the day
            dfs = [pd.read_parquet(f) for f in parquet_files]
            df = pd.concat(dfs).sort_values('timestamp')
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Resample to 1-minute OHLCV
            # We use the 'volume' column which is cumulative session volume
            minute_df = df.resample('1min', on='timestamp').agg({
                'ltp': ['first', 'max', 'min', 'last'],
                'volume': ['min', 'max']
            })
            
            # Flatten columns
            minute_df.columns = ['open', 'high', 'low', 'close', 'vol_min', 'vol_max']
            minute_df = minute_df.dropna()
            
            # Calculate 1-minute volume
            # If session volume, it's max - min
            minute_df['vol_1m'] = minute_df['vol_max'] - minute_df['vol_min']
            
            closes = minute_df['close'].values
            opens = minute_df['open'].values
            highs = minute_df['high'].values
            lows = minute_df['low'].values
            vols = minute_df['vol_1m'].values
            times = minute_df.index.values
            
            active_trade = None
            target_pct = 1.0
            sl_pct = 2.0
            
            for k in range(1, len(minute_df)):
                if active_trade:
                    # Check exit
                    if active_trade['side'] == 'LONG':
                        pnl = (highs[k] - active_trade['ent_p']) / active_trade['ent_p'] * 100
                        loss = (active_trade['ent_p'] - lows[k]) / active_trade['ent_p'] * 100
                        if pnl >= target_pct:
                            all_trades.append({'symbol': symbol, 'date': date_str, 'side': 'LONG', 'entry_time': active_trade['ent_t'], 'exit_time': times[k], 'pnl': target_pct, 'reason': 'TARGET'})
                            active_trade = None
                        elif loss >= sl_pct:
                            all_trades.append({'symbol': symbol, 'date': date_str, 'side': 'LONG', 'entry_time': active_trade['ent_t'], 'exit_time': times[k], 'pnl': -sl_pct, 'reason': 'STOP_LOSS'})
                            active_trade = None
                    else:
                        pnl = (active_trade['ent_p'] - lows[k]) / active_trade['ent_p'] * 100
                        loss = (highs[k] - active_trade['ent_p']) / active_trade['ent_p'] * 100
                        if pnl >= target_pct:
                            all_trades.append({'symbol': symbol, 'date': date_str, 'side': 'SHORT', 'entry_time': active_trade['ent_t'], 'exit_time': times[k], 'pnl': target_pct, 'reason': 'TARGET'})
                            active_trade = None
                        elif loss >= sl_pct:
                            all_trades.append({'symbol': symbol, 'date': date_str, 'side': 'SHORT', 'entry_time': active_trade['ent_t'], 'exit_time': times[k], 'pnl': -sl_pct, 'reason': 'STOP_LOSS'})
                            active_trade = None
                            
                    # EOD check (15:20)
                    if active_trade:
                        dt = pd.to_datetime(times[k])
                        if dt.hour == 15 and dt.minute >= 20:
                            final_pnl = (closes[k] - active_trade['ent_p']) / active_trade['ent_p'] * 100 if active_trade['side'] == 'LONG' else (active_trade['ent_p'] - closes[k]) / active_trade['ent_p'] * 100
                            all_trades.append({'symbol': symbol, 'date': date_str, 'side': active_trade['side'], 'entry_time': active_trade['ent_t'], 'exit_time': times[k], 'pnl': final_pnl, 'reason': 'EOD'})
                            active_trade = None
                    continue

                # TRIGGER
                if vols[k] >= high_conviction_threshold:
                    bar_move = (closes[k] - opens[k]) / opens[k] * 100
                    if abs(bar_move) < 0.1: continue
                    
                    active_trade = {
                        'side': 'LONG' if bar_move > 0 else 'SHORT',
                        'ent_p': closes[k],
                        'ent_t': times[k]
                    }
                    
        except Exception as e:
            # print(f"Error processing {symbol} on {date_str}: {e}")
            pass
            
    return all_trades

def worker_init(m_data):
    global magic_data
    magic_data = m_data

def process_symbol(symbol):
    dates = ['2026-03-09', '2026-03-10', '2026-03-12']
    hc_vol = magic_data[symbol].get('high_conviction_magic')
    if not hc_vol: return []
    return run_backtest_on_parquet(symbol, dates, hc_vol)

def main():
    with open(MAGIC_JSON, 'r') as f:
        m_data = json.load(f)
    
    # Filter symbols that actually have parquet data
    all_symbol_dirs = os.listdir(DATA_ROOT)
    symbols_to_process = [s for s in all_symbol_dirs if s in m_data]
    
    print(f"🚀 Running High Conviction Parquet Backtest (9th, 10th, 12th March) on {len(symbols_to_process)} symbols...")
    
    with Pool(cpu_count(), initializer=worker_init, initargs=(m_data,)) as p:
        results = p.map(process_symbol, symbols_to_process)
        
    flat_results = [t for sub in results for t in sub]
    df = pd.DataFrame(flat_results)
    
    if not df.empty:
        df.to_csv('high_conviction_parquet_results.csv', index=False)
        wr = (df['pnl'] > 0).mean() * 100
        print("\n--- HIGH CONVICTION PARQUET SUMMARY ---")
        print(f"Total Trades: {len(df)}")
        print(f"Win Rate:     {wr:.2f}%")
        print(f"Net PnL:      {df['pnl'].sum():.2f}%")
        print("\nExit Distributions:")
        print(df['reason'].value_counts())
        print(f"\n✅ Created high_conviction_parquet_results.csv")
    else:
        print("No trades triggered with high conviction thresholds in this window.")

if __name__ == "__main__":
    main()
