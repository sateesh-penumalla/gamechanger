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
output_report = "/Users/sateeshbabu/fractionalcto/GameChanger copy/rvol_adaptive_backtest_detailed.csv"

# Load Median Baselines globally for workers
with open("/tmp/vol_median_5m.json", "r") as f:
    vol_medians = json.load(f)

def analyze_single_stock_day(symbol, file_path, target_date):
    """Worker function for parallel processing"""
    try:
        median_5m = vol_medians.get(symbol)
        if not median_5m or median_5m == 0: return []
        
        df = pd.read_parquet(file_path)
        if df.empty: return []
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        df['tick_vol'] = df['volume'].diff().fillna(0)
        df.loc[df['tick_vol'] < 0, 'tick_vol'] = df.loc[df['tick_vol'] < 0, 'volume']
        df = df.set_index('timestamp')
        
        # 1. Core Metrics
        df['vol_5m'] = df['tick_vol'].rolling('5min').sum()
        df['buy_vol_5m'] = df['buy_vol'].rolling('5min').sum()
        df['sell_vol_5m'] = df['sell_vol'].rolling('5min').sum()
        df['aggression'] = df['buy_vol_5m'] / (df['sell_vol_5m'] + 1)
        df['rvol'] = df['vol_5m'] / median_5m
        
        # 2. Adaptive Pulse Logic
        pulse_threshold = median_5m * 0.25
        df['is_pulse'] = df['tick_vol'] >= pulse_threshold
        df['pulses_15m'] = df['is_pulse'].rolling('15min').sum()
        
        # 3. Stability & Efficiency
        df['bid_stability'] = df['bid_pct'].rolling('15min').std()
        df['ltp_5m_ago'] = df['ltp'].shift(1, freq='5min').reindex(df.index, method='ffill')
        df['price_change_5m'] = ((df['ltp'] / df['ltp_5m_ago']) - 1) * 100
        df['efficiency'] = df['price_change_5m'] / ((df['vol_5m'] / 1000000.0) + 0.001)
        
        # 4. Institutional Presence for Exit
        df['inst_presence'] = (df['tick_vol'] > 300000) | (df['aggression'] > 3.0)
        df['inst_active_20m'] = df['inst_presence'].rolling('20min').max()
        
        # 5. Entry Rules
        df['after_925'] = df.index.time >= pd.to_datetime("09:25").time()
        df['long_sig'] = (df['after_925']) & \
                          (df['rvol'] > 5.0) & \
                          (df['vol_5m'] > 500000) & \
                          (df['aggression'] > 2.0) & \
                          (df['pulses_15m'] >= 3) & \
                          (df['bid_stability'] < 5.0) & \
                          (df['efficiency'] > 0.25) & \
                          (df['ltp'] > df['vwap'])
        
        trades = []
        is_in_trade = False
        entry_price = 0
        entry_time = None
        high_water = 0
        half_out = False
        half_out_price = 0
        
        # Simulation Loop (The heavy part)
        for t, row in df.iterrows():
            if not is_in_trade:
                if row['long_sig']:
                    is_in_trade = True
                    entry_price = row['ltp']
                    entry_time = t
                    high_water = entry_price
                    half_out = False
                    entry_metrics = {
                        'date': target_date, 'symbol': symbol,
                        'entry_time': t, 'entry_price': entry_price,
                        'rvol': row['rvol'], 'pulses': row['pulses_15m'],
                        'stability': row['bid_stability'], 'efficiency': row['efficiency'],
                        'aggression': row['aggression']
                    }
            else:
                if row['ltp'] > high_water: high_water = row['ltp']
                
                # Exit Logic
                if not half_out and (row['ltp'] >= entry_price * 1.02):
                    half_out = True
                    half_out_price = row['ltp']
                    continue
                
                exit_triggered = False
                reason = ""
                
                # Rule: Institutions Gone OR VWAP Break with no recent inst activity
                if (row['ltp'] < row['vwap']) and (row['inst_active_20m'] == 0):
                    exit_triggered = True
                    reason = "INST_GONE"
                
                # Trailing stop
                trail_pct = 0.007 if half_out else 0.015
                if row['ltp'] <= high_water * (1 - trail_pct):
                    exit_triggered = True
                    reason = "TRAILING_STOP"
                
                if t == df.index[-1]:
                    exit_triggered = True
                    reason = "EOD"
                
                if exit_triggered:
                    final_pnl = ((row['ltp'] / entry_price) - 1) * 100
                    total_pnl = (0.5 * (((half_out_price/entry_price)-1)*100) + 0.5 * final_pnl) if half_out else final_pnl
                    peak_pnl = ((high_water / entry_price) - 1) * 100
                    
                    entry_metrics.update({
                        'exit_time': t, 'exit_price': row['ltp'],
                        'pnl_pct': total_pnl, 'peak_profit': peak_pnl,
                        'exit_reason': reason, 'half_out': half_out
                    })
                    trades.append(entry_metrics)
                    is_in_trade = False
        return trades
    except Exception:
        return []

if __name__ == "__main__":
    task_list = []
    for d in target_dates:
        for sdir in glob.glob(os.path.join(data_dir, "*")):
            symbol = os.path.basename(sdir)
            p = os.path.join(sdir, f"{d}.parquet")
            if os.path.exists(p):
                task_list.append((symbol, p, d))

    print(f"Starting Parallel Backtest for {len(task_list)} stock-days...")
    
    all_trade_logs = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(analyze_single_stock_day, *task): task for task in task_list}
        
        completed = 0
        for future in as_completed(futures):
            res = future.result()
            if res:
                all_trade_logs.extend(res)
            completed += 1
            if completed % 100 == 0:
                print(f"Progress: {completed}/{len(task_list)} tasks done...")

    if all_trade_logs:
        results_df = pd.DataFrame(all_trade_logs)
        results_df = results_df.sort_values(['date', 'pnl_pct'], ascending=[True, False])
        results_df.to_csv(output_report, index=False)
        
        print("\n--- Adaptive Parallel Backtest Summary ---")
        summary = results_df.groupby('date').agg(
            trades=('symbol', 'count'),
            avg_pnl=('pnl_pct', 'mean'),
            avg_peak=('peak_profit', 'mean'),
            win_rate=('pnl_pct', lambda x: (x > 0).mean() * 100),
            total_pnl=('pnl_pct', 'sum')
        )
        print(summary)
        print(f"\nOverall Avg PnL: {results_df['pnl_pct'].mean():.2f}%")
        print(f"Full trade metrics saved to: {output_report}")
    else:
        print("No signals found.")
