
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
output_report = "/Users/sateeshbabu/fractionalcto/GameChanger copy/research/rvol_strategy/backtest_986_results_today.csv"

# Load Magic Numbers
with open(magic_json_path, "r") as f:
    magic_data = json.load(f)

def run_sniper_sim(symbol, file_path, target_date):
    try:
        symbol_magic = magic_data.get(symbol)
        if not symbol_magic: return []
        
        v1m_limit = symbol_magic.get('high_conviction_magic')
        v3m_limit = symbol_magic.get('high_conviction_magic_3m')
        if not v1m_limit: return []

        df = pd.read_parquet(file_path)
        if df.empty: return []
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        df['tick_vol'] = df['volume'].diff().fillna(0)
        df.loc[df['tick_vol'] < 0, 'tick_vol'] = df.loc[df['tick_vol'] < 0, 'volume']
        df = df.set_index('timestamp')
        
        # Core Metrics
        df['vol_1m'] = df['tick_vol'].rolling('1min').sum()
        df['vol_3m'] = df['tick_vol'].rolling('3min').sum()
        df['vol_5m'] = df['tick_vol'].rolling('5min').sum()
        df['buy_vol_5m'] = df['buy_vol'].rolling('5min').sum()
        df['sell_vol_5m'] = df['sell_vol'].rolling('5min').sum()
        df['aggression'] = df['buy_vol_5m'] / (df['sell_vol_5m'] + 1)
        
        # Efficiency
        df['ltp_5m_ago'] = df['ltp'].shift(1, freq='5min').reindex(df.index, method='ffill')
        df['price_change_5m'] = ((df['ltp'] / df['ltp_5m_ago']) - 1) * 100
        df['efficiency'] = df['price_change_5m'] / ((df['vol_5m'] / 1000000.0) + 0.001)
        
        # Pulse Logic
        pulse_threshold = v1m_limit * 0.3
        df['is_pulse'] = df['tick_vol'] >= pulse_threshold
        df['pulses_15m'] = df['is_pulse'].rolling('15min').sum()
        
        # VWAP Dist
        df['vwap_dist'] = ((df['ltp'] / df['vwap']) - 1) * 100
        df['range_15m'] = ((df['ltp'].rolling('15min').max() / df['ltp'].rolling('15min').min()) - 1) * 100
        df['trend_15m'] = ((df['ltp'] / df['ltp'].shift(1, freq='15min').reindex(df.index, method='ffill')) - 1) * 100

        # HOLY GRAIL SNIPER SIG
        # Long
        df['long_sig'] = ( (df['vol_1m'] >= v1m_limit) | (df['vol_3m'] >= v3m_limit) ) & \
                         (df['efficiency'] > 0.6) & \
                         (df['aggression'] >= 4.0) & (df['aggression'] <= 35.0) & \
                         (df['pulses_15m'] >= 3) & \
                         (df['trend_15m'] <= 2.0) & \
                         (df['range_15m'] <= 2.0)

        # Short
        df['short_sig'] = ( (df['vol_1m'] >= v1m_limit) | (df['vol_3m'] >= v3m_limit) ) & \
                          (df['efficiency'] < -5.0) & \
                          (df['aggression'] <= 0.10) & \
                          (df['pulses_15m'] >= 3) & \
                          (df['trend_15m'] >= -2.0)

        all_trades = []
        is_in_trade = False
        side = None
        
        for t, row in df.iterrows():
            if not is_in_trade:
                if row['long_sig']:
                    is_in_trade = True; side = 'LONG'; entry_p = row['ltp']; entry_t = t; high_water = entry_p; low_water = entry_p
                elif row['short_sig']:
                    is_in_trade = True; side = 'SHORT'; entry_p = row['ltp']; entry_t = t; high_water = entry_p; low_water = entry_p
            else:
                if row['ltp'] > high_water: high_water = row['ltp']
                if row['ltp'] < low_water: low_water = row['ltp']
                
                pnl = ((row['ltp'] / entry_p) - 1) * 100 if side == 'LONG' else ((entry_p / row['ltp']) - 1) * 100
                peak_pnl = ((high_water / entry_p) - 1) * 100 if side == 'LONG' else ((entry_p / low_water) - 1) * 100
                
                exit_triggered = False
                reason = ""
                
                if pnl >= 1.0:
                    exit_triggered = True; reason = "TARGET_HIT"; final_p = 1.0
                elif peak_pnl >= 0.75 and pnl <= 0.05:
                    exit_triggered = True; reason = "PROTECTED_ENTRY"; final_p = pnl
                elif pnl <= -2.0:
                    exit_triggered = True; reason = "STOP_LOSS"; final_p = -2.0
                elif t == df.index[-1]:
                    exit_triggered = True; reason = "EOD"; final_p = pnl
                
                if exit_triggered:
                    all_trades.append({
                        'date': target_date, 'symbol': symbol, 'side': side,
                        'entry_time': entry_t, 'entry_price': entry_p,
                        'exit_time': t, 'exit_price': row['ltp'],
                        'pnl_pct': final_p, 'peak_profit': peak_pnl, 'exit_reason': reason
                    })
                    is_in_trade = False
        return all_trades
    except Exception as e:
        return []

if __name__ == "__main__":
    task_list = []
    symbol_dirs = glob.glob(os.path.join(data_dir, "*"))
    for d in target_dates:
        for sdir in symbol_dirs:
            symbol = os.path.basename(sdir)
            p = os.path.join(sdir, f"{d}.parquet")
            if os.path.exists(p): task_list.append((symbol, p, d))

    print(f"Starting 98.6th Percentile Backtest for 2026-03-16 ({len(task_list)} stock-days)...")
    
    all_trade_logs = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_sniper_sim, *task): task for task in task_list}
        for future in as_completed(futures):
            res = future.result()
            if res: all_trade_logs.extend(res)

    if all_trade_logs:
        results_df = pd.DataFrame(all_trade_logs)
        results_df.to_csv(output_report, index=False)
        print("\n--- Backtest Summary (2026-03-16) ---")
        print(results_df)
        wr = (results_df['pnl_pct'] >= 1.0).mean() * 100
        print(f"\nWin Rate: {wr:.1f}% | Total Trades: {len(results_df)}")
    else:
        print("No sniper signals found for today with 98.6th thresholds.")
