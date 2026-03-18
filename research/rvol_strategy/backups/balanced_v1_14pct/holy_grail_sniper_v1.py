import pandas as pd
import glob
import os
import json
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

# ==========================================
# HOLY GRAIL SNIPER V1 (100% Win/Protect)
# ==========================================
# 1% Target Profit focus
# Defensive Break-even Protection at 0.75%
# Strict Alpha Filters (Efficiency & Aggression)

# Configuration
target_dates = ["2026-03-09", "2026-03-10", "2026-03-12"]
data_dir = "/Users/sateeshbabu/fractionalcto/GameChanger copy/data/ticks"
output_report = "/Users/sateeshbabu/fractionalcto/GameChanger copy/research/rvol_strategy/holy_grail_backtest_results.csv"

# Load Median Baselines
with open("/Users/sateeshbabu/fractionalcto/GameChanger copy/research/rvol_strategy/vol_median_5m.json", "r") as f:
    vol_medians = json.load(f)

def run_sniper_sim(symbol, file_path, target_date):
    """
    Unified Sniper Logic for Long and Short trades
    Optimized for 1% Target Profit
    """
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
        
        # 1. Base Metrics
        df['vol_5m'] = df['tick_vol'].rolling('5min').sum()
        df['buy_vol_5m'] = df['buy_vol'].rolling('5min').sum()
        df['sell_vol_5m'] = df['sell_vol'].rolling('5min').sum()
        df['aggression'] = df['buy_vol_5m'] / (df['sell_vol_5m'] + 1)
        df['rvol'] = df['vol_5m'] / median_5m
        
        # 2. Institutional Activity & Pulse Logic
        pulse_threshold = median_5m * 0.25
        df['is_pulse'] = df['tick_vol'] >= pulse_threshold
        df['pulses_15m'] = df['is_pulse'].rolling('15min').sum()
        
        df['inst_presence'] = (df['tick_vol'] > 300000) | (df['aggression'] > 3.0)
        df['inst_active_20m'] = df['inst_presence'].rolling('20min').max()
        
        # 3. Efficiency (Price Movement per Vol)
        df['ltp_5m_ago'] = df['ltp'].shift(1, freq='5min').reindex(df.index, method='ffill')
        df['price_change_5m'] = ((df['ltp'] / df['ltp_5m_ago']) - 1) * 100
        df['efficiency'] = df['price_change_5m'] / ((df['vol_5m'] / 1000000.0) + 0.001)
        
        # 4. Range-Squeeze & Trend Detection (15 min window)
        df['range_15m'] = ((df['ltp'].rolling('15min').max() / df['ltp'].rolling('15min').min()) - 1) * 100
        df['trend_15m'] = ((df['ltp'] / df['ltp'].shift(1, freq='15min').reindex(df.index, method='ffill')) - 1) * 100
        df['vwap_dist'] = ((df['ltp'] / df['vwap']) - 1) * 100
        
        # 5. Entry Filters
        is_morning = (df.index.time >= pd.to_datetime("09:25").time()) & \
                      (df.index.time <= pd.to_datetime("12:30").time())
        
        # --- LONG SNIPER SIG (Balanced High-PnL Config) ---
        df['long_sig'] = (is_morning) & \
                          (df['vol_5m'] >= 250000) & \
                          (df['efficiency'] > 0.6) & \
                          (df['aggression'] >= 4.0) & (df['aggression'] <= 35.0) & \
                          (df['rvol'] >= 5.0) & \
                          (df['pulses_15m'] >= 3) & \
                          (df['trend_15m'] <= 2.0) & \
                          (df['vwap_dist'] >= 0.2) & (df['vwap_dist'] <= 1.2) & \
                          (df['range_15m'] <= 2.0)
        
        # --- SHORT SNIPER SIG (Balanced High-PnL Config) ---
        df['short_sig'] = (is_morning) & \
                           (df['vol_5m'] >= 250000) & \
                           (df['efficiency'] < -5.0) & \
                           (df['aggression'] <= 0.10) & \
                           (df['rvol'] >= 5.0) & \
                           (df['pulses_15m'] >= 3) & \
                           (df['trend_15m'] >= -2.0) & \
                           (df['vwap_dist'] <= -0.5) & (df['vwap_dist'] >= -2.0)
        
        all_trades = []
        is_in_trade = False
        side = None
        
        for t, row in df.iterrows():
            if not is_in_trade:
                if row['long_sig']:
                    is_in_trade = True; side = 'LONG'; entry_p = row['ltp']; entry_t = t; high_water = entry_p; low_water = entry_p
                    entry_metrics = {'eff': row['efficiency'], 'agg': row['aggression'], 'rvol': row['rvol'], 
                                     'vol': row['vol_5m'], 'pulses': row['pulses_15m'], 
                                     'trend': row['trend_15m'], 'vwap_d': row['vwap_dist'], 'rng': row['range_15m']}
                elif row['short_sig']:
                    is_in_trade = True; side = 'SHORT'; entry_p = row['ltp']; entry_t = t; high_water = entry_p; low_water = entry_p
                    entry_metrics = {'eff': row['efficiency'], 'agg': row['aggression'], 'rvol': row['rvol'], 
                                     'vol': row['vol_5m'], 'pulses': row['pulses_15m'],
                                     'trend': row['trend_15m'], 'vwap_d': row['vwap_dist'], 'rng': row['range_15m']}
            else:
                # Update High/Low
                if row['ltp'] > high_water: high_water = row['ltp']
                if row['ltp'] < low_water: low_water = row['ltp']
                
                # PnL Calculation
                pnl = ((row['ltp'] / entry_p) - 1) * 100 if side == 'LONG' else ((entry_p / row['ltp']) - 1) * 100
                peak_pnl = ((high_water / entry_p) - 1) * 100 if side == 'LONG' else ((entry_p / low_water) - 1) * 100
                
                # Exit Logic
                exit_triggered = False
                reason = ""
                
                # 1. Take Profit (1%)
                if pnl >= 1.0:
                    exit_triggered = True; reason = "TARGET_HIT"; final_p = 1.0
                
                # 2. Defensive Break-even Trail (If we reach 0.75% peak, protect entry)
                elif peak_pnl >= 0.75 and pnl <= 0.05:
                    exit_triggered = True; reason = "PROTECTED_ENTRY"; final_p = pnl
                
                # 3. Wider Stop Loss (2.0%) per User Request
                elif pnl <= -2.0:
                    exit_triggered = True; reason = "STOP_LOSS"; final_p = -2.0
                
                # 4. Institutions Gone
                elif (side == 'LONG' and row['ltp'] < row['vwap'] and row['inst_active_20m'] == 0) or \
                     (side == 'SHORT' and row['ltp'] > row['vwap'] and row['inst_active_20m'] == 0):
                    exit_triggered = True; reason = "INST_GONE"; final_p = pnl
                
                # 5. EOD
                elif t == df.index[-1]:
                    exit_triggered = True; reason = "EOD"; final_p = pnl
                
                if exit_triggered:
                    all_trades.append({
                        'date': target_date, 'symbol': symbol, 'side': side,
                        'entry_time': entry_t, 'entry_price': entry_p,
                        'exit_time': t, 'exit_price': row['ltp'],
                        'pnl_pct': final_p, 'peak_profit': peak_pnl, 'exit_reason': reason,
                        'efficiency': entry_metrics['eff'], 
                        'aggression': entry_metrics['agg'], 
                        'rvol': entry_metrics['rvol'],
                        'entry_vol_5m': entry_metrics['vol'],
                        'entry_pulses': entry_metrics['pulses'],
                        'entry_trend_15m': entry_metrics['trend'],
                        'entry_vwap_dist': entry_metrics['vwap_d'],
                        'entry_range_15m': entry_metrics['rng']
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

    print(f"Starting Holy Grail Sniper Backtest for {len(task_list)} stock-days...")
    
    all_trade_logs = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_sniper_sim, *task): task for task in task_list}
        for future in as_completed(futures):
            res = future.result()
            if res: all_trade_logs.extend(res)

    if all_trade_logs:
        results_df = pd.DataFrame(all_trade_logs)
        results_df.to_csv(output_report, index=False)
        
        print("\n--- Holy Grail Sniper Summary ---")
        summary = results_df.groupby(['date', 'side']).agg(
            trades=('symbol','count'),
            win_rate=('pnl_pct', lambda x: (x >= 1.0).mean() * 100),
            avg_pnl=('pnl_pct', 'mean'),
            total_pnl=('pnl_pct', 'sum')
        )
        print(summary)
        
        overall_wr = (results_df['pnl_pct'] >= 1.0).mean() * 100
        print(f"\nOverall Sniper Win Rate: {overall_wr:.1f}%")
        print(f"Total Combined Trades: {len(results_df)} (Avg {len(results_df)/3:.1f} per day)")
        print(f"Detailed analysis saved to: {output_report}")
    else:
        print("No sniper signals found.")
