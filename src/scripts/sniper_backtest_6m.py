
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm
import json
import os
from multiprocessing import Pool, cpu_count

DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"

def run_sniper_backtest(df_dict, symbol, threshold_vol, target_pct=3.0, sl_pct=1.5):
    closes = df_dict['close']
    opens = df_dict['open']
    volumes = df_dict['volume']
    highs = df_dict['high']
    lows = df_dict['low']
    hours = df_dict['hour']
    minutes = df_dict['minute']
    timestamps = df_dict['timestamp']
    
    # Pre-calculate 5m rolling average of volume for the "Flash Filter"
    vol_series = pd.Series(volumes)
    avg_vol_5m = vol_series.rolling(window=5).mean().shift(1).values
    avg_vol_60m = vol_series.rolling(window=60).mean().shift(1).values
    
    trades = []
    active_trade = None
    debounce_mins = 45 # Increased debounce for higher quality
    last_signal_idx = -100
    
    n = len(closes)
    for k in range(60, n): # Start from 60 to have rolling data
        cp = closes[k]
        ct = timestamps[k]
        
        if active_trade:
            pnl = 0
            exit_reason = None
            exit_p = 0
            
            if active_trade['side'] == 'LONG':
                if highs[k] >= active_trade['tp']:
                    pnl = target_pct
                    exit_reason = 'TP'
                    exit_p = active_trade['tp']
                elif lows[k] <= active_trade['sl']:
                    pnl = -sl_pct
                    exit_reason = 'SL'
                    exit_p = active_trade['sl']
            else:
                if lows[k] <= active_trade['tp']:
                    pnl = target_pct
                    exit_reason = 'TP'
                    exit_p = active_trade['tp']
                elif highs[k] >= active_trade['sl']:
                    pnl = -sl_pct
                    exit_reason = 'SL'
                    exit_p = active_trade['sl']
            
            # EOD
            if not exit_reason and hours[k] == 15 and minutes[k] >= 20:
                exit_reason = 'EOD'
                exit_p = cp
                if active_trade['side'] == 'LONG':
                    pnl = (cp - active_trade['ent_p']) / active_trade['ent_p'] * 100
                else:
                    pnl = (active_trade['ent_p'] - cp) / active_trade['ent_p'] * 100
            
            if exit_reason:
                trades.append({
                    'symbol': symbol,
                    'side': active_trade['side'],
                    'entry_time': active_trade['ent_t'],
                    'exit_time': ct,
                    'pnl': round(pnl, 4),
                    'reason': exit_reason,
                    'trigger_vol': active_trade['trigger_vol'],
                    'surge_ratio': active_trade['surge_ratio']
                })
                active_trade = None
                last_signal_idx = k
            continue
            
        # --- SNIPER ENTRY LOGIC ---
        
        # 1. Base Filters
        if hours[k] < 9 or (hours[k] == 9 and minutes[k] < 45): continue # Skip opening volatility
        if hours[k] >= 14 and minutes[k] >= 30: continue
        if (k - last_signal_idx) < debounce_mins: continue
        
        # 2. Magic Number Trigger
        if volumes[k] < threshold_vol: continue
        
        # 3. SNIPER GUARD 1: The Flash Filter (Max Surge 12x)
        # Prevents buying into isolated error spikes
        prev_avg = avg_vol_5m[k]
        if prev_avg == 0: continue
        surge_ratio = volumes[k] / prev_avg
        if surge_ratio > 12.0: continue 
        
        # 4. SNIPER GUARD 2: The Activity Filter (Momentum Build-up)
        # 5m average volume must be at least 1.5x the 1h baseline
        baseline = avg_vol_60m[k]
        if baseline == 0: continue
        if prev_avg < (1.5 * baseline): continue
        
        # 5. SNIPER GUARD 3: Directional Confirmation
        # Current bar must move in direction of primary trend
        bar_change = cp - opens[k]
        if bar_change == 0: continue
        
        # Trend check (Last 3 mins)
        trend_3m = cp - closes[k-3]
        if (bar_change > 0 and trend_3m <= 0) or (bar_change < 0 and trend_3m >= 0): continue
        
        # Entry
        side = 'LONG' if bar_change > 0 else 'SHORT'
        active_trade = {
            'side': side, 
            'ent_p': cp,
            'ent_t': ct,
            'tp': round(cp * (1 + target_pct/100), 2) if side == 'LONG' else round(cp * (1 - target_pct/100), 2),
            'sl': round(cp * (1 - sl_pct/100), 2) if side == 'LONG' else round(cp * (1 + sl_pct/100), 2),
            'trigger_vol': int(volumes[k]),
            'surge_ratio': round(surge_ratio, 2)
        }
            
    return trades

def process_stock(item):
    symbol, config = item
    try:
        engine = create_engine(DATABASE_URL)
        query = f"SELECT timestamp, `open`, high, low, `close`, volume FROM historical_intraday_ticks WHERE symbol = '{symbol}' ORDER BY timestamp ASC"
        df = pd.read_sql(query, engine)
        if df.empty: return []
        
        df_dict = {
            'timestamp': df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').values,
            'close': df['close'].values,
            'open': df['open'].values,
            'high': df['high'].values,
            'low': df['low'].values,
            'volume': df['volume'].values,
            'hour': df['timestamp'].dt.hour.values,
            'minute': df['timestamp'].dt.minute.values
        }
        
        return run_sniper_backtest(df_dict, symbol, config['magic_number'])
    except Exception as e:
        return []

def main():
    with open('magic_volume.json', 'r') as f:
        magic_data = json.load(f)
    
    items = list(magic_data.items())
    print(f"🚀 Running Sniper Backtest with 3 Guards on {len(items)} symbols...")
    
    with Pool(cpu_count()) as p:
        results = list(tqdm(p.imap(process_stock, items), total=len(items)))
    
    all_trades = [trade for sublist in results for trade in sublist]
    df_all = pd.DataFrame(all_trades)
    
    if not df_all.empty:
        df_all = df_all.sort_values(by=['entry_time', 'symbol'])
        df_all.to_csv('sniper_refined_trades_6m.csv', index=False)
        
        win_trades = df_all[df_all['pnl'] > 0]
        loss_trades = df_all[df_all['pnl'] < 0]
        win_rate = (len(win_trades) / len(df_all)) * 100
        
        print("\n--- SNIPER REFINED RESULTS (6 MONTHS) ---")
        print(f"Total Trades: {len(df_all)} (Highly Selective)")
        print(f"Win Rate: {win_rate:.2f}%")
        print(f"Total Net PnL: {df_all['pnl'].sum():.2f}%")
        print(f"Profit Factor: {abs(win_trades['pnl'].sum() / loss_trades['pnl'].sum()):.2f}")
        
        # Best Symbols
        print("\n--- Top Sniper Symbols ---")
        print(df_all.groupby('symbol')['pnl'].sum().sort_values(ascending=False).head(15))
        
    else:
        print("No sniper trades triggered. Guards may be too strict.")

if __name__ == "__main__":
    main()
