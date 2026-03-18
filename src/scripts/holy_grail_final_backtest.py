
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm
import json
import os
from multiprocessing import Pool, cpu_count
from collections import deque

DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"

def run_holy_grail_final_sim(df_dict, symbol, threshold_vol, target_pct=1.0, sl_pct=2.0):
    """
    EXACT Mirror of Live Holy Grail Logic in orc.py
    """
    closes = df_dict['close']
    opens = df_dict['open']
    highs = df_dict['high']
    lows = df_dict['low']
    volumes = df_dict['volume']
    timestamps = df_dict['timestamp']
    hours = df_dict['hour']
    minutes = df_dict['minute']
    
    trades = []
    active_trade = None
    pending_signal = None
    debounce_mins = 60
    last_signal_idx = -100
    
    n = len(closes)
    # We need at least 20 mins of data for rolling history
    for k in range(20, n):
        cp = closes[k]
        ct = timestamps[k]
        
        # 1. Manage Active Trade
        if active_trade:
            if active_trade['side'] == 'LONG':
                if highs[k] > active_trade['hwm']: active_trade['hwm'] = highs[k]
                pnl = (cp - active_trade['ent_p']) / active_trade['ent_p'] * 100
                peak_pnl = (active_trade['hwm'] - active_trade['ent_p']) / active_trade['ent_p'] * 100
            else:
                if lows[k] < active_trade['hwm']: active_trade['hwm'] = lows[k]
                pnl = (active_trade['ent_p'] - cp) / active_trade['ent_p'] * 100
                peak_pnl = (active_trade['ent_p'] - active_trade['hwm']) / active_trade['ent_p'] * 100
            
            exit_reason = None
            exit_p = cp
            
            # Exit Logic
            if pnl >= target_pct:
                exit_reason = 'TARGET'
                exit_p = active_trade['tp']
            elif peak_pnl >= 0.7 and pnl <= 0.05: # Break-even Protection
                exit_reason = 'PROTECTED'
                exit_p = active_trade['ent_p']
            elif pnl <= -sl_pct:
                exit_reason = 'STOP_LOSS'
                exit_p = active_trade['sl']
            elif hours[k] == 15 and minutes[k] >= 20: # EOD
                exit_reason = 'EOD'
                exit_p = cp
            
            if exit_reason:
                trades.append({
                    'symbol': symbol,
                    'side': active_trade['side'],
                    'entry_time': active_trade['ent_t'],
                    'exit_time': ct,
                    'entry_price': round(float(active_trade['ent_p']), 2),
                    'exit_price': round(float(exit_p), 2),
                    'pnl': round(pnl if exit_reason != 'TARGET' else target_pct, 2),
                    'reason': exit_reason
                })
                active_trade = None
                last_signal_idx = k
            continue

        # 2. Check Confirmation Gate
        if pending_signal:
            elapsed = k - pending_signal['idx']
            if elapsed > 10: # 10 minutes max to confirm
                pending_signal = None
                continue
                
            move_pct = (cp - pending_signal['price']) / pending_signal['price'] * 100
            if (pending_signal['side'] == 'LONG' and move_pct >= 0.1) or \
               (pending_signal['side'] == 'SHORT' and move_pct <= -0.1):
                # CONFIRMED -> ENTER
                active_trade = {
                    'side': pending_signal['side'],
                    'ent_p': cp,
                    'ent_t': ct,
                    'hwm': cp,
                    'tp': cp * (1 + target_pct/100) if pending_signal['side'] == 'LONG' else cp * (1 - target_pct/100),
                    'sl': cp * (1 - sl_pct/100) if pending_signal['side'] == 'LONG' else cp * (1 + sl_pct/100)
                }
                pending_signal = None
                continue
            continue

        # 3. Base Strategy Selection
        if (hours[k] == 9 and minutes[k] < 45) or hours[k] < 9: continue
        if hours[k] >= 14 and minutes[k] >= 30: continue
        if (k - last_signal_idx) < debounce_mins: continue
        
        # Guard 1: Magic Number Trigger
        if volumes[k] < threshold_vol: continue
        
        # Guard 2: Pulse Filter (3-bar rule)
        # Check volume in last 15 mins. Is there at least 2 other bars > 30% of magic?
        recent_vols = volumes[k-15:k]
        pulses = [v for v in recent_vols if v >= (threshold_vol * 0.3)]
        if len(pulses) < 2: continue # 2 + current one = 3
        
        # Guard 3: Efficiency Check (Price move per vol)
        # abs(Price Change 5m) / (Vol_5m / 1M + 0.1) >= 0.2
        price_move_5m = abs(closes[k] - closes[k-5]) / closes[k-5] * 100
        vol_5m = np.sum(volumes[k-4:k+1])
        eff = price_move_5m / (vol_5m / 1_000_000 + 0.1)
        if eff < 0.2: continue
        
        # Filter: Needs some directional conviction in the current bar
        bar_move = (closes[k] - opens[k]) / opens[k] * 100
        if abs(bar_move) < 0.1: continue
        
        # Trigger Pending
        pending_signal = {
            'side': 'LONG' if bar_move > 0 else 'SHORT',
            'price': cp,
            'idx': k
        }
        
    return trades

def process_stock(item):
    symbol, config = item
    try:
        engine = create_engine(DATABASE_URL)
        query = f"SELECT timestamp, `open`, high, low, `close`, volume FROM historical_intraday_ticks WHERE symbol = '{symbol}' ORDER BY timestamp ASC"
        df = pd.read_sql(query, engine)
        if len(df) < 5000: return []
        
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
        return run_holy_grail_final_sim(df_dict, symbol, config['magic_number'])
    except Exception as e:
        return []

def main():
    # Load the magic numbers
    with open('magic_volume.json', 'r') as f:
        magic_data = json.load(f)
    
    items = list(magic_data.items())
    print(f"🚀 Running FINAL Holy Grail Backtest on {len(items)} symbols...")
    
    with Pool(cpu_count()) as p:
        results = list(tqdm(p.imap(process_stock, items), total=len(items)))
    
    all_trades = [t for sub in results for t in sub]
    df = pd.DataFrame(all_trades)
    
    if not df.empty:
        df = df.sort_values('entry_time')
        df.to_csv('holy_grail_final_trades_6m.csv', index=False)
        
        # Stats
        wr = (df['pnl'] > 0).mean() * 100
        total_pnl = df['pnl'].sum()
        winners = df[df['pnl'] > 0]
        losers = df[df['pnl'] < 0]
        profit_factor = abs(winners['pnl'].sum() / losers['pnl'].sum()) if not losers.empty else 0
        
        print("\n--- 🏆 HOLY GRAIL FINAL BACKTEST RESULTS ---")
        print(f"Total Trades: {len(df)}")
        print(f"Win Rate:     {wr:.2f}%")
        print(f"Profit Factor:{profit_factor:.2f}")
        print(f"Total Net PnL:{total_pnl:.2f}%")
        print("\nExit Type Distribution:")
        print(df['reason'].value_counts(normalize=True) * 100)
        
        print(f"\n✅ Exported trades to holy_grail_final_trades_6m.csv")
    else:
        print("No trades triggered. Pulse/Efficiency guards might be too high for these thresholds.")

if __name__ == "__main__":
    main()
