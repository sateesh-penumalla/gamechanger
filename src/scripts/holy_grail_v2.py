
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm
import json
import os
from multiprocessing import Pool, cpu_count

DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"

def run_holy_grail_v2(df_dict, symbol, threshold_vol, target_pct=1.0, sl_pct=1.5):
    """
    Holy Grail V2: Confirmation Gate + Build-up Filter.
    Designed for maximum win rate.
    """
    closes = df_dict['close']
    opens = df_dict['open']
    volumes = df_dict['volume']
    highs = df_dict['high']
    lows = df_dict['low']
    hours = df_dict['hour']
    minutes = df_dict['minute']
    timestamps = df_dict['timestamp']
    
    vol_series = pd.Series(volumes)
    avg_vol_5m = vol_series.rolling(window=5).mean().shift(1).values
    
    trades = []
    active_trade = None
    pending_signal = None 
    debounce_mins = 60
    last_signal_idx = -100
    
    n = len(closes)
    for k in range(10, n):
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
            
            if pnl >= target_pct:
                exit_reason = 'TARGET'
                exit_p = active_trade['tp']
            elif peak_pnl >= 0.7 and pnl <= 0.05: # Protection
                exit_reason = 'PROTECTED'
                exit_p = active_trade['ent_p']
            elif pnl <= -sl_pct:
                exit_reason = 'STOP_LOSS'
                exit_p = active_trade['sl']
            elif hours[k] == 15 and minutes[k] >= 20:
                exit_reason = 'EOD'
                exit_p = cp
            
            if exit_reason:
                trades.append({
                    'symbol': symbol,
                    'side': active_trade['side'],
                    'entry_time': active_trade['ent_t'],
                    'exit_time': ct,
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
            if pending_signal['side'] == 'LONG' and move_pct >= 0.1:
                # CONFIRMED
                active_trade = {
                    'side': 'LONG', 'ent_p': cp, 'ent_t': ct, 'hwm': cp,
                    'tp': cp * (1 + target_pct/100), 'sl': cp * (1 - sl_pct/100)
                }
                pending_signal = None
                continue
            elif pending_signal['side'] == 'SHORT' and move_pct <= -0.1:
                # CONFIRMED
                active_trade = {
                    'side': 'SHORT', 'ent_p': cp, 'ent_t': ct, 'hwm': cp,
                    'tp': cp * (1 - target_pct/100), 'sl': cp * (1 + sl_pct/100)
                }
                pending_signal = None
                continue
            continue

        # 3. Base Discovery
        if hours[k] < 9 or (hours[k] == 9 and minutes[k] < 45): continue
        if hours[k] >= 14 and minutes[k] >= 30: continue
        if (k - last_signal_idx) < debounce_mins: continue
        
        # Trigger
        if volumes[k] >= threshold_vol:
            prev_avg = avg_vol_5m[k]
            if prev_avg > 0 and (volumes[k] / prev_avg) <= 12.0: # Filter isolated spikes
                bar_move = (cp - opens[k]) / opens[k] * 100
                if abs(bar_move) >= 0.1:
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
        return run_holy_grail_v2(df_dict, symbol, config['magic_number'])
    except: return []

if __name__ == "__main__":
    with open('magic_volume.json', 'r') as f:
        magic_data = json.load(f)
    
    symbols = list(magic_data.items())
    print(f"🚀 Running Confirmation Gate Sniper on {len(symbols)} symbols...")
    
    with Pool(cpu_count()) as p:
        results = list(tqdm(p.imap(process_stock, symbols), total=len(symbols)))
    
    all_trades = [t for sub in results for t in sub]
    df = pd.DataFrame(all_trades)
    
    if not df.empty:
        wr = (df['pnl'] > 0).mean() * 100
        print("\n--- FINAL HOLY GRAIL SUMMARY ---")
        print(f"Total Trades: {len(df)}")
        print(f"Win Rate: {wr:.2f}%")
        print(f"Total Net PnL: {df['pnl'].sum():.2f}%")
        print("\nExit Reasons:")
        print(df['reason'].value_counts(normalize=True) * 100)
        df.to_csv('holy_grail_confirmation_gate.csv', index=False)
    else:
        print("No trades triggered.")
