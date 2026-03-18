
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm
import json
import os
from multiprocessing import Pool, cpu_count
from functools import partial

DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"

def run_mini_backtest(df_dict, threshold_vol, target_pct=3.0, sl_pct=1.5):
    """Expects a dictionary of numpy arrays for speed"""
    closes = df_dict['close']
    opens = df_dict['open']
    volumes = df_dict['volume']
    highs = df_dict['high']
    lows = df_dict['low']
    hours = df_dict['hour']
    minutes = df_dict['minute']
    
    trades = []
    active_trade = None
    debounce_mins = 30
    last_signal_idx = -100 # index based debounce
    
    n = len(closes)
    for k in range(1, n):
        cp = closes[k]
        
        if active_trade:
            if active_trade['side'] == 'LONG':
                if highs[k] >= active_trade['tp']:
                    trades.append(target_pct)
                    active_trade = None
                    last_signal_idx = k
                elif lows[k] <= active_trade['sl']:
                    trades.append(-sl_pct)
                    active_trade = None
                    last_signal_idx = k
            else:
                if lows[k] <= active_trade['tp']:
                    trades.append(target_pct)
                    active_trade = None
                    last_signal_idx = k
                elif highs[k] >= active_trade['sl']:
                    trades.append(-sl_pct)
                    active_trade = None
                    last_signal_idx = k
            
            # EOD Square-off
            if active_trade and hours[k] == 15 and minutes[k] >= 20:
                if active_trade['side'] == 'LONG':
                    trades.append((cp - active_trade['ent_p']) / active_trade['ent_p'] * 100)
                else:
                    trades.append((active_trade['ent_p'] - cp) / active_trade['ent_p'] * 100)
                active_trade = None
                last_signal_idx = k
            continue
            
        # Entry Logic (Index based debounce approx)
        if hours[k] < 9 or (hours[k] == 9 and minutes[k] < 30): continue
        if hours[k] >= 14 and minutes[k] >= 30: continue
        if (k - last_signal_idx) < debounce_mins: continue
        
        if volumes[k] >= threshold_vol:
            bar_change = cp - opens[k]
            if bar_change == 0: continue
            side = 'LONG' if bar_change > 0 else 'SHORT'
            active_trade = {
                'side': side, 'ent_p': cp,
                'tp': round(cp * (1 + target_pct/100), 2) if side == 'LONG' else round(cp * (1 - target_pct/100), 2),
                'sl': round(cp * (1 - sl_pct/100), 2) if side == 'LONG' else round(cp * (1 + sl_pct/100), 2)
            }
            
    if not trades: return 0, 0
    win_rate = (len([t for t in trades if t > 0]) / len(trades)) * 100
    return win_rate, len(trades)

def process_single_stock(symbol):
    try:
        engine = create_engine(DATABASE_URL)
        query = f"SELECT timestamp, `open`, high, low, `close`, volume FROM historical_intraday_ticks WHERE symbol = '{symbol}' ORDER BY timestamp ASC"
        df = pd.read_sql(query, engine)
        if len(df) < 5000: return None
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        df['minute'] = df['timestamp'].dt.minute
        
        # Prepare Dict of Arrays
        df_dict = {
            'close': df['close'].values,
            'open': df['open'].values,
            'high': df['high'].values,
            'low': df['low'].values,
            'volume': df['volume'].values,
            'hour': df['hour'].values,
            'minute': df['minute'].values
        }
        
        # Find 3% spikes
        closes = df_dict['close']
        volumes = df_dict['volume']
        peaks = []
        i = 0
        while i < len(df) - 60:
            start_p = closes[i]
            found = False
            for j in range(i+1, min(i+61, len(df))):
                if abs((closes[j] - start_p) / start_p) >= 0.03:
                    peaks.append(np.max(volumes[i:j+1]))
                    i = j
                    found = True
                    break
            if not found: i += 1
            
        if len(peaks) < 5:
            candidates = [np.percentile(volumes, p) for p in [98, 99, 99.5, 99.9]]
        else:
            candidates = [np.percentile(peaks, p) for p in [10, 25, 50, 75, 90]]
            
        best_wr = -1
        best_vol = 0
        best_cnt = 0
        
        for vol in candidates:
            wr, cnt = run_mini_backtest(df_dict, vol)
            # Strategy: Highest Win Rate with min 5 trades
            if wr > best_wr and cnt >= 5:
                best_wr, best_vol, best_cnt = wr, int(vol), cnt
            elif wr == best_wr and cnt > best_cnt:
                best_vol, best_cnt = int(vol), cnt
                
        if best_vol > 0:
            return symbol, {"magic_number": best_vol, "win_rate": round(best_wr, 2), "trade_count": best_cnt}
    except Exception as e:
        return None
    return None

def main():
    engine = create_engine(DATABASE_URL)
    query_symbols = "SELECT symbol, COUNT(*) as count FROM historical_intraday_ticks GROUP BY symbol"
    df_symbols = pd.read_sql(query_symbols, engine)
    symbols = df_symbols[df_symbols['count'] > 5000]['symbol'].tolist()
    
    print(f"🚀 Optimizing {len(symbols)} stocks using {cpu_count()} cores...")
    
    with Pool(cpu_count()) as p:
        results = list(tqdm(p.imap(process_single_stock, symbols), total=len(symbols)))
    
    optimized_magic = {res[0]: res[1] for res in results if res is not None}
    
    with open('magic_volume.json', 'w') as f:
        json.dump(optimized_magic, f, indent=4)
        
    sorted_res = sorted(optimized_magic.items(), key=lambda x: x[1]['win_rate'], reverse=True)
    print("\n--- Optimized Results (Target: Max Win Rate) ---")
    for sym, data in sorted_res[:30]:
        print(f"{sym:12} | Vol: {data['magic_number']:10} | WR: {data['win_rate']:>6}% | Trades: {data['trade_count']}")
        
    print(f"\n✅ Created optimized magic_volume.json with {len(optimized_magic)} symbols.")

if __name__ == "__main__":
    main()
