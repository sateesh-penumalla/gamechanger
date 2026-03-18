
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm
import json
import os
from multiprocessing import Pool, cpu_count

DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"

def run_full_backtest(df_dict, symbol, threshold_vol, target_pct=3.0, sl_pct=1.5):
    closes = df_dict['close']
    opens = df_dict['open']
    volumes = df_dict['volume']
    highs = df_dict['high']
    lows = df_dict['low']
    hours = df_dict['hour']
    minutes = df_dict['minute']
    timestamps = df_dict['timestamp']
    
    trades = []
    active_trade = None
    debounce_mins = 30
    last_signal_idx = -100
    
    n = len(closes)
    for k in range(1, n):
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
                    'entry_price': active_trade['ent_p'],
                    'exit_price': exit_p,
                    'pnl': round(pnl, 4),
                    'reason': exit_reason,
                    'trigger_vol': active_trade['trigger_vol']
                })
                active_trade = None
                last_signal_idx = k
            continue
            
        # Entry Logic
        if hours[k] < 9 or (hours[k] == 9 and minutes[k] < 30): continue
        if hours[k] >= 14 and minutes[k] >= 30: continue
        if (k - last_signal_idx) < debounce_mins: continue
        
        if volumes[k] >= threshold_vol:
            bar_change = cp - opens[k]
            if bar_change == 0: continue
            side = 'LONG' if bar_change > 0 else 'SHORT'
            active_trade = {
                'side': side, 
                'ent_p': cp,
                'ent_t': ct,
                'tp': round(cp * (1 + target_pct/100), 2) if side == 'LONG' else round(cp * (1 - target_pct/100), 2),
                'sl': round(cp * (1 - sl_pct/100), 2) if side == 'LONG' else round(cp * (1 + sl_pct/100), 2),
                'trigger_vol': int(volumes[k])
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
        
        return run_full_backtest(df_dict, symbol, config['magic_number'])
    except Exception as e:
        print(f"Error on {symbol}: {e}")
        return []

def main():
    with open('magic_volume.json', 'r') as f:
        magic_data = json.load(f)
    
    items = list(magic_data.items())
    print(f"🚀 Generating full trade history for {len(items)} symbols...")
    
    with Pool(cpu_count()) as p:
        results = list(tqdm(p.imap(process_stock, items), total=len(items)))
    
    # Flatten results
    all_trades = [trade for sublist in results for trade in sublist]
    df_all = pd.DataFrame(all_trades)
    
    if not df_all.empty:
        df_all = df_all.sort_values(by=['entry_time', 'symbol'])
        df_all.to_csv('all_optimized_trades_6m.csv', index=False)
        print(f"\n✅ Exported {len(df_all)} trades to all_optimized_trades_6m.csv")
        
        # summary stats
        print("\n--- Strategy Summary (6 Months) ---")
        print(f"Total Trades: {len(df_all)}")
        print(f"Overall Win Rate: {(len(df_all[df_all['pnl'] > 0]) / len(df_all) * 100):.2f}%")
        print(f"Total Net PnL (Gross Sum): {df_all['pnl'].sum():.2f}%")
    else:
        print("No trades found.")

if __name__ == "__main__":
    main()
