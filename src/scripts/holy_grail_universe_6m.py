
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm
import json
import os
from multiprocessing import Pool, cpu_count

DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"

def run_holy_grail_sim(df_dict, symbol, threshold_vol, target_pct=1.0, sl_pct=2.0):
    """
    Holy Grail Sniper Logic
    Aims for high win-rate via Efficiency and Pulse filters.
    Includes Break-even Protection.
    """
    closes = df_dict['close']
    opens = df_dict['open']
    volumes = df_dict['volume']
    highs = df_dict['high']
    lows = df_dict['low']
    hours = df_dict['hour']
    minutes = df_dict['minute']
    timestamps = df_dict['timestamp']
    
    # 5m Rolling Avg for filters
    vol_series = pd.Series(volumes)
    avg_vol_5m = vol_series.rolling(window=5).mean().shift(1).values
    
    # Pulses (Bars > 30% of Magic Number)
    is_pulse = (vol_series >= (threshold_vol * 0.3)).astype(int)
    pulses_15m = is_pulse.rolling(window=15).sum().shift(1).values
    
    trades = []
    active_trade = None
    debounce_mins = 60 # Very selective
    last_signal_idx = -100
    
    n = len(closes)
    for k in range(20, n):
        cp = closes[k]
        ct = timestamps[k]
        
        if active_trade:
            # High Water Mark for Protection
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
            
            # --- EXIT LOGIC ---
            # 1. Target Hit
            if pnl >= target_pct:
                exit_reason = 'TARGET'
                exit_p = active_trade['tp']
            # 2. Break-even Protection (If reached 0.7%, stop out at entry)
            elif peak_pnl >= 0.7 and pnl <= 0.1:
                exit_reason = 'PROTECTED'
                exit_p = active_trade['ent_p']
            # 3. Stop Loss
            elif pnl <= -sl_pct:
                exit_reason = 'STOP_LOSS'
                exit_p = active_trade['sl']
            # 4. EOD
            elif hours[k] == 15 and minutes[k] >= 20:
                exit_reason = 'EOD'
                exit_p = cp
            
            if exit_reason:
                trades.append({
                    'symbol': symbol,
                    'side': active_trade['side'],
                    'entry_time': active_trade['ent_t'],
                    'exit_time': ct,
                    'pnl': round((exit_p/active_trade['ent_p'] - 1)*100 if active_trade['side'] == 'LONG' else (active_trade['ent_p']/exit_p - 1)*100, 2),
                    'reason': exit_reason
                })
                active_trade = None
                last_signal_idx = k
            continue
            
        # --- SNIPER ENTRY LOGIC ---
        if hours[k] < 9 or (hours[k] == 9 and minutes[k] < 45): continue
        if hours[k] >= 14 and minutes[k] >= 30: continue
        if (k - last_signal_idx) < debounce_mins: continue
        
        # Guard 1: Volume Trigger
        if volumes[k] < threshold_vol: continue
        
        # Guard 2: Pulse Check (Institutional activity in the last 15 mins)
        if pulses_15m[k] < 2: continue
        
        # Guard 3: Efficiency Check
        price_change_5m = abs(closes[k] - closes[k-5]) / closes[k-5] * 100
        vol_5m_sum = np.sum(volumes[k-4 : k+1])
        efficiency = price_change_5m / (vol_5m_sum / 1_000_000 + 0.1)
        if efficiency < 0.2: continue # Move is too "noisy"
        
        # Guard 4: Directional Confirmation
        bar_change = cp - opens[k]
        if abs(bar_change/opens[k]*100) < 0.1: continue
        
        side = 'LONG' if bar_change > 0 else 'SHORT'
        active_trade = {
            'side': side, 'ent_p': cp, 'ent_t': ct, 'hwm': cp,
            'tp': round(cp * (1 + target_pct/100), 2) if side == 'LONG' else round(cp * (1 - target_pct/100), 2),
            'sl': round(cp * (1 - sl_pct/100), 2) if side == 'LONG' else round(cp * (1 + sl_pct/100), 2)
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
        return run_holy_grail_sim(df_dict, symbol, config['magic_number'])
    except: return []

def main():
    with open('magic_volume.json', 'r') as f:
        magic_data = json.load(f)
    
    symbols = list(magic_data.items())
    print(f"🚀 Running Holy Grail Universe Backtest on {len(symbols)} symbols...")
    
    with Pool(cpu_count()) as p:
        results = list(tqdm(p.imap(process_stock, symbols), total=len(symbols)))
    
    all_trades = [t for sub in results for t in sub]
    df = pd.DataFrame(all_trades)
    
    if not df.empty:
        df.to_csv('holy_grail_universe_trades.csv', index=False)
        wr = (df['pnl'] > 0).mean() * 100
        print("\n--- HOLY GRAIL UNIVERSE SUMMARY ---")
        print(f"Total Trades: {len(df)}")
        print(f"Win Rate: {wr:.2f}%")
        print(f"Total PnL: {df['pnl'].sum():.2f}%")
        
        # Categorize Exits
        print("\n--- Exit Reason Distribution ---")
        print(df['reason'].value_counts(normalize=True) * 100)
        
        print(f"\n✅ Created holy_grail_universe_trades.csv")
    else:
        print("No trades triggered. Pulse/Efficiency guards might be too high.")

if __name__ == "__main__":
    main()
