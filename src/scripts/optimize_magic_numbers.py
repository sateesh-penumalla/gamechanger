
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm
import json
import os

DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"
engine = create_engine(DATABASE_URL)

def run_mini_backtest(df, threshold_vol, target_pct=3.0, sl_pct=1.5):
    """Returns (win_rate, trade_count) for a specific volume threshold"""
    closes = df['close'].values
    opens = df['open'].values
    volumes = df['volume'].values
    highs = df['high'].values
    lows = df['low'].values
    timestamps = df['timestamp'].values
    
    trades = []
    active_trade = None
    debounce_mins = 30
    last_signal_time = timestamps[0] - np.timedelta64(30, 'm')
    
    for k in range(1, len(df)):
        ct = timestamps[k]
        cp = closes[k]
        
        if active_trade:
            pnl = 0
            exit_reason = None
            if active_trade['side'] == 'LONG':
                if highs[k] >= active_trade['tp']:
                    pnl = target_pct
                    exit_reason = 'TP'
                elif lows[k] <= active_trade['sl']:
                    pnl = -sl_pct
                    exit_reason = 'SL'
            else:
                if lows[k] <= active_trade['tp']:
                    pnl = target_pct
                    exit_reason = 'TP'
                elif highs[k] >= active_trade['sl']:
                    pnl = -sl_pct
                    exit_reason = 'SL'
            
            ct_dt = pd.to_datetime(ct)
            if not exit_reason and ct_dt.hour == 15 and ct_dt.minute >= 20:
                exit_reason = 'EOD'
                if active_trade['side'] == 'LONG':
                    pnl = (cp - active_trade['ent_p']) / active_trade['ent_p'] * 100
                else:
                    pnl = (active_trade['ent_p'] - cp) / active_trade['ent_p'] * 100
            
            if exit_reason:
                trades.append(pnl)
                active_trade = None
                last_signal_time = ct
            continue
            
        # Entry Logic
        ct_dt = pd.to_datetime(ct)
        if ct_dt.hour < 9 or (ct_dt.hour == 9 and ct_dt.minute < 30): continue
        if ct_dt.hour >= 14 and ct_dt.minute >= 30: continue
        if (ct - last_signal_time) / np.timedelta64(1, 'm') < debounce_mins: continue
        
        if volumes[k] >= threshold_vol:
            bar_change = cp - opens[k]
            if bar_change == 0: continue
            side = 'LONG' if bar_change > 0 else 'SHORT'
            active_trade = {
                'side': side, 'ent_p': cp,
                'tp': round(cp * (1 + target_pct/100), 2) if side == 'LONG' else round(cp * (1 - target_pct/100), 2),
                'sl': round(cp * (1 - sl_pct/100), 2) if side == 'LONG' else round(cp * (1 + sl_pct/100), 2)
            }
            
    if not trades:
        return 0, 0
    
    win_rate = (len([t for t in trades if t > 0]) / len(trades)) * 100
    return win_rate, len(trades)

def optimize_all_magic_numbers():
    query_symbols = "SELECT symbol, COUNT(*) as count FROM historical_intraday_ticks GROUP BY symbol"
    df_symbols = pd.read_sql(query_symbols, engine)
    symbols = df_symbols[df_symbols['count'] > 5000]['symbol'].tolist()
    
    optimized_magic = {}
    
    print(f"🚀 Optimizing Magic Numbers for {len(symbols)} tickers to maximize Win Rate...")
    
    for symbol in tqdm(symbols):
        try:
            query = f"SELECT timestamp, `open`, high, low, `close`, volume FROM historical_intraday_ticks WHERE symbol = '{symbol}' ORDER BY timestamp ASC"
            df = pd.read_sql(query, engine)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Find candidate thresholds based on peak volumes of moves
            closes = df['close'].values
            volumes = df['volume'].values
            move_peak_volumes = []
            i = 0
            while i < len(df) - 60:
                start_p = closes[i]
                found = False
                for j in range(i + 1, min(i + 61, len(df))):
                    change = (closes[j] - start_p) / start_p
                    if abs(change) >= 0.03:
                        move_peak_volumes.append(np.max(volumes[i:j+1]))
                        i = j
                        found = True
                        break
                if not found: i += 1
            
            if len(move_peak_volumes) < 5:
                # If no 3% moves, use generic volume percentiles
                candidates = [np.percentile(volumes, p) for p in [95, 98, 99, 99.5, 99.9]]
            else:
                # Use percentiles of peaks
                candidates = [np.percentile(move_peak_volumes, p) for p in [10, 25, 50, 75, 90]]
            
            best_win_rate = -1
            best_vol = 0
            best_trade_count = 0
            
            for vol in candidates:
                wr, count = run_mini_backtest(df, vol)
                
                # Rule: Maximize Win Rate, but requires at least 5 trades to avoid noise
                if wr > best_win_rate and count >= 5:
                    best_win_rate = wr
                    best_vol = int(vol)
                    best_trade_count = count
                elif wr == best_win_rate and count > best_trade_count:
                    # Tie break: choose more trades
                    best_vol = int(vol)
                    best_trade_count = count
            
            if best_vol > 0:
                optimized_magic[symbol] = {
                    "magic_number": best_vol,
                    "win_rate": round(best_win_rate, 2),
                    "trade_count": best_trade_count
                }
        except Exception as e:
            print(f"Error optimizing {symbol}: {e}")

    # Save to JSON
    with open('magic_volume.json', 'w') as f:
        json.dump(optimized_magic, f, indent=4)
    
    # Sort results for console display
    sorted_res = sorted(optimized_magic.items(), key=lambda x: x[1]['win_rate'], reverse=True)
    print("\n--- Optimized Leaderboard (Max Win Rate) ---")
    for sym, data in sorted_res[:20]:
        print(f"{sym:12} | Magic: {data['magic_number']:10} | WR: {data['win_rate']:>6}% | Trades: {data['trade_count']}")

    print(f"\n✅ Created optimized magic_volume.json with {len(optimized_magic)} symbols.")

if __name__ == "__main__":
    optimize_all_magic_numbers()
