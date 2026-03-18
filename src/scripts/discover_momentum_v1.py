
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm
import os
from datetime import datetime

DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"
engine = create_engine(DATABASE_URL)

def discover_magic_numbers():
    # 1. Get List of Symbols with enough data
    query_symbols = "SELECT symbol, COUNT(*) as count FROM historical_intraday_ticks GROUP BY symbol HAVING count > 10000"
    df_symbols = pd.read_sql(query_symbols, engine)
    symbols = df_symbols['symbol'].tolist()
    
    results = []
    
    print(f"🚀 Analyzing {len(symbols)} symbols for Magic Numbers and Momentum Patterns...")
    
    for symbol in tqdm(symbols):
        try:
            # Load Data
            query = f"SELECT timestamp, `open`, high, low, `close`, volume FROM historical_intraday_ticks WHERE symbol = '{symbol}' ORDER BY timestamp ASC"
            df = pd.read_sql(query, engine)
            if len(df) < 5000: continue
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            closes = df['close'].values
            opens = df['open'].values
            volumes = df['volume'].values
            timestamps = df['timestamp'].values
            
            # Find 3% moves
            moves = []
            threshold = 0.03 # 3%
            i = 0
            while i < len(df):
                start_p = closes[i]
                found = False
                for j in range(i + 1, min(i + 61, len(df))):
                    change = (closes[j] - start_p) / start_p
                    if abs(change) >= threshold:
                        peak_vol = np.max(volumes[i:j+1])
                        moves.append(peak_vol)
                        i = j
                        found = True
                        break
                if not found: i += 1
            
            if len(moves) < 5: continue # Need enough samples
            
            # Magic Number Calculation
            magic_number = np.percentile(moves, 25) # 25th percentile (conservative conviction)
            
            # Backtest with this Magic Number
            target_pct = 3.0
            sl_pct = 1.5
            debounce_mins = 30
            
            trades = []
            active_trade = None
            last_signal_time = timestamps[0] - np.timedelta64(30, 'm')
            
            for k in range(1, len(df)):
                curr_row = df.iloc[k]
                ct = timestamps[k]
                cp = closes[k]
                
                if active_trade:
                    pnl = 0
                    exit_reason = None
                    if active_trade['side'] == 'LONG':
                        if curr_row['high'] >= active_trade['tp']:
                            pnl = target_pct
                            exit_reason = 'TP'
                        elif curr_row['low'] <= active_trade['sl']:
                            pnl = -sl_pct
                            exit_reason = 'SL'
                    else:
                        if curr_row['low'] <= active_trade['tp']:
                            pnl = target_pct
                            exit_reason = 'TP'
                        elif curr_row['high'] >= active_trade['sl']:
                            pnl = -sl_pct
                            exit_reason = 'SL'
                    
                    # EOD
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
                
                if volumes[k] >= magic_number:
                    bar_change = cp - opens[k]
                    if bar_change == 0: continue
                    side = 'LONG' if bar_change > 0 else 'SHORT'
                    active_trade = {
                        'side': side, 'ent_p': cp, 'ent_t': ct,
                        'tp': round(cp * (1 + target_pct/100), 2) if side == 'LONG' else round(cp * (1 - target_pct/100), 2),
                        'sl': round(cp * (1 - sl_pct/100), 2) if side == 'LONG' else round(cp * (1 + sl_pct/100), 2)
                    }
            
            if trades:
                total_pnl = sum(trades)
                win_rate = (len([t for t in trades if t > 0]) / len(trades)) * 100
                results.append({
                    'symbol': symbol,
                    'magic_number': int(magic_number),
                    'trades': len(trades),
                    'win_rate': round(win_rate, 2),
                    'total_pnl': round(total_pnl, 2),
                    'profit_factor': round(sum([t for t in trades if t > 0]) / abs(sum([t for t in trades if t < 0])) if sum([t for t in trades if t < 0]) != 0 else 99, 2)
                })
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")

    # Output Results
    results_df = pd.DataFrame(results)
    if not results_df.empty:
        # Sort by PnL and Win Rate
        results_df = results_df.sort_values(by='total_pnl', ascending=False)
        print("\n--- Discovery Complete: Top Momentum Stocks ---")
        print(results_df.head(20).to_string())
        results_df.to_csv("momentum_discovery_results.csv", index=False)
    else:
        print("No results found.")

if __name__ == "__main__":
    discover_magic_numbers()
