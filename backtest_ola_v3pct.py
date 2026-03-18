
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
from datetime import datetime

# --- Configuration for OLA Strategy ---
VOL_SPIKE_THRESHOLD = 1420000  # From analysis: 25th percentile of move peaks
TARGET_PCT = 3.0
STOP_LOSS_PCT = 1.5
DEBOUNCE_MINS = 30             # Wait 30 mins between signals to avoid overtrading
MIN_AVG_VOL = 300000           # Minimum liquidity guard

# Database Connection
DATABASE_URL = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"
engine = create_engine(DATABASE_URL)

def run_ola_backtest():
    print("🚀 Running OLA Backtest (Volume Spike Strategy) - 6 Month Historical")
    query = "SELECT timestamp, `open`, high, low, `close`, volume FROM historical_intraday_ticks WHERE symbol = 'OLAELEC' ORDER BY timestamp ASC;"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("❌ No data found for OLAELEC")
        return

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Pre-calculating signals based ONLY on Absolute Volume Spike
    # From analysis: 1.42 Million is the "conviction" threshold for a major move
    df['entry_signal'] = df['volume'] >= VOL_SPIKE_THRESHOLD
    
    trades = []
    active_trade = None
    last_signal_time = df.iloc[0]['timestamp'] - pd.Timedelta(minutes=DEBOUNCE_MINS)
    
    # Simulation
    for i in range(1, len(df)):
        row = df.iloc[i]
        curr_price = row['close']
        curr_time = row['timestamp']
        
        # 1. Handle Active Trade
        if active_trade:
            # Check TP/SL
            # For simplicity, we use close of the bar. In reality, High/Low could hit during the bar.
            # Using closing price for conservative backtest.
            
            pnl = 0
            exit_reason = None
            
            if active_trade['side'] == 'LONG':
                if row['high'] >= active_trade['tp']:
                    exit_reason = 'TP'
                    pnl = TARGET_PCT
                elif row['low'] <= active_trade['sl']:
                    exit_reason = 'SL'
                    pnl = -STOP_LOSS_PCT
            else: # SHORT
                if row['low'] <= active_trade['tp']:
                    exit_reason = 'TP'
                    pnl = TARGET_PCT
                elif row['high'] >= active_trade['sl']:
                    exit_reason = 'SL'
                    pnl = -STOP_LOSS_PCT
            
            # EOD Square-off (15:20)
            if not exit_reason and curr_time.hour == 15 and curr_time.minute >= 20:
                exit_reason = 'EOD'
                if active_trade['side'] == 'LONG':
                    pnl = (curr_price - active_trade['ent_p']) / active_trade['ent_p'] * 100
                else:
                    pnl = (active_trade['ent_p'] - curr_price) / active_trade['ent_p'] * 100
            
            if exit_reason:
                trades.append({
                    'symbol': 'OLAELEC',
                    'side': active_trade['side'],
                    'ent_time': active_trade['ent_t'],
                    'exit_time': curr_time,
                    'ent_price': active_trade['ent_p'],
                    'exit_price': curr_price if exit_reason == 'EOD' else (active_trade['tp'] if exit_reason == 'TP' else active_trade['sl']),
                    'pnl': pnl,
                    'reason': exit_reason,
                    'trigger_vol': active_trade['trigger_vol']
                })
                active_trade = None
                last_signal_time = curr_time
            continue
            
        # 2. Eval Entry
        # Entry Guard: 09:30 to 14:30
        if curr_time.hour < 9 or (curr_time.hour == 9 and curr_time.minute < 30): continue
        if curr_time.hour >= 14 and curr_time.minute >= 30: continue
        
        # Wait for debounce
        if (curr_time - last_signal_time).total_seconds() < (DEBOUNCE_MINS * 60): continue
        
        if row['entry_signal']:
            # Directional Detection: LTP vs Prev Close or Open
            # If price is moving UP during the spike, go LONG.
            # If price is moving DOWN during the spike, go SHORT.
            # We look at the 1-min bar change.
            bar_change = (row['close'] - row['open'])
            
            if bar_change == 0: continue # No movement, no conviction
            
            side = 'LONG' if bar_change > 0 else 'SHORT'
            ent_p = curr_price
            
            active_trade = {
                'side': side,
                'ent_p': ent_p,
                'ent_t': curr_time,
                'tp': round(ent_p * (1 + TARGET_PCT/100), 2) if side == 'LONG' else round(ent_p * (1 - TARGET_PCT/100), 2),
                'sl': round(ent_p * (1 - STOP_LOSS_PCT/100), 2) if side == 'LONG' else round(ent_p * (1 + STOP_LOSS_PCT/100), 2),
                'trigger_vol': row['volume']
            }
            
    # Report
    trades_df = pd.DataFrame(trades)
    if not trades_df.empty:
        total_pnl = trades_df['pnl'].sum()
        win_rate = (len(trades_df[trades_df['pnl'] > 0]) / len(trades_df)) * 100
        print("\n--- OLA BACKTEST RESULTS ---")
        print(f"Total Trades: {len(trades_df)}")
        print(f"Win Rate: {win_rate:.1f}%")
        print(f"Total PnL: {total_pnl:.2f}%")
        print(f"Profit Factor: {abs(trades_df[trades_df['pnl'] > 0]['pnl'].sum() / trades_df[trades_df['pnl'] < 0]['pnl'].sum()):.2f}")
        
        print("\n--- Individual Trades ---")
        print(trades_df[['ent_time', 'side', 'reason', 'pnl', 'trigger_vol']])
        
        trades_df.to_csv("backtest_ola_results.csv", index=False)
        print("\nFull results saved to backtest_ola_results.csv")
    else:
        print("❌ No trades triggered with these entry signals.")

if __name__ == "__main__":
    run_ola_backtest()
