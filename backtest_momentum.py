
import os
import pandas as pd
import numpy as np
from datetime import datetime
from collections import deque
from tqdm import tqdm
from sqlalchemy import create_engine

# --- Optimized Config (Synchronized with Orchestrator) ---
MOMENTUM_VOL_SURGE = 12.0
MOMENTUM_VQS = 0.70
SL_PCT = 2.0
TP_PCT = 1.0
DEBOUNCE_MINS = 5
MAX_HISTORY = 100

def get_nifty_sentiment(target_date):
    """Calculates NIFTY VQS timeline."""
    path = f"data/ticks/NIFTY/{target_date}.parquet"
    if not os.path.exists(path): return {}
    try:
        df = pd.read_parquet(path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        hist = deque(maxlen=MAX_HISTORY)
        sentiment = {}
        for _, row in df.iterrows():
            hist.append(float(row['ltp']))
            if len(hist) > 1:
                ticks = np.sign(np.diff(list(hist)))
                vqs = np.mean(ticks[ticks != 0]) if len(ticks[ticks != 0]) > 0 else 0.0
                sentiment[row['timestamp']] = vqs
        return sentiment
    except: return {}

def run_backtest(target_date="2026-03-09"):
    # 1. Setup
    nifty_map = get_nifty_sentiment(target_date)
    nifty_times = sorted(nifty_map.keys())
    
    db_url = "mysql+pymysql://root:root@localhost:3307/bharatquant_sniper"
    engine = create_engine(db_url)
    try:
        focus_df = pd.read_sql(f"SELECT symbol, oracle_status FROM daily_focus WHERE date = '{target_date}'", engine)
        bias_map = dict(zip(focus_df['symbol'], focus_df['oracle_status']))
    except: bias_map = {}

    base_dir = "data/ticks"
    symbols = [d for d in os.listdir(base_dir) if os.path.isdir(f"{base_dir}/{d}") and d != "NIFTY"]
    
    all_trades = []
    
    print(f"🚀 RE-PROCESSED BACKTEST FOR {target_date} (WARM-UP INCLUDED)")
    
    for symbol in tqdm(symbols):
        file_path = f"{base_dir}/{symbol}/{target_date}.parquet"
        if not os.path.exists(file_path): continue
        
        try:
            df = pd.read_parquet(file_path)
            if df.empty: continue
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
        except: continue
        
        # State
        price_history = deque(maxlen=MAX_HISTORY)
        vol_history = deque(maxlen=MAX_HISTORY)
        prev_ttq = 0
        vwap_num = 0.0
        vwap_den = 0.0
        last_signal_time = df.iloc[0]['timestamp'] - pd.Timedelta(days=1)
        active_trade = None
        oracle_status = bias_map.get(symbol, "IGNORE")

        for _, row in df.iterrows():
            ltp, ttq, ct = float(row['ltp']), int(row['volume']), row['timestamp']
            
            # 1. Update Metrics regardless of time (WARM-UP)
            vol_delta = ttq - prev_ttq if ttq > prev_ttq else (ttq if ttq > 0 and prev_ttq == 0 else 0)
            if vol_delta > 0:
                vwap_num += (ltp * vol_delta)
                vwap_den += vol_delta
                vol_history.append(vol_delta)
                prev_ttq = ttq
            price_history.append(ltp)
            vwap = vwap_num / vwap_den if vwap_den > 0 else ltp

            # 2. Handle Active Trade (Allow exits until 15:30)
            if active_trade:
                side = active_trade['side']
                exit_r = None
                if side == "LONG":
                    if ltp >= active_trade['tp']: exit_r = "TP"
                    elif ltp <= active_trade['sl']: exit_r = "SL"
                else:
                    if ltp <= active_trade['tp']: exit_r = "TP"
                    elif ltp >= active_trade['sl']: exit_r = "SL"
                
                # FINAL EOD EXIT at 15:30
                if ct.hour >= 15 and ct.minute >= 30: exit_r = exit_r or "EOD_EXIT"
                
                if exit_r:
                    pnl = TP_PCT if exit_r == "TP" else (-SL_PCT if exit_r == "SL" else ( (ltp-active_trade['ent'])/active_trade['ent']*100 if side=="LONG" else (active_trade['ent']-ltp)/active_trade['ent']*100 ))
                    all_trades.append({
                        "symbol": symbol, "side": side, "ent_time": active_trade['ent_t'], "exit_time": ct,
                        "ent_p": active_trade['ent'], "exit_p": ltp, "pnl": round(pnl, 2), "reason": exit_r
                    })
                    active_trade = None
                    last_signal_time = ct
                continue

            # 3. Entry Window Guard (09:15 to 14:30 ONLY)
            if ct.hour < 9 or (ct.hour == 9 and ct.minute < 15): continue
            if ct.hour >= 14 and ct.minute >= 30: continue

            # 5. Signal Evaluation (Strict Filters)
            if (ct - last_signal_time).total_seconds() < (DEBOUNCE_MINS * 60): continue
            
            if len(price_history) > 1 and len(vol_history) > 0:
                ticks = np.sign(np.diff(list(price_history)))
                ticks = ticks[ticks != 0]
                vqs = np.mean(ticks) if len(ticks) > 0 else 0.0
                
                v_hist = list(vol_history)
                surge = v_hist[-1] / (sum(v_hist)/len(v_hist)) if len(v_hist) > 0 else 1.0
                
                if surge >= MOMENTUM_VOL_SURGE and abs(vqs) >= MOMENTUM_VQS:
                    side = "LONG" if vqs > 0 else "SHORT"
                    
                    # Gold Guard
                    if (side == "LONG" and oracle_status != "UP_SNIPER") or (side == "SHORT" and oracle_status != "DOWN_SNIPER"): continue
                    
                    # Precision Balance
                    bp, ap = row.get('bid_pct', 50.0), row.get('ask_pct', 50.0)
                    strength = bp if side == "LONG" else ap
                    if not (35.0 <= strength <= 65.0): continue
                    
                    # Nifty Market Alignment (Stricter - Dead Zone filter)
                    idx = np.searchsorted(nifty_times, ct)
                    nvqs = nifty_map[nifty_times[idx]] if idx < len(nifty_times) else 0.0
                    
                    # REVISED STRICTOR CONDITIONS:
                    # LONG only if NIFTY > +0.25 (Strong Up)
                    # SHORT only if NIFTY < -0.25 (Strong Down)
                    if side == "LONG" and nvqs <= 0.25: continue
                    if side == "SHORT" and nvqs >= -0.25: continue
                    
                    active_trade = {
                        "side": side, "ent": ltp, "ent_t": ct,
                        "sl": round(ltp * 0.98, 2) if side == "LONG" else round(ltp * 1.02, 2),
                        "tp": round(ltp * 1.01, 2) if side == "LONG" else round(ltp * 0.99, 2)
                    }

    trades_df = pd.DataFrame(all_trades)
    if not trades_df.empty:
        wr = (len(trades_df[trades_df['pnl'] > 0]) / len(trades_df)) * 100
        print(f"\n✅ FINAL REPORT: Total PnL: {trades_df['pnl'].sum():.2f}% | Win Rate: {wr:.1f}% | Trades: {len(trades_df)}")
        trades_df.to_csv(f"backtest_trades_{target_date}.csv", index=False)
        print("\nTOP TRADES:")
        print(trades_df.head(20).to_string())
    else: print("❌ NO TRADES FOUND")

if __name__ == "__main__":
    run_backtest()
