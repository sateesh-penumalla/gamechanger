"""
Consolidated Magic Number Generator v3
======================================
Reads from historical_intraday_ticks (DB).

Core Logic:
1. Scan for all 3% price moves within 60-minute windows.
2. Capture peak volume during those moves.
3. Fine-grained threshold search across many percentiles + interpolated values.
4. Two-pass selection:
   Pass 1 - Find the threshold giving 100% WR with max trades.
   Pass 2 - If no 100% WR, find the threshold giving >= 80% WR with max trades.
5. Only outputs symbols with >= 80% WR and positive EV.
6. Backtests with ACTUAL live R:R (1% TP / 2% SL).

Rationale:
If volume hits the magic number, history says a 3%+ move is underway.
We only need to capture 1% of it - high hit-rate.
"""

import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'magic_volume.json')

MOVE_THRESHOLD = 0.03       # 3% price move detection
MOVE_WINDOW = 60            # Look ahead up to 60 bars (60 minutes)
N_MONTHS = 3                # Only use last N months of data
MARKET_OPEN = "09:16"       # Exclude 9:15 auction bar
MARKET_CLOSE = "15:20"      # Exclude settlement bars
MAX_GAP_MINUTES = 2         # Max gap for 3m rolling sum continuity

# Fine-grained search percentiles
SEARCH_PERCENTILES = list(range(5, 100, 5)) + [92, 94, 95, 96, 97, 98, 99]

# Live R:R for backtest (must match orc_sniper_v2.py)
BACKTEST_TP_PCT = 1.0
BACKTEST_SL_PCT = 2.0

MIN_TRADES = 5              # Minimum trades for statistical validity
MIN_BARS = 500              # Minimum bars for a symbol to be considered
MIN_WIN_RATE = 80.0         # Only output symbols with >= 80% WR
MIN_MOVES = 3               # Minimum 3% moves needed (skip otherwise)


# --- HELPERS ---

def filter_market_hours(df):
    """Keep only bars within regular trading hours (exclude auction & settlement)."""
    df = df.copy()
    df['time_str'] = df['timestamp'].dt.strftime('%H:%M')
    df = df[(df['time_str'] >= MARKET_OPEN) & (df['time_str'] <= MARKET_CLOSE)]
    df = df.drop(columns=['time_str'])
    return df.reset_index(drop=True)

def find_3pct_move_peak_volumes(df):
    """Scan for 3% price moves within 60-minute windows.
    Returns the list of peak volumes observed during each such move.
    """
    closes = df['close'].values
    volumes = df['volume'].values
    n = len(df)
    
    move_peak_volumes = []
    i = 0
    while i < n - MOVE_WINDOW:
        start_p = closes[i]
        if start_p <= 0:
            i += 1
            continue
            
        found = False
        for j in range(i + 1, min(i + MOVE_WINDOW + 1, n)):
            change = (closes[j] - start_p) / start_p
            if abs(change) >= MOVE_THRESHOLD:
                # Capture peak volume in the window [i:j+1]
                peak_v = float(np.max(volumes[i : j + 1]))
                move_peak_volumes.append(peak_v)
                i = j  # Skip to end of move to avoid double-counting
                found = True
                break
        if not found:
            i += 1
            
    return move_peak_volumes

def compute_rolling_3m_with_gap_check(df):
    """Compute 3-bar volume sum, but only where consecutive bars 
    are within MAX_GAP_MINUTES of each other (skip lunch/day gaps).
    """
    volumes = df['volume'].values
    timestamps = df['timestamp'].values
    rolling_3m = []
    
    for i in range(2, len(df)):
        t0 = pd.Timestamp(timestamps[i-2])
        t1 = pd.Timestamp(timestamps[i-1])
        t2 = pd.Timestamp(timestamps[i])
        
        gap_01 = (t1 - t0).total_seconds() / 60
        gap_12 = (t2 - t1).total_seconds() / 60
        
        if gap_01 <= MAX_GAP_MINUTES and gap_12 <= MAX_GAP_MINUTES:
            rolling_3m.append(volumes[i-2] + volumes[i-1] + volumes[i])
            
    return np.array(rolling_3m) if rolling_3m else np.array([])

def run_mini_backtest(df, threshold_vol, target_pct=BACKTEST_TP_PCT, sl_pct=BACKTEST_SL_PCT):
    """Backtest a volume threshold using the ACTUAL live R:R (1% TP / 2% SL).
    Returns (win_rate, trade_count, avg_pnl, wins, losses).
    """
    closes = df['close'].values
    opens = df['open'].values
    volumes = df['volume'].values
    highs = df['high'].values
    lows = df['low'].values
    hours = df['timestamp'].dt.hour.values
    minutes = df['timestamp'].dt.minute.values
    dates = df['timestamp'].dt.date.values
    
    trades = []
    active_trade = None
    last_trade_date = None
    
    for k in range(len(df)):
        cp = closes[k]
        
        if active_trade:
            # Check exit
            if active_trade['side'] == 'LONG':
                if highs[k] >= active_trade['tp']:
                    trades.append(target_pct / 100) # Target PnL
                    active_trade = None
                elif lows[k] <= active_trade['sl']:
                    trades.append(-sl_pct / 100) # Stop Loss
                    active_trade = None
            else: # SHORT
                if lows[k] <= active_trade['tp']:
                    trades.append(target_pct / 100)
                    active_trade = None
                elif highs[k] >= active_trade['sl']:
                    trades.append(-sl_pct / 100)
                    active_trade = None
            
            # EOD square-off
            if active_trade and hours[k] == 15 and minutes[k] >= 20:
                pnl = (cp - active_trade['ent_p']) / active_trade['ent_p']
                if active_trade['side'] == 'SHORT': pnl = -pnl
                trades.append(pnl)
                active_trade = None
                
            continue

        # Entry logic: skip auction noise & late hours
        if hours[k] < 9 or (hours[k] == 9 and minutes[k] < 31): continue
        if hours[k] >= 14 and minutes[k] >= 30: continue
        
        # Max 1 trade per day
        if dates[k] == last_trade_date: continue
        
        if volumes[k] >= threshold_vol:
            bar_change = cp - opens[k]
            if bar_change == 0: continue
            
            side = 'LONG' if bar_change > 0 else 'SHORT'
            tp = cp * (1 + target_pct/100) if side == 'LONG' else cp * (1 - target_pct/100)
            sl = cp * (1 - sl_pct/100) if side == 'LONG' else cp * (1 + sl_pct/100)
            
            active_trade = {
                'side': side,
                'ent_p': cp,
                'tp': tp,
                'sl': sl
            }
            last_trade_date = dates[k]

    if not trades:
        return 0, 0, 0, 0, 0
        
    wins = len([t for t in trades if t > 0])
    losses = len(trades) - wins
    win_rate = (wins / len(trades)) * 100
    avg_pnl = np.mean(trades)
    
    return round(win_rate, 2), len(trades), round(avg_pnl, 4), wins, losses

def build_candidates(move_peak_volumes):
    """Build fine-grained candidate thresholds from peak move volumes.
    Tests percentiles at 5% intervals + interpolated values between p50 and p95.
    """
    candidates = []
    
    # Standard percentiles
    for p in SEARCH_PERCENTILES:
        vol = int(np.percentile(move_peak_volumes, p))
        candidates.append((f"p{p}", vol))
        
    # Interpolated values between p50 and p95 for finer resolution
    p50 = int(np.percentile(move_peak_volumes, 50))
    p95 = int(np.percentile(move_peak_volumes, 95))
    if p95 > p50:
        step = (p95 - p50) // 15
        if step > 0:
            for mult in range(1, 15):
                v = p50 + step * mult
                candidates.append((f"interp{mult}", v))
                
    # Low percentiles for symbols with many moves (can go lower)
    if len(move_peak_volumes) >= 30:
        for p in [3, 5, 7]:
            vol = int(np.percentile(move_peak_volumes, p))
            candidates.append((f"low_p{p}", vol))
            
    # Deduplicate by volume value
    seen = set()
    unique = []
    for label, vol in candidates:
        if vol > 0 and vol not in seen:
            seen.add(vol)
            unique.append((label, vol))
            
    return unique

def find_best_threshold(df, move_peak_volumes):
    """Two-pass search for the best volume threshold:
    Pass 1: Find the threshold giving 100% WR with most trades.
    Pass 2: If no 100%, find the threshold giving >= 80% WR with most trades.
    """
    candidates = build_candidates(move_peak_volumes)
    results = []
    for label, vol in candidates:
        wr, count, avg_pnl, wins, losses = run_mini_backtest(df, vol)
        if count >= MIN_TRADES:
            results.append((label, vol, wr, count, avg_pnl, wins, losses))
            
    if not results:
        return None
        
    # Pass 1: Look for 100% WR - pick the one with MOST trades
    perfect = [r for r in results if r[2] == 100.0]
    if perfect:
        perfect.sort(key=lambda x: x[3], reverse=True)
        return perfect[0]
        
    # Pass 2: Look for >= 80% Win Rate with positive EV - pick the one with MOST trades
    high_wr = [r for r in results if r[2] >= MIN_WIN_RATE and r[4] > 0]
    if high_wr:
        # Sort by: Most trades, then Win Rate, then Avg PnL
        high_wr.sort(key=lambda x: (x[3], x[2], x[4]), reverse=True)
        return high_wr[0]
        
    return None

def generate_magic_numbers():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set in environment. Add it to .env")
        return
        
    engine = create_engine(db_url)
    cutoff_date = (datetime.now() - timedelta(days=N_MONTHS * 30)).strftime('%Y-%m-%d')
    
    # Get all symbols with sufficient data in the recency window
    print(f"Fetching symbols from DB (data since {cutoff_date})...")
    with engine.connect() as conn:
        symbols_df = pd.read_sql(text(f"""
            SELECT symbol, COUNT(*) as cnt
            FROM historical_intraday_ticks
            WHERE timestamp >= :cutoff
            GROUP BY symbol HAVING cnt >= :min_bars
        """), conn, params={"cutoff": cutoff_date, "min_bars": MIN_BARS})
        
    symbols = sorted(symbols_df['symbol'].tolist())
    print(f"Computing magic numbers for {len(symbols)} symbols...")
    print(f"Criteria: WR >= {MIN_WIN_RATE}% | Trades >= {MIN_TRADES} | Positive EV | R:R = {BACKTEST_TP_PCT}TP/{BACKTEST_SL_PCT}SL")
    
    magic_data = {}
    stats = {
        'skipped_low_data': 0, 'skipped_few_moves': 0, 
        'skipped_low_wr': 0, 'generated': 0, 'perfect_wr': 0
    }
    
    for symbol in tqdm(symbols):
        with engine.connect() as conn:
            df = pd.read_sql(text("""
                SELECT timestamp, open, high, low, close, volume
                FROM historical_intraday_ticks
                WHERE symbol = :sym AND timestamp >= :cutoff
                ORDER BY timestamp ASC
            """), conn, params={"sym": symbol, "cutoff": cutoff_date}, parse_dates=['timestamp'])
            
        if len(df) < MIN_BARS:
            stats['skipped_low_data'] += 1
            continue
            
        df = filter_market_hours(df)
        if len(df) < MIN_BARS:
            stats['skipped_low_data'] += 1
            continue
            
        # CORE: Find 3% price moves and capture peak volumes
        move_peak_volumes = find_3pct_move_peak_volumes(df)
        
        if len(move_peak_volumes) < MIN_MOVES:
            stats['skipped_few_moves'] += 1
            continue
            
        # Fine-grained threshold search
        best = find_best_threshold(df, move_peak_volumes)
        
        if best is None:
            stats['skipped_low_wr'] += 1
            continue
            
        label, vol, wr, count, avg_pnl, wins, losses = best
        if wr == 100.0:
            stats['perfect_wr'] += 1
            
        # 3m accumulation threshold (gap-validated rolling sum)
        rolling_3m = compute_rolling_3m_with_gap_check(df)
        if len(rolling_3m) > 0:
            thresh_3m = int(np.percentile(rolling_3m, 98.6)) # High percentile fallback
        else:
            thresh_3m = vol * 3 # fall back to triple 1m threshold
            
        magic_data[symbol] = {
            "magic_number": vol,
            "high_conviction_magic": vol,
            "high_conviction_magic_3m": thresh_3m,
            "win_rate": wr,
            "trade_count": count,
            "avg_pnl_pct": round(avg_pnl * 100, 2),
            "move_count": len(move_peak_volumes),
            "data_since": cutoff_date,
            "note": f"F3% Move Peak ({label}) | 1/2 R:R"
        }
        stats['generated'] += 1
        
    # Save
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(magic_data, f, indent=4)
        
    # --- REPORT ---
    print(f"\nGenerated: {stats['generated']} (100%WR: {stats['perfect_wr']}) | "
          f"Skipped (low data): {stats['skipped_low_data']} | "
          f"Skipped (few moves): {stats['skipped_few_moves']} | "
          f"Skipped (below {MIN_WIN_RATE}% WR): {stats['skipped_low_wr']}")

    # --- Leaderboard by WR then trades ---
    sorted_res = sorted(magic_data.items(), 
                       key=lambda x: (x[1]['win_rate'], x[1]['trade_count'], x[1]['avg_pnl_pct']),
                       reverse=True)

    # 100% WR
    perfect = [(s, d) for s, d in sorted_res if d['win_rate'] == 100.0]
    if perfect:
        print(f"\n--- 100% WIN RATE: {len(perfect)} symbols ---")
        print(f"{'Symbol':>14} | {'Magic':>12} | {'3m':>12} | {'WR':>6} | {'Trades':>6} | {'AvgPnL':>8} | {'3%Moves':>7}")
        print("-" * 85)
        for sym, d in perfect[:20]: # Show top 20
            print(f"{sym:>14} | {d['high_conviction_magic']:>12} | {d['high_conviction_magic_3m']:>12} | "
                  f"{d['win_rate']:>5.1f}% | {d['trade_count']:>6} | {d['avg_pnl_pct']:>7.3f}% | {d['move_count']:>7}")

    # 80-99% WR
    high_wr = [(s, d) for s, d in sorted_res if MIN_WIN_RATE <= d['win_rate'] < 100.0]
    if high_wr:
        print(f"\n--- 80-99% WIN RATE: {len(high_wr)} symbols ---")
        print(f"{'Symbol':>14} | {'Magic':>12} | {'3m':>12} | {'WR':>6} | {'Trades':>6} | {'AvgPnL':>8} | {'3%Moves':>7}")
        print("-" * 85)
        for sym, d in high_wr[:20]: # Show top 20
            print(f"{sym:>14} | {d['high_conviction_magic']:>12} | {d['high_conviction_magic_3m']:>12} | "
                  f"{d['win_rate']:>5.1f}% | {d['trade_count']:>6} | {d['avg_pnl_pct']:>7.3f}% | {d['move_count']:>7}")

    # Overall Metrics
    if magic_data:
        total_trades = sum(d['trade_count'] for d in magic_data.values())
        total_wins   = sum(int(d['trade_count'] * d['win_rate'] / 100) for d in magic_data.values())
        avg_wr = np.mean([d['win_rate'] for d in magic_data.values()])
        avg_pnl = np.mean([d['avg_pnl_pct'] for d in magic_data.values()])
        
        print(f"\n--- OVERALL ---")
        print(f"Symbols: {len(magic_data)} | Trades: {total_trades} | Wins: {total_wins}")
        print(f"Avg WR: {avg_wr:.1f}% | Avg PnL: {avg_pnl:.3f}%")

    print(f"\nSaved to {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_magic_numbers()
