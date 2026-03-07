import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def optimize_strategy():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not found.")
        return

    engine = create_engine(db_url)
    
    query = """
        SELECT 
            e.*, 
            h.ORB_range_pct, 
            h.ORB_range_pct_clean,
            t.avg_daily_turnover
        FROM history_events e
        JOIN history_testing h ON e.symbol = h.symbol AND e.trade_date = h.trade_date
        JOIN tickers t ON e.symbol = t.symbol
    """
    
    print("Fetching history events with Range & ADTV...")
    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    if df.empty:
        print("No historical events found.")
        return

    print(f"Loaded {len(df)} events. Analyzing for high Win Rate...")

    # Expanded Grid Search covering User Request (Range, ADTV) + Logic (RSI, Vol)
    results = []

    print("Running Comprehensive Optimization (Range, ADTV, Sector, Vol)...")

    # 1. Boundary Type
    for boundary in ['STANDARD', 'CLEAN']:
        # 2. RSI Thresholds (Strong Momentum)
        # We'll stick to 60 as a baseline good filter
        for rsi_long in [60, 65]:
            # 3. Volume Surge
            for vol_surge in [2.0, 3.0]:
                # 4. ORB Range % (The "Goldilocks" Zone)
                # Filter out ranges that are too wide (already moved) or too narrow (noise)
                for max_range in [1.5, 2.0, 3.0]:
                    # 5. ADTV (Institutional Liquidity)
                    for min_adtv in [100, 500]:
                                                
                        # Filter Data
                        # LONG
                        long_mask = (
                            (df['event_type'] == 'BREAKOUT') & 
                            (df['boundary_type'] == boundary) & 
                            (df['rsi'] >= rsi_long) & 
                            (df['vol_surge'] >= vol_surge) &
                            (df['sector_change_at_entry'] > 0) & # Sector is mandatory for high prob
                            (df['avg_daily_turnover'] >= min_adtv)
                        )
                        
                        # Apply Range Logic based on Boundary Type
                        if boundary == 'STANDARD':
                            long_mask &= (df['ORB_range_pct'] <= max_range)
                        else:
                            long_mask &= (df['ORB_range_pct_clean'] <= max_range)

                        longs = df[long_mask]
                        
                        # SHORT
                        short_mask = (
                            (df['event_type'] == 'BREAKDOWN') & 
                            (df['boundary_type'] == boundary) & 
                            (df['rsi'] <= (100 - rsi_long)) & 
                            (df['vol_surge'] >= vol_surge) &
                            (df['sector_change_at_entry'] < 0) &
                            (df['avg_daily_turnover'] >= min_adtv)
                        )
                        
                        if boundary == 'STANDARD':
                            short_mask &= (df['ORB_range_pct'] <= max_range)
                        else:
                            short_mask &= (df['ORB_range_pct_clean'] <= max_range)

                        shorts = df[short_mask]
                        
                        combined = pd.concat([longs, shorts])
                        total_trades = len(combined)
                        
                        # Simulate 0.5% Target instead of 1.0%
                        # We need to re-evaluate the outcome based on price data, but we don't have row-level data here.
                        # However, history_events has 'pnl'. If outcome was TARGET (1%), it's definitely a win for 0.5%.
                        # If outcome was SL_HIT or SQUARE_OFF, we need to check if 'pnl' reached 0.5% at any point? 
                        # No, 'pnl' in DB is the realized PnL.
                        # Wait, the DB 'pnl' is the final PnL. 
                        # If we want to test 0.5%, we strictly need to re-run the simulation (populate_history).
                        # BUT, as a proxy: 
                        # If PnL >= 0.5%, we count it as a WIN.
                        
                        if total_trades < 10: continue

                        wins = combined[combined['pnl'] >= 0.5]
                        win_rate = (len(wins) / total_trades) * 100
                        
                        results.append({
                            "boundary": boundary,
                            "rsi": rsi_long,
                            "vol_surge": vol_surge,
                            "max_range": max_range,
                            "min_adtv": min_adtv,
                            "trades": total_trades,
                            "win_rate": round(win_rate, 2)
                        })

    # Sort
    results_df = pd.DataFrame(results).sort_values(by='win_rate', ascending=False)
    
    print("\n--- TOP 10 CONFIGURATIONS ---")
    print(results_df.head(10).to_string(index=False))
    
    if not results_df.empty and results_df.iloc[0]['win_rate'] < 90:
         print(f"\nNote: Max Win Rate is {results_df.iloc[0]['win_rate']}%.")

if __name__ == "__main__":
    optimize_strategy()
