
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.services.order_flow_simulator import OrderFlowSimulator

def run_batch_simulation():
    print("\n--- Starting Batch Order Flow Simulation Report ---")
    base_dir = "data/historical_ticks"
    
    if not os.path.exists(base_dir):
        print(f"❌ Directory not found: {base_dir}")
        return

    results = []
    
    symbols = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    
    for symbol in symbols:
        symbol_dir = os.path.join(base_dir, symbol)
        files = [f for f in os.listdir(symbol_dir) if f.endswith(".parquet")]
        
        for file in files:
            date_str = file.replace(".parquet", "")
            try:
                date_dt = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
                
            sim = OrderFlowSimulator(symbol, date_dt)
            if not sim.load_data():
                continue
                
            # Run simulation with auto-ORB
            ticks = sim.simulate()
            
            if not ticks:
                continue
                
            # Check for signals
            df = pd.DataFrame(ticks)
            if 'signal' in df.columns:
                signals = df[df['signal'].notnull()]
                if not signals.empty:
                    first_signal = signals.iloc[0]
                    tick_time = first_signal['timestamp']
                    # Standard Bar Entry: end of the minute
                    bar_time = tick_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
                    time_saved = (bar_time - tick_time).total_seconds()
                    
                    results.append({
                        "symbol": symbol,
                        "date": date_str,
                        "tick_entry": tick_time.strftime("%H:%M:%S.%f")[:-3],
                        "bar_entry": bar_time.strftime("%H:%M:%S"),
                        "seconds_saved": time_saved,
                        "imbalance": first_signal['imbalance'],
                        "side": first_signal['signal']
                    })
    
    if not results:
        print("ℹ️ No breakout signals detected in the historical dataset.")
        return

    report_df = pd.DataFrame(results)
    avg_saved = report_df['seconds_saved'].mean()
    max_saved = report_df['seconds_saved'].max()
    
    print("\n[BATCH SIMULATION SUMMARY]")
    print(f"Total Trading Days Analyzed: {len(results)}")
    print(f"Average Speed Advantage: {avg_saved:.2f} seconds")
    print(f"Maximum Speed Advantage: {max_saved:.2f} seconds")
    
    print("\nTop 10 Faster Entries:")
    print(report_df.sort_values(by="seconds_saved", ascending=False).head(10)[['symbol', 'date', 'seconds_saved', 'imbalance', 'side']])
    
    # Save to CSV for user review
    report_file = "data/batch_simulation_results.csv"
    report_df.to_csv(report_file, index=False)
    print(f"\n✅ Detailed report saved to {report_file}")

if __name__ == "__main__":
    run_batch_simulation()
