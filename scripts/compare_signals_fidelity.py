
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.services.order_flow_simulator import OrderFlowSimulator

def run_comparison(symbol: str, date: datetime, orb_high: float):
    print(f"\n--- Fidelity Comparison: {symbol} on {date.date()} ---")
    print(f"Opening Range Breakout High (ORB-H) to Beat: {orb_high}")
    
    sim = OrderFlowSimulator(symbol, date)
    if not sim.load_data():
        print(f"❌ No historical ticks found for {symbol}. Download them first using 'scripts/download_orb_history.py'")
        return

    # Simulate
    ticks = sim.simulate(orb_high=orb_high)
    
    if not ticks:
        print("❌ Simulation returned no data.")
        return

    # Find first signal
    df = pd.DataFrame(ticks)
    signals = df[df['signal'].notnull()]
    
    if signals.empty:
        print("ℹ️ No breakout signals detected in the tick data for this price level.")
        return

    first_tick_signal = signals.iloc[0]
    
    # 1. Bar Fidelity (Slow)
    # A bar signal usually triggers at the end of the minute bar or when the LTP is checked every 30-60s
    bar_time = first_tick_signal['timestamp'].replace(second=0, microsecond=0) + timedelta(minutes=1)
    
    # 2. Tick Fidelity (Fast)
    tick_time = first_tick_signal['timestamp']
    
    time_saved = (bar_time - tick_time).total_seconds()
    
    print("\n[RESULT]")
    print(f"⚡ Tick-Level Entry: {tick_time.strftime('%H:%M:%S.%f')[:-3]} (LTP: {first_tick_signal['ltp']})")
    print(f"🐢 Standard Bar Entry: {bar_time.strftime('%H:%M:%S')} (Typical check loop)")
    print(f"🚀 **Efficiency Gain: {time_saved:.1f} seconds saved**")
    print(f"📊 Market Imbalance at Entry: {first_tick_signal['imbalance']:.4f} (Bias: {'Bulls' if first_tick_signal['imbalance'] > 0 else 'Bears'})")

if __name__ == "__main__":
    # Example: RELIANCE on 2026-02-12
    # Assuming the ORB High was around 1465 from our previous test logs
    run_comparison("RELIANCE", datetime.now(), orb_high=1465.0)
