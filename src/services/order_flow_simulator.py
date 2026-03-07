
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time as dt_time
from loguru import logger
from src.utils.storage_manager import StorageManager
from collections import deque

class OrderFlowSimulator:
    """
    Simulates real-time order flow and signal generation using historical tick data.
    Allows for testing if millisecond-level imbalance or iceberg detection 
    could have generated faster or more accurate signals.
    """
    
    def __init__(self, symbol: str, date: datetime):
        self.symbol = symbol
        self.date = date
        self.storage = StorageManager(base_path="data/historical_ticks")
        self.ticks_df = None
        self.results = []
        
        # Simulation State
        self.last_ttq = 0
        self.buy_vol_total = 0
        self.sell_vol_total = 0
        self.imbalance_history = deque(maxlen=60) # Last 60 ticks
        
    def load_data(self):
        """Loads tick data from Parquet for the specified date."""
        path = self.storage.get_tick_file_path(self.symbol, self.date)
        if not os.path.exists(path):
            logger.error(f"Tick file not found: {path}")
            return False
            
        try:
            self.ticks_df = pd.read_parquet(path)
            # Ensure chronological order
            self.ticks_df.sort_values('timestamp', inplace=True)
            logger.info(f"Loaded {len(self.ticks_df)} ticks for {self.symbol} on {self.date.date()}")
            return True
        except Exception as e:
            logger.error(f"Error loading ticks: {e}")
            return False

    def simulate(self, orb_high=None, orb_low=None):
        """
        Replays ticks and calculates order flow metrics.
        Infers transaction side (Aggressive Buy/Sell) using Best Bid/Ask.
        """
        if self.ticks_df is None:
            if not self.load_data(): return []

        # Automatic ORB calculation (9:15 to 9:30)
        if orb_high is None or orb_low is None:
            orb_start = datetime.combine(self.date.date(), dt_time(9, 15))
            orb_end = datetime.combine(self.date.date(), dt_time(9, 30))
            orb_data = self.ticks_df[(self.ticks_df['timestamp'] >= orb_start) & 
                                     (self.ticks_df['timestamp'] <= orb_end)]
            
            if not orb_data.empty:
                orb_high = orb_data['ltp'].max()
                orb_low = orb_data['ltp'].min()
                logger.info(f"Calculated ORB for {self.symbol}: High={orb_high}, Low={orb_low}")
            else:
                logger.warning(f"No ORB data found for {self.symbol} on {self.date.date()}")

        logger.info(f"Starting Simulation for {self.symbol}...")
        
        # Only simulate post-ORB (9:30 onwards)
        sim_start = datetime.combine(self.date.date(), dt_time(9, 30, 1))
        
        for idx, row in self.ticks_df.iterrows():
            if row['timestamp'] < sim_start:
                continue
                
            ltp = row['ltp']
            bid_price = row.get('bid_price', 0)
            ask_price = row.get('ask_price', 0)
            bid_qty = row.get('bid_qty', 0)
            ask_qty = row.get('ask_qty', 0)
            
            # 1. Calculate Depth Imbalance
            imbalance = 0
            if (bid_qty + ask_qty) > 0:
                imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty)
            
            # 2. Side Inference (Aggression)
            # If LTP hits Ask -> Aggressive Buyer
            # If LTP hits Bid -> Aggressive Seller
            side = "Neutral"
            if ltp >= ask_price > 0:
                side = "Buy"
            elif ltp <= bid_price > 0:
                side = "Sell"
                
            # 3. Track Metrics
            metric_row = {
                "timestamp": row['timestamp'],
                "ltp": ltp,
                "imbalance": round(imbalance, 4),
                "side": side,
                "bid_qty": bid_qty,
                "ask_qty": ask_qty
            }
            
            # 4. Faster Breakout Detection logic
            if orb_high and ltp > orb_high:
                metric_row['signal'] = 'BUY_BREAKOUT'
                metric_row['signal_strength'] = imbalance # High imbalance confirms breakout
            elif orb_low and ltp < orb_low:
                metric_row['signal'] = 'SELL_BREAKOUT'
                metric_row['signal_strength'] = -imbalance
                
            self.results.append(metric_row)
            
        logger.success(f"Simulation Complete. Processed {len(self.results)} ticks.")
        return self.results

    def get_analysis_report(self):
        """Summarizes findings from the simulation."""
        if not self.results: return "No simulation data."
        
        df = pd.DataFrame(self.results)
        
        # Example Analysis: How often was imbalance > 0.5 during positive price moves?
        strong_imbalance = df[df['imbalance'].abs() > 0.5]
        
        report = f"--- Simulation Analysis: {self.symbol} ---\n"
        report += f"Total Ticks: {len(df)}\n"
        report += f"Ticks with Significant Imbalance (>50%): {len(strong_imbalance)}\n"
        
        if 'signal' in df.columns:
            signals = df[df['signal'].notnull()]
            report += f"Total Breakthroughs Detected: {len(signals)}\n"
            if len(signals) > 0:
                report += f"First Signal at: {signals['timestamp'].iloc[0]}\n"
                
        return report

if __name__ == "__main__":
    # Test on existing ONMOBILE data if available
    sim = OrderFlowSimulator("ONMOBILE", datetime.now())
    if sim.load_data():
        sim.simulate(orb_high=55.0, orb_low=50.0)
        print(sim.get_analysis_report())
