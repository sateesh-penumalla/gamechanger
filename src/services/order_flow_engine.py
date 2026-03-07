
import os
import pandas as pd
import numpy as np
import sys
from datetime import datetime, timedelta, time as dt_time
from loguru import logger

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.utils.storage_manager import StorageManager

class OrderFlowEngine:
    """
    Advanced simulation engine that manages trades at the tick level.
    Evaluates TP, SL, and TSL for every price change (millisecond fidelity).
    """
    
    def __init__(self, symbol: str, date: datetime):
        self.symbol = symbol
        self.date = date
        self.storage = StorageManager(base_path="data/historical_ticks")
        self.ticks_df = None
        self.results = []
        
        # State Management
        self.active_trade = None
        self.trade_history = []
        
    def load_data(self):
        """Loads tick data from Parquet."""
        path = self.storage.get_tick_file_path(self.symbol, self.date)
        if not os.path.exists(path):
            logger.error(f"Tick file not found: {path}")
            return False
            
        try:
            self.ticks_df = pd.read_parquet(path)
            self.ticks_df.sort_values('timestamp', inplace=True)
            return True
        except Exception as e:
            logger.error(f"Error loading ticks: {e}")
            return False

    def _precalculate_indicators(self):
        """Pre-calculates VWAP and Rolling Volume average for the session."""
        df = self.ticks_df
        
        # 1. VWAP
        df['cum_vol'] = df['volume'].cumsum()
        df['cum_pv'] = (df['ltp'] * df['volume']).cumsum()
        df['vwap'] = df['cum_pv'] / df['cum_vol']
        
        # 2. Rolling Volume (100 ticks)
        df['rolling_vol_avg'] = df['volume'].rolling(window=100).mean().fillna(0)
        
        # 3. VQS (Volume Quotation Score) - Last 5 seconds momentum
        # Score = (pos_ticks - neg_ticks) / total_ticks in 5s window
        df['price_diff'] = df['ltp'].diff().fillna(0)
        df['tick_dir'] = df['price_diff'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        
        # We use a time-based rolling sum. For this, we temporarily set the index to timestamp.
        temp_df = df[['timestamp', 'tick_dir']].set_index('timestamp')
        df['vqs_score'] = temp_df['tick_dir'].rolling('5s').sum().values
        # Normalize by count of ticks (excluding neutral ticks)
        counts = temp_df['tick_dir'].rolling('5s').count().values
        df['vqs_score'] = (df['vqs_score'] / counts).fillna(0)

        # Cleanup
        df.drop(columns=['cum_vol', 'cum_pv', 'price_diff', 'tick_dir'], inplace=True)

    def run_simulation(self, config):
        """
        Runs the simulation with the provided configuration.
        """
        if self.ticks_df is None:
            if not self.load_data(): return []

        # 1. Calculate ORB
        date_obj = self.date.date()
        orb_start = datetime.combine(date_obj, dt_time(9, 15))
        orb_window = config.get('orb_window', 15)
        orb_end = orb_start + timedelta(minutes=orb_window)
        
        orb_data = self.ticks_df[(self.ticks_df['timestamp'] >= orb_start) & 
                                 (self.ticks_df['timestamp'] <= orb_end)]
        
        if orb_data.empty:
            logger.warning(f"No ORB data for {self.symbol}")
            return []

        orb_h = orb_data['ltp'].max()
        orb_l = orb_data['ltp'].min()
        
        # 2. Pre-calculate Metrics
        self._precalculate_indicators()

        # 3. Simulation Loop (Post-ORB)
        sig_start_t = datetime.strptime(config.get('start_time', '09:30'), '%H:%M').time()
        sig_end_t = datetime.strptime(config.get('end_time', '14:00'), '%H:%M').time()
        
        sim_start = orb_end + timedelta(seconds=1)
        sim_data = self.ticks_df[self.ticks_df['timestamp'] >= sim_start]
        
        for idx, row in sim_data.iterrows():
            ltp = row['ltp']
            ts = row['timestamp']
            cur_time = ts.time()
            
            # Data for Filters
            bid = row.get('bid', 0)
            ask = row.get('ask', 0)
            bid_qty = row.get('bidqty', 0)
            ask_qty = row.get('askqty', 0)
            
            # Order Flow Imbalance
            imbalance = 0
            if (bid_qty + ask_qty) > 0:
                imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty)

            # --- SIGNAL DETECTION ---
            if not self.active_trade:
                if not (sig_start_t <= cur_time <= sig_end_t):
                    continue
                    
                # Apply Execution Filters
                # Filter A: Spread
                if config.get('use_spread_filter'):
                    if bid > 0 and ask > 0:
                        if ((ask - bid) / ltp * 100) > config.get('max_spread_pct', 0.05):
                            continue

                # Filter B: Volume Surge
                if config.get('use_vol_surge'):
                    rolling_avg = row.get('rolling_vol_avg', 0)
                    if rolling_avg > 0 and row['volume'] < rolling_avg * config.get('vol_surge_threshold', 10):
                        continue

                side = None
                # LONG Conditions
                if ltp > orb_h and imbalance >= config['imbalance_threshold']:
                    is_valid = True
                    if config.get('use_vwap_filter') and ltp < row.get('vwap', 0): is_valid = False
                    if config.get('oracle_filter') and config.get('oracle_status') != 'UP_SNIPER': is_valid = False
                    if config.get('use_vqs_filter') and row.get('vqs_score', 0) <= 0: is_valid = False
                    if is_valid: side = 'LONG'
                        
                # SHORT Conditions
                elif ltp < orb_l and imbalance <= -config['imbalance_threshold']:
                    is_valid = True
                    if config.get('use_vwap_filter') and ltp > row.get('vwap', 0): is_valid = False
                    if config.get('oracle_filter') and config.get('oracle_status') != 'DOWN_SNIPER': is_valid = False
                    if config.get('use_vqs_filter') and row.get('vqs_score', 0) >= 0: is_valid = False
                    if is_valid: side = 'SHORT'
                
                if side:
                    if config.get('sl_type') == 'ORB_BOUNDARY':
                        sl_price = orb_l if side == 'LONG' else orb_h
                    else:
                        sl_pct = config.get('sl_pct', 0.5)
                        sl_price = ltp * (1 - sl_pct/100) if side == 'LONG' else ltp * (1 + sl_pct/100)

                    self.active_trade = {
                        'symbol': self.symbol,
                        'side': side,
                        'entry_time': ts,
                        'entry_price': ltp,
                        'sl_price': sl_price,
                        'tp_price': ltp * (1 + config['tp_pct']/100) if side == 'LONG' else ltp * (1 - config['tp_pct']/100),
                        'max_favorable_price': ltp,
                        'imbalance_at_entry': round(imbalance, 4),
                        'oracle_status': config.get('oracle_status', 'N/A')
                    }
                    continue

            # --- TRADE MANAGEMENT ---
            if self.active_trade:
                at = self.active_trade
                side = at['side']
                
                # Update Max Favorable for TSL
                if side == 'LONG':
                    at['max_favorable_price'] = max(at['max_favorable_price'], ltp)
                    if config.get('enable_tsl'):
                        trail_price = at['max_favorable_price'] * (1 - config['tsl_step']/100)
                        at['sl_price'] = max(at['sl_price'], trail_price)
                else: 
                    at['max_favorable_price'] = min(at['max_favorable_price'], ltp)
                    if config.get('enable_tsl'):
                        trail_price = at['max_favorable_price'] * (1 + config['tsl_step']/100)
                        at['sl_price'] = min(at['sl_price'], trail_price)

                # Exit Conditions
                exit_reason = None
                if (side == 'LONG' and ltp >= at['tp_price']) or (side == 'SHORT' and ltp <= at['tp_price']):
                    exit_reason = 'TARGET'
                elif (side == 'LONG' and ltp <= at['sl_price']) or (side == 'SHORT' and ltp >= at['sl_price']):
                    exit_reason = 'STOPLOSS'
                elif ts.time() >= dt_time(15, 20):
                    exit_reason = 'EOD'

                if exit_reason:
                    pnl = ((ltp - at['entry_price']) / at['entry_price'] * 100) if side == 'LONG' else \
                          ((at['entry_price'] - ltp) / at['entry_price'] * 100)
                    duration = (ts - at['entry_time']).total_seconds()
                    
                    self.trade_history.append({
                        **at,
                        'exit_time': ts,
                        'exit_price': ltp,
                        'pnl_pct': round(pnl, 4),
                        'exit_reason': exit_reason,
                        'duration_secs': round(duration, 2)
                    })
                    self.active_trade = None

        return self.trade_history
