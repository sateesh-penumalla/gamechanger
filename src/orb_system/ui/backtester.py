import sys
import os
import pandas as pd
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional
from loguru import logger

# Add root directory to path for imports - MUST BE AT TOP
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.orb_system.config.database import db
from src.orb_system.config.settings import settings
from src.orb_system.models.data_models import ORBSetup, SignalType, TradingSignal
from src.orb_system.services.market_data import MarketDataService
from src.orb_system.core.orb_formation import ORBFormationAnalyzer
from src.orb_system.core.signal_generator import SignalGenerator

class BacktestingEngine:
    """Historical simulator for ORB strategy fine-tuning"""
    
    def __init__(self):
        self.mds = MarketDataService()
        self.orb_analyzer = ORBFormationAnalyzer(self.mds)
        self.signal_gen = SignalGenerator(self.mds)
        
    def run_backtest(self, 
                     symbols: List[str], 
                     start_date: date, 
                     end_date: date,
                     params: Dict = None) -> pd.DataFrame:
        """Run simulation across multiple symbols and dates"""
        logger.info(f"Starting ORB Backtest Simulation: {start_date} to {end_date}")
        results = []
        
        current_date = start_date
        while current_date <= end_date:
            if current_date.weekday() < 5: # Monday-Friday
                for symbol in symbols:
                    trade_result = self._simulate_day(symbol, current_date, params)
                    if trade_result:
                        results.append(trade_result)
            current_date += timedelta(days=1)
            
        return pd.DataFrame(results)

    def _simulate_day(self, symbol: str, trade_date: date, params: Dict) -> Optional[Dict]:
        """Simulate a single trading day for a symbol"""
        params = params or {}
        orb_window = params.get('orb_window', 30)
        orb_style = params.get('orb_style', 'STANDARD')
        sl_strategy = params.get('sl_strategy', 'ORB_STANDARD')
        
        # Extract preset specific filters
        presets = params.get('presets', {})
        range_filter = presets.get('range_pct')
        vol_quality_min = presets.get('vol_quality')
        
        # 1. Formation Phase
        setup = self.orb_analyzer.analyze_orb(
            symbol, trade_date, 
            orb_window_mins=orb_window, 
            orb_style=orb_style,
            range_filter=range_filter,
            vol_quality_min=vol_quality_min,
            min_quality=params.get('min_orb_quality')
        )
        if not setup or not setup.is_tradeable: return None
        
             # 2. Monitoring Phase
        try:
             # Fetch day's full 1-min data
             orb_start = datetime.combine(trade_date, settings.MARKET_OPEN)
             full_end = datetime.combine(trade_date, settings.MARKET_CLOSE)
             
             df = self.mds.get_historical_data(symbol, orb_start, full_end, interval="1")
             if df.empty: return None
             
             # Split into pre-orb and post-orb monitoring window
             # Respect preset timings if provided
             start_time_str = presets.get('start')
             end_time_str = presets.get('end')
             
             if start_time_str:
                 start_t = datetime.strptime(start_time_str, "%H:%M:%S").time()
                 monitor_start_dt = datetime.combine(trade_date, start_t)
             else:
                 monitor_start_dt = orb_start + timedelta(minutes=orb_window)
                 
             if end_time_str:
                 end_t = datetime.strptime(end_time_str, "%H:%M:%S").time()
                 monitor_end_dt = datetime.combine(trade_date, end_t)
             else:
                 monitor_end_dt = full_end
             
             monitor_df = df[(df['timestamp'] >= monitor_start_dt) & (df['timestamp'] <= monitor_end_dt)]
             
             for idx, row in monitor_df.iterrows():
                  signal = None
                  if row['high'] > setup.orb_high:
                       signal = self.signal_gen._generate_signal(
                           setup, SignalType.BREAKOUT, setup.orb_high + 0.05, row['timestamp'], 
                           df[df['timestamp'] <= row['timestamp']], 
                           sl_strategy=sl_strategy,
                           params=params # Pass full params for indicator overrides
                       )
                  elif row['low'] < setup.orb_low:
                       signal = self.signal_gen._generate_signal(
                           setup, SignalType.BREAKDOWN, setup.orb_low - 0.05, row['timestamp'], 
                           df[df['timestamp'] <= row['timestamp']], 
                           sl_strategy=sl_strategy,
                           params=params
                       )
                  
                  if signal:
                       outcome = self._simulate_lifecycle(signal, monitor_df[monitor_df['timestamp'] > row['timestamp']], setup, params=params)
                       return outcome
                       
        except Exception as e:
            logger.error(f"Backtest error for {symbol} on {trade_date}: {e}")
        return None

    def _simulate_lifecycle(self, signal: TradingSignal, future_df: pd.DataFrame, setup: ORBSetup, params: Dict = {}) -> Dict:
        """Simulate the trade with Partial Profits, Trailing SL, and EOD constraints"""
        entry_price = signal.entry_price
        sl = signal.stop_loss
        t1 = signal.target_1
        t2 = signal.target_2
        
        is_partial = params.get('partial_profits', False)
        trail_style = params.get('trail_style', 'None')
        eod_force = params.get('eod_exit', True)
        
        outcome = "OPEN"
        exit_price = entry_price
        exit_time = signal.signal_time + timedelta(hours=2)
        
        # Tracking for partial profits
        booked_t1 = False
        pnl_sum = 0.0
        
        # 3:15 PM EOD Cutoff
        eod_cutoff = datetime.combine(signal.signal_time.date(), time(15, 15))
        
        for idx, row in future_df.iterrows():
             # 0. EOD Enforcement
             if eod_force and row['timestamp'] >= eod_cutoff:
                  outcome = "EOD"
                  exit_price = row['close']
                  exit_time = row['timestamp']
                  break

             if signal.signal_type == SignalType.BREAKOUT:
                  # 1. Stop Loss Check
                  if row['low'] <= sl:
                       outcome = "SL"
                       exit_price = sl
                       exit_time = row['timestamp']
                       break
                  
                  # 2. Target 1 Check (Partial or Move SL)
                  if not booked_t1 and row['high'] >= t1:
                       if is_partial:
                            # Book 50% at T1
                            pnl_sum += 0.5 * ((t1 - entry_price) / entry_price * 100)
                            booked_t1 = True
                            logger.debug(f"Partial T1 booked for {signal.symbol}")
                       
                       # Move SL to Break-Even after T1
                       sl = entry_price
                       
                  # 3. Target 2 or Trailing SL
                  if row['high'] >= t2:
                       outcome = "T2"
                       exit_price = t2
                       exit_time = row['timestamp']
                       break
                       
                  # 4. Trailing Style Implementation
                  if booked_t1 or not is_partial:
                       if trail_style == "EMA-9":
                            # Use EMA-9 as dynamic SL (simplification: using a sliding window)
                            # In reality, needs full historical df for EMA, but we'll use a local calculation
                            pass # TODO: Implement EMA trail in future if needed
                       elif trail_style == "Candle-Step":
                            # Trail SL to previous candle low
                            sl = max(sl, row['low'])
                            
             else: # BREAKDOWN
                  if row['high'] >= sl:
                       outcome = "SL"
                       exit_price = sl
                       exit_time = row['timestamp']
                       break
                  
                  if not booked_t1 and row['low'] <= t1:
                       if is_partial:
                            pnl_sum += 0.5 * ((entry_price - t1) / entry_price * 100)
                            booked_t1 = True
                       sl = entry_price

                  if row['low'] <= t2:
                       outcome = "T2"
                       exit_price = t2
                       exit_time = row['timestamp']
                       break
                  
                  if booked_t1 or not is_partial:
                       if trail_style == "Candle-Step":
                            sl = min(sl, row['high'])

        if outcome == "OPEN" and not future_df.empty:
             outcome = "TIMEO" # Time stop
             exit_price = future_df.iloc[-1]['close']
             exit_time = future_df.iloc[-1]['timestamp']
             
        # Calculate Final PnL
        current_pnl = ((exit_price - entry_price) / entry_price) * 100
        if signal.signal_type == SignalType.BREAKDOWN: current_pnl = -current_pnl
        
        if is_partial and booked_t1:
             final_pnl = pnl_sum + (0.5 * current_pnl)
        else:
             final_pnl = current_pnl
             
        pnl_pct = ((exit_price - entry_price) / entry_price) * 100
        if signal.signal_type == SignalType.BREAKDOWN: pnl_pct = -pnl_pct
        
        # Enhanced result set for UI transparency
        return {
            "date": signal.signal_time.date(),
            "symbol": signal.symbol,
            "type": signal.signal_type.value,
            "entry_time": signal.signal_time.strftime("%H:%M"),
            "entry_price": round(entry_price, 2),
            "orb_high": setup.orb_high,
            "orb_low": setup.orb_low,
            "range_pct": setup.orb_range_pct,
            "sl": round(sl, 2),
            "sl_style": signal.sl_strategy, # Added style for clarity
            "target_1": round(signal.target_1, 2),
            "target_2": round(signal.target_2, 2),
            "exit_time": exit_time.strftime("%H:%M"),
            "exit_price": round(exit_price, 2),
            "outcome": outcome,
            "pnl_pct": round(final_pnl, 2),
            "conviction": signal.total_conviction,
            "breakdown": f"Q:{setup.orb_quality_score} C:{signal.confluence.total_score} R:{setup.market_regime_score}"
        }

