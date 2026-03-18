import os
import sys
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, date, time as dt_time, timedelta
from collections import defaultdict, deque

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)


class OrcBacktestOrchestrator:
    def __init__(self, symbol, confirm_pct=0.20, confirm_time=30, tick_window=100, regime_filter=None, start_time=None):
        self.symbol = symbol
        self.last_signal_time = {}
        self.confirm_pct = confirm_pct
        self.confirm_time = confirm_time
        self.start_time = start_time          # datetime.time: no signals before this
        self.regime_filter = regime_filter
        self.pending_confirmation = {}
        
        self.price_history = deque(maxlen=tick_window)
        self.vol_history = deque(maxlen=tick_window)
        self.prev_volume = 0
        self.vwap_num = 0.0
        self.vwap_den = 0.0
        
        self.momentum_vol_surge = 12.0 
        self.momentum_vqs = 0.70      
        self.vwap_deviation_pct = 0.025 
        self.vwap_vol_surge = 12.0 
        
        self.signals_generated = []
        self.active_trades = []
        self.completed_trades = []

    def _evaluate_active_trades(self, current_time, ltp):
        still_active = []
        for trade in self.active_trades:
            side = trade['side']
            tp = trade['tp']
            sl = trade['sl']
            entry = trade['entry']
            
            closed = False
            if side == 'LONG':
                if ltp >= tp:
                    trade['exit_price'] = ltp
                    trade['pnl_pct'] = (ltp - entry) / entry * 100
                    trade['result'] = 'WIN'
                    trade['exit_time'] = current_time
                    self.completed_trades.append(trade)
                    closed = True
                elif ltp <= sl:
                    trade['exit_price'] = ltp
                    trade['pnl_pct'] = (ltp - entry) / entry * 100
                    trade['result'] = 'LOSS'
                    trade['exit_time'] = current_time
                    self.completed_trades.append(trade)
                    closed = True
            elif side == 'SHORT':
                if ltp <= tp:
                    trade['exit_price'] = ltp
                    trade['pnl_pct'] = (entry - ltp) / entry * 100
                    trade['result'] = 'WIN'
                    trade['exit_time'] = current_time
                    self.completed_trades.append(trade)
                    closed = True
                elif ltp >= sl:
                    trade['exit_price'] = ltp
                    trade['pnl_pct'] = (entry - ltp) / entry * 100
                    trade['result'] = 'LOSS'
                    trade['exit_time'] = current_time
                    self.completed_trades.append(trade)
                    closed = True
                    
            if not closed:
                still_active.append(trade)
        self.active_trades = still_active

    def _update_local_metrics(self, packet):
        ltp = packet['ltp']
        current_time = packet['timestamp']
        if hasattr(current_time, "to_pydatetime"):
            current_time = current_time.to_pydatetime()
            
        self._evaluate_active_trades(current_time, ltp)

        current_v = packet.get('volume', 0)
        prev_v = self.prev_volume
        
        if current_v > prev_v:
            diff = current_v - prev_v
        elif current_v > 0 and prev_v == 0:
            diff = current_v
        else:
            diff = 0
            
        if diff > 0:
            self.vwap_num += (ltp * diff)
            self.vwap_den += diff
            self.vol_history.append(diff)
        
        self.prev_volume = current_v

        den = self.vwap_den
        local_vwap = self.vwap_num / den if den > 0 else ltp
        
        self.price_history.append(ltp)
        history = list(self.price_history)
        if len(history) > 1:
            ticks = []
            for i in range(1, len(history)):
                if history[i] > history[i-1]: ticks.append(1)
                elif history[i] < history[i-1]: ticks.append(-1)
            local_vqs = sum(ticks) / len(ticks) if ticks else 0.0
        else:
            local_vqs = 0.0

        v_hist = list(self.vol_history)
        avg_v = sum(v_hist) / len(v_hist) if v_hist else 0.0
        last_v = v_hist[-1] if v_hist else 0.0
        local_surge = last_v / avg_v if avg_v > 0 else 1.0

        packet['vwap'] = local_vwap
        packet['vqs_score'] = local_vqs
        packet['vol_surge'] = local_surge
        return packet

    def process_tick(self, packet):
        packet = self._update_local_metrics(packet)
        ltp = packet['ltp']
        current_time = packet['timestamp']
        if hasattr(current_time, "to_pydatetime"):
            current_time = current_time.to_pydatetime()
            
        self._detect_momentum_surge(ltp, packet, current_time)
        self._check_pending_confirmations(ltp, packet, current_time)

    def _detect_momentum_surge(self, ltp, packet, current_time):
        # Opening filter: skip if before the allowed start time
        if self.start_time and current_time.time() < self.start_time:
            return

        vol_surge = packet.get('vol_surge', 1.0)
        vqs_score = packet.get('vqs_score', 0.0)
        vwap = packet.get('vwap', ltp)
        
        if vol_surge >= self.momentum_vol_surge:
            side_candidate = None
            if vqs_score >= self.momentum_vqs and ltp > vwap:
                side_candidate = "LONG"
            elif vqs_score <= -self.momentum_vqs and ltp < vwap:
                side_candidate = "SHORT"
                
            if side_candidate and self.symbol not in self.pending_confirmation:
                self.pending_confirmation[self.symbol] = {
                    "side":          side_candidate,
                    "trigger_price": ltp,
                    "trigger_time":  current_time,
                    "packet":        packet.copy()
                }

    def _check_pending_confirmations(self, ltp, packet, current_time):
        if self.symbol not in self.pending_confirmation: return

        pending = self.pending_confirmation[self.symbol]
        elapsed = (current_time - pending['trigger_time']).total_seconds()

        if elapsed > self.confirm_time:
            del self.pending_confirmation[self.symbol]
            return

        trigger_price = pending['trigger_price']
        side          = pending['side']
        required_move = trigger_price * (self.confirm_pct / 100.0)

        confirmed = (
            (side == "LONG"  and ltp >= trigger_price + required_move) or
            (side == "SHORT" and ltp <= trigger_price - required_move)
        )

        if confirmed:
            # --- Market Regime Gate ---
            if self.regime_filter:
                regime = self.regime_filter(current_time)
                if side == 'LONG' and regime == 'BEAR':
                    del self.pending_confirmation[self.symbol]
                    return  # Don't take LONG on a BEAR market day
                if side == 'SHORT' and regime == 'BULL':
                    del self.pending_confirmation[self.symbol]
                    return  # Don't take SHORT on a BULL market day

            del self.pending_confirmation[self.symbol]
            self._evaluate_trigger(side, ltp, pending['packet'], current_time)

    def _evaluate_trigger(self, side, ltp, packet, current_time):
        # Debounce (1 per 5 mins)
        debounce_key = f"{self.symbol}_MOMENTUM_SQUEEZE"
        last_t = self.last_signal_time.get(debounce_key, datetime.min)
        if hasattr(current_time, "to_pydatetime"):
            current_time = current_time.to_pydatetime()
            
        if (current_time - last_t).total_seconds() < 300:
            return

        # PRECISION BALANCE: 35% - 65% Filter
        bid_p = packet.get('bid_pct', 50)
        ask_p = packet.get('ask_pct', 50)
        strength = bid_p if side == "LONG" else ask_p
        
        # Note: This is an exact mirror of orc.py logic, which prevents
        # order flow dominance > 65% from firing.
        if strength < 35.0 or strength > 65.0:
            return

        self._generate_signal(side, ltp, packet, current_time)
        self.last_signal_time[debounce_key] = current_time

    def _generate_signal(self, side, ltp, packet, current_time):
        sl_buffer = ltp * 0.02    # 2% SL
        sl = ltp - sl_buffer if side == "LONG" else ltp + sl_buffer
            
        tp_buffer = ltp * 0.01    # 1% TP
        tp = ltp + tp_buffer if side == "LONG" else ltp - tp_buffer

        trade = {
            'time': current_time,
            'side': side,
            'entry': ltp,
            'sl': sl,
            'tp': tp,
            'vol_surge': packet.get('vol_surge'),
            'vwap': packet.get('vwap'),
            'status': 'OPEN'
        }
        self.signals_generated.append(trade)
        self.active_trades.append(trade)


def run_orc_backtest(parquet_path: str, quiet=False, confirm_pct=0.20, confirm_time=30, tick_window=100, regime_filter=None, start_time=None):
    df = pd.read_parquet(parquet_path)
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(by='timestamp').reset_index(drop=True)
    
    if df.empty:
        return [], []

    symbol = df['symbol'].iloc[0]
    orchestrator = OrcBacktestOrchestrator(
        symbol,
        confirm_pct=confirm_pct,
        confirm_time=confirm_time,
        tick_window=tick_window,
        regime_filter=regime_filter,
        start_time=start_time
    )
    
    records = df.to_dict('records')
    for packet in records:
        orchestrator.process_tick(packet)

    if not quiet:
        logger.info(f"ORC Backtest Complete. Total Signals: {len(orchestrator.signals_generated)}")
        for sig in orchestrator.signals_generated:
            logger.info(f"  -> [{symbol}] {sig['side']} @ {sig['entry']} ({sig['time']})")
            
    return orchestrator.signals_generated, orchestrator.completed_trades

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('parquet', type=str, help='Path to parquet file')
    args = parser.parse_args()
    
    run_orc_backtest(args.parquet)
