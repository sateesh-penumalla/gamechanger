import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, time as dt_time, timedelta
from collections import defaultdict, deque

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
logger = logging.getLogger(__name__)

class CumulativeFlowOrchestrator:
    def __init__(self, symbol, window_secs=60, min_vol_ratio=0.7, min_vqs=0.3, sl_pct=0.5, tp_pct=1.0):
        self.symbol = symbol
        self.window_secs = window_secs
        self.min_vol_ratio = min_vol_ratio
        self.min_vqs = min_vqs
        self.sl_pct = sl_pct
        self.tp_pct = tp_pct
        
        self.prev_volume = 0
        self.prev_ltp = 0
        self.last_trade_side = 'NEUTRAL'
        
        self.rolling_ticks = deque()
        self.price_history = deque(maxlen=100)
        
        self.active_trades = []
        self.completed_trades = []
        self.signals_generated = []
        self.last_signal_time = datetime.min
        
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

    def process_tick(self, packet):
        ltp = packet['ltp']
        current_time = packet['timestamp']
        if isinstance(current_time, pd.Timestamp):
            current_time = current_time.to_pydatetime()
            
        self._evaluate_active_trades(current_time, ltp)
        
        current_v = packet.get('volume', 0)
        diff = current_v - self.prev_volume if current_v > self.prev_volume else (current_v if current_v > 0 and self.prev_volume == 0 else 0)
        self.prev_volume = current_v
        
        if ltp > self.prev_ltp:
            trade_side = 'BUY'
        elif ltp < self.prev_ltp:
            trade_side = 'SELL'
        else:
            trade_side = self.last_trade_side
            
        self.prev_ltp = ltp
        self.last_trade_side = trade_side
        
        self.rolling_ticks.append({
            'time': current_time,
            'vol': diff,
            'side': trade_side,
            'ltp': ltp
        })
        
        while self.rolling_ticks and (current_time - self.rolling_ticks[0]['time']).total_seconds() > self.window_secs:
            self.rolling_ticks.popleft()
            
        self.price_history.append(ltp)
        history = list(self.price_history)
        if len(history) > 1:
            ticks = [1 if history[i] > history[i-1] else (-1 if history[i] < history[i-1] else 0) for i in range(1, len(history))]
            local_vqs = sum(ticks) / len(ticks) if ticks else 0.0
        else:
            local_vqs = 0.0
            
        if (current_time - self.last_signal_time).total_seconds() < 300: # 5 min debounce
            return
            
        if current_time.time() < dt_time(9, 15) or current_time.time() > dt_time(15, 0):
            return
            
        # Analyze cumulative window
        buy_vol = sum(t['vol'] for t in self.rolling_ticks if t['side'] == 'BUY')
        sell_vol = sum(t['vol'] for t in self.rolling_ticks if t['side'] == 'SELL')
        total_vol = buy_vol + sell_vol
        
        # Require minimum liquidity in the window (e.g. at least 5000 volume)
        if total_vol < 5000:
            return
            
        buy_ratio = buy_vol / total_vol if total_vol > 0 else 0
        sell_ratio = sell_vol / total_vol if total_vol > 0 else 0
        
        vwap = packet.get('vwap', ltp)
        
        signal_side = None
        if buy_ratio >= self.min_vol_ratio and local_vqs >= self.min_vqs and ltp > vwap:
            signal_side = 'LONG'
        elif sell_ratio >= self.min_vol_ratio and local_vqs <= -self.min_vqs and ltp < vwap:
            signal_side = 'SHORT'
            
        if signal_side:
            sl_buffer = ltp * (self.sl_pct / 100.0)
            sl = ltp - sl_buffer if signal_side == "LONG" else ltp + sl_buffer
            tp_buffer = ltp * (self.tp_pct / 100.0)
            tp = ltp + tp_buffer if signal_side == "LONG" else ltp - tp_buffer
            
            trade = {
                'time': current_time,
                'side': signal_side,
                'entry': ltp,
                'sl': sl,
                'tp': tp,
                'buy_ratio': buy_ratio,
                'sell_ratio': sell_ratio,
                'vqs': local_vqs,
                'status': 'OPEN'
            }
            self.signals_generated.append(trade)
            self.active_trades.append(trade)
            self.last_signal_time = current_time

def run_cumulative_backtest(parquet_path: str, quiet=False, **kwargs):
    df = pd.read_parquet(parquet_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(by='timestamp').reset_index(drop=True)
    
    if df.empty: return [], []
    
    symbol = df['symbol'].iloc[0]
    orchestrator = CumulativeFlowOrchestrator(symbol, **kwargs)
    
    records = df.to_dict('records')
    for packet in records:
        orchestrator.process_tick(packet)
        
    return orchestrator.signals_generated, orchestrator.completed_trades
