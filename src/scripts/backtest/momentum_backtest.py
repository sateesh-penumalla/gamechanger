import os
import sys
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, date, time as dt_time, timedelta
from collections import defaultdict, deque

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# Parameters from orcpure.py
MOMENTUM_SL_PCT = 0.8  
MOMENTUM_TP_PCT = 2.0  
MIN_AVG_VOL_FLOOR = 250
MOMENTUM_DEBOUNCE_SECS = 180   
TOD_WINDOWS = [
    (dt_time(10,  0), dt_time(11, 30), 1.0),   
    (dt_time(11, 30), dt_time(13,  0), 1.3),   
    (dt_time(13,  0), dt_time(14,  0), 1.2),   
    (dt_time(14,  0), dt_time(14, 30), 1.0),   
]
PRICE_CONFIRM_PCT  = 0.20   
CONFIRMATION_WINDOW_SECS = 30
ICEBERG_ALIGNMENT_SURGE_FACTOR = 0.85  
ICEBERG_BLOCK_WINDOW_SECS = 120   
MOMENTUM_START_TIME = dt_time(10, 0)
MOMENTUM_END_TIME   = dt_time(14, 30)

class BacktestOrchestrator:
    def __init__(self, symbol, vol_surge_req=8.0, vqs_req=0.75, imb_req=0.35, confirm_pct=0.20, confirm_time=30):
        self.symbol = symbol
        self.active_blocks    = {}
        self.absorption_stats = {}  # {price: {cum_vol, refill_count, last_ts, side}}
        self.last_signal_time = {}
        self.prev_state       = None
        
        self.session_avg_tick_vol = defaultdict(float)
        self.pending_confirmation = {}
        
        # We must recalculate vol_surge and vqs because historical parquet data 
        # prior to our bugfix had them hardcoded to 1.0 and 0.0 respectively
        from collections import deque
        self.vol_history = deque(maxlen=100)
        self.price_history = deque(maxlen=100)
        self.prev_volume = 0
        
        self.momentum_vol_surge = vol_surge_req
        self.momentum_vqs       = vqs_req
        self.imbalance_req      = imb_req
        self.confirm_pct        = confirm_pct
        self.confirm_time       = confirm_time
        self.nifty_vqs          = 0.0 # Mocking global market as neutral for now
        self.refill_multiplier  = 20.0
        self.open_prices = {}
        
        self.signals_generated = []
        self.active_trades = []
        self.completed_trades = []

    def _get_tod_surge_multiplier(self, current_time) -> float:
        now = current_time.time()
        for start, end, multiplier in TOD_WINDOWS:
            if start <= now < end:
                return multiplier
        return 1.0

    def _get_active_icebergs(self, current_time) -> list:
        result = []
        for price, stats in self.absorption_stats.items():
            age = (current_time - stats.get('last_ts', current_time)).total_seconds()
            if age <= ICEBERG_BLOCK_WINDOW_SECS:
                result.append({
                    "price":        price,
                    "side":         stats['side'],
                    "refill_count": stats['refill_count'],
                    "cum_vol":      stats['cum_vol'],
                })
        return result

    def _check_iceberg_gate(self, side: str, current_time) -> tuple:
        icebergs = self._get_active_icebergs(current_time)
        if not icebergs:
            return False, False, ""

        blocking_side = "SELL" if side == "LONG" else "BUY"
        aligning_side = "BUY"  if side == "LONG" else "SELL"

        is_blocked = False
        is_aligned = False
        block_reason = ""

        for ice in icebergs:
            if ice['side'] == blocking_side and ice['refill_count'] >= 5:
                is_blocked   = True
                block_reason = f"active {ice['side']} iceberg @ {ice['price']} (count={ice['refill_count']})"
                break
            if ice['side'] == aligning_side and ice['refill_count'] >= 3:
                is_aligned = True

        return is_blocked, is_aligned, block_reason

    def _get_dynamic_floor(self, symbol: str, current_avg_tick_vol: float) -> float:
        if current_avg_tick_vol > 0:
            prev = self.session_avg_tick_vol[symbol]
            if prev == 0:
                self.session_avg_tick_vol[symbol] = current_avg_tick_vol
            else:
                alpha = 0.02
                self.session_avg_tick_vol[symbol] = alpha * current_avg_tick_vol + (1 - alpha) * prev
        
        session_avg = self.session_avg_tick_vol[symbol]
        dynamic_floor = session_avg * 0.25 if session_avg > 0 else 0.0
        return max(MIN_AVG_VOL_FLOOR, dynamic_floor)

    def _evaluate_active_trades(self, packet):
        ltp = packet['ltp']
        current_time = packet['timestamp']
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

    def _update_metrics(self, packet):
        ltp = packet['ltp']
        current_v = packet.get('volume', 0)
        prev_v = self.prev_volume
        
        diff = current_v - prev_v if current_v > prev_v else (current_v if current_v > 0 and prev_v == 0 else 0)
            
        if diff > 0:
            self.vol_history.append(diff)
        
        self.prev_volume = current_v
        
        self.price_history.append(ltp)
        history = list(self.price_history)
        if len(history) > 1:
            ticks = [1 if history[i] > history[i-1] else (-1 if history[i] < history[i-1] else 0) for i in range(1, len(history))]
            local_vqs = sum(ticks) / len(ticks) if ticks else 0.0
        else:
            local_vqs = 0.0

        v_hist = list(self.vol_history)
        avg_v = sum(v_hist) / len(v_hist) if v_hist else 0.0
        last_v = v_hist[-1] if v_hist else 0.0
        local_surge = last_v / avg_v if avg_v > 0 else 1.0

        packet['vqs_score'] = local_vqs
        packet['vol_surge'] = local_surge
        return packet

    def process_tick(self, packet):
        packet = self._update_metrics(packet)
        ltp = packet['ltp']
        current_time = packet['timestamp']
        
        if self.symbol not in self.open_prices and current_time.time() >= dt_time(9, 15):
            self.open_prices[self.symbol] = ltp

        current_vol = packet.get('current_bar_volume', 0)
        
        # Iceberg detection requires past state
        if self.prev_state and current_vol > self.prev_state.get('current_bar_volume', 0):
            self._detect_refills(packet, self.prev_state, current_time)

        packet['fs_imbalance'] = packet.get('imbalance', 0.0)
        
        self._detect_momentum_surge(ltp, packet, current_time)
        self._check_pending_confirmations(ltp, packet, current_time)
        
        self.prev_state = packet.copy()

    def _detect_refills(self, current, prev, current_time):
        ltp       = current['ltp']
        vol_delta = current['current_bar_volume'] - prev['current_bar_volume']

        prev_bids = prev.get('bids', [])
        prev_asks = prev.get('asks', [])
        
        if isinstance(prev_bids, np.ndarray): prev_bids = prev_bids.tolist()
        if isinstance(prev_asks, np.ndarray): prev_asks = prev_asks.tolist()

        prev_bid_p = prev_bids[0].get('price', 0) if len(prev_bids)>0 and isinstance(prev_bids[0], dict) else 0
        prev_ask_p = prev_asks[0].get('price', 0) if len(prev_asks)>0 and isinstance(prev_asks[0], dict) else 0

        is_buy_trade  = (ltp >= prev_ask_p) if prev_ask_p > 0 else False
        is_sell_trade = (ltp <= prev_bid_p) if prev_bid_p > 0 else False

        if not (is_buy_trade or is_sell_trade): return

        target_price = ltp
        wall_side    = "SELL" if is_buy_trade else "BUY"

        curr_depth = current.get('asks' if is_buy_trade else 'bids', [])
        prev_depth = prev.get('asks' if is_buy_trade else 'bids', [])
        
        if isinstance(curr_depth, np.ndarray): curr_depth = curr_depth.tolist()
        if isinstance(prev_depth, np.ndarray): prev_depth = prev_depth.tolist()

        curr_qty = next((x.get('qty', 0) for x in curr_depth if isinstance(x, dict) and x.get('price') == target_price), 0)
        prev_qty = next((x.get('qty', 0) for x in prev_depth if isinstance(x, dict) and x.get('price') == target_price), 0)

        expected_qty = max(0, prev_qty - vol_delta)

        if curr_qty > expected_qty:
            if target_price not in self.absorption_stats:
                self.absorption_stats[target_price] = {"cum_vol": 0, "refill_count": 0, "side": wall_side}

            stats = self.absorption_stats[target_price]
            stats['cum_vol'] += vol_delta
            stats['refill_count'] += 1
            stats['last_ts'] = current_time

        # Cleanup old stats
        for price in list(self.absorption_stats.keys()):
            if (current_time - self.absorption_stats[price]['last_ts']).total_seconds() > 300:
                del self.absorption_stats[price]

    def _detect_momentum_surge(self, ltp, packet, current_time):
        vol_delta = packet.get('volume', 0) - self.prev_state.get('volume', 0) if self.prev_state else 0
        if vol_delta <= 0: return

        if current_time.time() < MOMENTUM_START_TIME or current_time.time() > MOMENTUM_END_TIME:
            return

        vol_surge = packet.get('vol_surge', 1.0)
        vqs_score = packet.get('vqs_score', 0.0)
        vwap      = packet.get('vwap', ltp)

        # Pass the real rolling average of the tick volume delta
        avg_tick_vol  = packet.get('current_bar_volume', 0) / 100 if packet.get('current_bar_volume', 0) > 0 else 0 
        
        dynamic_floor = self._get_dynamic_floor(self.symbol, avg_tick_vol)
        if avg_tick_vol < dynamic_floor: return

        local_imbalance = packet.get('fs_imbalance', 0.0)

        vwap_dist_pct = abs(ltp - vwap) / vwap * 100 if vwap > 0 else 0
        if vwap_dist_pct > 1.5: return
        extension_penalty = max(0.0, (vwap_dist_pct - 0.5) * 6.0)
        required_surge    = self.momentum_vol_surge + extension_penalty

        tod_multiplier  = self._get_tod_surge_multiplier(current_time)
        required_surge *= tod_multiplier

        side_candidate = None
        if vqs_score >= self.momentum_vqs and ltp > vwap and local_imbalance >= self.imbalance_req:
            side_candidate = "LONG"
        elif vqs_score <= -self.momentum_vqs and ltp < vwap and local_imbalance <= -self.imbalance_req:
            side_candidate = "SHORT"

        if side_candidate is None: return

        is_blocked, is_aligned, block_reason = self._check_iceberg_gate(side_candidate, current_time)

        if is_blocked: return

        if is_aligned:
            required_surge *= ICEBERG_ALIGNMENT_SURGE_FACTOR

        if vol_surge >= required_surge:
            if self.symbol not in self.pending_confirmation:
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
            del self.pending_confirmation[self.symbol]
            self._evaluate_trigger(side, ltp, packet, current_time)

    def _evaluate_trigger(self, side, ltp, packet, current_time):
        debounce_key = f"{self.symbol}_MOMENTUM_SQUEEZE"
        last_t = self.last_signal_time.get(debounce_key, datetime.min)
        if (current_time - last_t).total_seconds() < MOMENTUM_DEBOUNCE_SECS:
            return

        open_p = self.open_prices.get(self.symbol)
        if open_p:
            move_pct = ((ltp - open_p) / open_p) * 100
            if side == "LONG" and move_pct > 2.0: return
            if side == "SHORT" and move_pct < -2.0: return

        bid_p    = packet.get('bid_pct', 50.0)
        ask_p    = packet.get('ask_pct', 50.0)
        strength = bid_p if side == "LONG" else ask_p

        # If data is completely missing (50.0 flat), ignore the strength filter for backtesting
        if strength != 50.0 and strength < 55.0: return

        self._generate_signal(side, ltp, packet, current_time)
        self.last_signal_time[debounce_key] = current_time

    def _generate_signal(self, side, ltp, packet, current_time):
        sl_buffer = ltp * (MOMENTUM_SL_PCT / 100.0)
        sl = ltp - sl_buffer if side == "LONG" else ltp + sl_buffer
        tp_buffer = ltp * (MOMENTUM_TP_PCT / 100.0)
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
        logger.info(f"🔥 MOMENTUM SQUEEZE: {self.symbol} {side} @ {ltp} SL: {sl:.2f} TP: {tp:.2f} Time: {current_time.time()}")

def run_backtest(parquet_path: str, quiet=False, vol_surge_req=8.0, vqs_req=0.75, imb_req=0.35, confirm_pct=0.20, confirm_time=30):
    if not quiet: logger.info(f"Loading {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    
    # We must sort by timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(by='timestamp').reset_index(drop=True)
    
    if df.empty:
        if not quiet: logger.warning("Parquet is empty")
        return []

    symbol = df['symbol'].iloc[0]
    if not quiet: logger.info(f"Running backtest for {symbol} | Total Ticks: {len(df)}")
    
    orchestrator = BacktestOrchestrator(
        symbol, 
        vol_surge_req=vol_surge_req, 
        vqs_req=vqs_req, 
        imb_req=imb_req,
        confirm_pct=confirm_pct,
        confirm_time=confirm_time
    )
    
    # Simulate avg tick vol simply by doing diffs if avg_tick_vol is not in parquet
    if 'volume' not in df.columns:
        if 'buy_vol' in df.columns and 'sell_vol' in df.columns:
            df['volume'] = df['buy_vol'] + df['sell_vol']
        else:
            df['volume'] = 0

    if 'avg_tick_vol' not in df.columns:
        df['vol_delta'] = df['volume'].diff().fillna(0)
        df['total_vol_approx_avg'] = df['vol_delta'].rolling(100).mean().fillna(300)

    records = df.to_dict('records')
    for packet in records:
        # Suppress internal orchestrator logs in quiet mode if needed, 
        # but for now we'll just let it print actual signals.
        orchestrator.process_tick(packet)

    if not quiet:
        logger.info(f"Backtest Complete. Total Signals: {len(orchestrator.signals_generated)}")
        for sig in orchestrator.signals_generated:
            logger.info(f"  -> {sig['side']} @ {sig['entry']} ({sig['time']})")
            
    return orchestrator.signals_generated, orchestrator.completed_trades

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('parquet', type=str, help='Path to parquet file')
    args = parser.parse_args()
    
    run_backtest(args.parquet)
