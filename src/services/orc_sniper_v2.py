import os
import redis
import json
import pandas as pd
import numpy as np
import time as py_time
from datetime import datetime, date, time as dt_time, timedelta
from collections import defaultdict, deque, Counter
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Project Imports
from src.db.schema import Position, IntradayTick, SystemJob, Ticker
from src.services.portfolio_manager import PortfolioManager
from src.utils.notifications import notify_new_signal
from src.utils.depth_packet import PacketReader

load_dotenv()

class OrderFlowSniperV2:
    """
    Orc Sniper V2 (The Merge).
    
    Combines 'Holy Grail' V2's robust infrastructure with 'Improved' orc_sniper.py's
    multi-bar accumulation features.
    
    Key Features:
    - Bug #1: Fixes buy/sell delta math.
    - Bug #2: Atomic Redis locking (SET NX).
    - Bug #3: Active Lock Release on config skip.
    - Bug #4: Success-only notifications.
    - S3: Windowed pulse counting (1-min bins).
    - S6/S7: Sector Correlation & Trend Alignment guards.
    - Directionality Fix: Coupled Efficiency & Aggression (No more LONG on Selling).
    - Absorption Guard: Higher efficiency required for ultra-high aggression spikes.
    - Multi-Bar Accumulation: Uses magic_volume.json for 1m/3m surge detection.
    """
    
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        if not self.db_url:
            raise EnvironmentError("DATABASE_URL missing in .env")
            
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        
        # Redis setup
        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True
        )
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe("market_depth:LIVE")
        
        # State & History
        self.history = defaultdict(lambda: deque(maxlen=5000))
        
        # Multi-bar accum state (from Improved orc_sniper)
        self.bar_data = defaultdict(lambda: {
            'start_time': None,
            'open_p': 0,
            'current_vol': 0,
            'current_buy_vol': 0,
            'current_sell_vol': 0,
            'prev_1m_vols': deque(maxlen=15),
            'prev_1m_buy_vols': deque(maxlen=15),
            'prev_1m_sell_vols': deque(maxlen=15),
            'prev_opens': deque(maxlen=15)
        })

        self.vwap_num = defaultdict(float)
        self.vwap_den = defaultdict(float)
        self.prev_oi = defaultdict(float)
        self.last_reset_date = date.today()

        # Load Magic Numbers
        self._load_magic_numbers()

        # S6/S7: Sector Controls
        self.MAX_PER_SECTOR = 2
        self.SECTOR_TREND_THRESHOLD = -0.3
        self.sector_map = {} 
        self.symbol_indices = {} 
        self.sector_trade_count = defaultdict(int)
        
        # Index Tracking for S7 (NIFTY/BANKNIFTY etc)
        self.known_indices = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTY_BANK"}
        self._load_sector_map()

        self.last_signal_time = {}
        self.last_rejection_log = {} 
        self.port_mgr = PortfolioManager()
        self.pkt = PacketReader('OrderFlowSniperV2')

        logger.success("🎯 Orc Sniper V2.5 Initialized (Holy Grail Mode). Monitoring Flux...")

    def _load_magic_numbers(self):
        m_path = os.path.join(os.path.dirname(__file__), '..', '..', 'magic_volume.json')
        if not os.path.exists(m_path):
             m_path = "magic_volume.json"
        
        try:
            with open(m_path, 'r') as f:
                self.magic_data = json.load(f)
            logger.info(f"Sniper V2: Loaded {len(self.magic_data)} magic thresholds.")
        except Exception as e:
            logger.warning(f"Sniper V2: magic_volume.json not loaded: {e}")
            self.magic_data = {}

    def _load_sector_map(self):
        session = self.Session()
        try:
            tickers = session.query(Ticker.symbol, Ticker.sector, Ticker.dhan_index_name).all()
            for t in tickers:
                if t.sector and t.sector != "UNKNOWN":
                    self.sector_map[t.symbol] = t.sector
                if t.dhan_index_name:
                    indices = []
                    if isinstance(t.dhan_index_name, list):
                        indices = t.dhan_index_name
                    elif isinstance(t.dhan_index_name, str):
                        indices = [i.strip() for i in t.dhan_index_name.replace("[","").replace("]","").replace("\"","").split(",")]
                    if indices:
                        self.symbol_indices[t.symbol] = indices
                        self.known_indices.update(indices)
            logger.info(f"Sniper V2: Loaded sector map for {len(self.sector_map)} symbols.")
        except Exception as e:
            logger.error(f"Sniper V2: Failed to load sector map: {e}")
        finally:
            session.close()

    def run(self):
        """Main loop."""
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    packet = json.loads(message['data'])
                    self._process_tick(packet)
                except Exception as e:
                    logger.error(f"Sniper V2 Global Error: {e}")

    def _process_tick(self, packet):
        symbol = packet['symbol']
        ltp    = packet['ltp']
        now    = datetime.now()

        # Day Reset
        if now.date() > self.last_reset_date:
            logger.info("Sniper V2: Day Reset.")
            self.vwap_num.clear(); self.vwap_den.clear(); self.history.clear()
            self.bar_data.clear(); self.sector_trade_count.clear()
            self.last_reset_date = now.date()

        # 1. Update Index History (Optimized for S7)
        if symbol in self.known_indices:
            self.history[symbol].append((now, ltp))
            return # Index ticks don't trigger signals

        # 2. Extract per-tick deltas.
        # `volume` in the packet is CUMULATIVE session volume — reading it as a
        # delta (as this file used to) inflated 5-minute sums by the packet
        # count, driving efficiency to ~0 so the LONG branch could never fire.
        # PacketReader is the single place that knows the field contract.
        v_delta, b_delta, s_delta = self.pkt.deltas(packet)
        vwap_live = float(packet.get('vwap', ltp))

        # 3. Bar Management (Improved logic)
        state = self.bar_data[symbol]
        if not state['start_time'] or (now - state['start_time']).total_seconds() >= 60:
            # Shift completed bar to history
            if state['start_time']:
                state['prev_1m_vols'].append(state['current_vol'])
                state['prev_1m_buy_vols'].append(state['current_buy_vol'])
                state['prev_1m_sell_vols'].append(state['current_sell_vol'])
                state['prev_opens'].append(state['open_p'])
            
            # Reset bar accumulators
            state['start_time'] = now
            state['open_p']     = ltp
            state['current_vol'] = 0; state['current_buy_vol'] = 0; state['current_sell_vol'] = 0

        # Accumulate
        state['current_vol']     += v_delta
        state['current_buy_vol']  += b_delta
        state['current_sell_vol'] += s_delta
        
        # VWAP Session tracking
        if v_delta > 0:
            self.vwap_num[symbol] += (ltp * v_delta)
            self.vwap_den[symbol] += v_delta

        # 4. History for Metrics
        self.history[symbol].append((now, ltp, v_delta, b_delta, s_delta))
        
        # 5. Signal Evaluation
        bid_pct = packet.get('bid_pct', 50.0)
        oi      = packet.get('oi', 0.0)
        p_oi    = self.prev_oi[symbol]
        oi_chg  = oi - p_oi if p_oi > 0 else 0
        self.prev_oi[symbol] = oi if oi > 0 else p_oi

        self._evaluate_signals(symbol, ltp, now, packet, bid_pct, oi_chg)

    def _evaluate_signals(self, symbol, ltp, now, packet, bid_pct, oi_chg):
        # Time Guard: 09:31 AM (Skip opening auction noise as per Improved orc_sniper)
        curr_time = now.time()
        if not (dt_time(9, 31) <= curr_time <= dt_time(14, 30)):
            return

        state = self.bar_data[symbol]
        if len(state['prev_1m_vols']) < 2: return # Need at least some bars

        # --- A. Accumulation Surges (Improved Feature) ---
        thresh = self.magic_data.get(symbol, {})
        v1m_limit = thresh.get('high_conviction_magic', 1_000_000_000)
        v3m_limit = thresh.get('high_conviction_magic_3m', 1_000_000_000)
        
        recent_3m_vol = state['current_vol'] + sum(list(state['prev_1m_vols'])[-2:])
        hit_v = (state['current_vol'] >= v1m_limit) or (recent_3m_vol >= v3m_limit)
        
        if not hit_v: 
            return # Skip if no magic accumulation surge detected (strictly following backtest)

        # --- B. Advanced Alpha Filters (Combined & Corrected) ---
        # Fixed Directionality Bug: Coupling Efficiency & Aggression
        hist = [h for h in self.history[symbol] if (now - h[0]).total_seconds() <= 300] # last 5m
        vol_5m = sum(h[2] for h in hist)
        buy_vol_5m  = sum(h[3] for h in hist)
        sell_vol_5m = sum(h[4] for h in hist)
        aggression = buy_vol_5m / (sell_vol_5m + 1.0)
        
        ltp_5m_ago = hist[0][1] if hist else ltp
        price_change_5m = ((ltp / ltp_5m_ago) - 1) * 100
        # Efficiency = movement per 1M shares
        efficiency = price_change_5m / ((vol_5m / 1000000.0) + 0.001)

        # Pulse Gate (Windowed 1-min buckets)
        minute_vols = Counter()
        for h in hist:
            minute_vols[h[0].replace(second=0, microsecond=0)] += h[2]
        
        # Use magic threshold as pulse baseline if medians missing
        pulse_thresh = v1m_limit * 0.3
        pulses = sum(1 for v in minute_vols.values() if v >= pulse_thresh)

        # VWAP Shield
        vwap = self.vwap_num[symbol] / self.vwap_den[symbol] if self.vwap_den[symbol] > 0 else ltp
        vwap_dist = ((ltp / vwap) - 1) * 100

        # Anti-Exhaustion
        range_15m = 0
        h15 = [h for h in self.history[symbol] if (now - h[0]).total_seconds() <= 900]
        if h15:
            high = max(h[1] for h in h15)
            low  = min(h[1] for h in h15)
            range_15m = ((high / low) - 1) * 100

        # --- D. Directional Logic (The Big Fix) ---
        is_long, is_short = False, False

        # LONG Logic (Coupled)
        # Absorption Guard: Aggression > 12 requires 2x Efficiency
        long_eff_req = 0.6 if aggression <= 12.0 else 1.2
        if (efficiency > long_eff_req and 4.0 <= aggression <= 35.0 and 
            pulses >= 3 and 0.2 <= vwap_dist <= 1.2 and range_15m <= 2.2):
            is_long = True

        # SHORT Logic (Coupled)
        short_eff_req = -5.0 if aggression >= 0.05 else -8.0
        if (efficiency < short_eff_req and aggression <= 0.10 and 
            pulses >= 3 and -2.2 <= vwap_dist <= -0.5 and range_15m <= 2.2):
            is_short = True

        if is_long:
            # OI + Bid% Synergy (from improved code logic)
            # If no OI buildup, require a stronger bid book (> 52%)
            if oi_chg <= 0 and bid_pct < 52.0:
                return
            self._trigger_order(symbol, "LONG", ltp, vwap_dist, aggression, pulses, efficiency, packet)
        elif is_short:
            # For Short: No OI buildup + strong bid book (>= 45%) is risky
            if oi_chg <= 0 and bid_pct >= 45.0:
                return
            self._trigger_order(symbol, "SHORT", ltp, vwap_dist, aggression, pulses, efficiency, packet)

    def _trigger_order(self, symbol, side, ltp, vwap_dist, aggression, pulses, efficiency, packet):
        now = datetime.now()
        
        # Throttled rejection logs
        last_rej = self.last_rejection_log.get(symbol, datetime.min)
        should_log = (now - last_rej).total_seconds() > 30

        # S7: Sector Trend Alignment Guard
        indices = self.symbol_indices.get(symbol, ["NIFTY"])
        sector_trend = None
        for idx in indices:
            idx_hist = self.history.get(idx)
            if idx_hist and len(idx_hist) >= 2:
                # 5-min index trend
                cutoff = now - timedelta(minutes=5)
                idx_5m = [h for h in idx_hist if h[0] >= cutoff]
                if len(idx_5m) >= 2:
                    sector_trend = ((idx_5m[-1][1] / idx_5m[0][1]) - 1) * 100
                    break
        
        if side == "LONG" and sector_trend is not None and sector_trend < self.SECTOR_TREND_THRESHOLD:
             if should_log:
                 logger.debug(f"🛡️ Sniper V2 Sector Block: {symbol} LONG vs Sector {sector_trend:.2f}%")
                 self.last_rejection_log[symbol] = now
             return
        if side == "SHORT" and sector_trend is not None and sector_trend > abs(self.SECTOR_TREND_THRESHOLD):
             if should_log:
                 logger.debug(f"🛡️ Sniper V2 Sector Block: {symbol} SHORT vs Sector {sector_trend:.2f}%")
                 self.last_rejection_log[symbol] = now
             return

        # S6: Sector Correlation Guard
        sector = self.sector_map.get(symbol)
        if sector and self.sector_trade_count[sector] >= self.MAX_PER_SECTOR:
             if should_log:
                 logger.warning(f"🛡️ Sector Count Block: {symbol} in {sector} (Max 2 reached)")
                 self.last_rejection_log[symbol] = now
             return

        # Atomic Lock Acquisition (SET NX)
        lock_key = f"lock:trade:{symbol}:{now.date()}"
        if not self.redis_client.set(lock_key, "1", ex=86400, nx=True):
            return

        # Config Guards (with Lock Release)
        master_switch = os.getenv("ENABLE_AUTO_TRADING", "false").lower() == "true"
        if not master_switch:
            self.redis_client.delete(lock_key)
            return

        with self.Session() as session:
            # Once-per-day DB Guard
            today = now.date()
            existing = session.query(Position).filter(Position.symbol == symbol, Position.date == today).first()
            if existing:
                self.redis_client.setex(lock_key, 86400, "1") # Re-lock for day
                return

            # Auto-execute Check
            preset_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'strategy_presets.json')
            try:
                with open(preset_file, "r") as f:
                    presets = json.load(f)
                config = presets.get("sateesh", {}).copy()
                job = session.query(SystemJob).filter_by(job_id='signal_generator').first()
                if job and job.config: config.update(job.config)
            except: config = {}

            auto_exec = config.get("auto_execute_sniper", False)
            if side == "LONG": auto_exec = auto_exec and config.get("auto_execute_long", False)
            else: auto_exec = auto_exec and config.get("auto_execute_short", False)

            if not auto_exec:
                self.redis_client.delete(lock_key)
                return

            # --- EXECUTION ---
            entry_price = round(round(ltp / 0.05) * 0.05, 2)
            sl_price    = round(round((ltp * 0.98 if side == "LONG" else ltp * 1.02) / 0.05) * 0.05, 2)
            tp_price    = round(round((ltp * 1.01 if side == "LONG" else ltp * 0.99) / 0.05) * 0.05, 2)

            new_pos = Position(
                symbol=symbol,
                side=side,
                status='PENDING',
                entry_price=entry_price,
                sl=sl_price,
                tp=tp_price,
                date=today,
                entry_time=now,
                signal_type='ORC_SNIPER_V2',
                entry_metrics={
                    "rvol_m": round(ltp, 2), # Placeholder or actual rvol calculation
                    "aggression": round(aggression, 2),
                    "efficiency": round(efficiency, 2),
                    "pulses": pulses,
                    "model": "Holy Grail V2.5 Merge"
                }
            )
            
            try:
                session.add(new_pos)
                session.flush()
                success, msg = self.port_mgr._execute_entry(session, new_pos)
                if success:
                    session.commit()
                    logger.success(f"🚀 ORC V2 PLACED: {symbol} @ {entry_price}")
                    if sector: self.sector_trade_count[sector] += 1
                    notify_new_signal(symbol, side, "ORC_V2", entry_price, f"V2 Merge. Agg: {aggression:.2f} | Eff: {efficiency:.2f}")
                else:
                    logger.warning(f"❌ ORC V2 FAILED: {msg}")
                    self.redis_client.delete(lock_key)
                    session.rollback()
            except Exception as e:
                logger.error(f"Execution failed: {e}")
                self.redis_client.delete(lock_key)
                session.rollback()

if __name__ == "__main__":
    sniper = OrderFlowSniperV2()
    sniper.run()
