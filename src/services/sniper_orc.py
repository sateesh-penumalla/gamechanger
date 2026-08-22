import os
import redis
import json
import pandas as pd
import numpy as np
from datetime import datetime, date, time as dt_time, timedelta
from collections import defaultdict, deque
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Project Imports
from src.db.schema import Position, IntradayTick, SystemJob
from src.services.portfolio_manager import PortfolioManager
from src.utils.notifications import notify_new_signal
from src.utils.depth_packet import PacketReader

load_dotenv()

class SniperOrchestrator:
    """
    Holy Grail Sniper System.
    Listens to market_depth:LIVE and identifies ultra-high conviction entries.
    Implements the 'Elite V2' logic: 
    - 2.5L Volume Gate
    - 3+ Pulse (0.25x Median)
    - Anti-Exhaustion (Trend < 2%)
    - VWAP Magnet (0.2% - 1.2% dev)
    - PnL: 1% TP / 2% SL
    """
    
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        
        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=os.getenv("REDIS_PORT", 6379),
            decode_responses=True
        )
        
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe("market_depth:LIVE")
        
        # State: symbol -> deque of (timestamp, ltp, total_vol, buy_vol, sell_vol)
        self.history = defaultdict(lambda: deque())
        self.vwap_num = defaultdict(float)
        self.vwap_den = defaultdict(float)
        self.last_reset_date = date.today()

        # Medians for RVOL/Pulse Logic
        median_path = os.path.join(os.path.dirname(__file__), '..', '..', 'research', 'rvol_strategy', 'vol_median_5m.json')
        try:
            with open(median_path, 'r') as f:
                self.vol_medians = json.load(f)
            logger.info(f"Sniper: Loaded {len(self.vol_medians)} medians for RVOL logic.")
        except Exception as e:
            logger.error(f"Sniper: Could not load medians from {median_path}: {e}")
            self.vol_medians = {}

        # Tracking for signal debouncing
        self.last_signal_time = {}
        self.last_rejection_log = {} # To throttle "skipping" logs
        self.port_mgr = PortfolioManager()
        self.pkt = PacketReader('SniperOrchestrator')

        logger.info("🎯 SniperOrchestrator Initialized (Elite V2 Mode). Monitoring Redis Ticks...")

    def run(self):
        """Main loop for processing real-time depth packets."""
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    packet = json.loads(message['data'])
                    self._process_tick(packet)
                except Exception as e:
                    logger.error(f"Sniper Loop Error: {e}")

    def _process_tick(self, packet):
        symbol = packet['symbol']
        ltp = packet['ltp']
        now = datetime.now()

        # 1. Day Reset
        if now.date() > self.last_reset_date:
            logger.info("Sniper: New day detected. Resetting session metrics.")
            self.vwap_num.clear()
            self.vwap_den.clear()
            self.history.clear()
            self.last_reset_date = now.date()

        # 2. Per-tick deltas.
        # buy_vol/sell_vol in the packet are ALREADY per-publish deltas — the
        # service resets them after every packet. Differencing them against a
        # previous value (as this did) produced ~0 for both, so `aggression`
        # was meaningless. vol_delta likewise comes straight from the packet.
        vol_delta, buy_delta, sell_delta = self.pkt.deltas(packet)

        # Update Session Metrics
        if vol_delta > 0:
            self.vwap_num[symbol] += (ltp * vol_delta)
            self.vwap_den[symbol] += vol_delta

        # 3. Update History Buffer
        self.history[symbol].append((now, ltp, vol_delta, buy_delta, sell_delta))
        
        # Prune older than 20 mins
        cutoff = now - timedelta(minutes=20)
        while self.history[symbol] and self.history[symbol][0][0] < cutoff:
            self.history[symbol].popleft()

        # 4. Evaluate Strategy
        self._evaluate_signals(symbol, ltp, now, packet)

    def _evaluate_signals(self, symbol, ltp, now, packet):
        # Time Filter: 09:25 to 12:30
        curr_time = now.time()
        if not (dt_time(9, 25) <= curr_time <= dt_time(12, 30)):
            return

        hist = self.history[symbol]
        if len(hist) < 10: return # Build some history first

        # Metrics Extraction
        cutoff_5m = now - timedelta(minutes=5)
        cutoff_15m = now - timedelta(minutes=15)
        
        h5 = [h for h in hist if h[0] >= cutoff_5m]
        h15 = [h for h in hist if h[0] >= cutoff_15m]
        
        if not h5 or not h15: return

        # 1. Volume & Pulse Metrics
        vol_5m = sum(h[2] for h in h5)
        buy_vol_5m = sum(h[3] for h in h5)
        sell_vol_5m = sum(h[4] for h in h5)
        
        median_5m = self.vol_medians.get(symbol, 0)
        if median_5m == 0: return # Skip if no baseline
        
        rvol = vol_5m / median_5m
        pulse_threshold = median_5m * 0.25
        pulses_15m = sum(1 for h in h15 if h[2] >= pulse_threshold)

        # 2. Efficiency & Aggression
        aggression = buy_vol_5m / (sell_vol_5m + 1)
        ltp_5m_ago = h5[0][1]
        price_change_5m = ((ltp / ltp_5m_ago) - 1) * 100
        efficiency = price_change_5m / ((vol_5m / 1000000.0) + 0.001)

        # 3. Exhaustion & VWAP Shield
        ltp_15m_ago = h15[0][1]
        trend_15m = ((ltp / ltp_15m_ago) - 1) * 100
        range_15m = ((max(h[1] for h in h15) / min(h[1] for h in h15)) - 1) * 100
        
        vwap = self.vwap_num[symbol] / self.vwap_den[symbol] if self.vwap_den[symbol] > 0 else ltp
        vwap_dist = ((ltp / vwap) - 1) * 100

        # --- MARKET OPEN GUARD ---
        # Backtest (High Conviction) only scans after 09:25 AM. 
        # Opening volume 9:15-9:25 is an outlier and produces 200+ false signals.
        now_time = datetime.now().time()
        if dt_time(9, 15) <= now_time < dt_time(9, 25):
            return

        # --- HOLY GRAIL LONG SNIPER ---
        if (vol_5m >= 250000 and efficiency > 0.6 and 4.0 <= aggression <= 35.0 and 
            rvol >= 5.0 and pulses_15m >= 3 and trend_15m <= 2.0 and 
            0.2 <= vwap_dist <= 1.2 and range_15m <= 2.0):
            self._trigger_order(symbol, "LONG", ltp, vwap_dist, rvol, pulses_15m, efficiency, packet)

        # --- HOLY GRAIL SHORT SNIPER ---
        elif (vol_5m >= 250000 and efficiency < -5.0 and aggression <= 0.10 and 
              rvol >= 5.0 and pulses_15m >= 3 and trend_15m >= -2.0 and 
              -2.0 <= vwap_dist <= -0.5):
            self._trigger_order(symbol, "SHORT", ltp, vwap_dist, rvol, pulses_15m, efficiency, packet)

    def _round_to_tick(self, price, tick=0.05):
        """Round price to the nearest tick size (NSE default 0.05)."""
        return round(round(price / tick) * tick, 2)

    def _get_config(self, session):
        """Loads strategy presets and merges with DB overrides (using signal_generator job ID as central config)."""
        try:
            # 1. Load Strategy Presets
            preset_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'strategy_presets.json')
            with open(preset_file, "r") as f:
                presets = json.load(f)
            config = presets.get("sateesh", {}).copy()

            # 2. Merge with DB Job Config (Overrides) - Borrowing from signal_generator job config
            from src.db.schema import SystemJob
            job = session.query(SystemJob).filter_by(job_id='signal_generator').first()
            if job and job.config:
                config.update(job.config)
            
            return config
        except Exception as e:
            logger.error(f"Error loading Orchestrator config: {e}")
            return {}


    def _trigger_order(self, symbol, side, ltp, vwap_dist, rvol, pulses, eff, packet):
        now = datetime.now()
        
        # 1. Redis Global Lock (Absolute Once-Per-Day per Stock)
        lock_key = f"lock:trade:{symbol}:{now.date()}"
        if self.redis_client.exists(lock_key):
            return

        # 2. Memory Debounce (Strict Once per Day per Stock)
        last_s = self.last_signal_time.get(symbol, datetime.min)
        if last_s.date() >= now.date():
            return

        if ltp > 2500:
            return
        
        # --- PRECISION BALANCE: 35% - 65% Filter ---
        bid_p = packet.get('bid_pct', -50)
        ask_p = packet.get('ask_pct', -50)

        # Log Throttling (Show rejection only once every 30 seconds per symbol)
        last_rej = self.last_rejection_log.get(symbol, datetime.min)
        should_log = (now - last_rej).total_seconds() > 30

        if bid_p == -50 or ask_p == -50:
            if should_log:
                logger.info(f"🛡️ skipping signal for {symbol} : {ltp} : NO BID% DATA")
                self.last_rejection_log[symbol] = now
            return

        strength = bid_p if side == "LONG" else ask_p
        
        if side == "LONG" and strength < 48.0:
            if should_log:
                logger.info(f"🛡️ skipping long signal for {symbol} : {ltp} : BID% < 53% its {strength}%")
                self.last_rejection_log[symbol] = now
            return
        if side == "SHORT" and strength > 45.0:
            if should_log:
                logger.info(f"🛡️ skipping short signal for {symbol} : {ltp} : BID% > 45% its {strength}%")
                self.last_rejection_log[symbol] = now
            return

        if strength < 35.0 or strength > 65.0:
            if should_log:
                logger.info(f"🛡️ skipping signal for {symbol} : {ltp} : BID% is either less than 35 or more than 65. its {strength}%")
                self.last_rejection_log[symbol] = now
            return          
   


        session = self.Session()
        try:
            # 3. DB Guard (Absolute One-Trade-Per-Day per Stock for TODAY)
            today = now.date()
            existing = session.query(Position).filter(
                Position.symbol == symbol,
                Position.date == today
            ).first()
            
            if existing:
                # If we already have a record for today (OPEN, CLOSED, PENDING, EXPIRED, etc.)
                # we do NOT create another one. Zero-Loss Strategy: Max 1 trade/stock/day.
                self.redis_client.setex(lock_key, 86400, "1") # Sync Redis
                self.last_signal_time[symbol] = now # Sync memory
                logger.warning(f"🛡️ Sniper Guard: {symbol} already has a record for today (ID: {existing.id}, Status: {existing.status}). Entry blocked.")
                return

            # SET LOCK IMMEDIATELY to prevent race conditions
            self.redis_client.setex(lock_key, 86400, "1")
            self.last_signal_time[symbol] = now

            # 4. Final Guard Check & Initialization
            logger.success(f"🎯 ELITE SNIPER {side}: {symbol} @ {ltp} | RVOL: {rvol:.1f} | Pulse: {pulses} | Dist: {vwap_dist:.2f}%")
            
            # 1% TP, 2% SL
            ltp = self._round_to_tick(ltp)
            sl_price = self._round_to_tick(ltp * 0.98 if side == "LONG" else ltp * 1.02)
            tp_price = self._round_to_tick(ltp * 1.01 if side == "LONG" else ltp * 0.99)

            # Auto-Execute Check (Zero-Loss Protocol Rule 3)
            master_switch = os.getenv("ENABLE_AUTO_TRADING", "false").lower() == "true"
            if not master_switch:
                logger.warning(f"🛡️ Sniper: {symbol} signal detected but ENABLE_AUTO_TRADING is FALSE in .env. Skipping.")
                return

            auto_exec = False
            try:
                preset_file = os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'config', 'strategy_presets.json')
                if not os.path.exists(preset_file):
                     # Fallback to local path relative to this file
                     preset_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'strategy_presets.json')
                
                with open(preset_file, "r") as f:
                    presets = json.load(f)

                p = self._get_config(session)
                auto_exec = False
                if side == 'LONG' and p.get('auto_execute_long', False): 
                    auto_exec = True
                elif side == 'SHORT' and p.get('auto_execute_short', False):
                    auto_exec = True

                auto_exec = auto_exec and p.get("auto_execute_sniper", False)
                
            except Exception as e:
                logger.warning(f"Sniper: Could not load auto_execute config: {e}. Defaulting to FALSE.")

            if not auto_exec:
                logger.info(f"🛡️ Sniper: {symbol} {side} signal detected but AUTO-EXECUTE is FALSE. Skipping position creation.")
                return        

            new_pos = Position(
                symbol=symbol,
                side=side,
                status='PENDING',
                entry_price=ltp,
                sl=sl_price,
                tp=tp_price,
                date=today,
                entry_time=now,
                signal_type='ELITE_SNIPER_V2',
                sl_type="ORB_BOUNDARY",
                agent_audit_log=f"[SniperV2] Automated execution trigger for sniper signal",
                entry_metrics={
                    "rvol": round(rvol, 2),
                    "pulses": pulses,
                    "vwap_dist": round(vwap_dist, 2),
                    "efficiency": round(eff, 2),
                    "model": "Holy Grail V2 (23% Profit Config)"
                }
            )
            try:
                session.add(new_pos)
                session.flush()

                # Trigger PortfolioManager Execution Logic
                success, msg = self.port_mgr._execute_entry(session, new_pos)
                if success:
                     logger.success(f"✅ SNIPER ORDER PLACED: {symbol} @ {ltp}")
                else:
                     logger.warning(f"❌ SNIPER ORDER FAILED: {msg}")

                session.commit()
            except Exception as e:
                logger.error(f"Sniper execution error: {e}")
                session.rollback()

            self.last_signal_time[symbol] = datetime.now()
            notify_new_signal(symbol, side, "SNIPER_V2", ltp, f"Elite V2 Sniper. WinRate Prop: 73%. RVOL: {rvol:.1f}")
        finally:
            session.close()

if __name__ == "__main__":
    sniper = SniperOrchestrator()
    sniper.run()
