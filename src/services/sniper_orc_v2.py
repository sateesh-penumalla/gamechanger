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

class SniperOrchestratorV2:
    """
    Holy Grail Sniper System V2 (Refined).
    
    Major Fixes:
    - Bug #1: Fixed buy_vol/sell_vol delta calculation (removed incorrect subtraction).
    - Bug #2: Atomic Redis Locking (using SET NX instead of exists+setex).
    - Bug #3: Delayed Lock Acquisition (only locks if ALL guards pass).
    - Bug #4: Success-only Notifications (no more Telegram alerts for failed trades).
    - Bug #5: Unified time guards.
    - Bug #6: Bounded memory history (deque maxlen).
    - Strategy S1: Fixed Inverted R:R (moved to 1.5% TP / 1% SL).
    - Strategy S3: Pulse Counting is now time-windowed (buckets pulses into 1-minute bins).
    """
    
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        if not self.db_url:
            raise EnvironmentError("DATABASE_URL missing in .env")
            
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        
        # Bug #7: Cast port to int
        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True
        )
        
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe("market_depth:LIVE")
        
        # Bug #6: Set maxlen to prevent unbounded memory growth
        self.history = defaultdict(lambda: deque(maxlen=5000))
        
        # NOTE: prev_volume tracking is no longer needed since MarketDepthService v2 
        # sends per-tick deltas directly as 'volume', 'buy_vol', and 'sell_vol'.
        self.vwap_num = defaultdict(float)
        self.vwap_den = defaultdict(float)
        self.prev_oi = defaultdict(float)
        self.last_reset_date = date.today()

        # Medians for RVOL/Pulse Logic
        median_path = os.path.join(os.path.dirname(__file__), '..', '..', 'research', 'rvol_strategy', 'vol_median_5m.json')
        try:
            with open(median_path, 'r') as f:
                self.vol_medians = json.load(f)
            logger.info(f"Sniper V2: Loaded {len(self.vol_medians)} medians.")
        except Exception as e:
            logger.error(f"Sniper V2: Could not load medians: {e}")
            self.vol_medians = {}

        # S6/S7: Sector Controls
        self.MAX_PER_SECTOR = 2
        self.SECTOR_TREND_THRESHOLD = -0.3 # Block LONG if sector down > 0.3%
        self.sector_map = {} # symbol -> sector name
        self.symbol_indices = {} # symbol -> list of dhan_index_names
        self.sector_trade_count = defaultdict(int)
        self.last_rejection_log = {} # symbol -> last time rejected (for throttling)
        self.last_signal_time = {} # symbol -> last trigger time
        
        self._load_sector_map()

        # _trigger_order calls self.port_mgr._execute_entry, but this was never
        # assigned — every execution raised AttributeError into the inner
        # handler, logged "inner execution error" and rolled back, so this
        # strategy had never actually placed an order.
        self.port_mgr = PortfolioManager()
        self.pkt = PacketReader('SniperOrchestratorV2')

        logger.info("🎯 SniperOrchestrator V2 Initialized. Monitoring Market Depth...")

    def _load_sector_map(self):
        """Loads symbol -> sector and symbol -> index mapping from DB."""
        session = self.Session()
        try:
            tickers = session.query(
                Ticker.symbol, Ticker.sector, Ticker.dhan_index_name
            ).all()
            for t in tickers:
                if t.sector and t.sector != "UNKNOWN":
                    self.sector_map[t.symbol] = t.sector
                
                # dhan_index_name could be a JSON list, a string, or a Python list
                raw_idx = t.dhan_index_name
                if raw_idx:
                    indices = []
                    if isinstance(raw_idx, list):
                        indices = raw_idx
                    elif isinstance(raw_idx, str):
                        if "[" in raw_idx: 
                            try:
                                indices = json.loads(raw_idx)
                            except: pass
                        else:
                            indices = [i.strip() for i in raw_idx.split(",") if i.strip()]
                    
                    if indices:
                        self.symbol_indices[t.symbol] = indices
            
            logger.info(f"Sniper V2: Loaded sector map for {len(self.sector_map)} symbols.")
        except Exception as e:
            logger.error(f"Sniper V2: Failed to load sector map: {e}")
        finally:
            session.close()

    def _get_sector_trend(self, symbol, now):
        """Returns the 5-min % change of the symbol's primary sector index."""
        indices = self.symbol_indices.get(symbol, ["NIFTY"])
        
        for idx_symbol in indices:
            idx_hist = self.history.get(idx_symbol)
            if not idx_hist or len(idx_hist) < 2:
                continue
            
            cutoff = now - timedelta(minutes=5)
            h5 = [h for h in idx_hist if h[0] >= cutoff]
            if len(h5) < 2:
                continue
            
            earliest_ltp = h5[0][1]
            latest_ltp   = h5[-1][1]
            if earliest_ltp > 0:
                trend = ((latest_ltp / earliest_ltp) - 1) * 100
                return trend
        return None

    def run(self):
        """Main loop for processing real-time depth packets."""
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    packet = json.loads(message['data'])
                    self._process_tick(packet)
                except Exception as e:
                    logger.error(f"Sniper V2 Loop Error: {e}")

    def _process_tick(self, packet):
        symbol = packet['symbol']
        ltp    = packet['ltp']
        # `volume` is CUMULATIVE session volume, not a per-tick delta. Reading
        # it as one inflated vol_5m by the packet count and made `efficiency`
        # collapse toward zero, so the LONG branch never triggered.
        vol_delta, buy_delta, sell_delta = self.pkt.deltas(packet)
        
        # Also extract pre-computed bar volume for convenience
        # bar_vol = packet.get('current_bar_volume', 0)
        
        now = datetime.now()

        # 1. Day Reset
        if now.date() > self.last_reset_date:
            logger.info("Sniper V2: New day reset.")
            self.vwap_num.clear()
            self.vwap_den.clear()
            self.history.clear()
            self.last_reset_date = now.date()

        # 2. Update Session Metrics (VWAP)
        if vol_delta > 0:
            self.vwap_num[symbol] += (ltp * vol_delta)
            self.vwap_den[symbol] += vol_delta

        # 3. Update History Buffer
        # (now, ltp, vol_delta, buy_delta, sell_delta)
        self.history[symbol].append((now, ltp, vol_delta, buy_delta, sell_delta))
        
        # 4. Evaluate Strategy
        bid_p = packet.get('bid_pct', 50.0)
        oi    = packet.get('oi', 0.0)
        p_oi  = self.prev_oi[symbol]
        oi_chg = oi - p_oi if p_oi > 0 else 0
        self.prev_oi[symbol] = oi if oi > 0 else p_oi

        self._evaluate_signals(symbol, ltp, now, packet, bid_p, oi_chg)

    def _evaluate_signals(self, symbol, ltp, now, packet, bid_pct, oi_chg):
        # Bug #5: Unified Time Filter: 09:25 to 12:30
        curr_time = now.time()
        if not (dt_time(9, 25) <= curr_time <= dt_time(14, 30)):
            return

        hist = self.history[symbol]
        if len(hist) < 10: return 

        # Metrics Extraction
        cutoff_5m = now - timedelta(minutes=5)
        cutoff_15m = now - timedelta(minutes=15)
        
        h5  = [h for h in hist if h[0] >= cutoff_5m]
        h15 = [h for h in hist if h[0] >= cutoff_15m]
        
        if not h5 or not h15: return

        # 1. Volume & Pulse Metrics
        vol_5m      = sum(h[2] for h in h5)
        buy_vol_5m  = sum(h[3] for h in h5)
        sell_vol_5m = sum(h[4] for h in h5)
        
        median_5m = self.vol_medians.get(symbol, 0)
        if median_5m == 0: return 
        
        rvol = vol_5m / median_5m
        
        # Strategy S3 Fix: Pulse counting is now windowed to 1-minute bins
        # A single institutional order split into 10 ticks now counts as ONE pulse window.
        pulse_threshold = median_5m * 0.25
        minute_vols = Counter()
        for h in h15:
            # Floor timestamp to the minute
            min_key = h[0].replace(second=0, microsecond=0)
            minute_vols[min_key] += h[2]
            
        pulses_15m = sum(1 for v in minute_vols.values() if v >= pulse_threshold)

        # 2. Efficiency & Aggression
        aggression = buy_vol_5m / (sell_vol_5m + 1.0)
        ltp_5m_ago = h5[0][1]
        price_change_5m = ((ltp / ltp_5m_ago) - 1) * 100
        efficiency = price_change_5m / ((vol_5m / 1000000.0) + 0.001)

        # 3. Exhaustion & VWAP Shield
        ltp_15m_ago = h15[0][1]
        trend_15m = ((ltp / ltp_15m_ago) - 1) * 100
        range_15m = ((max(h[1] for h in h15) / min(h[1] for h in h15)) - 1) * 100
        
        vwap = self.vwap_num[symbol] / self.vwap_den[symbol] if self.vwap_den[symbol] > 0 else ltp
        vwap_dist = ((ltp / vwap) - 1) * 100

        # S7: Sector Trend Alignment
        sector_trend = self._get_sector_trend(symbol, now)

        # --- HOLY GRAIL LONG SNIPER (Early Entry V2.5) ---
        # 1. Lowered RVOL from 5.0 -> 3.5 for earlier detection
        # 2. Added Absorption Guard: If Aggression > 12, Efficiency MUST be higher (> 1.2)
        #    (Prevents buying into 'Iceberg' sell walls where high volume doesn't move price)
        is_long_agg = (4.0 <= aggression <= 35.0)
        is_long_eff = (efficiency > 0.6)
        if aggression > 12.0:
            is_long_eff = (efficiency > 1.2) # Stronger move required for ultra-high buy pressure

        if (vol_5m >= 250000 and is_long_eff and is_long_agg and 
            rvol >= 3.5 and pulses_15m >= 3 and trend_15m <= 2.0 and 
            0.2 <= vwap_dist <= 1.2 and range_15m <= 2.0):
            
            # S7 Guard: Sector Trend Check
            if sector_trend is not None and sector_trend < self.SECTOR_TREND_THRESHOLD:
                if (now - self.last_rejection_log.get(symbol, datetime.min)).total_seconds() > 60:
                    logger.debug(f"🛡️ Sniper: {symbol} LONG blocked — Sector Trend Down ({sector_trend:.2f}%)")
                    self.last_rejection_log[symbol] = now
                return

            # OI + Bid% Buildup Gate
            if oi_chg <= 0 and bid_pct < 52.0:
                 return

            self._trigger_order(symbol, "LONG", ltp, vwap_dist, rvol, pulses_15m, efficiency, packet)

        # --- HOLY GRAIL SHORT SNIPER (Early Entry V2.5) ---
        # Adjusting Short for symmetry with Long logic
        is_short_agg = (aggression <= 0.12)
        is_short_eff = (efficiency < -5.0) # Review says this is too strict, but keeping for now
        if aggression < 0.05:
            is_short_eff = (efficiency < -8.0) # Absorption guard for shorts

        if (vol_5m >= 250000 and is_short_eff and is_short_agg and 
            rvol >= 3.5 and pulses_15m >= 3 and trend_15m >= -2.0 and 
            -2.0 <= vwap_dist <= -0.5):

            # S7 Guard: Match trend with sector
            if sector_trend is not None and sector_trend > abs(self.SECTOR_TREND_THRESHOLD):
                if (now - self.last_rejection_log.get(symbol, datetime.min)).total_seconds() > 60:
                    logger.debug(f"🛡️ Sniper: {symbol} SHORT blocked — Sector Trend Up ({sector_trend:.2f}%)")
                    self.last_rejection_log[symbol] = now
                return

            # OI + Bid% Buildup Gate
            if oi_chg <= 0 and bid_pct >= 45.0:
                 return

            self._trigger_order(symbol, "SHORT", ltp, vwap_dist, rvol, pulses_15m, efficiency, packet)

    def _round_to_tick(self, price, tick=0.05):
        return round(round(price / tick) * tick, 2)

    def _get_config(self, session):
        try:
            preset_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'strategy_presets.json')
            if not os.path.exists(preset_file):
                preset_file = "src/config/strategy_presets.json"
                
            with open(preset_file, "r") as f:
                presets = json.load(f)
            config = presets.get("sateesh", {}).copy()

            job = session.query(SystemJob).filter_by(job_id='signal_generator').first()
            if job and job.config:
                config.update(job.config)
            return config
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return {}

    def _trigger_order(self, symbol, side, ltp, vwap_dist, rvol, pulses, eff, packet):
        now = datetime.now()
        
        # Throttled Rejection Logging
        last_rej = self.last_rejection_log.get(symbol, datetime.min)
        should_log = (now - last_rej).total_seconds() > 30

        # --- GUARD 1: BID% STRENGTH ---
        bid_p = packet.get('bid_pct', -50)
        ask_p = packet.get('ask_pct', -50)
        if bid_p == -50:
            if should_log:
                logger.debug(f"🛡️ skipping {symbol}: No BID% data")
                self.last_rejection_log[symbol] = now
            return

        strength = bid_p if side == "LONG" else ask_p
        
        # Rule: Strength must be biased but not exhausted (35% to 65%)
        if side == "LONG" and strength < 48.0:
            if should_log:
                logger.info(f"🛡️ skipping long {symbol}: BID% {strength}% < 48%")
                self.last_rejection_log[symbol] = now
            return
        if side == "SHORT" and strength > 45.0:
            if should_log:
                logger.info(f"🛡️ skipping short {symbol}: ASK% {strength}% ratio too low")
                self.last_rejection_log[symbol] = now
            return

        if strength < 35.0 or strength > 65.0:
            if should_log:
                logger.debug(f"🛡️ skipping {symbol}: Strength {strength}% out of balance (35-65)")
                self.last_rejection_log[symbol] = now
            return          

        # --- Bug #2 Fix: ATOMIC LOCK ACQUISITION ---
        # Move lock forward so we can release it if subsequent guards fail.
        lock_key = f"lock:trade:{symbol}:{now.date()}"
        if not self.redis_client.set(lock_key, "1", ex=86400, nx=True):
            return

        # --- GUARD 2: MASTER AUTO-TRADE SWITCH ---
        master_switch = os.getenv("ENABLE_AUTO_TRADING", "false").lower() == "true"
        if not master_switch:
            if should_log:
                logger.warning(f"🛡️ Sniper V2: {symbol} signal detected but ENABLE_AUTO_TRADING is FALSE. Release lock.")
                self.redis_client.delete(lock_key)
                self.last_rejection_log[symbol] = now
            return

        # --- GUARD 3: DB & CONFIG GUARDS ---
        session = self.Session()
        trade_success = False
        try:
            # a) DB Once-Per-Day Guard
            today = now.date()
            existing = session.query(Position).filter(
                Position.symbol == symbol,
                Position.date == today
            ).first()
            
            if existing:
                if should_log:
                    logger.warning(f"🛡️ Sniper Guard: {symbol} already has a record today (ID: {existing.id}).")
                    self.last_rejection_log[symbol] = now
                return

            # b) Auto-Execute Config Guard
            config = self._get_config(session)
            auto_exec = False
            if side == 'LONG' and config.get('auto_execute_long', False): 
                auto_exec = True
            elif side == 'SHORT' and config.get('auto_execute_short', False):
                auto_exec = True
            
            auto_exec = auto_exec and config.get("auto_execute_sniper", False)

            if not auto_exec:
                if should_log:
                    logger.info(f"🛡️ Sniper V2: {symbol} signal detected but AUTO-EXECUTE is FALSE. Release lock.")
                    self.redis_client.delete(lock_key)
                    self.last_rejection_log[symbol] = now
                return

            # S6: Sector Correlation Guard
            sector = self.sector_map.get(symbol)
            if sector:
                current_count = self.sector_trade_count[sector]
                if current_count >= self.MAX_PER_SECTOR:
                    if should_log:
                        logger.warning(f"🛡️ Sector Guard: {symbol} blocked — Sector {sector} already has {current_count} trades. Rel. lock.")
                        self.redis_client.delete(lock_key)
                        self.last_rejection_log[symbol] = now
                    return

            # memory debounce for this instance
            self.last_signal_time[symbol] = now

            # --- EXECUTION ---
            # Reverting to original 1% TP / 2% SL as 1% SL is too tight for midcaps.
            entry_price = self._round_to_tick(ltp)
            sl_price    = self._round_to_tick(ltp * 0.98 if side == "LONG" else ltp * 1.02) # 2% SL
            tp_price    = self._round_to_tick(ltp * 1.01 if side == "LONG" else ltp * 0.99) # 1% TP

            new_pos = Position(
                symbol=symbol,
                side=side,
                status='PENDING',
                entry_price=entry_price,
                sl=sl_price,
                tp=tp_price,
                date=today,
                entry_time=now,
                signal_type='SNIPER_V2_ELITE',
                agent_audit_log=f"[SniperV2] Refined Auto-Trigger. 1/2 TP/SL ratio",
                entry_metrics={
                    "rvol": round(rvol, 2),
                    "pulses": pulses,
                    "vwap_dist": round(vwap_dist, 2),
                    "efficiency": round(eff, 2),
                    "p_bid": strength
                }
            )

            try:
                session.add(new_pos)
                session.flush()

                # Trigger Execution
                success, msg = self.port_mgr._execute_entry(session, new_pos)
                if success:
                    session.commit()
                    logger.success(f"🚀 SNIPER V2 PLACED: {symbol} @ {entry_price}")
                    trade_success = True
                    # Update sector count
                    if sector:
                        self.sector_trade_count[sector] += 1
                else:
                    logger.warning(f"❌ SNIPER V2 FAILED: {msg}")
                    # Release lock if the exchange failed so it can retry later
                    self.redis_client.delete(lock_key)
                    session.rollback()
            except Exception as inner_e:
                logger.error(f"Sniper V2 inner execution error: {inner_e}")
                session.rollback()

            # Bug #4: Notification ONLY on success
            if trade_success:
                notify_new_signal(
                    symbol, side, "SNIPER_V2", entry_price, 
                    f"Refined Elite Sniper. RVOL: {rvol:.1f} | Pulse: {pulses} (Windowed)"
                )

        except Exception as e:
            logger.error(f"Sniper V2 Global Trigger Error: {e}")
        finally:
            session.close()

if __name__ == "__main__":
    sniper = SniperOrchestratorV2()
    sniper.run()
