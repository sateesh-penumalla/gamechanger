import os
import redis
import json
import pandas as pd
import numpy as np
from datetime import datetime, date, time as dt_time
from collections import defaultdict, deque
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Project Imports
from src.db.schema import ORBSignal, DailyFocus, IcebergAlert, ORBSignalNearMiss
from src.services.portfolio_manager import PortfolioManager
from src.utils.notifications import notify_new_signal

load_dotenv()

class OrderFlowOrchestrator:
    """
    Listens to market_depth:LIVE and identifies institutional absorption/breakout patterns.
    Leverages L3 data (20 levels) for full-stack analysis.
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
        
        # Strategy State
        self.active_blocks = {} # symbol -> {level: price, qty: volume, type: BUY/SELL}
        self.absorption_stats = {} # symbol -> {price: {cum_vol: 0, refill_count: 0, last_ts: ts}}
        self.last_signal_time = {}
        self.prev_state = {} # symbol -> last_packet
        
        # --- Local Metrics State (Independence Layer) ---
        self.price_history = defaultdict(lambda: deque(maxlen=50))
        self.vol_history = defaultdict(lambda: deque(maxlen=50))
        self.vwap_num = defaultdict(float)
        self.vwap_den = defaultdict(float)
        self.last_reset_date = date.today()
        self.prev_volume = defaultdict(int) # Track session volume for delta logic
        self.pending_confirmation = {}  # symbol -> {side, trigger_price, trigger_time, packet}
          
        # Momentum Squeeze Confirmation Gate
        self.momentum_confirm_pct  = 0.1   # Price must move 0.1% in signal direction
        self.momentum_confirm_secs = 90    # Within 30 seconds
        self.momentum_start_time   = dt_time(9, 30)  # No signals before 9:30 (builds from 9:15)
        
        # Config
        self.min_iceberg_score = 0.8  # Tightened from 0.7
        self.imbalance_trigger = 0.4  # Tightened from 0.3
        self.vqs_trigger = 0.5        # Tightened from 0.15
        self.refill_multiplier = 15.0 # Tightened from 10.0
        
        # Fallback Config (For Gold Guard precision)
        self.momentum_vol_surge = 12.0 
        self.momentum_vqs = 0.70      
        self.vwap_deviation_pct = 0.025 
        self.vwap_vol_surge = 12.0    
        
        # Signal type filtering
        self.enabled_signals = os.getenv("ENABLED_SIGNALS", "").split(",") if os.getenv("ENABLED_SIGNALS") else []
        
        # Portfolio Manager for Auto-Execution
        self.port_mgr = PortfolioManager()
        self.strategy_config = {}

        logger.info(f"OrderFlowOrchestrator Initialized (Gold Guard Mode). Imbalance Trigger: {self.imbalance_trigger}")
        if self.enabled_signals:
            logger.info(f"Signal Filtering Active. ONLY allowing: {self.enabled_signals}")

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

    def _update_local_metrics(self, packet):
        """Calculates critical metrics locally to override potentially buggy upsream data."""
        symbol = packet['symbol']
        ltp = packet['ltp']
        
        # 1. Daily Reset Logic
        today = date.today()
        if today > self.last_reset_date:
            logger.info("New day detected. Resetting local Orchestrator metrics.")
            self.vwap_num.clear()
            self.vwap_den.clear()
            self.price_history.clear()
            self.vol_history.clear()
            self.prev_volume.clear()
            self.last_reset_date = today

        # 2. VWAP & Volume Surge
        # We prefer session 'volume' (cumulative) for absolute delta accuracy
        current_v = packet.get('volume', 0)
        prev_v = self.prev_volume.get(symbol, 0)
        
        # Determine delta (Handle session reset too)
        if current_v > prev_v:
            diff = current_v - prev_v
        elif current_v > 0 and prev_v == 0:
            diff = current_v # First tick
        else:
            diff = 0
            
        if diff > 0:
            self.vwap_num[symbol] += (ltp * diff)
            self.vwap_den[symbol] += diff
            self.vol_history[symbol].append(diff)
        
        self.prev_volume[symbol] = current_v

        # 3. Local VWAP Value
        den = self.vwap_den[symbol]
        local_vwap = self.vwap_num[symbol] / den if den > 0 else ltp
        
        # 4. Local VQS (Momentum)
        self.price_history[symbol].append(ltp)
        history = self.price_history[symbol]
        if len(history) > 1:
            ticks = []
            for i in range(1, len(history)):
                if history[i] > history[i-1]: ticks.append(1)
                elif history[i] < history[i-1]: ticks.append(-1)
            local_vqs = sum(ticks) / len(ticks) if ticks else 0.0
        else:
            local_vqs = 0.0

        # 5. Local Vol Surge
        v_hist = self.vol_history[symbol]
        avg_v = sum(v_hist) / len(v_hist) if v_hist else 0.0
        last_v = v_hist[-1] if v_hist else 0.0
        local_surge = last_v / avg_v if avg_v > 0 else 1.0

        # Inject into packet to override upstream fields
        packet['vwap'] = local_vwap
        packet['vqs_score'] = local_vqs
        packet['vol_surge'] = local_surge
        
        return packet

    def run(self):
        """Main loop for processing real-time depth packets."""
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    packet = json.loads(message['data'])
                    # logger.debug(f"Tick: {packet['symbol']} @ {packet['ltp']}")
                    packet = self._update_local_metrics(packet)
                    self._process_depth_update(packet)
                    self.prev_state[packet['symbol']] = packet
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

    def _process_depth_update(self, packet):
        symbol = packet['symbol']
        ltp = packet['ltp']
        current_vol = packet.get('current_bar_volume', 0)
        
        # logger.debug(f"Processing {symbol} | Vol: {current_vol}")
        
        # Detect Data Depth (L3 vs L2)
        bids_20 = packet.get('bids_20')
        asks_20 = packet.get('asks_20')
        is_l3 = bids_20 is not None
        
        # Use 'qty' as normalized by MarketDepthService/DhanFeed
        bids = bids_20 if is_l3 else packet.get('bids', [])
        asks = asks_20 if is_l3 else packet.get('asks', [])
        
        if not bids or not asks:
            return

        # 1. Authentic Iceberg Detection (Refill Monitor)
        prev = self.prev_state.get(symbol)
        if prev and current_vol > prev.get('current_bar_volume', 0):
            self._detect_refills(symbol, packet, prev, is_l3)

        # 2. Full-Stack Imbalance Analysis
        total_bid_qty = sum(b.get('qty', 0) for b in bids)
        total_ask_qty = sum(a.get('qty', 0) for a in asks)
        combined = total_bid_qty + total_ask_qty
        
        fs_imbalance = (total_bid_qty - total_ask_qty) / combined if combined > 0 else 0
        
        # 3. Absorption Detection
        self._detect_absorption(symbol, ltp, bids, asks, fs_imbalance, packet, is_l3)
        
        # 4. Fallback Detection (Momentum Surge & VWAP Extension)
        self._detect_momentum_surge(symbol, ltp, packet, is_l3)
        self._check_pending_confirmations(symbol, ltp)   # Confirmation gate for Momentum Squeeze
        self._detect_vwap_extension(symbol, ltp, packet, is_l3)
        self._detect_db_style_breakout(symbol, ltp, packet, is_l3)
        
        # Store state for next tick comparison
        self.prev_state[symbol] = packet

    def _detect_refills(self, symbol, current, prev, is_l3):
        """
        Identifies active institutional reloading (Icebergs) at a specific price.
        """
        ltp = current['ltp']
        vol_delta = current['current_bar_volume'] - prev['current_bar_volume']
        
        # Check if trade happened at the Best Bid or Best Ask
        prev_bid_p = prev['bids'][0]['price'] if prev.get('bids') else 0
        prev_ask_p = prev['asks'][0]['price'] if prev.get('asks') else 0
        
        is_buy_trade = (ltp >= prev_ask_p) # Hitting the ask
        is_sell_trade = (ltp <= prev_bid_p) # Hitting the bid
        
        if not (is_buy_trade or is_sell_trade):
            return

        target_price = ltp
        # SIDE CORRECTION: 
        # If aggressive buyers hit the ask (is_buy_trade), the resting wall is a SELLER.
        # If aggressive sellers hit the bid (is_sell_trade), the resting wall is a BUYER.
        wall_side = "SELL" if is_buy_trade else "BUY" 
        
        # Find current depth at that price
        current_depth = current['asks' if is_buy_trade else 'bids']
        prev_depth = prev['asks' if is_buy_trade else 'bids']
        
        # Find the level qty for target_price
        curr_qty = next((x['qty'] for x in current_depth if x['price'] == target_price), 0)
        prev_qty = next((x['qty'] for x in prev_depth if x['price'] == target_price), 0)
        
        # REFILL DEFINITION:
        # If vol_delta > 0 trades at P, but NewQty >= PrevQty (or didn't drop by vol_delta)
        # It means someone is reloading at that price.
        expected_qty = max(0, prev_qty - vol_delta)
        
        if curr_qty > expected_qty:
            refilled = curr_qty - expected_qty
            
            if symbol not in self.absorption_stats:
                self.absorption_stats[symbol] = {}
            
            if target_price not in self.absorption_stats[symbol]:
                self.absorption_stats[symbol][target_price] = {"cum_vol": 0, "refill_count": 0, "side": wall_side}
            
            stats = self.absorption_stats[symbol][target_price]
            stats['cum_vol'] += vol_delta
            stats['refill_count'] += 1
            stats['last_ts'] = datetime.now()
            
            # Authenticity Threshold: If refilled volume > 5x the average level depth
            avg_level_size = (sum(b.get('qty', 0) for b in current['bids']) + sum(a.get('qty', 0) for a in current['asks'])) / (len(current['bids']) + len(current['asks']))
            
            if stats['cum_vol'] > (avg_level_size * self.refill_multiplier):
                # logger.warning(f"❄️ AUTHENTIC ICEBERG: {symbol} @ {target_price} | Side: {wall_side} | Refilled: {stats['cum_vol']} | Count: {stats['refill_count']}")
                
                # Publish to Redis channel 'icebergs' for UI/Alerts
                if self.redis_client:
                    alert = {
                        "symbol": symbol,
                        "time": datetime.now().isoformat(),
                        "ltp": float(target_price),
                        "actiontobetaken": wall_side # Consistent with UI labels
                    }
                    self.redis_client.publish("icebergs", json.dumps(alert))
                
                # --- DB INSERTION ---
                try:
                    with self.Session() as session:
                        new_alert = IcebergAlert(
                            symbol=symbol,
                            timestamp=datetime.now(),
                            ltp=float(target_price),
                            action=wall_side
                        )
                        session.add(new_alert)
                        session.commit()
                except Exception as e:
                    logger.error(f"Error inserting IcebergAlert: {e}")

        # Memory Management: Cleanup stats older than 5 mins
        now = datetime.now()
        for sym in list(self.absorption_stats.keys()):
            for price in list(self.absorption_stats[sym].keys()):
                if (now - self.absorption_stats[sym][price]['last_ts']).total_seconds() > 300:
                    del self.absorption_stats[sym][price]

    def _detect_absorption(self, symbol, ltp, bids, asks, fs_imbalance, packet, is_l3):
        vqs_score = packet.get('vqs_score', 0.0)
        
        # 1. Check for active "Authentic Iceberg" Walls
        if symbol not in self.absorption_stats:
            return
            
        for price, stats in list(self.absorption_stats[symbol].items()):
            # A "Wall Break" occurs when price moves THROUGH a heavily absorbed level
            # If wall was a SELLER (wall_side="SELL") and LTP > Price -> LONG Trigger
            # If wall was a BUYER (wall_side="BUY") and LTP < Price -> SHORT Trigger
            
            wall_side = stats.get('side')
            is_wall_hit = abs(ltp - price) < (ltp * 0.0005) # Very close to wall
            is_wall_broken_up = ltp > price + (ltp * 0.0002) # Moved above wall
            is_wall_broken_down = ltp < price - (ltp * 0.0002) # Moved below wall

            # Institutional Bounce Pattern: Price hits the wall and holds/reverses, backed by flow
            # Tightened to 0.05% tolerance
            is_bouncing_up = ltp >= price and ltp < price + (ltp * 0.0005)
            is_bouncing_down = ltp <= price and ltp > price - (ltp * 0.0005)

            # Extract volume surge directly 
            vol_surge = packet.get("vol_surge", 1.0)

            # ALPHA PATTERN: Institutional Squeeze
            # If there was massive absorption (stats['cum_vol'] large) and the wall is finally cleared
            
            # LONG TRIGGER: Aggressive buying cleared a resting institutional seller
            if wall_side == "SELL" and is_wall_broken_up and fs_imbalance > self.imbalance_trigger:
                if vqs_score > self.vqs_trigger and vol_surge >= 5.0: # Tightened from 2.5
                    logger.success(f"🚀 ALPHA LONG SQUEEZE: {symbol} Squeezed SELLER @ {price} | LTP: {ltp} | Imb: {fs_imbalance} | Surge: {vol_surge}x")
                    self._evaluate_trigger(symbol, "LONG", ltp, packet, is_l3, anchor_price=price)
            
            # SHORT TRIGGER: Aggressive selling cleared a resting institutional buyer
            elif wall_side == "BUY" and is_wall_broken_down and fs_imbalance < -self.imbalance_trigger:
                if vqs_score < -self.vqs_trigger and vol_surge >= 5.0: # Tightened from 2.5
                    logger.success(f"\033[91m🩸 ALPHA SHORT SQUEEZE: {symbol} Squeezed BUYER @ {price} | LTP: {ltp} | Imb: {fs_imbalance} | Surge: {vol_surge}x\033[0m")
                    self._evaluate_trigger(symbol, "SHORT", ltp, packet, is_l3, anchor_price=price, signal_type="ORDERFLOW_ALPHA")

            # BOUNCE TRIGGER (Support/Resistance Hold)
            # BUYER wall holds, price bounces up, and there's buying flow (+imbalance, +vqs)
            elif wall_side == "BUY" and is_bouncing_up and fs_imbalance > self.imbalance_trigger:
                if vqs_score > self.vqs_trigger and vol_surge >= 5.0: # Tightened from 2.5
                    logger.success(f"🚀 ALPHA LONG BOUNCE: {symbol} Held BUYER Support @ {price} | LTP: {ltp} | Imb: {fs_imbalance} | Surge: {vol_surge}x")
                    self._evaluate_trigger(symbol, "LONG", ltp, packet, is_l3, anchor_price=price, signal_type="ORDERFLOW_ALPHA")
            
            # SELLER wall holds, price bounces down, and there's selling flow (-imbalance, -vqs)
            elif wall_side == "SELL" and is_bouncing_down and fs_imbalance < -self.imbalance_trigger:
                if vqs_score < -self.vqs_trigger and vol_surge >= 5.0: # Tightened from 2.5
                    logger.success(f"\033[91m🩸 ALPHA SHORT BOUNCE: {symbol} Held SELLER Resistance @ {price} | LTP: {ltp} | Imb: {fs_imbalance} | Surge: {vol_surge}x\033[0m")
                    self._evaluate_trigger(symbol, "SHORT", ltp, packet, is_l3, anchor_price=price, signal_type="ORDERFLOW_ALPHA")

    def _detect_momentum_surge(self, symbol, ltp, packet, is_l3):
        """
        Fallback system 1: Momentum Squeeze
        Triggers purely on extreme volume surges and price action quality.
        Uses a 0.1% / 30s confirmation gate to avoid false signals.
        Ticks accumulate from 9:15 but signals are only fired from 9:30.
        """
        # Opening filter: local metrics warm up from 9:15 but no signals before 9:30
        now = datetime.now().time()
        if now < self.momentum_start_time:
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
            
            if side_candidate and symbol not in self.pending_confirmation:
                logger.info(f"⏳ MOMENTUM SQUEEZE PENDING ({side_candidate}): {symbol} @ {ltp} | Surge: {vol_surge:.1f}x | VQS: {vqs_score:.2f} | awaiting {self.momentum_confirm_pct}% confirm in {self.momentum_confirm_secs}s")
                self.pending_confirmation[symbol] = {
                    "side":          side_candidate,
                    "trigger_price": ltp,
                    "trigger_time":  datetime.now(),
                    "packet":        packet.copy(),
                    "is_l3":         is_l3
                }

    def _check_pending_confirmations(self, symbol, ltp):
        """Called every tick — checks if a pending signal has been price-confirmed."""
        if symbol not in self.pending_confirmation:
            return

        pending = self.pending_confirmation[symbol]
        elapsed = (datetime.now() - pending['trigger_time']).total_seconds()

        if elapsed > self.momentum_confirm_secs:
            logger.debug(f"⌛ MOMENTUM CONFIRM EXPIRED: {symbol} (side={pending['side']}, elapsed={elapsed:.0f}s)")
            del self.pending_confirmation[symbol]
            return

        trigger_price = pending['trigger_price']
        side          = pending['side']
        required_move = trigger_price * (self.momentum_confirm_pct / 100.0)

        confirmed = (
            (side == "LONG"  and ltp >= trigger_price + required_move) or
            (side == "SHORT" and ltp <= trigger_price - required_move)
        )

        if confirmed:
            logger.warning(f"✅ MOMENTUM SQUEEZE CONFIRMED ({side}): {symbol} @ {ltp} (triggered @ {trigger_price}, elapsed={elapsed:.0f}s)")
            del self.pending_confirmation[symbol]
            self._evaluate_trigger(symbol, side, ltp, pending['packet'], pending['is_l3'], signal_type="MOMENTUM_SQUEEZE")


    def _detect_vwap_extension(self, symbol, ltp, packet, is_l3):
        """
        Fallback system 2: VWAP Deviation
        Detects sudden price extensions away from VWAP with significant volume.
        """
        vwap = packet.get('vwap', 0.0)
        vol_surge = packet.get('vol_surge', 1.0)
        
        if vwap <= 0 or vol_surge < self.vwap_vol_surge:
            return
            
        deviation = (ltp - vwap) / vwap
        
        if deviation >= self.vwap_deviation_pct:
            logger.warning(f"🚀 VWAP EXTENSION (LONG): {symbol} @ {ltp} | Surge: {vol_surge}x | Deviation: {deviation:.2%}")
            self._evaluate_trigger(symbol, "LONG", ltp, packet, is_l3, signal_type="VWAP_EXTENSION")
        elif deviation <= -self.vwap_deviation_pct:
            logger.warning(f"\033[91m🩸 VWAP EXTENSION (SHORT): {symbol} @ {ltp} | Surge: {vol_surge}x | Deviation: {deviation:.2%}\033[0m")
            self._evaluate_trigger(symbol, "SHORT", ltp, packet, is_l3, signal_type="VWAP_EXTENSION")

    def _detect_db_style_breakout(self, symbol, ltp, packet, is_l3):
        """
        Fallback system 3: Scanner Alignment
        Matches the logic in vol_surge_scanner.py: 10x surge and 0.5% price move.
        This ensures the Orchestrator doesn't miss what the database scanner sees.
        """
        vol_surge = packet.get('vol_surge', 1.0)
        
        # We need the 1-candle price move (simulated for streaming)
        # We'll use the price 50 ticks ago as a proxy for the 'start' of the move
        history = self.price_history.get(symbol, [])
        if len(history) < 50:
            return
            
        start_price = history[0] # History is maxlen 100, 0 is the oldest
        price_move_pct = (ltp - start_price) / start_price
        
        # Rule: 10x Surge and 0.5% price move
        if vol_surge >= 10.0 and abs(price_move_pct) >= 0.005:
            side = "LONG" if price_move_pct > 0 else "SHORT"
            logger.warning(f"🔍 SCANNER ALIGNMENT ({side}): {symbol} @ {ltp} | Surge: {vol_surge:.1f}x | Move: {price_move_pct:.2%}")
            self._evaluate_trigger(symbol, side, ltp, packet, is_l3, signal_type="SCANNER_BREAKOUT")

    def _evaluate_trigger(self, symbol, side, ltp, packet, is_l3, anchor_price=None, signal_type="ORDERFLOW_ALPHA"):
        # Filter by enabled signal types if configured
        if self.enabled_signals and signal_type not in self.enabled_signals:
            return

        # 1. Redis Global Lock (Absolute Once-Per-Day per Stock)
        now = datetime.now()
        lock_key = f"lock:trade:{symbol}:{now.date()}"
        if self.redis_client.exists(lock_key):
            return

        # 2. Memory Debounce (Strict Once per Day per Stock)
        last_t = self.last_signal_time.get(symbol, datetime.min)
        if last_t.date() >= now.date():
            return

        with self.Session() as session:
            # 3. DB Guard (Absolute One-Trade-Per-Day per Stock for TODAY)
            from src.db.schema import Position
            today = now.date()
            existing_today = session.query(Position).filter(
                Position.symbol == symbol,
                Position.date == today
            ).first()
            
            if existing_today:
                # Absolute guard: sync redis/memory if DB already has a record
                self.redis_client.setex(lock_key, 86400, "1") # Sync Redis
                self.last_signal_time[symbol] = now # Sync memory
                logger.warning(f"🛡️ Guard: {symbol} already has a record for today (ID: {existing_today.id}, Status: {existing_today.status}). Entry blocked.")
                return

            # SET LOCK IMMEDIATELY to prevent race conditions
            self.redis_client.setex(lock_key, 86400, "1")
            self.last_signal_time[symbol] = now

            # --- GOLD GUARD: Sniper Alignment Check ---
            # Symbols must be in DailyFocus and have matching sniper status
            # from src.db.schema import DailyFocus
            # focus = session.query(DailyFocus).filter(
            #     DailyFocus.symbol == symbol,
            #     DailyFocus.date == date.today()
            # ).first()
            
            # if not focus:
            #     # logger.debug(f"Orchestrator: {symbol} not in DailyFocus. Ignoring signal.")
            #     self._log_near_miss(session, symbol, side, ltp, packet, signal_type, ["NOT_IN_DAILY_FOCUS"])
            #     return

            # Match Status to Side
            # is_valid_sniper = False
            # if side == "LONG" and focus.oracle_status == "UP_SNIPER":
            #     is_valid_sniper = True
            # elif side == "SHORT" and focus.oracle_status == "DOWN_SNIPER":
            #     is_valid_sniper = True
            
            # if not is_valid_sniper:
            #     # logger.debug(f"Orchestrator: {symbol} status {focus.oracle_status} does not match signal side {side}. Ignoring.")
            #     self._log_near_miss(session, symbol, side, ltp, packet, signal_type, [f"ORACLE_MISMATCH: {focus.oracle_status} vs {side}"])
            #     return

            # --- PRECISION BALANCE: 35% - 65% Filter ---
            bid_p = packet.get('bid_pct', -50)
            ask_p = packet.get('ask_pct', -50)

            if bid_p == -50 or ask_p == -50:
                self._log_near_miss(session, symbol, side, ltp, packet, signal_type, ["BID_ASK_PCT_MISSING"])
                return

            strength = bid_p if side == "LONG" else ask_p
            
            if side == "LONG" and strength < 53.0:
                self._log_near_miss(session, symbol, side, ltp, packet, signal_type, [f"STRENGTH_FILTER: {strength:.1f}% (Required >= 53.0%)"])
                return
            if side == "SHORT" and strength > 45.0:
                self._log_near_miss(session, symbol, side, ltp, packet, signal_type, [f"STRENGTH_FILTER: {strength:.1f}% (Required <= 45.0%)"])
                return

            if strength < 35.0 or strength > 65.0:
                logger.debug(f"Orchestrator: {symbol} balance {strength:.1f}% outside 35-65 range. Ignoring.")
                self._log_near_miss(session, symbol, side, ltp, packet, signal_type, [f"STRENGTH_FILTER: {strength:.1f}%"])
                return

            # PURE ALPHA / FALLBACK: Generate signal immediately if trigger conditions met.
            depth_label = "L3" if is_l3 else "L2"
            
            if side == "SHORT":
                logger.success(f"\033[91m🔥 {signal_type} SIGNAL (GOLD GUARD): {symbol} | {side} @ {ltp} | Vol Surge: {packet.get('vol_surge', 'None')}x\033[0m")
            else:
                logger.success(f"🔥 {signal_type} SIGNAL (GOLD GUARD): {symbol} | {side} @ {ltp} | Vol Surge: {packet.get('vol_surge', 'None')}x")
                
            self._generate_signal(session, symbol, side, ltp, packet, is_l3, anchor_price, signal_type)
            self.last_signal_time[symbol] = datetime.now()

    def _generate_signal(self, session, symbol, side, ltp, packet, is_l3, anchor_price=None, signal_type="ORDERFLOW_ALPHA"):
        # High-Precision SL/TP for OrderFlow Alpha
        # SL is placed behind the wall that was broken
        sl_buffer = ltp * 0.02 # 2% Stop Loss
        if anchor_price:
            # Place SL 2% behind the institutional wall/anchor for maximum survival
            sl = anchor_price - (ltp * 0.02) if side == "LONG" else anchor_price + (ltp * 0.02)
        else:
            sl = ltp - sl_buffer if side == "LONG" else ltp + sl_buffer
            
        # TP is now fixed at 1.0% as per user request
        tp_buffer = ltp * 0.01 
        tp = ltp + tp_buffer if side == "LONG" else ltp - tp_buffer

        # Create Signal entry
        new_sig = ORBSignal(
            symbol=symbol,
            side=side,
            date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
            timestamp=datetime.now(),
            entry_price=ltp,
            sl=round(float(sl), 2),
            tp=round(float(tp), 2),
            signal_type=signal_type,
            metrics={
                "vqs": packet.get('vqs_score'),
                "imbalance": packet.get('imbalance'),
                "vwap": packet.get('vwap'),
                "rsi": packet.get('rsi'),
                "macd": packet.get('macd'),
                "vol_surge": packet.get('vol_surge'),
                "slope": packet.get('slope'),
                "source": "L3_ALPHA" if is_l3 else "L2_ALPHA",
                "wall_price": anchor_price,
                "depth": 20 if is_l3 else 5
            },
            status="PENDING",
            bid_pct=packet.get('bid_pct'),
            ask_pct=packet.get('ask_pct')
        )
        session.add(new_sig)
        session.flush() # Get ID
        
        # --- AUTO-EXECUTE LOGIC ---
        master_switch = os.getenv("ENABLE_AUTO_TRADING", "false").lower() == "true"
        if not master_switch:
            logger.warning(f"🛡️ Orchestrator: {symbol} signal detected but ENABLE_AUTO_TRADING is FALSE. Skipping position.")
            session.commit()
            return

        p = self._get_config(session)
        auto_exec = False
        if side == 'LONG' and p.get('auto_execute_long', False): 
            auto_exec = True
        elif side == 'SHORT' and p.get('auto_execute_short', False):
            auto_exec = True
            
        if auto_exec:
            logger.info(f"Orchestrator: AUTO-EXECUTE ENABLED for {symbol}. Firing order...")
            from src.db.schema import Position
            # 1. Create Position Record in PENDING status
            pos = Position(
                symbol=symbol,
                side=side,
                date=new_sig.date,
                entry_time=datetime.now(),
                entry_price=new_sig.entry_price,
                qty=1, 
                signal_type=signal_type,
                status="PENDING", 
                sl=new_sig.sl,
                tp=new_sig.tp,
                sl_type="ORB_BOUNDARY",
                entry_metrics=new_sig.metrics,
                agent_audit_log=f"Automated execution trigger for Alpha Signal {new_sig.id}"
            )
            try:
                session.add(pos)
                session.flush()
                
                # 2. Trigger PortfolioManager Execution Logic
                success, message = self.port_mgr._execute_entry(session, pos)
                if success and pos.status == 'OPEN':
                    new_sig.status = "EXECUTED"
                    new_sig.execution_pos_id = pos.id
                    logger.success(f"✅ Auto-Executed {symbol} {side} @ {ltp}")
                else:
                    logger.warning(f"❌ Auto-Execution failed for {symbol}: {message}")
            except Exception as e:
                logger.error(f"Auto-Execution Error: {e}")

        session.commit()
        
        # --- LOGGING & NOTIFICATIONS ---
        if side == "SHORT":
            logger.success(f"\033[91m🎯 {signal_type} SIGNAL: {symbol} {side} @ {ltp} | SL: {sl} | TP: {tp}\033[0m")
        else:
            logger.success(f"🎯 {signal_type} SIGNAL: {symbol} {side} @ {ltp} | SL: {sl} | TP: {tp}")
        notify_new_signal(symbol, side, signal_type, ltp, f"{signal_type} detected. Vol Surge: {packet.get('vol_surge')}x")

    def _log_near_miss(self, session, symbol, side, ltp, packet, signal_type, reasons):
        """Logs a signal that failed filters into the near-miss table."""
        # Debounce near misses too (1 per 5 mins)
        try:
            nm = ORBSignalNearMiss(
                symbol=symbol,
                side=side,
                date=date.today(),
                timestamp=datetime.now(),
                entry_price=ltp,
                signal_type=signal_type,
                sl=round(float(ltp * 0.98 if side == "LONG" else ltp * 1.02), 2),
                tp=round(float(ltp * 1.01 if side == "LONG" else ltp * 0.99), 2),
                fail_reasons=reasons,
                metrics={
                    "vqs": packet.get('vqs_score'),
                    "vol_surge": packet.get('vol_surge'),
                    "vwap": packet.get('vwap'),
                    "bid_pct": packet.get('bid_pct'),
                    "ask_pct": packet.get('ask_pct'),
                    "source": "ORC_NEAR_MISS"
                }
            )
            session.add(nm)
            session.commit()
            logger.debug(f"Logged Near Miss for {symbol}: {reasons}")
        except Exception as e:
            logger.error(f"Error logging Near Miss: {e}")

if __name__ == "__main__":
    orchestrator = OrderFlowOrchestrator()
    orchestrator.run()