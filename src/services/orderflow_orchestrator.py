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
from src.db.schema import ORBSignal, DailyFocus
from src.services.portfolio_manager import PortfolioManager
from src.utils.notifications import notify_new_signal

load_dotenv()
os.makedirs("logs", exist_ok=True)
logger.add("logs/orderflow_orchestrator.log", rotation="500 MB", level="DEBUG", retention="10 days")
logger.info("OrderFlowOrchestrator logging to logs/orderflow_orchestrator.log")

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
        self.price_history = defaultdict(lambda: deque(maxlen=100))
        self.vol_history = defaultdict(lambda: deque(maxlen=100))
        self.vwap_num = defaultdict(float)
        self.vwap_den = defaultdict(float)
        self.last_reset_date = date.today()
        self.prev_volume = defaultdict(int) # Track session volume for delta logic
        self.open_prices = {} # Track morning open (09:15) for extension guard
        self.nifty_vqs = 0.0 # Global Market Sentiment Cache
        
        # Config
        self.min_iceberg_score = 0.85 # Tightened from 0.8
        self.imbalance_trigger = 0.6  # Tightened from 0.4
        self.vqs_trigger = 0.7        # Tightened from 0.5
        self.refill_multiplier = 20.0 # Tightened from 15.0
        
        # Fallback Config (For Gold Guard precision)
        self.momentum_vol_surge = 15.0 # Tightened from 12.0
        self.momentum_vqs = 0.85       # Tightened from 0.7
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
            self.open_prices.clear()
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
        
        if symbol == "NIFTY":
            # 1. Expand Window for Smoother Sentiment
            if self.price_history[symbol].maxlen < 300:
                current_hist = list(self.price_history[symbol])
                self.price_history[symbol] = deque(current_hist, maxlen=300)
            
            # 2. Ghost Tick Filter: Ignore impossible jumps back to the open price
            open_p = self.open_prices.get("NIFTY")
            if open_p and len(self.price_history[symbol]) > 0:
                prev_ltp = self.price_history[symbol][-1]
                if abs(ltp - prev_ltp) > 50 and abs(ltp - open_p) < 0.5:
                    logger.warning(f"🛡️ NIFTY GHOST TICK DETECTED: Jumped from {prev_ltp} to {ltp} (Open: {open_p}). Filtering.")
                    return packet

        # 2a. Track morning baseline for Extension Guard (Sync with DB if missing)
        if symbol not in self.open_prices:
            now_time = datetime.now().time()
            if now_time >= dt_time(9, 15):
                try:
                    with self.Session() as session:
                        from sqlalchemy import text
                        res = session.execute(text(
                            "SELECT open FROM intraday_ticks WHERE symbol = :s AND date(timestamp) = :d ORDER BY timestamp ASC LIMIT 1"
                        ), {"s": symbol, "d": date.today()}).first()
                        if res:
                            self.open_prices[symbol] = float(res[0])
                            logger.info(f"Morning open for {symbol} synced from DB: {self.open_prices[symbol]}")
                        else:
                            # If DB doesn't have it yet, this is the first tick at/after 9:15
                            self.open_prices[symbol] = ltp
                            logger.info(f"Morning open for {symbol} set from first 9:15 tick: {ltp}")
                except Exception as e:
                    logger.error(f"Error fetching open price for {symbol}: {e}")
                    self.open_prices[symbol] = ltp
        
        # 3. Local VWAP Value
        den = self.vwap_den[symbol]
        local_vwap = self.vwap_num[symbol] / den if den > 0 else ltp
        
        # 4. Local VQS (Momentum) - Using full 100-tick window for smooth Velocity
        self.price_history[symbol].append(ltp)
        history = list(self.price_history[symbol])
        if len(history) > 1:
            ticks = []
            for i in range(1, len(history)):
                if history[i] > history[i-1]: ticks.append(1)
                elif history[i] < history[i-1]: ticks.append(-1)
            local_vqs = sum(ticks) / len(ticks) if ticks else 0.0
        else:
            local_vqs = 0.0

        # 5. Local Vol Surge
        # 5. Local Vol Surge - Improved Accuracy (Excluding current spike from average)
        v_hist = list(self.vol_history[symbol])
        avg_v = sum(v_hist) / len(v_hist) if v_hist else 0.0
        last_v = v_hist[-1] if v_hist else 0.0
        local_surge = last_v / avg_v if avg_v > 0 else 1.0

        # Inject into packet to override upstream fields
        packet['vwap'] = local_vwap
        packet['vqs_score'] = local_vqs
        packet['vol_surge'] = local_surge
        packet['vol_delta'] = diff # Track if volume actually moved (Real Tick vs Depth Update)
        
        # 6. Global Sentiment Cache (NIFTY)
        # Optimized: We track NIFTY just like any other symbol, but cache its VQS globally
        # so other signals can reference it without extra calculations.
        if symbol == "NIFTY":
            self.nifty_vqs = local_vqs
            # Trend Bias: If market is up > 0.4% on the day, keep sentiment bullish even during small flat spots
            open_p = self.open_prices.get("NIFTY")
            raw_vqs = local_vqs
            day_change = 0.0
            if open_p:
                day_change = (ltp - open_p) / open_p
                # Dynamic Threshold: 0.15 base + proportional boost for stronger trends
                # e.g. 0.8% move results in ~0.31 floor
                dynamic_floor = min(0.5, 0.15 + max(0, (abs(day_change) - 0.004) * 40))
                
                if day_change >= 0.004 and self.nifty_vqs < dynamic_floor:
                    self.nifty_vqs = dynamic_floor # Bullish Override
                elif day_change <= -0.004 and self.nifty_vqs > -dynamic_floor:
                    self.nifty_vqs = -dynamic_floor # Bearish Override
                    
            logger.debug(f"Market Sentiment Updated (NIFTY): LTP={ltp:.1f} | Change={day_change*100:.2f}% | RawVQS={raw_vqs:.2f} | FinalVQS={self.nifty_vqs:.4f}")

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
        
        # 3. Absorption Detection (DISABLED - Momentum Squeeze Only)
        # self._detect_absorption(symbol, ltp, bids, asks, fs_imbalance, packet, is_l3)
        
        # 4. Fallback Detection (Momentum Surge ONLY)
        self._detect_momentum_surge(symbol, ltp, packet, is_l3)
        # self._detect_vwap_extension(symbol, ltp, packet, is_l3)
        # self._detect_db_style_breakout(symbol, ltp, packet, is_l3)
        
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
                logger.warning(f"❄️ AUTHENTIC ICEBERG: {symbol} @ {target_price} | Side: {wall_side} | Refilled: {stats['cum_vol']} | Count: {stats['refill_count']}")
                
                # Publish to Redis channel 'icebergs' for UI/Alerts
                if self.redis_client:
                    alert = {
                        "symbol": symbol,
                        "time": datetime.now().isoformat(),
                        "ltp": float(target_price),
                        "actiontobetaken": wall_side # Consistent with UI labels
                    }
                    self.redis_client.publish("icebergs", json.dumps(alert))

        # Memory Management: Cleanup stats older than 5 mins
        now = datetime.now()
        for sym in list(self.absorption_stats.keys()):
            for price in list(self.absorption_stats[sym].keys()):
                if (now - self.absorption_stats[sym][price]['last_ts']).total_seconds() > 300:
                    del self.absorption_stats[sym][price]

    def _detect_absorption(self, symbol, ltp, bids, asks, fs_imbalance, packet, is_l3):
        # --- FIX: Only evaluate absorption triggers on Trade ticks ---
        if packet.get('vol_delta', 0) <= 0:
            return

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
        """
        # --- FIX: Avoid double-logging on non-trade packets (e.g. depth updates) ---
        if packet.get('vol_delta', 0) <= 0:
            return

        vol_surge = packet.get('vol_surge', 1.0)
        vqs_score = packet.get('vqs_score', 0.0)
        vwap = packet.get('vwap', ltp)
        imbalance = packet.get('imbalance', 0.0)
        
        # Guard: Avoid chasing spikes > 1.0% from VWAP
        vwap_dist_pct = abs(ltp - vwap) / vwap * 100 if vwap > 0 else 0
        if vwap_dist_pct > 1.0:
            return

        if vol_surge >= self.momentum_vol_surge:
            # LONG: VQS positive, price > VWAP, and actual Buy Imbalance in order book
            if vqs_score >= self.momentum_vqs and ltp > vwap and imbalance >= 0.25:
                # Tightened extension for fallback: 2.0%
                self._evaluate_trigger(symbol, "LONG", ltp, packet, is_l3, signal_type="MOMENTUM_SQUEEZE", max_extension=2.0)
            
            # SHORT: VQS negative, price < VWAP, and actual Sell Imbalance
            elif vqs_score <= -self.momentum_vqs and ltp < vwap and imbalance <= -0.25:
                self._evaluate_trigger(symbol, "SHORT", ltp, packet, is_l3, signal_type="MOMENTUM_SQUEEZE", max_extension=2.0)

    def _detect_vwap_extension(self, symbol, ltp, packet, is_l3):
        """
        Fallback system 2: VWAP Deviation
        Detects sudden price extensions away from VWAP with significant volume.
        """
        vwap = packet.get('vwap', 0.0)
        vol_surge = packet.get('vol_surge', 1.0)
        
        if vwap <= 0 or vol_surge < self.vwap_vol_surge or packet.get('vol_delta', 0) <= 0:
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
        if packet.get('vol_delta', 0) <= 0:
            return

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

    def _evaluate_trigger(self, symbol, side, ltp, packet, is_l3, anchor_price=None, signal_type="ORDERFLOW_ALPHA", max_extension=3.5):
        # --- TIME WINDOW GUARD: 09:15 to 14:30 ---
        now_time = datetime.now().time()
        start_time = dt_time(9, 1)
        end_time = dt_time(14, 30)
        
        if now_time < start_time or now_time > end_time:
            # logger.debug(f"Orchestrator: Signal outside trading window ({now_time}). Ignoring.")
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
                self.redis_client.setex(lock_key, 86400, "1") # Sync Redis
                self.last_signal_time[symbol] = now # Sync memory
                logger.warning(f"🛡️ Orchestrator Guard: {symbol} already has a record for today (ID: {existing_today.id}, Status: {existing_today.status}). Entry blocked.")
                return

            # SET LOCK IMMEDIATELY to prevent race conditions
            self.redis_client.setex(lock_key, 86400, "1")
            self.last_signal_time[symbol] = now

            # --- EXTENSION GUARD: 2.5% Boundary ---
            # Rule: Don't chase a move that has already fallen/risen too much.
            open_p = self.open_prices.get(symbol)
            if open_p:
                move_pct = ((ltp - open_p) / open_p) * 100
                if side == "LONG" and move_pct > max_extension:
                    logger.debug(f"Orchestrator: {symbol} LONG over-extended ({move_pct:.2f}% > {max_extension}%). Ignoring.")
                    return
                if side == "SHORT" and move_pct < -max_extension:
                    logger.debug(f"Orchestrator: {symbol} SHORT over-extended ({move_pct:.2f}% < -{max_extension}%). Ignoring.")
                    return

            # --- DYNAMIC WATCHLIST CHECK: Must be in DailyFocus ---
            # Even if we ignore Oracle Status, we only trade symbols in our universe for the day.
            focus = session.query(DailyFocus).filter(
                DailyFocus.symbol == symbol,
                DailyFocus.date == date.today()
            ).first()
            
            if not focus:
                logger.debug(f"Orchestrator: {symbol} ignored. Not in Today's DailyFocus watchlist.")
                return

            # --- PRECISION BALANCE: 35% - 65% Filter ---
            bid_p = packet.get('bid_pct', 50)
            ask_p = packet.get('ask_pct', 50)
            strength = bid_p if side == "LONG" else ask_p
            
            if strength < 35.0 or strength > 65.0:
                logger.debug(f"Orchestrator: {symbol} balance {strength:.1f}% outside 35-65 range. Ignoring.")
                return

            # --- MARKET ALIGNMENT: NIFTY Sentiment Filter ---
            # Rule: Don't fight the market. Signal direction must match NIFTY's momentum.
            # Long only if Nifty VQS > 0.25. Short only if Nifty VQS < -0.25.
            is_aligned = False
            if side == "LONG" and self.nifty_vqs > 0.1:
                is_aligned = True
            elif side == "SHORT" and self.nifty_vqs < -0.1:
                is_aligned = True
            
            if not is_aligned:
                logger.debug(f"Orchestrator: {symbol} {side} avoided. Against Market Sentiment (NIFTY VQS: {self.nifty_vqs:.2f})")
                return

            # PURE ALPHA / FALLBACK: Generate signal immediately if trigger conditions met.
            depth_label = "L3" if is_l3 else "L2"
            
            if side == "SHORT":
                logger.success(f"\033[91m🔥 {signal_type} SIGNAL (DYNAMIC): {symbol} | {side} @ {ltp} | Vol Surge: {packet.get('vol_surge', 'None')}x\033[0m")
            else:
                logger.success(f"🔥 {signal_type} SIGNAL (DYNAMIC): {symbol} | {side} @ {ltp} | Vol Surge: {packet.get('vol_surge', 'None')}x")
                
            self._generate_signal(session, symbol, side, ltp, packet, is_l3, anchor_price, signal_type)
            self.last_signal_time[symbol] = datetime.now()

    def _generate_signal(self, session, symbol, side, ltp, packet, is_l3, anchor_price=None, signal_type="ORDERFLOW_ALPHA"):
        # Load Global Config for SL/TP
        config = self._get_config(session)
        sl_pct = float(config.get('sl_pct', 2.0))
        tp_pct = float(config.get('tp_pct', 1.0))
        
        # High-Precision SL/TP for OrderFlow Alpha
        sl_buffer = ltp * (sl_pct / 100.0)
        if anchor_price:
            # Place SL behind the institutional wall/anchor for maximum survival
            sl = anchor_price - sl_buffer if side == "LONG" else anchor_price + sl_buffer
        else:
            sl = ltp - sl_buffer if side == "LONG" else ltp + sl_buffer
            
        # Target Price
        tp_buffer = ltp * (tp_pct / 100.0) 
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
                # Fetch remote positions for the double-entry guard
                remote_positions = self.port_mgr.dhan_client.get_positions()
                success, message = self.port_mgr._execute_entry(session, pos, remote_positions)
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

if __name__ == "__main__":
    orchestrator = OrderFlowOrchestrator()
    orchestrator.run()
