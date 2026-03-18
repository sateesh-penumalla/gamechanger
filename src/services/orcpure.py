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

# ---------------------------------------------------------------------------
# MOMENTUM SQUEEZE — HARDCODED RISK PARAMETERS
# R:R is intentionally set to 1:2.5 minimum for momentum breakout strategies.
# SL is tight (0.8%) because momentum entries should not need 2% room —
# if a squeeze reverses 0.8% it has already failed. TP is wider (2.0%)
# to let winners run while the vol surge is still active.
# ---------------------------------------------------------------------------
MOMENTUM_SL_PCT = 0.8   # Tight SL — momentum that reverses 0.8% has failed
MOMENTUM_TP_PCT = 2.0   # 1:2.5 R:R minimum — let winners run with the surge

# Minimum average tick volume below which surge ratios are meaningless noise
MIN_AVG_VOL_FLOOR = 250

# Debounce cooldown (seconds) per signal type — shorter for momentum re-entry
MOMENTUM_DEBOUNCE_SECS = 180   # 3 minutes (was 300 for all signals)
DEFAULT_DEBOUNCE_SECS  = 300   # 5 minutes for other signal types

# Time window: avoid noisy opening range — momentum is cleanest from 10:00
MOMENTUM_START_TIME = dt_time(10, 0)
MOMENTUM_END_TIME   = dt_time(14, 30)

# ---------------------------------------------------------------------------
# IMPROVEMENT 3: TIME-OF-DAY SURGE MULTIPLIERS
# NSE session quality varies significantly across the day:
#   10:00–11:30  Prime window    — directional, high conviction  → base threshold (1.0x)
#   11:30–13:00  Chop zone       — lunch drift, low conviction   → +30% surge required
#   13:00–14:00  Lunch recovery  — thin, mixed signals           → +20% surge required
#   14:00–14:30  Pre-close push  — real directional flow returns → base threshold (1.0x)
# ---------------------------------------------------------------------------
TOD_WINDOWS = [
    (dt_time(10,  0), dt_time(11, 30), 1.0),   # Prime window
    (dt_time(11, 30), dt_time(13,  0), 1.3),   # Chop zone — raise bar
    (dt_time(13,  0), dt_time(14,  0), 1.2),   # Lunch drift
    (dt_time(14,  0), dt_time(14, 30), 1.0),   # Pre-close push
]

# ---------------------------------------------------------------------------
# IMPROVEMENT 2: POST-SURGE PRICE CONFIRMATION
# After a surge trigger fires, we require price to move at least this much
# in the signal direction within CONFIRMATION_WINDOW_SECS before generating
# a signal. Filters surges where volume spikes but price goes nowhere.
# ---------------------------------------------------------------------------
PRICE_CONFIRM_PCT  = 0.20   # Require 0.20% move in signal direction
CONFIRMATION_WINDOW_SECS = 30

# ---------------------------------------------------------------------------
# IMPROVEMENT 5: ICEBERG ALIGNMENT BONUS
# If an active aligned iceberg exists (BUY iceberg + LONG signal, or SELL
# iceberg + SHORT signal), reduce the required surge by this factor.
# E.g. at 0.85: if required surge is 8x, it becomes 6.8x when iceberg aligns.
# ---------------------------------------------------------------------------
ICEBERG_ALIGNMENT_SURGE_FACTOR = 0.85  # 15% lower surge required when iceberg aligns

# ---------------------------------------------------------------------------
# IMPROVEMENT 1: ICEBERG BLOCKING LOOKBACK
# If an opposing iceberg has been active within this many seconds, block signal.
# ---------------------------------------------------------------------------
ICEBERG_BLOCK_WINDOW_SECS = 120   # 2 minute window


class OrderFlowOrchestrator:
    """
    Listens to market_depth:LIVE and identifies institutional absorption/breakout patterns.
    Leverages L3 data (20 levels) for full-stack analysis.

    Improvements over v1:
    1. Iceberg confirmation gate     — blocks signals when opposing large iceberg is active
    2. Post-surge price confirmation — requires actual price move before signal fires
    3. Time-of-day surge weighting   — higher threshold during low-quality chop windows
    4. Dynamic avg_tick_vol floor    — scales floor with symbol's own session activity
    5. Iceberg trajectory bonus      — aligned icebergs reduce required surge threshold
    """

    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True
        )

        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe("market_depth:LIVE")

        # Strategy State
        self.active_blocks    = {}  # symbol -> {level: price, qty: volume, type: BUY/SELL}
        self.absorption_stats = {}  # symbol -> {price: {cum_vol, refill_count, last_ts, side}}
        self.last_signal_time = {}  # key: "symbol_signaltype" -> datetime
        self.prev_state       = {}  # symbol -> last_packet

        # --- Local Metrics State (Independence Layer) ---
        self.price_history   = defaultdict(lambda: deque(maxlen=100))
        self.vol_history     = defaultdict(lambda: deque(maxlen=100))
        self.vwap_num        = defaultdict(float)
        self.vwap_den        = defaultdict(float)
        self.last_reset_date = date.today()
        self.prev_volume     = defaultdict(int)
        self.open_prices     = {}
        self.nifty_vqs       = 0.0  # Global Market Sentiment Cache

        # IMPROVEMENT 2: Pending confirmation queue
        # symbol -> {"side": str, "trigger_price": float, "trigger_time": datetime,
        #            "packet": dict, "is_l3": bool}
        self.pending_confirmation = {}

        # IMPROVEMENT 4: Per-symbol session volume tracker for dynamic floor
        # Tracks session-level avg tick vol (slow EMA) to calibrate floor dynamically
        self.session_avg_tick_vol = defaultdict(float)  # symbol -> slow EMA of avg_tick_vol

        # Config
        self.min_iceberg_score  = 0.85
        self.imbalance_trigger  = 0.6
        self.vqs_trigger        = 0.7
        self.refill_multiplier  = 20.0

        # ---------------------------------------------------------------------------
        # MOMENTUM SQUEEZE THRESHOLDS (FIX: Lower surge entry + tighter qualifiers)
        # ---------------------------------------------------------------------------
        self.momentum_vol_surge = 8.0
        self.momentum_vqs       = 0.75
        self.vwap_deviation_pct = 0.025
        self.vwap_vol_surge     = 12.0

        # Fallback Config
        self.momentum_vol_surge_fallback = 8.0
        self.momentum_vqs_fallback       = 0.75

        # Signal type filtering
        self.enabled_signals = (
            os.getenv("ENABLED_SIGNALS", "").split(",")
            if os.getenv("ENABLED_SIGNALS") else []
        )

        # Portfolio Manager for Auto-Execution
        self.port_mgr      = PortfolioManager()
        self.strategy_config = {}

        logger.info(
            f"OrderFlowOrchestrator Initialized (v2 — Enhanced Precision). "
            f"Momentum SL: {MOMENTUM_SL_PCT}% | Momentum TP: {MOMENTUM_TP_PCT}% | "
            f"Price Confirm: {PRICE_CONFIRM_PCT}% in {CONFIRMATION_WINDOW_SECS}s | "
            f"Iceberg Block Window: {ICEBERG_BLOCK_WINDOW_SECS}s"
        )
        if self.enabled_signals:
            logger.info(f"Signal Filtering Active. ONLY allowing: {self.enabled_signals}")

    # -----------------------------------------------------------------------
    # CONFIG
    # -----------------------------------------------------------------------

    def _get_config(self, session):
        """Loads strategy presets and merges with DB overrides."""
        try:
            preset_file = os.path.join(
                os.path.dirname(__file__), '..', 'config', 'strategy_presets.json'
            )
            with open(preset_file, "r") as f:
                presets = json.load(f)
            config = presets.get("sateesh", {}).copy()

            from src.db.schema import SystemJob
            job = session.query(SystemJob).filter_by(job_id='signal_generator').first()
            if job and job.config:
                config.update(job.config)

            return config
        except Exception as e:
            logger.error(f"Error loading Orchestrator config: {e}")
            return {}

    # -----------------------------------------------------------------------
    # IMPROVEMENT 3: TIME-OF-DAY SURGE MULTIPLIER
    # -----------------------------------------------------------------------

    def _get_tod_surge_multiplier(self) -> float:
        """
        Returns a surge threshold multiplier based on the current time of day.
        Higher values mean a stronger surge is required to fire a signal.
        Prevents chasing low-conviction breakouts during NSE's dead zones.
        """
        now = datetime.now().time()
        for start, end, multiplier in TOD_WINDOWS:
            if start <= now < end:
                return multiplier
        return 1.0  # Fallback (should not reach here during trading hours)

    # -----------------------------------------------------------------------
    # IMPROVEMENT 1: ICEBERG GATE HELPERS
    # -----------------------------------------------------------------------

    def _get_active_icebergs(self, symbol: str) -> list:
        """
        Returns list of active icebergs for a symbol within the block window.
        Each entry: {"price": float, "side": str, "refill_count": int, "cum_vol": float}
        """
        now    = datetime.now()
        result = []
        for price, stats in self.absorption_stats.get(symbol, {}).items():
            age = (now - stats.get('last_ts', now)).total_seconds()
            if age <= ICEBERG_BLOCK_WINDOW_SECS:
                result.append({
                    "price":        price,
                    "side":         stats['side'],
                    "refill_count": stats['refill_count'],
                    "cum_vol":      stats['cum_vol'],
                })
        return result

    def _check_iceberg_gate(self, symbol: str, side: str) -> tuple:
        """
        IMPROVEMENT 1: Iceberg blocking gate.

        Rules:
        - If a large SELL iceberg is active and we want to go LONG → block
        - If a large BUY iceberg is active and we want to go SHORT → block
        - "Large" means refill_count >= 5 (persistent wall, not just a
          single institutional print)

        Also returns an alignment bonus flag:
        - If a BUY iceberg is active and we want to go LONG → aligned (bonus)
        - If a SELL iceberg is active and we want to go SHORT → aligned (bonus)

        Returns: (is_blocked: bool, is_aligned: bool, blocking_reason: str)
        """
        icebergs = self._get_active_icebergs(symbol)
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
                block_reason = (
                    f"active {ice['side']} iceberg @ {ice['price']} "
                    f"(count={ice['refill_count']}, cum_vol={ice['cum_vol']:.0f})"
                )
                break  # One large opposing iceberg is enough to block

            if ice['side'] == aligning_side and ice['refill_count'] >= 3:
                is_aligned = True  # Aligned iceberg — eligible for surge bonus

        return is_blocked, is_aligned, block_reason

    # -----------------------------------------------------------------------
    # IMPROVEMENT 4: DYNAMIC FLOOR
    # -----------------------------------------------------------------------

    def _get_dynamic_floor(self, symbol: str, current_avg_tick_vol: float) -> float:
        """
        IMPROVEMENT 4: Dynamic avg_tick_vol floor.

        The static floor of 500 is too permissive for high-volume names (RELIANCE,
        ICICIBANK) and appropriate for mid-caps. This makes the floor adapt to each
        symbol's own session activity using a slow EMA.

        Floor = max(MIN_AVG_VOL_FLOOR, 0.25 * session_avg)

        The session_avg is updated via a slow EMA (alpha=0.02) so it reflects the
        symbol's baseline without being thrown off by burst periods.
        """
        if current_avg_tick_vol > 0:
            prev = self.session_avg_tick_vol[symbol]
            if prev == 0:
                self.session_avg_tick_vol[symbol] = current_avg_tick_vol
            else:
                # Slow EMA — doesn't overreact to momentary spikes
                alpha = 0.02
                self.session_avg_tick_vol[symbol] = (
                    alpha * current_avg_tick_vol + (1 - alpha) * prev
                )

        session_avg = self.session_avg_tick_vol[symbol]
        dynamic_floor = session_avg * 0.25 if session_avg > 0 else 0.0
        return max(MIN_AVG_VOL_FLOOR, dynamic_floor)

    # -----------------------------------------------------------------------
    # LOCAL METRICS
    # -----------------------------------------------------------------------

    def _update_local_metrics(self, packet):
        """Calculates critical metrics locally to override potentially buggy upstream data."""
        symbol = packet['symbol']
        ltp    = packet['ltp']

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
            self.session_avg_tick_vol.clear()    # IMPROVEMENT 4: reset per-symbol baseline
            self.pending_confirmation.clear()    # IMPROVEMENT 2: clear stale confirmations
            self.last_reset_date = today

        # 2. VWAP & Volume Delta
        current_v = packet.get('volume', 0)
        prev_v    = self.prev_volume.get(symbol, 0)

        if current_v > prev_v:
            diff = current_v - prev_v
        elif current_v > 0 and prev_v == 0:
            diff = current_v
        else:
            diff = 0

        if diff > 0:
            self.vwap_num[symbol] += (ltp * diff)
            self.vwap_den[symbol] += diff
            self.vol_history[symbol].append(diff)

        self.prev_volume[symbol] = current_v

        if symbol == "NIFTY":
            if self.price_history[symbol].maxlen < 300:
                current_hist = list(self.price_history[symbol])
                self.price_history[symbol] = deque(current_hist, maxlen=300)

            # Ghost Tick Filter
            open_p = self.open_prices.get("NIFTY")
            if open_p and len(self.price_history[symbol]) > 0:
                prev_ltp = self.price_history[symbol][-1]
                if abs(ltp - prev_ltp) > 50 and abs(ltp - open_p) < 0.5:
                    logger.warning(
                        f"🛡️ NIFTY GHOST TICK DETECTED: Jumped from {prev_ltp} to {ltp} "
                        f"(Open: {open_p}). Filtering."
                    )
                    return packet

        # 3. Morning Open Sync
        if symbol not in self.open_prices:
            now_time = datetime.now().time()
            if now_time >= dt_time(9, 15):
                try:
                    with self.Session() as session:
                        from sqlalchemy import text
                        res = session.execute(text(
                            "SELECT open FROM intraday_ticks "
                            "WHERE symbol = :s AND date(timestamp) = :d "
                            "ORDER BY timestamp ASC LIMIT 1"
                        ), {"s": symbol, "d": date.today()}).first()
                        if res:
                            self.open_prices[symbol] = float(res[0])
                            logger.info(
                                f"Morning open for {symbol} synced from DB: {self.open_prices[symbol]}"
                            )
                        else:
                            self.open_prices[symbol] = ltp
                            logger.info(
                                f"Morning open for {symbol} set from first 9:15 tick: {ltp}"
                            )
                except Exception as e:
                    logger.error(f"Error fetching open price for {symbol}: {e}")
                    self.open_prices[symbol] = ltp

        # 4. Local VWAP
        den        = self.vwap_den[symbol]
        local_vwap = self.vwap_num[symbol] / den if den > 0 else ltp

        # 5. Local VQS (Momentum) — 100-tick window
        self.price_history[symbol].append(ltp)
        history = list(self.price_history[symbol])
        if len(history) > 1:
            ticks = []
            for i in range(1, len(history)):
                if   history[i] > history[i - 1]: ticks.append(1)
                elif history[i] < history[i - 1]: ticks.append(-1)
            local_vqs = sum(ticks) / len(ticks) if ticks else 0.0
        else:
            local_vqs = 0.0

        # 6. Local Vol Surge
        v_hist      = list(self.vol_history[symbol])
        avg_v       = sum(v_hist) / len(v_hist) if v_hist else 0.0
        # FIX (original): Use vol_history[:-1] as baseline so the current tick
        # doesn't inflate the average it's being compared against.
        baseline_v  = sum(v_hist[:-1]) / len(v_hist[:-1]) if len(v_hist) > 1 else avg_v
        last_v      = v_hist[-1] if v_hist else 0.0
        local_surge = last_v / baseline_v if baseline_v > 0 else 1.0

        # Inject computed values into packet, preferring high-quality upstream metrics
        packet['vwap']         = packet.get('vwap', local_vwap)
        packet['vqs_score']    = packet.get('vqs_score', local_vqs)
        packet['vol_surge']    = packet.get('vol_surge', local_surge)
        packet['vol_delta']    = diff
        packet['avg_tick_vol'] = avg_v

        # 7. Global Sentiment Cache (NIFTY)
        if symbol == "NIFTY":
            self.nifty_vqs = local_vqs
            open_p    = self.open_prices.get("NIFTY")
            raw_vqs   = local_vqs
            day_change = 0.0
            if open_p:
                day_change     = (ltp - open_p) / open_p
                dynamic_floor  = min(0.5, 0.15 + max(0, (abs(day_change) - 0.004) * 40))

                if   day_change >=  0.004 and self.nifty_vqs < dynamic_floor:
                    self.nifty_vqs = dynamic_floor
                elif day_change <= -0.004 and self.nifty_vqs > -dynamic_floor:
                    self.nifty_vqs = -dynamic_floor

            logger.debug(
                f"Market Sentiment Updated (NIFTY): LTP={ltp:.1f} | "
                f"Change={day_change*100:.2f}% | RawVQS={raw_vqs:.2f} | "
                f"FinalVQS={self.nifty_vqs:.4f}"
            )

        return packet

    # -----------------------------------------------------------------------
    # MAIN LOOP
    # -----------------------------------------------------------------------

    def run(self):
        """Main loop for processing real-time depth packets."""
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    packet = json.loads(message['data'])
                    packet = self._update_local_metrics(packet)
                    self._process_depth_update(packet)
                    self.prev_state[packet['symbol']] = packet
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

    def _process_depth_update(self, packet):
        symbol      = packet['symbol']
        ltp         = packet['ltp']
        current_vol = packet.get('current_bar_volume', 0)

        bids_20 = packet.get('bids_20')
        asks_20 = packet.get('asks_20')
        is_l3   = bids_20 is not None

        bids = bids_20 if is_l3 else packet.get('bids', [])
        asks = asks_20 if is_l3 else packet.get('asks', [])

        if not bids or not asks:
            return

        # 1. Iceberg Detection
        prev = self.prev_state.get(symbol)
        if prev and current_vol > prev.get('current_bar_volume', 0):
            self._detect_refills(symbol, packet, prev, is_l3)

        # 2. Full-Stack Imbalance (already computed accurately by upstream Feed)
        # We map it to 'fs_imbalance' as that's what downstream code looks for.
        packet['fs_imbalance'] = packet.get('imbalance', 0.0)

        # 3. Momentum Surge Detection (primary active strategy)
        self._detect_momentum_surge(symbol, ltp, packet, is_l3)

        # IMPROVEMENT 2: Check pending confirmation queue on every tick
        self._check_pending_confirmations(symbol, ltp, packet, is_l3)

        # 4. (Disabled — kept for future re-enablement)
        # self._detect_absorption(symbol, ltp, bids, asks, fs_imbalance, packet, is_l3)
        # self._detect_vwap_extension(symbol, ltp, packet, is_l3)
        # self._detect_db_style_breakout(symbol, ltp, packet, is_l3)

        self.prev_state[symbol] = packet

    # -----------------------------------------------------------------------
    # ICEBERG / REFILL DETECTION
    # -----------------------------------------------------------------------

    def _detect_refills(self, symbol, current, prev, is_l3):
        """Identifies active institutional reloading (Icebergs) at a specific price."""
        ltp       = current['ltp']
        vol_delta = current['current_bar_volume'] - prev['current_bar_volume']

        prev_bid_p = prev['bids'][0]['price'] if prev.get('bids') else 0
        prev_ask_p = prev['asks'][0]['price'] if prev.get('asks') else 0

        is_buy_trade  = (ltp >= prev_ask_p)
        is_sell_trade = (ltp <= prev_bid_p)

        if not (is_buy_trade or is_sell_trade):
            return

        target_price = ltp
        wall_side    = "SELL" if is_buy_trade else "BUY"

        current_depth = current['asks' if is_buy_trade else 'bids']
        prev_depth    = prev['asks'    if is_buy_trade else 'bids']

        curr_qty = next((x['qty'] for x in current_depth if x['price'] == target_price), 0)
        prev_qty = next((x['qty'] for x in prev_depth    if x['price'] == target_price), 0)

        expected_qty = max(0, prev_qty - vol_delta)

        if curr_qty > expected_qty:
            refilled = curr_qty - expected_qty

            if symbol not in self.absorption_stats:
                self.absorption_stats[symbol] = {}
            if target_price not in self.absorption_stats[symbol]:
                self.absorption_stats[symbol][target_price] = {
                    "cum_vol": 0, "refill_count": 0, "side": wall_side
                }

            stats = self.absorption_stats[symbol][target_price]
            stats['cum_vol']      += vol_delta
            stats['refill_count'] += 1
            stats['last_ts']       = datetime.now()

            all_levels = current['bids'] + current['asks']
            avg_level_size = (
                sum(l.get('qty', 0) for l in all_levels) / len(all_levels)
                if all_levels else 1
            )

            if stats['cum_vol'] > (avg_level_size * self.refill_multiplier):
                logger.warning(
                    f"❄️ AUTHENTIC ICEBERG: {symbol} @ {target_price} | "
                    f"Side: {wall_side} | Refilled: {stats['cum_vol']} | "
                    f"Count: {stats['refill_count']}"
                )
                if self.redis_client:
                    alert = {
                        "symbol":          symbol,
                        "time":            datetime.now().isoformat(),
                        "ltp":             float(target_price),
                        "actiontobetaken": wall_side
                    }
                    self.redis_client.publish("icebergs", json.dumps(alert))

        # Memory Management: Cleanup stats older than 5 mins
        now = datetime.now()
        for sym in list(self.absorption_stats.keys()):
            for price in list(self.absorption_stats[sym].keys()):
                if (now - self.absorption_stats[sym][price]['last_ts']).total_seconds() > 300:
                    del self.absorption_stats[sym][price]

    # -----------------------------------------------------------------------
    # ABSORPTION DETECTION (DISABLED — kept for future re-enablement)
    # -----------------------------------------------------------------------

    def _detect_absorption(self, symbol, ltp, bids, asks, fs_imbalance, packet, is_l3):
        if packet.get('vol_delta', 0) <= 0:
            return

        vqs_score = packet.get('vqs_score', 0.0)

        if symbol not in self.absorption_stats:
            return

        for price, stats in list(self.absorption_stats[symbol].items()):
            wall_side           = stats.get('side')
            is_wall_broken_up   = ltp > price + (ltp * 0.0002)
            is_wall_broken_down = ltp < price - (ltp * 0.0002)
            is_bouncing_up      = ltp >= price and ltp < price + (ltp * 0.0005)
            is_bouncing_down    = ltp <= price and ltp > price - (ltp * 0.0005)
            vol_surge           = packet.get("vol_surge", 1.0)

            if wall_side == "SELL" and is_wall_broken_up and fs_imbalance > self.imbalance_trigger:
                if vqs_score > self.vqs_trigger and vol_surge >= 5.0:
                    logger.success(
                        f"🚀 ALPHA LONG SQUEEZE: {symbol} Squeezed SELLER @ {price} | "
                        f"LTP: {ltp} | Imb: {fs_imbalance} | Surge: {vol_surge}x"
                    )
                    self._evaluate_trigger(
                        symbol, "LONG", ltp, packet, is_l3, anchor_price=price
                    )

            elif wall_side == "BUY" and is_wall_broken_down and fs_imbalance < -self.imbalance_trigger:
                if vqs_score < -self.vqs_trigger and vol_surge >= 5.0:
                    logger.success(
                        f"\033[91m🩸 ALPHA SHORT SQUEEZE: {symbol} Squeezed BUYER @ {price} | "
                        f"LTP: {ltp} | Imb: {fs_imbalance} | Surge: {vol_surge}x\033[0m"
                    )
                    self._evaluate_trigger(
                        symbol, "SHORT", ltp, packet, is_l3,
                        anchor_price=price, signal_type="ORDERFLOW_ALPHA"
                    )

            elif wall_side == "BUY" and is_bouncing_up and fs_imbalance > self.imbalance_trigger:
                if vqs_score > self.vqs_trigger and vol_surge >= 5.0:
                    logger.success(
                        f"🚀 ALPHA LONG BOUNCE: {symbol} Held BUYER Support @ {price} | "
                        f"LTP: {ltp} | Imb: {fs_imbalance} | Surge: {vol_surge}x"
                    )
                    self._evaluate_trigger(
                        symbol, "LONG", ltp, packet, is_l3,
                        anchor_price=price, signal_type="ORDERFLOW_ALPHA"
                    )

            elif wall_side == "SELL" and is_bouncing_down and fs_imbalance < -self.imbalance_trigger:
                if vqs_score < -self.vqs_trigger and vol_surge >= 5.0:
                    logger.success(
                        f"\033[91m🩸 ALPHA SHORT BOUNCE: {symbol} Held SELLER Resistance @ {price} | "
                        f"LTP: {ltp} | Imb: {fs_imbalance} | Surge: {vol_surge}x\033[0m"
                    )
                    self._evaluate_trigger(
                        symbol, "SHORT", ltp, packet, is_l3,
                        anchor_price=price, signal_type="ORDERFLOW_ALPHA"
                    )

    # -----------------------------------------------------------------------
    # MOMENTUM SQUEEZE DETECTION  ← PRIMARY STRATEGY
    # -----------------------------------------------------------------------

    def _detect_momentum_surge(self, symbol, ltp, packet, is_l3):
        """
        Momentum Squeeze — catches institutional breakouts via vol surge + VQS + VWAP alignment.

        Key fixes vs original (v1):
        1. SL/TP hardcoded to 0.8% / 2.0% (was inverted 2.0% SL / 1.0% TP)
        2. Entry surge threshold lowered to 8x (was 15x — too late in the move)
        3. Imbalance recomputed locally from packet depth (was using upstream field)
        4. Graduated VWAP guard with surge penalty (was binary 1.0% kill)
        5. NIFTY alignment check is "not opposing" rather than hard threshold
        6. Thin-tape guard: surge ratio is meaningless if avg tick vol < floor
        7. Time window starts at 10:00 (was 09:01) — avoids opening range noise
        8. Imbalance threshold raised to 0.35 (was 0.25)
        9. Per-signal-type debounce key

        New improvements (v2):
        A. ICEBERG GATE: Blocks signal if large opposing iceberg is active
        B. CONFIRMATION QUEUE: Defers to price confirmation before firing
        C. TOD WEIGHTING: Higher surge required during low-quality time windows
        D. DYNAMIC FLOOR: Adapts thin-tape floor to symbol's session activity
        E. ICEBERG BONUS: Aligned iceberg reduces required surge threshold
        """
        # --- Only evaluate on actual trade ticks ---
        if packet.get('vol_delta', 0) <= 0:
            return

        # --- Time Window: Momentum is cleanest 10:00–14:30 ---
        now_time = datetime.now().time()
        if now_time < MOMENTUM_START_TIME or now_time > MOMENTUM_END_TIME:
            return

        vol_surge = packet.get('vol_surge', 1.0)
        vqs_score = packet.get('vqs_score', 0.0)
        vwap      = packet.get('vwap', ltp)

        # ---------------------------------------------------------------
        # IMPROVEMENT D: Dynamic thin-tape guard
        # Floor adapts to the symbol's own session baseline.
        # A symbol like RELIANCE with typical avg_tick_vol of 2000+
        # gets a proportionally higher floor than a mid-cap at 600.
        # ---------------------------------------------------------------
        avg_tick_vol  = packet.get('avg_tick_vol', 0.0)
        dynamic_floor = self._get_dynamic_floor(symbol, avg_tick_vol)
        if avg_tick_vol < dynamic_floor:
            # logger.debug(
            #     # f"Momentum: {symbol} skipped — thin tape "
            #     f"(avg tick vol {avg_tick_vol:.0f} < floor {dynamic_floor:.0f})"
            # )
            return

        # ---------------------------------------------------------------
        # FIX 3: Locally recomputed imbalance from packet depth
        # ---------------------------------------------------------------
        local_imbalance = packet.get('fs_imbalance', 0.0)

        # ---------------------------------------------------------------
        # FIX 4: Graduated VWAP guard
        # ---------------------------------------------------------------
        vwap_dist_pct = abs(ltp - vwap) / vwap * 100 if vwap > 0 else 0
        if vwap_dist_pct > 1.5:
            # logger.debug(
            #     f"Momentum: {symbol} skipped — price too extended from VWAP "
            #     f"({vwap_dist_pct:.2f}% > 1.5%)"
            # )
            return
        extension_penalty = max(0.0, (vwap_dist_pct - 0.5) * 6.0)
        required_surge    = self.momentum_vol_surge + extension_penalty

        # ---------------------------------------------------------------
        # IMPROVEMENT C: Time-of-day surge weighting
        # Multiply the required surge by the TOD quality factor.
        # During dead zones (11:30–13:00) require 30% stronger surge.
        # ---------------------------------------------------------------
        tod_multiplier  = self._get_tod_surge_multiplier()
        required_surge *= tod_multiplier
        # if tod_multiplier > 1.0:
            # logger.debug(
            #     f"Momentum: {symbol} TOD multiplier {tod_multiplier:.1f}x applied "
            #     f"(required surge now {required_surge:.1f}x)"
            # )

        # ---------------------------------------------------------------
        # FIX 5: NIFTY alignment — "not opposing"
        # ---------------------------------------------------------------
        nifty_is_opposing = (
            (vqs_score > 0 and self.nifty_vqs < -0.4) or
            (vqs_score < 0 and self.nifty_vqs >  0.4)
        )
        if nifty_is_opposing:
            # logger.debug(
            #     f"Momentum: {symbol} skipped — NIFTY actively opposing "
            #     f"(NIFTY VQS: {self.nifty_vqs:.2f}, Stock VQS: {vqs_score:.2f})"
            # )
            return

        # ---------------------------------------------------------------
        # IMPROVEMENT A: Iceberg blocking gate
        # Block if a large opposing iceberg is active.
        # Apply surge bonus if an aligned iceberg is active.
        # ---------------------------------------------------------------
        side_candidate = None
        if vqs_score >= self.momentum_vqs and ltp > vwap and local_imbalance >= 0.35:
            side_candidate = "LONG"
        elif vqs_score <= -self.momentum_vqs and ltp < vwap and local_imbalance <= -0.35:
            side_candidate = "SHORT"

        if side_candidate is None:
            return  # Conditions not met — exit early before iceberg check

        is_blocked, is_aligned, block_reason = self._check_iceberg_gate(
            symbol, side_candidate
        )

        if is_blocked:
            logger.debug(
                f"Momentum: {symbol} {side_candidate} BLOCKED by iceberg gate — {block_reason}"
            )
            return

        # ---------------------------------------------------------------
        # IMPROVEMENT E: Iceberg alignment bonus
        # If an aligned iceberg exists, reduce the required surge threshold.
        # This rewards setups where the iceberg has been absorbing supply
        # and the surge is the breakout above that wall.
        # ---------------------------------------------------------------
        if is_aligned:
            adjusted_surge = required_surge * ICEBERG_ALIGNMENT_SURGE_FACTOR
            logger.debug(
                f"Momentum: {symbol} iceberg alignment bonus applied — "
                f"required surge {required_surge:.1f}x → {adjusted_surge:.1f}x"
            )
            required_surge = adjusted_surge

        if vol_surge >= required_surge:
            # ---------------------------------------------------------------
            # IMPROVEMENT B: Post-surge price confirmation
            # Instead of firing the signal immediately, register the setup
            # in the pending confirmation queue. The signal only fires if
            # price actually moves PRICE_CONFIRM_PCT in the right direction
            # within CONFIRMATION_WINDOW_SECS.
            # This filters vol surges where the price spike is immediately
            # reversed (algo whipsaw, iceberg absorption, etc.)
            # ---------------------------------------------------------------
            if symbol not in self.pending_confirmation:
                logger.info(
                    f"⏳ MOMENTUM_SQUEEZE {side_candidate} queued for confirmation: "
                    f"{symbol} @ {ltp} | Surge: {vol_surge:.1f}x "
                    f"(req {required_surge:.1f}x) | "
                    f"VQS: {vqs_score:.2f} | Imb: {local_imbalance:.2f} | "
                    f"VWAP dist: {vwap_dist_pct:.2f}% | "
                    f"TOD mult: {tod_multiplier:.1f}x | "
                    f"Iceberg aligned: {is_aligned}"
                )
                self.pending_confirmation[symbol] = {
                    "side":          side_candidate,
                    "trigger_price": ltp,
                    "trigger_time":  datetime.now(),
                    "packet":        packet.copy(),
                    "is_l3":         is_l3,
                }
            else:
                logger.debug(
                    f"Momentum: {symbol} surge qualified but confirmation already pending. "
                    f"Skipping duplicate queue entry."
                )

    # -----------------------------------------------------------------------
    # IMPROVEMENT 2: POST-SURGE PRICE CONFIRMATION CHECKER
    # -----------------------------------------------------------------------

    def _check_pending_confirmations(self, symbol, ltp, packet, is_l3):
        """
        IMPROVEMENT 2: Checks whether any pending confirmation for this symbol
        has been satisfied or expired.

        A confirmation is satisfied when:
          - Price has moved >= PRICE_CONFIRM_PCT in the signal direction
            since the trigger price

        A confirmation is expired when:
          - CONFIRMATION_WINDOW_SECS have elapsed without sufficient price move

        On confirmation: calls _evaluate_trigger with the current packet
        so entry price reflects actual market conditions at time of execution.
        """
        if symbol not in self.pending_confirmation:
            return

        pending = self.pending_confirmation[symbol]
        elapsed = (datetime.now() - pending['trigger_time']).total_seconds()

        # --- Expiry check ---
        if elapsed > CONFIRMATION_WINDOW_SECS:
            logger.debug(
                f"⏰ Confirmation EXPIRED: {symbol} {pending['side']} — "
                f"no {PRICE_CONFIRM_PCT}% move in {CONFIRMATION_WINDOW_SECS}s. "
                f"Trigger was @ {pending['trigger_price']:.2f}, current LTP {ltp:.2f}"
            )
            del self.pending_confirmation[symbol]
            return

        trigger_price = pending['trigger_price']
        side          = pending['side']
        required_move = trigger_price * (PRICE_CONFIRM_PCT / 100.0)

        confirmed = (
            (side == "LONG"  and ltp >= trigger_price + required_move) or
            (side == "SHORT" and ltp <= trigger_price - required_move)
        )

        if confirmed:
            actual_move_pct = abs(ltp - trigger_price) / trigger_price * 100
            logger.info(
                f"✅ Confirmation PASSED: {symbol} {side} — "
                f"price moved {actual_move_pct:.2f}% in {elapsed:.1f}s "
                f"(required {PRICE_CONFIRM_PCT}%). Firing signal."
            )
            # Use current packet to avoid phantom slippage upon execution
            del self.pending_confirmation[symbol]

            self._evaluate_trigger(
                symbol, side, ltp, packet, is_l3,
                signal_type="MOMENTUM_SQUEEZE", max_extension=2.0
            )

    # -----------------------------------------------------------------------
    # VWAP EXTENSION (DISABLED)
    # -----------------------------------------------------------------------

    def _detect_vwap_extension(self, symbol, ltp, packet, is_l3):
        """Fallback system 2: VWAP Deviation. Currently disabled."""
        vwap      = packet.get('vwap', 0.0)
        vol_surge = packet.get('vol_surge', 1.0)

        if vwap <= 0 or vol_surge < self.vwap_vol_surge or packet.get('vol_delta', 0) <= 0:
            return

        deviation = (ltp - vwap) / vwap

        if deviation >= self.vwap_deviation_pct:
            logger.warning(
                f"🚀 VWAP EXTENSION (LONG): {symbol} @ {ltp} | "
                f"Surge: {vol_surge}x | Deviation: {deviation:.2%}"
            )
            self._evaluate_trigger(
                symbol, "LONG", ltp, packet, is_l3, signal_type="VWAP_EXTENSION"
            )
        elif deviation <= -self.vwap_deviation_pct:
            logger.warning(
                f"\033[91m🩸 VWAP EXTENSION (SHORT): {symbol} @ {ltp} | "
                f"Surge: {vol_surge}x | Deviation: {deviation:.2%}\033[0m"
            )
            self._evaluate_trigger(
                symbol, "SHORT", ltp, packet, is_l3, signal_type="VWAP_EXTENSION"
            )

    # -----------------------------------------------------------------------
    # SCANNER BREAKOUT (DISABLED)
    # -----------------------------------------------------------------------

    def _detect_db_style_breakout(self, symbol, ltp, packet, is_l3):
        """Fallback system 3: Scanner Alignment. Currently disabled."""
        if packet.get('vol_delta', 0) <= 0:
            return

        vol_surge = packet.get('vol_surge', 1.0)
        history   = self.price_history.get(symbol, [])
        if len(history) < 50:
            return

        start_price    = history[0]
        price_move_pct = (ltp - start_price) / start_price

        if vol_surge >= 10.0 and abs(price_move_pct) >= 0.005:
            side = "LONG" if price_move_pct > 0 else "SHORT"
            logger.warning(
                f"🔍 SCANNER ALIGNMENT ({side}): {symbol} @ {ltp} | "
                f"Surge: {vol_surge:.1f}x | Move: {price_move_pct:.2%}"
            )
            self._evaluate_trigger(
                symbol, side, ltp, packet, is_l3, signal_type="SCANNER_BREAKOUT"
            )

    # -----------------------------------------------------------------------
    # TRIGGER EVALUATION (GUARDS)
    # -----------------------------------------------------------------------

    def _evaluate_trigger(
        self, symbol, side, ltp, packet, is_l3,
        anchor_price=None, signal_type="ORDERFLOW_ALPHA", max_extension=3.5
    ):
        # --- TIME WINDOW GUARD: 09:15 to 14:30 ---
        now_time   = datetime.now().time()
        start_time = dt_time(9, 1)
        end_time   = dt_time(14, 30)
        if now_time < start_time or now_time > end_time:
            return

        # --- Signal type whitelist ---
        if self.enabled_signals and signal_type not in self.enabled_signals:
            return

        # -----------------------------------------------------------------------
        # FIX 9: Per-signal-type debounce key
        # -----------------------------------------------------------------------
        debounce_secs = (
            MOMENTUM_DEBOUNCE_SECS
            if signal_type == "MOMENTUM_SQUEEZE"
            else DEFAULT_DEBOUNCE_SECS
        )
        debounce_key = f"{symbol}_{signal_type}"
        last_t = self.last_signal_time.get(debounce_key, datetime.min)
        if (datetime.now() - last_t).total_seconds() < debounce_secs:
            return

        with self.Session() as session:

            # --- EXTENSION GUARD ---
            open_p = self.open_prices.get(symbol)
            if open_p:
                move_pct = ((ltp - open_p) / open_p) * 100
                if side == "LONG" and move_pct > max_extension:
                    logger.debug(
                        f"Orchestrator: {symbol} LONG over-extended "
                        f"({move_pct:.2f}% > {max_extension}%). Ignoring."
                    )
                    return
                if side == "SHORT" and move_pct < -max_extension:
                    logger.debug(
                        f"Orchestrator: {symbol} SHORT over-extended "
                        f"({move_pct:.2f}% < -{max_extension}%). Ignoring."
                    )
                    return

            # --- DYNAMIC WATCHLIST CHECK ---
            focus = session.query(DailyFocus).filter(
                DailyFocus.symbol == symbol,
                DailyFocus.date   == date.today()
            ).first()
            if not focus:
                logger.debug(
                    f"Orchestrator: {symbol} ignored. Not in Today's DailyFocus watchlist."
                )
                return

            # -----------------------------------------------------------------------
            # ORACLE BIAS GUARD (enforce_bias)
            # If Oracle has a strong directional call and enforce_bias is set,
            # block signals that trade against the Oracle direction.
            # -----------------------------------------------------------------------
            config = self._get_config(session)
            if config.get('enforce_bias', False):
                oracle = focus.oracle_status
                if side == "LONG" and oracle == "DOWN_SNIPER":
                    logger.debug(
                        f"Orchestrator: {symbol} LONG blocked — Oracle is DOWN_SNIPER"
                    )
                    return
                if side == "SHORT" and oracle == "UP_SNIPER":
                    logger.debug(
                        f"Orchestrator: {symbol} SHORT blocked — Oracle is UP_SNIPER"
                    )
                    return

            # -----------------------------------------------------------------------
            # FIX 10: Signal-type-aware balance filter
            # -----------------------------------------------------------------------
            bid_p    = packet.get('bid_pct', 50)
            ask_p    = packet.get('ask_pct', 50)
            strength = bid_p if side == "LONG" else ask_p

            if signal_type == "MOMENTUM_SQUEEZE":
                if side == "LONG" and strength < 55.0:
                    logger.debug(
                        f"Orchestrator: {symbol} LONG momentum lacks bid dominance "
                        f"(bid_pct {strength:.1f}% < 55%). Ignoring."
                    )
                    return
                if side == "SHORT" and strength < 55.0:
                    logger.debug(
                        f"Orchestrator: {symbol} SHORT momentum lacks ask dominance "
                        f"(ask_pct {strength:.1f}% < 55%). Ignoring."
                    )
                    return
            else:
                if strength < 35.0 or strength > 65.0:
                    logger.debug(
                        f"Orchestrator: {symbol} balance {strength:.1f}% "
                        f"outside 35-65 range. Ignoring."
                    )
                    return

            # --- MARKET ALIGNMENT: NIFTY Sentiment Filter ---
            if signal_type != "MOMENTUM_SQUEEZE":
                is_aligned = (
                    (side == "LONG"  and self.nifty_vqs >  0.1) or
                    (side == "SHORT" and self.nifty_vqs < -0.1)
                )
                if not is_aligned:
                    logger.debug(
                        f"Orchestrator: {symbol} {side} avoided. "
                        f"Against Market Sentiment (NIFTY VQS: {self.nifty_vqs:.2f})"
                    )
                    return

            depth_label = "L3" if is_l3 else "L2"

            if side == "SHORT":
                logger.success(
                    f"\033[91m🔥 {signal_type} SIGNAL (DYNAMIC): {symbol} | "
                    f"{side} @ {ltp} | Vol Surge: {packet.get('vol_surge', 'N/A')}x\033[0m"
                )
            else:
                logger.success(
                    f"🔥 {signal_type} SIGNAL (DYNAMIC): {symbol} | "
                    f"{side} @ {ltp} | Vol Surge: {packet.get('vol_surge', 'N/A')}x"
                )

            self._generate_signal(
                session, symbol, side, ltp, packet, is_l3, anchor_price, signal_type
            )
            self.last_signal_time[debounce_key] = datetime.now()

    # -----------------------------------------------------------------------
    # SIGNAL GENERATION & AUTO-EXECUTION
    # -----------------------------------------------------------------------

    def _generate_signal(
        self, session, symbol, side, ltp, packet, is_l3,
        anchor_price=None, signal_type="ORDERFLOW_ALPHA"
    ):
        # -----------------------------------------------------------------------
        # SL / TP CALCULATION
        # MOMENTUM_SQUEEZE uses hardcoded parameters.
        # All other signal types use config-driven percentages.
        # -----------------------------------------------------------------------
        if signal_type == "MOMENTUM_SQUEEZE":
            sl_pct = MOMENTUM_SL_PCT  # 0.8% — hardcoded
            tp_pct = MOMENTUM_TP_PCT  # 2.0% — hardcoded
            logger.debug(
                f"Signal SL/TP: using hardcoded MOMENTUM params "
                f"(SL {sl_pct}% / TP {tp_pct}%)"
            )
        else:
            config = self._get_config(session)
            sl_pct = float(config.get('sl_pct', 2.0))
            tp_pct = float(config.get('tp_pct', 1.0))
            logger.debug(
                f"Signal SL/TP: using config params "
                f"(SL {sl_pct}% / TP {tp_pct}%)"
            )

        sl_buffer = ltp * (sl_pct / 100.0)
        if anchor_price:
            sl = anchor_price - sl_buffer if side == "LONG" else anchor_price + sl_buffer
        else:
            sl = ltp - sl_buffer if side == "LONG" else ltp + sl_buffer

        tp_buffer = ltp * (tp_pct / 100.0)
        tp = ltp + tp_buffer if side == "LONG" else ltp - tp_buffer

        # Collect iceberg context for signal metrics
        active_icebergs = self._get_active_icebergs(symbol)
        iceberg_summary = [
            {"side": i["side"], "refill_count": i["refill_count"], "price": i["price"]}
            for i in active_icebergs
        ]

        new_sig = ORBSignal(
            symbol      = symbol,
            side        = side,
            date        = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
            timestamp   = datetime.now(),
            entry_price = ltp,
            sl          = round(float(sl), 2),
            tp          = round(float(tp), 2),
            signal_type = signal_type,
            metrics     = {
                "vqs":              packet.get('vqs_score'),
                "imbalance":        packet.get('fs_imbalance'),
                "vwap":             packet.get('vwap'),
                "rsi":              packet.get('rsi'),
                "macd":             packet.get('macd'),
                "vol_surge":        packet.get('vol_surge'),
                "slope":            packet.get('slope'),
                "source":           "L3_ALPHA" if is_l3 else "L2_ALPHA",
                "wall_price":       anchor_price,
                "depth":            20 if is_l3 else 5,
                "sl_pct":           sl_pct,
                "tp_pct":           tp_pct,
                "nifty_vqs":        self.nifty_vqs,
                "active_icebergs":  iceberg_summary,   # NEW: iceberg context in signal
            },
            status  = "PENDING",
            bid_pct = packet.get('bid_pct'),
            ask_pct = packet.get('ask_pct')
        )
        session.add(new_sig)
        session.flush()

        # --- AUTO-EXECUTE LOGIC ---
        p         = self._get_config(session)
        auto_exec = False
        if   side == 'LONG'  and p.get('auto_execute_long',  False): auto_exec = True
        elif side == 'SHORT' and p.get('auto_execute_short', False): auto_exec = True

        if auto_exec:
            logger.info(
                f"Orchestrator: AUTO-EXECUTE ENABLED for {symbol}. Firing order..."
            )
            from src.db.schema import Position
            pos = Position(
                symbol          = symbol,
                side            = side,
                date            = new_sig.date,
                entry_time      = datetime.now(),
                entry_price     = new_sig.entry_price,
                qty             = 1,
                signal_type     = signal_type,
                status          = "PENDING",
                sl              = new_sig.sl,
                tp              = new_sig.tp,
                sl_type         = "ORB_BOUNDARY",
                entry_metrics   = new_sig.metrics,
                agent_audit_log = (
                    f"Automated execution trigger for Alpha Signal {new_sig.id}"
                )
            )
            try:
                session.add(pos)
                session.flush()
                remote_positions = self.port_mgr.dhan_client.get_positions()
                success, message = self.port_mgr._execute_entry(
                    session, pos, remote_positions
                )
                if success and pos.status == 'OPEN':
                    new_sig.status           = "EXECUTED"
                    new_sig.execution_pos_id = pos.id
                    logger.success(f"✅ Auto-Executed {symbol} {side} @ {ltp}")
                else:
                    logger.warning(
                        f"❌ Auto-Execution failed for {symbol}: {message}"
                    )
            except Exception as e:
                logger.error(f"Auto-Execution Error: {e}")

        session.commit()

        if side == "SHORT":
            logger.success(
                f"\033[91m🎯 {signal_type} SIGNAL: {symbol} {side} @ {ltp} | "
                f"SL: {sl} ({sl_pct}%) | TP: {tp} ({tp_pct}%)\033[0m"
            )
        else:
            logger.success(
                f"🎯 {signal_type} SIGNAL: {symbol} {side} @ {ltp} | "
                f"SL: {sl} ({sl_pct}%) | TP: {tp} ({tp_pct}%)"
            )

        notify_new_signal(
            symbol, side, signal_type, ltp,
            f"{signal_type} detected. Vol Surge: {packet.get('vol_surge')}x | "
            f"SL: {sl_pct}% | TP: {tp_pct}%"
        )


if __name__ == "__main__":
    orchestrator = OrderFlowOrchestrator()
    orchestrator.run()