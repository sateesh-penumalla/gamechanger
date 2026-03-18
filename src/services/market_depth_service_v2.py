import os
import time
import json
import random
import pytz
from datetime import datetime, date, timedelta
import threading
from loguru import logger
from dotenv import load_dotenv
import pandas as pd
import redis
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import insert as mysql_insert

# Custom Imports
from src.data.dhan_client import DhanDataClient
from src.data.dhan_feed_v2 import DhanFeedClient
from src.db.schema import DailyFocus, IntradayTick, Ticker
from src.utils.storage_manager import StorageManager
from collections import defaultdict, deque
import asyncio
import queue

# ---------------------------------------------------------------------------
# PRIORITY MIDCAP SYMBOLS
# Last reviewed: 2026-03-12. Move to DB/config if this list grows beyond 50.
# ---------------------------------------------------------------------------
PRIORITY_MIDCAPS = [
    "ALPEXSOLAR", "APEX", "AVANTIFEED", "BALUFORGE", "BELRISE",
    "BLISSGVS", "BLUESTARCO", "BSE", "COCHINSHIP", "COFORGE",
    "DBREALTY", "FORTIS", "HEXT", "IDEA", "KALYANKJIL",
    "LEMONTREE", "OLAELEC", "ONMOBILE", "ORIANA", "PFOCUS",
    "PGEL", "PREMIERENE", "RAIN", "RICOAUTO", "RPOWER",
    "TARIL", "TECHLABS", "TEJASNET"
]

load_dotenv()

log_file = "logs/market_depth_service.log"
os.makedirs("logs", exist_ok=True)
logger.add(log_file, rotation="500 MB", level="INFO", retention="10 days")
logger.info(f"MarketDepthService Logging Initialized: {log_file}")


class MarketDepthService:
    """
    Ingests Real-Time Market Depth (Level 2/3) & Ticks from DhanHQ.
    Calculates Order Flow Metrics.
    Publishes to Redis Pub/Sub.

    BSE feed is intentionally disabled — NSE data only.
    """

    REDIS_HOST    = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT    = int(os.getenv("REDIS_PORT", 6379))
    REDIS_CHANNEL = "market_depth:LIVE"

    def __init__(self, symbols=None):
        self.symbols      = symbols
        self.redis_client = None
        self.running      = True
        self.mode         = os.getenv("MODE", "PRODUCTION").upper()
        self.use_mock     = (self.mode == "MOCK")
        self.depth_source = os.getenv("DEPTH_SOURCE", "DHAN").upper()
        self.enable_dhan_deep_depth = os.getenv("ENABLE_DHAN_DEEP_DEPTH", "true").lower() == "true"

        # NOTE: BSE bridge is permanently disabled. All BSE/_BSE ticks are dropped
        # in _process_dhan_tick before they reach any state or metric calculation.

        # Database
        db_url = os.getenv("DATABASE_URL")
        self.engine       = create_engine(db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.target_indices = set()

        self._setup_redis()
        self.storage_manager = StorageManager()
        self.dhan_data = DhanDataClient()
        self.dhan_feed = None

        self._setup_dhan_feed()

        # -----------------------------------------------------------------------
        # State Buffers
        # -----------------------------------------------------------------------
        self.current_state = defaultdict(dict)
        self.price_history = defaultdict(lambda: deque(maxlen=500))
        # FIX 14: vol_history stores raw deltas; baseline avg excludes the current tick
        self.vol_history   = defaultdict(lambda: deque(maxlen=100))
        self.vqs_history   = defaultdict(lambda: deque(maxlen=100))
        self.vwap_num      = defaultdict(float)
        self.vwap_den      = defaultdict(float)
        self.pcr_data      = defaultdict(dict)

        # Tick buffer for minute-bar aggregation
        self.tick_buffer   = defaultdict(list)
        self.last_ttq      = defaultdict(int)
        self.buffer_lock   = threading.Lock()
        self.flush_thread  = None

        # Heartbeat / health
        self.tick_count          = 0
        self.last_heartbeat_time = time.time()
        self.heartbeat_thread    = None
        self.last_reset_date     = date.today()

        # FIX 8: Drop counter for visibility into queue saturation
        self._drop_count = 0

        # Daily reset watchdog
        self.watchdog_thread = threading.Thread(target=self._reset_watchdog, daemon=True)
        self.watchdog_thread.start()

        # Redis high-performance pipeline bridge
        self.redis_queue = queue.Queue(maxsize=50000)
        self.redis_worker_thread = threading.Thread(target=self._redis_worker, daemon=True)
        self.redis_worker_thread.start()

        self.option_chain_thread = None

    # -----------------------------------------------------------------------
    # SETUP
    # -----------------------------------------------------------------------

    def _setup_redis(self):
        try:
            self.redis_client = redis.Redis(
                host=self.REDIS_HOST,
                port=self.REDIS_PORT,
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
                retry_on_timeout=True
            )
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {self.REDIS_HOST}:{self.REDIS_PORT}")
        except Exception as e:
            logger.error(f"Redis Connection Failed: {e}")
            self.redis_client = None

    def _setup_dhan_feed(self):
        cid   = os.getenv("DHAN_CLIENT_ID")
        token = os.getenv("DHAN_ACCESS_TOKEN")
        if not cid or not token:
            logger.error("Dhan Credentials Missing. Falling back to MOCK.")
            self.use_mock = True
            return
        self.dhan_feed = DhanFeedClient(cid, token, enable_deep=self.enable_dhan_deep_depth)
        self.target_indices.update(["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"])

    # -----------------------------------------------------------------------
    # WATCHDOG / RESET
    # -----------------------------------------------------------------------

    def _reset_watchdog(self):
        """Monitors for new trading days and clears stale cumulative state."""
        logger.info("Daily Reset Watchdog started.")
        while True:
            try:
                today = date.today()
                if today > self.last_reset_date:
                    logger.info("New day detected. Resetting all cumulative metrics.")
                    with self.buffer_lock:
                        self.vwap_num.clear()
                        self.vwap_den.clear()
                        self.price_history.clear()
                        self.vol_history.clear()
                        self.vqs_history.clear()
                        self.last_ttq.clear()
                        self.current_state.clear()
                        self.last_reset_date = today
                time.sleep(60)
            except Exception as e:
                logger.error(f"Watchdog Error: {e}")
                time.sleep(60)

    # -----------------------------------------------------------------------
    # REDIS PIPELINE WORKER
    # -----------------------------------------------------------------------

    def _redis_worker(self):
        """Drains redis_queue and pushes to Redis using pipelining."""
        logger.info("Redis Bridge Worker: PIPELINE MODE STARTED")
        batch_size = 100
        while True:
            try:
                items = []
                try:
                    first_item = self.redis_queue.get(timeout=2.0)
                    items.append(first_item)
                except queue.Empty:
                    if not self.running:
                        break
                    continue

                while len(items) < batch_size:
                    try:
                        items.append(self.redis_queue.get_nowait())
                    except queue.Empty:
                        break

                if self.redis_client and items:
                    pipe = self.redis_client.pipeline()
                    for item in items:
                        pipe.publish(self.REDIS_CHANNEL, item['json'])
                        pipe.set(f"depth:{item['symbol']}", item['json'], ex=60)
                    pipe.execute()
                    for _ in items:
                        self.redis_queue.task_done()

            except Exception as e:
                logger.error(f"Redis Bridge Pipeline Error: {e}")
                time.sleep(1)

    # -----------------------------------------------------------------------
    # SYMBOL LOADING
    # -----------------------------------------------------------------------

    def _get_focus_symbols(self) -> list:
        """Fetch today's focus stocks + active Snipers from DB."""
        session = self.SessionLocal()
        try:
            # FIX 15: Use date.today() directly — DailyFocus.date is a Date column
            today_date = date.today()

            snipers = session.query(Ticker).filter(
                Ticker.oracle_status.in_(["UP_SNIPER", "DOWN_SNIPER"])
            ).all()

            sniper_syms = []
            for s in snipers:
                sym = s.symbol.replace(".NS", "").replace("NSE:", "").strip()
                sniper_syms.append(sym)
                if s.dhan_index_name and isinstance(s.dhan_index_name, list):
                    for idx in s.dhan_index_name:
                        self.target_indices.add(idx)
                elif s.sector and s.sector != "UNKNOWN":
                    self.target_indices.add(s.sector)

            stocks = session.query(DailyFocus).filter(
                DailyFocus.date == today_date,
                ~DailyFocus.oracle_status.like("REJECTED%")
            ).order_by(DailyFocus.avg_daily_turnover.desc()).all()

            db_symbols = [s.symbol for s in stocks]

            final_list = []
            seen = set()

            for sym in PRIORITY_MIDCAPS:
                if sym not in seen:
                    final_list.append(sym)
                    seen.add(sym)

            for sym in sniper_syms:
                if sym not in seen:
                    final_list.append(sym)
                    seen.add(sym)

            for raw_sym in db_symbols:
                sym = raw_sym.replace(".NS", "").replace("NSE:", "").strip()
                if len(final_list) >= 500:
                    break
                if sym not in seen:
                    final_list.append(sym)
                    seen.add(sym)

            logger.info(
                f"Loaded {len(final_list)} focus symbols. "
                f"(Priority: {len(PRIORITY_MIDCAPS)}, Snipers: {len(sniper_syms)}, "
                f"Total: {len(final_list)})"
            )
            logger.info(f"Target Indices for Sub: {list(self.target_indices)}")
            return final_list

        except Exception as e:
            logger.error(f"Error fetching focus symbols from DB: {e}")
            return ["ACC", "SBIN", "RELIANCE"]
        finally:
            session.close()

    # -----------------------------------------------------------------------
    # HISTORICAL PRIMING
    # -----------------------------------------------------------------------

    def _prime_historical_data(self, symbols: list):
        """Fetch morning 1-minute bars to seed VWAP and vol_history before live ticks arrive."""
        if self.use_mock:
            return

        logger.info(f"Priming historical data for {len(symbols)} symbols...")
        ist_tz     = pytz.timezone('Asia/Kolkata')
        now        = datetime.now(ist_tz)
        today_open = now.replace(hour=9, minute=15, second=0, microsecond=0)

        if now <= today_open:
            logger.info("Market not open yet. Skipping historical priming.")
            return

        # FIX 12: Simple request counter to avoid hammering REST API
        request_count  = 0
        MAX_REQUESTS_PER_MIN = 55  # Conservative Dhan REST limit
        window_start   = time.time()

        for symbol in symbols:
            try:
                # Rate limit guard
                request_count += 1
                if request_count >= MAX_REQUESTS_PER_MIN:
                    elapsed = time.time() - window_start
                    if elapsed < 60:
                        sleep_for = 60 - elapsed + 1
                        logger.info(f"REST rate limit reached. Sleeping {sleep_for:.1f}s...")
                        time.sleep(sleep_for)
                    request_count = 0
                    window_start  = time.time()

                # Check Redis coverage
                bar_key = f"bars:{symbol}"
                if self.redis_client:
                    first_bar_json = self.redis_client.lindex(bar_key, 0)
                    last_bar_json  = self.redis_client.lindex(bar_key, -1)
                    if first_bar_json and last_bar_json:
                        first_bar = json.loads(first_bar_json)
                        last_bar  = json.loads(last_bar_json)
                        first_ts  = pd.to_datetime(first_bar['timestamp']).replace(tzinfo=None)
                        last_ts   = pd.to_datetime(last_bar['timestamp']).replace(tzinfo=None)
                        has_morning = first_ts <= today_open.replace(tzinfo=None) + timedelta(minutes=5)
                        is_fresh    = last_ts  >= now.replace(tzinfo=None) - timedelta(minutes=5)
                        if has_morning and is_fresh:
                            logger.debug(f"Redis has full coverage for {symbol}. Skipping.")
                            request_count -= 1  # didn't actually make a request
                            continue

                logger.info(f"Priming {symbol} from Dhan REST...")
                from_date_str = today_open.strftime("%Y-%m-%d")
                df = self.dhan_data.fetch_realtime_data(symbol, interval="1m", from_date_str=from_date_str)
                if df is not None and not df.empty:
                    df = df[df.index >= today_open.replace(tzinfo=None)]

                if df is None or df.empty:
                    time.sleep(0.1)
                    continue

                # Sync to Redis
                if self.redis_client:
                    existing_bars = self.redis_client.lrange(bar_key, 0, -1)
                    existing_ts   = set()
                    earliest_ts   = None

                    for b_json in existing_bars:
                        b      = json.loads(b_json)
                        ts_val = pd.to_datetime(b['timestamp']).replace(tzinfo=None)
                        existing_ts.add(ts_val)
                        if earliest_ts is None or ts_val < earliest_ts:
                            earliest_ts = ts_val

                    for ts, row in df.sort_index(ascending=False).iterrows():
                        clean_ts = ts.replace(tzinfo=None)
                        if clean_ts not in existing_ts:
                            bar = {
                                "symbol":    symbol,
                                "timestamp": ts.isoformat(),
                                "open":      float(row['Open']),
                                "high":      float(row['High']),
                                "low":       float(row['Low']),
                                "close":     float(row['Close']),
                                "volume":    int(row['Volume']),
                                "source":    "DHAN_PRIME"
                            }
                            if earliest_ts and clean_ts < earliest_ts:
                                self.redis_client.lpush(bar_key, json.dumps(bar))
                            else:
                                self.redis_client.rpush(bar_key, json.dumps(bar))
                    self.redis_client.expire(bar_key, 86400)

                # Sync to MySQL
                session = self.SessionLocal()
                try:
                    bars_to_upsert = []
                    for ts, row in df.iterrows():
                        clean_ts = ts.replace(tzinfo=None) if hasattr(ts, 'tzinfo') else ts
                        bars_to_upsert.append({
                            "symbol":           symbol,
                            "timestamp":        clean_ts,
                            "open":             float(row['Open']),
                            "high":             float(row['High']),
                            "low":              float(row['Low']),
                            "close":            float(row['Close']),
                            "volume":           int(row['Volume']),
                            "total_turnover":   0.0,
                            "source":           "DHAN_PRIME",
                            "buy_volume":       0.0,
                            "sell_volume":      0.0,
                            "avg_bid_qty":      0.0,
                            "avg_ask_qty":      0.0,
                            "mean_imbalance":   0.0,
                            "iceberg_score":    0.0,
                            "iceberg_timestamp": None,
                            "iceberg_side":     None,
                            "total_bid_qty":    0.0,
                            "total_ask_qty":    0.0,
                            "bid_pct":          50.0,
                            "ask_pct":          50.0
                        })
                    for bar_data in bars_to_upsert:
                        stmt = mysql_insert(IntradayTick).values(bar_data)
                        update_dict = {
                            c.name: getattr(stmt.inserted, c.name)
                            for c in IntradayTick.__table__.columns
                            if not c.primary_key and c.name != 'last_updated'
                        }
                        session.execute(stmt.on_duplicate_key_update(**update_dict))
                    session.commit()
                except Exception as e:
                    session.rollback()
                    logger.error(f"MySQL Priming Error for {symbol}: {e}")
                finally:
                    session.close()

                # Seed in-memory state
                last_row = df.iloc[-1]
                self.current_state[symbol] = {
                    "ltp":               float(last_row['Close']),
                    "volume":            int(last_row['Volume']),
                    "bid_price":         0.0, "bid_qty":   0.0,
                    "ask_price":         0.0, "ask_qty":   0.0,
                    "total_bid_qty":     0.0, "total_ask_qty": 0.0,
                    "v_buy":             0.0, "v_sell":    0.0,
                    "prev_ltp":          float(last_row['Close']),
                    "bids":              [],  "asks":      [],
                    "current_bar_volume": 0.0
                }

                # Seed VWAP from historical bars
                total_v = df['Volume'].sum()
                if total_v > 0:
                    self.vwap_num[symbol] = float((df['Close'] * df['Volume']).sum())
                    self.vwap_den[symbol] = float(total_v)

                # Seed vol_history with per-bar volume deltas for a valid surge baseline
                vols = df['Volume'].diff().dropna()
                for v in vols:
                    if v > 0:
                        self.vol_history[symbol].append(float(v))

                self.last_ttq[symbol] = int(last_row['Volume'])

                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error priming {symbol}: {e}")

    # -----------------------------------------------------------------------
    # TICK PROCESSING  (Dhan path — primary)
    # -----------------------------------------------------------------------

    def _process_dhan_tick(self, dhan_tick: dict):
        """Processes a normalized tick from DhanFeedClient."""
        try:
            symbol = dhan_tick.get('symbol')
            if not symbol:
                return

            # FIX: Drop ALL BSE ticks immediately — we operate NSE-only
            if str(symbol).endswith("_BSE") or symbol == "SENSEX":
                return

            self.tick_count += 1  # FIX 1: Single increment point per tick

            # Derivative ticks: OI update only, no depth publishing needed
            if dhan_tick.get('is_derivative'):
                if symbol not in self.current_state:
                    self.current_state[symbol] = {"oi": 0.0, "ltp": 0.0}
                oi = float(dhan_tick.get('oi', 0.0))
                if oi > 0:
                    self.current_state[symbol]['oi'] = oi
                return  # Don't publish an OI-only packet; wait for next LTP tick

            # Initialise state defaults for new symbols
            if symbol not in self.current_state:
                self.current_state[symbol] = {}
            state = self.current_state[symbol]

            defaults = {
                "ltp": 0.0, "volume": 0, "bid_price": 0.0, "bid_qty": 0.0,
                "ask_price": 0.0, "ask_qty": 0.0, "total_bid_qty": 0.0,
                "total_ask_qty": 0.0, "v_buy": 0.0, "v_sell": 0.0,
                "prev_ltp": 0.0, "bids": [], "asks": [],
                "current_bar_volume": 0.0, "oi": 0.0
            }
            for k, v in defaults.items():
                if k not in state:
                    state[k] = v

            bids   = dhan_tick.get('bid', [])
            asks   = dhan_tick.get('ask', [])
            ltp    = float(dhan_tick.get('ltp', 0.0))
            volume = int(dhan_tick.get('volume', 0))

            # Update LTP
            if ltp > 0:
                state['ltp'] = ltp

            # Update Volume and derived metrics
            if volume > 0:
                prev_vol = state.get('volume', 0)
                if prev_vol > 0 and volume > prev_vol:
                    diff     = volume - prev_vol
                    prev_ltp = state.get('prev_ltp', state['ltp'])

                    if state['ltp'] > prev_ltp:
                        state['v_buy'] += diff
                    elif state['ltp'] < prev_ltp:
                        state['v_sell'] += diff

                    state['current_bar_volume'] = state.get('current_bar_volume', 0.0) + diff

                    # VWAP accumulation
                    self.vwap_num[symbol] += (state['ltp'] * diff)
                    self.vwap_den[symbol] += diff

                    # FIX 2: vol_history updated here (was missing in L1 path entirely)
                    self.vol_history[symbol].append(float(diff))

                state['volume'] = volume

            # Update Depth (L1 best bid/ask)
            if bids:
                state['bid_qty']   = float(bids[0].get('qty', 0))
                state['bid_price'] = float(bids[0].get('price', 0))
                state['bids']      = bids
            if asks:
                state['ask_qty']   = float(asks[0].get('qty', 0))
                state['ask_price'] = float(asks[0].get('price', 0))
                state['asks']      = asks

            # Full-stack total quantities (L3 if available, else sum of available levels)
            total_bid_qty = float(dhan_tick.get('total_buy_qty', 0.0))
            total_ask_qty = float(dhan_tick.get('total_sell_qty', 0.0))

            if total_bid_qty > 0:
                state['total_bid_qty'] = total_bid_qty
            elif bids:
                state['total_bid_qty'] = float(sum(b.get('qty', 0) for b in bids))

            if total_ask_qty > 0:
                state['total_ask_qty'] = total_ask_qty
            elif asks:
                state['total_ask_qty'] = float(sum(a.get('qty', 0) for a in asks))

            # Publish — single exit point for all metric computation
            self._publish_depth_packet(symbol, state)
            state['prev_ltp'] = state['ltp']

        except Exception as e:
            logger.error(f"Error processing Dhan tick for {dhan_tick.get('symbol', 'UNKNOWN')}: {e}")

    # -----------------------------------------------------------------------
    # METRIC COMPUTATION & PUBLISHING
    # -----------------------------------------------------------------------

    def _publish_depth_packet(self, target_symbol: str, state: dict):
        """
        Single exit point: computes all metrics from current state and
        publishes a complete packet to Redis.
        """
        try:
            ltp = state.get('ltp', 0.0)
            if ltp <= 0:
                return

            # FIX 6: price_history appended ONLY here (was also appended in _process_tick,
            # causing double-sampling and VQS bias)
            self.price_history[target_symbol].append(ltp)

            # ---------------------------------------------------------------
            # FIX 11: Imbalance uses full-stack totals, not just L1 best bid/ask
            # Falls back to L1 only if totals are unavailable.
            # ---------------------------------------------------------------
            total_bid_qty = state.get('total_bid_qty', 0.0)
            total_ask_qty = state.get('total_ask_qty', 0.0)
            bid_qty       = state.get('bid_qty', 0.0)
            ask_qty       = state.get('ask_qty', 0.0)

            fs_bid = total_bid_qty if total_bid_qty > 0 else bid_qty
            fs_ask = total_ask_qty if total_ask_qty > 0 else ask_qty
            combined_fs   = fs_bid + fs_ask
            imbalance     = (fs_bid - fs_ask) / combined_fs if combined_fs > 0 else 0.0

            # L1-only imbalance (kept for reference in tick buffer)
            l1_combined   = bid_qty + ask_qty
            l1_imbalance  = (bid_qty - ask_qty) / l1_combined if l1_combined > 0 else 0.0

            # VQS (Momentum)
            history = list(self.price_history[target_symbol])
            if len(history) > 1:
                ticks = []
                for i in range(1, len(history)):
                    if   history[i] > history[i - 1]: ticks.append(1)
                    elif history[i] < history[i - 1]: ticks.append(-1)
                vqs_score = sum(ticks) / len(ticks) if ticks else 0.0
            else:
                vqs_score = 0.0

            # VWAP
            vwap_num = self.vwap_num[target_symbol]
            vwap_den = self.vwap_den[target_symbol]
            vwap     = vwap_num / vwap_den if vwap_den > 0 else ltp

            # ---------------------------------------------------------------
            # FIX 14: vol_surge baseline excludes the current tick so a genuine
            # spike doesn't inflate the average and suppress itself next tick.
            # ---------------------------------------------------------------
            vol_history = list(self.vol_history[target_symbol])
            if len(vol_history) >= 2:
                baseline = vol_history[:-1]         # all but the current tick
                avg_vol  = sum(baseline) / len(baseline)
                last_vol = vol_history[-1]
                vol_surge = round(last_vol / avg_vol, 2) if avg_vol > 0 else 1.0
            else:
                vol_surge = 1.0

            combined_total = total_bid_qty + total_ask_qty

            # ---------------------------------------------------------------
            # FIX 13: L3 detection depth logging
            # If depth has exactly 5 levels, log at DEBUG so silent degradation
            # is visible. bids_20/asks_20 only added when > 5 levels confirmed.
            # ---------------------------------------------------------------
            bids = state.get('bids', [])
            asks = state.get('asks', [])
            if 0 < len(bids) <= 5:
                logger.debug(
                    f"{target_symbol}: Only {len(bids)} bid levels available "
                    f"— running in L2 mode (no bids_20 published)"
                )

            packet = {
                "symbol":             target_symbol,
                "ltp":                ltp,
                "imbalance":          round(imbalance, 4),     # full-stack
                "l1_imbalance":       round(l1_imbalance, 4),  # best bid/ask only
                "vwap":               round(vwap, 2),
                "vqs_score":          round(vqs_score, 4),
                "vol_surge":          vol_surge,
                "volume":             state.get('volume', 0),
                "current_bar_volume": state.get('current_bar_volume', 0.0),
                "bid_qty":            bid_qty,
                "ask_qty":            ask_qty,
                "total_bid_qty":      total_bid_qty,
                "total_ask_qty":      total_ask_qty,
                "bid_pct":            round(total_bid_qty / combined_total * 100, 2) if combined_total > 0 else 50.0,
                "ask_pct":            round(total_ask_qty / combined_total * 100, 2) if combined_total > 0 else 50.0,
                "oi":                 state.get('oi', 0.0),
                "buy_vol":            state.get('v_buy', 0.0),
                "sell_vol":           state.get('v_sell', 0.0),
                "pcr_oi":             self.pcr_data[target_symbol].get('pcr_oi', 0.0),
                "pcr_vol":            self.pcr_data[target_symbol].get('pcr_vol', 0.0),
                "max_pain":           self.pcr_data[target_symbol].get('max_pain', 0.0),
                "timestamp":          datetime.now(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).isoformat(),
                "source":             self.depth_source,
                "bids":               bids[:5],
                "asks":               asks[:5],
            }

            # Attach full 20-level depth only when genuinely available
            if len(bids) > 5:
                packet["bids_20"] = bids
                packet["asks_20"] = asks

            # FIX 3: Reset accumulators as float (was int 0)
            state['v_buy']  = 0.0
            state['v_sell'] = 0.0

            # Enqueue for Redis pipeline (non-blocking)
            try:
                self.redis_queue.put_nowait({
                    "symbol": target_symbol,
                    "json":   json.dumps(packet)
                })
            except queue.Full:
                # FIX 8: Count drops; heartbeat will report total per minute
                self._drop_count += 1
                if self._drop_count % 500 == 1:   # log at first drop then every 500
                    logger.warning(
                        f"⚠️ Redis queue FULL — packets being dropped "
                        f"(total this minute: {self._drop_count}, "
                        f"queue size: {self.redis_queue.qsize()})"
                    )

            with self.buffer_lock:
                self.tick_buffer[target_symbol].append(packet)

        except Exception as e:
            logger.error(f"Error publishing depth packet for {target_symbol}: {e}")

    # -----------------------------------------------------------------------
    # STORAGE FLUSH
    # -----------------------------------------------------------------------

    def _flush_to_storage(self):
        """Periodically aggregates buffered ticks into 1-min OHLCV bars and persists them."""
        logger.info("Storage Flush Thread started. Aligning to next minute boundary...")
        while self.running:
            try:
                ist_tz   = pytz.timezone('Asia/Kolkata')
                now      = datetime.now(ist_tz)
                next_min = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
                sleep_sec = (next_min - now).total_seconds()
                if sleep_sec > 0:
                    time.sleep(sleep_sec)

                flush_start = time.time()

                with self.buffer_lock:
                    if not self.tick_buffer:
                        continue
                    current_buffer   = dict(self.tick_buffer)
                    self.tick_buffer = defaultdict(list)

                try:
                    bars_to_upsert = []

                    threading.Thread(
                        target=self._async_parquet_archive,
                        args=(current_buffer,),
                        daemon=True
                    ).start()

                    for symbol, ticks in current_buffer.items():
                        if not ticks:
                            continue

                        df              = pd.DataFrame(ticks)
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df['minute_bin'] = df['timestamp'].dt.floor('1min')

                        for minute_val, min_df in df.groupby('minute_bin'):
                            df_ohlc = min_df[min_df['ltp'] > 0]
                            if df_ohlc.empty:
                                continue

                            total_buy_vol  = min_df['buy_vol'].sum()
                            total_sell_vol = min_df['sell_vol'].sum()
                            total_vol      = total_buy_vol + total_sell_vol

                            bars_to_upsert.append({
                                "symbol":           symbol,
                                "timestamp":        minute_val.to_pydatetime(),
                                "open":             float(df_ohlc['ltp'].iloc[0]),
                                "high":             float(df_ohlc['ltp'].max()),
                                "low":              float(df_ohlc['ltp'].min()),
                                "close":            float(df_ohlc['ltp'].iloc[-1]),
                                "volume":           int(total_vol) if total_vol > 0 else int(df_ohlc['ltp'].count()),
                                "buy_volume":       float(total_buy_vol),
                                "sell_volume":      float(total_sell_vol),
                                "avg_bid_qty":      float(min_df['bid_qty'].mean()),
                                "avg_ask_qty":      float(min_df['ask_qty'].mean()),
                                "total_bid_qty":    float(min_df['total_bid_qty'].mean()) if 'total_bid_qty' in min_df.columns else 0.0,
                                "total_ask_qty":    float(min_df['total_ask_qty'].mean()) if 'total_ask_qty' in min_df.columns else 0.0,
                                "bid_pct":          float(min_df['bid_pct'].mean()) if 'bid_pct' in min_df.columns else 50.0,
                                "ask_pct":          float(min_df['ask_pct'].mean()) if 'ask_pct' in min_df.columns else 50.0,
                                "mean_imbalance":   float(min_df['imbalance'].mean()),
                                "iceberg_score":    0.0,
                                "iceberg_timestamp": None,
                                "iceberg_side":     None,
                                "total_turnover":   float(df_ohlc['ltp'].sum()),
                                "oi":               float(min_df['oi'].mean()) if 'oi' in min_df.columns else 0.0,
                                "pcr_oi":           float(min_df['pcr_oi'].mean()) if 'pcr_oi' in min_df.columns else 0.0,
                                "pcr_vol":          float(min_df['pcr_vol'].mean()) if 'pcr_vol' in min_df.columns else 0.0,
                                "max_pain":         float(min_df['max_pain'].mean()) if 'max_pain' in min_df.columns else 0.0,
                                "source":           self.depth_source
                            })

                    # Sync bars to Redis
                    if self.redis_client and bars_to_upsert:
                        try:
                            for bar in bars_to_upsert:
                                bar_key  = f"bars:{bar['symbol']}"
                                bar_copy = bar.copy()
                                if isinstance(bar_copy['timestamp'], datetime):
                                    bar_copy['timestamp'] = bar_copy['timestamp'].isoformat()
                                self.redis_client.rpush(bar_key, json.dumps(bar_copy))
                                self.redis_client.expire(bar_key, 86400)
                            logger.info(f"Synced {len(bars_to_upsert)} bars to Redis.")
                        except Exception as redis_err:
                            logger.error(f"Redis bar sync failed: {redis_err}")

                    if bars_to_upsert:
                        threading.Thread(
                            target=self._async_mysql_upsert,
                            args=(bars_to_upsert,),
                            daemon=True
                        ).start()

                    # FIX 7 + FIX 10: Reset current_bar_volume under buffer_lock,
                    # iterating a snapshot of keys to avoid RuntimeError on dict resize
                    with self.buffer_lock:
                        for s in list(self.current_state.keys()):
                            self.current_state[s]['current_bar_volume'] = 0.0

                    duration = time.time() - flush_start
                    logger.success(f"Storage flush completed in {duration:.2f}s ({len(bars_to_upsert)} bars)")

                except Exception as e:
                    logger.error(f"Storage flush aggregation error: {e}")

            except Exception as e:
                logger.error(f"Storage flush loop error: {e}")

    def _async_mysql_upsert(self, bars: list):
        """Upserts 1-min bars to MySQL in a background thread."""
        session = None
        try:
            session    = self.SessionLocal()
            start_time = time.time()
            stmt       = mysql_insert(IntradayTick).values(bars)
            # FIX 4: Correct update_dict construction using getattr(stmt.inserted, c.name)
            # (was `c.name: c` which set Column objects as values, not inserted row values)
            update_dict = {
                c.name: getattr(stmt.inserted, c.name)
                for c in IntradayTick.__table__.columns
                if not c.primary_key and c.name != 'last_updated'
            }
            session.execute(stmt.on_duplicate_key_update(**update_dict))
            session.commit()
            logger.success(
                f"Async MySQL upsert: {len(bars)} bars in {time.time() - start_time:.2f}s"
            )
        except Exception as e:
            if session:
                session.rollback()
            logger.error(f"Async MySQL upsert failed: {e}")
        finally:
            if session:
                session.close()

    def _async_parquet_archive(self, buffer: dict):
        """Archives raw ticks to Parquet in the background."""
        try:
            start_time = time.time()
            count      = 0
            for symbol, ticks in buffer.items():
                self.storage_manager.save_ticks_parquet(symbol, ticks)
                count += len(ticks)
            logger.success(
                f"Parquet archive: {count} ticks for {len(buffer)} symbols "
                f"in {time.time() - start_time:.2f}s"
            )
        except Exception as e:
            logger.error(f"Parquet archive failed: {e}")

    # -----------------------------------------------------------------------
    # MOCK MODE
    # -----------------------------------------------------------------------

    def _run_mock_loop(self):
        """Generates synthetic ticks when credentials are missing."""
        logger.info("Starting Mock Data Loop...")
        symbols = self.symbols or self._get_focus_symbols()
        while self.running:
            for symbol in symbols:
                price = round(random.uniform(100, 3000), 2)
                mock_tick = {
                    "symbol":   symbol,
                    "ltp":      price,
                    "volume":   random.randint(1000, 50000),
                    "bid":      [{"price": price - 0.05, "qty": random.randint(100, 2000)}],
                    "ask":      [{"price": price + 0.05, "qty": random.randint(100, 2000)}],
                    "total_buy_qty":  random.randint(5000, 20000),
                    "total_sell_qty": random.randint(5000, 20000),
                }
                self._process_dhan_tick(mock_tick)
            time.sleep(1)

    # -----------------------------------------------------------------------
    # RUN
    # -----------------------------------------------------------------------

    def run(self):
        self.running = True

        if self.use_mock:
            logger.info("MarketDepthService started in MOCK mode.")
            self.flush_thread = threading.Thread(target=self._flush_to_storage, daemon=True)
            self.flush_thread.start()
            self._run_mock_loop()
            return

        self.symbols = self.symbols or self._get_focus_symbols()

        self.heartbeat_thread = threading.Thread(target=self._run_heartbeat, daemon=True)
        self.heartbeat_thread.start()

        threading.Thread(
            target=self._prime_historical_data,
            args=(self.symbols,),
            daemon=True
        ).start()

        self.option_chain_thread = threading.Thread(
            target=self._run_option_chain_loop,
            daemon=True
        )
        self.option_chain_thread.start()

        self._run_dhan()

    def _run_dhan(self):
        """Initialises and runs the Dhan 20-level depth feed."""
        logger.info(f"Starting Dhan feed. Total symbols: {len(self.symbols)}")

        priority_syms  = ["SBIN", "RELIANCE", "BALUFORGE", "TEJASNET"]
        sorted_symbols = sorted(self.symbols, key=lambda x: (x not in priority_syms, x))

        id_map      = {}
        stock_ids   = []
        self.futures_to_cash = {}

        for sym in sorted_symbols:
            sec_id = self.dhan_data.get_security_id(sym)
            if sec_id:
                id_map[sec_id] = sym
                stock_ids.append(sec_id)

            fut_id = self.dhan_data.get_active_futures_id(sym)
            if fut_id:
                self.futures_to_cash[fut_id] = sym
                if fut_id not in stock_ids:
                    stock_ids.append(fut_id)

        index_map      = self.dhan_data.get_selective_indices(index_names=list(self.target_indices))
        all_index_ids  = list(index_map.values())

        comprehensive_meta = {}
        for sym in sorted_symbols:
            meta = self.dhan_data._meta_map.get(sym)
            if meta:
                comprehensive_meta[meta['id']] = {"symbol": sym, "segment": meta['segment']}

            fut_id = self.dhan_data.get_active_futures_id(sym)
            if fut_id:
                fno_list = self.dhan_data._fno_map.get(sym.replace(".NS", ""))
                if fno_list:
                    fut_meta = next((f for f in fno_list if f['id'] == fut_id), None)
                    if fut_meta:
                        comprehensive_meta[fut_id] = {
                            "symbol":        f"{sym}_FUT",
                            "is_derivative": True,
                            "segment":       fut_meta['segment']
                        }

        for sym, sid in index_map.items():
            meta = self.dhan_data._meta_map.get(sym)
            if meta:
                comprehensive_meta[sid] = {"symbol": sym, "segment": meta['segment']}
            else:
                comprehensive_meta[sid] = {"symbol": sym, "segment": "IDX_I"}

        self.dhan_feed.set_id_meta_mapping(comprehensive_meta)

        std_ids  = stock_ids + all_index_ids
        deep_ids = stock_ids[:50]

        logger.info(
            f"Dhan Feed: Standard={len(std_ids)} "
            f"(Stocks={len(stock_ids)}, Indices={len(all_index_ids)}), "
            f"Deep={len(deep_ids)}"
        )
        logger.info(f"Indices subscribed: {list(index_map.keys())}")

        self.flush_thread = threading.Thread(target=self._flush_to_storage, daemon=True)
        self.flush_thread.start()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                self.dhan_feed.connect(std_ids, deep_ids, on_tick=self._process_dhan_tick)
            )
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            logger.error(f"Dhan Feed Error: {e}")

    # -----------------------------------------------------------------------
    # HEARTBEAT
    # -----------------------------------------------------------------------

    def _run_heartbeat(self):
        """Logs system health every 60 seconds."""
        logger.info("Heartbeat thread started.")
        while self.running:
            try:
                time.sleep(60)
                now     = time.time()
                elapsed = now - self.last_heartbeat_time
                tps     = self.tick_count / elapsed if elapsed > 0 else 0

                worker_alive = (
                    self.redis_worker_thread.is_alive()
                    if self.redis_worker_thread else False
                )
                q_size = self.redis_queue.qsize() if self.redis_queue else 0

                # FIX 8: Report drop count per minute
                logger.info(
                    f"❤️ HEARTBEAT: {self.tick_count} ticks in {elapsed:.1f}s "
                    f"({tps:.2f} tps) | Redis worker: {'ALIVE' if worker_alive else '⚠️ DEAD'} "
                    f"| Queue: {q_size} | Dropped this min: {self._drop_count}"
                )
                self.tick_count  = 0
                self._drop_count = 0
                self.last_heartbeat_time = now

            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    # -----------------------------------------------------------------------
    # OPTION CHAIN
    # -----------------------------------------------------------------------

    def _run_option_chain_loop(self):
        """Fetches and analyses Option Chain every 5 minutes (respecting total loop time)."""
        logger.info("Option Chain Analysis Thread started.")
        while self.running:
            try:
                # FIX 9: Track total loop duration so sleep(300) means 300s BETWEEN
                # loop starts, not 300s after a potentially long loop finishes
                loop_start = time.time()

                for symbol in self.symbols:
                    if not self.running:
                        break

                    sid = self.dhan_data.get_security_id(symbol)
                    if not sid:
                        continue

                    seg      = "IDX_I" if symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"] else "NSE_EQ"
                    fno_list = self.dhan_data._fno_map.get(symbol.replace(".NS", ""))
                    if not fno_list:
                        continue

                    option_expiries = sorted(list(set(
                        f['expiry'] for f in fno_list
                        if f.get('instrument_type') in ['OPTIDX', 'OPTSTK']
                    )))
                    if not option_expiries:
                        continue

                    chain = self.dhan_data.get_option_chain(sid, seg, option_expiries[0])
                    if chain:
                        self._calculate_pcr_and_pain(symbol, chain)
                        # FIX 17: Per-symbol log at DEBUG, not INFO (500 symbols × every 5min = log flood)
                        logger.debug(
                            f"Option chain [{symbol}]: "
                            f"PCR={self.pcr_data[symbol]['pcr_oi']:.2f}, "
                            f"MaxPain={self.pcr_data[symbol]['max_pain']}"
                        )

                # FIX 17: One INFO summary per cycle
                logger.info(
                    f"Option chain cycle complete for {len(self.symbols)} symbols "
                    f"in {time.time() - loop_start:.1f}s"
                )

                # FIX 9: Sleep only the REMAINING time to ensure 300s total cadence
                elapsed        = time.time() - loop_start
                sleep_remaining = max(0, 300 - elapsed)
                if sleep_remaining < 10:
                    logger.warning(
                        f"Option chain loop took {elapsed:.1f}s — longer than 300s interval. "
                        f"Consider reducing symbol count or increasing interval."
                    )
                time.sleep(sleep_remaining)

            except Exception as e:
                logger.error(f"Option chain loop error: {e}")
                time.sleep(60)

    def _calculate_pcr_and_pain(self, symbol: str, chain: list):
        """Calculates PCR (OI/Volume) and Max Pain from Option Chain data."""
        total_call_oi  = 0
        total_put_oi   = 0
        total_call_vol = 0
        total_put_vol  = 0
        strikes        = []

        for strike_data in chain:
            strike_price = float(strike_data['strike_price'])
            strikes.append(strike_price)
            total_call_oi  += float(strike_data.get('call_oi', 0))
            total_call_vol += float(strike_data.get('call_volume', 0))
            total_put_oi   += float(strike_data.get('put_oi', 0))
            total_put_vol  += float(strike_data.get('put_volume', 0))

        pcr_oi  = total_put_oi  / total_call_oi  if total_call_oi  > 0 else 0.0
        pcr_vol = total_put_vol / total_call_vol if total_call_vol > 0 else 0.0

        # Max Pain
        min_pain         = float('inf')
        max_pain_strike  = 0
        unique_strikes   = sorted(list(set(strikes)))

        for test_strike in unique_strikes:
            total_loss = 0
            for strike_data in chain:
                strike = float(strike_data['strike_price'])
                c_oi   = float(strike_data.get('call_oi', 0))
                p_oi   = float(strike_data.get('put_oi', 0))
                if test_strike > strike:
                    total_loss += (test_strike - strike) * c_oi
                if test_strike < strike:
                    total_loss += (strike - test_strike) * p_oi
            if total_loss < min_pain:
                min_pain        = total_loss
                max_pain_strike = test_strike

        self.pcr_data[symbol] = {
            "pcr_oi":     round(pcr_oi, 4),
            "pcr_vol":    round(pcr_vol, 4),
            "max_pain":   max_pain_strike,
            "updated_at": datetime.now().isoformat()
        }

        if self.redis_client:
            all_calls = sorted(
                [s for s in chain if s.get('call_oi', 0) > 0],
                key=lambda x: x.get('call_oi', 0), reverse=True
            )[:5]
            all_puts = sorted(
                [s for s in chain if s.get('put_oi', 0) > 0],
                key=lambda x: x.get('put_oi', 0), reverse=True
            )[:5]
            summary = {
                "pcr_oi":          round(pcr_oi, 4),
                "pcr_vol":         round(pcr_vol, 4),
                "max_pain":        max_pain_strike,
                "top_call_walls":  [{"strike": c['strike_price'], "oi": c['call_oi']} for c in all_calls],
                "top_put_walls":   [{"strike": p['strike_price'], "oi": p['put_oi']} for p in all_puts],
                "updated_at":      datetime.now().isoformat()
            }
            self.redis_client.set(f"sentiment:{symbol}", json.dumps(summary), ex=3600)

    # -----------------------------------------------------------------------
    # STOP
    # -----------------------------------------------------------------------

    def stop(self):
        """Gracefully shuts down the service, flushing the Redis pipeline queue."""
        self.running = False
        logger.info("Stopping MarketDepthService...")
        # FIX 18: Wait for the Redis worker to drain all queued packets before exit
        try:
            self.redis_queue.join()
            logger.info("Redis pipeline queue drained cleanly.")
        except Exception as e:
            logger.warning(f"Redis queue join error during stop: {e}")


if __name__ == "__main__":
    service = MarketDepthService()
    try:
        service.run()
    except KeyboardInterrupt:
        service.stop()