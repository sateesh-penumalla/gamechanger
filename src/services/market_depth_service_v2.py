import os
import time
import json
import logging
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
from src.data.truedata_client import TrueDataClient
from src.data.dhan_client import DhanDataClient
from src.data.dhan_feed_v2 import DhanFeedClient
from src.db.schema import DailyFocus, IntradayTick, Ticker
from src.utils.storage_manager import StorageManager
from collections import defaultdict, deque
import asyncio
import queue # Added for high-performance bridge

# Priority Midcap Symbols as requested by user
PRIORITY_MIDCAPS = [
    "ALPEXSOLAR", "APEX", "AVANTIFEED", "BALUFORGE", "BELRISE", 
    "BLISSGVS", "BLUESTARCO", "BSE", "COCHINSHIP", "COFORGE", 
    "DBREALTY", "FORTIS", "HEXT", "IDEA", "KALYANKJIL", 
    "LEMONTREE", "OLAELEC", "ONMOBILE", "ORIANA", "PFOCUS", 
    "PGEL", "PREMIERENE", "RAIN", "RICOAUTO", "RPOWER", 
    "TARIL", "TECHLABS", "TEJASNET"
]

load_dotenv()

# Configure Logging
log_file = "logs/market_depth_service.log"
os.makedirs("logs", exist_ok=True)
logger.add(log_file, rotation="500 MB", level="INFO", retention="10 days")
logger.info(f"MarketDepthService Logging Initialized: {log_file}")

class MarketDepthService:
    """
    Ingests Real-Time Market Depth (Level 2) & Ticks from TrueData.
    Calculates Order Flow Metrics.
    Publishes to Redis Pub/Sub.
    """
    
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_CHANNEL = "market_depth:LIVE"
    
    def __init__(self, symbols=None):
        self.symbols = symbols # Will fetch if None
        self.td_client = None
        self.redis_client = None
        self.running = True
        self.mode = os.getenv("TRUEDATA_MODE", "PRODUCTION").upper()
        self.use_mock = (self.mode == "MOCK")
        self.enable_bse_bridge = os.getenv("ENABLE_BSE_BRIDGE", "true").lower() == "true"
        self.depth_source = os.getenv("DEPTH_SOURCE", "TRUEDATA").upper()
        self.enable_dhan_deep_depth = os.getenv("ENABLE_DHAN_DEEP_DEPTH", "true").lower() == "true"
        
        # Database setup
        db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.target_indices = set() # Track indices for selective subscription
        
        self._setup_redis()
        self.storage_manager = StorageManager()
        self.dhan_data = DhanDataClient() # Helper for ID mapping
        self.dhan_feed = None
        
        if self.depth_source == "TRUEDATA":
            self._setup_truedata()
        elif self.depth_source == "DHAN":
            self._setup_dhan_feed()
        
        # Buffering for metrics calculation
        self.current_state = defaultdict(dict) # Last known state: {symbol: {ltp, v, bid, ask, etc.}}
        self.price_history = defaultdict(lambda: deque(maxlen=500))
        self.vol_history = defaultdict(lambda: deque(maxlen=100))   # Last 100 ticks
        self.vqs_history = defaultdict(lambda: deque(maxlen=100))   # Momentum history
        self.vwap_num = defaultdict(float) # Running sum Price * Vol
        self.vwap_den = defaultdict(float) # Running sum Vol
        self.pcr_data = defaultdict(dict) # {symbol: {pcr_oi, pcr_vol, max_pain}}
        
        # Buffering for aggregation
        self.tick_buffer = defaultdict(list)
        self.last_ttq = defaultdict(int) # Track Total Traded Qty per symbol
        self.buffer_lock = threading.Lock()
        self.flush_thread = None
        
        # Watchdog / Heartbeat
        self.tick_count = 0
        self.last_heartbeat_time = time.time()
        self.heartbeat_thread = None
        self.last_reset_date = date.today()
        
        # Start Daily Reset Watchdog
        self.watchdog_thread = threading.Thread(target=self._reset_watchdog, daemon=True)
        self.watchdog_thread.start()
        
        # Redis High-Performance Bridge
        self.redis_queue = queue.Queue(maxsize=50000)
        self.redis_worker_thread = threading.Thread(target=self._redis_worker, daemon=True)
        self.redis_worker_thread.start()
        
        self.option_chain_thread = None
        

    def _reset_watchdog(self):
        """Monitors for new trading days and clears stale state."""
        logger.info("Daily Reset Watchdog started.")
        while True:
            try:
                today = date.today()
                if today > self.last_reset_date:
                    logger.info("New day detected in MarketDepthService. Resetting all cumulative metrics.")
                    with self.buffer_lock:
                        self.vwap_num.clear()
                        self.vwap_den.clear()
                        self.price_history.clear()
                        self.vol_history.clear()
                        self.vqs_history.clear()
                        self.last_ttq.clear()
                        self.current_state.clear()
                        self.last_reset_date = today
                time.sleep(60) # Check every minute
            except Exception as e:
                logger.error(f"Watchdog Error: {e}")
                time.sleep(60)

    def _setup_redis(self):
        try:
            # Robust Redis Client with timeouts and retries
            self.redis_client = redis.Redis(
                host=self.REDIS_HOST, 
                port=self.REDIS_PORT, 
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
                retry_on_timeout=True
            )
            self.redis_client.ping()
            logger.info(f"Connected to Redis High-Performance Bridge at {self.REDIS_HOST}:{self.REDIS_PORT}")
        except Exception as e:
            logger.error(f"Redis Connection Failed: {e}")
            self.redis_client = None

    def _redis_worker(self):
        """Background thread to drain the redis_queue and push to Redis using Pipelining."""
        logger.info("🚀 Redis Bridge Worker: PIPELINE MODE STARTED")
        batch_size = 100
        while True:
            try:
                items = []
                # Try to get first item (blocking)
                try:
                    first_item = self.redis_queue.get(timeout=2.0)
                    items.append(first_item)
                except queue.Empty:
                    if not self.running: break
                    continue

                # Hot-drain more items if available (non-blocking)
                while len(items) < batch_size:
                    try:
                        items.append(self.redis_queue.get_nowait())
                    except queue.Empty:
                        break

                if self.redis_client and items:
                    pipe = self.redis_client.pipeline()
                    for item in items:
                        target_symbol = item['symbol']
                        json_packet = item['json']
                        
                        pipe.publish(self.REDIS_CHANNEL, json_packet)
                        pipe.set(f"depth:{target_symbol}", json_packet, ex=60)
                        tick_key = f"ticks:{target_symbol}"
                        pipe.rpush(tick_key, json_packet)
                        pipe.ltrim(tick_key, -3000, -1)
                    
                    pipe.execute()
                    
                    for _ in items:
                        self.redis_queue.task_done()
                
            except Exception as e:
                logger.error(f"Redis Bridge Pipeline Error: {e}")
                time.sleep(1)


    def _setup_truedata(self):
        user = os.getenv("TRUEDATA_USER_ID")
        pwd = os.getenv("TRUEDATA_PASSWORD")
        
        if self.use_mock:
            logger.info("TRUEDATA_MODE is MOCK. Skipping login.")
            return

        if not user or not pwd:
            logger.warning("TrueData Credentials Missing. Switching to MOCK MODE.")
            self.use_mock = True
            return

        self.td_client = TrueDataClient(user, pwd)

    def _setup_dhan_feed(self):
        cid = os.getenv("DHAN_CLIENT_ID")
        token = os.getenv("DHAN_ACCESS_TOKEN")
        if not cid or not token:
            logger.error("Dhan Credentials Missing. Falling back to MOCK.")
            self.use_mock = True
            return
        self.dhan_feed = DhanFeedClient(cid, token, enable_deep=self.enable_dhan_deep_depth)

    def _get_focus_symbols(self) -> list:
        """Fetch today's focus stocks + active Snipers from DB."""
        session = self.SessionLocal()
        try:
            today_date = date.today()
            
            # 1. Fetch All Active Snipers from Tickers table (Dynamic)
            snipers = session.query(Ticker).filter(
                Ticker.oracle_status.in_(["UP_SNIPER", "DOWN_SNIPER"])
            ).all()
            
            # 2. Extract sniper symbols and their benchmark indices
            sniper_syms = []
            for s in snipers:
                sym = s.symbol.replace(".NS", "").replace("NSE:", "").strip()
                sniper_syms.append(sym)
                
                # Check for DB-driven index names (Multi-index support)
                if s.dhan_index_name and isinstance(s.dhan_index_name, list):
                    for idx in s.dhan_index_name:
                        self.target_indices.add(idx)
                elif s.sector and s.sector != "UNKNOWN":
                    # Fallback to sector-based logic if dhan_index_name is missing
                    self.target_indices.add(s.sector)
            
            # 3. Fetch DailyFocus (Today's prioritized stocks)
            stocks = session.query(DailyFocus).filter(
                DailyFocus.date >= datetime.combine(today_date, datetime.min.time()),
                ~DailyFocus.oracle_status.like("REJECTED%")
            ).order_by(DailyFocus.avg_daily_turnover.desc()).all()
            
            db_symbols = [s.symbol for s in stocks]
            
            # 4. Merge Logic: Priority Midcaps > Active Snipers > Daily Focus
            final_list = []
            seen = set()
            
            # A. Start with Priority Midcaps
            for sym in PRIORITY_MIDCAPS:
                if sym not in seen:
                    final_list.append(sym)
                    seen.add(sym)
            
            # B. Add Active Snipers
            for sym in sniper_syms:
                if sym not in seen:
                    final_list.append(sym)
                    seen.add(sym)
            
            # C. Fill remaining from Daily Focus until safe limit (500)
            for raw_sym in db_symbols:
                sym = raw_sym.replace(".NS", "").replace("NSE:", "").strip()
                if len(final_list) >= 500:
                    break
                if sym not in seen:
                    final_list.append(sym)
                    seen.add(sym)
            
            logger.info(f"Loaded {len(final_list)} focus symbols. (Priority: {len(PRIORITY_MIDCAPS)}, Snipers: {len(sniper_syms)}, Total: {len(final_list)})")
            logger.info(f"Target Indices for Sub: {list(self.target_indices)}")
            return final_list
        except Exception as e:
            logger.error(f"Error fetching focus symbols from DB: {e}")
            return ["ACC", "SBIN", "RELIANCE"] # Fallback
        finally:
            session.close()

    def _prime_historical_data(self, symbols: list):
        """Fetch morning 1-minute bars from chosen depth source to fill Redis/MySQL gaps."""
        if self.use_mock:
            return

        logger.info(f"Priming historical data for {len(symbols)} symbols from {self.depth_source}...")
        # IST is UTC+5:30
        ist_tz = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist_tz)
        today_open = now.replace(hour=9, minute=15, second=0, microsecond=0)

        # Naive comparison
        if now.replace(tzinfo=None) <= today_open.replace(tzinfo=None):
            logger.info("Market not open yet. Skipping historical priming.")
            return

        # Prioritize top symbols to avoid REST quota hits
        # Increased to 100 for Dhan as it is faster
        # No longer limited to 100 to ensure full coverage of the watchlist
        prime_list = symbols 
        
        for symbol in prime_list:
            try:
                # Check coverage in Redis
                bar_key = f"bars:{symbol}"
                should_prime = True
                if self.redis_client:
                    first_bar_json = self.redis_client.lindex(bar_key, 0)
                    last_bar_json = self.redis_client.lindex(bar_key, -1)
                    
                    if first_bar_json:
                        first_bar = json.loads(first_bar_json)
                        first_ts = pd.to_datetime(first_bar['timestamp']).replace(tzinfo=None)
                        has_morning = first_ts <= today_open.replace(tzinfo=None) + timedelta(minutes=5)
                        
                        last_ts = None
                        is_fresh = False
                        if last_bar_json:
                            last_bar = json.loads(last_bar_json)
                            last_ts = pd.to_datetime(last_bar['timestamp']).replace(tzinfo=None)
                            # Check if data is fresh (within last 5 mins)
                            if last_ts >= now.replace(tzinfo=None) - timedelta(minutes=5):
                                is_fresh = True
                        
                        if has_morning and is_fresh:
                            logger.debug(f"Redis has full coverage for {symbol} (Morning: {has_morning}, Fresh: {is_fresh}). Skipping.")
                            continue
                        elif has_morning and not is_fresh:
                            logger.info(f"Redis has morning data for {symbol} but looks stale (Last: {last_ts}). Topping up.")
                        else:
                            logger.info(f"Redis missing morning data for {symbol} (First: {first_ts}). Backfilling.")

                logger.info(f"Fetching morning bars for {symbol} from {self.depth_source}...")
                
                df = None
                source_tag = "UNKNOWN_PRIME"
                
                if self.depth_source == "TRUEDATA" and self.td_client:
                    df = self.td_client.fetch_intraday_bars(symbol, today_open, now)
                    source_tag = "TRUEDATA_PRIME"
                elif self.depth_source == "DHAN" and self.dhan_data:
                    # Dhan fetch_realtime_data expects string date or period
                    from_date_str = today_open.strftime("%Y-%m-%d")
                    df = self.dhan_data.fetch_realtime_data(symbol, interval="1m", from_date_str=from_date_str)
                    # Filter for trades today after 9:15
                    if df is not None and not df.empty:
                        df = df[df.index >= today_open.replace(tzinfo=None)]
                    source_tag = "DHAN_PRIME"
                
                if df is not None and not df.empty:
                    # 1. Sync to Redis
                    if self.redis_client:
                        # Get existing timestamps to avoid duplicates
                        existing_bars = self.redis_client.lrange(bar_key, 0, -1)
                        existing_ts = set()
                        latest_ts = None
                        earliest_ts = None
                        
                        for b_json in existing_bars:
                            b = json.loads(b_json)
                            ts_val = pd.to_datetime(b['timestamp']).replace(tzinfo=None)
                            existing_ts.add(ts_val)
                            if earliest_ts is None or ts_val < earliest_ts:
                                earliest_ts = ts_val
                            if latest_ts is None or ts_val > latest_ts:
                                latest_ts = ts_val

                        # Prepend bars that are earlier than our earliest live bar
                        # (Iterate in reverse to LPUSH and maintain order)
                        for ts, row in df.sort_index(ascending=False).iterrows():
                            clean_ts = ts.replace(tzinfo=None)
                            if clean_ts not in existing_ts:
                                bar = {
                                    "symbol": symbol,
                                    "timestamp": ts.isoformat(),
                                    "open": float(row['Open']),
                                    "high": float(row['High']),
                                    "low": float(row['Low']),
                                    "close": float(row['Close']),
                                    "volume": int(row['Volume']),
                                    "source": "DHAN_REST_FEEDER" if self.depth_source == "DHAN" else "TRUEDATA_REST_FEEDER"
                                }
                                if earliest_ts and clean_ts < earliest_ts:
                                    self.redis_client.lpush(bar_key, json.dumps(bar))
                                else:
                                    self.redis_client.rpush(bar_key, json.dumps(bar))
                        self.redis_client.expire(bar_key, 86400) # 24h

                    # 2. Sync to MySQL (Bulk)
                    session = self.SessionLocal()
                    try:
                        bars_to_upsert = []
                        for ts, row in df.iterrows():
                            # Remove tzinfo for MySQL
                            clean_ts = ts.replace(tzinfo=None) if hasattr(ts, 'tzinfo') else ts
                            bars_to_upsert.append({
                                "symbol": symbol,
                                "timestamp": clean_ts,
                                "open": float(row['Open']),
                                "high": float(row['High']),
                                "low": float(row['Low']),
                                "close": float(row['Close']),
                                "volume": int(row['Volume']),
                                "total_turnover": 0.0,
                                "source": source_tag,
                                "buy_volume": 0.0,
                                "sell_volume": 0.0,
                                "avg_bid_qty": 0.0,
                                "avg_ask_qty": 0.0,
                                "mean_imbalance": 0.0,
                                "iceberg_score": 0.0,
                                "iceberg_timestamp": None,
                                "iceberg_side": None,
                                "total_bid_qty": 0.0,
                                "total_ask_qty": 0.0,
                                "bid_pct": 50.0,
                                "ask_pct": 50.0
                            })
                        
                        if bars_to_upsert:
                            # Use simpler loop for priming to avoid complex bulk parameter issues
                            for bar_data in bars_to_upsert:
                                stmt = mysql_insert(IntradayTick).values(bar_data)
                                update_dict = {
                                    c.name: getattr(stmt.inserted, c.name)
                                    for c in IntradayTick.__table__.columns
                                    if not c.primary_key and c.name != 'last_updated'
                                }
                                upsert_stmt = stmt.on_duplicate_key_update(**update_dict)
                                session.execute(upsert_stmt)
                            session.commit()
                    except Exception as e:
                        session.rollback()
                        logger.error(f"MySQL Priming Error for {symbol}: {e}")
                    finally:
                        session.close()

                    # 3. Seed initial state for tick processing
                    last_row = df.iloc[-1]
                    self.current_state[symbol] = {
                        "ltp": float(last_row['Close']),
                        "volume": int(last_row['Volume']), # Seed 'volume' instead of 'ttq' for Dhan
                        "bid_price": 0.0, "bid_qty": 0.0,
                        "ask_price": 0.0, "ask_qty": 0.0,
                        "total_bid_qty": 0.0, "total_ask_qty": 0.0,
                        "v_buy": 0.0, "v_sell": 0.0,
                        "bridge_active": False
                    }
                    
                    # Seed VWAP from historical bars to ensure mid-day restart accuracy
                    # vwap_num = Sum(Close * Volume), vwap_den = Sum(Volume)
                    total_v = df['Volume'].sum()
                    if total_v > 0:
                        self.vwap_num[symbol] = (df['Close'] * df['Volume']).sum()
                        self.vwap_den[symbol] = total_v
                    
                    # Seed volume for delta calculation
                    self.last_ttq[symbol] = int(last_row['Volume'])
                    
                # Small sleep to be nice to API
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error priming {symbol}: {e}")

    def _process_tick(self, data):
        """Callback for TrueData ticks. Merges Trade (LTP) and Bid/Ask (Depth) updates statefully."""
        try:
            # 1. Standardize data (handles dict for mock, objects for live)
            if isinstance(data, dict):
                symbol = data.get('symbol')
                update_type = 'depth' if 'bid_price' in data or 'bid_list' in data else 'trade'
            else:
                symbol = getattr(data, 'symbol', None)
                # TrueData-ws: TradeLiveData has 'ltp', bidask_feed has 'bid'/'ask'
                update_type = 'depth' if hasattr(data, 'bid') or hasattr(data, 'ask') else 'trade'

            if not symbol: return

            # BSE -> NSE Bridge (Mirroring logic)
            # Handle _BSE suffix or Special Cases
            parent_symbol = None
            if str(symbol).endswith("_BSE"):
                # Skip if bridge is disabled
                if not self.enable_bse_bridge:
                    return
                parent_symbol = str(symbol).replace("_BSE", "")
            elif symbol == "SENSEX": # Special case for Market Mood
                # Skip if bridge is disabled
                if not self.enable_bse_bridge:
                    return
                parent_symbol = "BSE"
            
            # Key used for calculations/storage
            target_symbol = parent_symbol if parent_symbol else symbol

            if target_symbol not in self.current_state:
                self.current_state[target_symbol] = {
                    "ltp": 0.0, "volume": 0.0, "bid_price": 0.0, "bid_qty": 0.0, 
                    "ask_price": 0.0, "ask_qty": 0.0, "total_bid_qty": 0.0, "total_ask_qty": 0.0,
                    "v_buy": 0.0, "v_sell": 0.0, "prev_ltp": 0.0,
                    "bridge_active": False,
                    "current_bar_volume": 0.0
                }
            state = self.current_state[target_symbol]

            if parent_symbol:
                state['bridge_active'] = True


            # 2. Extract Data (Unified Mapping)
            # Try to get LTP from any field
            new_ltp = float(getattr(data, 'ltp', 0.0) or getattr(data, 'best_bid_price', 0.0) or 0.0)
            if new_ltp > 0:
                state['ltp'] = new_ltp
                self.tick_count += 1

            # Handle Volume
            if not parent_symbol: # Only treat NSE volume as primary
                current_v = float(getattr(data, 'ttq', 0.0) or getattr(data, 'v', 0.0) or 0.0)
                if current_v > 0:
                    if state.get('volume', 0) > 0 and current_v > state['volume']:
                        vol_delta = current_v - state['volume']
                        
                        prev_ltp = state.get('prev_ltp', state['ltp'])
                        if state['ltp'] > prev_ltp:
                            state['v_buy'] += vol_delta
                        elif state['ltp'] < prev_ltp:
                            state['v_sell'] += vol_delta
                        
                        state['current_bar_volume'] += vol_delta
                        self.vwap_num[target_symbol] += (state['ltp'] * vol_delta)
                        self.vwap_den[target_symbol] += vol_delta
                        self.vol_history[target_symbol].append(vol_delta)
                    
                    state['volume'] = current_v
                
                self.price_history[target_symbol].append(state['ltp'])
                self.tick_count += 1

            # Handle Depth
            # Try to extract best Bid/Ask lists (Level 2)
            bid_list = getattr(data, 'bid', [])
            ask_list = getattr(data, 'ask', [])

            if bid_list and isinstance(bid_list, list) and len(bid_list) > 0:
                state['bid_price'] = float(bid_list[0][0])
                state['bid_qty'] = float(bid_list[0][1])
                state['total_bid_qty'] = sum(float(b[1]) for b in bid_list)
                state['bids'] = [[float(b[0]), float(b[1])] for b in bid_list]
            else:
                # Fallback to Level 1 Attributes
                b_p = float(getattr(data, 'best_bid_price', 0.0) or getattr(data, 'bid', 0.0) or 0.0)
                b_q = float(getattr(data, 'best_bid_qty', 0.0) or getattr(data, 'bidqty', 0.0) or getattr(data, 'bid_qty', 0.0) or 0.0)
                if b_p > 0: state['bid_price'] = b_p
                if b_q > 0: state['bid_qty'] = b_q
                
                t_bid = getattr(data, 'total_bid', None)
                if t_bid is not None:
                    state['total_bid_qty'] = float(t_bid)
                elif b_q > 0:
                    state['total_bid_qty'] = b_q

            if ask_list and isinstance(ask_list, list) and len(ask_list) > 0:
                state['ask_price'] = float(ask_list[0][0])
                state['ask_qty'] = float(ask_list[0][1])
                state['total_ask_qty'] = sum(float(a[1]) for a in ask_list)
                state['asks'] = [[float(a[0]), float(a[1])] for a in ask_list]
            else:
                # Fallback to Level 1 Attributes
                a_p = float(getattr(data, 'best_ask_price', 0.0) or getattr(data, 'ask', 0.0) or 0.0)
                a_q = float(getattr(data, 'best_ask_qty', 0.0) or getattr(data, 'askqty', 0.0) or getattr(data, 'ask_qty', 0.0) or 0.0)
                if a_p > 0: state['ask_price'] = a_p
                if a_q > 0: state['ask_qty'] = a_q
                
                t_ask = getattr(data, 'total_ask', None)
                if t_ask is not None:
                    state['total_ask_qty'] = float(t_ask)
                elif a_q > 0:
                    state['total_ask_qty'] = a_q

            # 4. Final Processing & Publishing (Common Method)
            self._publish_depth_packet(target_symbol, state)
            state['prev_ltp'] = state['ltp']

        except Exception as e:
            logger.error(f"CRITICAL Error processing tick: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _flush_to_storage(self):
        """Periodically flushes buffered ticks to MySQL (bars) and Parquet (raw)."""
        logger.info("Storage Flush Thread Started. Waiting for first minute boundary...")
        while self.running:
            try:
                # Align with next minute boundary (e.g. 10:05:00.0) to ensure data is written "on time"
                ist_tz = pytz.timezone('Asia/Kolkata')
                now = datetime.now(ist_tz)
                next_min = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
                sleep_sec = (next_min - now).total_seconds()
                if sleep_sec > 0:
                    time.sleep(sleep_sec)
                
                flush_start = time.time()
                
                with self.buffer_lock:
                    if not self.tick_buffer:
                        continue
                    current_buffer = dict(self.tick_buffer)
                    self.tick_buffer = defaultdict(list)

                try:
                    bars_to_upsert = []
                    
                    # 1. Start Parquet Archive in Background (NON-BLOCKING)
                    threading.Thread(target=self._async_parquet_archive, args=(current_buffer,), daemon=True).start()
                    
                    # 2. Aggregation Loop (Optimized to handle multiple minutes in buffer)
                    for symbol, ticks in current_buffer.items():
                        if not ticks: continue
                        
                        df = pd.DataFrame(ticks)
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        # Floor to 1min to handle multiple minutes if the cycle carries over
                        df['minute_bin'] = df['timestamp'].dt.floor('1min')
                        
                        for minute_val, min_df in df.groupby('minute_bin'):
                            df_ohlc = min_df[min_df['ltp'] > 0]
                            if df_ohlc.empty: continue
                            
                            bar_time_ist = minute_val.to_pydatetime()
                            
                            total_buy_vol = min_df['buy_vol'].sum()
                            total_sell_vol = min_df['sell_vol'].sum()
                            total_vol = total_buy_vol + total_sell_vol
                            avg_depth = (min_df['bid_qty'].mean() + min_df['ask_qty'].mean()) / 2
                            
                            iceberg_score = 0.0
                            iceberg_side = None
                            iceberg_timestamp = None
                            
                            ohlc = {
                                "symbol": symbol,
                                "timestamp": bar_time_ist,
                                "open": float(df_ohlc['ltp'].iloc[0]),
                                "high": float(df_ohlc['ltp'].max()),
                                "low": float(df_ohlc['ltp'].min()),
                                "close": float(df_ohlc['ltp'].iloc[-1]),
                                "volume": int(total_vol) if total_vol > 0 else int(df_ohlc['ltp'].count()),
                                "buy_volume": float(total_buy_vol),
                                "sell_volume": float(total_sell_vol),
                                "avg_bid_qty": float(min_df['bid_qty'].mean()),
                                "avg_ask_qty": float(min_df['ask_qty'].mean()),
                                "total_bid_qty": float(min_df['total_bid_qty'].mean()) if 'total_bid_qty' in min_df.columns else 0.0,
                                "total_ask_qty": float(min_df['total_ask_qty'].mean()) if 'total_ask_qty' in min_df.columns else 0.0,
                                "bid_pct": float(min_df['bid_pct'].mean()) if 'bid_pct' in min_df.columns else 50.0,
                                "ask_pct": float(min_df['ask_pct'].mean()) if 'ask_pct' in min_df.columns else 50.0,
                                "mean_imbalance": float(min_df['imbalance'].mean()),
                                "iceberg_score": iceberg_score,
                                "iceberg_timestamp": iceberg_timestamp,
                                "iceberg_side": iceberg_side,
                                "total_turnover": float(df_ohlc['ltp'].sum()), 
                                "oi": float(min_df['oi'].mean()) if 'oi' in min_df.columns else 0.0,
                                "pcr_oi": float(min_df['pcr_oi'].mean()) if 'pcr_oi' in min_df.columns else 0.0,
                                "pcr_vol": float(min_df['pcr_vol'].mean()) if 'pcr_vol' in min_df.columns else 0.0,
                                "max_pain": float(min_df['max_pain'].mean()) if 'max_pain' in min_df.columns else 0.0,
                                "source": self.depth_source
                            }
                            bars_to_upsert.append(ohlc)
                    
                    # 3. Sync to Redis (PRIORITY)
                    if self.redis_client and bars_to_upsert:
                        try:
                            for bar in bars_to_upsert:
                                bar_key = f"bars:{bar['symbol']}"
                                bar_copy = bar.copy()
                                if isinstance(bar_copy['timestamp'], datetime):
                                    bar_copy['timestamp'] = bar_copy['timestamp'].isoformat()
                                if isinstance(bar_copy['iceberg_timestamp'], datetime):
                                    bar_copy['iceberg_timestamp'] = bar_copy['iceberg_timestamp'].isoformat()
                                self.redis_client.rpush(bar_key, json.dumps(bar_copy))
                                self.redis_client.expire(bar_key, 86400)
                            logger.info(f"Synced {len(bars_to_upsert)} bars to Redis.")
                        except Exception as redis_err:
                            logger.error(f"Redis Sync Failed: {redis_err}")

                    # 4. Sync to MySQL (ASYNCHRONOUS)
                    if bars_to_upsert:
                        threading.Thread(target=self._async_mysql_upsert, args=(bars_to_upsert,), daemon=True).start()
                    
                    duration = time.time() - flush_start
                    logger.success(f"Storage flush cycle completed in {duration:.2f}s")
                    
                    # Reset Volume Accumulator for the next minute
                    for s in self.current_state:
                        self.current_state[s]['current_bar_volume'] = 0.0
                except Exception as e:
                    logger.error(f"Error during storage flush aggregation: {e}")

            except Exception as e:
                logger.error(f"Storage Flush Loop Main Error: {e}")

    def _async_mysql_upsert(self, bars):
        """Helper to handle MySQL upserts in a background thread."""
        session = None
        try:
            session = self.SessionLocal()
            start_time = time.time()
            stmt = mysql_insert(IntradayTick).values(bars)
            update_dict = {
                c.name: c for c in stmt.inserted 
                if not c.primary_key and c.name != 'last_updated'
            }
            upsert_stmt = stmt.on_duplicate_key_update(**update_dict)
            session.execute(upsert_stmt)
            session.commit()
            logger.success(f"Async MySQL Upsert: Saved {len(bars)} bars in {time.time() - start_time:.2f}s")
        except Exception as e:
            if session: session.rollback()
            logger.error(f"Async MySQL Upsert Failed: {e}")
        finally:
            if session: session.close()

    def _async_parquet_archive(self, buffer):
        """Archives raw ticks to Parquet in the background."""
        try:
            start_time = time.time()
            count = 0
            for symbol, ticks in buffer.items():
                self.storage_manager.save_ticks_parquet(symbol, ticks)
                count += len(ticks)
            logger.success(f"Async Parquet Archive: Saved {count} ticks for {len(buffer)} symbols in {time.time() - start_time:.2f}s")
        except Exception as e:
            logger.error(f"Async Parquet Archive Failed: {e}")

    def _run_mock_loop(self):
        """Generates fake data when credentials are missing."""
        logger.info("Starting Mock Data Loop...")
        symbols = self.symbols or self._get_focus_symbols()
        while self.running:
            for symbol in symbols:
                mock_tick = {
                    "symbol": symbol,
                    "ltp": round(random.uniform(100, 3000), 2),
                    "bid_qty": random.randint(1000, 5000),
                    "ask_qty": random.randint(1000, 5000)
                }
                self._process_tick(mock_tick)
            
            time.sleep(1)

    def run(self):
        self.running = True
        
        if self.use_mock:
            logger.info("MarketDepthService Started in MOCK mode.")
            # Start flush thread even for mock data
            self.flush_thread = threading.Thread(target=self._flush_to_storage, daemon=True)
            self.flush_thread.start()
            self._run_mock_loop()
        else:
            self.symbols = self.symbols or self._get_focus_symbols()
            
            # Start Watchdog
            self.heartbeat_thread = threading.Thread(target=self._run_heartbeat, daemon=True)
            self.heartbeat_thread.start()
            
            # REST historical priming (Move to background thread to allow immediate live data)
            prime_thread = threading.Thread(target=self._prime_historical_data, args=(self.symbols,), daemon=True)
            prime_thread.start()
            
            # Start Option Chain Analysis Loop
            self.option_chain_thread = threading.Thread(target=self._run_option_chain_loop, daemon=True)
            self.option_chain_thread.start()
            
            if self.depth_source == "TRUEDATA":
                self._run_truedata()
            elif self.depth_source == "DHAN":
                self._run_dhan()

    def _run_truedata(self):
            # Automatically add BSE counterparts for subscription to enable the Bridge
            MAX_TOTAL_SLOTS = 200
            full_subscription_list = []
            mirrored_count = 0
            
            # 1. Try to add pairs for all focus symbols up to the limit
            for s in self.symbols:
                if len(full_subscription_list) >= MAX_TOTAL_SLOTS:
                    break
                
                # Add NSE
                full_subscription_list.append(s)
                
                # Add BSE Mirror (_BSE suffix) if enabled
                if self.enable_bse_bridge and len(full_subscription_list) < MAX_TOTAL_SLOTS:
                    full_subscription_list.append(f"{s}_BSE")
                    mirrored_count += 1
            
            # Special case for SENSEX mood
            if self.enable_bse_bridge and "SENSEX" not in full_subscription_list and len(full_subscription_list) < MAX_TOTAL_SLOTS:
                full_subscription_list.append("SENSEX")

            logger.info(f"MarketDepthService Started. Monitoring {len(self.symbols)} NSE symbols. Bridge Enabled: {self.enable_bse_bridge}. Pairs created for {mirrored_count} stocks. Total Subs: {len(full_subscription_list)}")
            logger.debug(f"FULL SUBSCRIPTION LIST: {full_subscription_list}")
            
            # Start flush thread
            self.flush_thread = threading.Thread(target=self._flush_to_storage, daemon=True)
            self.flush_thread.start()

            # Start Actual WebSocket
            self.td_client.start_websocket(full_subscription_list, on_tick=self._process_tick)
            
            # Keep main thread alive
            while self.running:
                try:
                    time.sleep(1)
                except KeyboardInterrupt:
                    self.stop()
                    break

    def _run_dhan(self):
        """Initializes and runs the Dhan 20-level depth feed."""
        logger.info(f"MarketDepthService starting with DHAN. Total symbols: {len(self.symbols)}")
        
        # Prioritize critical symbols for Deep Feed (First 50 get depth)
        priority_syms = ["SBIN", "RELIANCE", "BALUFORGE", "TEJASNET"]
        sorted_symbols = sorted(self.symbols, key=lambda x: (x not in priority_syms, x))
        
        # 1. Prepare Security ID mapping for standard symbols
        id_map = {}
        stock_ids = []
        # Mapping from Future Security ID -> Cash Symbol (for OI)
        self.futures_to_cash = {}
        
        for sym in sorted_symbols:
            # Add Cash Stock
            sec_id = self.dhan_data.get_security_id(sym)
            if sec_id:
                id_map[sec_id] = sym
                stock_ids.append(sec_id)
                
            # Add Future for OI (if available)
            fut_id = self.dhan_data.get_active_futures_id(sym)
            if fut_id:
                self.futures_to_cash[fut_id] = sym
                if fut_id not in stock_ids:
                    stock_ids.append(fut_id)
        
        # 2. Add Selective Indices to Standard Feed (Core + Relevant Benchmarks)
        index_map = self.dhan_data.get_selective_indices(index_names=list(self.target_indices))
        all_index_ids = list(index_map.values())
        
        # Prepare Comprehensive Meta Mapping
        comprehensive_meta = {}
        # Stocks and Futures
        for sym in sorted_symbols:
            # Cash Meta
            meta = self.dhan_data._meta_map.get(sym)
            if meta:
                comprehensive_meta[meta['id']] = {"symbol": sym, "segment": meta['segment']}
            
            # Future Meta
            fut_id = self.dhan_data.get_active_futures_id(sym)
            if fut_id:
                # Find the future meta in _fno_map (flattened or direct)
                fno_list = self.dhan_data._fno_map.get(sym.replace(".NS", ""))
                if fno_list:
                    fut_meta = next((f for f in fno_list if f['id'] == fut_id), None)
                    if fut_meta:
                        # Register the Future ID to the Cash Symbol with a special routing flag?
                        # Or let the Feed Client handle the multi-mapping.
                        comprehensive_meta[fut_id] = {
                            "symbol": sym, # Route to Cash Symbol
                            "is_derivative": True,
                            "segment": fut_meta['segment']
                        }
        
        # Indices
        for sym, sid in index_map.items():
            meta = self.dhan_data._meta_map.get(sym)
            if meta:
                 comprehensive_meta[sid] = {"symbol": sym, "segment": meta['segment']}
            else:
                 # Fallback for core indices
                 comprehensive_meta[sid] = {"symbol": sym, "segment": "IDX_I"}

        self.dhan_feed.set_id_meta_mapping(comprehensive_meta)
        
        # 3. Split IDs for Standard vs Deep
        # Standard: All Stocks + Identified Indices
        # Deep: Top 50 Stocks (Priortize by order of arrival: Priority Midcaps > Snipers)
        std_ids = stock_ids + all_index_ids
        deep_ids = stock_ids[:50]
        
        logger.info(f"Dhan Feed Setup: Standard={len(std_ids)} (Stocks={len(stock_ids)}, Indices={len(all_index_ids)}), Deep={len(deep_ids)}")
        logger.info(f"Indices Subscribed: {list(index_map.keys())}")
        
        # Start flush thread
        self.flush_thread = threading.Thread(target=self._flush_to_storage, daemon=True)
        self.flush_thread.start()
        
        # 3. Start feed in an event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            loop.run_until_complete(self.dhan_feed.connect(std_ids, deep_ids, on_tick=self._process_dhan_tick))
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            logger.error(f"Dhan Feed Error: {e}")

    def _process_dhan_tick(self, dhan_tick: dict):
        """Processes a normalized tick from DhanFeedClient."""
        try:
            symbol = dhan_tick.get('symbol')
            if not symbol:
                return

            self.tick_count += 1
            
            # 1. Handle Derivative Ticks (OI Updates)
            if dhan_tick.get('is_derivative'):
                if symbol not in self.current_state:
                    self.current_state[symbol] = {"oi": 0.0, "ltp": 0.0}
                
                # For Dhan Futures, OI is now extracted correctly from the binary feed parser
                oi = float(dhan_tick.get('oi', 0.0))
                if oi > 0:
                    self.current_state[symbol]['oi'] = oi
                    # Optional: Publish an OI-only update or wait for next LTP
                    # For now, let's publish so UI/Signals see it immediately
                    self._publish_depth_packet(symbol, self.current_state[symbol])
                return

            # 2. Handle standard (Cash/Index) Ticks
            if symbol not in self.current_state:
                self.current_state[symbol] = {}
            state = self.current_state[symbol]
            
            defaults = {
                "ltp": 0.0, "volume": 0, "bid_price": 0.0, "bid_qty": 0.0, 
                "ask_price": 0.0, "ask_qty": 0.0, "total_bid_qty": 0.0, "total_ask_qty": 0.0,
                "v_buy": 0.0, "v_sell": 0.0, "prev_ltp": 0.0,
                "bridge_active": False, "bids": [], "asks": [],
                "current_bar_volume": 0.0
            }
            for k, v in defaults.items():
                if k not in state:
                    state[k] = v
            
            # Extract fields safely
            bids = dhan_tick.get('bid', [])
            asks = dhan_tick.get('ask', [])
            ltp = float(dhan_tick.get('ltp', 0.0))
            volume = int(dhan_tick.get('volume', 0))
            
            # 1. Update LTP and Volume Safely
            if ltp > 0:
                state['ltp'] = ltp
            if volume > 0:
                if state.get('volume', 0) > 0 and volume > state['volume']:
                    diff = volume - state['volume']
                    prev_ltp = state.get('prev_ltp', state['ltp'])
                    
                    if state['ltp'] > prev_ltp:
                        state['v_buy'] += diff
                    elif state['ltp'] < prev_ltp:
                        state['v_sell'] += diff
                    
                    state['current_bar_volume'] = state.get('current_bar_volume', 0.0) + diff
                    self.vwap_num[symbol] += (state['ltp'] * diff)
                    self.vwap_den[symbol] += diff
                state['volume'] = volume

            # 2. Update Depth Safely (L1 Depth)
            if bids:
                state['bid_qty'] = float(bids[0].get('qty', 0))
                state['bid_price'] = float(bids[0].get('price', 0))
                state['bids'] = bids
            if asks:
                state['ask_qty'] = float(asks[0].get('qty', 0))
                state['ask_price'] = float(asks[0].get('price', 0))
                state['asks'] = asks

            # 3. Update Total Quantities Safely (L3 Depth Indicators)
            total_bid_qty = float(dhan_tick.get('total_buy_qty', 0.0))
            total_ask_qty = float(dhan_tick.get('total_sell_qty', 0.0))
            
            if total_bid_qty > 0:
                state['total_bid_qty'] = total_bid_qty
            elif bids:
                # Fallback to sum of current level if total not present but depth is
                state['total_bid_qty'] = sum(b.get('qty', 0) for b in bids)
                
            if total_ask_qty > 0:
                state['total_ask_qty'] = total_ask_qty
            elif asks:
                state['total_ask_qty'] = sum(a.get('qty', 0) for a in asks)
            
            # 4. Final Processing & Publishing (Common Method)
            self._publish_depth_packet(symbol, state)
            state['prev_ltp'] = state['ltp']
            
        except Exception as e:
            logger.error(f"Error processing Dhan tick for {dhan_tick.get('symbol', 'UNKNOWN')}: {e}")

    def _publish_depth_packet(self, target_symbol: str, state: dict):
        """Shared logic to calculate metrics and publish to Redis."""
        try:
            ltp = state.get('ltp', 0.0)
            if ltp <= 0:
                return 

            bid_qty = state.get('bid_qty', 0.0)
            ask_qty = state.get('ask_qty', 0.0)
            total_qty = bid_qty + ask_qty
            imbalance = (bid_qty - ask_qty) / total_qty if total_qty > 0 else 0.0
            
            # Update price history for momentum
            self.price_history[target_symbol].append(ltp)
            
            # Momentum (VQS) Calculation - Directional Count over price history
            history = self.price_history[target_symbol]
            if len(history) > 1:
                ticks = []
                for i in range(1, len(history)):
                    if history[i] > history[i-1]: ticks.append(1)
                    elif history[i] < history[i-1]: ticks.append(-1)
                vqs_score = sum(ticks) / len(ticks) if ticks else 0.0
            else:
                vqs_score = 0.0
            
            vwap_num = self.vwap_num[target_symbol]
            vwap_den = self.vwap_den[target_symbol]
            vwap = vwap_num / vwap_den if vwap_den > 0 else ltp

            # Volume Surge Calculation (Relative to last 100 ticks)
            vol_history = self.vol_history[target_symbol]
            avg_vol = sum(vol_history) / len(vol_history) if vol_history else 0.0
            last_vol = vol_history[-1] if vol_history else 0.0
            vol_surge = round(last_vol / avg_vol, 2) if avg_vol > 0 else 1.0

            total_bid_qty = state.get('total_bid_qty', 0.0)
            total_ask_qty = state.get('total_ask_qty', 0.0)
            combined_total = total_bid_qty + total_ask_qty
            
            packet = {
                "symbol": target_symbol,
                "ltp": ltp,
                "imbalance": round(imbalance, 4),
                "vwap": round(vwap, 2),
                "vqs_score": round(vqs_score, 4),
                "vol_surge": vol_surge,
                "volume": state.get('volume', 0.0),
                "current_bar_volume": state.get('current_bar_volume', 0.0),
                "bid_qty": bid_qty,
                "ask_qty": ask_qty,
                "total_bid_qty": total_bid_qty,
                "total_ask_qty": total_ask_qty,
                "bid_pct": round((total_bid_qty / combined_total * 100), 2) if combined_total > 0 else 50.0,
                "ask_pct": round((total_ask_qty / combined_total * 100), 2) if combined_total > 0 else 50.0,
                "oi": state.get('oi', 0.0), # Include real-time OI
                "buy_vol": state.get('v_buy', 0.0),
                "sell_vol": state.get('v_sell', 0.0),
                "pcr_oi": self.pcr_data[target_symbol].get('pcr_oi', 0.0),
                "pcr_vol": self.pcr_data[target_symbol].get('pcr_vol', 0.0),
                "max_pain": self.pcr_data[target_symbol].get('max_pain', 0.0),
                "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).isoformat(),
                "source": self.depth_source,
                "exchange_bridge": "Dhan20" if self.depth_source == "DHAN" else ("BSE+NSE" if state.get('bridge_active') else "NSE"),
                "bids": state.get('bids', [])[:5],
                "asks": state.get('asks', [])[:5]
            }
            
            # Add Top 20 for priority focus symbols
            if len(state.get('bids', [])) > 5:
                packet["bids_20"] = state.get('bids', [])
                packet["asks_20"] = state.get('asks', [])
            
            state['v_buy'] = 0
            state['v_sell'] = 0

            # 5. Toss to High-Performance Bridge (Non-blocking)
            try:
                self.redis_queue.put_nowait({
                    "symbol": target_symbol,
                    "json": json.dumps(packet)
                })
            except queue.Full:
                # Throttle logging to once every 5 seconds per symbol
                if not hasattr(self, '_last_drop_log') or time.time() - self._last_drop_log > 5:
                    logger.warning(f"⚠️ Redis Bridge Queue FULL! Dropping packets (Queue size: {self.redis_queue.qsize()})")
                    self._last_drop_log = time.time()
            
            with self.buffer_lock:
                self.tick_buffer[target_symbol].append(packet)
        except Exception as e:
            logger.error(f"Error publishing depth packet for {target_symbol}: {e}")

    def _run_heartbeat(self):
        """Watchdog thread to log system health every 60 seconds."""
        logger.info("Watchdog Heartbeat Thread Started.")
        while self.running:
            try:
                time.sleep(60)
                now = time.time()
                elapsed = now - self.last_heartbeat_time
                tps = self.tick_count / elapsed if elapsed > 0 else 0
                
                worker_alive = self.redis_worker_thread.is_alive() if self.redis_worker_thread else False
                q_size = self.redis_queue.qsize() if self.redis_queue else 0
                logger.info(f"❤️ HEARTBEAT: Processed {self.tick_count} ticks in last {elapsed:.1f}s ({tps:.2f} tps) | Redis Worker: {'ALIVE' if worker_alive else 'DEAD'} | Queue: {q_size}")
                
                # Reset counters for next minute
                self.tick_count = 0
                self.last_heartbeat_time = now
                
            except Exception as e:
                logger.error(f"Watchdog Error: {e}")

    def _run_option_chain_loop(self):
        """Background thread to fetch and analyze Option Chain every 5 minutes."""
        logger.info("Option Chain Analysis Thread Started.")
        # FNO Expiries (Current month approx, would be better to fetch dynamically)
        # For now, let's try to get them from the master list or assume next Thursday
        
        while self.running:
            try:
                # 1. Fetch Option Chain for each focus symbol
                # Only iterate through symbols that we have Security IDs for
                for symbol in self.symbols:
                    if not self.running: break
                    
                    sid = self.dhan_data.get_security_id(symbol)
                    if not sid: continue
                    
                    # Get segments (NSE_EQ for indices/stocks)
                    # Dhan Option Chain needs UnderlyingSeg
                    seg = "IDX_I" if symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"] else "NSE_EQ"
                    
                    # TODO: Fetch dynamic expiry list. 
                    # For now, we search for the nearest expiry in the FNO map.
                    fno_list = self.dhan_data._fno_map.get(symbol.replace(".NS", ""))
                    if not fno_list: continue
                    
                    # Sort by expiry and pick the nearest one
                    option_expiries = sorted(list(set(f['expiry'] for f in fno_list if f.get('instrument_type') in ['OPTIDX', 'OPTSTK'])))
                    if not option_expiries: continue
                    
                    nearest_expiry = option_expiries[0]
                    
                    chain = self.dhan_data.get_option_chain(sid, seg, nearest_expiry)
                    if chain:
                        self._calculate_pcr_and_pain(symbol, chain)
                        logger.info(f"📊 OPTION CHAIN [{symbol}]: PCR={self.pcr_data[symbol]['pcr_oi']:.2f}, MaxPain={self.pcr_data[symbol]['max_pain']}")
                
                # Sleep for 5 minutes between full updates to respect rate limits (3s/request)
                time.sleep(300) 
                
            except Exception as e:
                logger.error(f"Option Chain Loop Error: {e}")
                time.sleep(60)

    def _calculate_pcr_and_pain(self, symbol: str, chain: list):
        """
        Calculates PCR (OI/Volume) and Max Pain from Option Chain.
        """
        total_call_oi = 0
        total_put_oi = 0
        total_call_vol = 0
        total_put_vol = 0
        
        strikes = []
        
        for strike_data in chain:
            strike_price = float(strike_data['strike_price'])
            strikes.append(strike_price)
            
            # Call side
            c_oi = float(strike_data.get('call_oi', 0))
            c_vol = float(strike_data.get('call_volume', 0))
            total_call_oi += c_oi
            total_call_vol += c_vol
            
            # Put side
            p_oi = float(strike_data.get('put_oi', 0))
            p_vol = float(strike_data.get('put_volume', 0))
            total_put_oi += p_oi
            total_put_vol += p_vol
            
        # 1. PCR
        pcr_oi = total_put_oi / total_call_oi if total_call_oi > 0 else 0.0
        pcr_vol = total_put_vol / total_call_vol if total_call_vol > 0 else 0.0
        
        # 2. Max Pain Calculation
        # Strike where total loss to option buyers is minimum
        min_pain = float('inf')
        max_pain_strike = 0
        
        # We only check strikes that are in the chain
        unique_strikes = sorted(list(set(strikes)))
        
        for test_strike in unique_strikes:
            total_loss = 0
            for strike_data in chain:
                strike = float(strike_data['strike_price'])
                
                # Loss for Call Buyers if market ends at test_strike
                c_oi = float(strike_data.get('call_oi', 0))
                if test_strike > strike:
                    total_loss += (test_strike - strike) * c_oi
                
                # Loss for Put Buyers if market ends at test_strike
                p_oi = float(strike_data.get('put_oi', 0))
                if test_strike < strike:
                    total_loss += (strike - test_strike) * p_oi
            
            if total_loss < min_pain:
                min_pain = total_loss
                max_pain_strike = test_strike
        
        self.pcr_data[symbol] = {
            "pcr_oi": round(pcr_oi, 4),
            "pcr_vol": round(pcr_vol, 4),
            "max_pain": max_pain_strike,
            "updated_at": datetime.now().isoformat()
        }

        # Save to Redis for UI/Persistance
        if self.redis_client:
            sentiment_key = f"sentiment:{symbol}"
            # Extract Top 5 Call and Put strikes by OI for "Walls" detection
            all_calls = sorted([s for s in chain if s.get('call_oi', 0) > 0], key=lambda x: x.get('call_oi', 0), reverse=True)[:5]
            all_puts = sorted([s for s in chain if s.get('put_oi', 0) > 0], key=lambda x: x.get('put_oi', 0), reverse=True)[:5]
            
            summary = {
                "pcr_oi": round(pcr_oi, 4),
                "pcr_vol": round(pcr_vol, 4),
                "max_pain": max_pain_strike,
                "top_call_walls": [{"strike": c['strike_price'], "oi": c['call_oi']} for c in all_calls],
                "top_put_walls": [{"strike": p['strike_price'], "oi": p['put_oi']} for p in all_puts],
                "updated_at": datetime.now().isoformat()
            }
            self.redis_client.set(sentiment_key, json.dumps(summary), ex=3600) # Keep for 1 hour

    def stop(self):
        self.running = False
        logger.info("Stopping MarketDepthService...")

if __name__ == "__main__":
    service = MarketDepthService()
    try:
        service.run()
    except KeyboardInterrupt:
        service.stop()

