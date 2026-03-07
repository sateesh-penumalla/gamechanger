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
from src.data.dhan_feed import DhanFeedClient
from src.db.schema import DailyFocus, IntradayTick, Ticker
from src.utils.storage_manager import StorageManager
from collections import defaultdict, deque
import asyncio

# Priority Midcap Symbols as requested by user
PRIORITY_MIDCAPS = [
    "ALPEXSOLAR", "APEX", "AVANTIFEED", "BELRISE", "BLISSGVS", 
    "BLUESTARCO", "BSE", "COCHINSHIP", "COFORGE", "DBREALTY", 
    "FORTIS", "HEXT", "IDEA", "KALYANKJIL", "LEMONTREE", 
    "OLAELEC", "ONMOBILE", "ORIANA", "PFOCUS", "PGEL", 
    "PREMIERENE", "RAIN", "RICOAUTO", "RPOWER", "TARIL", 
    "TECHLABS"
]

load_dotenv()

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
        self.running = False
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
        
        # Buffering for aggregation
        self.tick_buffer = defaultdict(list)
        self.last_ttq = defaultdict(int) # Track Total Traded Qty per symbol
        self.buffer_lock = threading.Lock()
        self.flush_thread = None
        

    def _setup_redis(self):
        try:
            self.redis_client = redis.Redis(host=self.REDIS_HOST, port=self.REDIS_PORT, decode_responses=True)
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {self.REDIS_HOST}:{self.REDIS_PORT}")
        except Exception as e:
            logger.error(f"Redis Connection Failed: {e}")
            self.redis_client = None


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
                                    "source": source_tag
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
                        "ttq": float(last_row['Volume']),
                        "bid_price": 0.0, "bid_qty": 0.0,
                        "ask_price": 0.0, "ask_qty": 0.0,
                        "total_bid_qty": 0.0, "total_ask_qty": 0.0,
                        "v_buy": 0.0, "v_sell": 0.0,
                        "bridge_active": False
                    }
                    self.last_ttq[symbol] = float(last_row['Volume'])
                    
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
                    "ltp": 0.0, "ttq": 0.0, "bid_price": 0.0, "bid_qty": 0.0, 
                    "ask_price": 0.0, "ask_qty": 0.0, "total_bid_qty": 0.0, "total_ask_qty": 0.0,
                    "v_buy": 0.0, "v_sell": 0.0,
                    "bridge_active": False
                }
            state = self.current_state[target_symbol]

            if parent_symbol:
                state['bridge_active'] = True


            # 2. Extract Data (Unified Mapping)
            # Try to get LTP from any field
            new_ltp = float(getattr(data, 'ltp', 0.0) or getattr(data, 'best_bid_price', 0.0) or 0.0)
            if new_ltp > 0:
                state['ltp'] = new_ltp

            # Handle Volume
            if not parent_symbol: # Only treat NSE volume as primary
                current_ttq = float(getattr(data, 'ttq', 0.0) or getattr(data, 'v', 0.0) or 0.0)
                if current_ttq > 0 and symbol in self.last_ttq:
                    vol_delta = current_ttq - self.last_ttq[symbol]
                    if vol_delta > 0:
                        if state['ltp'] >= state['ask_price'] > 0: state['v_buy'] += vol_delta
                        elif state['ltp'] <= state['bid_price'] > 0: state['v_sell'] += vol_delta
                
                if current_ttq > 0:
                    self.last_ttq[symbol] = current_ttq
                
                # Update price history for VQS
                if state['ltp'] > 0:
                    self.price_history[symbol].append(state['ltp'])
                    # VWAP accumulation
                    if current_ttq > 0:
                        self.vwap_num[symbol] += (state['ltp'] * vol_delta) if 'vol_delta' in locals() else 0
                        self.vwap_den[symbol] += vol_delta if 'vol_delta' in locals() else 0

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

            # 3. IF LTP is still 0, try to pull from td_client's live_data cache
            if state['ltp'] == 0.0 and self.td_client and self.td_client.td_app:
                try:
                    # TD app has symbol_id_map and live_data
                    for req_id, live_obj in self.td_client.td_app.live_data.items():
                        if live_obj.symbol == symbol and live_obj.ltp and live_obj.ltp > 0:
                            state['ltp'] = float(live_obj.ltp)
                            break
                except Exception:
                    pass

            # 4. IF LTP is STILL 0, bail out. Don't publish or buffer zero-price ticks.
            if state['ltp'] <= 0:
                return

            # 5. Calculate derived metrics from MERGED state
            ltp = state['ltp']
            bid_qty = state['bid_qty']
            ask_qty = state['ask_qty']
            imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty) if (bid_qty + ask_qty) > 0 else 0.0
            
            # VQS Momentum
            # VQS should only be calculated from the primary feed (target_symbol)
            prev_ltp = self.price_history[target_symbol][-2] if len(self.price_history[target_symbol]) > 1 else ltp
            tick_dir = 1 if ltp > prev_ltp else (-1 if ltp < prev_ltp else 0)
            self.vqs_history[target_symbol].append(tick_dir)
            vqs_score = sum(self.vqs_history[target_symbol]) / len(self.vqs_history[target_symbol]) if self.vqs_history[target_symbol] else 0.0
            
            vwap = self.vwap_num[target_symbol] / self.vwap_den[target_symbol] if self.vwap_den[target_symbol] > 0 else ltp

            packet = {
                "symbol": target_symbol, # Always report as the parent NSE symbol
                "ltp": ltp,
                "imbalance": round(imbalance, 4),
                "vwap": round(vwap, 2),
                "vqs_score": round(vqs_score, 4),
                "vol_surge": 1.0,
                "bid_qty": bid_qty,
                "ask_qty": ask_qty,
                "total_bid_qty": state['total_bid_qty'],
                "total_ask_qty": state['total_ask_qty'],
                "bid_pct": round((state['total_bid_qty'] / (state['total_bid_qty'] + state['total_ask_qty']) * 100), 2) if (state['total_bid_qty'] + state['total_ask_qty']) > 0 else 50.0,
                "ask_pct": round((state['total_ask_qty'] / (state['total_bid_qty'] + state['total_ask_qty']) * 100), 2) if (state['total_bid_qty'] + state['total_ask_qty']) > 0 else 50.0,
                "buy_vol": state['v_buy'],
                "sell_vol": state['v_sell'],
                "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).isoformat(),
                "source": "TrueData" if not self.use_mock else "MockData",
                "exchange_bridge": "BSE+NSE" if state.get('bridge_active') else "NSE"
            }
            
            state['v_buy'] = 0
            state['v_sell'] = 0

            # 6. Publish to Redis & Buffer for storage
            if self.redis_client:
                self.redis_client.publish(self.REDIS_CHANNEL, json.dumps(packet))
                self.redis_client.set(f"depth:{target_symbol}", json.dumps(packet), ex=60)
                tick_key = f"ticks:{target_symbol}"
                self.redis_client.rpush(tick_key, json.dumps(packet))
                self.redis_client.ltrim(tick_key, -3000, -1)
            
            with self.buffer_lock:
                self.tick_buffer[target_symbol].append(packet)

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

                session = self.SessionLocal()
                try:
                    bars_to_upsert = []
                    for symbol, ticks in current_buffer.items():
                        if not ticks: continue
                        
                        # 1. Archive raw ticks to Parquet
                        self.storage_manager.save_ticks_parquet(symbol, ticks)
                        
                        # 2. Aggregate to 1-minute bar for MySQL
                        df = pd.DataFrame(ticks)
                        df['timestamp'] = pd.to_datetime(df['timestamp'])

                        # Filter out invalid ticks (ltp=0)
                        df_ohlc = df[df['ltp'] > 0]
                        if df_ohlc.empty:
                            logger.warning(f"Skipping aggregation for {symbol} - No valid price ticks (ltp > 0)")
                            continue
                        
                        # Aggregate by minute (Naive IST is preferred for MySQL parity)
                        bar_time_ist = df_ohlc['timestamp'].iloc[0].replace(second=0, microsecond=0)
                        
                        # Calculate Iceberg/OrderFlow metrics
                        total_buy_vol = df['buy_vol'].sum()
                        total_sell_vol = df['sell_vol'].sum()
                        total_vol = total_buy_vol + total_sell_vol
                        
                        avg_depth = (df['bid_qty'].mean() + df['ask_qty'].mean()) / 2
                        
                        iceberg_score = 0.0
                        iceberg_side = None
                        iceberg_timestamp = None
                        
                        if avg_depth > 0:
                            # 1. Calculate side-specific iceberg probability
                            # Increase thresholds to 20x and 10x from 10x and 5x to reduce false positives
                            buy_iceberg = min(1.0, (total_buy_vol / (avg_depth * 20))) if total_buy_vol > (avg_depth * 10) else 0.0
                            sell_iceberg = min(1.0, (total_sell_vol / (avg_depth * 20))) if total_sell_vol > (avg_depth * 10) else 0.0
                            
                            # Ensure minimum institutional size: 10 Lakhs (~1M INR) turnover in the minute
                            min_turnover_inr = 1000000
                            if (total_buy_vol * float(df_ohlc['ltp'].mean())) < min_turnover_inr:
                                buy_iceberg = 0.0
                            if (total_sell_vol * float(df_ohlc['ltp'].mean())) < min_turnover_inr:
                                sell_iceberg = 0.0
                            
                            if buy_iceberg >= sell_iceberg and buy_iceberg > 0:
                                iceberg_score = buy_iceberg
                                iceberg_side = "SELL" # Hidden seller absorbed buys -> Action: Caution (SELL mood)
                            elif sell_iceberg > buy_iceberg:
                                iceberg_score = sell_iceberg
                                iceberg_side = "BUY"  # Hidden buyer absorbed sells -> Action: Opportunity (BUY mood)
                                
                            # 2. Identify the peak volume timestamp in this minute
                            if iceberg_score > 0:
                                # Find the tick with the highest individual vol_surge (or just highest absolute vol)
                                peak_idx = df['buy_vol'].idxmax() if iceberg_side == "BUY" else df['sell_vol'].idxmax()
                                iceberg_timestamp = df.loc[peak_idx, 'timestamp']
                                
                                # 3. Publish to dedicated Real-time Iceberg channel
                                if self.redis_client:
                                    alert = {
                                        "symbol": symbol,
                                        "time": iceberg_timestamp.isoformat() if hasattr(iceberg_timestamp, 'isoformat') else str(iceberg_timestamp),
                                        "ltp": float(df.loc[peak_idx, 'ltp']),
                                        "actiontobetaken": iceberg_side
                                    }
                                    self.redis_client.publish("icebergs", json.dumps(alert))
                                    logger.warning(f"ICEBERG DETECTED: {symbol} @ {alert['ltp']} -> ACTION: {iceberg_side}")

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
                            "avg_bid_qty": float(df['bid_qty'].mean()),
                            "avg_ask_qty": float(df['ask_qty'].mean()),
                            "total_bid_qty": float(df['total_bid_qty'].mean()) if 'total_bid_qty' in df.columns else 0.0,
                            "total_ask_qty": float(df['total_ask_qty'].mean()) if 'total_ask_qty' in df.columns else 0.0,
                            "bid_pct": float(df['bid_pct'].mean()) if 'bid_pct' in df.columns else 50.0,
                            "ask_pct": float(df['ask_pct'].mean()) if 'ask_pct' in df.columns else 50.0,
                            "mean_imbalance": float(df['imbalance'].mean()),
                            "iceberg_score": iceberg_score,
                            "iceberg_timestamp": iceberg_timestamp,
                            "iceberg_side": iceberg_side,
                            "total_turnover": float(df_ohlc['ltp'].sum()), 
                            "source": self.depth_source
                        }
                        bars_to_upsert.append(ohlc)
                    
                    if bars_to_upsert:
                        # Perform Bulk Upsert (MySQL optimized)
                        stmt = mysql_insert(IntradayTick).values(bars_to_upsert)
                        # Update all non-PK columns on collision
                        update_dict = {
                            c.name: c for c in stmt.inserted 
                            if not c.primary_key and c.name != 'last_updated'
                        }
                        upsert_stmt = stmt.on_duplicate_key_update(**update_dict)
                        session.execute(upsert_stmt)
                        session.commit()
                        
                        # 3. Cache 1-Minute Bars in Redis (Full Day)
                        if self.redis_client:
                            for bar in bars_to_upsert:
                                bar_key = f"bars:{bar['symbol']}"
                                # Convert timestamp to string for JSON serialization
                                bar_copy = bar.copy()
                                bar_copy['timestamp'] = bar_copy['timestamp'].isoformat()
                                self.redis_client.rpush(bar_key, json.dumps(bar_copy))
                                self.redis_client.expire(bar_key, 86400) # 24h
                        
                        duration = time.time() - flush_start
                        logger.success(f"Successfully flushed {len(bars_to_upsert)} bars to MySQL and Redis in {duration:.2f}s")
                    else:
                        logger.info("Nothing to flush to MySQL this interval.")

                except Exception as e:
                    session.rollback()
                    logger.error(f"Error during storage flush: {e}")
                finally:
                    session.close()

            except Exception as e:
                logger.error(f"Storage Flush Loop Error: {e}")

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
            
            # REST historical priming (Move to background thread to allow immediate live data)
            prime_thread = threading.Thread(target=self._prime_historical_data, args=(self.symbols,), daemon=True)
            prime_thread.start()
            
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
        priority_syms = ["SBIN", "RELIANCE"]
        sorted_symbols = sorted(self.symbols, key=lambda x: (x not in priority_syms, x))
        
        # 1. Prepare Security ID mapping for standard symbols
        id_map = {}
        stock_ids = []
        for sym in sorted_symbols:
            sec_id = self.dhan_data.get_security_id(sym)
            if sec_id:
                id_map[sec_id] = sym
                stock_ids.append(sec_id)
        
        # 2. Add Selective Indices to Standard Feed (Core + Relevant Benchmarks)
        index_map = self.dhan_data.get_selective_indices(index_names=list(self.target_indices))
        all_index_ids = list(index_map.values())
        
        # Prepare Comprehensive Meta Mapping
        comprehensive_meta = {}
        # Stocks
        for sym in sorted_symbols:
            meta = self.dhan_data._meta_map.get(sym)
            if meta:
                comprehensive_meta[meta['id']] = {"symbol": sym, "segment": meta['segment']}
        
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
            symbol = dhan_tick['symbol']
            
            # Normalize to match _process_tick expected structure
            # Note: Dhan 20-depth doesn't give TTQ/LTP. 
            # We will use the last known LTP from priming or state.
            if symbol not in self.current_state:
                self.current_state[symbol] = {}
            
            state = self.current_state[symbol]
            
            # Ensure all required keys exist
            defaults = {
                "ltp": 0.0, "volume": 0, "bid_price": 0.0, "bid_qty": 0.0, 
                "ask_price": 0.0, "ask_qty": 0.0, "total_bid_qty": 0.0, "total_ask_qty": 0.0,
                "v_buy": 0.0, "v_sell": 0.0, "prev_ltp": 0.0,
                "bridge_active": False
            }
            for k, v in defaults.items():
                if k not in state:
                    state[k] = v
            
            # Update depth from Dhan rows
            bids = dhan_tick['bid']
            asks = dhan_tick['ask']
            ltp = dhan_tick.get('ltp', 0.0)
            volume = dhan_tick.get('volume', 0)
            
            total_bid_qty = dhan_tick.get('total_buy_qty', sum(b['qty'] for b in bids))
            total_ask_qty = dhan_tick.get('total_sell_qty', sum(a['qty'] for a in asks))
            
            # Update state for the NSE symbol
            if ltp > 0:
                state['ltp'] = ltp
            if volume > 0:
                # We calculate buy/sell vol by diffing volume if possible
                if state.get('volume', 0) > 0 and volume > state['volume']:
                    diff = volume - state['volume']
                    if state['ltp'] > state.get('prev_ltp', state['ltp']):
                        state['v_buy'] += diff
                    elif state['ltp'] < state.get('prev_ltp', state['ltp']):
                        state['v_sell'] += diff
                    
                    # Accumulate VWAP
                    self.vwap_num[symbol] += (state['ltp'] * diff)
                    self.vwap_den[symbol] += diff
                    
                state['volume'] = volume

            state['bid_qty'] = bids[0]['qty'] if bids else 0.0
            state['bid_price'] = bids[0]['price'] if bids else 0.0
            state['ask_qty'] = asks[0]['qty'] if asks else 0.0
            state['ask_price'] = asks[0]['price'] if asks else 0.0
            state['total_bid_qty'] = total_bid_qty
            state['total_ask_qty'] = total_ask_qty
            state['bids'] = bids
            state['asks'] = asks
            state['bridge_active'] = False 
            
            # Reuse the derived metrics and publishing logic
            self._publish_depth_packet(symbol, state)
            state['prev_ltp'] = state['ltp']
            
        except Exception as e:
            logger.error(f"Error processing Dhan tick: {e}")

    def _publish_depth_packet(self, target_symbol: str, state: dict):
        """Shared logic to calculate metrics and publish to Redis."""
        try:
            # Reusing the logic from _process_tick (lines 380-425)
            # Extracted to a helper to avoid duplication
            ltp = state.get('ltp', 0.0)
            if ltp <= 0:
                # logger.debug(f"Skipping depth packet for {target_symbol}: No LTP yet.")
                return 

            bid_qty = state['bid_qty']
            ask_qty = state['ask_qty']
            imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty) if (bid_qty + ask_qty) > 0 else 0.0
            
            # Update price history for momentum
            self.price_history[target_symbol].append(ltp)
            
            # VQS Momentum
            prev_ltp = self.price_history[target_symbol][-2] if len(self.price_history[target_symbol]) > 1 else ltp
            tick_dir = 1 if ltp > prev_ltp else (-1 if ltp < prev_ltp else 0)
            self.vqs_history[target_symbol].append(tick_dir)
            vqs_score = sum(self.vqs_history[target_symbol]) / len(self.vqs_history[target_symbol]) if self.vqs_history[target_symbol] else 0.0
            
            vwap = self.vwap_num[target_symbol] / self.vwap_den[target_symbol] if self.vwap_den[target_symbol] > 0 else ltp

            packet = {
                "symbol": target_symbol,
                "ltp": ltp,
                "imbalance": round(imbalance, 4),
                "vwap": round(vwap, 2),
                "vqs_score": round(vqs_score, 4),
                "vol_surge": 1.0,
                "bid_qty": bid_qty,
                "ask_qty": ask_qty,
                "total_bid_qty": state['total_bid_qty'],
                "total_ask_qty": state['total_ask_qty'],
                "bid_pct": round((state['total_bid_qty'] / (state['total_bid_qty'] + state['total_ask_qty']) * 100), 2) if (state['total_bid_qty'] + state['total_ask_qty']) > 0 else 50.0,
                "ask_pct": round((state['total_ask_qty'] / (state['total_bid_qty'] + state['total_ask_qty']) * 100), 2) if (state['total_bid_qty'] + state['total_ask_qty']) > 0 else 50.0,
                "buy_vol": state['v_buy'],
                "sell_vol": state['v_sell'],
                "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).isoformat(),
                "source": self.depth_source,
                "exchange_bridge": "Dhan20" if self.depth_source == "DHAN" else ("BSE+NSE" if state.get('bridge_active') else "NSE"),
                "bids": state.get('bids', [])[:5],
                "asks": state.get('asks', [])[:5]
            }
            
            # Add Top 20 for priority focus symbols
            if len(state.get('bids', [])) > 5:
                packet["bids_20"] = state['bids']
                packet["asks_20"] = state['asks']
            
            state['v_buy'] = 0
            state['v_sell'] = 0

            if self.redis_client:
                self.redis_client.publish(self.REDIS_CHANNEL, json.dumps(packet))
                self.redis_client.set(f"depth:{target_symbol}", json.dumps(packet), ex=60)
                tick_key = f"ticks:{target_symbol}"
                self.redis_client.rpush(tick_key, json.dumps(packet))
                self.redis_client.ltrim(tick_key, -3000, -1)
            
            with self.buffer_lock:
                self.tick_buffer[target_symbol].append(packet)
        except Exception as e:
            logger.error(f"Error publishing depth packet: {e}")

    def stop(self):
        self.running = False
        logger.info("Stopping MarketDepthService...")

if __name__ == "__main__":
    service = MarketDepthService()
    try:
        service.run()
    except KeyboardInterrupt:
        service.stop()

