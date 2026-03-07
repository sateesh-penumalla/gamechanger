import pandas as pd
from datetime import datetime
import pytz
import os
import json
import redis
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func
from sqlalchemy.dialects.mysql import insert as mysql_insert

from src.db.schema import IntradayTick, DailyFocus, SystemJob
from src.data.dhan_client import DhanDataClient
from src.data.truedata_client import TrueDataClient

load_dotenv()

# Configure Logging
log_file = "logs/intraday_feeder.log"
os.makedirs("logs", exist_ok=True)
logger.add(log_file, rotation="100 MB", level="INFO")

class IntradayFeeder:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.dhan_client = DhanDataClient()
        self.td_client = TrueDataClient()
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        
        # Redis setup
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.redis_client = None
        self._setup_redis()

    def _setup_redis(self):
        try:
            self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
        except Exception as e:
            logger.error(f"Redis Connection Failed: {e}")

    def _get_config(self, session):
        job = session.query(SystemJob).filter_by(job_id='intraday_feeder').first()
        return job.config if job and job.config else {}

    def run_cycle(self):
        """Fetches 1-minute data for Focused Stocks and updates DB + Redis."""
        session = self.Session()
        try:
            # 1. Get Focused Stocks for Today
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()
            
            focused_symbols = [r[0] for r in session.query(DailyFocus.symbol).distinct().all()]
            
            if not focused_symbols:
                logger.warning("No focused stocks found. Skipping fetch.")
                return

            logger.info(f"IntradayFeeder: Starting cycle for {len(focused_symbols)} symbols.")
            
            # 2. Determine Fetch Windows (Gap Filling)
            config = self._get_config(session)
            fetch_h = config.get('fetch_from_hour', 9)
            fetch_m = config.get('fetch_from_minute', 0)
            default_start = today.strftime("%Y-%m-%d") + f" {fetch_h:02}:{fetch_m:02}:00"
            default_start_dt = datetime.strptime(default_start, "%Y-%m-%d %H:%M:%S")

            latest_ticks = session.query(
                IntradayTick.symbol, 
                func.max(IntradayTick.timestamp).label('max_ts')
            ).filter(
                IntradayTick.symbol.in_(focused_symbols),
                func.date(IntradayTick.timestamp) == today
            ).group_by(IntradayTick.symbol).all()
            
            latest_map = {r[0]: r[1] for r in latest_ticks}
            
            from_dates = {}
            for sym in focused_symbols:
                if sym in latest_map:
                    from_dt = latest_map[sym] - pd.Timedelta(minutes=10)
                    if from_dt < default_start_dt:
                        from_dt = default_start_dt
                    from_dates[sym] = from_dt.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    from_dates[sym] = default_start

            source = config.get('source', 'DHAN_REST')
            api_results = {}

            if source == 'TRUEDATA_REST':
                for sym in focused_symbols:
                    fd_dt = datetime.strptime(from_dates[sym], "%Y-%m-%d %H:%M:%S")
                    df = self.td_client.fetch_intraday_bars(sym, fd_dt)
                    if df is not None:
                        api_results[sym] = df
            else:
                api_results = self.dhan_client.fetch_bulk_intraday(
                    focused_symbols, 
                    interval="1m", 
                    from_dates=from_dates
                )
            
            # 3. Process & Sync (DB + Redis)
            all_tick_objects = []
            for sym, df_ticks in api_results.items():
                if df_ticks is None or df_ticks.empty: continue
                
                # Sort ascending for chronological consistency
                df_ticks = df_ticks.sort_index(ascending=True)
                
                symbol_bars = []
                for ts, row in df_ticks.iterrows():
                    ts_obj = pd.to_datetime(ts)
                    if ts_obj.tzinfo:
                        ts_obj = ts_obj.tz_convert('Asia/Kolkata').tz_localize(None)
                    
                    bar_data = {
                        "symbol": sym,
                        "timestamp": ts_obj,
                        "open": float(row['Open']),
                        "high": float(row['High']),
                        "low": float(row['Low']),
                        "close": float(row['Close']),
                        "volume": int(row['Volume']),
                        "source": "DHAN_REST_FEEDER" if source == "DHAN_REST" else "TRUEDATA_REST_FEEDER"
                    }
                    all_tick_objects.append(bar_data)
                    symbol_bars.append(bar_data)

                # Redis Sync per Symbol
                if self.redis_client and symbol_bars:
                    try:
                        bar_key = f"bars:{sym}"
                        # For gap filling, we use chronological order
                        # We only push if not already in Redis (simplified check)
                        existing_bars = self.redis_client.lrange(bar_key, -100, -1)
                        existing_ts = set()
                        for b_json in existing_bars:
                            try:
                                b = json.loads(b_json)
                                existing_ts.add(b['timestamp'])
                            except: continue

                        for bar in symbol_bars:
                            ts_str = bar['timestamp'].isoformat()
                            if ts_str not in existing_ts:
                                bar_copy = bar.copy()
                                bar_copy['timestamp'] = ts_str
                                self.redis_client.rpush(bar_key, json.dumps(bar_copy))
                        
                        self.redis_client.expire(bar_key, 86400)
                    except Exception as redis_err:
                        logger.error(f"Redis Sync Failed for {sym}: {redis_err}")

            if all_tick_objects:
                stmt = mysql_insert(IntradayTick).values(all_tick_objects)
                upsert_stmt = stmt.on_duplicate_key_update(
                    open=func.if_(IntradayTick.source != 'TRUEDATA', stmt.inserted.open, IntradayTick.open),
                    high=func.if_(IntradayTick.source != 'TRUEDATA', stmt.inserted.high, IntradayTick.high),
                    low=func.if_(IntradayTick.source != 'TRUEDATA', stmt.inserted.low, IntradayTick.low),
                    close=func.if_(IntradayTick.source != 'TRUEDATA', stmt.inserted.close, IntradayTick.close),
                    volume=func.if_(IntradayTick.source != 'TRUEDATA', stmt.inserted.volume, IntradayTick.volume),
                    source=func.if_(IntradayTick.source != 'TRUEDATA', stmt.inserted.source, IntradayTick.source),
                    last_updated=func.now()
                )
                session.execute(upsert_stmt)
                session.commit()
                logger.info(f"IntradayFeeder: Synced {len(all_tick_objects)} ticks across {len(api_results)} symbols.")
            else:
                logger.info("IntradayFeeder: No new ticks to sync.")
            
        except Exception as e:
            logger.error(f"IntradayFeeder Error: {e}")
            session.rollback()
        finally:
            session.close()

if __name__ == "__main__":
    # Test Run
    feeder = IntradayFeeder()
    feeder.run_cycle()
