import os
import sys
import time

# Add root directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import pandas as pd
import redis
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from collections import deque, defaultdict
from src.db.schema import IntradayTick, DailyFocus
from dotenv import load_dotenv

load_dotenv()

class MarketDepthSimulator:
    """
    REPLAY MODE: Reads historical CSV ticks and mocks the MarketDepthService.
    Publishes to Redis and populates MySQL at accelerated speed.
    """
    
    def __init__(self, symbol: str, playback_date: str, speed: float = 5.0):
        self.symbol = symbol.replace('.NS', '').strip()
        self.playback_date = playback_date
        self.speed = speed
        
        # Flex History: Search in Both directories
        self.history_file = None
        
        # Option A: CSV in data/history
        csv_path = f"data/history/{playback_date}/{self.symbol}.csv"
        # Option B: Parquet in data/historical_ticks
        parquet_path = f"data/historical_ticks/{self.symbol}/{playback_date}.parquet"
        
        if os.path.exists(parquet_path):
            self.history_file = parquet_path
            self.file_type = "parquet"
        elif os.path.exists(csv_path):
            self.history_file = csv_path
            self.file_type = "csv"
        
        if not self.history_file:
            # Check for alternative CSV path in some setups
            alt_csv = f"data/historical_ticks/{self.symbol}/{playback_date}.csv"
            if os.path.exists(alt_csv):
                self.history_file = alt_csv
                self.file_type = "csv"
        
        # Redis setup
        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True
        )
        
        # Database setup
        db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Metrics Tracking (Parity with live service)
        self.price_history = deque(maxlen=100)
        self.vol_history = deque(maxlen=100)
        self.vqs_history = deque(maxlen=100)
        self.vwap_num = 0.0
        self.vwap_den = 0.0
        self.last_ttq = 0
        
        # Buffering for aggregation
        self.tick_buffer = []

    def _cleanup_db(self):
        """Clears existing data for a clean replay."""
        session = self.SessionLocal()
        try:
            print(f"Sim: Cleaning up signals and ticks for {self.symbol} on {self.playback_date}...")
            # We map historical data onto 'today' for the live signal generator to see it
            today = date.today().strftime('%Y-%m-%d')
            
            session.execute(text(f"DELETE FROM orb_signals WHERE symbol = '{self.symbol}.NS' AND DATE(date) = '{today}'"))
            session.execute(text(f"DELETE FROM intraday_ticks WHERE symbol = '{self.symbol}.NS' AND DATE(timestamp) = '{today}'"))
            session.commit()
            
            # Also clear Redis bars to avoid duplicated history during replay
            bar_key = f"bars:{self.symbol}.NS"
            self.redis_client.delete(bar_key)
            print(f"Sim: Cleared Redis bars for {self.symbol}.NS")
        finally:
            session.close()

    def run(self):
        if not self.history_file or not os.path.exists(self.history_file):
            print(f"Error: History file not found for {self.symbol} on {self.playback_date}")
            print(f"Checked paths: data/history/{self.playback_date}/{self.symbol}.csv and data/historical_ticks/{self.symbol}/{self.playback_date}.parquet")
            return

        self._cleanup_db()
        
        print(f"Sim: Loading {self.file_type} for {self.symbol} from {self.history_file}...")
        if self.file_type == "parquet":
            df = pd.read_parquet(self.history_file)
        else:
            df = pd.read_csv(self.history_file)
        
        # Standardize columns
        df.columns = [c.lower() for c in df.columns]
        if 'updated_datetime' in df.columns:
            df = df.rename(columns={'updated_datetime': 'timestamp'})
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')

        # --- 0. Initialize Daily Focus for Today ---
        # The SignalGenerator scans DailyFocus for 'today'
        session = self.SessionLocal()
        today = date.today()
        
        # Calculate ORB from historical data (09:15-09:30)
        orb_data = df[(df['timestamp'].dt.time >= datetime.strptime("09:15", "%H:%M").time()) & 
                      (df['timestamp'].dt.time < datetime.strptime("09:30", "%H:%M").time())]
        
        if not orb_data.empty:
            o_h = orb_data['ltp'].max()
            o_l = orb_data['ltp'].min()
            print(f"Sim: Initializing DailyFocus for {self.symbol}.NS | ORB: {o_h} / {o_l}")
            
            focus = DailyFocus(
                symbol=f"{self.symbol}.NS",
                date=today,
                sector="SIMULATED",
                oracle_status="UP_SNIPER", # Default for simulation
                orb_high=o_h,
                orb_low=o_l,
                orb_high_clean=o_h + (o_h * 0.001),
                orb_low_clean=o_l - (o_l * 0.001),
                orb_window=15,
                avg_daily_turnover=100.0, # Default high liquidity for simulation
                weekly_rsi=65.0, # Pass RSI Long filters
                weekly_sma=100.0 # Pass SMA alignment (if price > 100)
            )
            session.merge(focus)
            session.commit()
        else:
            print("Sim Warning: No ORB data (09:15-09:30) found to initialize DailyFocus.")

        # --- 1. Start Replay ---
        print(f"Sim: Starting Replay at {self.speed}x speed...")
        
        session = self.SessionLocal()
        start_real_time = time.time()
        start_sim_time = df['timestamp'].iloc[0]
        
        last_flush_min = -1
        today = date.today()

        for idx, row in df.iterrows():
            # Virtual Clock Sync
            sim_elapsed = (row['timestamp'] - start_sim_time).total_seconds()
            real_wait = sim_elapsed / self.speed
            
            # Wait until it's time for this tick
            while (time.time() - start_real_time) < real_wait:
                time.sleep(0.001)

            # --- 1. Order Flow Metrics (Parity with MarketDepthService) ---
            ltp = float(row['ltp'])
            bid_qty = float(row.get('bid_qty', 0))
            ask_qty = float(row.get('ask_qty', 0))
            bid_price = float(row.get('bid', 0))
            ask_price = float(row.get('ask', 0))
            ttq = int(row.get('ttq', 0))
            
            vol_delta = ttq - self.last_ttq if self.last_ttq > 0 else 0
            self.last_ttq = ttq
            
            # Side Inference
            v_buy = vol_delta if ltp >= ask_price > 0 else 0
            v_sell = vol_delta if ltp <= bid_price > 0 else 0
            
            # Imbalance
            imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty) if (bid_qty + ask_qty) > 0 else 0
            
            # VWAP
            self.vwap_num += (ltp * vol_delta)
            self.vwap_den += vol_delta
            vwap = self.vwap_num / self.vwap_den if self.vwap_den > 0 else ltp
            
            # VQS (Momentum)
            prev_ltp = self.price_history[-1] if self.price_history else ltp
            self.price_history.append(ltp)
            self.vol_history.append(vol_delta)
            tick_dir = 1 if ltp > prev_ltp else (-1 if ltp < prev_ltp else 0)
            self.vqs_history.append(tick_dir)
            vqs_score = sum(self.vqs_history) / len(self.vqs_history) if self.vqs_history else 0.0
            
            # Map time to 'Today' for the production generator
            # This is key: we want the generator to think it's happening right now
            sim_timestamp = datetime.combine(today, row['timestamp'].time())

            packet = {
                "symbol": self.symbol,
                "ltp": ltp,
                "imbalance": round(imbalance, 4),
                "vwap": round(vwap, 2),
                "vqs_score": round(vqs_score, 4),
                "vol_surge": round(vol_delta / (sum(self.vol_history)/len(self.vol_history)) if self.vol_history and sum(self.vol_history)>0 else 1.0, 2),
                "bid_qty": bid_qty,
                "ask_qty": ask_qty,
                "buy_vol": v_buy,
                "sell_vol": v_sell,
                "timestamp": sim_timestamp.isoformat(),
                "source": "SIMULATOR"
            }

            # 2. Update Redis
            self.redis_client.publish("market_depth:LIVE", json.dumps(packet))
            self.redis_client.set(f"depth:{self.symbol}", json.dumps(packet), ex=60)

            # 3. Buffer for MySQL Aggregation
            self.tick_buffer.append(packet)
            
            # 4. Flush to MySQL on minute boundary
            cur_min = row['timestamp'].minute
            if last_flush_min != -1 and cur_min != last_flush_min:
                self._flush_buffer(session, sim_timestamp)
            last_flush_min = cur_min

        # Final flush
        self._flush_buffer(session, datetime.combine(today, df['timestamp'].iloc[-1].time()))
        print("Sim: Replay Complete.")
        session.close()

    def _flush_buffer(self, session, current_ts):
        if not self.tick_buffer: return
        
        df = pd.DataFrame(self.tick_buffer)
        bar_time = current_ts.replace(second=0, microsecond=0)
        
        # Calculate Iceberg (Simulated)
        total_vol = df['buy_vol'].sum() + df['sell_vol'].sum()
        avg_depth = (df['bid_qty'].mean() + df['ask_qty'].mean()) / 2
        iceberg_score = min(1.0, (total_vol / (avg_depth * 10))) if avg_depth > 0 else 0.0

        ohlc = {
            "symbol": f"{self.symbol}.NS", # Ensure .NS suffix for parity
            "timestamp": bar_time,
            "open": float(df['ltp'].iloc[0]),
            "high": float(df['ltp'].max()),
            "low": float(df['ltp'].min()),
            "close": float(df['ltp'].iloc[-1]),
            "volume": int(total_vol) if total_vol > 0 else len(df),
            "buy_volume": float(df['buy_vol'].sum()),
            "sell_volume": float(df['sell_vol'].sum()),
            "avg_bid_qty": float(df['bid_qty'].mean()),
            "avg_ask_qty": float(df['ask_qty'].mean()),
            "mean_imbalance": float(df['imbalance'].mean()),
            "iceberg_score": iceberg_score,
            "total_turnover": float(df['ltp'].sum()),
            "source": "SIMULATOR"
        }
        
        try:
            db_bar = IntradayTick(**ohlc)
            session.merge(db_bar)
            session.commit()

            # Sync to Redis for Dashboard Charts & Signal Generator History
            bar_key = f"bars:{ohlc['symbol']}" # Use ohlc['symbol'] which has .NS
            bar_json = json.dumps({
                **ohlc,
                "timestamp": bar_time.isoformat()
            })
            self.redis_client.rpush(bar_key, bar_json)
            self.redis_client.expire(bar_key, 86400)

            print(f"Sim: Flushed Bar for {bar_time.strftime('%H:%M')}")
        except Exception as e:
            session.rollback()
            print(f"Sim Flush Error: {e}")
            
        self.tick_buffer = []

if __name__ == "__main__":
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else "AVANTIFEED"
    playback_date = sys.argv[2] if len(sys.argv) > 2 else "2026-02-12"
    sim = MarketDepthSimulator(symbol, playback_date, speed=20.0)
    sim.run()
