
import os
import pandas as pd
import redis
import json
import time
import pytz
from datetime import datetime
from collections import defaultdict, deque
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

class ParquetReplay:
    def __init__(self, target_date, symbols=None, speed=1.0, redis_host="localhost", redis_port=6379, map_to_today=True):
        self.target_date = target_date
        self.symbols = symbols
        self.speed = speed
        self.redis_host = redis_host
        self.redis_port = int(redis_port)
        self.redis_channel = "market_depth:LIVE"
        self.redis_client = None
        self.base_dir = "data/historical_ticks"
        self.map_to_today = map_to_today
        self.today_date = datetime.now(pytz.timezone('Asia/Kolkata')).date()
        
        # Metrics state (to match MarketDepthService)
        self.price_history = defaultdict(lambda: deque(maxlen=100))
        self.vqs_history = defaultdict(lambda: deque(maxlen=100))
        self.vwap_num = defaultdict(float)
        self.vwap_den = defaultdict(float)
        self.last_ttq = defaultdict(int)

    def _setup_redis(self):
        try:
            self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
        except Exception as e:
            logger.error(f"Redis Connection Failed: {e}")
            raise

    def _load_data(self):
        all_ticks = []
        target_symbols = self.symbols
        if not target_symbols:
            target_symbols = [d for d in os.listdir(self.base_dir) if os.path.isdir(os.path.join(self.base_dir, d))]
        
        logger.info(f"Loading data for {len(target_symbols)} symbols...")
        for sym in target_symbols:
            file_path = os.path.join(self.base_dir, sym, f"{self.target_date}.parquet")
            if os.path.exists(file_path):
                try:
                    df = pd.read_parquet(file_path)
                    df['symbol'] = sym
                    all_ticks.append(df)
                    logger.debug(f"Loaded {len(df)} ticks for {sym}")
                except Exception as e:
                    logger.error(f"Error loading {file_path}: {e}")
            else:
                logger.warning(f"File not found: {file_path}")
        
        if not all_ticks:
            return pd.DataFrame()
            
        combined_df = pd.concat(all_ticks)
        combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
        
        if self.map_to_today:
            logger.info(f"Mapping historical {self.target_date} timestamps to Today ({self.today_date})...")
            combined_df['timestamp'] = combined_df['timestamp'].apply(
                lambda x: datetime.combine(self.today_date, x.time())
            )
            
        combined_df = combined_df.sort_values(by='timestamp')
        return combined_df

    def _calculate_metrics(self, symbol, row):
        ltp = float(row['ltp'])
        volume = int(row['volume'])
        
        # VQS Momentum
        prev_ltp = self.price_history[symbol][-1] if self.price_history[symbol] else ltp
        tick_dir = 1 if ltp > prev_ltp else (-1 if ltp < prev_ltp else 0)
        self.vqs_history[symbol].append(tick_dir)
        vqs_score = sum(self.vqs_history[symbol]) / len(self.vqs_history[symbol]) if self.vqs_history[symbol] else 0.0
        self.price_history[symbol].append(ltp)
        
        # VWAP
        if volume > self.last_ttq[symbol]:
            vol_delta = volume - self.last_ttq[symbol]
            self.vwap_num[symbol] += (ltp * vol_delta)
            self.vwap_den[symbol] += vol_delta
            self.last_ttq[symbol] = volume
            
        vwap = self.vwap_num[symbol] / self.vwap_den[symbol] if self.vwap_den[symbol] > 0 else ltp
        
        return {
            "vqs_score": round(vqs_score, 4),
            "vwap": round(vwap, 2)
        }

    def run(self):
        self._setup_redis()
        df = self._load_data()
        if df.empty:
            logger.error("No data found to replay.")
            return

        logger.info(f"Starting Replay of {len(df)} ticks at {self.speed}x speed...")
        
        start_time = time.time()
        first_tick_ts = df.iloc[0]['timestamp']
        
        # Group by minute for bar pushing
        df['minute'] = df['timestamp'].dt.floor('min')
        last_flush_min = None
        minute_buffer = defaultdict(list)

        for _, row in df.iterrows():
            current_ts = row['timestamp']
            
            # Simulated sleep
            if self.speed > 0:
                elapsed_sim = (current_ts - first_tick_ts).total_seconds()
                elapsed_real = (time.time() - start_time) * self.speed
                wait_time = (elapsed_sim - elapsed_real) / self.speed
                if wait_time > 0:
                    time.sleep(wait_time)

            symbol = row['symbol']
            metrics = self._calculate_metrics(symbol, row)
            
            # Construct Packet (Dhan/MarketDepthService Format)
            bid_qty = float(row.get('bidqty', 0))
            ask_qty = float(row.get('askqty', 0))
            imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty) if (bid_qty + ask_qty) > 0 else 0.0
            
            packet = {
                "symbol": symbol,
                "ltp": float(row['ltp']),
                "imbalance": round(imbalance, 4),
                "vwap": metrics['vwap'],
                "vqs_score": metrics['vqs_score'],
                "vol_surge": 1.0,
                "bid_qty": bid_qty,
                "ask_qty": ask_qty,
                "total_bid_qty": bid_qty, # Simplified for Parquet
                "total_ask_qty": ask_qty,
                "bid_pct": round((bid_qty / (bid_qty + ask_qty) * 100), 2) if (bid_qty + ask_qty) > 0 else 50.0,
                "ask_pct": round((ask_qty / (bid_qty + ask_qty) * 100), 2) if (bid_qty + ask_qty) > 0 else 50.0,
                "buy_vol": 0, # Not easily derived from Parquet per-tick without side
                "sell_vol": 0,
                "timestamp": current_ts.isoformat(),
                "source": "ParquetReplay",
                "exchange_bridge": "NSE"
            }
            
            # Publish Ticket
            self.redis_client.publish(self.redis_channel, json.dumps(packet))
            self.redis_client.set(f"depth:{symbol}", json.dumps(packet), ex=60)
            
            # Buffer for bar creation
            current_min = row['minute']
            if last_flush_min and current_min > last_flush_min:
                self._flush_bars(minute_buffer, last_flush_min)
                minute_buffer = defaultdict(list)
            
            minute_buffer[symbol].append(packet)
            last_flush_min = current_min

        # Final flush
        if minute_buffer:
            self._flush_bars(minute_buffer, last_flush_min)

        logger.success("Replay Complete.")

    def _flush_bars(self, buffer, minute_ts):
        for symbol, ticks in buffer.items():
            if not ticks: continue
            
            df_ticks = pd.DataFrame(ticks)
            bar = {
                "symbol": symbol,
                "timestamp": minute_ts.isoformat(),
                "open": float(df_ticks['ltp'].iloc[0]),
                "high": float(df_ticks['ltp'].max()),
                "low": float(df_ticks['ltp'].min()),
                "close": float(df_ticks['ltp'].iloc[-1]),
                "volume": len(ticks), # Total ticks in that minute as proxy
                "source": "ParquetReplay"
            }
            bar_key = f"bars:{symbol}"
            self.redis_client.rpush(bar_key, json.dumps(bar))
            self.redis_client.expire(bar_key, 86400)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Replay Parquet ticks to Redis.")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--symbols", nargs="+", help="Symbols to replay")
    parser.add_argument("--speed", type=float, default=1.0, help="Replay speed multiplier")
    parser.add_argument("--port", type=int, default=int(os.getenv("REDIS_PORT", 6379)), help="Redis port")
    parser.add_argument("--no-map-to-today", action="store_false", dest="map_to_today", help="Do not shift timestamps to Today")
    parser.set_defaults(map_to_today=True)
    
    args = parser.parse_args()
    
    replay = ParquetReplay(
        target_date=args.date,
        symbols=args.symbols,
        speed=args.speed,
        redis_port=args.port,
        map_to_today=args.map_to_today
    )
    replay.run()
