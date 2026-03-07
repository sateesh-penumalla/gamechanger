import os
import json
import redis
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

def sync_redis_bars():
    # 1. Setup Connections
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True
    )
    
    # 2. Fetch data from MySQL for today
    query = text("""
        SELECT symbol, timestamp, open, high, low, close, volume, 
               buy_volume, sell_volume, avg_bid_qty, avg_ask_qty, 
               total_bid_qty, total_ask_qty, mean_imbalance, 
               iceberg_score, total_turnover, source
        FROM intraday_ticks 
        WHERE DATE(timestamp) = '2026-02-16'
        ORDER BY symbol, timestamp ASC
    """)
    
    logger.info("Fetching data from MySQL...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    if df.empty:
        logger.warning("No data found for today in MySQL.")
        return

    # 3. Process each symbol
    symbols = df['symbol'].unique()
    logger.info(f"Rebuilding Redis bars for {len(symbols)} symbols...")
    
    for symbol in symbols:
        bar_key = f"bars:{symbol}"
        # Delete old list
        redis_client.delete(bar_key)
        
        # Get symbol bars
        sym_df = df[df['symbol'] == symbol]
        
        bars = []
        for _, row in sym_df.iterrows():
            bar = {
                "symbol": row['symbol'],
                "timestamp": row['timestamp'].isoformat(),
                "open": float(row['open']),
                "high": float(row['high']),
                "low": float(row['low']),
                "close": float(row['close']),
                "volume": int(row['volume']),
                "buy_volume": float(row['buy_volume']),
                "sell_volume": float(row['sell_volume']),
                "avg_bid_qty": float(row['avg_bid_qty']),
                "avg_ask_qty": float(row['avg_ask_qty']),
                "total_bid_qty": float(row['total_bid_qty']),
                "total_ask_qty": float(row['total_ask_qty']),
                "mean_imbalance": float(row['mean_imbalance']),
                "iceberg_score": float(row['iceberg_score']),
                "total_turnover": float(row['total_turnover']),
                "source": row['source']
            }
            bars.append(json.dumps(bar))
        
        if bars:
            redis_client.rpush(bar_key, *bars)
            redis_client.expire(bar_key, 86400) # 24h
            
    logger.success(f"Successfully rebuilt Redis cache for {len(symbols)} symbols.")

if __name__ == "__main__":
    sync_redis_bars()
