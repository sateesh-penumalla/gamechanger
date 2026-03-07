import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import time
import json
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Project Imports
from src.db.schema import ORBSignal, IntradayTick
from src.utils.notifications import notify_new_signal

load_dotenv()

class VolumeSurgeScanner:
    """
    Periodically scans the intraday_ticks table for massive volume surges 
    and strong price action. Acts as a safety net for fast-moving stocks.
    """
    
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        
        # Scanner Config
        self.scan_interval = 60 # Seconds
        self.lookback_mins = 5
        self.min_vol_surge = 10.0 # 10x average volume
        self.min_price_move_pct = 0.5 # 0.5% in the last candle
        
        logger.info(f"VolumeSurgeScanner Initialized. Interval: {self.scan_interval}s, Min Surge: {self.min_vol_surge}x")

    def run(self):
        """Main loop for periodic scanning."""
        while True:
            try:
                self.scan()
            except Exception as e:
                logger.error(f"Scanner Error: {e}")
            
            time.sleep(self.scan_interval)

    def scan(self):
        """Perform one scan cycle across all symbols."""
        with self.Session() as session:
            # 1. Identity symbols with a recent tick
            now = datetime.now()
            cutoff = now - timedelta(minutes=self.lookback_mins)
            
            # Query for latest ticks across all symbols in the lookback
            # We want to find cases where the LATEST tick has a massive volume hurdle
            query = text(f"""
            SELECT t1.symbol, t1.timestamp, t1.close, t1.open, t1.volume
            FROM intraday_ticks t1
            INNER JOIN (
                SELECT symbol, MAX(timestamp) as max_ts
                FROM intraday_ticks
                WHERE timestamp > '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}'
                GROUP BY symbol
            ) t2 ON t1.symbol = t2.symbol AND t1.timestamp = t2.max_ts
            """)
            
            df_latest = pd.read_sql(query, self.engine.connect())
            if df_latest.empty:
                return

            for _, row in df_latest.iterrows():
                symbol = row['symbol']
                latest_vol = row['volume']
                latest_close = row['close']
                latest_open = row['open']
                timestamp = row['timestamp']
                
                # 2. Get Average Volume for this symbol (from the last 20 ticks)
                avg_vol_query = text(f"""
                SELECT AVG(volume) as avg_vol
                FROM (
                    SELECT volume
                    FROM intraday_ticks
                    WHERE symbol = '{symbol}'
                    AND timestamp < '{timestamp}'
                    ORDER BY timestamp DESC
                    LIMIT 20
                ) sub
                """)
                avg_vol_res = session.execute(avg_vol_query).fetchone()
                avg_vol = avg_vol_res[0] if avg_vol_res and avg_vol_res[0] else 0
                
                if avg_vol <= 0:
                    continue
                    
                surge = latest_vol / avg_vol
                price_move = (latest_close - latest_open) / latest_open
                
                # 3. Decision Logic
                if surge >= self.min_vol_surge:
                    if abs(price_move) >= (self.min_price_move_pct / 100.0):
                        side = "LONG" if price_move > 0 else "SHORT"
                        self._trigger_signal(session, symbol, side, latest_close, surge, price_move, row)

    def _trigger_signal(self, session, symbol, side, price, surge, move_pct, row_data):
        # Debounce: One scanner signal per symbol per 15 mins
        # (Scanner is slower, so we want to avoid spamming the same move)
        cutoff = datetime.now() - timedelta(minutes=15)
        existing = session.query(ORBSignal).filter(
            ORBSignal.symbol == symbol,
            ORBSignal.signal_type == "SCANNER_BREAKOUT",
            ORBSignal.timestamp > cutoff
        ).first()
        
        if existing:
            return

        logger.success(f"🔍 SCANNER BREAKOUT: {symbol} {side} @ {price} | Surge: {surge:.1f}x | Move: {move_pct*100:.2f}%")
        
        # Calculate SL/TP (Standard 0.5% SL, 1.5% TP for scanner)
        sl = price * 0.995 if side == "LONG" else price * 1.005
        tp = price * 1.015 if side == "LONG" else price * 0.985
        
        new_sig = ORBSignal(
            symbol=symbol,
            side=side,
            date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
            timestamp=datetime.now(),
            entry_price=price,
            sl=round(float(sl), 2),
            tp=round(float(tp), 2),
            signal_type="SCANNER_BREAKOUT",
            metrics={
                "surge": float(surge),
                "move_pct": float(move_pct * 100),
                "source": "DB_SCANNER"
            },
            status="PENDING"
        )
        
        session.add(new_sig)
        session.commit()
        
        # Notifications
        notify_new_signal(symbol, side, "SCANNER_BREAKOUT", price, f"Massive Database Surge: {surge:.1f}x identified.")

if __name__ == "__main__":
    scanner = VolumeSurgeScanner()
    scanner.run()
