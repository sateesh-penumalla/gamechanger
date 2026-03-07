import os
import sys
import argparse
from datetime import datetime
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import pandas as pd

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()

from src.data.dhan_client import DhanDataClient
from src.db.schema import HistoryDailyOHLC

def get_symbols_needing_backfill(engine):
    """Fetches all symbols that have records with 0 OI."""
    with engine.connect() as conn:
        res = conn.execute(text("SELECT symbol, MIN(timestamp), MAX(timestamp) FROM history_daily_ohlc WHERE oi = 0 GROUP BY symbol"))
        return res.fetchall()

def backfill_oi(session, client, symbol, min_date, max_date):
    """Fetches OI data from Dhan and updates existing records."""
    try:
        from_date_str = min_date.strftime("%Y-%m-%d")
        to_date_str = max_date.strftime("%Y-%m-%d")
        
        logger.info(f"Backfilling OI for {symbol} from {from_date_str} to {to_date_str}...")
        
        # Resolve Futures ID (OI comes from Futures, not Equity)
        fut_id = client.get_active_futures_id(symbol)
        
        if not fut_id:
            logger.warning(f"No FNO contract found for {symbol}. OI will remain 0.0")
            return
            
        segment = "NSE_FNO"
        instrument = "FUTSTK" # Default for stocks, will be FUTIDX for indices in mapping
        
        # Check if it's an index
        clean_sym = symbol.replace(".NS", "").replace("^", "").strip()
        if clean_sym in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NSEI", "NSEBANK"]:
            instrument = "FUTIDX"

        # Use historical charts API with the Futures ID to get OI
        df = client.get_historical_ohlc(
            security_id=str(fut_id),
            exchange_segment=segment,
            from_date=from_date_str,
            to_date=to_date_str,
            instrument=instrument,
            oi=True
        )
        
        if df is not None and not df.empty:
            count = 0
            for ts, row in df.iterrows():
                # Only update if OI > 0 (Dhan might still return 0, which is fine)
                new_oi = float(row.get('OI', 0.0))
                if new_oi >= 0:
                    clean_ts = ts.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    # Manual update to be safe and targeted
                    session.execute(
                        text("UPDATE history_daily_ohlc SET oi = :oi WHERE symbol = :symbol AND timestamp = :ts"),
                        {"oi": new_oi, "symbol": symbol, "ts": clean_ts}
                    )
                    count += 1
            
            session.commit()
            logger.success(f"Updated {count} records for {symbol}")
        else:
            logger.warning(f"No data returned for {symbol}")
            
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to backfill {symbol}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Backfill missing OI data for historical records.")
    parser.add_argument("--symbol", type=str, help="Specific symbol to backfill (optional).")
    args = parser.parse_args()
    
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    client = DhanDataClient()
    
    if args.symbol:
        # Check if this specific symbol needs backfill
        with engine.connect() as conn:
            res = conn.execute(text("SELECT MIN(timestamp), MAX(timestamp) FROM history_daily_ohlc WHERE symbol = :s AND oi = 0"), {"s": args.symbol})
            row = res.fetchone()
            if row and row[0]:
                backfill_oi(session, client, args.symbol, row[0], row[1])
            else:
                logger.info(f"No records needing backfill for {args.symbol}")
    else:
        results = get_symbols_needing_backfill(engine)
        logger.info(f"Found {len(results)} symbols needing OI backfill.")
        
        for symbol, min_date, max_date in results:
            backfill_oi(session, client, symbol, min_date, max_date)

    session.close()
    logger.success("Backfill process completed.")

if __name__ == "__main__":
    main()
